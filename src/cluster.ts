import http from 'node:http';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
import { spawn, type ChildProcess } from 'node:child_process';

/**
 * Coordinator process: spawns a real cluster of Raft node processes (one OS
 * process per node — genuine crash-and-restart, not simulated) and serves a
 * live dashboard for watching/steering it: leader election, term changes,
 * log replication, and killing/reviving nodes on demand.
 */

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRoot = path.resolve(__dirname, '..');

const NODE_COUNT = Number(process.env.CLUSTER_SIZE ?? 5);
const BASE_PORT = Number(process.env.BASE_PORT ?? 4001);
const DASHBOARD_PORT = Number(process.env.DASHBOARD_PORT ?? 4000);

const ids = Array.from({ length: NODE_COUNT }, (_, i) => `n${i + 1}`);
const ports = new Map(ids.map((id, i) => [id, BASE_PORT + i]));
const addresses = new Map(ids.map((id) => [id, `http://localhost:${ports.get(id)}`]));

const children = new Map<NodeId, ChildProcess>();
type NodeId = string;
const killedByUser = new Set<NodeId>();

function spawnNode(id: NodeId) {
  const port = ports.get(id)!;
  const peers = ids.filter((p) => p !== id);
  const peersEnv = peers.map((p) => `${p}=${addresses.get(p)}`).join(',');

  const child = spawn(process.execPath, ['--import', 'tsx', path.join(projectRoot, 'src/server.ts')], {
    cwd: projectRoot,
    env: { ...process.env, NODE_ID: id, PORT: String(port), PEERS: peersEnv },
    stdio: 'inherit',
  });
  children.set(id, child);
  child.on('exit', () => {
    if (children.get(id) === child) children.delete(id);
  });
}

for (const id of ids) spawnNode(id);

function killNode(id: NodeId) {
  const child = children.get(id);
  killedByUser.add(id);
  if (child) child.kill('SIGKILL');
}

function reviveNode(id: NodeId) {
  killedByUser.delete(id);
  if (!children.has(id)) spawnNode(id);
}

async function fetchStatus(id: NodeId) {
  const port = ports.get(id)!;
  if (killedByUser.has(id) || !children.has(id)) return { id, role: 'down' as const, port };
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 400);
    const res = await fetch(`${addresses.get(id)}/status`, { signal: controller.signal });
    clearTimeout(timer);
    if (!res.ok) throw new Error(String(res.status));
    const json = (await res.json()) as Record<string, unknown>;
    return { ...json, port };
  } catch {
    return { id, role: 'down' as const, port };
  }
}

async function readJson<T>(req: http.IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString('utf8');
  return raw.length ? (JSON.parse(raw) as T) : ({} as T);
}

function sendJson(res: http.ServerResponse, status: number, body: unknown) {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

/** Try every live node for a write/read, preferring whichever one last claimed to be leader. */
let lastKnownLeader: NodeId | null = null;

async function proposeToCluster(path_: string, body: unknown): Promise<{ status: number; json: any }> {
  const order = lastKnownLeader ? [lastKnownLeader, ...ids.filter((i) => i !== lastKnownLeader)] : ids;
  let last: { status: number; json: any } = { status: 503, json: { error: 'cluster unreachable' } };
  for (const id of order) {
    if (killedByUser.has(id) || !children.has(id)) continue;
    try {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 800);
      const res = await fetch(`${addresses.get(id)}${path_}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      clearTimeout(timer);
      const json = (await res.json()) as { leaderId?: NodeId; [key: string]: unknown };
      if (res.ok) {
        lastKnownLeader = id;
        return { status: res.status, json };
      }
      if (json.leaderId) lastKnownLeader = json.leaderId;
      last = { status: res.status, json };
    } catch {
      // try next node
    }
  }
  return last;
}

const dashboardHtml = fs.readFileSync(path.join(projectRoot, 'public/dashboard.html'), 'utf8');

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url ?? '/', `http://localhost:${DASHBOARD_PORT}`);

  if (req.method === 'GET' && url.pathname === '/') {
    res.writeHead(200, { 'content-type': 'text/html' });
    res.end(dashboardHtml);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/api/status') {
    const statuses = await Promise.all(ids.map(fetchStatus));
    return sendJson(res, 200, { nodes: statuses, clusterSize: ids.length });
  }

  if (req.method === 'POST' && url.pathname === '/api/kill') {
    const { id } = await readJson<{ id: NodeId }>(req);
    killNode(id);
    return sendJson(res, 200, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/revive') {
    const { id } = await readJson<{ id: NodeId }>(req);
    reviveNode(id);
    return sendJson(res, 200, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/write') {
    const { key, value, _delete } = await readJson<{ key: string; value?: string; _delete?: boolean }>(req);
    const result = _delete
      ? await proposeToCluster('/kv/delete', { key })
      : await proposeToCluster('/kv/set', { key, value });
    return sendJson(res, result.status, result.json);
  }

  if (req.method === 'GET' && url.pathname === '/api/read') {
    const key = url.searchParams.get('key') ?? '';
    for (const id of ids) {
      if (killedByUser.has(id) || !children.has(id)) continue;
      try {
        const r = await fetch(`${addresses.get(id)}/kv/get?key=${encodeURIComponent(key)}`);
        const j = await r.json();
        if (r.ok) return sendJson(res, 200, j);
      } catch {
        // try next node
      }
    }
    return sendJson(res, 404, { key, value: null });
  }

  sendJson(res, 404, { error: 'not found' });
});

server.listen(DASHBOARD_PORT, () => {
  console.log(`\nDashboard:  http://localhost:${DASHBOARD_PORT}`);
  console.log(`Cluster:    ${ids.map((id) => `${id}@${ports.get(id)}`).join(', ')}\n`);
});

function shutdown() {
  for (const child of children.values()) child.kill('SIGTERM');
  server.close(() => process.exit(0));
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
