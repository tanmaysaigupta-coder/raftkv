import http from 'node:http';
import { RaftNode } from './raft/node.js';
import { HttpTransport } from './raft/transport.http.js';
import { KVStore } from './kv/store.js';
import type { Command, NodeId } from './raft/types.js';

/**
 * A single cluster member: one HTTP server exposing both the internal Raft
 * RPC endpoints (/raft/*) and the public client API (/kv/*, /status).
 * Started as its own OS process, one per Raft node — see cluster.ts for the
 * process that spawns a whole cluster of these.
 *
 * Config comes from env vars so cluster.ts can launch each node as a plain
 * child process with no shared memory:
 *   NODE_ID    e.g. "n1"
 *   PORT       e.g. 4001
 *   PEERS      e.g. "n2=http://localhost:4002,n3=http://localhost:4003"
 */
function readConfig() {
  const id = process.env.NODE_ID;
  const port = Number(process.env.PORT);
  const peersEnv = process.env.PEERS ?? '';
  if (!id || !port) {
    throw new Error('NODE_ID and PORT env vars are required');
  }
  const addresses = new Map<NodeId, string>();
  const peers: NodeId[] = [];
  if (peersEnv.trim().length > 0) {
    for (const pair of peersEnv.split(',')) {
      const [peerId, addr] = pair.split('=');
      if (!peerId || !addr) continue;
      peers.push(peerId);
      addresses.set(peerId, addr);
    }
  }
  return { id, port, peers, addresses };
}

async function readJsonBody<T>(req: http.IncomingMessage): Promise<T> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString('utf8');
  return raw.length ? (JSON.parse(raw) as T) : ({} as T);
}

function sendJson(res: http.ServerResponse, status: number, body: unknown) {
  const payload = JSON.stringify(body);
  res.writeHead(status, {
    'content-type': 'application/json',
    'content-length': Buffer.byteLength(payload),
    'access-control-allow-origin': '*',
  });
  res.end(payload);
}

async function main() {
  const { id, port, peers, addresses } = readConfig();
  const store = new KVStore();
  const transport = new HttpTransport(addresses);

  const node = new RaftNode({
    id,
    peers,
    transport,
    onApply: (command: Command, index: number) => store.apply(command, index),
    onEvent: (message) => console.log(`[${id}] ${message}`),
  });

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', `http://localhost:${port}`);

      if (req.method === 'POST' && url.pathname === '/raft/request-vote') {
        const args = await readJsonBody(req);
        return sendJson(res, 200, node.handleRequestVote(args as any));
      }

      if (req.method === 'POST' && url.pathname === '/raft/append-entries') {
        const args = await readJsonBody(req);
        return sendJson(res, 200, node.handleAppendEntries(args as any));
      }

      if (req.method === 'GET' && url.pathname === '/status') {
        return sendJson(res, 200, { ...node.getStatus(), kvSize: store.size, appliedIndex: store.lastAppliedIndex });
      }

      if (req.method === 'GET' && url.pathname === '/kv/all') {
        return sendJson(res, 200, { entries: store.entries() });
      }

      if (req.method === 'GET' && url.pathname === '/kv/get') {
        const key = url.searchParams.get('key') ?? '';
        const value = store.get(key);
        return sendJson(res, value === undefined ? 404 : 200, { key, value: value ?? null });
      }

      if (req.method === 'POST' && url.pathname === '/kv/set') {
        const { key, value } = await readJsonBody<{ key: string; value: string }>(req);
        const result = await node.propose({ type: 'set', key, value });
        if (!result.ok) return sendJson(res, 409, { error: 'not leader', leaderId: result.leaderId });
        return sendJson(res, 200, { ok: true, index: result.index });
      }

      if (req.method === 'POST' && url.pathname === '/kv/delete') {
        const { key } = await readJsonBody<{ key: string }>(req);
        const result = await node.propose({ type: 'delete', key });
        if (!result.ok) return sendJson(res, 409, { error: 'not leader', leaderId: result.leaderId });
        return sendJson(res, 200, { ok: true, index: result.index });
      }

      sendJson(res, 404, { error: 'not found' });
    } catch (err) {
      sendJson(res, 500, { error: (err as Error).message });
    }
  });

  server.listen(port, () => {
    console.log(`[${id}] listening on :${port} (peers: ${peers.join(', ') || 'none'})`);
  });

  node.start();

  const shutdown = () => {
    node.stop();
    server.close(() => process.exit(0));
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
