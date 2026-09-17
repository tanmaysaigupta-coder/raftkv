import { test } from 'node:test';
import assert from 'node:assert/strict';
import { RaftNode } from '../src/raft/node.js';
import { InMemoryTransport } from '../src/raft/transport.memory.js';
import { KVStore } from '../src/kv/store.js';
import type { NodeId, RaftStatus } from '../src/raft/types.js';

interface Cluster {
  nodes: RaftNode[];
  stores: Map<NodeId, KVStore>;
  transport: InMemoryTransport;
}

function buildCluster(size: number, opts: { electionTimeoutMs?: number; heartbeatIntervalMs?: number } = {}): Cluster {
  const transport = new InMemoryTransport(1);
  const ids = Array.from({ length: size }, (_, i) => `n${i + 1}`);
  const stores = new Map<NodeId, KVStore>();
  const nodes = ids.map((id) => {
    const store = new KVStore();
    stores.set(id, store);
    const node = new RaftNode({
      id,
      peers: ids.filter((p) => p !== id),
      transport,
      onApply: (command, index) => store.apply(command, index),
      electionTimeoutMs: opts.electionTimeoutMs ?? 40,
      heartbeatIntervalMs: opts.heartbeatIntervalMs ?? 10,
    });
    transport.register(node);
    return node;
  });
  return { nodes, stores, transport };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2000, stepMs = 10) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, stepMs));
  }
  throw new Error('timed out waiting for condition');
}

function statuses(nodes: RaftNode[]): RaftStatus[] {
  return nodes.map((n) => n.getStatus());
}

function findLeader(nodes: RaftNode[]): RaftNode | undefined {
  return nodes.find((n) => n.getStatus().role === 'leader');
}

test('a cluster elects exactly one leader', async () => {
  const { nodes } = buildCluster(5);
  nodes.forEach((n) => n.start());
  try {
    await waitFor(() => statuses(nodes).filter((s) => s.role === 'leader').length === 1);
    const leaders = statuses(nodes).filter((s) => s.role === 'leader');
    assert.equal(leaders.length, 1);
  } finally {
    nodes.forEach((n) => n.stop());
  }
});

test('a committed write is replicated to every node', async () => {
  const { nodes, stores } = buildCluster(5);
  nodes.forEach((n) => n.start());
  try {
    await waitFor(() => findLeader(nodes) !== undefined);
    const leader = findLeader(nodes)!;

    const result = await leader.propose({ type: 'set', key: 'hello', value: 'world' });
    assert.equal(result.ok, true);

    await waitFor(() => nodes.every((n) => stores.get(n.id)!.get('hello') === 'world'));
    for (const n of nodes) {
      assert.equal(stores.get(n.id)!.get('hello'), 'world');
    }
  } finally {
    nodes.forEach((n) => n.stop());
  }
});

test('a non-leader rejects writes and reports the current leader', async () => {
  const { nodes } = buildCluster(3);
  nodes.forEach((n) => n.start());
  try {
    await waitFor(() => findLeader(nodes) !== undefined);
    const leader = findLeader(nodes)!;
    const follower = nodes.find((n) => n.id !== leader.id)!;

    // Give the follower a moment to receive the leader's first heartbeat
    // and learn its id, exactly as a real client would have to.
    await waitFor(() => follower.getStatus().leaderId === leader.id);

    const result = await follower.propose({ type: 'set', key: 'x', value: '1' });
    assert.equal(result.ok, false);
    if (!result.ok) assert.equal(result.leaderId, leader.id);
  } finally {
    nodes.forEach((n) => n.stop());
  }
});

test('killing the leader triggers re-election and the cluster keeps accepting writes', async () => {
  const { nodes, stores, transport } = buildCluster(5);
  nodes.forEach((n) => n.start());
  try {
    await waitFor(() => findLeader(nodes) !== undefined);
    const firstLeader = findLeader(nodes)!;
    const firstTerm = firstLeader.getStatus().term;

    // Simulate a hard crash: stop its timers AND make it unreachable, so it
    // can neither win a new election nor keep acting as the old leader.
    firstLeader.stop();
    transport.partition(firstLeader.id);

    const survivors = nodes.filter((n) => n.id !== firstLeader.id);
    await waitFor(() => {
      const leader = findLeader(survivors);
      return leader !== undefined && leader.getStatus().term > firstTerm;
    });

    const newLeader = findLeader(survivors)!;
    const result = await newLeader.propose({ type: 'set', key: 'after-failover', value: 'ok' });
    assert.equal(result.ok, true);

    await waitFor(() => survivors.every((n) => stores.get(n.id)!.get('after-failover') === 'ok'));
  } finally {
    nodes.forEach((n) => n.stop());
  }
});

test('a rejoining node catches up on missed log entries', async () => {
  const { nodes, stores, transport } = buildCluster(3, { electionTimeoutMs: 30, heartbeatIntervalMs: 8 });
  nodes.forEach((n) => n.start());
  try {
    await waitFor(() => findLeader(nodes) !== undefined);
    let leader = findLeader(nodes)!;
    const laggard = nodes.find((n) => n.id !== leader.id)!;

    // Take the laggard offline before the write happens.
    laggard.stop();
    transport.partition(laggard.id);

    const r1 = await leader.propose({ type: 'set', key: 'k1', value: 'v1' });
    assert.equal(r1.ok, true);

    // Bring it back — it should catch up via normal AppendEntries replication.
    transport.heal(laggard.id);
    laggard.start();

    await waitFor(() => stores.get(laggard.id)!.get('k1') === 'v1', 3000);
    assert.equal(stores.get(laggard.id)!.get('k1'), 'v1');
  } finally {
    nodes.forEach((n) => n.stop());
  }
});
