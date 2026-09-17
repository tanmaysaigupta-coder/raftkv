# RaftKV

**A distributed key-value store built on a Raft consensus implementation written from scratch — no consensus library, no framework.** Ships with a live cluster dashboard: watch leader election happen in real time, kill the leader mid-demo, and watch the cluster recover and keep serving writes.

Raft is the algorithm behind etcd (Kubernetes), Consul, CockroachDB, and TiKV. This project implements it directly against [the paper](https://raft.github.io/raft.pdf) (Ongaro & Ousterhout, 2014) — leader election, log replication, safety, and the §5.3 fast-backtrack optimization — as a standalone, tested library, then wraps it in a real multi-process cluster you can break on purpose.

## What it does

Run a 5-node cluster as five **separate OS processes**, each fully independent, each running the same `RaftNode`. They elect a leader among themselves with no coordinator. Writes go to the leader, get replicated to a majority before they're acknowledged, and are visible on every node once committed. Kill the leader process outright — `SIGKILL`, not a graceful shutdown — and the remaining nodes detect the failure within one election timeout and elect a new leader, with zero data loss for anything that was already committed.

The dashboard shows this happening live: node roles (leader/candidate/follower/down), current term, log length, commit index, and the actual replicated key-value state, polled twice a second. Kill/Revive buttons on each node let you crash and restart real processes and watch the log catch a rejoined node back up.

## Quickstart

```bash
npm install
npm run cluster
```

Open **http://localhost:4000**. Five node processes spin up automatically (ports 4001–4005), a leader is elected within ~1–2 seconds, and the dashboard starts polling. Use the SET/GET/DELETE form to write through the cluster, or kill the current leader and watch a new one take over.

```bash
npm run cli -- set foo bar
npm run cli -- get foo
```

talks to the same coordinator API the dashboard uses.

## Architecture

```
src/
  raft/
    types.ts             Wire types (RequestVote/AppendEntries RPCs, LogEntry, ...)
    node.ts               RaftNode — the actual consensus algorithm
    transport.memory.ts   In-process transport for deterministic tests
    transport.http.ts     HTTP+JSON transport for the real multi-process cluster
  kv/
    store.ts              The replicated state machine (a Map, applied in log order)
  server.ts                One cluster member: HTTP server exposing Raft RPCs + a client API
  cluster.ts                Coordinator: spawns N node processes, serves the dashboard + kill/revive controls
  client.ts                 Minimal CLI
public/
  dashboard.html             Live cluster visualization (vanilla JS, no build step)
test/
  raft.test.ts                Election, replication, leader failover, and catch-up, over the in-memory transport
```

`RaftNode` is transport-agnostic — it only knows about a `Transport` interface with `requestVote` / `appendEntries`. That's what makes the test suite possible: tests wire up 3–5 `RaftNode`s over an `InMemoryTransport` that can simulate network partitions on demand, and assert on real election/replication behavior in milliseconds, without spinning up processes or racing wall-clock timers. The live cluster swaps in `HttpTransport` and nothing else changes.

## How the algorithm works

1. **Leader election.** Every node starts a randomized election timer (150–300ms scale, tunable). If it fires without hearing from a leader, the node becomes a candidate, increments its term, votes for itself, and requests votes from every peer in parallel. A candidate that gets votes from a majority (including itself) becomes leader; everyone else remains, or reverts to, follower.
2. **Log replication.** The leader appends client commands to its own log, then replicates them to followers via `AppendEntries`. An entry is **committed** once it's stored on a majority of nodes — at that point it's applied to the key-value state machine and the client gets an answer.
3. **Safety.** A candidate can only win votes if its log is at least as up-to-date as the voter's (§5.4.1), which is what guarantees a new leader always has every previously-committed entry — no committed write is ever lost or overwritten by an election. Followers detect and truncate diverging log tails (§5.3) if a previous leader partially replicated something that never got committed.
4. **Catch-up.** A node that was down (or partitioned) and rejoins just looks stale to the leader — its `nextIndex` gets walked back via the fast-backtrack conflict hints in `AppendEntries` replies, and it's replayed the entries it missed over ordinary heartbeats. No special "recovery" path; it's the same mechanism that handles a normal slow follower.

## Testing

```bash
npm test
```

Runs five scenarios end-to-end against the in-memory transport: a cluster converges on one leader; a committed write reaches every node; a follower correctly rejects a write and reports the real leader; killing the leader (a partitioned, stopped node) triggers a new election in a higher term and the cluster keeps accepting writes; and a node that was offline during a write catches back up once it rejoins.

## What this deliberately doesn't do

No disk persistence (state is in-memory, so a real process restart loses that node's log — a genuine implementation would fsync the log and persistent state before replying to RPCs), no log compaction/snapshotting, no cluster membership changes, and reads aren't linearizable (they're served by whichever node answers first, not gated on a leader lease or ReadIndex round). Each of those is a well-known, well-scoped extension to the base algorithm implemented here — the goal of this project is a correct, readable, tested implementation of the core protocol, not a production datastore.

## Stack

TypeScript · Node.js (`node:http`, `node:child_process`, `node:test`) · zero runtime dependencies

## License

[MIT](LICENSE)
