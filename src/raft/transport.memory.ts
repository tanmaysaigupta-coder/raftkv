import type {
  AppendEntriesArgs,
  AppendEntriesReply,
  NodeId,
  RequestVoteArgs,
  RequestVoteReply,
  Transport,
} from './types.js';
import type { RaftNode } from './node.js';

/**
 * An in-process transport that calls peer RPC handlers directly (with a
 * small simulated network delay). Lets the Raft test suite exercise real
 * elections and log replication deterministically, without spinning up
 * HTTP servers or racing real wall-clock timers across processes.
 */
export class InMemoryTransport implements Transport {
  private nodes = new Map<NodeId, RaftNode>();
  private partitioned = new Set<NodeId>();
  private latencyMs: number;

  constructor(latencyMs = 2) {
    this.latencyMs = latencyMs;
  }

  register(node: RaftNode) {
    this.nodes.set(node.id, node);
  }

  /** Simulate a network partition: RPCs to/from this node start failing. */
  partition(id: NodeId) {
    this.partitioned.add(id);
  }

  heal(id: NodeId) {
    this.partitioned.delete(id);
  }

  private async delay() {
    if (this.latencyMs > 0) await new Promise((r) => setTimeout(r, this.latencyMs));
  }

  async requestVote(peer: NodeId, args: RequestVoteArgs): Promise<RequestVoteReply> {
    if (this.partitioned.has(peer)) throw new Error('unreachable');
    await this.delay();
    const node = this.nodes.get(peer);
    if (!node) throw new Error(`unknown peer ${peer}`);
    return node.handleRequestVote(args);
  }

  async appendEntries(peer: NodeId, args: AppendEntriesArgs): Promise<AppendEntriesReply> {
    if (this.partitioned.has(peer)) throw new Error('unreachable');
    await this.delay();
    const node = this.nodes.get(peer);
    if (!node) throw new Error(`unknown peer ${peer}`);
    return node.handleAppendEntries(args);
  }
}
