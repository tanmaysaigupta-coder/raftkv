import type {
  AppendEntriesArgs,
  AppendEntriesReply,
  Command,
  LogEntry,
  NodeId,
  RaftRole,
  RaftStatus,
  RequestVoteArgs,
  RequestVoteReply,
  Transport,
} from './types.js';

export interface RaftNodeOptions {
  id: NodeId;
  peers: NodeId[];
  transport: Transport;
  onApply: (command: Command, index: number) => void;
  /** ms; a random value in [electionTimeoutMs, 2*electionTimeoutMs) is chosen on every reset (§5.2). */
  electionTimeoutMs?: number;
  heartbeatIntervalMs?: number;
  onEvent?: (message: string) => void;
}

const SENTINEL: LogEntry = { term: 0, index: 0, command: { type: 'delete', key: '' } };

/**
 * A single Raft node: leader election + log replication over a pluggable
 * Transport. Implements the algorithm from Figure 2 of the Raft paper
 * (Ongaro & Ousterhout, 2014) directly — no consensus library.
 */
export class RaftNode {
  readonly id: NodeId;
  private readonly peers: NodeId[];
  private readonly transport: Transport;
  private readonly onApply: (command: Command, index: number) => void;
  private readonly onEvent: (message: string) => void;
  private readonly electionTimeoutMs: number;
  private readonly heartbeatIntervalMs: number;

  // --- Persistent state (would survive a restart if written to disk; kept in memory here for the demo) ---
  private currentTerm = 0;
  private votedFor: NodeId | null = null;
  private log: LogEntry[] = [];

  // --- Volatile state on all servers ---
  private commitIndex = 0;
  private lastApplied = 0;
  private role: RaftRole = 'follower';
  private leaderId: NodeId | null = null;

  // --- Volatile state on leaders only (reinitialized after every election) ---
  private nextIndex = new Map<NodeId, number>();
  private matchIndex = new Map<NodeId, number>();

  private electionTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private stopped = false;

  constructor(opts: RaftNodeOptions) {
    this.id = opts.id;
    this.peers = opts.peers;
    this.transport = opts.transport;
    this.onApply = opts.onApply;
    this.electionTimeoutMs = opts.electionTimeoutMs ?? 1000;
    this.heartbeatIntervalMs = opts.heartbeatIntervalMs ?? 250;
    this.onEvent = opts.onEvent ?? (() => {});
  }

  start() {
    this.stopped = false;
    this.becomeFollower(this.currentTerm);
  }

  stop() {
    this.stopped = true;
    this.role = 'down';
    if (this.electionTimer) clearTimeout(this.electionTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
  }

  getStatus(): RaftStatus {
    return {
      id: this.id,
      role: this.role,
      term: this.currentTerm,
      leaderId: this.leaderId,
      logLength: this.log.length,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      votedFor: this.votedFor,
    };
  }

  // ---------------------------------------------------------------------
  // Log helpers (1-indexed to match the paper; index 0 is a term-0 sentinel)
  // ---------------------------------------------------------------------

  private getEntry(index: number): LogEntry {
    if (index === 0) return SENTINEL;
    const entry = this.log[index - 1];
    if (!entry) throw new Error(`log index ${index} out of range (len=${this.log.length})`);
    return entry;
  }

  private lastLogIndex(): number {
    return this.log.length;
  }

  private lastLogTerm(): number {
    return this.log.length > 0 ? this.log[this.log.length - 1]!.term : 0;
  }

  // ---------------------------------------------------------------------
  // Role transitions
  // ---------------------------------------------------------------------

  private resetElectionTimer() {
    if (this.electionTimer) clearTimeout(this.electionTimer);
    if (this.stopped) return;
    const timeout = this.electionTimeoutMs + Math.random() * this.electionTimeoutMs;
    this.electionTimer = setTimeout(() => this.startElection(), timeout);
  }

  private becomeFollower(term: number, leaderId: NodeId | null = null) {
    const wasLeader = this.role === 'leader';
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
    }
    this.role = 'follower';
    if (leaderId) this.leaderId = leaderId;
    if (wasLeader && this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    this.resetElectionTimer();
  }

  private async startElection() {
    if (this.stopped) return;
    this.role = 'candidate';
    this.currentTerm += 1;
    this.votedFor = this.id;
    this.leaderId = null;
    this.resetElectionTimer();
    this.onEvent(`term ${this.currentTerm}: starting election`);

    const electionTerm = this.currentTerm;
    let votes = 1; // vote for self

    await Promise.all(
      this.peers.map(async (peer) => {
        try {
          const reply = await this.transport.requestVote(peer, {
            term: electionTerm,
            candidateId: this.id,
            lastLogIndex: this.lastLogIndex(),
            lastLogTerm: this.lastLogTerm(),
          });
          if (this.stopped || this.currentTerm !== electionTerm || this.role !== 'candidate') return;
          if (reply.term > this.currentTerm) {
            this.becomeFollower(reply.term);
            return;
          }
          if (reply.voteGranted) {
            votes += 1;
            if (this.isMajority(votes) && this.role === 'candidate') {
              this.becomeLeader();
            }
          }
        } catch {
          // peer unreachable (down, or partitioned) — simply doesn't count toward this round
        }
      }),
    );
  }

  private becomeLeader() {
    this.role = 'leader';
    this.leaderId = this.id;
    if (this.electionTimer) clearTimeout(this.electionTimer);
    this.onEvent(`term ${this.currentTerm}: became leader`);

    for (const peer of this.peers) {
      this.nextIndex.set(peer, this.lastLogIndex() + 1);
      this.matchIndex.set(peer, 0);
    }

    this.sendHeartbeats();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => this.sendHeartbeats(), this.heartbeatIntervalMs);
  }

  private isMajority(count: number): boolean {
    return count * 2 > this.peers.length + 1;
  }

  // ---------------------------------------------------------------------
  // Replication (leader side)
  // ---------------------------------------------------------------------

  private sendHeartbeats() {
    if (this.role !== 'leader' || this.stopped) return;
    for (const peer of this.peers) this.replicateTo(peer);
  }

  private async replicateTo(peer: NodeId) {
    const term = this.currentTerm;
    const nextIdx = this.nextIndex.get(peer) ?? this.lastLogIndex() + 1;
    const prevLogIndex = nextIdx - 1;
    const prevLogTerm = this.getEntry(prevLogIndex).term;
    const entries = this.log.slice(nextIdx - 1);

    try {
      const reply = await this.transport.appendEntries(peer, {
        term,
        leaderId: this.id,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit: this.commitIndex,
      });

      if (this.stopped || this.role !== 'leader' || this.currentTerm !== term) return;

      if (reply.term > this.currentTerm) {
        this.becomeFollower(reply.term);
        return;
      }

      if (reply.success) {
        const newMatch = prevLogIndex + entries.length;
        this.matchIndex.set(peer, newMatch);
        this.nextIndex.set(peer, newMatch + 1);
        this.advanceCommitIndex();
      } else {
        // Fast-backtrack using the follower's conflict hint instead of decrementing by one (§5.3).
        if (reply.conflictTerm !== undefined) {
          let idx = this.lastLogIndex();
          while (idx > 0 && this.getEntry(idx).term > reply.conflictTerm) idx--;
          if (idx > 0 && this.getEntry(idx).term === reply.conflictTerm) {
            this.nextIndex.set(peer, idx + 1);
          } else {
            this.nextIndex.set(peer, reply.conflictIndex ?? 1);
          }
        } else {
          this.nextIndex.set(peer, reply.conflictIndex ?? Math.max(1, nextIdx - 1));
        }
      }
    } catch {
      // peer unreachable this round — the next heartbeat tick will retry
    }
  }

  private advanceCommitIndex() {
    for (let n = this.lastLogIndex(); n > this.commitIndex; n--) {
      if (this.getEntry(n).term !== this.currentTerm) continue;
      let count = 1; // self
      for (const peer of this.peers) {
        if ((this.matchIndex.get(peer) ?? 0) >= n) count++;
      }
      if (this.isMajority(count)) {
        this.commitIndex = n;
        this.applyCommitted();
        break;
      }
    }
  }

  private applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
      const entry = this.getEntry(this.lastApplied);
      this.onApply(entry.command, entry.index);
    }
  }

  // ---------------------------------------------------------------------
  // RPC handlers (follower / candidate side) — called by the HTTP layer
  // ---------------------------------------------------------------------

  handleRequestVote(args: RequestVoteArgs): RequestVoteReply {
    if (args.term > this.currentTerm) this.becomeFollower(args.term);

    if (args.term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }

    const upToDate =
      args.lastLogTerm > this.lastLogTerm() ||
      (args.lastLogTerm === this.lastLogTerm() && args.lastLogIndex >= this.lastLogIndex());

    const canVote = this.votedFor === null || this.votedFor === args.candidateId;

    if (canVote && upToDate) {
      this.votedFor = args.candidateId;
      this.resetElectionTimer();
      this.onEvent(`term ${this.currentTerm}: voted for ${args.candidateId}`);
      return { term: this.currentTerm, voteGranted: true };
    }

    return { term: this.currentTerm, voteGranted: false };
  }

  handleAppendEntries(args: AppendEntriesArgs): AppendEntriesReply {
    if (args.term > this.currentTerm) this.becomeFollower(args.term, args.leaderId);

    if (args.term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    // Valid leader for our term — stay/return to follower and reset the election clock.
    this.role = 'follower';
    this.leaderId = args.leaderId;
    this.resetElectionTimer();

    if (args.prevLogIndex > this.lastLogIndex()) {
      return { term: this.currentTerm, success: false, conflictIndex: this.lastLogIndex() + 1 };
    }
    if (args.prevLogIndex > 0 && this.getEntry(args.prevLogIndex).term !== args.prevLogTerm) {
      const conflictTerm = this.getEntry(args.prevLogIndex).term;
      let conflictIndex = args.prevLogIndex;
      while (conflictIndex > 1 && this.getEntry(conflictIndex - 1).term === conflictTerm) conflictIndex--;
      return { term: this.currentTerm, success: false, conflictIndex, conflictTerm };
    }

    // Append new entries, truncating on the first conflict (§5.3).
    let insertAt = args.prevLogIndex;
    for (const entry of args.entries) {
      insertAt++;
      const existing = insertAt <= this.lastLogIndex() ? this.getEntry(insertAt) : undefined;
      if (!existing || existing.term !== entry.term) {
        this.log = this.log.slice(0, insertAt - 1);
        this.log.push(entry);
      }
    }

    if (args.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(args.leaderCommit, this.lastLogIndex());
      this.applyCommitted();
    }

    return { term: this.currentTerm, success: true };
  }

  // ---------------------------------------------------------------------
  // Client-facing API
  // ---------------------------------------------------------------------

  /** Appends `command` to the log (leader only) and resolves once it's committed & applied. */
  async propose(command: Command, timeoutMs = 5000): Promise<{ ok: true; index: number } | { ok: false; leaderId: NodeId | null }> {
    if (this.role !== 'leader') {
      return { ok: false, leaderId: this.leaderId };
    }

    const index = this.lastLogIndex() + 1;
    this.log.push({ term: this.currentTerm, index, command });
    this.matchIndex.set(this.id, index);
    for (const peer of this.peers) this.replicateTo(peer);

    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
      if (this.commitIndex >= index) return { ok: true, index };
      if (this.role !== 'leader') return { ok: false, leaderId: this.leaderId };
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error('propose timed out waiting for commit');
  }

  isLeader(): boolean {
    return this.role === 'leader';
  }
}
