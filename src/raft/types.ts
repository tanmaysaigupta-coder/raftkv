/**
 * Wire types for the Raft consensus protocol, following the naming in the
 * original paper (Ongaro & Ousterhout, "In Search of an Understandable
 * Consensus Algorithm", 2014) so the implementation in node.ts can be read
 * side by side with Figure 2 of that paper.
 */

export type NodeId = string;

export type Command =
  | { type: 'set'; key: string; value: string }
  | { type: 'delete'; key: string };

export interface LogEntry {
  term: number;
  index: number;
  command: Command;
}

export type RaftRole = 'follower' | 'candidate' | 'leader' | 'down';

export interface RequestVoteArgs {
  term: number;
  candidateId: NodeId;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteReply {
  term: number;
  voteGranted: boolean;
}

export interface AppendEntriesArgs {
  term: number;
  leaderId: NodeId;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesReply {
  term: number;
  success: boolean;
  /** Fast-backtrack hints (§5.3 optimization) so a lagging follower catches up in one round trip instead of one entry at a time. */
  conflictIndex?: number;
  conflictTerm?: number;
}

/** Pluggable RPC transport so the same RaftNode can run over real HTTP (multi-process demo) or in-memory (deterministic unit tests). */
export interface Transport {
  requestVote(peer: NodeId, args: RequestVoteArgs): Promise<RequestVoteReply>;
  appendEntries(peer: NodeId, args: AppendEntriesArgs): Promise<AppendEntriesReply>;
}

export interface RaftStatus {
  id: NodeId;
  role: RaftRole;
  term: number;
  leaderId: NodeId | null;
  logLength: number;
  commitIndex: number;
  lastApplied: number;
  votedFor: NodeId | null;
}
