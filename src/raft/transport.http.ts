import type {
  AppendEntriesArgs,
  AppendEntriesReply,
  NodeId,
  RequestVoteArgs,
  RequestVoteReply,
  Transport,
} from './types.js';

/** HTTP+JSON transport: each Raft peer is addressed as `http://host:port`. */
export class HttpTransport implements Transport {
  constructor(private addresses: Map<NodeId, string>, private timeoutMs = 300) {}

  private async post<T>(peer: NodeId, path: string, body: unknown): Promise<T> {
    const base = this.addresses.get(peer);
    if (!base) throw new Error(`no address for peer ${peer}`);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const res = await fetch(`${base}${path}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (!res.ok) throw new Error(`peer ${peer} responded ${res.status}`);
      return (await res.json()) as T;
    } finally {
      clearTimeout(timer);
    }
  }

  requestVote(peer: NodeId, args: RequestVoteArgs): Promise<RequestVoteReply> {
    return this.post(peer, '/raft/request-vote', args);
  }

  appendEntries(peer: NodeId, args: AppendEntriesArgs): Promise<AppendEntriesReply> {
    return this.post(peer, '/raft/append-entries', args);
  }
}
