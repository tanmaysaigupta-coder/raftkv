import type { Command } from '../raft/types.js';

/** The replicated state machine: every Raft node applies the same committed commands in the same order and ends up with the same map. */
export class KVStore {
  private data = new Map<string, string>();
  private appliedIndex = 0;

  apply(command: Command, index: number) {
    if (command.type === 'set') this.data.set(command.key, command.value);
    else this.data.delete(command.key);
    this.appliedIndex = index;
  }

  get(key: string): string | undefined {
    return this.data.get(key);
  }

  entries(): [string, string][] {
    return [...this.data.entries()];
  }

  get size(): number {
    return this.data.size;
  }

  get lastAppliedIndex(): number {
    return this.appliedIndex;
  }
}
