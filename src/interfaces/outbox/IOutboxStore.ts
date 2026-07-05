import type { OutboxEntry } from "./OutboxEntry";
import type { OutboxStats } from "../types";

export interface IOutboxStore {
  /**
   * Add a new outbox entry.
   * Returns the created entry with its assigned ID.
   */
  add(entry: Omit<OutboxEntry, "id">): Promise<OutboxEntry>;

  /**
   * Fetch the oldest pending entries (for delivery attempt).
   * Limited by count parameter.
   */
  fetchOldestPending(count: number): Promise<OutboxEntry[]>;

  /**
   * Mark an outbox entry as delivered.
   */
  markDelivered(entryId: string): Promise<void>;

  /**
   * Get the count of currently pending entries.
   */
  getPendingCount(): Promise<number>;

  /**
   * Get outbox store statistics.
   */
  getStats(): OutboxStats;
}
