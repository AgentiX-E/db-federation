/**
 * OutboxStore — Durable LMDB-backed outbox for cross-DC replication.
 *
 * Design principles (from sqlvec-db OutboxManager, stripped to essentials):
 *   1. Entries are written BEFORE returning to caller (crash-safe, WAL consistency)
 *   2. O(1) ACK/delivery tracking via msgId → index in separate databases
 *   3. At-least-once delivery: retry with dedup on the receiving side
 *   4. Dead letter queue: entries exceeding maxRetries (default: 3)
 *   5. Backpressure detection via LMDB map utilization (>80% triggers warning)
 *
 * Differences from sqlvec-db OutboxManager (more complex, per-target DC indices):
 *   - Federation OutboxStore is simpler: flat pending queue, no per-DC index
 *   - Multi-DC routing is handled at the MultiLeaderReplication layer, not in the store
 *   - No per-peer outbox work partitioning — the store is a shared durable queue
 */

import type { IOutboxStore } from "../interfaces/outbox/IOutboxStore";
import type { OutboxEntry, OutboxStatus } from "../interfaces/outbox/OutboxEntry";
import type { OutboxStats } from "../interfaces/types";
import type { IHLCTimestamp } from "../interfaces/hlc/IHLCTimestamp";
import { open, type RootDatabase } from "lmdb";
import * as path from "path";
import * as fs from "fs";
import { randomUUID } from "crypto";

export interface OutboxStoreConfig {
  /** Path to the LMDB data directory */
  dataDir: string;
  /** Entry TTL (ms) before auto-housekeeping — default 14 days */
  ttlMs?: number;
  /** Max retries before moving to dead letter queue — default 3 */
  maxRetries?: number;
  /** Dead letter TTL (ms) before auto-cleanup — default 7 days */
  deadLetterTtlMs?: number;
  /** Main LMDB map size — default 1GB */
  mapSize?: number;
}

export class OutboxStore implements IOutboxStore {
  /** Main entry storage: seq → OutboxEntry */
  private mainDb: RootDatabase;
  /** O(1) delivery index: delivered:{id} → timestamp */
  private deliveryDb: RootDatabase;
  /** Sequence counter database */
  private seqDb: RootDatabase;
  /** Dead letter queue: dlq:{id} → OutboxEntry */
  private deadLetterDb: RootDatabase;
  /** Pending index for O(1) sorted fetch: pending:{id} → seq */
  private pendingDb: RootDatabase;
  /** msgId → seq reverse index for O(1) ACK lookup */
  private msgIdToSeqDb: RootDatabase;

  private readonly ttlMs: number;
  private readonly maxRetries: number;
  private readonly deadLetterTtlMs: number;
  private readonly mapSize: number;

  constructor(config: OutboxStoreConfig) {
    this.ttlMs = config.ttlMs ?? 14 * 24 * 60 * 60 * 1000;
    this.maxRetries = config.maxRetries ?? 3;
    this.deadLetterTtlMs = config.deadLetterTtlMs ?? 7 * 24 * 60 * 60 * 1000;
    this.mapSize = config.mapSize ?? 1024 * 1024 * 1024;

    fs.mkdirSync(config.dataDir, { recursive: true });

    this.mainDb = open({ path: path.join(config.dataDir, "outbox-main.lmdb"), mapSize: this.mapSize });
    this.deliveryDb = open({ path: path.join(config.dataDir, "outbox-delivery.lmdb"), mapSize: 64 * 1024 * 1024 });
    this.seqDb = open({ path: path.join(config.dataDir, "outbox-seq.lmdb"), mapSize: 64 * 1024 * 1024 });
    this.deadLetterDb = open({ path: path.join(config.dataDir, "outbox-dead-letter.lmdb"), mapSize: 64 * 1024 * 1024 });
    this.pendingDb = open({ path: path.join(config.dataDir, "outbox-pending.lmdb"), mapSize: 256 * 1024 * 1024 });
    this.msgIdToSeqDb = open({ path: path.join(config.dataDir, "outbox-msgid-seq.lmdb"), mapSize: 64 * 1024 * 1024 });
  }

  /**
   * Add a new outbox entry.
   * The entry is persisted to LMDB before the promise resolves (crash-safe).
   * Returns the created entry with an assigned ID and pending status.
   */
  async add(entry: Omit<OutboxEntry, "id">): Promise<OutboxEntry> {
    const seq = this.nextSeq();
    const id = `entry:${seq}`;
    const full: OutboxEntry = {
      ...entry,
      id,
      status: "pending",
      createdAt: entry.createdAt ?? Date.now(),
      lastAttemptAt: null,
      attemptCount: 0,
      lastError: null,
    };

    // Durable write: persists to LMDB before returning (AC 40.1)
    this.mainDb.putSync(id, full);
    this.pendingDb.putSync(`pending:${id}`, seq);
    this.msgIdToSeqDb.putSync(full.id, seq);

    return full;
  }

  /**
   * Fetch the oldest pending entries.
   * Sorts by entry ID (lexicographic seq order) to maintain FIFO ordering.
   */
  async fetchOldestPending(count: number): Promise<OutboxEntry[]> {
    // Determine the range of seq values to scan.
    // We iterate through the sorted pending index for correct ordering.
    const entries: OutboxEntry[] = [];
    const range = this.pendingDb.getRange({ start: "pending:", end: "pending:\xff" });

    for (const item of range) {
      if (entries.length >= count) break;
      const seq = item.value as number;
      const entryKey = `entry:${seq}`;
      const entry = this.mainDb.get(entryKey) as OutboxEntry | undefined;
      if (entry && entry.status === "pending") {
        entries.push(entry);
      }
    }

    return entries;
  }

  /**
   * Mark an outbox entry as delivered (O(1) via deliveryDb index).
   * Also updates the main entry for backward compatibility.
   */
  async markDelivered(entryId: string): Promise<void> {
    // Write delivery index (O(1))
    this.deliveryDb.putSync(`delivered:${entryId}`, Date.now());

    // Look up seq from msgIdToSeqDb for O(1) main entry access
    const seq = this.msgIdToSeqDb.get(entryId) as number | undefined;
    if (seq !== undefined) {
      const entryKey = `entry:${seq}`;
      const existing = this.mainDb.get(entryKey) as OutboxEntry | undefined;
      if (existing) {
        existing.status = "delivered";
        existing.lastAttemptAt = Date.now();
        this.mainDb.putSync(entryKey, existing);
        // Remove from pending index
        this.pendingDb.removeSync(`pending:${entryKey}`);
        return;
      }
    }

    // Fallback: O(n) scan for entries without index
    for (const item of this.mainDb.getRange({ start: "entry:", end: "entry:\xff" })) {
      const entry = item.value as OutboxEntry;
      if (entry?.id === entryId) {
        entry.status = "delivered";
        entry.lastAttemptAt = Date.now();
        this.mainDb.putSync(item.key as string, entry);
        this.pendingDb.removeSync(`pending:${item.key as string}`);
        return;
      }
    }
  }

  /**
   * Check if an entry has been delivered (O(1) via deliveryDb index).
   */
  isDelivered(entryId: string): boolean {
    return this.deliveryDb.get(`delivered:${entryId}`) !== undefined &&
           this.deliveryDb.get(`delivered:${entryId}`) !== null;
  }

  /**
   * Get the count of currently pending entries.
   */
  async getPendingCount(): Promise<number> {
    let count = 0;
    for (const _ of this.pendingDb.getRange({ start: "pending:", end: "pending:\xff" })) {
      count++;
    }
    return count;
  }

  /**
   * Get outbox store statistics.
   */
  async getStats(): Promise<OutboxStats> {
    const pendingCount = await this.getPendingCount();
    let deliveredCount = 0;
    for (const _ of this.deliveryDb.getRange({ start: "delivered:", end: "delivered:\xff" })) {
      deliveredCount++;
    }
    const totalEntries = pendingCount + deliveredCount;

    // Find oldest pending timestamp
    let oldestPendingTimestamp: number | null = null;
    for (const item of this.pendingDb.getRange({ start: "pending:", end: "pending:\xff", limit: 1 })) {
      const seq = item.value as number;
      const entry = this.mainDb.get(`entry:${seq}`) as OutboxEntry | undefined;
      if (entry) {
        oldestPendingTimestamp = entry.createdAt;
      }
    }

    return {
      totalEntries,
      pendingCount,
      deliveredCount,
      oldestPendingTimestamp,
    };
  }

  // ──────────────────────────────────────────
  //  Retry & Dead Letter (AC 40.4, 40.5)
  // ──────────────────────────────────────────

  /**
   * Mark an entry delivery attempt as failed.
   * Increments retry count. If maxRetries exceeded, moves to dead letter queue.
   * Returns true if the entry was moved to dead letter, false otherwise.
   */
  markFailed(entryId: string, error?: string): boolean {
    const seq = this.msgIdToSeqDb.get(entryId) as number | undefined;
    if (seq === undefined) {
      // Fallback scan
      for (const item of this.mainDb.getRange({ start: "entry:", end: "entry:\xff" })) {
        const entry = item.value as OutboxEntry;
        if (entry?.id === entryId) {
          return this._markFailed(entry, item.key as string, error);
        }
      }
      return false;
    }

    const entryKey = `entry:${seq}`;
    const entry = this.mainDb.get(entryKey) as OutboxEntry | undefined;
    if (!entry) return false;

    return this._markFailed(entry, entryKey, error);
  }

  private _markFailed(entry: OutboxEntry, entryKey: string, error?: string): boolean {
    entry.attemptCount = (entry.attemptCount ?? 0) + 1;
    entry.lastAttemptAt = Date.now();
    entry.lastError = error ?? null;

    if (entry.attemptCount >= this.maxRetries) {
      // Move to dead letter queue
      this.deadLetterDb.putSync(`dlq:${entry.id}`, {
        ...entry,
        status: "failed" as OutboxStatus,
        deadAt: Date.now(),
      });
      // Remove from main + pending
      this.mainDb.removeSync(entryKey);
      this.pendingDb.removeSync(`pending:${entryKey}`);
      return true; // moved to dead letter
    }

    // Update in place
    this.mainDb.putSync(entryKey, entry);
    return false; // still pending
  }

  /**
   * Retry a dead letter entry by re-inserting it into the main queue.
   */
  retryDeadLetter(entryId: string): boolean {
    const deadKey = `dlq:${entryId}`;
    const deadEntry = this.deadLetterDb.get(deadKey) as (OutboxEntry & { deadAt?: number }) | undefined;
    if (!deadEntry) return false;

    // Re-insert with fresh seq and reset counters
    const seq = this.nextSeq();
    const newKey = `entry:${seq}`;
    const revived: OutboxEntry = {
      ...deadEntry,
      id: newKey,
      status: "pending",
      attemptCount: 0,
      lastAttemptAt: null,
      lastError: null,
      createdAt: Date.now(),
    };

    this.mainDb.putSync(newKey, revived);
    this.pendingDb.putSync(`pending:${newKey}`, seq);
    this.msgIdToSeqDb.putSync(revived.id, seq);

    // Remove from dead letter
    this.deadLetterDb.removeSync(deadKey);
    return true;
  }

  /**
   * Get all dead letter entries.
   */
  getDeadLetterEntries(): Array<OutboxEntry & { deadAt?: number }> {
    const entries: Array<OutboxEntry & { deadAt?: number }> = [];
    for (const item of this.deadLetterDb.getRange({ start: "dlq:", end: "dlq:\xff" })) {
      entries.push(item.value as OutboxEntry & { deadAt?: number });
    }
    return entries;
  }

  /**
   * Get dead letter stats.
   */
  getDeadLetterStats(): { count: number } {
    const entries = this.getDeadLetterEntries();
    return { count: entries.length };
  }

  // ──────────────────────────────────────────
  //  Backpressure (AC 41.4)
  // ──────────────────────────────────────────

  /**
   * Check if LMDB map utilization exceeds 80%.
   */
  isCapacityExceeded(): boolean {
    try {
      const stats = this.mainDb.getStats() as any;
      const pageSize = stats.pageSize || 4096;
      const usedBytes = stats.lastPageNumber ? stats.lastPageNumber * pageSize : 0;
      const mapSize = stats.mapSize || this.mapSize;
      return usedBytes / mapSize > 0.8;
    } catch {
      return false;
    }
  }

  // ──────────────────────────────────────────
  //  Housekeeping
  // ──────────────────────────────────────────

  /**
   * Remove entries older than ttlMs from the deliveryDb and dead letter queue.
   */
  housekeeping(): void {
    const now = Date.now();
    const cutoff = now - this.ttlMs;

    // Clean up delivered entries
    for (const item of this.deliveryDb.getRange({ start: "delivered:", end: "delivered:\xff" })) {
      const ts = item.value as number;
      if (ts < cutoff) {
        const entryId = (item.key as string).slice("delivered:".length);
        this.deliveryDb.removeSync(item.key as string);
        // Clean up reverse index
        this.msgIdToSeqDb.removeSync(entryId);
      }
    }

    // Clean up dead letter entries
    const dlCutoff = now - this.deadLetterTtlMs;
    for (const item of this.deadLetterDb.getRange({ start: "dlq:", end: "dlq:\xff" })) {
      const entry = item.value as OutboxEntry & { deadAt?: number };
      if (entry.deadAt && entry.deadAt < dlCutoff) {
        this.deadLetterDb.removeSync(item.key as string);
      }
    }
  }

  /**
   * Close all LMDB databases.
   */
  close(): void {
    this.mainDb.close();
    this.deliveryDb.close();
    this.seqDb.close();
    this.deadLetterDb.close();
    this.pendingDb.close();
    this.msgIdToSeqDb.close();
  }

  // ──────────────────────────────────────────
  //  Internal helpers
  // ──────────────────────────────────────────

  private nextSeq(): number {
    const current = (this.seqDb.get("seq") as number) || 0;
    const next = current + 1;
    this.seqDb.putSync("seq", next);
    return next;
  }
}
