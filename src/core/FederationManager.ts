/**
 * FederationManager — Top-level orchestrator for cross-DC replication.
 *
 * Owns the full lifecycle:
 *   1. MultiLeaderReplication — cross-DC write replication
 *   2. CDCManager — change data capture for event streaming (MA1-MA5)
 *   3. DCSplitBrainDetector — detects and recovers from DC split-brain (MA6-MA9)
 *   4. ConflictResolver — resolves write conflicts on the replication hot path
 *   5. AntiEntropy — Merkle tree reconciliation for eventual consistency
 *   6. HealthMonitor — periodic health checks + lag tracking
 *
 * The replicate() method is now the write hot path:
 *   conflict resolution → outbox → replication → CDC event
 *
 * Consumers (sqlvec-db, graphvec-db) register via registerConsumer() to:
 *   - Receive replicated entries for local Raft proposal
 *   - Provide HLC for timestamp attachment
 *   - Receive conflict resolution notifications
 */

import type { IMultiLeaderReplication, ReplicationConfig } from "../interfaces/replication/IMultiLeaderReplication";
import type { ICDCManager } from "../interfaces/replication/ICDCManager";
import type { IAntiEntropy } from "../interfaces/replication/IAntiEntropy";
import type { ITransport } from "../interfaces/transport/ITransport";
import type { IOutboxStore } from "../interfaces/outbox/IOutboxStore";
import type { IHLCTimestamp } from "../interfaces/hlc/IHLCTimestamp";
import { HLC } from "../hlc/HLC";
import { HealthMonitor, type FederationHealth, type HealthMonitorConfig } from "../monitoring/HealthMonitor";
import { CDCManager, type CDCTarget, type CDCEvent } from "./CDCManager";
import { DCSplitBrainDetector, type SplitBrainEvent } from "./DCSplitBrainDetector";
import { ConflictResolver, type ConflictValue, type ConflictStrategy } from "../replication/ConflictResolver";
import { AntiEntropy } from "../replication/AntiEntropy";
import type { DataEntry, DataProvider } from "../replication/AntiEntropy";
import { EventEmitter } from "events";

// ── Interfaces (defined here for Phase 2, will move to separate files) ──

export interface ReplicatedEntry {
  /** The raw data payload */
  data: string;
  /** Message type */
  type: string;
  /** Source DC */
  sourceDc: string;
  /** Source node */
  sourceNodeId: string;
  /** HLC timestamp for causal ordering */
  hlcTimestamp: IHLCTimestamp;
}

export interface ConflictEntry {
  /** The key that had a conflict */
  key: string;
  /** Local value */
  localValue: string;
  /** Remote value */
  remoteValue: string;
  /** Local HLC timestamp */
  localHlc: IHLCTimestamp;
  /** Remote HLC timestamp */
  remoteHlc: IHLCTimestamp;
  /** Source DC */
  sourceDc: string;
}

export interface IFederationConsumer {
  /** Propose a replicated entry back into the local Raft cluster */
  proposeReplicated(entry: ReplicatedEntry): Promise<void>;
  /** Get the local HLC instance for timestamp attachment */
  getHLC(): HLC;
  /** Notify of a conflict that needs resolution */
  onConflict(conflict: ConflictEntry): Promise<void>;
}

/** @internal */
export const NOT_IMPLEMENTED_YET = "Not implemented in Phase 2";

// ── FederationManager ──────────────────────────

export interface FederationManagerConfig {
  /** Node ID */
  nodeId: string;
  /** Data center / region */
  dc: string;
  /** Replication config (targets, topology) */
  replicationConfig: ReplicationConfig;
  /** Transport layer (must be started and connected) */
  transport: ITransport;
  /** Outbox store for crash-safe persistence */
  outbox: IOutboxStore;
  /** HLC instance (optional — created if not provided) */
  hlc?: HLC;
  /** Health monitor config (optional) */
  healthConfig?: HealthMonitorConfig;
}

export class FederationManager {
  private readonly config: FederationManagerConfig;
  private readonly hlc: HLC;
  private readonly healthMonitor: HealthMonitor;
  private readonly emitter = new EventEmitter();
  private consumers: IFederationConsumer[] = [];
  private _started = false;

  // Multi-Active production components
  private _cdcManager: CDCManager | null = null;
  private _splitBrainDetector: DCSplitBrainDetector | null = null;
  private _conflictResolver: ConflictResolver | null = null;
  private _antiEntropy: AntiEntropy | null = null;

  // Local key-value store for AntiEntropy (MA10-MA12)
  private readonly localStore = new Map<string, { key: string; value: string; hlc: { wallTime: number; logical: number; nodeId: string }; nodeId: string }>();

  constructor(
    private readonly replication: IMultiLeaderReplication,
    config: FederationManagerConfig,
  ) {
    this.config = config;
    this.hlc = config.hlc ?? new HLC(config.nodeId);
    this.healthMonitor = new HealthMonitor(replication, config.healthConfig);
  }

  /**
   * Start the federation manager.
   * Initializes all sub-systems:
   *   1. MultiLeaderReplication (outbox workers per target)
   *   2. CDCManager (change data capture)
   *   3. DCSplitBrainDetector (heartbeat-based split-brain detection)
   *   4. ConflictResolver (LWW + CRDT strategies)
   *   5. AntiEntropy (Merkle tree reconciliation)
   *   6. HealthMonitor (periodic health checks)
   */
  async start(cdcTargets?: CDCTarget[]): Promise<void> {
    if (this._started) return;

    // 1. Start replication engine
    await this.replication.start(this.config.replicationConfig);

    // 2. Start CDC Manager
    this._cdcManager = new CDCManager({
      nodeId: this.config.nodeId,
      hlc: this.hlc,
    });
    await this._cdcManager.start(cdcTargets ?? []);

    // 3. Start Split-Brain Detector
    this._splitBrainDetector = new DCSplitBrainDetector({
      nodeId: this.config.nodeId,
      dcId: this.config.dc,
      hlc: this.hlc,
    });
    this._splitBrainDetector.start();

    // 4. Initialize ConflictResolver
    this._conflictResolver = new ConflictResolver(this.config.nodeId);

    // 5. Start AntiEntropy with data provider
    this._antiEntropy = new AntiEntropy(
      { nodeId: this.config.nodeId, merkleDepth: 4, intervalMs: 5000 },
      this.buildDataProvider(this.localStore, this.config.nodeId),
      this._conflictResolver,
    );

    // 6. Start HealthMonitor
    this.healthMonitor.start();

    // Listen for split-brain events
    this._splitBrainDetector.onSplitBrainDetected((event: SplitBrainEvent) => {
      this.emitter.emit("split-brain-detected", event);
    });

    this._started = true;
  }

  /**
   * Build the AntiEntropy DataProvider backed by the given local store.
   *
   * Extracted from `start()` so the four data-access closures are standalone
   * and independently testable units. The provider is intentionally
   * self-contained: it reads from and writes to a single `Map` keyed by entry
   * key. All functions are pure with respect to the store.
   */
  private buildDataProvider(
    localStore: Map<string, DataEntry>,
    nodeId: string,
  ): DataProvider {
    return {
      async getRange(start: string, end: string): Promise<DataEntry[]> {
        const entries: DataEntry[] = [];
        for (const [, e] of localStore) {
          if (e.key >= start && e.key <= end) {
            entries.push(e);
          }
        }
        return entries;
      },

      async getEntriesAfter(afterKey: string, limit: number): Promise<DataEntry[]> {
        const sorted = Array.from(localStore.values()).sort((a, b) =>
          a.key.localeCompare(b.key),
        );
        const idx = sorted.findIndex((e) => e.key > afterKey);
        // findIndex returns -1 when no entry is strictly greater than
        // afterKey; slice(-1, …) would incorrectly wrap around to the last
        // element, so we must bail out explicitly.
        if (idx === -1) return [];
        return sorted.slice(idx, idx + limit);
      },

      async get(key: string): Promise<DataEntry | null> {
        return localStore.get(key) ?? null;
      },

      async apply(key: string, value: string): Promise<void> {
        localStore.set(key, {
          key,
          value,
          hlc: { wallTime: Date.now(), logical: 0, nodeId },
          nodeId,
        });
      },
    };
  }

  /**
   * Graceful shutdown.
   */
  async stop(): Promise<void> {
    this._started = false;
    this.healthMonitor.stop();
    this._splitBrainDetector?.stop();
    await this._cdcManager?.stop();
    await this.replication.stop();
  }

  /**
   * Replicate data to all targets.
   * Hot path: conflict resolution → local store → outbox → CDC event.
   *
   * @param key — The data key for conflict resolution
   * @param value — The data to replicate
   * @param shardId — Target shard (optional, uses 0 if unspecified)
   * @param type — Operation type ("insert", "update", "delete", etc.)
   */
  async replicate(
    shardIdOrKey: number | string,
    valueOrData: string,
    type: string,
    remoteValue?: ConflictValue,
  ): Promise<{ msgId: string }> {
    // Default params when called with old signature (shardId, data, type)
    const key = typeof shardIdOrKey === "string" ? shardIdOrKey : `key:${shardIdOrKey}:${Date.now()}`;
    const data = valueOrData;
    const shardId = typeof shardIdOrKey === "number" ? shardIdOrKey : 0;

    // MA10: Conflict resolution on the write hot path.
    // `HLC.now()` already returns a full `IHLCTimestamp` (compare/serialize/
    // deserialize), so there is no need to hand-roll a redundant snapshot.
    const now = this.hlc.now();

    if (remoteValue && this._conflictResolver) {
      const local: ConflictValue = {
        key,
        value: data,
        hlc: now,
        nodeId: this.config.nodeId,
      };

      await this._conflictResolver.resolve(key, local, remoteValue);
      // Notify consumers of conflict
      for (const consumer of this.consumers) {
        await consumer.onConflict({
          key,
          localValue: local.value,
          remoteValue: remoteValue.value,
          localHlc: local.hlc,
          remoteHlc: remoteValue.hlc,
          sourceDc: remoteValue.nodeId,
        });
      }
    }

    // Store locally (for AntiEntropy)
    this.localStore.set(key, {
      key,
      value: data,
      hlc: { wallTime: Number(now.wallTime), logical: now.logical, nodeId: now.nodeId },
      nodeId: this.config.nodeId,
    });

    // Record split-brain heartbeat
    this._splitBrainDetector?.recordApply();

    // Replicate via outbox
    const result = await this.replication.replicate(shardId, data, type);

    // Capture CDC event (MA1-MA5)
    if (this._cdcManager?.isStarted()) {
      await this._cdcManager.capture({
        eventId: result.msgId,
        key,
        value: data,
        type: type as "insert" | "update" | "delete",
        sourceNodeId: this.config.nodeId,
        sourceDc: this.config.dc,
        hlcTimestamp: { wallTime: Number(now.wallTime), logical: now.logical, nodeId: now.nodeId },
        appliedAt: Date.now(),
      });
    }

    return result;
  }

  /**
   * Get the replication engine.
   */
  getReplication(): IMultiLeaderReplication {
    return this.replication;
  }

  /**
   * Get the CDC manager.
   */
  getCDC(): CDCManager {
    if (!this._cdcManager) {
      throw new Error("CDCManager not started. Call start() first.");
    }
    return this._cdcManager;
  }

  /**
   * Get the split-brain detector.
   */
  getSplitBrainDetector(): DCSplitBrainDetector {
    if (!this._splitBrainDetector) {
      throw new Error("DCSplitBrainDetector not started. Call start() first.");
    }
    return this._splitBrainDetector;
  }

  /**
   * Get the conflict resolver.
   */
  getConflictResolver(): ConflictResolver {
    if (!this._conflictResolver) {
      throw new Error("ConflictResolver not initialized. Call start() first.");
    }
    return this._conflictResolver;
  }

  /**
   * Get the anti-entropy engine.
   */
  getAntiEntropy(): AntiEntropy {
    if (!this._antiEntropy) {
      throw new Error("AntiEntropy not started. Call start() first.");
    }
    return this._antiEntropy;
  }

  /**
   * Get current cluster health.
   */
  async getHealth(): Promise<FederationHealth> {
    return this.healthMonitor.check();
  }

  /**
   * Register a downstream consumer (sqlvec-db or graphvec-db adapter).
   */
  registerConsumer(consumer: IFederationConsumer): void {
    this.consumers.push(consumer);
  }

  /**
   * Get registered consumers.
   */
  getConsumers(): IFederationConsumer[] {
    return [...this.consumers];
  }

  /**
   * Get the local HLC instance.
   */
  getHLC(): HLC {
    return this.hlc;
  }

  /**
   * Check if the manager has started.
   */
  isStarted(): boolean {
    return this._started;
  }

  /**
   * Register a health change callback.
   */
  onHealthChange(callback: (health: FederationHealth) => void): () => void {
    return this.healthMonitor.onHealthChange(callback);
  }

  /**
   * Register a high-lag callback.
   */
  onHighLag(callback: (targetNodeId: string, lagMs: number) => void): () => void {
    return this.healthMonitor.onHighLag(callback);
  }

  /**
   * Register a split-brain detection callback.
   */
  onSplitBrainDetected(callback: (event: SplitBrainEvent) => void): () => void {
    this.emitter.on("split-brain-detected", callback);
    return () => { this.emitter.off("split-brain-detected", callback); };
  }

  /**
   * Get the local key-value store for AntiEntropy reconciliation.
   */
  getLocalStore(): Map<string, { key: string; value: string; hlc: { wallTime: number; logical: number; nodeId: string } }> {
    return this.localStore;
  }
}
