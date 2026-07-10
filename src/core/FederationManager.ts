/**
 * FederationManager — Top-level orchestrator for cross-DC replication.
 *
 * Owns the full lifecycle:
 *   1. MultiLeaderReplication — cross-DC write replication
 *   2. HealthMonitor — periodic health checks + lag tracking
 *   3. (Phase 3) CDCManager — change data capture
 *   4. (Phase 3) AntiEntropy — Merkle tree reconciliation
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

  // Phase 3 stubs
  private _cdcManager: ICDCManager | null = null;
  private _antiEntropy: IAntiEntropy | null = null;

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
   * This starts:
   *   1. MultiLeaderReplication (outbox workers per target)
   *   2. HealthMonitor (periodic health checks)
   */
  async start(): Promise<void> {
    if (this._started) return;

    await this.replication.start(this.config.replicationConfig);
    this.healthMonitor.start();
    this._started = true;
  }

  /**
   * Graceful shutdown.
   */
  async stop(): Promise<void> {
    this._started = false;
    this.healthMonitor.stop();
    await this.replication.stop();
  }

  /**
   * Replicate data to all targets.
   */
  async replicate(shardId: number, data: string, type: string): Promise<{ msgId: string }> {
    return this.replication.replicate(shardId, data, type);
  }

  /**
   * Get the replication engine.
   */
  getReplication(): IMultiLeaderReplication {
    return this.replication;
  }

  /**
   * Get the CDC manager (Phase 3).
   */
  getCDC(): ICDCManager {
    if (!this._cdcManager) {
      throw new Error(NOT_IMPLEMENTED_YET);
    }
    return this._cdcManager;
  }

  /**
   * Get the anti-entropy engine (Phase 3).
   */
  getAntiEntropy(): IAntiEntropy {
    if (!this._antiEntropy) {
      throw new Error(NOT_IMPLEMENTED_YET);
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
}
