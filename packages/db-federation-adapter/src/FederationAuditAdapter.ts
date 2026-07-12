/**
 * FederationAuditAdapter — Bridges db-federation audit operations
 * into the IAuditStore interface from db-operations-audit.
 *
 * Every cross-shard query is an audit event that tracks:
 *   - source: originating DC/node
 *   - target shard(s): which shards were queried
 *   - query type: read, write, replicate, conflict-resolution
 *   - duration: how long the operation took
 *   - result: success, failure, or denied
 *
 * Federation-specific audit event types:
 *   - 'federation.query.routed'       → Cross-shard query routing
 *   - 'federation.replication.sent'   → Data replicated to a target
 *   - 'federation.replication.recv'   → Data received from a remote DC
 *   - 'federation.conflict.resolved'  → LWW or custom conflict resolution
 *   - 'federation.topology.change'    → Peer added/removed/changed
 *
 * @module @agentix-e/db-federation-adapter
 */

import type { IAuditStore, WriteAuditEntryOptions, AuditQueryFilter } from '@agentix-e/db-operations-audit';
import type { AuditEntry } from '@agentix-e/db-operations-core/types';
import type { IFederationNode } from './_types.js';

export type FederationAuditEventType =
  | 'federation.query.routed'
  | 'federation.replication.sent'
  | 'federation.replication.recv'
  | 'federation.conflict.resolved'
  | 'federation.topology.change'
  | 'federation.anti-entropy.cycle';

export interface FederationAuditEventBase {
  /** The federation audit event type */
  operation: FederationAuditEventType;
  /** Who triggered the operation */
  actor: string;
  /** The resource (shard / target / key) */
  resource: string;
  /** Whether the operation succeeded */
  result: 'success' | 'failure' | 'denied';
  /** Optional trace ID for distributed tracing correlation */
  traceId?: string;
  /** Additional details */
  details?: Record<string, unknown>;
}

export interface FederationAuditAdapterConfig {
  /** The FederationManager (or compatible) instance for context enrichment */
  federationNode: IFederationNode;
  /** Backing audit store implementation */
  auditStore: IAuditStore;
}

/**
 * IAuditStore adapter for db-federation audit events.
 *
 * Delegates to a backing IAuditStore after enriching each event
 * with federation-specific context (DC, nodeId, uptime).
 *
 * Also tracks aggregate counters for audit event statistics.
 */
export class FederationAuditAdapter implements IAuditStore {
  private readonly federationNode: IFederationNode;
  private readonly auditStore: IAuditStore;

  /** Statistics counters */
  private totalEvents = 0;
  private readonly eventsByType = new Map<FederationAuditEventType, number>();
  private lastEventAt: number | null = null;

  constructor(config: FederationAuditAdapterConfig) {
    this.federationNode = config.federationNode;
    this.auditStore = config.auditStore;
  }

  /**
   * Write a federation audit event.
   *
   * Automatically enriches the entry with:
   *   - Federation DC and nodeId context
   *   - Server-side timestamp
   *
   * Returns the generated audit entry ID.
   */
  async write(entry: WriteAuditEntryOptions): Promise<string> {
    const enriched: WriteAuditEntryOptions = {
      ...entry,
      details: {
        ...entry.details,
        federationDc: this.getDc(),
        federationNodeId: this.getNodeId(),
      },
    };

    const id = await this.auditStore.write(enriched);

    // Update local stats
    this.totalEvents++;
    if (isFederationAuditEventType(entry.operation)) {
      this.eventsByType.set(entry.operation, (this.eventsByType.get(entry.operation) ?? 0) + 1);
    }
    this.lastEventAt = Date.now();

    return id;
  }

  /** Query audit entries with filters. */
  async query(filter: AuditQueryFilter): Promise<AuditEntry[]> {
    return this.auditStore.query(filter);
  }

  /** Get a single audit entry by ID. */
  async get(id: string): Promise<AuditEntry | null> {
    return this.auditStore.get(id);
  }

  /** Get the total count of audit entries, optionally filtered. */
  async count(filter?: AuditQueryFilter): Promise<number> {
    return this.auditStore.count(filter);
  }

  /** Prune audit entries older than the given timestamp. */
  async pruneOlderThan(timestamp: Date): Promise<number> {
    return this.auditStore.pruneOlderThan(timestamp);
  }

  // ── Convenience recorders ──

  /** Record a cross-shard query routing event. */
  async recordQueryRouted(params: {
    sourceDc: string;
    targetShard: string;
    queryType: string;
    durationMs: number;
    success: boolean;
    traceId?: string;
  }): Promise<string> {
    return this.write({
      operation: 'federation.query.routed',
      actor: `dc:${params.sourceDc}`,
      resource: `shard:${params.targetShard}`,
      result: params.success ? 'success' : 'failure',
      traceId: params.traceId,
      details: {
        queryType: params.queryType,
        durationMs: params.durationMs,
        sourceDc: params.sourceDc,
      },
    });
  }

  /** Record a replication send event. */
  async recordReplicationSent(params: {
    targetNodeId: string;
    shardId: number;
    entryCount: number;
    success: boolean;
  }): Promise<string> {
    return this.write({
      operation: 'federation.replication.sent',
      actor: `node:${this.getNodeId()}`,
      resource: `target:${params.targetNodeId}:shard:${params.shardId}`,
      result: params.success ? 'success' : 'failure',
      details: {
        entryCount: params.entryCount,
        shardId: params.shardId,
      },
    });
  }

  /** Record a conflict resolution event. */
  async recordConflictResolved(params: {
    key: string;
    resolutionStrategy: string;
    sourceDc: string;
  }): Promise<string> {
    return this.write({
      operation: 'federation.conflict.resolved',
      actor: `dc:${params.sourceDc}`,
      resource: `key:${params.key}`,
      result: 'success',
      details: {
        resolutionStrategy: params.resolutionStrategy,
        sourceDc: params.sourceDc,
      },
    });
  }

  // ── Statistics ──

  /** Total audit events recorded. */
  getTotalEvents(): number {
    return this.totalEvents;
  }

  /** Audit events breakdown by type. */
  getEventsByType(): Map<FederationAuditEventType, number> {
    return new Map(this.eventsByType);
  }

  /** Timestamp of the last audit event (ms epoch). */
  getLastEventAt(): number | null {
    return this.lastEventAt;
  }

  // ── Private helpers ──

  private getDc(): string {
    try {
      return this.federationNode.getDc();
    } catch {
      return 'unknown';
    }
  }

  private getNodeId(): string {
    try {
      return this.federationNode.getNodeId();
    } catch {
      return 'unknown';
    }
  }
}

function isFederationAuditEventType(op: string): op is FederationAuditEventType {
  return [
    'federation.query.routed',
    'federation.replication.sent',
    'federation.replication.recv',
    'federation.conflict.resolved',
    'federation.topology.change',
    'federation.anti-entropy.cycle',
  ].includes(op);
}
