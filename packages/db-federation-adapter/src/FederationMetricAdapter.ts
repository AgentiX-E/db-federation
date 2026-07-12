/**
 * FederationMetricAdapter — Bridges db-federation operational metrics
 * into the IMetricsRegistry interface from db-operations-metrics.
 *
 * Tracks federation-specific metrics:
 *   - federation_queries_routed_total — counter per shard
 *   - federation_cross_shard_query_duration_ms — histogram
 *   - federation_replication_batch_size — histogram
 *   - federation_errors_per_shard_total — counter per shard
 *   - federation_connected_peers — gauge
 *   - federation_replication_lag_ms — gauge per target
 *   - federation_outbox_pending — gauge per target
 *
 * @module @agentix-e/db-federation-adapter
 */

import type { IMetricsRegistry, ICounter, IGauge, IHistogram } from '@agentix-e/db-operations-metrics';
import type { MetricsSnapshot } from '@agentix-e/db-operations-core/types';
import type { IFederationNode, IFederationNodeHealth } from './_types.js';

/**
 * Lazy-imported MetricsRegistry to avoid circular dependency issues.
 * This import is only used in the constructor when no registry is provided.
 */
function getMetricsRegistryClass(): new () => IMetricsRegistry {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { MetricsRegistry } = require('@agentix-e/db-operations-metrics');
  return MetricsRegistry;
}

export interface FederationMetricAdapterConfig {
  /** The FederationManager (or compatible) instance */
  federationNode: IFederationNode;
  /** Optional metrics registry — created internally if not provided */
  registry?: IMetricsRegistry;
}

/**
 * IMetricsRegistry adapter that instruments db-federation operations.
 *
 * Provides pre-defined metric names for federation-specific concerns
 * and auto-collects federation health stats into gauges on snapshot.
 *
 * Standard metric names:
 *   - federation.queries_routed.total       → counter  { shard }
 *   - federation.query.duration_ms          → histogram { shard }
 *   - federation.replication.batch_size     → histogram { target }
 *   - federation.errors.total               → counter  { shard }
 *   - federation.peers.connected            → gauge    {}
 *   - federation.replication.lag_ms         → gauge    { target }
 *   - federation.outbox.pending             → gauge    { target }
 */
export class FederationMetricAdapter implements IMetricsRegistry {
  private readonly registry: IMetricsRegistry;
  private readonly federationNode: IFederationNode;
  private readonly gaugeCache: Map<string, IGauge> = new Map();
  private readonly counterCache: Map<string, ICounter> = new Map();
  private readonly histogramCache: Map<string, IHistogram> = new Map();

  constructor(config: FederationMetricAdapterConfig) {
    this.federationNode = config.federationNode;
    this.registry = config.registry ?? new (getMetricsRegistryClass())();
  }

  /** Create or retrieve a counter. */
  counter(name: string, labels?: Record<string, string>): ICounter {
    const key = this.labelKey(name, labels);
    let c = this.counterCache.get(key);
    if (!c) {
      c = this.registry.counter(name, labels);
      this.counterCache.set(key, c);
    }
    return c;
  }

  /** Create or retrieve a gauge. */
  gauge(name: string, labels?: Record<string, string>): IGauge {
    const key = this.labelKey(name, labels);
    let g = this.gaugeCache.get(key);
    if (!g) {
      g = this.registry.gauge(name, labels);
      this.gaugeCache.set(key, g);
    }
    return g;
  }

  /** Create or retrieve a histogram. */
  histogram(name: string, labels?: Record<string, string>, buckets?: number[]): IHistogram {
    const key = this.labelKey(name, labels);
    let h = this.histogramCache.get(key);
    if (!h) {
      h = this.registry.histogram(name, labels, buckets);
      this.histogramCache.set(key, h);
    }
    return h;
  }

  /**
   * Take a snapshot, auto-collecting federation health metrics
   * into the gauges before snapshotting.
   */
  snapshot(): MetricsSnapshot {
    this.collectHealthGauges().catch(() => { /* fire-and-forget */ });
    return this.registry.snapshot();
  }

  // ── Convenience recorders ──

  /** Record a federated query being routed. */
  recordQueryRouted(shard: string | number): void {
    const c = this.counter('federation.queries_routed.total', { shard: String(shard) });
    c.inc();
  }

  /** Record a cross-shard query duration. */
  recordQueryDuration(durationMs: number, shard: string | number): void {
    const h = this.histogram(
      'federation.query.duration_ms',
      { shard: String(shard) },
      [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
    );
    h.observe(durationMs);
  }

  /** Record a replication batch size. */
  recordReplicationBatchSize(size: number, target: string): void {
    const h = this.histogram(
      'federation.replication.batch_size',
      { target },
      [1, 10, 50, 100, 500, 1000],
    );
    h.observe(size);
  }

  /** Record an error for a specific shard. */
  recordError(shard: string | number): void {
    const c = this.counter('federation.errors.total', { shard: String(shard) });
    c.inc();
  }

  // ── Private ──

  private async collectHealthGauges(): Promise<void> {
    try {
      const health: IFederationNodeHealth = await this.federationNode.getHealth();

      // Per-target gauges
      for (const target of health.targets) {
        const targetLabels = { target: target.targetNodeId };

        const lagGauge = this.gauge('federation.replication.lag_ms', targetLabels);
        lagGauge.set(target.connected ? target.lagMs : -1);

        const pendingGauge = this.gauge('federation.outbox.pending', targetLabels);
        pendingGauge.set(target.pendingCount);
      }

      // Aggregate connected peers gauge
      const peersGauge = this.gauge('federation.peers.connected');
      peersGauge.set(health.healthyTargets);
    } catch {
      // Federation not available — gauges remain at their last value
    }
  }

  private labelKey(name: string, labels?: Record<string, string>): string {
    if (!labels || Object.keys(labels).length === 0) return name;
    const sorted = Object.entries(labels)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    return `${name}{${sorted}}`;
  }
}
