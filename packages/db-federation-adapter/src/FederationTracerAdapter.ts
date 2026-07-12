/**
 * FederationTracerAdapter — Bridges db-federation distributed query
 * tracing into the IDistributedTracer interface from db-operations-observability.
 *
 * Federation queries NATURALLY span multiple shards (and DCs), making
 * distributed tracing a first-class concern. This adapter:
 *
 *   1. Creates a span for every cross-shard query
 *   2. Propagates W3C TraceContext headers for distributed trace correlation
 *   3. Records per-shard spans within a query trace
 *   4. Supports span hierarchy: query → shard → operation
 *
 * Span mapping (federation → W3C TraceContext):
 *   - traceId = queryId (UUID)
 *   - spanId = shard operation instance
 *   - parentSpanId = query-level span
 *   - traceparent header = `00-{traceId}-{spanId}-01`
 *
 * @module @agentix-e/db-federation-adapter
 */

import type { IDistributedTracer } from '@agentix-e/db-operations-observability';
import type { Span } from '@agentix-e/db-operations-observability';
import type { IFederationNode } from './_types.js';

export interface FederationTracerAdapterConfig {
  /** The FederationManager (or compatible) instance for context enrichment */
  federationNode: IFederationNode;
  /** Optional: backing tracer (creates InMemoryDistributedTracer if not provided) */
  tracer?: IDistributedTracer;
}

/**
 * IDistributedTracer adapter for db-federation.
 *
 * Creates spans for:
 *   - Cross-shard queries (top-level span)
 *   - Per-shard operations (child spans)
 *   - Replication sends/receives (child spans)
 *
 * Uses W3C TraceContext for distributed propagation:
 *   inject() sets traceparent header
 *   extract() parses traceparent header
 */
export class FederationTracerAdapter implements IDistributedTracer {
  private readonly federationNode: IFederationNode;
  private readonly tracer: IDistributedTracer;

  constructor(config: FederationTracerAdapterConfig) {
    this.federationNode = config.federationNode;
    if (config.tracer) {
      this.tracer = config.tracer;
    } else {
      const { InMemoryDistributedTracer } = require('@agentix-e/db-operations-observability');
      this.tracer = new InMemoryDistributedTracer();
    }
  }

  /**
   * Start a span for a federation operation.
   *
   * Supports:
   *   - traceId: Explicit trace ID (for continuing an existing trace)
   *   - parentSpanId: Explicit parent (for nested operations)
   *   - attributes: Span metadata (shard, dc, operation type)
   *
   * Federation-specific attribute conventions:
   *   - 'federation.shard'     → target shard ID
   *   - 'federation.dc'        → source or target DC
   *   - 'federation.operation' → query, replicate, resolve, anti-entropy
   *   - 'federation.target'    → target node ID
   */
  startSpan(name: string, options?: {
    parentSpanId?: string;
    traceId?: string;
    attributes?: Record<string, string | number | boolean>;
  }): { spanId: string; end: (status?: 'ok' | 'error', errorMessage?: string) => void } {
    const enrichedAttrs: Record<string, string | number | boolean> = {
      ...options?.attributes,
    };

    // Add node context if not already present
    if (!('federation.node' in enrichedAttrs)) {
      try { enrichedAttrs['federation.node'] = this.federationNode.getNodeId(); }
      catch { enrichedAttrs['federation.node'] = 'unknown'; }
    }
    if (!('federation.dc' in enrichedAttrs)) {
      try { enrichedAttrs['federation.dc'] = this.federationNode.getDc(); }
      catch { enrichedAttrs['federation.dc'] = 'unknown'; }
    }

    return this.tracer.startSpan(name, {
      parentSpanId: options?.parentSpanId,
      traceId: options?.traceId,
      attributes: enrichedAttrs,
    });
  }

  /** Get the current span context. */
  getCurrentSpan(): { traceId: string; spanId: string } | undefined {
    return this.tracer.getCurrentSpan();
  }

  /** Get all completed spans for export. */
  getCompletedSpans(): Span[] {
    return this.tracer.getCompletedSpans();
  }

  /** Clear completed spans (after export). */
  clearCompletedSpans(): void {
    this.tracer.clearCompletedSpans();
  }

  /**
   * Inject trace context into W3C TraceContext carrier.
   *
   * Sets the 'traceparent' header and enriches 'tracestate'
   * with federation metadata.
   */
  inject(carrier: Record<string, string>): void {
    this.tracer.inject(carrier);

    // Also inject federation-specific metadata via tracestate
    const current = this.getCurrentSpan();
    if (current) {
      try {
        carrier['tracestate'] = `federation_node=${this.federationNode.getNodeId()},federation_dc=${this.federationNode.getDc()}`;
      } catch {
        // Tracestate is optional — skip on error
      }
    }
  }

  /**
   * Extract trace context from W3C TraceContext carrier.
   *
   * Parses 'traceparent' header: `00-{traceId}-{parentSpanId}-01`
   */
  extract(carrier: Record<string, string>): { traceId: string; parentSpanId: string } | undefined {
    return this.tracer.extract(carrier);
  }

  /**
   * Convenience: trace a cross-shard query execution.
   *
   * Creates a span and executes the provided fn within that span context.
   *
   * @returns The result of the executed function.
   */
  async traceCrossShardQuery<T>(
    queryId: string,
    shard: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    const { end } = this.startSpan('federation.query.cross-shard', {
      traceId: queryId,
      attributes: {
        'federation.shard': shard,
        'federation.operation': 'query',
      },
    });

    try {
      const result = await fn();
      end('ok');
      return result;
    } catch (err) {
      end('error', err instanceof Error ? err.message : String(err));
      throw err;
    }
  }

  /**
   * Convenience: trace a replication operation.
   */
  async traceReplication<T>(
    targetNodeId: string,
    fn: () => Promise<T>,
  ): Promise<T> {
    const { end } = this.startSpan('federation.replication.send', {
      attributes: {
        'federation.target': targetNodeId,
        'federation.operation': 'replicate',
      },
    });

    try {
      const result = await fn();
      end('ok');
      return result;
    } catch (err) {
      end('error', err instanceof Error ? err.message : String(err));
      throw err;
    }
  }
}
