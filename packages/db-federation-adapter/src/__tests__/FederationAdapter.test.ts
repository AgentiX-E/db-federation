/**
 * FederationAdapter Tests — Integration tests verifying all four
 * db-federation adapter classes work with real db-federation imports.
 *
 * Tests:
 *   1. FederationHealthAdapter — IHealthProbe (liveness/readiness/startup)
 *   2. FederationMetricAdapter — IMetricsRegistry (counter/gauge/histogram)
 *   3. FederationAuditAdapter — IAuditStore (write/query/count/prune)
 *   4. FederationTracerAdapter — IDistributedTracer (spans/inject/extract)
 *
 * Uses real FederationManager instances with InProcessTransport
 * and mock replication for integration-style tests (vitest resolves
 * the runtime imports via relative paths).
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';

// Real db-federation types — vitest resolves these via file: symlink
import {
  FederationManager,
  InProcessTransport,
  HLC,
} from '../../../../src/index.js';
import type { ReplicationConfig } from '../../../../src/interfaces/replication/IMultiLeaderReplication';

// Adapter imports
import { FederationHealthAdapter } from '../FederationHealthAdapter.js';
import { FederationMetricAdapter } from '../FederationMetricAdapter.js';
import { FederationAuditAdapter } from '../FederationAuditAdapter.js';
import { FederationTracerAdapter } from '../FederationTracerAdapter.js';
import type { IFederationNode } from '../_types.js';

// db-operations imports
import type { IHealthProbe } from '@agentix-e/db-operations-health';
import type { IMetricsRegistry } from '@agentix-e/db-operations-metrics';
import type { IAuditStore } from '@agentix-e/db-operations-audit';
import type { IDistributedTracer } from '@agentix-e/db-operations-observability';
import { InMemoryAuditStore } from '@agentix-e/db-operations-audit';
import { InMemoryDistributedTracer } from '@agentix-e/db-operations-observability';

// ── Helpers ────────────────────────────────────────

function createMockReplication() {
  return {
    start: vi.fn().mockResolvedValue(undefined),
    stop: vi.fn().mockResolvedValue(undefined),
    replicate: vi.fn().mockResolvedValue({ msgId: 'mock-msg-1' }),
    getStatus: vi.fn().mockResolvedValue({
      nodeId: 'test-node',
      dc: 'test-dc',
      targets: [
        { targetNodeId: 'peer-a', connected: true, lagMs: 12, pendingCount: 0, lastSuccessAt: Date.now() - 100, lastErrorAt: null, lastErrorMessage: null },
        { targetNodeId: 'peer-b', connected: true, lagMs: 45, pendingCount: 2, lastSuccessAt: Date.now() - 500, lastErrorAt: null, lastErrorMessage: null },
      ],
    }),
    retryFailed: vi.fn().mockResolvedValue(0),
    getPendingCounts: vi.fn().mockResolvedValue({ 'peer-a': 0, 'peer-b': 2 }),
    onTargetConnected: vi.fn().mockReturnValue(() => {}),
    onTargetDisconnected: vi.fn().mockReturnValue(() => {}),
    onHighLag: vi.fn().mockReturnValue(() => {}),
  };
}

function createFederationManager(replication: ReturnType<typeof createMockReplication>): FederationManager {
  const transport = new InProcessTransport({ nodeId: 'test-node' });
  const hlc = new HLC('test-node');
  const outbox = {
    init: vi.fn().mockResolvedValue(undefined),
    write: vi.fn().mockResolvedValue({ msgId: 'outbox-1' }),
  } as any;

  const replicationConfig: ReplicationConfig = {
    nodeId: 'test-node',
    dc: 'test-dc',
    targets: [
      { nodeId: 'peer-a', dc: 'dc-east', address: 'tcp://peer-a:7001', topology: 'cross-dc', maxBackoffSeconds: 60 },
      { nodeId: 'peer-b', dc: 'dc-west', address: 'tcp://peer-b:7001', topology: 'cross-dc', maxBackoffSeconds: 60 },
    ],
    curveDir: '/tmp/test-curve',
    tickMs: 100,
    batchSize: 100,
    maxRetries: 3,
  };

  return new FederationManager(replication, {
    nodeId: 'test-node',
    dc: 'test-dc',
    replicationConfig,
    transport,
    outbox,
    hlc,
  });
}

// Wrap FederationManager as IFederationNode
function asFederationNode(fm: FederationManager): IFederationNode {
  return {
    isStarted: () => fm.isStarted(),
    getDc: () => (fm as any).config?.dc ?? 'test-dc',
    getNodeId: () => (fm as any).config?.nodeId ?? 'test-node',
    getHealth: async () => { const h = await fm.getHealth(); return h as any; },
  };
}

// ═══════════════════════════════════════════════════
//  FederationHealthAdapter Tests
// ═══════════════════════════════════════════════════

describe('FederationHealthAdapter — IHealthProbe', () => {
  let replication: ReturnType<typeof createMockReplication>;
  let federationManager: FederationManager;
  let federationNode: IFederationNode;
  let adapter: FederationHealthAdapter;

  beforeEach(() => {
    replication = createMockReplication();
    federationManager = createFederationManager(replication);
    federationNode = asFederationNode(federationManager);
    adapter = new FederationHealthAdapter({
      federationNode,
      minConnectedPeers: 1,
      maxLagMs: 60_000,
    });
  });

  it('should satisfy the IHealthProbe interface contract', () => {
    const probe: IHealthProbe = adapter;
    expect(probe.probeId).toBe('federation-health');
    expect(probe.componentName).toBe('federation-node');
  });

  it('should report liveness = false before federation manager starts', () => {
    const result = adapter.liveness();
    expect(result.ok).toBe(false);
    expect(result.message).toContain('not started');
  });

  it('should report liveness = true after federation manager starts', async () => {
    await federationManager.start();
    const result = adapter.liveness();
    expect(result.ok).toBe(true);
    expect(result.message).toContain('alive');
    await federationManager.stop();
  });

  it('should report startup = false before start, true after', async () => {
    expect(adapter.startup().ok).toBe(false);
    await federationManager.start();
    expect(adapter.startup().ok).toBe(true);
    expect(adapter.startup().message).toContain('completed');
    await federationManager.stop();
  });

  it('should update readiness from health monitor', async () => {
    await federationManager.start();
    await adapter.updateReadinessFromHealth();
    const result = adapter.getLastReadinessResult();
    expect(result.ok).toBe(true);
    expect(result.details!.connectedPeers).toBe(2);
    expect(result.details!.maxLagMs).toBeDefined();
    await federationManager.stop();
  });

  it('should report degraded readiness when peers are below minimum', async () => {
    const lowPeerAdapter = new FederationHealthAdapter({
      federationNode,
      minConnectedPeers: 5,
      maxLagMs: 60_000,
    });
    await federationManager.start();
    await lowPeerAdapter.updateReadinessFromHealth();
    expect(lowPeerAdapter.getLastReadinessResult().ok).toBe(false);
    expect(lowPeerAdapter.getLastReadinessResult().message).toContain('below minimum');
    await federationManager.stop();
  });
});

// ═══════════════════════════════════════════════════
//  FederationMetricAdapter Tests
// ═══════════════════════════════════════════════════

describe('FederationMetricAdapter — IMetricsRegistry', () => {
  let replication: ReturnType<typeof createMockReplication>;
  let federationManager: FederationManager;
  let adapter: FederationMetricAdapter;

  beforeEach(() => {
    replication = createMockReplication();
    federationManager = createFederationManager(replication);
    adapter = new FederationMetricAdapter({ federationNode: asFederationNode(federationManager) });
  });

  it('should satisfy the IMetricsRegistry interface contract', () => {
    const registry: IMetricsRegistry = adapter;
    expect(registry.counter).toBeDefined();
    expect(registry.gauge).toBeDefined();
    expect(registry.histogram).toBeDefined();
    expect(registry.snapshot).toBeDefined();
  });

  it('should create and increment a counter', () => {
    const counter = adapter.counter('federation.queries_routed.total', { shard: '0' });
    counter.inc();
    counter.incBy(3);
    expect(counter.get()).toBe(4);
  });

  it('should create and set a gauge', () => {
    const gauge = adapter.gauge('federation.peers.connected');
    gauge.set(3);
    expect(gauge.get()).toBe(3);
    gauge.inc();
    expect(gauge.get()).toBe(4);
    gauge.dec();
    expect(gauge.get()).toBe(3);
  });

  it('should create a histogram and record observations', () => {
    const hist = adapter.histogram('federation.query.duration_ms', { shard: '0' });
    hist.observe(10);
    hist.observe(20);
    hist.observe(30);
    expect(hist.getCount()).toBe(3);
    expect(hist.getSum()).toBe(60);
  });

  it('should snapshot all metrics', () => {
    const counter = adapter.counter('test.counter');
    counter.inc();
    const gauge = adapter.gauge('test.gauge');
    gauge.set(42);

    const snapshot = adapter.snapshot();
    expect(snapshot.points.length).toBeGreaterThanOrEqual(2);
    expect(snapshot.capturedAt).toBeGreaterThan(0);
  });

  it('should record query routed and increment counter', () => {
    adapter.recordQueryRouted(0);
    adapter.recordQueryRouted(1);
    adapter.recordQueryRouted(0);

    const c0 = adapter.counter('federation.queries_routed.total', { shard: '0' });
    const c1 = adapter.counter('federation.queries_routed.total', { shard: '1' });
    expect(c0.get()).toBe(2);
    expect(c1.get()).toBe(1);
  });

  it('should record errors per shard', () => {
    adapter.recordError('shard-0');
    adapter.recordError('shard-0');
    adapter.recordError('shard-1');

    const c0 = adapter.counter('federation.errors.total', { shard: 'shard-0' });
    const c1 = adapter.counter('federation.errors.total', { shard: 'shard-1' });
    expect(c0.get()).toBe(2);
    expect(c1.get()).toBe(1);
  });

  it('should record replication batch sizes', () => {
    adapter.recordReplicationBatchSize(5, 'peer-a');
    adapter.recordReplicationBatchSize(15, 'peer-a');
    adapter.recordReplicationBatchSize(50, 'peer-b');

    const histA = adapter.histogram('federation.replication.batch_size', { target: 'peer-a' });
    const histB = adapter.histogram('federation.replication.batch_size', { target: 'peer-b' });
    expect(histA.getCount()).toBe(2);
    expect(histB.getCount()).toBe(1);
  });
});

// ═══════════════════════════════════════════════════
//  FederationAuditAdapter Tests
// ═══════════════════════════════════════════════════

describe('FederationAuditAdapter — IAuditStore', () => {
  let replication: ReturnType<typeof createMockReplication>;
  let federationManager: FederationManager;
  let store: InMemoryAuditStore;
  let adapter: FederationAuditAdapter;

  beforeEach(() => {
    replication = createMockReplication();
    federationManager = createFederationManager(replication);
    store = new InMemoryAuditStore();
    adapter = new FederationAuditAdapter({
      federationNode: asFederationNode(federationManager),
      auditStore: store,
    });
  });

  it('should satisfy the IAuditStore interface contract', () => {
    const audit: IAuditStore = adapter;
    expect(audit.write).toBeDefined();
    expect(audit.query).toBeDefined();
    expect(audit.get).toBeDefined();
    expect(audit.count).toBeDefined();
    expect(audit.pruneOlderThan).toBeDefined();
  });

  it('should write a federation audit event', async () => {
    const id = await adapter.write({
      operation: 'federation.query.routed',
      actor: 'dc:dc-east',
      resource: 'shard:shard-0',
      result: 'success',
    });
    expect(id).toBeDefined();
    expect(id.length).toBeGreaterThan(0);
  });

  it('should write a query routed event via convenience method', async () => {
    const id = await adapter.recordQueryRouted({
      sourceDc: 'dc-east',
      targetShard: 'shard-0',
      queryType: 'read',
      durationMs: 42,
      success: true,
    });
    expect(id).toBeDefined();

    const entry = await adapter.get(id);
    expect(entry).not.toBeNull();
    expect(entry!.operation).toBe('federation.query.routed');
    expect(entry!.details!.sourceDc).toBe('dc-east');
    expect(entry!.details!.durationMs).toBe(42);
  });

  it('should write a replication sent event', async () => {
    const id = await adapter.recordReplicationSent({
      targetNodeId: 'peer-b',
      shardId: 1,
      entryCount: 10,
      success: true,
    });
    expect(id).toBeDefined();

    const entry = await adapter.get(id);
    expect(entry).not.toBeNull();
    expect(entry!.operation).toBe('federation.replication.sent');
    expect(entry!.details!.entryCount).toBe(10);
  });

  it('should write a conflict resolution event', async () => {
    const id = await adapter.recordConflictResolved({
      key: 'user:42',
      resolutionStrategy: 'lww',
      sourceDc: 'dc-east',
    });
    expect(id).toBeDefined();

    const entry = await adapter.get(id);
    expect(entry).not.toBeNull();
    expect(entry!.operation).toBe('federation.conflict.resolved');
    expect(entry!.details!.resolutionStrategy).toBe('lww');
  });

  it('should query audit entries', async () => {
    await adapter.recordQueryRouted({ sourceDc: 'dc-east', targetShard: 's0', queryType: 'read', durationMs: 10, success: true });
    await adapter.recordQueryRouted({ sourceDc: 'dc-west', targetShard: 's1', queryType: 'write', durationMs: 20, success: false });
    await adapter.recordReplicationSent({ targetNodeId: 'peer-a', shardId: 0, entryCount: 5, success: true });

    const all = await adapter.query({});
    expect(all.length).toBe(3);

    const filtered = await adapter.query({ operation: 'federation.replication.sent' });
    expect(filtered.length).toBe(1);
  });

  it('should count audit entries', async () => {
    await adapter.recordQueryRouted({ sourceDc: 'dc-east', targetShard: 's0', queryType: 'read', durationMs: 10, success: true });
    await adapter.recordQueryRouted({ sourceDc: 'dc-west', targetShard: 's1', queryType: 'write', durationMs: 20, success: false });

    expect(await adapter.count()).toBe(2);
    expect(await adapter.count({ operation: 'federation.query.routed' })).toBe(2);
  });

  it('should prune old audit entries', async () => {
    await adapter.recordQueryRouted({ sourceDc: 'dc-east', targetShard: 's0', queryType: 'read', durationMs: 10, success: true });

    const pruned = await adapter.pruneOlderThan(new Date(Date.now() + 86400000));
    expect(pruned).toBe(1);
    expect(await adapter.count()).toBe(0);
  });

  it('should track audit statistics', async () => {
    expect(adapter.getTotalEvents()).toBe(0);

    await adapter.recordQueryRouted({ sourceDc: 'dc-east', targetShard: 's0', queryType: 'read', durationMs: 10, success: true });
    await adapter.recordReplicationSent({ targetNodeId: 'peer-a', shardId: 0, entryCount: 5, success: true });

    expect(adapter.getTotalEvents()).toBe(2);
    const byType = adapter.getEventsByType();
    expect(byType.get('federation.query.routed')).toBe(1);
    expect(byType.get('federation.replication.sent')).toBe(1);
    expect(adapter.getLastEventAt()).not.toBeNull();
  });
});

// ═══════════════════════════════════════════════════
//  FederationTracerAdapter Tests
// ═══════════════════════════════════════════════════

describe('FederationTracerAdapter — IDistributedTracer', () => {
  let federationManager: FederationManager;
  let replication: ReturnType<typeof createMockReplication>;
  let adapter: FederationTracerAdapter;

  beforeEach(() => {
    replication = createMockReplication();
    federationManager = createFederationManager(replication);
    const underlying = new InMemoryDistributedTracer();
    adapter = new FederationTracerAdapter({
      federationNode: asFederationNode(federationManager),
      tracer: underlying,
    });
  });

  it('should satisfy the IDistributedTracer interface contract', () => {
    const tracer: IDistributedTracer = adapter;
    expect(tracer.startSpan).toBeDefined();
    expect(tracer.getCurrentSpan).toBeDefined();
    expect(tracer.getCompletedSpans).toBeDefined();
    expect(tracer.clearCompletedSpans).toBeDefined();
    expect(tracer.inject).toBeDefined();
    expect(tracer.extract).toBeDefined();
  });

  it('should start and end a span', () => {
    const { spanId, end } = adapter.startSpan('test-span', {
      attributes: { 'federation.shard': 'shard-0' },
    });
    expect(spanId).toBeDefined();
    expect(spanId.length).toBeGreaterThan(0);

    end('ok');
    const spans = adapter.getCompletedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].name).toBe('test-span');
    expect(spans[0].status).toBe('ok');
    expect(spans[0].attributes['federation.shard']).toBe('shard-0');
  });

  it('should track current span context', () => {
    adapter.startSpan('active-span');
    const current = adapter.getCurrentSpan();
    expect(current).toBeDefined();
    expect(current!.traceId).toBeDefined();
    expect(current!.spanId).toBeDefined();
  });

  it('should inject W3C traceparent headers', () => {
    adapter.startSpan('trace-test');
    const carrier: Record<string, string> = {};
    adapter.inject(carrier);
    expect(carrier['traceparent']).toBeDefined();
    expect(carrier['traceparent']).toMatch(/^00-[a-f0-9-]+-[a-f0-9-]+-01$/);
  });

  it('should inject tracestate with federation metadata', () => {
    adapter.startSpan('tracestate-test');
    const carrier: Record<string, string> = {};
    adapter.inject(carrier);
    expect(carrier['tracestate']).toBeDefined();
    expect(carrier['tracestate']).toContain('federation_node=');
    expect(carrier['tracestate']).toContain('federation_dc=');
  });

  it('should extract traceparent from carrier', () => {
    const carrier: Record<string, string> = {
      traceparent: '00-abc123-def456-01',
    };
    const result = adapter.extract(carrier);
    expect(result).toBeDefined();
    expect(result!.traceId).toBe('abc123');
    expect(result!.parentSpanId).toBe('def456');
  });

  it('should return undefined when no traceparent in carrier', () => {
    const result = adapter.extract({});
    expect(result).toBeUndefined();
  });

  it('should create parent-child span hierarchy', () => {
    const { spanId: parentId } = adapter.startSpan('parent-query', {
      traceId: 'trace-42',
    });
    const { spanId: childId, end: childEnd } = adapter.startSpan('child-shard', {
      parentSpanId: parentId,
      traceId: 'trace-42',
      attributes: { 'federation.shard': 'shard-0' },
    });

    childEnd('ok');
    // End parent explicitly to capture it
    const spans = adapter.getCompletedSpans();
    // Child should be completed (parent is still "in-flight")
    expect(spans.length).toBe(1);
    expect(spans[0].spanId).toBe(childId);
    expect(spans[0].parentSpanId).toBe(parentId);
    expect(spans[0].traceId).toBe('trace-42');
  });

  it('should clear completed spans', () => {
    const { end } = adapter.startSpan('ephemeral');
    end('ok');
    expect(adapter.getCompletedSpans().length).toBe(1);
    adapter.clearCompletedSpans();
    expect(adapter.getCompletedSpans().length).toBe(0);
  });

  it('should trace a cross-shard query with success', async () => {
    const result = await adapter.traceCrossShardQuery('query-1', 'shard-0', async () => {
      return 'query-result';
    });
    expect(result).toBe('query-result');

    const spans = adapter.getCompletedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].name).toBe('federation.query.cross-shard');
    expect(spans[0].traceId).toBe('query-1');
    expect(spans[0].status).toBe('ok');
    expect(spans[0].attributes['federation.shard']).toBe('shard-0');
  });

  it('should trace a cross-shard query with error', async () => {
    await expect(
      adapter.traceCrossShardQuery('query-2', 'shard-1', async () => {
        throw new Error('shard-down');
      }),
    ).rejects.toThrow('shard-down');

    const spans = adapter.getCompletedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].status).toBe('error');
    expect(spans[0].errorMessage).toBe('shard-down');
  });

  it('should trace replication operations', async () => {
    const result = await adapter.traceReplication('peer-a', async () => {
      return { success: true };
    });
    expect(result).toEqual({ success: true });

    const spans = adapter.getCompletedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].name).toBe('federation.replication.send');
    expect(spans[0].attributes['federation.target']).toBe('peer-a');
    expect(spans[0].status).toBe('ok');
  });

  it('should enrich spans with federation node context', () => {
    const { end } = adapter.startSpan('context-test');
    end('ok');

    const spans = adapter.getCompletedSpans();
    expect(spans.length).toBe(1);
    expect(spans[0].attributes['federation.node']).toBe('test-node');
    expect(spans[0].attributes['federation.dc']).toBe('test-dc');
  });
});

// ═══════════════════════════════════════════════════
//  Integration: End-to-End Federation Flow
// ═══════════════════════════════════════════════════

describe('Integration: Federation Manager → Adapters', () => {
  it('should coordinate health, metrics, audit, and tracing adapters together', async () => {
    const replication = createMockReplication();
    const federationManager = createFederationManager(replication);
    const federationNode = asFederationNode(federationManager);

    const healthAdapter = new FederationHealthAdapter({ federationNode });
    const metricAdapter = new FederationMetricAdapter({ federationNode });
    const auditStore = new InMemoryAuditStore();
    const auditAdapter = new FederationAuditAdapter({ federationNode, auditStore });
    const underlyingTracer = new InMemoryDistributedTracer();
    const tracerAdapter = new FederationTracerAdapter({ federationNode, tracer: underlyingTracer });

    await federationManager.start();

    // 1. Health: verify node is alive and ready
    expect(healthAdapter.liveness().ok).toBe(true);
    expect(healthAdapter.startup().ok).toBe(true);
    await healthAdapter.updateReadinessFromHealth();
    expect(healthAdapter.getLastReadinessResult().ok).toBe(true);

    // 2. Metrics: track a query route + duration
    metricAdapter.recordQueryRouted(0);
    metricAdapter.recordQueryDuration(42, 0);
    const queryCounter = metricAdapter.counter('federation.queries_routed.total', { shard: '0' });
    expect(queryCounter.get()).toBe(1);

    // 3. Audit: record the query routing
    const auditId = await auditAdapter.recordQueryRouted({
      sourceDc: 'dc-east',
      targetShard: 'shard-0',
      queryType: 'read',
      durationMs: 42,
      success: true,
    });
    expect(auditId).toBeDefined();

    // 4. Tracing: trace the cross-shard query
    const traceResult = await tracerAdapter.traceCrossShardQuery('integ-query-1', 'shard-0', async () => {
      return { data: 'hello' };
    });
    expect(traceResult).toEqual({ data: 'hello' });

    // 5. Verify all spans completed
    const spans = tracerAdapter.getCompletedSpans();
    expect(spans.length).toBeGreaterThanOrEqual(1);

    // 6. Verify audit entry is queryable
    const entries = await auditAdapter.query({ operation: 'federation.query.routed' });
    expect(entries.length).toBeGreaterThanOrEqual(1);

    await federationManager.stop();
    expect(healthAdapter.liveness().ok).toBe(false);
  });

  it('should handle federation errors through all adapter layers', async () => {
    const replication = createMockReplication();
    const federationManager = createFederationManager(replication);
    const federationNode = asFederationNode(federationManager);

    const metricAdapter = new FederationMetricAdapter({ federationNode });
    const auditStore = new InMemoryAuditStore();
    const auditAdapter = new FederationAuditAdapter({ federationNode, auditStore });
    const tracerAdapter = new FederationTracerAdapter({ federationNode });

    await federationManager.start();

    // Simulate an error scenario
    metricAdapter.recordError('shard-1');
    await auditAdapter.recordQueryRouted({
      sourceDc: 'dc-west',
      targetShard: 'shard-1',
      queryType: 'write',
      durationMs: 5000,
      success: false,
    });

    // Verify error metrics
    const errorCounter = metricAdapter.counter('federation.errors.total', { shard: 'shard-1' });
    expect(errorCounter.get()).toBe(1);

    // Verify error was audited
    const errorAudits = await auditAdapter.query({ result: 'failure' });
    expect(errorAudits.length).toBe(1);
    expect(errorAudits[0].details!.durationMs).toBe(5000);

    await federationManager.stop();
  });
});
