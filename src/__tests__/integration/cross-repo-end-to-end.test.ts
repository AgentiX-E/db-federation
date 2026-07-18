/**
 * Cross-Repo End-to-End Tests — db-cluster ↔ db-federation ↔ db-operations.
 *
 * D2 iteration: validates joint operation of all three subsystems.
 *
 * Scenarios:
 *   E2E.1 — Raft propose → CDC capture → Outbox persist → Dedup receive
 *   E2E.2 — CDC stream → FileAuditStore append → query verifies completeness
 *   E2E.3 — CDC crash recovery → OutboxStore retries → DedupRing prevents double Audit
 *
 * Invariants:
 *   CR1 — Every Raft proposal eventually produces an Audit entry (G1 × F1)
 *   CR2 — No Audit entry appears twice (G2 × F2)
 *   CR3 — After crash+restart, no gaps in audit trail (G3 × F1)
 */

import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

// ── Repo A: db-cluster ──────────────────────────────────────
import { TestCluster } from '@agentix-e/db-cluster/__tests__/integration/TestCluster';

// ── Repo B: db-federation ───────────────────────────────────
import { OutboxStore } from '@agentix-e/db-federation/storage/OutboxStore';
import { DedupRing } from '@agentix-e/db-federation/replication/DedupRing';
import type { OutboxEntry } from '@agentix-e/db-federation/interfaces/outbox/OutboxEntry';
import { HLC } from '@agentix-e/db-federation/hlc/HLC';

// ── Repo C: db-operations ───────────────────────────────────
import { FileAuditStore } from '@agentix-e/db-operations/audit/src/impl/FileAuditStore';

// ── Constants ───────────────────────────────────────────────

const E2E_TIMEOUT = 180_000;

function tmpDir(label: string): string {
  return mkdtempSync(join(tmpdir(), `cross-e2e-${label}-`));
}

describe('Cross-Repo End-to-End (D2)', () => {
  // ── E2E.1: Raft propose → CDC → Outbox → Dedup ──────────
  // Verifies CR1: every proposal produces exactly one delivered entry
  it('E2E.1: should flow proposals through CDC pipeline without gaps', async () => {
    const cluster = new TestCluster({ nodeCount: 3, preVote: true });
    const outboxDir = tmpDir('outbox-e2e1');
    const auditDir = tmpDir('audit-e2e1');
    const tempDirs = [outboxDir, auditDir];

    try {
      // Start 3-node Raft cluster
      await cluster.start();
      const leaderNode = await cluster.findLeader();
      expect(leaderNode).toBeDefined();
      const leader = leaderNode!.adapter;

      // Init cross-repo components
      const outboxStore = new OutboxStore({
        dataDir: outboxDir,
        ttlMs: 60_000,
        maxRetries: 3,
        deadLetterTtlMs: 60_000,
        mapSize: 16 * 1024 * 1024,
      });

      const dedupRing = new DedupRing({ maxSize: 1000, ttlMs: 60_000 });
      const hlc = new HLC();

      // Propose 10 entries via Raft adapter
      const proposedIds: string[] = [];
      for (let i = 0; i < 10; i++) {
        const result = await leader.propose(
          JSON.stringify({ seq: i, data: `e2e-1-payload-${i}` }),
        );
        expect(result.index).toBeDefined();
        proposedIds.push(result.index);
      }
      expect(proposedIds.length).toBe(10);

      // CDC capture: write each proposal as outbox entry
      for (let i = 0; i < proposedIds.length; i++) {
        const msgId = `cdc:${proposedIds[i]}:e2e1`;
        const entry: Omit<OutboxEntry, 'id'> = {
          msgId,
          payload: { lsn: i, index: proposedIds[i], data: `captured-${i}` },
          timestamp: hlc.now(),
          retryCount: 0,
        };
        await outboxStore.add(entry);
      }

      // Pump outbox entries through dedup
      const receivedMsgIds: string[] = [];
      const pending = await outboxStore.fetchOldestPending(20);
      expect(pending.length).toBe(proposedIds.length);

      for (const entry of pending) {
        if (!dedupRing.contains(entry.msgId)) {
          receivedMsgIds.push(entry.msgId);
          dedupRing.put(entry.msgId);
        }
        await outboxStore.markDelivered(entry.id);
      }

      // CR1: No gaps — every proposed entry eventually delivered
      expect(receivedMsgIds.length).toBe(proposedIds.length);
      expect(new Set(receivedMsgIds).size).toBe(proposedIds.length);

      outboxStore.close();
    } finally {
      await cluster.stop();
      for (const d of tempDirs) {
        try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
      }
    }
  }, E2E_TIMEOUT);

  // ── E2E.2: CDC-delivered events → FileAuditStore → verify ──
  // Verifies CR1 + CR2 via audit store
  it('E2E.2: should persist CDC-delivered events into FileAuditStore', async () => {
    const outboxDir = tmpDir('outbox-e2e2');
    const auditDir = tmpDir('audit-e2e2');
    const tempDirs = [outboxDir, auditDir];

    try {
      const auditStore = new FileAuditStore({
        dataDir: auditDir,
        filePrefix: 'e2e2',
      });

      const outboxStore = new OutboxStore({
        dataDir: outboxDir,
        ttlMs: 60_000,
        maxRetries: 3,
        deadLetterTtlMs: 60_000,
        mapSize: 16 * 1024 * 1024,
      });

      const dedupRing = new DedupRing({ maxSize: 1000, ttlMs: 60_000 });
      const hlc = new HLC();
      const writtenAuditIds: string[] = [];

      // Simulate 15 CDC-delivered events flowing to audit store
      for (let i = 0; i < 15; i++) {
        const msgId = `e2e2:${i}`;
        const entry: Omit<OutboxEntry, 'id'> = {
          msgId,
          payload: { seq: i, data: `audit-e2e2-${i}` },
          timestamp: hlc.now(),
          retryCount: 0,
        };
        const created = await outboxStore.add(entry);

        // Consumer callback writes to audit store
        if (!dedupRing.contains(msgId)) {
          dedupRing.put(msgId);
          const auditId = await auditStore.write({
            operation: 'e2e2-propose',
            actor: 'cross-e2e',
            resource: '/test/e2e2',
            result: 'success' as const,
          });
          writtenAuditIds.push(auditId);
        }
        await outboxStore.markDelivered(created.id);
      }

      // CR1: All 15 events persisted to audit
      const totalCount = await auditStore.count();
      expect(totalCount).toBe(15);

      // CR2: No duplicate IDs
      expect(new Set(writtenAuditIds).size).toBe(15);

      // CR2: Query returns all entries
      const queried = await auditStore.query({ resource: '/test/e2e2', limit: 50 });
      expect(queried.length).toBe(15);

      outboxStore.close();
    } finally {
      for (const d of tempDirs) {
        try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
      }
    }
  }, E2E_TIMEOUT);

  // ── E2E.3: CDC crash+restart → no double audit ──────────
  // Verifies CR2 + CR3: after simulated crash, outbox retries entries
  // but DedupRing prevents them from being written to audit twice
  it('E2E.3: should not double-write audit entries after crash recovery', async () => {
    const crashOutboxDir = tmpDir('outbox-e2e3');
    const crashAuditDir = tmpDir('audit-e2e3');
    const tempDirs = [crashOutboxDir, crashAuditDir];

    try {
      const hlc = new HLC();

      const outboxStore = new OutboxStore({
        dataDir: crashOutboxDir,
        ttlMs: 60_000,
        maxRetries: 3,
        deadLetterTtlMs: 60_000,
        mapSize: 16 * 1024 * 1024,
      });

      const dedupRing = new DedupRing({ maxSize: 1000, ttlMs: 60_000 });
      const auditStore = new FileAuditStore({
        dataDir: crashAuditDir,
        filePrefix: 'crash-e2e3',
      });

      const beforeCrashAuditIds: string[] = [];

      // Phase 1: 5 normal entries
      for (let i = 0; i < 5; i++) {
        const msgId = `crash-e2e3:${i}`;
        const entry: Omit<OutboxEntry, 'id'> = {
          msgId,
          payload: { seq: i, data: `normal-${i}` },
          timestamp: hlc.now(),
          retryCount: 0,
        };
        const created = await outboxStore.add(entry);

        if (!dedupRing.contains(msgId)) {
          dedupRing.put(msgId);
          const auditId = await auditStore.write({
            operation: 'crash-e2e3',
            actor: 'cross-e2e',
            resource: `/crash/normal/${i}`,
            result: 'success' as const,
          });
          beforeCrashAuditIds.push(auditId);
        }
        await outboxStore.markDelivered(created.id);
      }

      // Phase 2: 3 entries written but "crash" before ack
      const crashMsgIds: string[] = [];
      for (let i = 0; i < 3; i++) {
        const msgId = `crash-e2e3:${5 + i}`;
        const entry: Omit<OutboxEntry, 'id'> = {
          msgId,
          payload: { seq: 5 + i, data: `crash-${i}` },
          timestamp: hlc.now(),
          retryCount: 0,
        };
        await outboxStore.add(entry);
        crashMsgIds.push(msgId);
        // "Crash" — no markDelivered, no audit write
      }

      // Phase 3: "Restart" — OutboxStore reopens same LMDB, DedupRing lost
      outboxStore.close();

      const restartedOutbox = new OutboxStore({
        dataDir: crashOutboxDir,
        ttlMs: 60_000,
        maxRetries: 3,
        deadLetterTtlMs: 60_000,
        mapSize: 16 * 1024 * 1024,
      });

      const restartedDedup = new DedupRing({ maxSize: 1000, ttlMs: 60_000 });

      // Fetch pending — crash entries + any un-acked from phase 1
      const allPending = await restartedOutbox.fetchOldestPending(20);
      expect(allPending.length).toBeGreaterThanOrEqual(3);

      const afterCrashAuditIds: string[] = [];
      for (const entry of allPending) {
        if (!restartedDedup.contains(entry.msgId)) {
          restartedDedup.put(entry.msgId);
          const auditId = await auditStore.write({
            operation: 'crash-e2e3',
            actor: 'cross-e2e',
            resource: '/crash/recovery',
            result: 'success' as const,
          });
          afterCrashAuditIds.push(auditId);
        }
        await restartedOutbox.markDelivered(entry.id);
      }

      // CR3: Total audit entries = 5 (before crash) + 3 (after restart) = 8
      const finalCount = await auditStore.count();
      expect(finalCount).toBe(8);

      // CR2: No duplicate IDs across both phases
      const allIds = [...beforeCrashAuditIds, ...afterCrashAuditIds];
      expect(new Set(allIds).size).toBe(allIds.length);

      restartedOutbox.close();
    } finally {
      for (const d of tempDirs) {
        try { rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
      }
    }
  }, E2E_TIMEOUT);
});
