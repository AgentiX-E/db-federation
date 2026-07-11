# db-federation — Cross-Cluster Data Federation Layer

**The orchestration layer that unifies geographically distributed database clusters into a single, consistent, fault-tolerant data plane.** db-federation sits above individual cluster consensus (Raft) and enables multi-leader replication, real-time change data capture, causal consistency, and automatic conflict resolution across datacenters — without sacrificing the ACID guarantees of each participating cluster.

**Package**: `@agentix-e/db-federation`

---

## Why db-federation?

Modern applications span multiple datacenters and regions. A single Raft cluster, no matter how well-tuned, has a fundamental physical limitation: the speed of light. db-federation solves three problems that a single consensus group cannot:

| Problem | Impact | db-federation Solution |
|---------|--------|----------------------|
| **Write latency across regions** | Raft commit requires majority; cross-Atlantic RTT is ~80ms, cross-Pacific ~180ms | Async outbox replication — local Raft commit in <5ms, async fan-out to other DCs |
| **Durable cross-DC writes** | Network partitions, packet loss, DC outage — writes can be lost | Outbox-before-send pattern: persisted to LMDB before transmission, at-least-once with dedup |
| **Concurrent writes to same data** | Two leaders in different DCs can accept writes to the same key | Configurable conflict resolution: LWW, CRDT merge, or custom resolvers |
| **Catch-up after partition** | Split network heals, data has diverged | Merkle-tree anti-entropy reconciliation with deterministic conflict resolution |

### What db-federation is NOT

- **Not a global consensus protocol.** Each DC runs its own Raft cluster (via db-cluster for sqlvec-db). db-federation coordinates async replication _between_ clusters.
- **Not a replication of existing Raft logs.** db-federation operates at the application/outbox level, not the Raft log level. This allows storage-engine-agnostic federation.
- **Not a distributed SQL engine.** You don't query "through" db-federation — it's the glue that keeps data synchronized across isolated consensus domains.

---

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                     Your Application                          │
│         (sqlvec-db / graphvec-db / custom clients)           │
├───────────────────────────────────────────────────────────────┤
│                    Federation Manager                         │
│        Top-level orchestrator — lifecycle, wiring            │
├───────────────────┬───────────────────┬───────────────────────┤
│  Multi-Leader     │  CDC & Events     │  Anti-Entropy         │
│  Replication      │                   │  (Reconciliation)     │
│                   │                   │                       │
│  ┌─────────────┐  │  ┌─────────────┐  │  ┌─────────────────┐  │
│  │ OutboxStore │  │  │ CDCEngine   │  │  │ MerkleTree Sync │  │
│  │ (LMDB)      │  │  │ CDCManager  │  │  │ Range Partition │  │
│  │ DedupRing   │  │  │ Webhooks    │  │  │ Gap Repair      │  │
│  └─────────────┘  │  └─────────────┘  │  └─────────────────┘  │
├───────────────────┼───────────────────┼───────────────────────┤
│              Conflict Resolution (LWW / CRDT / Custom)        │
├───────────────────┼───────────────────┼───────────────────────┤
│              Transport Layer (TCP/mTLS / InProcess)           │
│              HLC — Hybrid Logical Clock (causal ordering)     │
├───────────────────┼───────────────────┼───────────────────────┤
│  DC A (db-cluster)│  DC B (db-cluster)│  DC C (db-cluster)   │
└───────────────────┴───────────────────┴───────────────────────┘
```

### Core Design Principles

| Principle | How It's Achieved |
|-----------|-------------------|
| **Storage-engine agnostic** | All integration is via consumer interfaces (`IFederationConsumer`). sqlvec-db and graphvec-db both implement it differently — db-federation doesn't care. |
| **Zero data loss** | Every written value goes to LMDB outbox before network send. Crash recovery restores outbox. At-least-once delivery with deduplication on receive. |
| **Causal consistency** | Every replicated write carries an HLC timestamp. Receiving cluster advances its HLC and applies in HLC order. Read-your-writes within HLC drift ensures causality. |
| **Bounded backpressure** | Topology-aware retry: intra-DC 3s cap, cross-DC 60s cap, edge 300s cap. Outbox freeze on partition prevents unbounded accumulation. |
| **Deterministic conflict resolution** | LWW (by HLC, tiebreak by nodeId) for simple cases. CRDT merge for counters, sets, maps. Custom resolvers for domain-specific logic. |

---

## Modules

### FederationManager
The top-level orchestrator. Owns the lifecycle (start → stop) of all subsystems. Wires transport, replication, CDC, anti-entropy, and consumers together. Single entry point for the application.

```
FederationManager.start()
  ├─ starts Transport layer
  ├─ starts MultiLeaderReplication
  ├─ starts CDCEngine + CDCManager
  ├─ starts AntiEntropy engine
  └─ notifies all registered consumers → federation active

FederationManager.stop()
  ├─ unfreezes outbox (drain remaining)
  ├─ stops AntiEntropy
  ├─ stops CDC
  ├─ stops Replication
  └─ stops Transport
```

### MultiLeaderReplication
The core replication engine. Manages per-target outbox workers that poll a durable LMDB queue, batch entries, send via transport, and track delivery progress.

**State machine of an outbox entry:**

```
PENDING ──→ IN_FLIGHT ──→ DELIVERED ──→ CLEANUP (after TTL)
    ▲                       │
    │                       └── ACK received
    │── NAK / timeout ──────┘
    │── retryCount ≥ max ──→ DEAD_LETTER (manual retry → PENDING)
```

**Retry backoff per topology:**

| Topology | Initial | Ceiling | Doubles? |
|----------|---------|---------|----------|
| intra-DC | 100ms | 3s | ×2 |
| cross-DC | 1s | 60s | ×2 |
| edge | 5s | 300s | ×2 |

### OutboxStore
Durable LMDB-backed queue. Each entry is persisted before `replicate()` returns. Supports per-peer pending queues, ACK tracking (delete on ACK), and dead letter escalation.

```
OutboxStore
  ├─ add(entry) → msgId              // persist before send
  ├─ fetchOldestPending(peerId, limit) → entries[]
  ├─ markDelivered(msgId)             // remove from queue
  ├─ markFailed(msgId)                // increment retryCount
  ├─ getDeadLetterEntries() → entries[]
  └─ retryDeadLetter(msgId)           // back to PENDING
```

### CDC (Change Data Capture)
Two-tier design: `CDCEngine` watches database tables and fires raw change events; `CDCManager` manages publications, subscriptions, and webhook dispatch.

```
Application writes to "orders" table
  │
  CDCEngine.onChange → raw change event { table, op, old, new, hlc }
    │
    CDCManager.matchPublications → if matches subscription
      │
      ├─→ WebhookDelivery: POST to external endpoint
      └─→ OutboxForwarder: re-enter outbox for further cross-DC replication
```

### AntiEntropy (Reconciliation)
Periodic Merkle-tree sync that detects and repairs data divergence. Triggered on a configurable interval (default 30s) and on network heal after partition.

```
AntiEntropy.syncWith(peer)
  ├─ build Merkle tree from local data
  ├─ send root hash to peer
  ├─ compare → if equal, done
  ├─ if different → walk tree to find differing ranges
  └─ exchange missing entries → resolve conflicts
```

### ConflictResolver
Pluggable conflict resolution. Each table or key space can have its own strategy.

| Strategy | Mechanism | Use Case |
|----------|-----------|----------|
| LWW | Select write with larger (HLC timestamp, nodeId). Simple, deterministic. | General purpose; last write wins. |
| CRDT Counter | PN-counter: concurrent increments → sum. Each node tracks its own delta. | Voting, view counts, inventory deltas. |
| CRDT Set | OR-Set: concurrent add/remove → observed-remove semantics wins. | Tag systems, feature flags, user groups. |
| CRDT Map | Recursive merge of CRDT counters/sets per field. | Document fields with concurrent edits. |
| Custom | User-provided `(a, b) → resolved` callback. Registers per table. | Domain-specific business logic. |

### HLC (Hybrid Logical Clock)
Pure implementation with no external dependencies. Provides causal ordering across all participants without requiring synchronized wall clocks.

```typescript
const hlc = new HLC(wallClock);
const ts1 = hlc.now();                          // wall=100, logical=0
const ts2 = hlc.now();                          // wall=100, logical=1
hlc.observe(remoteTimestamp);                    // advances past remote
const serialized = hlcToKey(ts1);               // lexicographically sortable hex
const deserialized = keyToHlc(serialized);      // round-trip
assert(hlc.compare(ts2, ts1) > 0);              // ts2 happened after ts1
```

### Transport Layer
Pluggable transport abstracted behind `ITransport`. Two implementations:

| Implementation | Encryption | Use Case |
|---------------|-----------|----------|
| `TcpSecureTransport` | TLS/mTLS with client certificates | Production cross-DC |
| `InProcessTransport` | None (shared memory) | Single-process testing, CI |

Both share the same interface. Messages carry `targetPeerId` (or `broadcast` flag), `payload`, and metadata (HLC, msgId, retryCount).

---

## Data Flows

### Cross-DC Write (normal)

```
Client writes to DC-A
  │
  ▼
DC-A Raft Leader: Raft commit (local, <5ms)
  │
  ├─ apply to local state machine ── client sees success
  │
  └─ OutboxStore.append({ payload, targetDC: 'dc-b', hlc })
      │
      Worker polls outbox (~100ms tick)
      │
      TCPSecureTransport.send(msg) → DC-B
        │
        DC-B receiver:
          ├─ DedupRing.check(msgId) → skip if duplicate
          ├─ HLC.observe(remote hlc)
          ├─ ConflictResolver.resolve(if concurrent)
          └─ propose as Raft entry to DC-B cluster
```

### Partition Recovery

```
Network between DC-A and DC-B drops
  │
  ├─ DC-A: OutboxWorker detects send failure, enters backoff
  │         Outbox entries accumulate in LMDB (durable)
  │
  ├─ DC-B: OutboxWorker same for reverse direction
  │
  ├─ Both DCs continue accepting writes independently
  │
Network heals
  │
  ├─ OutboxWorker resumes, flushes backlog
  │
  ├─ AntiEntropy.syncAll()
  │     ├─ Merkle tree detects divergence
  │     └─ ConflictResolver handles concurrent writes
  │
  └─ Both DCs converge to consistent state
```

---

## Quick Start

```bash
npm install @agentix-e/db-federation
```

### Single-process demo (2 simulated DCs with InProcessTransport)

```typescript
import { FederationManager, InProcessTransport } from '@agentix-e/db-federation';

// Create two federation instances connected via in-process transport
const transportAB = new InProcessTransport('dc-a');
const transportBA = new InProcessTransport('dc-b');
transportAB.linkTo(transportBA);

const fmA = new FederationManager({
  dc: 'dc-a',
  transport: transportAB,
  consumers: [sqlVecConsumerA], // your IFederationConsumer impl
});

const fmB = new FederationManager({
  dc: 'dc-b',
  transport: transportBA,
  consumers: [sqlVecConsumerB],
});

await fmA.start();
await fmB.start();

// Writes to DC-A will be asynchronously replicated to DC-B
fmA.getReplication().replicate('shard-0', '{"balance":100}', 'update');

await fmA.stop();
await fmB.stop();
```

### Production (TcpSecureTransport with mTLS)

```typescript
import { FederationManager, TcpSecureTransport } from '@agentix-e/db-federation';

const transport = new TcpSecureTransport({
  localCert: fs.readFileSync('/etc/certs/dc-a.pem'),
  localKey: fs.readFileSync('/etc/certs/dc-a-key.pem'),
  ca: fs.readFileSync('/etc/certs/ca.pem'),
  listenPort: 7443,
  peers: [{ id: 'dc-b', host: '10.0.2.10', port: 7443 }],
});

const fm = new FederationManager({
  dc: 'dc-a',
  transport,
  consumers: [sqlVecConsumer],
});

await fm.start();
```

---

## Integration with sqlvec-db

`SqlVecFederationConsumer` bridges db-federation to a sqlvec-db cluster:

```typescript
const fm = new FederationManager({
  dc: 'dc-west',
  transport: new TcpSecureTransport({ ... }),
  consumers: [
    new SqlVecFederationConsumer(sqlVecRaftGroup, outboxManager, hlc),
  ],
});
```

The adapter hooks into the existing `OutboxManager` and proposes replicated entries back as Raft entries. See the [SqlVecFederationConsumer source](./src/integration/SqlVecFederationConsumer.ts).

## Integration with graphvec-db

`GraphVecFederationConsumer` bridges db-federation to a graphvec-db instance:

```typescript
const fm = new FederationManager({
  dc: 'dc-east',
  transport: new TcpSecureTransport({ ... }),
  consumers: [
    new GraphVecFederationConsumer(graphVecConsensusProvider, graphVecHlc),
  ],
});
```

The adapter wraps graphvec-db's `ConsensusProvider` interface and HLC clock. See the [GraphVecFederationConsumer source](./src/integration/GraphVecFederationConsumer.ts).

---

## Configuration Reference

```typescript
interface FederationConfig {
  /** DC identifier (used for HLC node ID, routing) */
  dc: string;
  /** Transport instance (TCP/mTLS or in-process) */
  transport: ITransport;
  /** Registered downstream consumers */
  consumers: IFederationConsumer[];
  /** Replication configuration */
  replication?: {
    tickMs?: number;        // default 100 — outbox poll interval
    maxBatchSize?: number;  // default 100 — max entries per send
    maxBackoffSeconds?: {   // per topology
      intraDC?: number;     // default 3
      crossDC?: number;     // default 60
      edge?: number;        // default 300
    };
  };
  /** CDC configuration */
  cdc?: {
    enabled: boolean;       // default false
    webhookTimeoutMs?: number; // default 5000
  };
  /** Anti-entropy configuration */
  antiEntropy?: {
    enabled: boolean;       // default true
    intervalSeconds?: number; // default 30
    merkleDepth?: number;   // default 8
  };
}
```

---

## Testing

```bash
# All tests
npm test

# Specific module
npx vitest run src/transport/InProcessTransport.test.ts
npx vitest run src/replication/MultiLeaderReplication.test.ts

# Integration tests (requires running db-cluster nodes)
npm run test:integration
```

**Current test coverage**: 275 tests across 16 test files, covering all modules and acceptance criteria (AC 40–46).

---

## License

MIT — see [LICENSE](./LICENSE)
