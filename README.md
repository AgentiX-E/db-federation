# db-federation — Cross-Cluster Data Federation Layer

**@agentix-e/db-federation** is the cross-datacenter (multi-DC) orchestration layer for the Agentix-E database ecosystem. It coordinates multi-leader replication, change data capture (CDC), causal consistency via HLC, conflict resolution, and anti-entropy repair across geographically separated clusters.

## Architecture Overview

db-federation is organized into four phases:

| Phase | Component | Description |
|-------|-----------|-------------|
| **1. Async Replication** | `MultiLeaderReplication`, `OutboxStore` | Outbox-pattern cross-DC write replication with crash-safe durability (LMDB), per-target backoff, and at-least-once delivery |
| **2. Consensus Gate** | `FederationManager` | Top-level orchestrator that ties replication, health monitoring, and consumer registration together. Owns the full lifecycle. |
| **3. CDC & Anti-Entropy** | `CDCEngine`, `CDCManager`, `AntiEntropy` | Change data capture (table watching, publications, subscriptions, webhook dispatch) and Merkle-tree-based reconciliation for gap detection and repair |
| **4. Conflict Resolution** | `ConflictResolver`, `DedupRing` | LWW (last-writer-wins), CRDT counter/set/map merging, custom resolver registration, and in-flight message deduplication |

### Core Design Principles

- **Zero data loss** — every write is durably persisted to LMDB outbox before cross-DC send
- **Causal consistency** — HLC timestamps ensure causally-ordered reads across clusters
- **Storage-engine agnostic** — works with both `sqlvec-db` (SQL/PG-wire) and `graphvec-db` (graph)
- **Bounded backpressure** — topology-aware retry backoff (intra-DC 3s, cross-DC 60s, edge 300s)

## Quick Start

```bash
# Install dependencies
npm install

# Build
npm run build

# Run tests
npm test

# Run a specific test suite
npx vitest run src/replication/DedupRing.test.ts
```

## Module Reference

| Module | File | Description |
|--------|------|-------------|
| `FederationManager` | `src/core/FederationManager.ts` | Top-level orchestrator, owns lifecycle of all sub-systems |
| `HLC` | `src/hlc/HLC.ts` | Hybrid Logical Clock for causal ordering |
| `MultiLeaderReplication` | `src/replication/MultiLeaderReplication.ts` | Core replication engine with per-target outbox workers |
| `OutboxStore` | `src/storage/OutboxStore.ts` | Durable LMDB-backed outbox with dead letter queue |
| `ConflictResolver` | `src/replication/ConflictResolver.ts` | LWW and CRDT conflict resolution strategies |
| `DedupRing` | `src/replication/DedupRing.ts` | In-flight message deduplication with TTL-based ring eviction |
| `CDCEngine` | `src/replication/CDCEngine.ts` | Change data capture engine — table watching and event dispatch |
| `CDCManager` | `src/replication/CDCManager.ts` | Publication/subscription management for CDC |
| `AntiEntropy` | `src/replication/AntiEntropy.ts` | Merkle-tree-based reconciliation for gap detection |
| `HealthMonitor` | `src/monitoring/HealthMonitor.ts` | Periodic health checks and lag tracking |
| `TopologyManager` | `src/topology/TopologyManager.ts` | Topology-aware routing and backoff calculation |
| `InProcessTransport` | `src/transport/InProcessTransport.ts` | In-memory transport for testing and intra-process messaging |
| `TcpSecureTransport` | `src/transport/TcpSecureTransport.ts` | TCP transport with authenticated channels |
| `SqlVecFederationConsumer` | `src/integration/SqlVecFederationConsumer.ts` | SQL database federation adapter |
| `GraphVecFederationConsumer` | `src/integration/GraphVecFederationConsumer.ts` | Graph database federation adapter |

## Key Interfaces

All major components are defined as interfaces in `src/interfaces/`:

| Interface | Description |
|-----------|-------------|
| `IOutboxStore` | Durable outbox contract (add, fetchOldestPending, markDelivered, getStats) |
| `IMultiLeaderReplication` | Replication engine contract (start, stop, replicate, getStatus, retryFailed) |
| `ITransport` | Transport layer contract (send, broadcast, subscribe, start, stop) |
| `ICDCEngine` | Change data capture contract (watchTable, unwatchTable, appendChange) |
| `ICDCManager` | Publication/subscription contract (createPublication, dropPublication, createSubscription, dropSubscription) |
| `IAntiEntropy` | Reconciliation contract (startSync, stopSync, getMerkleRoot, findDiffRanges, sync) |

## Architecture Document

For a detailed walkthrough of the system design, topology, data flow diagrams, and phase-by-phase implementation plan, see [docs/architecture.md](docs/architecture.md).
