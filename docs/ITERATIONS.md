# Iteration Index

| I | Topic | Repository | Tests | Coverage | Commit |
|:--|------|------|:--:|:--:|--------|
| I1-I7 | Foundation (Raft, outbox, pipeline, 20 packages) | all | ~1800 | ≥95% | — |
| I8 | Coverage boost + LTS flaky fix | db-cluster | 654 | ≥95% | `5050154` |
| I9 | Cross-repo stress resilience | db-federation | 13 new | ≥95% | `54eaf6e` |
| I10 | Unified CI/CD with dependency-gated triggers | db-federation | — | — | `8e58b9a` |
| I11 | Heavy-load performance & stress testing | db-federation | 6 new | ≥95% | `3bc15cb` |
| I12 | Security audit pipeline (3 repos) | all | — | — | `dc2b9cb` |
| I13 | Observability stack (logs + metrics + health + trace) | db-cluster + db-operations | 58 new | ≥95% | `e2707f8` / `ccc1a68` |
| I14 | Documentation system (ADRs, API ref, architecture) | all | — | — | `c3f8155` / `149e9ea` / `d7ca24d` |

## Test Totals

| Repository | Tests | Coverage |
|------------|:--:|:--:|
| db-cluster | 654 | ≥95% |
| db-federation | 434+ | ≥95% |
| db-operations | 1126 | ≥95% |
| **Total** | **~2214** | **≥95%** |

## ADR Inventory

| Repository | ADRs |
|------------|------|
| db-cluster | 001-raft-core, 002-transport, 003-snapshots, 004-joint-consensus, 005-leader-fencing, 006-observability |
| db-federation | 001-outbox-before-send, 002-exactly-once-dedup, 003-cross-repo-sync, 004-federation-pipeline |
| db-operations | 001-interface-first |

## Key Documents

- [Architecture Panorama](cross-repo-architecture.md) — Cross-repo topology, data flow, module map
- [db-cluster API Reference](../db-cluster/docs/api-reference.md)
- [db-federation API Reference](api-reference.md)
- [I10 CI/CD Pipeline](I10-CROSS-REPO-CI.md)
- [I12 Security Audit](I12-SECURITY-AUDIT.md)
- [I13 Observability](I13-OBSERVABILITY.md)
