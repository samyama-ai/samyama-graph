# Architecture Decision Records (ADR)

## About ADRs

Architecture Decision Records document significant architectural decisions made during the development of Samyama Graph Database. Each ADR captures the context, decision, consequences, and alternatives considered.

## ADR Template

```markdown
# ADR-XXX: Title

## Status
[Proposed | Accepted | Deprecated | Superseded]

## Date
YYYY-MM-DD

## Context
What is the issue we're facing? What factors are at play?

## Decision
What decision did we make?

## Consequences
What becomes easier or more difficult because of this decision?

## Alternatives Considered
What other options did we evaluate?

## Related Decisions
Links to related ADRs
```

## Index of ADRs

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [001](./ADR-001-use-rust-as-primary-language.md) | Use Rust as Primary Programming Language | Accepted and Implemented | 2025-10-14 |
| [002](./ADR-002-use-rocksdb-for-persistence.md) | Use RocksDB for Persistence Layer | Accepted and Implemented | 2025-10-14 |
| [003](./ADR-003-use-resp-protocol.md) | Use RESP Protocol for Network Communication | Accepted and Implemented | 2025-10-14 |
| [004](./ADR-004-use-raft-consensus.md) | Use Raft Consensus for Distributed Coordination | Accepted (Phase 3+) | 2025-10-14 |
| [005](./ADR-005-use-capnproto-serialization.md) | Use Cap'n Proto for Zero-Copy Serialization | **Superseded** | 2025-10-14 |
| [006](./ADR-006-use-tokio-async-runtime.md) | Use Tokio as Async Runtime | Accepted | 2025-10-14 |
| [007](./ADR-007-volcano-iterator-execution.md) | Use Volcano Iterator Model for Query Execution | Accepted and Implemented | 2025-10-14 |
| [008](./ADR-008-multi-tenancy-namespace-isolation.md) | Use Namespace Isolation for Multi-Tenancy | Accepted and Implemented | 2025-10-14 |
| [009](./ADR-009-graph-partitioning-strategy.md) | Graph-Aware Partitioning for Distributed Mode | Proposed (Phase 4+, stub only) | 2025-10-14 |
| [010](./ADR-010-observability-stack.md) | Use Prometheus + OpenTelemetry for Observability | Accepted (Enterprise only) | 2025-10-14 |
| [011](./ADR-011-cypher-crud-operations.md) | Implement Cypher CRUD Operations (DELETE, SET, REMOVE) | Implemented | 2025-12-27 |
| [012](./ADR-012-late-materialization.md) | Late Materialization with NodeRef/EdgeRef | Accepted and Implemented | 2025-12-15 |
| [013](./ADR-013-peg-grammar-atomic-keywords.md) | PEG Grammar with Atomic Keyword Rules | Accepted and Implemented | 2025-12-20 |
| [014](./ADR-014-explain-profile-queries.md) | EXPLAIN and PROFILE Query Plan Visualization | Accepted | 2026-02-16 |
| [015](./ADR-015-graph-native-query-planning.md) | Graph-Native Query Planning | Implemented | 2026-03-06 |
| [016](./ADR-016-billion-node-distributed-architecture.md) | Billion-Node Distributed Architecture | Proposed | 2026-03-23 |
| [017](./ADR-017-adjacency-aware-aggregation-planning.md) | Adjacency-Aware Aggregation Planning | Shipped (v1.0.0) | 2026-04-13 |
| [020](./ADR-020-mvcc-transaction-isolation.md) | MVCC Transaction Isolation (RC + SI) | Proposed | 2026-05-05 |
| [021](./ADR-021-columnar-property-store.md) | Columnar Property Store | Shipped (v1.0.0) | 2026-05-05 |
| [022](./ADR-022-snapshot-format.md) | Snapshot Format (`.sgsnap`) | Shipped (v1.0.0) | 2026-05-05 |
| [023](./ADR-023-wal-versioning.md) | WAL Versioning, Recovery, and Double-WAL Reconciliation | Partially Shipped (v1.0.0) | 2026-05-05 |
| [024](./ADR-024-edge-arena-removal.md) | Edge Arena Removal (DS-07c) | Shipped (v1.0.0) | 2026-05-05 |
| [025](./ADR-025-gpu-compute-wgsl.md) | GPU Compute — `samyama-gpu` crate, WGSL by default, CUDA opt-in | Shipped (v1.0.0) | 2026-05-05 |
| [026](./ADR-026-rao-optimization-crate.md) | Rao-Family Optimization Crate (`samyama-optimization`) | Shipped (v1.0.0) | 2026-05-05 |
| [027](./ADR-027-aggregation-with-pushdown.md) | Aggregation Push-Down and WITH Push-Down | Shipped (v1.0.0) | 2026-05-05 |
| [028](./ADR-028-label-interning.md) | Label and Property-Key Interning | Shipped (v1.0.0) | 2026-05-05 |
| [029](./ADR-029-index-manager.md) | Index Manager — Property, Unique, Composite Indexes | Shipped (v1.0.0) | 2026-05-05 |
| [030](./ADR-030-bandwidth-and-observability.md) | Bandwidth Accounting and Operator Observability | Proposed | 2026-05-05 |
| [034](./ADR-034-unified-memory-seam.md) | Unified-memory buffer seam for zero-ETL CPU/GPU graph sharing | Proposed / scaffolded | 2026-07-22 |
| [035](./ADR-035-multi-hop-query-optimization.md) | Multi-Hop Query Optimization † | Proposed | 2026-07-31 |
| [035](./ADR-035-oeh-hierarchy-index.md) | OEH — Order-Embedded Hierarchy Index (Subsumption + Index-Resident Roll-up) † | Partially shipped (v0.9.x) | 2026-08-13 |
| [036](./ADR-036-data-load-time-to-ready-optimization.md) | Data Load and Time-to-Ready Optimization | Proposed | 2026-07-31 |
| [037](./ADR-037-bolt-protocol-feasibility.md) | Bolt Protocol — Feasibility and Decision | Accepted, with a correction | 2026-08-30 |
| [038](./ADR-038-memory-allocator.md) | Memory Allocator for the Server Binary | Accepted | 2026-09-17 |
| [039](./ADR-039-streaming-query-results.md) | Streaming query results | Proposed | 2026-09-22 |

> **† Two records share the number 035.** `ADR-035-multi-hop-query-optimization.md`
> and `ADR-035-oeh-hierarchy-index.md` were both filed as ADR-035; both are listed
> above. Other documents citing "ADR-035" (for example `benchmarks/hier/README.md`)
> mean the OEH one. **This is unresolved** — which record keeps 035, and which is
> renumbered, is a maintainer decision (#1515). No ADR content has been changed.

> **Number gaps.** 018, 019, 031, 032 and 033 were never used. 35 records are on
> disk and all 35 are indexed above.

## Decision Process

1. **Propose**: ADR drafted and reviewed by team
2. **Accept**: ADR approved and implemented
3. **Deprecate**: Decision no longer recommended but still in use
4. **Supersede**: Decision replaced by a newer ADR

---

**Maintained by**: Samyama Graph Database Team
**Last Updated**: 2026-09-28
