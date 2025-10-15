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
| [001](./ADR-001-use-rust-as-primary-language.md) | Use Rust as Primary Programming Language | Accepted | 2025-10-14 |
| [002](./ADR-002-use-rocksdb-for-persistence.md) | Use RocksDB for Persistence Layer | Accepted | 2025-10-14 |
| [003](./ADR-003-use-resp-protocol.md) | Use RESP Protocol for Network Communication | Accepted | 2025-10-14 |
| [004](./ADR-004-use-raft-consensus.md) | Use Raft Consensus for Distributed Coordination | Accepted | 2025-10-14 |
| [005](./ADR-005-use-capnproto-serialization.md) | Use Cap'n Proto for Zero-Copy Serialization | Accepted | 2025-10-14 |
| [006](./ADR-006-use-tokio-async-runtime.md) | Use Tokio as Async Runtime | Accepted | 2025-10-14 |
| [007](./ADR-007-volcano-iterator-execution.md) | Use Volcano Iterator Model for Query Execution | Accepted | 2025-10-14 |
| [008](./ADR-008-multi-tenancy-namespace-isolation.md) | Use Namespace Isolation for Multi-Tenancy | Accepted | 2025-10-14 |
| [009](./ADR-009-graph-partitioning-strategy.md) | Graph-Aware Partitioning for Distributed Mode | Proposed | 2025-10-14 |
| [010](./ADR-010-observability-stack.md) | Use Prometheus + OpenTelemetry for Observability | Accepted | 2025-10-14 |

## Decision Process

1. **Propose**: ADR drafted and reviewed by team
2. **Accept**: ADR approved and implemented
3. **Deprecate**: Decision no longer recommended but still in use
4. **Supersede**: Decision replaced by a newer ADR

---

**Maintained by**: Samyama Graph Database Team
**Last Updated**: 2025-10-14
