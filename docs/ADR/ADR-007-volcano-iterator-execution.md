# ADR-007: Use Volcano Iterator Model for Query Execution

## Status
**Accepted**

## Date
2025-10-14

## Context

We need a query execution model for OpenCypher queries that:

1. **Composable**: Build complex plans from simple operators
2. **Efficient**: Lazy evaluation, minimal memory
3. **Debuggable**: Easy to reason about execution
4. **Standard**: Well-understood pattern

## Decision

**We will use the Volcano Iterator Model (also called "Pipeline Model") for query execution.**

### Architecture

```mermaid
graph TB
    PROJ[ProjectOperator<br/>SELECT b.name]
    FILT[FilterOperator<br/>WHERE a.age > 30]
    EXP[ExpandOperator<br/>-[:KNOWS]->]
    SCAN[NodeScanOperator<br/>MATCH :Person]

    PROJ -->|next()| FILT
    FILT -->|next()| EXP
    EXP -->|next()| SCAN

    style PROJ fill:#e3f2fd
    style FILT fill:#f3e5f5
    style EXP fill:#fff3e0
    style SCAN fill:#e8f5e9
```

### Iterator Protocol

```rust
trait PhysicalOperator {
    fn next(&mut self) -> Option<Record>;
    fn reset(&mut self);
}

struct FilterOperator {
    input: Box<dyn PhysicalOperator>,
    predicate: Predicate,
}

impl PhysicalOperator for FilterOperator {
    fn next(&mut self) -> Option<Record> {
        while let Some(record) = self.input.next() {
            if self.predicate.eval(&record) {
                return Some(record);
            }
        }
        None
    }
}
```

## Rationale

### Benefits

✅ **Lazy Evaluation**: Process one row at a time
- No need to materialize entire result set
- Memory-efficient for large graphs

✅ **Composability**: Chain operators like Lego blocks
```
Project → Filter → Expand → Scan
```

✅ **Pipelining**: Results flow through pipeline
- First result returned quickly
- Good for `LIMIT` queries

## Consequences

✅ **Standard Pattern**: Used by PostgreSQL, MySQL, SQL Server
✅ **Easy Optimization**: Operator reordering, filter pushdown
✅ **Memory Efficient**: O(1) memory per operator

⚠️ **Not Ideal for Joins**: Nested loop joins can be slow
- Mitigation: Use hash joins for large joins (Phase 2)

## Alternatives Considered

- **Vectorized Execution**: Process batches (used by ClickHouse)
  - Better for analytics, overkill for OLTP
- **Compilation** (JIT): Compile query to machine code
  - Complex, months of work

**Verdict**: Volcano is proven and sufficient for Phase 1.

---

**Last Updated**: 2025-10-14
**Status**: Accepted and Implemented
