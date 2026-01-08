# Phase 8: Query Optimization & Indexing - Implementation Plan

## Executive Summary

**Goal**: Solve the "Full Table Scan" bottleneck identified in benchmarks.

**Problem**: Currently, a query like `MATCH (n:Person) WHERE n.id = 123` iterates through *all* 10,000 Person nodes, deserializes their properties, and checks the ID. This results in ~6 QPS (160ms latency).

**Solution**: Implement **Property Indices** (B-Tree/Hash) and a **Cost-Based Optimizer (CBO)** that automatically selects the index when appropriate.

**Target**: Improve property lookup performance from **6 QPS** to **>10,000 QPS** (<0.1ms).

## Requirements Coverage

| Requirement | Description |
|-------------|-------------|
| **REQ-OPT-001** | Support **Property Indices** (Create/Drop Index) |
| **REQ-OPT-002** | Implement **IndexScanOperator** for O(log n) lookups |
| **REQ-OPT-003** | Collect **Table Statistics** (Node counts, selectivity) |
| **REQ-OPT-004** | Implement **Cost-Based Optimizer** to choose plans |
| **REQ-OPT-005** | Support `EXPLAIN` to view query plans |

## Module Structure

```
src/
├── index/                      # NEW Module
│   ├── mod.rs
│   ├── property_index.rs       # BTreeMap wrapper
│   └── manager.rs              # IndexManager (similar to VectorIndexManager)
├── query/
│   ├── optimizer/              # NEW Module
│   │   ├── mod.rs
│   │   ├── cost.rs             # Cost model
│   │   ├── rules.rs            # Rewrite rules (e.g., Scan -> Index)
│   │   └── statistics.rs       # Data stats
│   ├── executor/
│       ├── operator.rs         # Update: Add IndexScanOperator
```

## Implementation Roadmap (4 Weeks)

### Week 1: Property Indices
**Goal**: Create the underlying index structures.

1.  **Index Structure**: Use `BTreeMap<(Label, PropertyValue), NodeId>` for range queries (supports `=`, `>`, `<`).
2.  **Index Manager**: Map `(Label, PropertyKey)` to the BTreeMap.
3.  **Sync**: Update `GraphStore` hooks (`create_node`, `set_property`) to update these indices (just like Vector indices).

### Week 2: Index Scan Operator
**Goal**: Execute queries using the index.

1.  **Operator**: Implement `IndexScanOperator`.
    *   Input: Label, Property, Value/Range.
    *   Output: Iterator of `Record` (Node IDs).
2.  **Planner**: Manually force the planner to use `IndexScan` to verify functionality.

### Week 3: Cost-Based Optimizer (CBO)
**Goal**: Automatically choose the best path.

1.  **Statistics**: Maintain counts of nodes per label and maybe histograms.
2.  **Cost Model**:
    *   Cost(Scan) = N * Cost_Read
    *   Cost(Index) = log(N) * Cost_Read + Cost_Random_Access
3.  **Rule Engine**:
    *   If `WHERE` clause matches an Index -> Replace `NodeScan + Filter` with `IndexScan`.

### Week 4: DDL & Explain
**Goal**: User control.

1.  **DDL**: `CREATE INDEX ON :Person(id)`
2.  **EXPLAIN**: Implement `EXPLAIN MATCH ...` which returns the plan string instead of executing.
3.  **Benchmarks**: Re-run `full_benchmark.rs` to validate 1000x speedup.

## API Design

### Cypher API
```cypher
// Create index
CREATE INDEX ON :Person(id)

// Query (automatically optimized)
MATCH (n:Person) WHERE n.id = 123 RETURN n

// Analyze
EXPLAIN MATCH (n:Person) WHERE n.id = 123 RETURN n
// Output: "IndexScan(Person, id) -> Project"
```

## Team & Resources
- **Engineers**: 1 Backend / Database Engineer.
- **Duration**: 4 Weeks.

---
**Status**: Planned
**Version**: 1.0
