# ADR-014: EXPLAIN and PROFILE Query Plan Visualization

## Status
**Accepted**

## Date
2026-02-16

## Context

As Samyama's query engine grows in complexity (multiple operator types, filter pushdown, join strategies), users need visibility into how their queries are executed. Without this, query optimization is a guessing game.

Key pain points observed:
1. **No plan visibility**: Users cannot see which operators are used or in what order
2. **No performance insight**: No way to identify bottlenecks in multi-hop traversals
3. **Debugging difficulty**: When a query returns unexpected results, there is no way to inspect the execution path
4. **Optimization guidance**: Users cannot determine if adding an index or restructuring a query would help

Other graph databases provide this capability:
- **Neo4j**: `EXPLAIN` (plan without execution) and `PROFILE` (plan with runtime stats)
- **Memgraph**: `EXPLAIN` with cost estimates
- **TigerGraph**: `EXPLAIN` with distributed execution plans

## Decision

**We will add EXPLAIN and PROFILE query prefixes that display the execution plan as a formatted operator tree.**

### EXPLAIN: Plan Without Execution

`EXPLAIN` parses and plans the query but does not execute it. Returns the operator tree with estimated row counts:

```cypher
EXPLAIN MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 30 RETURN m.name
```

Output:
```
+----------------------------------+----------------+
| Operator                         | Estimated Rows |
+----------------------------------+----------------+
| ProjectOperator (m.name)         |             50 |
|   FilterOperator (n.age > 30)    |             50 |
|     ExpandOperator (-[:KNOWS]->) |            500 |
|       NodeScanOperator (:Person) |            100 |
+----------------------------------+----------------+
```

### PROFILE: Plan With Runtime Statistics

`PROFILE` executes the query and collects per-operator statistics:

```cypher
PROFILE MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.age > 30 RETURN m.name
```

Output:
```
+----------------------------------+----------------+-------------+-----------+
| Operator                         | Estimated Rows | Actual Rows | Time (ms) |
+----------------------------------+----------------+-------------+-----------+
| ProjectOperator (m.name)         |             50 |          47 |      0.12 |
|   FilterOperator (n.age > 30)    |             50 |          47 |      0.35 |
|     ExpandOperator (-[:KNOWS]->) |            500 |         312 |      1.80 |
|       NodeScanOperator (:Person) |            100 |         100 |      0.45 |
+----------------------------------+----------------+-------------+-----------+

Total rows returned: 47
Total execution time: 2.72 ms
```

### Architecture

#### Grammar Extension

Add `EXPLAIN` and `PROFILE` as query prefixes in `cypher.pest`:

```pest
query = { SOI ~ query_prefix? ~ query_body ~ EOI }
query_prefix = @{ (^"EXPLAIN" | ^"PROFILE") ~ !(ASCII_ALPHANUMERIC | "_") }
query_body = { clause+ }
```

#### Execution Flow

```
                    ┌─────────────┐
                    │  Parse Query │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ Build Plan   │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
        ┌─────▼─────┐ ┌───▼───┐ ┌─────▼─────┐
        │  EXPLAIN   │ │ Normal│ │  PROFILE   │
        │ Format plan│ │Execute│ │ Wrap ops   │
        │ Return text│ │       │ │ w/ timing  │
        └───────────┘ └───┬───┘ │ Execute    │
                          │     │ Return     │
                          │     │ plan+stats │
                          │     └─────┬─────┘
                          │           │
                    ┌─────▼───────────▼─────┐
                    │    Return Results      │
                    └───────────────────────┘
```

#### Profiling Wrapper Operator

For `PROFILE`, each operator is wrapped in a `ProfileOperator` that collects timing and row counts:

```rust
struct ProfileOperator {
    inner: Box<dyn PhysicalOperator>,
    operator_name: String,
    rows_produced: usize,
    elapsed: Duration,
}

impl PhysicalOperator for ProfileOperator {
    fn next(&mut self) -> Option<Record> {
        let start = Instant::now();
        let result = self.inner.next();
        self.elapsed += start.elapsed();
        if result.is_some() {
            self.rows_produced += 1;
        }
        result
    }
}
```

#### Row Estimation

Initial row estimates use simple heuristics:

| Operator | Estimation Rule |
|----------|----------------|
| `NodeScanOperator` | Count of nodes with matching label |
| `ExpandOperator` | Input rows * average edge degree |
| `FilterOperator` | Input rows * default selectivity (0.5) |
| `ProjectOperator` | Same as input rows |
| `LimitOperator` | min(input estimate, limit value) |
| `JoinOperator` | left rows * right rows * join selectivity |

These heuristics provide a starting point. A cost-based optimizer with histogram statistics can refine them in a future phase.

#### RESP Protocol Integration

Both `EXPLAIN` and `PROFILE` are accessible via the RESP protocol:

```
GRAPH.QUERY mygraph "EXPLAIN MATCH (n:Person) RETURN n"
GRAPH.QUERY mygraph "PROFILE MATCH (n:Person) RETURN n"
```

The plan output is returned as a RESP bulk string.

## Consequences

### Positive

- Users can inspect and understand query execution plans before running expensive queries
- Per-operator timing in `PROFILE` enables precise bottleneck identification
- Foundation for a future cost-based query optimizer (row estimates can be compared to actuals)
- Consistent with Neo4j/Memgraph conventions, reducing learning curve for users migrating from those systems
- `EXPLAIN` has zero execution cost -- safe to use on production queries

### Negative

- `PROFILE` adds timing overhead per operator (~5-10% for small queries, negligible for large ones)
- Row estimation heuristics may be inaccurate without statistics collection
- Plan output format adds string formatting code to the query engine
- New grammar keywords (`EXPLAIN`, `PROFILE`) must follow atomic keyword rules (ADR-013)

### Neutral

- `EXPLAIN` output format may evolve as the optimizer grows more sophisticated
- JSON output format for programmatic consumption can be added later alongside the text format

## Alternatives Considered

### Alternative 1: Logging-Only Approach

Log query plans and timing to the server log at DEBUG level.

**Rejected because**:
- Not interactive -- users must access server logs
- Cannot be used from RESP clients
- Mixes with other log output, hard to correlate
- Cannot selectively profile individual queries

### Alternative 2: Graphical Plan Visualization

Return plan as a DOT graph or SVG for visual rendering.

**Rejected because**:
- Too complex for the current phase
- Requires a frontend or external tool to render
- Text-based output works well in CLI and RESP clients
- Can be added as an additional output format later

### Alternative 3: Separate PLAN Command

Add a new `GRAPH.PLAN` command instead of prefixing queries with EXPLAIN/PROFILE.

**Rejected because**:
- Inconsistent with Neo4j/Memgraph conventions
- Requires users to learn a non-standard interface
- Query prefix is more ergonomic (modify the query, not the command)

### Alternative 4: Always Collect Profiling Data

Collect timing for every query and expose it via a separate stats endpoint.

**Rejected because**:
- Adds overhead to every query, not just those being debugged
- Memory overhead for storing per-query statistics
- Profiling should be opt-in for production workloads

## Related Decisions

- [ADR-007: Volcano Iterator Model](./ADR-007-volcano-iterator-execution.md) - EXPLAIN/PROFILE visualizes the Volcano operator tree
- [ADR-012: Late Materialization](./ADR-012-late-materialization.md) - Profile stats show materialization costs at ProjectOperator
- [ADR-013: PEG Grammar Atomic Keywords](./ADR-013-peg-grammar-atomic-keywords.md) - EXPLAIN/PROFILE keywords use atomic rules with word boundary checks
- [ADR-003: RESP Protocol](./ADR-003-use-resp-protocol.md) - Plan output delivered via RESP bulk strings

## References

- [Neo4j EXPLAIN/PROFILE Documentation](https://neo4j.com/docs/cypher-manual/current/query-tuning/)
- [Volcano Iterator Model - Goetz Graefe](https://doi.org/10.1109/69.273032)
- [Query Optimization in Graph Databases](https://arxiv.org/abs/2104.01265)

---

**Last Updated**: 2026-02-16
**Status**: Accepted
