# ADR-015: Graph-Native Query Planning

## Status
**Accepted**

## Date
2026-03-06

## Context

Samyama's query planner (v0.6.0) is a greedy single-plan generator. It always starts from the leftmost node in the MATCH pattern, always follows edge direction as written in the query, and never compares alternative plans. The existing optimization roadmap (see `query-execution-model-analysis.md`) frames improvements in relational terms: DP join enumeration, multiple join strategies (hash vs merge vs nested-loop), histogram-based selectivity estimation.

**This is the wrong model for a graph database.** The "join" in a graph database is the adjacency list lookup — it is inherently an index-nested-loop join. The dominant cost factors are:

1. **Which node to start from** — scanning 100 Company nodes vs 1M Person nodes is a 10,000x difference
2. **Which direction to traverse** — a Company may have 1,100 incoming WORKS_AT edges but a Person has 1 outgoing WORKS_AT edge
3. **Whether both endpoints are bound** — when both ends of an edge are already known, checking edge existence is O(min_degree), not O(degree)

These graph-native concerns dominate query cost by orders of magnitude. Relational concerns (join strategy selection, histograms) are secondary.

### Current Planner Limitations

| Problem | Impact |
|---------|--------|
| Always starts from leftmost node | 10-10,000x cost for asymmetric patterns |
| AST direction = execution direction | Cannot exploit lower-degree traversal direction |
| No Expand-Into when both endpoints bound | O(degree) instead of O(min_degree) |
| Multi-path MATCH → hash join | Unnecessary materialization when patterns share variables |
| Global avg_out_degree only | Cannot distinguish (:Person, :KNOWS, :Person) from (:Person, :WORKS_AT, :Company) |
| `GraphStatistics` computed but unused | Statistics displayed in EXPLAIN but not used for plan decisions |

### Industry Context

All major graph databases implement graph-native planning:
- **Neo4j**: IDP (Iterative Dynamic Programming) with starting point optimization, direction reversal, Expand-Into, and WCOJ (since 5.0)
- **Memgraph**: Greedy planner with starting point selection and direction reversal
- **TigerGraph**: Cost-based planner with triple-level statistics

## Decision

**We will replace the greedy single-plan planner with a graph-native plan enumeration pipeline that selects the optimal starting point, traversal direction, and operator placement using triple-level statistics.**

The new planner architecture has five stages:

```
AST → Pattern Analyzer → Plan Enumerator → Cost Estimator → Logical Optimizer → Physical Planner → ExecutionPlan
```

### Key Components

1. **GraphCatalog** — triple-level statistics `(:Label, :TYPE, :Label)` with avg out-degree, avg in-degree, count, and degree percentiles. Maintained incrementally on edge create/delete.

2. **Plan Enumeration** — for each node in the pattern, generate a candidate plan starting from that node. For each edge, consider both forward and reverse traversal. Score each candidate by estimated intermediate rows (multiplicative cost model).

3. **ExpandIntoOperator** — when both endpoints of an edge are already bound, check edge existence via `store.edges_between()` using O(min_degree) scan instead of enumerating all neighbors.

4. **Logical Plan IR** — a `LogicalPlanNode` enum separating plan structure from physical operators. The logical optimizer applies rewrite rules (predicate placement, ExpandInto insertion) before conversion to physical operators.

5. **Feature-flagged rollout** — the old planner remains default until the new planner is proven equivalent on the full test suite and benchmarked superior.

### Phased Implementation

| Phase | Scope | Impact |
|-------|-------|--------|
| **0: Foundation** | GraphCatalog, edges_between(), LogicalPlan IR, ExpandIntoOperator | Zero behavioral change |
| **1: Starting Point** | Plan enumeration, direction reversal, cost comparison | 10-100x for asymmetric patterns |
| **2: Expand-Into + Fusion** | Detect bound endpoints → ExpandInto; fuse multi-path pipelines | Eliminate hash join materialization |
| **3: Predicate Placement** | Optimal filter position relative to Expand operators | Reduced intermediate cardinality |
| **4: VarLength Paths** | BFS-based variable-length expansion with memoization | Better *1..N path queries |
| **5: WCOJ** | Sorted adjacency lists, IntersectionOperator for cyclic patterns | Optimal triangle/clique performance |

## Rationale

### Why Triple-Level Statistics

Global statistics (`avg_out_degree = 5.0`) are useless for plan selection. The same graph might have:
- `(:Person, :KNOWS, :Person)` avg out-degree = 5.0
- `(:Person, :WORKS_AT, :Company)` avg out-degree = 1.0
- `(:Company, :WORKS_AT, :Person)` avg in-degree = 1,100

Without triple-level granularity, the planner cannot distinguish these and cannot make informed decisions about traversal direction.

### Why Plan Enumeration over DP

For graph patterns, the number of candidate plans is manageable: `|nodes| × 2^|edges|` (starting points × direction choices). For typical Cypher patterns (3-6 nodes), this is at most ~200 candidates. Full DP enumeration of join orders is overkill — the graph structure constrains the space naturally.

### Why Expand-Into Matters

Consider: `MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' AND b.name = 'Bob'`

Without Expand-Into:
1. Scan Person, filter name='Alice' → 1 node
2. Expand KNOWS → ~500 neighbors
3. Filter name='Bob' → 1 result

With Expand-Into:
1. IndexScan name='Alice' → 1 node
2. IndexScan name='Bob' → 1 node
3. ExpandInto: check edge exists → 1 or 0 results

The second plan is O(1) instead of O(degree).

### Why Not Relational Optimizations First

The `query-execution-model-analysis.md` P0-P3 roadmap proposes:
- P0: Predicate pushdown, index-aware planning
- P1: Cost-based join ordering
- P2: Operator fusion, projection pushdown
- P3: WCOJ, parallel scans, JIT

The problem: P1 (cost-based join ordering with multiple join strategies) is solving the wrong problem. In a graph DB, the "join" is the adjacency lookup — there is only one strategy (follow the pointer). The real question is which pointer to follow first and in which direction. Starting point selection and direction reversal deliver 10-1000x improvements; multiple join strategies deliver 1-3x at best for this workload.

## Consequences

### Positive

- 10-100x performance improvement for asymmetric patterns (the common case in real-world graphs)
- Correct behavior: planner adapts to data distribution, not just query syntax
- Foundation for WCOJ (Phase 5) which requires understanding pattern topology
- EXPLAIN output shows all candidate plans with costs — users can understand why a plan was chosen
- Incremental statistics avoid per-query recomputation overhead
- Feature-flagged rollout eliminates risk of regression

### Negative

- Increased planner complexity (5-stage pipeline vs single function)
- Memory overhead for GraphCatalog (one TripleStats per unique triple pattern — typically < 1000 entries)
- Plan enumeration adds planning time (~0.1-1ms for typical patterns, amortized by plan cache)
- Two code paths during transition (old planner + new planner)

### Neutral

- Existing EXPLAIN/PROFILE (ADR-014) output format evolves to show candidate plans
- Plan cache key changes to include catalog generation (invalidate on schema change)
- JoinOperator remains for truly disjoint patterns — it is not eliminated, just used less

## Alternatives Considered

### Alternative 1: Relational-Style DP Join Enumeration

Implement DPccp or DPhyp with multiple join strategies (hash, merge, nested-loop).

**Rejected because:**
- Graph queries are dominated by adjacency traversal, not arbitrary joins
- The "join" is inherently index-nested-loop (follow the pointer) — there is no benefit to hash or merge join alternatives
- DP enumeration explores a much larger plan space than needed (all join orders) while missing the key dimension (traversal direction)
- Complexity is disproportionate to benefit for graph workloads

### Alternative 2: Greedy Heuristic Improvements Only

Keep single-plan generation but add heuristics: start from the node with the fewest label matches, prefer indexed properties.

**Rejected because:**
- Heuristics cannot capture traversal direction impact (which requires triple-level stats)
- No mechanism to detect Expand-Into opportunities
- Cannot compare plan quality without enumeration
- Easy to add heuristics that conflict or produce unexpected plans

### Alternative 3: Adaptive Query Execution (AQE)

Start executing a plan and re-plan mid-query when actual cardinalities diverge from estimates.

**Rejected because:**
- Significantly more complex (requires checkpointing, rollback, re-planning operators)
- In-memory graph traversal is fast enough that planning overhead is acceptable
- Can be added later as a Phase 6 enhancement once the base planner is mature

## Related Decisions

- [ADR-007: Volcano Iterator Model](./ADR-007-volcano-iterator-execution.md) — new operators (ExpandInto, NodeById) implement the PhysicalOperator trait
- [ADR-012: Late Materialization](./ADR-012-late-materialization.md) — new operators produce Value::NodeRef/EdgeRef, not clones
- [ADR-013: PEG Grammar Atomic Keywords](./ADR-013-peg-grammar-atomic-keywords.md) — no grammar changes needed for this ADR
- [ADR-014: EXPLAIN and PROFILE](./ADR-014-explain-profile-queries.md) — EXPLAIN output extended to show candidate plans and cost comparison

## References

- Mhedhbi, A., Salihoglu, S. (2019). "Optimizing Subgraph Queries by Combining Binary and Worst-Case Optimal Joins." VLDB.
- Veldhuizen, T. (2014). "Leapfrog Triejoin: A Simple, Worst-Case Optimal Join Algorithm." ICDT 2014.
- Neo4j Query Tuning Documentation: https://neo4j.com/docs/cypher-manual/current/query-tuning/
- Full design document: `samyama-cloud/docs/graph-native-planner-design.md`
- Existing analysis: `samyama-cloud/docs/query-execution-model-analysis.md`

---

**Last Updated**: 2026-03-06
**Status**: Accepted
