# ADR-017: Adjacency-Aware Aggregation Planning

## Status
**Proposed** — design for review. Not yet implemented.

## Date
2026-04-13

## Context

ADR-015 shipped the graph-native planner with starting-point enumeration, direction reversal, and cost-based plan selection. That planner **correctly** handles asymmetric traversal patterns — `MATCH (a:Article)-[:AUTHORED_BY]->(au:Author {name: 'Smith'})` now drives from `Author` via the selective name filter instead of the 40M-node `Article` scan.

But for **aggregation queries that touch every edge of a type**, starting-point selection cannot help. The total work is bounded below by the edge count, not by the cardinality of either endpoint.

The 500-query v1.0 mega benchmark exposes this with four failures, all hitting the 300-second query timeout on PubMed (1B edges):

| ID | Query | Bottleneck |
|---|---|---|
| MB049 | `MATCH (a:Article)-[:PUBLISHED_IN]->(j:Journal) RETURN j.title, count(a) ORDER BY count(a) DESC LIMIT 10` | 40M edges into aggregate |
| MB054 | Same shape, `:MENTIONS_CHEMICAL` instead of `:PUBLISHED_IN` | 55M edges |
| MB053 | Same shape with explicit `WITH m LIMIT 500` prefix | 40M post-LIMIT expansion¹ |
| OM27 | Depression comorbidity agg on OMOP 51M Person × ConditionOccurrence | Cross-product into aggregate |

¹ *PR #192 fixes MB053 and EX49 — the non-aggregation case where `WITH x LIMIT N` should short-circuit. This ADR addresses the remaining aggregation cases.*

### Why Starting-Point Selection Is Not Enough

Consider MB049. Two candidate plans:

| Plan | Start | Expand | Aggregate input |
|---|---|---|---|
| A | Scan Article (40M) | Forward `:PUBLISHED_IN` (avg_out = 1) | 40M tuples |
| B | Scan Journal (50k) | Reverse `:PUBLISHED_IN` (avg_in = 800) | 40M tuples |

Both plans feed 40M tuples into the `AggregateOperator`. Starting from the smaller side doesn't reduce work — it just changes which side of the edge list is walked. **The aggregation itself must see every edge to finalize group counts.**

The fundamental insight: when the aggregate is `count(neighbor)` grouped by a bound endpoint label, the count **is** the per-node in-degree (or out-degree) filtered by edge type. That quantity is already maintained in the adjacency list structure — we don't need to walk individual edges.

### What's Already There

The v1.0 planner has a narrow `EdgeTypeCountOperator` (planner.rs:1321-1339) that handles:

```cypher
MATCH ()-[r]->() RETURN type(r), count(*)
```

This is the **degenerate** case — global edge-type counts with no endpoint labels. Per planner.rs:1319-1326, it requires:
- Single MATCH, single path, single segment
- No labels on start or end nodes
- `group by` is `type(r)`

MB049 does not qualify because both endpoints have labels. The degree information is there in the adjacency lists; we just don't use it.

## Decision

**We will extend the planner to recognize a class of aggregation patterns where the aggregate reduces to per-node degree scans, and emit a new `AdjacencyCountAggregateOperator` that computes results in O(|nodes on the bound side|) instead of O(|edges|).**

### Pattern Recognition

The planner will detect this shape during logical-plan construction:

```cypher
MATCH (a[:LabelA])-[r:EdgeType]-(b:LabelB)
RETURN b.prop [, ...], count(a) AS n  -- or count(*), count(r), count(DISTINCT a)
[ORDER BY n DESC | ASC] [LIMIT N]
```

**Constraints for the rewrite:**
- Single MATCH clause, single edge segment (the common pattern of MB049/54 and similar)
- Exactly one `count()` aggregate — sum/avg/min/max over neighbor properties are a future phase
- Group-by keys are all properties of a single endpoint (`b` in the example) plus optional expressions on `b`
- WHERE clauses, if present, only reference that endpoint
- Edge type is concrete (not a wildcard)

The direction of the count — forward (out-degree of `a` grouped by `a`) or reverse (in-degree of `b` grouped by `b`) — is determined by which endpoint carries the group-by keys.

### New Operator: AdjacencyCountAggregate

Takes: source scan (the grouped side, e.g. `:Journal`), edge type, direction, group-by keys, optional ORDER BY and LIMIT.

Algorithm:
```
for each node n in source scan:
    let count = store.degree(n, edge_type, direction, [optional other-label filter])
    emit (group_by_exprs, count)
apply ORDER BY + LIMIT with bounded TopK heap
```

Cost: `O(|group_scan| × O(degree_lookup))`. For MB049: 50,000 Journals × O(1) degree lookup = 50,000 operations vs the current 40,000,000.

### When the Other-Label Filter Is Cheap

A subtlety: MB049 specifies `a:Article`. If `:PUBLISHED_IN` has both `:Article` and `:Book` sources, the count must filter to Article only. Three options:

1. **Degree cache is edge-type only**: filter by walking adjacencies, O(degree) per node (no improvement vs full scan)
2. **Per-(label, edge_type) degree cache**: maintain `degree_by_other_label(node, edge_type, other_label)` — additional storage, negligible per-node cost
3. **Ignore the filter when the schema implies uniqueness**: if catalog knows `:PUBLISHED_IN` only connects `:Article` and `:Journal`, the filter is a no-op

Phase 1 takes option 3 via catalog consultation; option 2 becomes a Phase 3 storage-layer enhancement if benchmarks show the schema-uniqueness check isn't covering common cases.

### Phased Implementation

| Phase | Scope | Delivers |
|-------|-------|----------|
| **0: Pattern detector** | `LogicalPlanNode::AdjacencyCountAggregate` variant; planner recognition; unit tests for detection | Zero behavioral change, new plan shape available behind env flag |
| **1: Operator + unschema'd cases** | `AdjacencyCountAggregateOperator`; physical conversion; catalog schema-uniqueness check; integration tests with ORDER BY DESC LIMIT N | MB049, MB054 pass; ~50x speedup for qualifying queries |
| **2: Label-filtered degree** | Extend adjacency to cache per-(edge_type, other_label) counts for hot node labels; lazy computation with invalidation | Handles mixed-source edge types correctly |
| **3: Multi-aggregate + WITH-wrapped form** | Extend to `count(*)`, `count(r)`, and the `WITH ... ORDER BY ... LIMIT N RETURN ...` shape (addresses OM27) | Broader coverage; OM27 passes |
| **4: Collected neighbors** | Extend to `sum(b.prop)` and `collect(b)` when b is the non-grouped side | LDBC BI query coverage |

### Interaction with Other Optimizations

- **`EdgeTypeCountOperator`** (the `type(r), count(*)` case): remains; it's the degenerate form of this ADR's pattern
- **`LabelCountOperator`**: remains; covers `MATCH (n:L) RETURN count(n)`
- **TopK fusion** (proposed in issue #190 part 2): `AdjacencyCountAggregate` emits its own TopK internally; no separate fusion needed
- **PR #192 (WithBarrier streaming LIMIT)**: orthogonal; covers the non-aggregation case

## Rationale

### Why This Is The Right Abstraction

The "count of things on the other side of an edge" is the most common aggregation shape in real graph workloads — degree distributions, popularity, influence. Every graph DB that's serious about this workload has a specialized plan shape for it:

- **Neo4j**: `GetDegree` operator, used when the planner detects count-star with pattern matching
- **Memgraph**: inline degree-lookup in aggregation planning since v2.4
- **TigerGraph**: `degree()` function exposed directly in GSQL

The pattern is pervasive enough to justify its own plan node rather than bolting onto the generic aggregate.

### Why Not Just Fix EdgeTypeCountOperator

The existing operator's constraints (planner.rs:1319) are too restrictive — they were designed for the global `type(r), count(*)` shape, not labelled-endpoint aggregations. Relaxing them mid-logic-block risks other plan paths. A new plan node with its own predicate is cleaner and separately testable.

### Why Not Materialized Views

A materialized view for "top-10 journals by article count" would make this specific query O(10). But:
- Requires view definition syntax and invalidation rules (big project)
- Hard-coded for the exact query; MB054 would need its own view
- Write overhead scales with view count

The degree-scan approach handles any `count(neighbor)` query with one code path and no view maintenance.

### Performance Target

For MB049 on the v1.0 mega-bench corpus:
- Current: timeout at 300s (query doesn't complete)
- Target: < 500 ms (50,000 Journal scans × ~10 μs per degree lookup)

This is a 600x improvement lower bound (timeout → success) and a plausible further 10-100x vs a cost-optimal edge-walking plan.

## Consequences

### Positive
- MB049, MB054 complete in < 1s instead of timing out (mega bench 493 → 495)
- OM27 completes once Phase 3 ships (mega bench → 496+)
- Pattern generalizes to all `count(neighbor)` groupby-one-endpoint queries — many LDBC BI queries benefit
- Catalog is already there — no schema or storage changes for Phase 1
- No query syntax changes — users keep writing standard Cypher

### Negative
- New plan node adds planner surface area to test
- Label-filtered degree cache (Phase 2) adds per-node storage (~8 bytes per (label, edge_type) pair)
- If the schema-uniqueness check is wrong, query returns wrong counts — must be defensive
- Phase 2's lazy cache invalidation is subtle around DELETE under MVCC; needs attention alongside the ADR-012 late-materialization and v1.0 MVCC work

### Neutral
- EXPLAIN output gains a new operator name; users running automated test diffs on plan shapes need an update
- Feature-flag rollout (`SAMYAMA_ADJACENCY_AGG=1`) mirrors the ADR-015 rollout pattern

## Alternatives Considered

### Alternative 1: Generic Streaming Aggregate With Cost-Based Early Termination
Compute aggregates in a streaming fashion and cut off when the current heap top can no longer be beaten.

**Rejected**: requires knowing the full distribution of remaining tuples to terminate early. Without pre-computed stats, we'd need to scan most of the input anyway. For MB049 specifically, the long tail of infrequent journals is exactly where we'd still be scanning.

### Alternative 2: Approximate Aggregation (Count-Min Sketch)
Use a sketch data structure to estimate group counts.

**Rejected**: Cypher `count()` is exact — approximate answers would break the semantics. Could be offered as an opt-in for analytics queries, but that's a separate feature (ADR candidate later).

### Alternative 3: Push Aggregate Below Scan Via Physical Pre-Aggregation
Maintain an incrementally-updated per-(label, edge_type, other_label) counter at ingestion time.

**Rejected for Phase 1**, kept as Phase 2: this is the label-filtered degree cache. Writes are in the hot path of loading; write overhead must be measured before committing to maintaining it unconditionally.

### Alternative 4: Query Rewriting at the API Layer
Recognize the pattern and rewrite the Cypher before parsing — e.g., transform MB049 into `MATCH (j:Journal) WHERE size((:Article)-[:PUBLISHED_IN]->(j)) > 0 ...`.

**Rejected**: leaks into the NLQ and Cypher-SDK surfaces, inconsistent with "engine handles its own optimization", brittle across Cypher dialects.

## Related Decisions

- [ADR-007: Volcano Iterator Model](./ADR-007-volcano-iterator-execution.md) — new operator implements `PhysicalOperator`
- [ADR-012: Late Materialization](./ADR-012-late-materialization.md) — new operator emits `Value::NodeRef` for grouped endpoint
- [ADR-015: Graph-Native Query Planning](./ADR-015-graph-native-query-planning.md) — this ADR extends the logical-plan layer introduced there
- PR #192 `perf/with-limit-streaming` — complementary; handles the non-aggregation WITH LIMIT case

## References

- Mega benchmark results: [`samyama-graph-book/src/data/benchmark/`](https://github.com/samyama-ai/samyama-graph-book/tree/main/src/data/benchmark) (v4 run: 493/500)
- Issue [#190](https://git.samyama.ai/Samyama.ai/samyama-graph-enterprise/issues/190) — motivating issue
- Issue [#191](https://git.samyama.ai/Samyama.ai/samyama-graph-enterprise/issues/191) — OM27 tracker
- Neo4j GetDegree operator: https://neo4j.com/docs/cypher-manual/current/query-tuning/using/
- Labelled Property Graph Model, Angles et al., 2017

---

**Last Updated**: 2026-04-13
**Status**: Proposed (awaiting review)
