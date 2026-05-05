# ADR-027: Aggregation Push-Down and WITH Push-Down

## Status
**Proposed** (2026-05-05) — retroactively formalising the v0.7 / v0.8 shipped rewrites.

## Date
2026-05-05

## Context

The naïve evaluation of `MATCH (n:Label) RETURN count(*)` is `O(n)` — scan every node, increment a counter. With a label index already maintained (`label_index: HashMap<Label, HashSet<NodeId>>`, §3.1), the same query is answerable in `O(1)` from `label_index.get(L).len()`. The same observation holds for `MATCH ()-[:T]->() RETURN count(*)` against `edge_type_index`.

A more general observation applies to multi-clause `WITH` pipelines: a projection or aggregation expressed *above* a `WITH` barrier may be safely moved *below* the barrier when this preserves semantics. Doing so reduces the cardinality crossing the barrier and shortens the materialised intermediate.

Without these rewrites:
- `count(*)` over a label or edge type takes seconds at hero scale instead of microseconds.
- `MATCH (a)-[:R]->(b) WITH b, count(*) AS c WHERE c > 100 RETURN b` materialises every (a,b) pair before counting, blowing up RAM for hub-style sources.

The wiki decision [[decisions/with-push-down.md]] captures the original rationale; this ADR formalises and extends.

## Decision

We will detect and apply two classes of push-down rewrite at planning time.

### 1. Aggregation push-down to count operators

Rewrite a `LogicalAggregate(count(*))` over `LogicalScan(:Label)` (with no other predicates and no projection beyond `count(*)`) into `LabelCountOperator(:Label)`.

Symmetrically: `LogicalAggregate(count(r))` over `LogicalExpand(edge_type=T)` (with no other predicates) into `EdgeTypeCountOperator(:T)`.

Detection lives in `src/query/executor/adjacency_agg_detector.rs`. Operators in `src/query/executor/operator.rs`.

### 2. WITH push-down

A projection / aggregation / filter `P` sitting above a `WithBarrierOperator` whose dependencies are a subset of the WITH-projected variables `V` may be moved below the barrier provided:

- `P` does not create new bindings the barrier was meant to hide.
- Above-barrier ordering / `LIMIT` / `DISTINCT` are not violated.
- No `OPTIONAL MATCH` separates `P` from its dependencies (null semantics).

The rewrite is a logical-level transformation in `logical_optimizer.rs`. The planner re-runs cost estimation after the rewrite to confirm the rewrite is beneficial; if not, it reverts (rare).

## Consequences

### Positive
- `count(*)` over a label / edge type goes from `O(n)` to `O(1)`.
- WITH push-down reduces materialisation at the barrier; downstream pipeline breakers (sort, hash join) start from a smaller dataset.
- Composes with ADR-017 (Adjacency-Aware Aggregation): direction selection and aggregation push-down can both fire on the same query.

### Negative
- Detection is conservative. A `WHERE` clause, a property predicate, an `OPTIONAL MATCH`, or a non-trivial expression in the projection blocks push-down even when, with more careful analysis, the rewrite would be safe.
- DISTINCT push-down is partial — only handled for single-column projections.
- Push-down depends on catalog accuracy. A stale `GraphCatalog` (§2.4) may cause the planner to under-estimate the win and revert the rewrite.
- Combinator queries (`count(*) AND count(DISTINCT prop)` in the same RETURN) get one push-down and one full scan, with no work sharing.

### Neutral
- Aggregation push-down is invisible to the user except through wallclock and `EXPLAIN`. PROFILE shows `LabelCount` / `EdgeTypeCount` instead of `Aggregate`.

## Alternatives Considered

| Option | Rejected because |
|--------|------------------|
| No push-down | Costs seconds where microseconds suffice; not viable at hero scale. |
| Always-materialise + vectorised executor | Misses the case where the right answer is "don't compute it at all". |
| Adaptive run-time push-down | Real wins for unknown selectivity; high engineering cost; deferred. |
| Push down `count(*)` only, never aggregations with predicates | Misses the FOLLOWERS hub pattern; cuts the value of the rewrite roughly in half. |

## Follow-ups

1. **Broaden detection**: lift restrictions on benign predicates (e.g., `WHERE n.year > 2000` where `year` is indexed should still allow `count` push-down to a range-count over the index).
2. **Combine with semi-join detection**: a single pass that emits `Semi-Join + Push-Down` for `EXISTS { ... }` patterns.
3. **DISTINCT push-down for column-store-only projections**.
4. **PROFILE annotations** showing whether each aggregate fired a push-down or fell back to full scan.
5. **Catalog warmup** in the snapshot import path so the planner sees correct counts immediately.

## References

- Code: `src/query/executor/adjacency_agg_detector.rs`, `src/query/executor/operator.rs` (LabelCountOperator, EdgeTypeCountOperator, WithBarrierOperator), `src/query/executor/logical_optimizer.rs`.
- Wiki: [[query-aggregation-with-pushdown.md]], [[query-multi-with-batching.md]], [[memory-cardinality-counts.md]], [[decisions/with-push-down.md]].
- Related ADRs: ADR-015, ADR-017, ADR-029.
