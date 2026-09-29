# ADR-040: Multi-Hop Query Optimization

> Renumbered from ADR-035 (#1515): it shared that number with the OEH hierarchy index record, which keeps 035.

## Status
**Proposed**

## Date
2026-07-31

## Objective

Improve the performance of two-hop and deeper graph queries by reducing the number of intermediate results carried between expansion steps.

## Context

### The problem

For selective multi-hop patterns, the executor can expand far more intermediate rows than the query ultimately returns. Every surplus row costs expansion work, memory, and — when the query sorts or aggregates — sorting and grouping work on rows that will be discarded.

Example:

```cypher
MATCH (person:Person {id: $personId})
      -[:KNOWS]->(friend)
      -[:KNOWS]->(candidate)
WHERE candidate.status = 'ACTIVE'
RETURN candidate
LIMIT 20
```

### What already exists

Three of the techniques below are already implemented in some form. This ADR is about closing the *gaps* in them, not introducing them from scratch:

| Capability | Where | State |
|---|---|---|
| Predicate pushdown (inline, AND-chain decomposition during `plan_match`) | `src/query/executor/planner.rs` (QP-01, `can_pushdown_match`) | Shipped, on by default |
| Predicate pushdown below `Expand` (logical rewrite) | `src/query/executor/logical_optimizer.rs` (`test_predicate_pushdown_below_expand`) | Shipped, but reached only via `plan_enumerator`, which is **gated behind `SAMYAMA_GRAPH_NATIVE=true` and off by default** (`src/query/mod.rs:199`) |
| Cardinality / selectivity estimation per plan node | `src/query/executor/cost_model.rs` + `GraphCatalog` (`estimate_label_scan`, `estimate_expand_out`, `estimate_expand_in`) | Shipped, same gating |
| Early `LIMIT` propagation into scans | `src/query/executor/planner.rs` (QP-04, `try_push_limit`, `NodeScanOperator::with_early_limit`) | Shipped, on by default |
| Plan visibility | `EXPLAIN` / `PROFILE` (ADR-014) | Shipped |
| **Top-N for `ORDER BY … LIMIT`** | — | **Missing.** `SortOperator` (`src/query/executor/operator.rs:4353`) buffers every input row into `records: Vec<Record>` and sorts the full set before `LimitOperator` discards the tail. |
| Filter pushdown across a `WITH` boundary | Explicitly deferred (`src/query/executor/adjacency_agg_detector.rs:334`, `:1007`) | Missing |

So the honest framing is: the machinery is largely built, but the strongest form of it is behind a default-off flag, one operator (`Sort`) has no bounded variant at all, and one boundary (`WITH`) is uncovered.

## Decision

Pursue four workstreams, in rough priority order.

### 1. Add a Top-N operator

Replace `Sort → Limit` with a bounded Top-N (heap of size `skip + limit`) when the plan has `ORDER BY` with a constant `LIMIT` and no intervening operator that needs the full sorted set. This is the only item here that is genuinely new code rather than an extension, and it is the clearest win: memory drops from O(rows) to O(limit), and sorting cost from O(n log n) to O(n log k).

### 2. Promote the cost-based path toward default-on

Decide what `SAMYAMA_GRAPH_NATIVE` needs before it can flip to on-by-default: benchmark coverage, regression thresholds, and a documented fallback. Until it flips, the pushdown-below-expand and cost-model work described above benefits nobody in a default deployment. If flipping it is not the intent, say so explicitly — a permanently-off optimizer path is a maintenance cost with no user.

### 3. Extend pushdown coverage

Filter kinds to confirm or add as pushable: equality, range, property-existence, node-label, relationship-type. Then close the `WITH`-boundary gap noted above (post-`WITH` `WHERE`), which currently forces conservative plans in aggregation-shaped queries.

### 4. Sharpen per-hop estimation

The cost model uses fixed default selectivities in several branches (`0.5` for a generic filter, `0.1^k` for TrieJoin). Replace the ones that matter with statistics-backed estimates — average degree per `(label, relationship type)` is the highest-value one for multi-hop.

## Implementation Tasks

* [ ] Profile current two-hop and deeper queries; record a baseline
* [ ] Add a Top-N operator and plan it for `ORDER BY … LIMIT`
* [ ] Inventory which filter kinds are pushable on each path (default vs graph-native)
* [ ] Close the post-`WITH` `WHERE` pushdown gap
* [ ] Replace default selectivities with degree statistics where they drive multi-hop plans
* [ ] Define the exit criteria for `SAMYAMA_GRAPH_NATIVE` defaulting to on
* [ ] Surface pushed predicates and Top-N in `EXPLAIN` / `PROFILE`
* [ ] Add correctness and performance tests

## Configuration

Samyama configures the query path through environment variables and `ServerConfig` (`src/main.rs`), not a YAML file. Any new switch should follow that convention:

```sh
SAMYAMA_GRAPH_NATIVE=true      # existing: enables the cost-based planner path
SAMYAMA_QUERY_TIMEOUT=30       # existing
```

New flags, if any, should be named in the same `SAMYAMA_*` style and default to preserving current behavior.

## Testing

Benchmarks must cover:

* Two-hop and three-hop queries
* Low- and high-selectivity filters
* Queries with and without filters
* Queries using `ORDER BY` and `LIMIT` (the Top-N path)
* Both planner paths (`SAMYAMA_GRAPH_NATIVE` on and off)
* At least two dataset sizes

LDBC SNB Interactive already provides multi-hop shapes at SF1/SF10 (`benches/ldbc_benchmark.rs`, `docs/BENCHMARKS.md`) — extend that rather than inventing a new harness.

Validate that optimized and existing plans return identical results.

## Consequences

**Easier:** selective multi-hop queries get bounded memory and lower latency; sorted-limited queries stop materializing the full result set; plan quality becomes measurable rather than assumed.

**Harder:** two planner paths must stay behaviorally identical while only one is optimized, which doubles the correctness surface — a reason to resolve item 2 rather than let the split persist. Statistics-backed estimation adds a maintenance burden: stale statistics produce worse plans than fixed defaults.

## Acceptance Criteria

* [ ] `ORDER BY … LIMIT` uses bounded memory proportional to the limit, not the result set
* [ ] Filters are applied during expansion wherever valid on the default path
* [ ] Selective multi-hop queries show measurable latency improvement against the recorded baseline
* [ ] Intermediate row count and memory usage are reduced
* [ ] Query plans show pushed predicates and Top-N
* [ ] Queries without filters do not materially regress
* [ ] Single-hop and indexed lookup queries do not regress
* [ ] Results remain functionally identical across both planner paths
* [ ] Benchmarks cover multiple hop depths and dataset sizes

## Out of Scope

* Data-loading optimization — see [ADR-036](./ADR-036-data-load-time-to-ready-optimization.md)
* Index-building optimization
* Hardware scaling
* Single-hop query optimization
* Unrelated query correctness defects

## Related Decisions

* [ADR-007](./ADR-007-volcano-iterator-execution.md) — Volcano iterator execution model
* [ADR-012](./ADR-012-late-materialization.md) — late materialization (why intermediate rows are `NodeRef`, not clones)
* [ADR-014](./ADR-014-explain-profile-queries.md) — `EXPLAIN` / `PROFILE`
* [ADR-015](./ADR-015-graph-native-query-planning.md) — the graph-native planner this ADR proposes promoting
* [ADR-017](./ADR-017-adjacency-aware-aggregation-planning.md) — adjacency-aware aggregation planning
* [ADR-027](./ADR-027-aggregation-with-pushdown.md) — aggregation `WITH` pushdown
