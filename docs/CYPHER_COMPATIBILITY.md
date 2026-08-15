# Cypher Compatibility Matrix

**Verified against:** Samyama v1.7.0, commit `6f36a54` (arithmetic null row re-verified after #457)
**Method:** every row below was executed against the engine by `examples/cypher_matrix_probe.rs`. Nothing here is from memory.

## How to read this

There is **no measured OpenCypher coverage percentage** in this document, and there will not be one until the openCypher TCK is actually run (#434). A previous version of this page claimed "~90% OpenCypher coverage"; that figure was self-assessed, never checked against the TCK, and an earlier internal assessment of the same engine put the number at 40–50%. Rather than pick between two unverified numbers, the claim is withdrawn.

What this page reports instead is narrower and checkable: **78 probes, 75 supported, 3 not.** Each probe is one representative query for one feature. Re-run it with:

```
cargo run --release --example cypher_matrix_probe
```

A ✅ here means *the query executed*. It does not certify semantics — a construct can run and still be wrong. Correctness lives in the test suites (`tests/cypher_projection_semantics.rs` and friends), and the one place where a ✅ construct is known to disagree with Cypher is called out inline below.

## What is not supported

Three things, all verified:

| Feature | Behaviour | Tracking |
| :--- | :--- | :--- |
| `split()` | `Unknown function: split` | — |
| `FOREACH` | Parse error | — |
| `algo.bfs`, `algo.dijkstra` | Do not exist under those names — use `algo.shortestPath` and `algo.weightedPath` | — |

## Feature Matrix

| Feature Category | Feature | Samyama | Notes |
| :--- | :--- | :---: | :--- |
| **Read** | `MATCH` | ✅ | Single and multi-hop, variable-length paths |
| | `OPTIONAL MATCH` | ✅ | |
| | `WHERE` | ✅ | |
| | `RETURN` / `RETURN DISTINCT` | ✅ | |
| | `ORDER BY` | ✅ | |
| | `SKIP` / `LIMIT` | ✅ | |
| | `EXPLAIN` | ✅ | |
| **Write** | `CREATE` | ✅ | |
| | `DELETE` / `DETACH DELETE` | ✅ | |
| | `SET` / `REMOVE` | ✅ | |
| | `MERGE` | ✅ | |
| | `MERGE ... ON CREATE / ON MATCH SET` | ✅ | |
| **Aggregation** | `count()` | ✅ | |
| | `sum()` / `avg()` | ✅ | |
| | `min()` / `max()` | ✅ | |
| | `collect()` | ✅ | |
| | `collect(DISTINCT x)` | ✅ | Previously listed as a gap; shipped in v0.6.x |
| | Implicit `GROUP BY` | ✅ | |
| **Structure** | `WITH` | ✅ | |
| | `UNWIND` | ✅ | Including leading `UNWIND` |
| | `UNION` / `UNION ALL` | ✅ | |
| | `EXISTS { }` subquery | ✅ | |
| | `CALL { }` subquery | ✅ | Leading, non-correlated form. Exports its columns; outer `WHERE`/`DISTINCT` apply. Fixed in #458. The importing form `MATCH (x) CALL { WITH x ... }` is still a parse error |
| | `FOREACH` | ❌ | Parse error |
| **String Functions** | `toUpper`, `toLower` | ✅ | |
| | `trim`, `replace` | ✅ | |
| | `substring`, `left`, `right` | ✅ | |
| | `reverse`, `toString` | ✅ | |
| | `split` | ❌ | |
| **Numeric Functions** | `abs`, `ceil`, `floor`, `round` | ✅ | |
| | `sqrt`, `sign` | ✅ | |
| | `toInteger`, `toFloat` | ✅ | |
| | `rand`, `log`, `exp` | ✅ | Previously listed as a gap; shipped in v0.6.x |
| **Collection Functions** | `size`, `length` | ✅ | |
| | `head`, `last`, `tail` | ✅ | |
| | `keys` | ✅ | Nodes and edges. Rejects maps — #452 |
| | `range` | ✅ | |
| | `nodes()`, `relationships()` | ✅ | Previously listed as a gap; shipped in v0.6.x |
| | List indexing `xs[0]` | ✅ | |
| | Chained indexing `xs[0][1]` | ✅ | Fixed in #453 |
| | List slicing `xs[0..2]` | ✅ | Previously listed as a gap |
| | `reduce()` | ✅ | |
| **Graph Functions** | `id()` | ✅ | |
| | `labels()`, `type()` | ✅ | |
| | `exists()`, `coalesce()` | ✅ | |
| | Named paths `p = (a)-[]->(b)` | ✅ | Previously listed as a gap |
| | `shortestPath()` | ✅ | |
| | Variable-length paths `[*1..2]` | ✅ | |
| **Expressions** | `CASE WHEN ... THEN ... END` | ✅ | |
| | Pattern comprehension | ✅ | Previously listed as a gap |
| | List comprehension | ✅ | |
| | Map literal `{a: 1}` | ✅ | Nested to arbitrary depth |
| | Map bracket access `m["a"]` | ✅ | Chaining fixed in #453 |
| | Map dot access `m.a` | ❌ | Parse error — #452 |
| **Predicates** | `STARTS WITH`, `ENDS WITH`, `CONTAINS` | ✅ | |
| | `=~` (regex) | ✅ | |
| | `IN` (list membership) | ✅ | |
| | `IS NULL`, `IS NOT NULL` | ✅ | Applies to subscripts since #453 |
| | `AND`, `OR`, `NOT`, `XOR` | ✅ | |
| | `all` / `any` / `none` / `single` | ✅ | |
| **Type Handling** | Integer/Float coercion | ✅ | |
| | Null propagation — comparison | ✅ | `1 > null` → `null` |
| | Null propagation — arithmetic | ✅ | `1 + null` → `null`; `p.a + p.missing` nulls only its own row. Fixed in #457 |
| | Temporal types | ✅ | `date()`, component access, arithmetic. No temporal index |
| | Duration arithmetic | ✅ | |
| **Extensions** | `CREATE VECTOR INDEX` | ✅ | |
| | `CALL db.index.vector.queryNodes` | ✅ | |
| | `algo.pageRank` | ✅ | Config map: `algo.pageRank({iterations: 2})` |
| | `algo.wcc` / `algo.scc` | ✅ | |
| | `algo.shortestPath` / `algo.weightedPath` | ✅ | **Positional** args: `algo.shortestPath(0, 2)` |
| | `algo.maxFlow` | ✅ | Positional args |
| | `algo.mst` | ✅ | |
| | `algo.triangleCount` | ✅ | |
| | `algo.cdlp` / `algo.lcc` | ✅ | |
| | `algo.bfs` / `algo.dijkstra` | ❌ | Not registered — use `shortestPath` / `weightedPath` |
| | `algo.or.solve` | ✅ | Requires write access |

## Known inconsistency

Algorithm procedures do not share a calling convention. `algo.pageRank` takes a config map; `algo.shortestPath`, `algo.weightedPath` and `algo.maxFlow` take **positional** arguments. The error text does not say which form a given procedure wants, so the first attempt at any of them tends to fail.

## Maintaining this page

Re-run the probe and update the rows it disagrees with. If a row changes, record the commit in the header. Any row asserted here without a probe backing it is a claim, not a fact — which is how the previous version of this page came to both overstate the headline and understate seven of its own rows.
