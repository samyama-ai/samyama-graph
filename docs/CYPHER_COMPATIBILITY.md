# Cypher Compatibility Matrix

**Verified against:** Samyama v1.7.0, commit `6f36a54` (arithmetic null row re-verified after #457)
**Method:** every row below was executed against the engine by `examples/cypher_matrix_probe.rs`. Nothing here is from memory.

## How to read this

There is **no measured OpenCypher coverage percentage** in this document, and there will not be one until the openCypher TCK is actually run (#434). A previous version of this page claimed "~90% OpenCypher coverage"; that figure was self-assessed, never checked against the TCK, and an earlier internal assessment of the same engine put the number at 40–50%. Rather than pick between two unverified numbers, the claim is withdrawn.

What this page reports instead is narrower and checkable: **78 probes, 77 supported, 1 not.** Each probe is one representative query for one feature. Re-run it with:

```
cargo run --release --example cypher_matrix_probe
```

For a machine-readable result, add `--json`:

```
cargo run --release --example cypher_matrix_probe -- --json cypher_matrix.json
```

That writes a conformance **result envelope** in the shape [spec 18](https://git.samyama.ai/Samyama.ai/samyama-cloud/src/branch/main/docs/product/spec/18-conformance-harness.md) requires of a quotable run — suite, requirement_ids, run_id, engine with commit, hardware, dataset with hash, measurements, status, artifacts. `status` is `pass` only when every probe is supported, following the rollup rule that an unmeasured requirement counts as failing rather than passing. The assembler that turns these into `SCORECARD.json` does not exist yet; this is one suite emitting the envelope it will consume.

A ✅ here means *the query executed*. It does not certify semantics — a construct can run and still be wrong. Correctness lives in the test suites (`tests/cypher_projection_semantics.rs` and friends), and the one place where a ✅ construct is known to disagree with Cypher is called out inline below.

## What is not supported

One thing, verified:

| Feature | Behaviour | Tracking |
| :--- | :--- | :--- |
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
| | `FOREACH` | ✅ | Leading and trailing forms; `CREATE`/`SET` bodies. A relationship pattern in the body is refused (#467); `MERGE`/`DELETE`/nested bodies remain unimplemented (#465) |
| **String Functions** | `toUpper`, `toLower` | ✅ | |
| | `trim`, `replace` | ✅ | |
| | `substring`, `left`, `right` | ✅ | |
| | `reverse`, `toString` | ✅ | |
| | `split` | ✅ | Multi-char delimiters; empty delimiter splits into characters |
| **Numeric Functions** | `abs`, `ceil`, `floor`, `round` | ✅ | |
| | `sqrt`, `sign` | ✅ | |
| | `toInteger`, `toFloat` | ✅ | |
| | `rand`, `log`, `exp` | ✅ | Previously listed as a gap; shipped in v0.6.x |
| **Collection Functions** | `size`, `length` | ✅ | |
| | `head`, `last`, `tail` | ✅ | |
| | `keys` | ✅ | Nodes, edges and maps (#452) |
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
| | Map dot access `m.a` | ✅ | `d.meta.a`, `d.meta.c.d`; desugars to the same `Index` path as brackets. Fixed in #452. Reads only — `SET d.meta.a = 1` is still a parse error |
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
| | `algo.bfs` / `algo.dijkstra` | ❌ | Not registered. The error now redirects to `algo.shortestPath` / `algo.weightedPath` and lists every procedure with its argument shape |
| | `algo.or.solve` | ✅ | Requires write access |

## Known inconsistency

Algorithm procedures do not share a calling convention. `algo.pageRank` and `algo.or.solve` take a config map; `algo.shortestPath`, `algo.weightedPath`, `algo.maxFlow`, `algo.mst`, `algo.cdlp` and `algo.lcc` take **positional** arguments. This is still inconsistent, but an unknown or misused name now reports the full list with each procedure's argument shape, so it costs one failed attempt rather than three.

## Maintaining this page

Re-run the probe and update the rows it disagrees with. If a row changes, record the commit in the header. Any row asserted here without a probe backing it is a claim, not a fact — which is how the previous version of this page came to both overstate the headline and understate seven of its own rows.
