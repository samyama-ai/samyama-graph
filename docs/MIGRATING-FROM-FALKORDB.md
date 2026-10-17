# Migrating from FalkorDB or RedisGraph

Both engines speak RESP and both take `GRAPH.QUERY`, so a FalkorDB or RedisGraph
application looks close to working before it is. The differences sit in three
places: the protocol around the query, the Cypher inside it, and the data. This
page takes them in that order.

Everything below was checked against FalkorDB's graph module 4.22.1
(`falkordb/falkordb:latest`) and this repository's `dev` branch on 2026-10-17.
RedisGraph is FalkorDB's predecessor and shares its protocol; it was not run
separately.

## 1. Point the report at your queries

```bash
cargo run --release --example compatibility_report -- --queries your_queries.cypher
```

The input format and the `--json` option are described in
[MIGRATING-FROM-NEO4J.md](MIGRATING-FROM-NEO4J.md#1-point-the-report-at-your-queries).
The report reads Cypher only. The protocol differences in section 2 are not
Cypher, so it cannot see them.

[`benchmarks/compat/falkordb-idioms.cypher`](../benchmarks/compat/falkordb-idioms.cypher)
holds 53 idioms from FalkorDB's documentation, each one run on FalkorDB before it
was added. Against it:

| | Count |
|---|---:|
| accepted by the report (parses and plans) | 38 of 53 |
| **actually runs** | **32 of 53** |

**Read the second line.** "Accepted" means the query parses and a plan can be
built, and six FalkorDB idioms get that far and then fail when executed:
`algo.BFS`, `algo.SPpaths`, `algo.WCC` and `algo.betweenness` with FalkorDB's
config keys, `string.join`, and `LOAD CSV … MERGE` ([#1895](https://github.com/samyama-ai/samyama-graph/issues/1895)).
Treat the report as a ceiling and run your queries before you count them.

## 2. What changes at the protocol

| | FalkorDB | Samyama |
|---|---|---|
| Graph key | any name; `GRAPH.QUERY social …` creates `social` | **only `default`**; any other name is refused, over RESP and HTTP alike. One graph per instance |
| Parameters | `CYPHER name='Alice' MATCH … $name` prefix | **trailing pairs**: `GRAPH.QUERY default "MATCH … $name" name Alice`. The `CYPHER` prefix is a syntax error here, and FalkorDB refuses trailing pairs |
| Parameter values | Cypher literals in the prefix | a pair's value is read as JSON when it parses as JSON, so `rows '[{"n":1}]'` binds a list of maps |
| Read reply | `[header, [rows…], [statistics]]` | `[header, row, row, …]`: rows inline, no statistics |
| Write reply | statistics (`Nodes created: 2`, …) | `[[]]` |
| `--compact` | typed compact reply | **refused**: read as an odd number of parameter arguments |
| `GRAPH.RO_QUERY` with a write | refused | refused |
| `GRAPH.EXPLAIN`, `GRAPH.PROFILE` | commands | `unknown command`; write `EXPLAIN …` or `PROFILE …` **inside** `GRAPH.QUERY` instead, which returns the plan as a `plan` column |
| `GRAPH.CONFIG`, `GRAPH.CONSTRAINT`, `GRAPH.SLOWLOG`, `GRAPH.COPY`, `GRAPH.INFO`, `GRAPH.MEMORY` | commands | `unknown command`. Constraints are Cypher here: `CREATE CONSTRAINT … REQUIRE p.name IS UNIQUE` |
| `GRAPH.LIST` | every graph key | `["default"]` |
| `AUTH` | Redis `AUTH <password>` or `AUTH <user> <password>` | only `AUTH <user> <secret>`, once `--auth-file` is set; the one-argument form is refused. See [DATA-HANDLING.md](DATA-HANDLING.md) |
| Several statements as one unit | Redis `MULTI` … `EXEC` queues them | `MULTI` is an unknown command; use `GRAPH.BEGIN`, then `GRAPH.COMMIT` or `GRAPH.ROLLBACK` on the same connection. See [ACID-GUARANTEES.md](ACID-GUARANTEES.md) |

**Client libraries.** FalkorDB's Python client appends `--compact` to every
query (`falkordb` 1.8.0, `graph.py`) and parses the three-part reply, so it
does not work against Samyama unchanged. Assume the same of FalkorDB's other
clients until you have checked them. Use a plain
Redis client and send `GRAPH.QUERY default <query> [name value …]` yourself, or
use the Python, TypeScript or Rust SDK over HTTP (see the [README](../README.md#clients)).

## 3. What to rewrite in your Cypher

These work unchanged: `CREATE INDEX FOR (p:Person) ON (p.age)`, the legacy
`CREATE INDEX ON :Person(name)` and `DROP INDEX ON :Person(name)`,
`CALL algo.pageRank('Person', 'KNOWS')`, `db.labels()`,
`db.relationshipTypes()`, `db.propertyKeys()`, `point()`, `distance()`,
`date()`, `duration()`, `randomUUID()`, `timestamp()`, `hasLabels()`, `CALL {}`
subqueries, `FOREACH`, `MERGE … ON CREATE SET … ON MATCH SET`, map projection,
list and pattern comprehension, and `reduce`.

| FalkorDB | Samyama |
|---|---|
| `CALL db.idx.fulltext.createNodeIndex('Movie', 'title')` | `CREATE FULLTEXT INDEX movie_title FOR (m:Movie) ON EACH [m.title]`. The index has a name, used to query and drop it |
| `CALL db.idx.fulltext.queryNodes('Movie', 'Matrix')` | `CALL db.index.fulltext.queryNodes('movie_title', 'Matrix')`, by index name |
| `CALL db.idx.fulltext.drop('Movie')` | `DROP FULLTEXT INDEX movie_title` |
| `OPTIONS {dimension: 3, similarityFunction: 'euclidean'}` | `OPTIONS {dimensions: 3, similarity: 'l2'}`. The keys are plural and renamed; the metrics are `cosine` and `l2` |
| `vecf32([0.1, 0.2])` | a plain list, `[0.1, 0.2]`. Remove every `vecf32`: inside `SET` it currently reports success and writes nothing ([#1897](https://github.com/samyama-ai/samyama-graph/issues/1897)) |
| `db.idx.vector.queryNodes('Person', 'embedding', k, vec)` | `db.index.vector.queryNodes('Person', 'embedding', vec, k)`: **the vector and `k` swap places** |
| `vec.euclideanDistance`, `vec.cosineDistance` | no equivalent function; rank with the vector index, or compute client-side |
| `CREATE INDEX FOR ()-[k:KNOWS]-() ON (k.since)` | not supported: no index on a relationship property |
| `CALL db.indexes()` | `SHOW INDEXES` |
| `CALL dbms.procedures()` | not available |
| `algo.WCC({nodeLabels: […]})` | `algo.wcc({label: 'Person'})`; the config keys are `label` and `edgeType` |
| `algo.betweenness({nodeLabels, relationshipTypes})` | `algo.betweenness({label: 'Person', edgeType: 'KNOWS'})` |
| `algo.BFS(a, 3, 'KNOWS')` | `MATCH (a)-[:KNOWS*1..3]->(n) RETURN DISTINCT n` |
| `algo.SPpaths`, or `shortestPath` in `RETURN` | `MATCH p = shortestPath((a)-[:KNOWS*]->(b)) RETURN p`. FalkorDB takes `shortestPath` only in `WITH` or `RETURN`; here it goes in `MATCH` |
| `indegree(a)`, `outdegree(a)` | `size([(a)<--() \| 1])`, `size([(a)-->() \| 1])` |
| `typeOf(x)` | `valueType(x)` |
| `toJSON(x)` | no equivalent; serialize client-side |
| `string.join(list, '-')` | `reduce(s = '', x IN list \| s + CASE WHEN s = '' THEN '' ELSE '-' END + x)` |
| `intern(s)` | drop it: it is a FalkorDB memory hint and changes no result |
| `LOAD CSV … AS row MERGE …` | fails today ([#1895](https://github.com/samyama-ai/samyama-graph/issues/1895)). `LOAD CSV … CREATE` works, and so does `UNWIND $rows AS row MERGE …` |

Two examples in Samyama's form:

```cypher
CREATE VECTOR INDEX person_emb FOR (p:Person) ON (p.embedding) OPTIONS {dimensions: 3, similarity: 'cosine'}

CALL db.index.vector.queryNodes('Person', 'embedding', [0.1, 0.2, 0.3], 5) YIELD node, score
RETURN node.name, score
```

```cypher
MATCH (a:Person) RETURN a.name, size([(a)<--() | 1]) AS indegree, size([(a)-->() | 1]) AS outdegree
```

`LOAD CSV` reads from the directory given to `--import-dir`: `'people.csv'` and
`'file://people.csv'` resolve inside it, and an absolute `file:///…` path is
accepted only if it lies inside it. `http` and `https` are refused; the reason is in
[MIGRATING-FROM-NEO4J.md](MIGRATING-FROM-NEO4J.md#3-moving-the-data).

## 4. Moving the data

**There is no FalkorDB importer yet.** `neo4j_import` has no FalkorDB
counterpart, and the tool half of #1787 is still open. What follows is the route
that was run end to end, written as steps for a script of your own.

**Read from FalkorDB with `--compact`.** Without it, lists and maps come back as
display strings such as `{name: Alice, tags: [x, y]}` that cannot be parsed
reliably. `toJSON(properties(n))` is no way round this: FalkorDB's JSON encoder
refuses dates and vectors. With `--compact` every value carries a type code, and
these are the ones a property can hold:

| Code | Type | Arrives as | Write it here as |
|---:|---|---|---|
| 1 | null | | omit the key |
| 2 | string | string | string |
| 3 | integer | integer | integer |
| 4 | boolean | `"true"` / `"false"` | boolean |
| 5 | double | string | float |
| 6 | list | list of typed values | list |
| 10 | map | flat `[key, value, …]` | map |
| 11 | point | `[latitude, longitude]` as strings | `point({latitude: …, longitude: …})` |
| 12 | vector | list of strings | list of floats |
| 14 | date | epoch seconds | `date(datetime({epochSeconds: …}))` |

Page with `ORDER BY id(n) SKIP $skip LIMIT $limit`, which in FalkorDB is
`CYPHER skip=0 limit=1000 MATCH (n) RETURN id(n), labels(n), properties(n) ORDER BY id(n) SKIP $skip LIMIT $limit`,
and the same for relationships with `id(a)`, `id(b)`, `type(r)` and
`properties(r)`.

**Write here in batches**, as one JSON parameter per batch. Labels and
relationship types cannot be parameters, so group the rows by label set and by
type and send one statement per group. Keep FalkorDB's id as a property with an
index on it, so the relationships can find their endpoints:

```cypher
CREATE INDEX FOR (n:Person) ON (n.falkor_id)

UNWIND $rows AS row CREATE (n:Person) SET n += row.props, n.falkor_id = row.id

UNWIND $rows AS row MATCH (a:Person {falkor_id: row.src}), (b:Person {falkor_id: row.dst}) CREATE (a)-[r:KNOWS]->(b) SET r += row.props
```

**Write `SET n += row.props`, not `SET n = row.props`.** With `=`, any other
property set on the same node in the same statement is silently dropped
([#1896](https://github.com/samyama-ai/samyama-graph/issues/1896)). Here that is
`falkor_id`, after which no relationship finds its endpoints and the import
reports success with every edge missing.

Create vector indexes after the load, with the options from section 3. Then
compare counts per label and per relationship type on both sides before you
switch over.

## 5. What you should know before you commit

- **One graph per instance.** Several FalkorDB graph keys become several
  instances, each with its own `--data-path` and ports.
- **Authentication is off by default.** Without `--auth-file` anyone who can
  reach the port can read and write. See [DATA-HANDLING.md](DATA-HANDLING.md)
  for credentials, TLS and the audit log.
- **Row order without `ORDER BY`** is guaranteed only for a label scan
  ([#1364](https://github.com/samyama-ai/samyama-graph/issues/1364)).
- **Leaving again** is documented, with what each route loses, in
  [LEAVING-SAMYAMA.md](LEAVING-SAMYAMA.md).

## 6. Tell us what refused

`benchmarks/compat/falkordb-idioms.cypher` is measured by
`tests/falkordb_compatibility_corpus.rs` on every run, so a change that stops one
of the 38 accepted idioms from parsing fails the build. If your report names a
cause that is not in that file, add the query. Run it on FalkorDB first, because
the corpus holds only what FalkorDB itself accepts.
