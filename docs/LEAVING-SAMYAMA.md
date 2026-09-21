# Leaving Samyama

How to get your graph out and into something else. Published because the
alternative — finding out at the moment you need it — is what lock-in feels
like from the inside.

The two routes that move a whole graph — the Cypher script and the snapshot —
are executed by tests in CI, so this page cannot go stale without something
going red. The Parquet example and the `gunzip` one-liner are illustrations and
are not. Where a path does **not** work, this page says so rather than leaving
it out.

---

## What you get out, and what it costs

| Route | Carries | Use it to move to |
|---|---|---|
| **Cypher script** | nodes, labels, properties, relationships, relationship properties | Neo4j, Memgraph, FalkorDB, another Samyama |
| **`.sgsnap` snapshot** | the above, plus node creation timestamps and hierarchy-index declarations | another Samyama |
| **Parquet / Arrow** | one *result set* | a warehouse, pandas, DuckDB |
| **RDF / Turtle** | nodes, labels, properties, relationships, relationship properties (reified), IRIs | a triple store, SPARQL tooling |
| **GraphML** | nodes, labels, relationships, relationship types, scalar properties; lists and temporals as text | Gephi, yEd, Cytoscape, NetworkX |

Nothing here needs a licence key, a support ticket, or a running network
connection to us.

---

## 1. Cypher script — the route with no vendor in it

```bash
cargo run --release --example export_cypher -- --out graph.cypher
```

It writes standard Cypher — this is the generated output, not something to
type, so the block is marked `ignore`: `doc_check` parses one statement per
block and this is four. The statements themselves are checked by running them,
in `tests/export_cypher_round_trip.rs`.

```cypher,ignore
CREATE INDEX ON :_Imported(_sgid);
CREATE
  (:Person:_Imported {_sgid: 1, age: 30, name: 'Alice'}),
  (:Company:_Imported {_sgid: 3, name: 'Acme'})
;
MATCH (a:_Imported {_sgid: 1}), (b:_Imported {_sgid: 3}) CREATE (a)-[:WORKS_AT {since: 2019}]->(b);

MATCH (n:_Imported) REMOVE n:_Imported REMOVE n._sgid;
```

### Why the `_sgid` property is there

A `CREATE` cannot refer to a node it did not create in the same statement, so
the relationships need some way to find their endpoints. The export writes the
original node id into `_sgid`, indexes it, matches on it, and removes it in the
final statement. It is our scaffolding, not your data, and the last line takes
it away.

The alternative — one enormous statement binding every node to a variable —
stops parsing somewhere in the low thousands of nodes on every engine we tried.

### What is tested, and what is not

`tests/export_cypher_round_trip.rs` feeds the script back into a fresh Samyama
and compares node count, edge count, every (labels, properties) pair and every
(source, type, properties, target) triple. It also pins three things that are
easy to get silently wrong: an apostrophe inside a string value, a float whose
value is a whole number (`2.0` written as `2` comes back as an integer, which
changes the property's *type* across the move), and a node with no labels.

**That is a test of our dialect, not of Neo4j's.** The script is standard
Cypher and we have no Neo4j in CI, so "Neo4j will accept this" is a reasonable
expectation and not a measured fact. If you hit a difference, please open an
issue with the statement that failed — that is exactly the report this page
wants.

---

## 2. `.sgsnap` snapshot — to another Samyama

```bash
curl -X POST http://localhost:8080/api/snapshot/export -o graph.sgsnap
```

**Read the loss report.** The export tells you what it did not carry, in the
`X-Samyama-Export-Dropped` response header and in the snapshot's own header
line:

```json
[{"what": "property_indexes", "count": 2,
  "detail": "... Re-create: CREATE INDEX ON :Person(email); ..."},
 {"what": "unique_constraints", "count": 1,
  "detail": "Uniqueness is not enforced on the restored graph until ..."},
 {"what": "edge_creation_timestamps", "count": 41203,
  "detail": "... Node timestamps do survive."}]
```

A row appears only when there was something to lose, so an empty list means
this export took everything the format can take.

To read a snapshot without Samyama at all: it is gzipped JSON-lines. Line 0 is
the header, then one object per node (`"t":"n"`) and per relationship
(`"t":"e"`).

```bash
gunzip -c graph.sgsnap | head -1 | python3 -m json.tool
```

---

## 3. Parquet / Arrow — for a warehouse, not for a graph

```bash
curl -X POST http://localhost:8080/api/query/export \
  -H 'Content-Type: application/json' \
  -d '{"query": "MATCH (n:Person) RETURN n.name AS name, n.age AS age", "format": "parquet"}' \
  -o people.parquet
```

This exports a **result set**, not a graph. The topology is whatever your query
projected, so moving a graph this way means writing one query for nodes, one
per relationship type, and reassembling the structure on the far side. For that
job the Cypher script above is less work and loses less.

---

## 4. RDF — a round trip, with three named losses

`GraphToRdfMapper::sync_to_rdf` writes the graph into an `RdfStore`, which the
existing Turtle, N-Triples, RDF/XML and JSON-LD serialisers can write out.
`RdfToGraphMapper::map_to_graph` reads it back. The mapping:

| Property graph | RDF |
|---|---|
| node | a subject IRI |
| label | `rdf:type {base}class/{Label}` |
| node property | `{base}prop/{key}` → a **typed** literal |
| relationship | `{source} {base}rel/{TYPE} {target}` |
| relationship property | an `rdf:Statement` reifying the triple |

Literals are typed, so an integer comes back an integer rather than `"30"`.
An imported resource keeps its original IRI in a `__rdf_iri` property and
export prefers it, so re-exporting an imported graph reproduces the IRIs that
came in rather than inventing new ones.

**What it costs, in the three places it costs something:**

- **Parallel relationships collapse.** RDF has no parallel edges, so two plain
  `KNOWS` between the same pair are one triple and come back as one
  relationship. `sync_to_rdf` returns a `SyncReport` whose
  `duplicates_collapsed` counts them — the loss is reported, not discovered.
- **Node ids change.** Identity crosses as the IRI, not the integer id, which
  is the same trade the Cypher route makes with `_sgid`.
- **Arrays, maps, vectors and temporals** cross as a JSON literal typed
  `https://samyama.ai/rdf/PropertyValue`, not as RDF lists or `xsd:dateTime`.
  They round-trip through Samyama exactly; another triple store will see a
  string.

Verified by running it rather than by reading it:
`cargo run --release --example rdf_round_trip` exports, imports, exports again
and exits non-zero if the nodes, relationships, property types or IRIs did not
survive. `tests/rdf_round_trip.rs` pins the same ground, including the
collapse above so it stays a documented loss.

Spec requirement INT-07 asks for tested steps to Neo4j *and* RDF; both halves
are now here. Closed [#1362](https://github.com/samyama-ai/samyama-graph/issues/1362).

---

## 5. GraphML — for the tools that draw graphs

```bash
cargo run --release --example graphml_export -- --snapshot graph.sgsnap --out graph.graphml
```

Gephi, yEd, Cytoscape and NetworkX all read GraphML. It carries the topology
faithfully and is the narrowest of these routes on **types**, for one reason
worth understanding before you rely on it: GraphML declares each attribute once,
up front, with a single type.

```xml
<key id="nd1" for="node" attr.name="age" attr.type="long"/>
```

A property graph does not promise that. If `age` is a number on one node and
the string `"unknown"` on another — and nothing here rejected that on the way
in — the declaration has to widen to `string`, and every `age` in the file
stops being a number to any reader. The export **names** the attributes this
happened to rather than letting you find out in Gephi:

```
  What GraphML could not carry:
  - 1 attribute(s) had values of more than one type, so the declaration widened
    to `string`: node.age
  - 1 list/map/vector value(s) written as JSON inside a string attribute.
```

The full list of narrowings:

| In the graph | In the file |
|---|---|
| mixed-type attribute | one `string` declaration; values written as text, named in the report |
| list, map, vector | JSON text inside a `string` attribute — content kept, structure invisible to a reader that does not parse it |
| date, time, datetime, duration | ISO-8601 text. Lossless as text, not as a type |
| labels | a `labels` attribute, space-separated — the convention every tool here follows, and ambiguous for a label containing a space, which is reported |
| relationship type | a `label` attribute on the edge |

`--json report.json` writes the same report machine-readably. The output is
byte-identical across runs of the same graph, so it can be diffed and
checksummed.

---

## What no export carries

Independent of route:

- **MVCC version history.** You get the current version of every node, not the
  chain behind it.
- **Query and result caches.** Rebuilt on the far side, which costs a warm-up
  and nothing else.
- **Tenant configuration and quotas.** Re-declare them wherever you land.
- **Statistics.** Recomputed from the data.

---

## If a step here does not work

That is a bug in this page, and it is the kind we want reported. The steps are
run by `tests/export_cypher_round_trip.rs` and `tests/snapshot_loss_report.rs`
on every pull request; a failure in CI means this document has gone stale
before you found it.
