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
| **RDF / Turtle** | — | **nothing: not implemented, see below** |

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

## 4. RDF — not implemented

`src/rdf/mapping.rs` exists and every mapping function returns
`NotImplemented`. There is a triple store and Turtle, N-Triples, RDF/XML and
JSON-LD serialisers, but **nothing maps a property graph into triples**, so
there is no RDF export path today.

Spec requirement INT-07 asks for tested steps to Neo4j *and* RDF. Half of that
is here; the RDF half is not, and this page says so rather than describing a
route that ends in an error. Tracked as
[#1362](https://github.com/samyama-ai/samyama-graph/issues/1362).

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
