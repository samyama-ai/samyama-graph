# Named subgraphs

A named subgraph is a saved, read-only view of the tenant graph: the nodes with
some labels (optionally narrowed by a property predicate per label) and the
relationships of some types between them. Query it by using its name where a
graph name goes. Nothing is copied. Design: [ADR-043](ADR/ADR-043-named-subgraphs.md).

## Membership

- A node is in the view if one of its labels is listed and, when that label has
  a `WHERE` predicate, the node satisfies it.
- A relationship is in the view if its type is listed **and** both its endpoints
  are in the view.
- Nodes keep all their labels and properties. A scoped query returns what the
  same query would return on the whole graph with those filters written by hand.

Predicates read the node's own properties, by bare name (`status = 'Recruiting'`)
or as `n.status`. Comparisons, `AND`/`OR`/`NOT`, `IN`, `CASE`, lists and scalar
functions are allowed; parameters, aggregates, `labels()`, `id()`, patterns and
subqueries are not.

## RESP

```
GRAPH.SUBGRAPH.CREATE trials LABELS Trial,Drug EDGETYPES TESTS,SIMILAR WHERE Trial "status = 'Recruiting'"
GRAPH.SUBGRAPH.LIST
GRAPH.SUBGRAPH.INFO trials          # name, definition (JSON), nodes, relationships
GRAPH.QUERY trials "MATCH (t:Trial)-[:TESTS]->(d) RETURN t.name, d.name"
GRAPH.RO_QUERY trials "MATCH (n) RETURN count(n)"
GRAPH.SUBGRAPH.DROP trials          # the data is untouched
```

`WHERE` names the label it restricts; with a single label the label may be left
out. A label may have one `WHERE`; combine conditions with `AND`.

## HTTP

| Method | Path | |
|---|---|---|
| `POST` | `/api/subgraphs` | `{"name", "labels", "edge_types", "where": {"Label": "predicate"}}` → 201 |
| `GET` | `/api/subgraphs` | every definition with its node and relationship counts |
| `GET` | `/api/subgraphs/{name}` | one |
| `DELETE` | `/api/subgraphs/{name}` | drop it |
| `GET` | `/api/schema?subgraph={name}` | the schema response, restricted to the view |
| `POST` | `/api/query` | `{"query": "...", "graph": "{name}"}` |

Creating and dropping need write access to the tenant graph; reading needs read
access. A credential bound to the tenant can use its subgraphs.

## What is refused

- Writes through a subgraph: run them against the tenant graph.
- Forms the engine cannot yet confine to the view: pattern predicates and
  `EXISTS {}`, pattern comprehensions, the hierarchy functions, and operators
  that have not been taught the scope (for example shortest path and `CALL`
  procedures). These fail with "… is not supported inside a named subgraph
  yet" — they are never run against the whole graph.
- `CYPHER planner=` / `plan=` options, HTTP transactions and streamed results.

## Durability

Definitions are stored in the graph's index catalog: they survive a restart and
travel inside a `.sgsnap` snapshot.
