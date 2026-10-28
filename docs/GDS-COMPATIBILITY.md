# GDS procedure names

Neo4j GDS procedure names resolve to our algorithms where the two compute the
same answer. This document is the mapping and, more usefully, the list of names
that deliberately do **not** resolve.

Measured by `examples/gds_aliases.rs` (INT-04). Run it to reproduce the table:

```text
cargo run --release --example gds_aliases -- --json gds.json
```

## What resolves

A `gds.` name is canonicalised by dropping the `alpha.`/`beta.` maturity
segment, which carries no semantics, and a trailing `.stream`. Everything else
is left on the name, so it does not resolve.

| GDS | ours | note |
|---|---|---|
| `gds.pageRank.stream` | `pageRank` | |
| `gds.wcc.stream` | `wcc` | |
| `gds.scc.stream` | `scc` | |
| `gds.triangleCount.stream` | `triangleCount` | count per node, as in GDS |
| `gds.localClusteringCoefficient.stream` | `lcc` | renamed |
| `gds.labelPropagation.stream` | `cdlp` | renamed |
| `gds.louvain.stream` | `louvain` | |
| `gds.nodeSimilarity.stream` | `nodeSimilarity` | |
| `gds.degree.stream` | `degree` | |
| `gds.betweenness.stream` | `betweenness` | |
| `gds.closeness.stream` | `closeness` | |
| `gds.eigenvector.stream` | `eigenvector` | |
| `gds.kcore.stream` | `kcore` | |
| `gds.spanningTree.stream` | `mst` | renamed |
| `gds.maxFlow.stream` | `maxflow` | |
| `gds.shortestPath.dijkstra.stream` | `weightedPath` | **not** `shortestPath` |
| `gds.alpha.jaccard.stream` | `jaccard` | |
| `gds.alpha.adamicAdar.stream` | `adamicAdar` | |

`gds.shortestPath.dijkstra` is the one to read twice. GDS's dijkstra is
weighted; our `shortestPath` is an unweighted BFS and our `weightedPath` is the
weighted one. The obvious mapping is the wrong one, and it would have returned
plausible numbers while answering a different question.

## What does not resolve, and why

### Execution modes

Every algorithm here streams a row per node. GDS's `write` and `mutate` persist
a property and `stats` returns a summary. Those names are refused, with the
reason rather than "Unknown procedure":

```text
CALL gds.pageRank.write()
-> `gds.pageRank.write` is not supported: this engine streams results, and
   GDS's `write` mode returns a summary row instead. To store the result on
   each node, call `algo.pagerank({writeProperty: 'name'})`; it streams the
   rows as well.
```

The write itself exists: `algo.<name>({writeProperty: '...'})` stores each
node's result (see "Write-back" in `ALGORITHM-CONVENTIONS.md`). It is not
aliased onto `gds.<name>.write` because GDS's write mode returns one summary
row (`nodePropertiesWritten`, ...) where ours returns the rows, and running a
different result shape under a name the user already trusts is worse than not
recognising the name. `mutate` writes into a graph projection; projections
exist (`CREATE GRAPH PROJECTION`), but cannot hold a written property yet.

### Divergent semantics

| GDS | why it is not aliased |
|---|---|
| `gds.alpha.triangles` | lists one row per triangle; our `triangleCount` returns a count per node — same word, different result shape |

### Not implemented

`gds.fastRP.stream`, `gds.knn.stream`,
`gds.beta.node2vec.stream`, `gds.alpha.ml.linkPrediction.train`. These have no
counterpart here. They are listed so the absence is a statement rather than a
gap in the table.

### Named graphs

GDS projects a named graph with `gds.graph.project` and runs algorithms on it.
The counterpart here is a statement, not a procedure, and is not aliased:

| GDS | here |
|---|---|
| `gds.graph.project('social', 'Person', 'KNOWS')` | `CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS` |
| `gds.graph.project(..., {relationshipProperties: 'cost'})` | `... WEIGHT cost` |
| `gds.pageRank.stream('social')` | `CALL algo.pageRank({graph: 'social'})` |
| `gds.graph.list()` | `SHOW GRAPH PROJECTIONS` |
| `gds.graph.drop('social')` | `DROP GRAPH PROJECTION social` |

`gds.graph.project` itself still does not resolve: GDS's form takes several
labels, several types and per-type property maps, and a projection here takes
one of each. Mapping the call would accept projections it then built
differently. Without a projection, our algorithms run against the store
directly, so a port can also simply drop the `gds.graph.project` call.

## What is not measured

`examples/gds_aliases.rs` measures whether a **name resolves**. It does not run
GDS, so "the same answer" in the table above is read from our implementation and
GDS's documented definition — it is an assertion, not a comparison. Two entries
were wrong in the first draft of that list, so treat the equivalence column as
the weaker half of this document.

Comparing outputs against a GDS instance on the same graph is the measurement
that would settle it, and it has not been done.
