# Algorithm conventions

Every graph algorithm answers a question the mathematics does not fully settle.
Does it follow edge direction? Does it use weights? Is a self-loop a triangle?
What does it return for a node nothing can reach? When two answers tie, which
one comes back? Is the result normalised, and by what?

Two implementations that disagree on any of these return different numbers from
the same graph, and both are right. So the answers are written down here, one
row per algorithm, for every algorithm callable from Cypher.

**These rows were read from the implementation, not from its documentation.**
Where a doc comment and the body disagreed, the body won and the mismatch was
filed. A convention here is therefore a statement about what the code does
today, and CI holds it to that: `CH-ALGO-COV` fails when a callable algorithm
has no row, or has a row with a cell it has not filled in.

What this document does **not** claim is that every convention is the *right*
one. Several are inherited from a first implementation and differ from NetworkX
or from LDBC; those are marked **(differs)** and each has an issue. Writing them
down is what made them visible.

Conventions are stated for the algorithm as the engine calls it. `algo.*`,
`samyama.*`, `gds.*` and the bare name are one algorithm: the dispatcher strips
the namespace and lower-cases, so `algo.pageRank`, `pagerank` and
`gds.pageRank` are the same row.

## How to read the columns

| Column | Means |
|---|---|
| **Directedness** | Which edges the algorithm walks. "Out-edges" follows direction; "both" walks out- and in-edges; "symmetrised" builds an undirected neighbour set first. A `bidirectional` argument, where one exists, is named. |
| **Weights** | Whether an edge property is read, and what stands in when it is absent. The view holds one weight array aligned to out-edges, so **nothing that walks in-edges can be weighted**. |
| **Self-loops** | Whether an edge from a node to itself contributes. |
| **Disconnected** | What comes back for a node, or a pair, that cannot be reached. |
| **Tie-breaking** | What fixes the answer when several are equally good. "Node id ascending" means reproducible across runs; anything else is called out. |
| **Normalisation** | Whether the score is divided by something, and by what. |

## Centrality

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| PageRank | pageRank | Out-edges: rank flows to successors, divided by out-degree | Ignored | Counted: a self-loop feeds its own node | Every node is returned, at least `(1-d)/n` | Returned as a map; ranking is the caller's | Starts at `1/n`; sums to 1 only when dangling mass is redistributed |
| Degree centrality | degree, degreeCentrality | Both, always: out-degree + in-degree. The `bidirectional` argument is ignored **(differs)** | Ignored | Counted twice, once per direction | Isolated node scores 0 | None produced | Divided by `n-1` |
| Closeness | closeness, closenessCentrality | `bidirectional` true walks both; false walks in-edges only, so the score is "how near everything is **to** this node" | Ignored: hop counts | Never re-entered; the source is at distance 0 | Unreachable nodes leave the sum; a node that reaches nothing scores 0 | None produced | Wasserman-Faust: `reachable/total × reachable/(n-1)` |
| Betweenness | betweenness, betweennessCentrality | Follows `bidirectional` | Ignored: hop counts | Excluded by the shortest-path level test | Unreachable nodes contribute nothing and stay 0 | None produced | `1/((n-1)(n-2))` always, with no halving for an undirected reading **(differs)** |
| Harmonic | harmonic, harmonicCentrality | Follows `bidirectional`; false walks in-edges only | Ignored: hop counts | Excluded (`d > 0`) | Unreachable adds 0; isolated node scores 0 | None produced | None: the raw sum of `1/d`, not divided by `n-1` |
| Eigenvector | eigenvector, eigenvectorCentrality | Out-edges; `bidirectional` adds the reverse pass | Ignored | Counted | A node with no in-edges settles to 0; an all-zero iterate refuses | None produced | Unit L2 each iteration; refuses if it has not converged |
| Katz | katz, katzCentrality | In-edges only; no `bidirectional` | Ignored | Counted | A node with no in-edges keeps `beta`, not 0 | None produced | Unit L2 on the converged result; refuses otherwise |
| HITS | hits, hubsAndAuthorities | Out-edges for both passes | Ignored | Counted | A sink gets authority 0; both vectors always full length | None produced | Each vector to L1 sum 1 |
| Personalised PageRank | personalizedPageRank, personalisedPageRank | Out-edges | Ignored | Counted | A component the teleport never touches converges to ~0 and is still returned | None produced | Mass-conserving; duplicate source ids get double teleport weight **(differs)** |
| ArticleRank | articleRank | Out-edges | Ignored | Counted | Dangling nodes are skipped and their mass is dropped **(differs)** | None produced | None: scores do not sum to 1 |
| VoteRank | voteRank | Votes arrive from in-edges; suppression applies to the elected node's voters | Ignored | Counted: a node can vote for itself | A node with no in-edges is never elected, so fewer than `k` may come back | Strict `>` keeps the lowest node index among ties | None; the decrement is `edges/n` |
| k-core | kcore, coreNumber | Both directions by degree, not deduplicated | Ignored | Removed before peeling | An isolated node peels first, at the core number then current | Lowest index among equal minimum degree | None: integer core numbers |

## Community and clustering

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| Label propagation | cdlp, labelPropagation | Both: a label is counted over successors and predecessors, so a reciprocal edge counts twice **(differs)** | Ignored | Counted twice, once per direction | An isolated node keeps its initial label, its own node id | Deterministic: among the most frequent labels, the smallest node id wins; updates are synchronous | None; labels are raw node ids, never renumbered |
| Louvain | louvain | Symmetrised adjacency; a reciprocal pair collapses to one entry by `max`, not sum **(differs)** | Used: the edge weight property, 1.0 when absent or non-numeric | Input self-loops dropped; contraction creates self-loops that are excluded from the move gain but kept in the degree | A node with no edges stays a singleton; an edgeless graph returns the identity partition | Deterministic: nodes swept in index order, candidate communities sorted by id, a move needs a gain above `1e-12` | Labels renumbered from 0 in order of first appearance |
| Modularity | modularity | Symmetrised adjacency | Used: 1.0 when absent or non-numeric | Skipped explicitly | Isolated nodes contribute nothing; an edgeless graph refuses | Nothing to break: it scores a given partition. Community sums are ordered by id so the float sum is reproducible | Newman's Q, divided by the total adjacency weight |
| WCC | wcc | Undirected in effect: union over successors is enough, since every edge appears once | Ignored | Irrelevant: a union of a node with itself does nothing | Every node is assigned; an isolated node is its own component | The component id is the union-find root, chosen by rank — deterministic but arbitrary **(differs)** | None |
| SCC | scc | Out-edges (Tarjan) | Ignored | Irrelevant to membership | Every node is in exactly one component; an isolated node is a singleton | Component ids in DFS-completion order, driven by index order | None |
| LCC | lcc | Undirected by default: the neighbour set is the union of successors and predecessors. A directed reading is available and uses Fagiolo's formula | Ignored | Excluded from the neighbour set | Degree below 2 scores 0, and those nodes are **in** the average's denominator | Nothing to break: integer counts per node | Undirected `2t/(d(d-1))`; directed divides by `d_tot(d_tot-1) - 2d_bi`, times 2 |
| Triangle count | triangleCount | Symmetrises itself; the caller cannot ask for a directed count | Ignored | Cannot contribute: the `u<v<w` enumeration excludes them | No effect | Each triangle enumerated once; parallel edges collapsed | None: a raw count |
| Transitivity | transitivity | Follows `bidirectional` | Ignored | Removed from the neighbour set | No effect | Fixed by the enumeration order | `3 × triangles / triples`; refuses when there are no triples |
| Square clustering | squareClustering | Follows `bidirectional` | Ignored | Removed from the neighbour set | No effect: it is a local measure | Nothing to break | Lind's ratio; refuses when the denominator is 0 |
| k-truss | kTruss, truss | Follows `bidirectional` | Ignored: support is a triangle count | Removed from the neighbour set | No effect: peeling is local to an edge | Drops are batched per round, so hash order cannot change the result | None: surviving nodes |
| Rich club | richClub, richClubCoefficient | Follows `bidirectional` | Ignored | Removed from the neighbour set | No effect | Each edge counted once | `2E/(m(m-1))`; refuses with fewer than two members |

## Paths and traversal

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| Shortest path | shortestPath | Out-edges | Ignored: hop counts | Never re-entered | No path: nothing comes back | The first parent to reach a node wins, in FIFO order over adjacency — deterministic for a given graph | Cost is the hop count; the path is returned source-first |
| Weighted path | weightedPath | Out-edges | Used, 1.0 when absent. A negative weight is **refused** by the Cypher call, which names `bellmanFord`; the algorithm itself would skip it and return a path through a different graph | Never improves a distance | No path: nothing comes back | Equal-cost pops are in heap order, which is not defined by node id **(differs)** | None: the raw sum of weights |
| A* | aStar | Out-edges | Used, 1.0 when absent. A negative weight is **refused** by the Cypher call; the algorithm itself has no guard, and its quantised heap key would order a negative edge wrongly | No effect when non-negative | No path: nothing comes back | Ties break to the smaller node index. Costs within `1e-6` tie artificially, since the heap key is quantised **(differs)** | None: the exact f64 sum |
| All shortest paths | allShortestPaths | Out-edges | Ignored: hop counts | Never a predecessor | No path: an empty result | Every tied path is returned, then sorted lexicographically by node id | Cost is the hop count per path |
| Yen's k shortest | yens | Out-edges | Used, 1.0 when absent; a parallel edge contributes its **minimum** weight. A negative weight is **refused** by the Cypher call | Not special-cased | Empty when the first path fails; fewer than `k` when candidates run out | Sorted by cost, then by the path lexicographically | None: raw weight sums |
| Bellman-Ford | bellmanFord | Out-edges | Used, 1.0 when absent. Negative weights are correct here; a negative cycle reachable from the source refuses | An ordinary edge; a negative self-loop is a negative cycle | Unreachable nodes come back as "no distance" | Nothing to break: distances only | None: raw distances |
| All-pairs hops | allPairs, allPairsShortestPath, allPairsHops | Out-edges: `(u,v)` and `(v,u)` are different pairs | Ignored: hop counts | Excluded: `(s,s)` is never emitted | Unreachable pairs are **absent**, not infinity | Nothing to break | None |
| Wiener index | wienerIndex | Out-edges, over ordered pairs | Ignored: hop counts | Excluded | Refuses when any ordered pair is unreachable **(differs)** | Nothing to break | None: the raw sum over ordered pairs, so twice the undirected convention |
| Transitive closure | transitiveClosure | Out-edges | Ignored | `(s,s)` appears when a real cycle, including a self-loop, returns to `s` | Unreachable pairs are absent | All pairs returned, sorted | None |
| DAG longest path | dagLongestPath, longestPath | Out-edges | Ignored: longest by edge count | A self-loop makes the graph cyclic, and the call refuses | The best endpoint is chosen across all components | Smallest index among equally long endpoints | None |
| Random walk | randomWalk | Out-edges | **Ignored**: the step is uniform over successors, not weight-proportional **(differs)** | Traversed like any edge | A dead end ends the walk early; there is no restart | Not applicable | Reproducible from the seed: a fixed xorshift, no clock. Seeds `2k` and `2k+1` give the same walk **(differs)** |
| Topological sort | topologicalSort, toposort | Out-edges | Ignored | A self-loop keeps the in-degree above 0 forever, so the graph reports as cyclic | All components emitted in one order, interleaved by index | The smallest ready index | None |
| Cycle detection | cycleDetection, findCycle | Out-edges | Ignored | Detected: a self-loop is a one-node cycle | The search restarts from every unvisited node | The first back edge, from the lowest start index | None |
| Max flow | maxFlow | Out-edges; each arc gets a reverse arc at capacity 0 | Used as capacity, 1.0 when absent; parallel edges sum | Inert | An unreachable sink is a flow of 0. A node that is not in the graph, and a source equal to the sink, are refused | The augmenting path comes from a hash-ordered scan, so **which** path is chosen varies; the flow value does not **(differs)** | None: the raw flow |
| MST | mst | Undirected: grows over both directions | Used, 1.0 when absent | Never added | **Only the component holding index 0 is returned**, silently **(differs)** | Equal weights are resolved by heap order, not by node id **(differs)** | None: the raw total |
| Bridges | bridges | Symmetrises itself; a reciprocal pair is **one** undirected edge, parallel edges are **many** (see below) | Ignored | Skipped explicitly | Every component gets its own search | All bridges returned, sorted | None |
| Articulation points | articulationPoints | As bridges: the same traversal and the same multiplicity rule | Ignored | Skipped explicitly | A root is judged per component | Ascending node index | None |

| Biconnected components | biconnected, biconnectedComponents | Follows `bidirectional` | Ignored | Removed from the neighbour set | Every component searched; isolated nodes produce none | Each component sorted, then the list sorted and deduplicated | None |


### What "symmetrises itself" counts as one edge

The column above had no room for this and the question had not been asked, so
`bridges` reported two parallel `a -> b` edges as a bridge: removing either
leaves the graph connected, so it is not one (samyama-graph#1308).

The multiplicity between two nodes under an undirected reading is
**`max(forward, reverse)`**:

| forward | reverse | undirected | why |
|---|---|---|---|
| 1 | 0 | 1 | a plain edge |
| 1 | 1 | 1 | a reciprocal pair is **one** undirected edge |
| 2 | 0 | 2 | genuinely parallel |
| 2 | 1 | 2 | one pair collapses; the leftover stands on its own |

**A reciprocal pair is one edge** because that is how the graph is drawn and how
the data was written: `x -> y` with `y -> x` is a single connection, and cutting
it disconnects the graph, so `x <-> y` is a bridge. It is also what the
traversal already did, so adopting it moves no answer except the defect.

The alternative — a reciprocal pair counting as **two**, which is what Neo4j
GDS's undirected projection does — would stop `x <-> y` being a bridge and
would change every algorithm that symmetrises: `bridges`,
`articulationPoints`, `countTriangles` and everything reached through
`neighbours()`. That is a larger product decision; it is recorded here as the
road not taken rather than left implicit.

## Shape, similarity and link prediction

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| Eccentricity | eccentricity | Follows `bidirectional` | Ignored: hop counts | Irrelevant | Refuses for any node that cannot reach every node | Nothing to break | None |
| Diameter | diameter | Follows `bidirectional` | Ignored: hop counts | Irrelevant | Refuses unless the graph is connected under the chosen reading | Nothing to break | None |
| Radius | radius | Follows `bidirectional` | Ignored: hop counts | Irrelevant | Refuses under the same test as diameter | Nothing to break | None |
| Average neighbour degree | averageNeighborDegree, averageNeighbourDegree | Follows `bidirectional`; the neighbour set is deduplicated **only** when bidirectional, so parallel edges inflate the directed reading **(differs)** | Ignored | Excluded | An isolated node scores 0 rather than refusing | Nothing to break | The mean over the node's own neighbours |
| Degree assortativity | degreeAssortativity | Follows `bidirectional`; each undirected edge taken once, then both orderings added | Ignored | Excluded | Computed over whatever edges exist | Nothing to break | Pearson r in [-1,1]; refuses on a regular graph, where it is undefined |
| Global efficiency | globalEfficiency | Follows `bidirectional` | Ignored: hop counts | Excluded | Unreachable ordered pairs contribute 0, so the result stays finite | Nothing to break | Divided by `n(n-1)`; refuses below two nodes |
| Bipartite check | bipartite, bipartiteSets | Follows `bidirectional` | Ignored | Removed, so a self-loop never makes a graph non-bipartite **(differs)** | Each component coloured from its lowest unvisited index; all merged into two sets | Component seed is the lowest index, coloured 0 | None |
| Maximal matching | maximalMatching, matching | Follows `bidirectional` | Ignored | Removed | Matches independently per component | Edges normalised to `(min,max)` and taken in sorted order | None; maximal, not maximum |
| Greedy colouring | colouring, coloring, greedyColouring | Follows `bidirectional` | Ignored | Removed, so a self-loop never blocks a colour | Colours are reused across components | Nodes by descending degree then index; the smallest unused colour | None: colour indices from 0 |
| Dominating set | dominatingSet | Follows `bidirectional` | Ignored | Removed; an isolated node is always chosen | Every component is covered | Strict `>` keeps the lowest index; output sorted | None |
| Reciprocity | reciprocity | Out-edges, both for the edge and for its reverse | Ignored | Excluded | Graph-level; refuses when there are no non-self edges | Nothing to break | `mutual / edges`, where parallel edges count once **(differs)** |
| Node similarity | nodeSimilarity, jaccard | Symmetrised neighbour sets | Ignored | Excluded from the neighbour set | A node with no neighbours is skipped; pairs scoring 0 are dropped | Score descending, then target index ascending | Jaccard: intersection over union |
| Cosine similarity | cosine, cosineSimilarity | Symmetrised neighbour sets | Ignored | Excluded from the neighbour set | Refuses when either neighbour set is empty | Nothing to break | Intersection over `sqrt(\|A\|\|B\|)` |
| Overlap coefficient | overlap, overlapCoefficient | Symmetrised neighbour sets | Ignored | Excluded from the neighbour set | Refuses when the smaller set is empty | Nothing to break | Divided by `min(\|A\|,\|B\|)` |
| Common neighbours | commonNeighbors, commonNeighbours | Symmetrised neighbour sets | Ignored | Excluded from the neighbour set | Pairs scoring 0 never appear | Score descending, then both node ids ascending | None: a raw count |
| Adamic-Adar | adamicAdar | Symmetrised neighbour sets | Ignored | Excluded from the neighbour set | Pairs scoring 0 never appear | Score descending, then both node ids ascending; the sum is taken in sorted order so the float is reproducible | None: the raw sum of `1/ln(deg)` |
| Effective size | effectiveSize | Symmetrised neighbour sets | Ignored: the unweighted form | Excluded from the neighbour set | Refuses for an isolated node | Nothing to break | None: `n - 2t/n`, between 1 and the degree |
| Constraint | constraint, burtConstraint | Symmetrised neighbour sets | Ignored: each tie is a uniform `1/\|N\|` **(differs)** | Excluded from the neighbour set | Refuses for an isolated node | The summation order is sorted so the float sum is reproducible | None: Burt's sum of squared proportions |

## Temporal and causal

These four walk edge timestamps, so reachability is not transitive: an edge that
fired before you arrived is not traversable. Times come from an edge property,
falling back to the edge's own `created_at`.

### Which walk each one shows you

All four yield `path` and `times`, and the choice of *which* walk is part of the
answer rather than an implementation detail (ALGO-15):

| Algorithm | The walk it returns |
|---|---|
| `temporalReachability`, `propagationRanking` | The walk that produced the **earliest arrival** at that node. Not a walk that exists — the one the traversal took to get there first, so the last edge time equals the reported arrival. |
| `temporalShortestPath` | The same walk, to one named target. |
| `symptomExplanation` | The walk to the symptom whose constraint set `onset`. A candidate may explain five symptoms by five walks and a row can show one; showing the **binding** one keeps the path and the onset describing the same journey, and it is the walk that explains why the fit is no tighter than it is. |

`times[i]` is the moment the edge between `path[i]` and `path[i+1]` fired, so it
is one shorter than `path`, and the sequence never decreases. The times are
returned beside the path because otherwise the path is a claim the caller has to
take on trust; with them it can be checked against their own edges.

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| Temporal reachability | temporalReachability | Out-edges, forward in time | Ignored: timestamps are used instead | Traversed, but can never lower an arrival time | Unreachable nodes are omitted, and so are the sources | Arrival time ascending, then node id ascending | None: raw timestamps |
| Temporal shortest path | temporalShortestPath | Out-edges, forward in time | Ignored: timestamps are used instead | Never becomes a parent edge | No time-respecting path: nothing comes back. Target equal to source gives a one-node path | The edge that last improved the arrival time, in slot order | None: "shortest" is earliest arrival, not hops or duration |
| Propagation ranking | propagationRanking | As temporal reachability, which it delegates to | Ignored | As temporal reachability | As temporal reachability | Arrival time ascending, then node id ascending | None |
| Symptom explanation | symptomExplanation | In-edges, backward in time | Ignored: timestamps are used instead | Only the symptom under consideration is skipped | Nodes explaining nothing are omitted | Symptoms explained descending, then latest onset descending, then node id ascending | None: a count, a timestamp and the binding walk |

## Write-back

`writeProperty` stores each node's result as a node property (ALGO-06):

```cypher
CALL algo.pageRank({writeProperty: 'rank'}) YIELD node, score
RETURN node.name, node.rank, score
```

It is accepted by every algorithm that returns exactly one value per node, and
stores the column below. The call still returns the same rows a plain call
returns, and `node` in those rows carries the written property.

| Algorithm | Procedure | Column stored |
|---|---|---|
| PageRank | pageRank | score |
| ArticleRank | articleRank | score |
| Katz | katz | score |
| Personalised PageRank | personalizedPageRank | score |
| Degree, closeness, betweenness, harmonic, eigenvector centrality | degree, closeness, betweenness, harmonic, eigenvector | score |
| Core number | kcore | score |
| Average neighbour degree | averageNeighborDegree | score |
| Weakly connected components | wcc | componentId |
| Strongly connected components | scc | componentId |
| Label propagation | cdlp | communityId |
| Louvain | louvain | communityId |
| Local clustering coefficient | lcc | coefficient |
| Square clustering | squareClustering | coefficient |
| Eccentricity | eccentricity | eccentricity |
| Bipartite sides | bipartite | side |
| Greedy colouring | colouring | colour |
| Effective size, Burt's constraint | effectiveSize, constraint | value |
| Bellman-Ford | bellmanFord | distance |

The list is `AlgorithmOperator::WRITE_BACK`; `tests/algorithm_write_back.rs`
runs every entry, under every alias, and fails if this table misses one.

What it does, exactly:

- **It is a write.** The call is routed like a `SET`: through the write
  executor, journalled for persistence, refused by `GRAPH.RO_QUERY` and by
  `read_only` requests.
- **All or nothing.** Each value is set the way `SET` sets it, so indexes and
  constraints apply. A value the store refuses (a unique constraint, say)
  fails the statement, and the statement is undone whole.
- **Computed first, written after.** The algorithm finishes on the graph as it
  was before any value is written.
- **Only the nodes it returns.** A label filter, a source node or an
  unreachable node limits the rows, and nodes with no row are left as they
  were. Re-running Bellman-Ford from a different source leaves the old
  distance on a node the new source cannot reach.
- **A null result removes the property**, as `SET n.p = null` does: a node
  with no eccentricity keeps no stale value from an earlier run.

What it refuses, before anything is written:

- an algorithm with no single value per node (paths, pairs, node sets,
  whole-graph numbers, `hits` with its two scores) -- write its rows with
  `SET`;
- a `writeProperty` that is not a non-empty string;
- `gds.<name>.stream({writeProperty: ...})`: stream mode does not write;
- `mutate` / `mutateProperty`: it writes into a graph projection, and a
  projection cannot hold a written property yet. Projections themselves exist
  (see "Graph projections" below); `writeProperty` works on one.

Config is read from a literal map. A config map passed as a parameter
(`$config`) is not read by any algorithm here, so a `writeProperty` inside one
is not honoured either.

## Graph projections

A projection keeps an algorithm's view of the graph under a name, so several
algorithms run on one build of it instead of each building its own (ALGO-05):

```cypher
CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS WEIGHT cost

CALL algo.pageRank({graph: 'social'}) YIELD node, score

CALL algo.louvain({graph: 'social'}) YIELD node, communityId

SHOW GRAPH PROJECTIONS

REFRESH GRAPH PROJECTION social

DROP GRAPH PROJECTION social
```

| Statement | What it does |
|---|---|
| `CREATE GRAPH PROJECTION name [IF NOT EXISTS] [NODES :L] [RELATIONSHIPS :T] [WEIGHT p]` | Builds and keeps the view. A filter left out takes all of it. `IF NOT EXISTS` leaves an existing one as it is and reports it. |
| `REFRESH GRAPH PROJECTION name` | Rebuilds it from the data as it is now, with the filters it was created with. |
| `DROP GRAPH PROJECTION name [IF EXISTS]` | Releases it. |
| `SHOW GRAPH PROJECTIONS` | One row each, by name: `name`, `node_label`, `relationship_type`, `weight_property`, `nodes`, `relationships`, `bytes`, `stale`, `status`. |

The filters are the ones every algorithm's view is built from: one node label,
one relationship type, one numeric weight property. A relationship whose weight
is missing or not a number weighs 1.0.

What it promises:

- **The same answer.** An algorithm on a projection returns exactly the rows it
  returns on the graph the projection was built from;
  `tests/graph_projections.rs` checks this algorithm by algorithm.
- **A snapshot.** Writes after the build do not reach it. `stale` turns true
  once data may have changed since: any change counts, inside the projection
  or not, so it can be true after a write the projection does not hold, and
  is never false after one it does. `REFRESH` brings it up to date.
- **Memory is reported, not capped.** `bytes` is the view's resident size,
  also listed as kind `graph_projection` in `/api/memory` and `/metrics`. No
  budget limits it yet (ALGO-12).
- **In memory only.** A restart forgets projections, as it forgets hierarchy
  indexes.
- **Writes, except SHOW.** `CREATE`, `REFRESH` and `DROP` need the write path,
  because they hold or free memory for every later query; a read-only
  connection can list projections and run algorithms on them.

What it refuses:

- `graph` together with a label, a relationship type or a weight, positional
  or in the config map: the projection already decides those;
- `graph` on the algorithms that read more than a projection keeps: the four
  temporal primitives (relationship times), `pca` (node properties) and
  `or.solve`;
- a `graph` that names no projection, or is not a non-empty string;
- a `graph` an algorithm never takes a view from, rather than ignoring it.

## Embeddings

**`gds.fastRP` and `gds.node2vec` are the one exception to "`gds.*` is the
same row" in the introduction above.** Both names are deliberately refused
rather than resolved (`GDS_DIVERGENT` in `operator.rs`): GDS's node2vec is
skip-gram-trained and ours is not, and FastRP's resemblance to GDS's has not
been checked against GDS's own defaults and knobs. `algo.fastRP`,
`samyama.fastRP`, the bare `fastRP`, and the `node2vec` equivalents are
unaffected -- only the `gds.` spelling is blocked, the same way an unrelated
name like `gds.alpha.triangles` is.

Both write a vector as a node property (`writeProperty`, default `embedding`)
rather than only streaming it -- ML-06 asks for the result "stored as vector
properties". They write whether or not `writeProperty` is given, which the
algorithms under "Write-back" above do only when asked; that is the
deliberate exception, documented at the call site. Both require write access (`CALL ... YIELD node, embedding` through
`MutQueryExecutor`) for that reason, and both are seeded and reproducible: no
call to a clock or a process RNG, every random draw is a pure function of
`(seed, node index, dimension index)` computed with SplitMix64.

**node2vec here runs the real biased 2nd-order random walks (`returnFactor` =
p, `inOutFactor` = q) but embeds them with a random-projection of the walks'
co-occurrence counts, not skip-gram/word2vec training** -- see
`samyama_graph_algorithms::embeddings` for what that means. It is not a
drop-in replacement for a from-scratch SGNS-trained node2vec.

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| FastRP | fastRP | Both, symmetrised: successors and predecessors are merged into one neighbour list before propagating | Ignored | Counted twice: a self-loop's target appears in both the out- and in-neighbour list that gets merged | An isolated node's row is the zero vector: nothing propagates into it at any depth | Not applicable: one row per node, no ranking | L2-normalised per row by default (`normalize: true`); deterministic given `seed`, no RNG |
| node2vec | node2vec | Both, symmetrised, the same way as FastRP, for both the walk and the co-occurrence projection | Ignored | A self-loop is a neighbour like any other and can be walked to and from | An isolated node's walk never leaves it, so it records no co-occurrence pair and its row is the zero vector | Not applicable: one row per node, no ranking | L2-normalised per row; deterministic given `seed`, no RNG. **Not skip-gram-trained (differs)** -- see above |

## Not a graph algorithm

| Algorithm | Cypher name | Directedness | Weights | Self-loops | Disconnected | Tie-breaking | Normalisation |
|---|---|---|---|---|---|---|---|
| PCA | pca | No edges: it reads numeric node properties | Node properties, not edge weights; a non-numeric or missing value is the caller's to handle | Not applicable | Not applicable: there is no graph | Components in eigenvalue-descending order; equal eigenvalues give an arbitrary but reproducible basis | Centred by default, scaled on request. **Component sign is not fixed** **(differs)**. Deterministic: a fixed start vector and a seeded RNG |
| Optimisation solvers | or.solve | Not applicable | Not applicable | Not applicable | Not applicable | Solver-specific; every solver is seed-reproducible (OPT-08) | Solver-specific. Every row carries `objective`, `feasible`, `gap` and `total_time_ms` (OPT-09); `fitness` includes the constraint penalty, `objective` does not |

## Where a convention differs from the obvious one

The rows marked **(differs)** are the ones a user coming from NetworkX, LDBC or
Neo4j would get wrong by assuming. They are not all defects — counting a
reciprocal edge twice in label propagation is a choice — but each is a place
where our number and another engine's number disagree for a reason that is not
the graph. `CH-ALGO-PARITY` checks the ones that have a reference
implementation; this document is where the rest are at least visible.

The strongest of them, in the order that would surprise a user most:

1. **MST returns a forest, not a tree.** A disconnected graph has no spanning
   tree, so `algo.mst` spans every component and yields `components` beside
   `total_weight`. The edge rows look the same either way; that number is what
   tells them apart.
2. **Weighted path, A\* and Yen's refuse negative edges.** The algorithms skip
   them, which answers over a different graph, so the Cypher calls refuse and
   name `bellmanFord`. A library caller reaching past Cypher still gets the
   skip (#1303).
3. **Degree centrality ignores its own `bidirectional` argument.** It is always
   out-degree plus in-degree.
4. **ArticleRank drops dangling mass**, so its scores do not sum to 1 and are
   not comparable with PageRank's.
5. **Max flow's augmenting path is hash-ordered.** The flow value is unique and
   stable; the path chosen to carry it is not.
