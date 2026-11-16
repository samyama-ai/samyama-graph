# Filtered ANN correlation sweep (#1985)

```sh
cargo bench --bench filtered_ann_benchmark -- --n 10000 --dim 32 --queries 100 --ef 32,128,512 --csv filtered.csv
```

Run on a quiet benchmark host. CI compiles the target and checks predicate
construction and the independent oracle; CI timings are not published results.
No benchmark numbers have been collected in this PR.

The twelve cells cross four exact cardinalities (0.1%, 1%, 10%, 50%) with three
query-dependent predicates: nearest-ranked (positive correlation), seeded
uniform sample (none), and farthest-ranked (negative correlation). Cardinality
is fixed independently of correlation. These are deliberately extreme synthetic
predicates, not a model of real tag distributions. The same corpus and query
set are used throughout. Seeded corpus generation is reproducible; HNSW topology
is randomized by the underlying library.

Each cell measures automatic filtered search, exact prefilter search, and
bounded one-shot postfilter search across the same ef sweep. Postfilter fetches
max(k,ef) candidates and may return short results; it does not pretend to be an
in-filter HNSW implementation. Independent f64 L2 truth is computed over passing
nodes, outside the timed region. Predicate construction is also excluded.
CSV includes recall@10, throughput, p50/p99, short results and filter violations.
Search errors abort the run instead of producing a successful-looking row.

These are single-client in-process measurements, not network DB comparisons.
For plan regret, compare only points in the same cell with zero filter violations
and recall >= the chosen floor (e.g. 0.9). Compare latency to the fastest eligible
point; a cell with no eligible point is unmeasured, not a zero regret.

## Graph-reachability predicates

```sh
cargo bench --bench filtered_ann_benchmark -- --n 10000 --dim 32 --queries 100 --ef 32,128,512 --graph-csv graph.csv
```

`--graph-csv` runs the second workload, "the k nearest among the nodes within
two hops of X", which only a graph engine can answer in one statement. It can
be passed with or without `--csv`.

- **Graph.** A directed k-nearest-neighbour graph over the same corpus: each
  node links to its `degree` nearest others (edge type `N<degree>`), so linked
  nodes are similar. Degrees 3, 10 and 31 give two-hop reaches of roughly
  0.1%, 1% and 5-10% at n = 10,000. Reach sizes vary by anchor, so the CSV
  records the mean measured (`mean_reach`), not a target.
- **Correlation.** Set by the anchor: the corpus node nearest the query
  (positive), a seeded random node (none), or the farthest (negative).
- **Truth.** The same f64 oracle, over `out(x) ∪ out(out(x))`. That is what
  `(x)-[:T*1..2]->(n)` matches on a graph with no self-loops or parallel edges,
  so it can include `x`.

The four strategies are timed with the traversal included in every one:

| strategy | what runs |
|---|---|
| `traverse+auto` | two hops through the store's adjacency, then the reach as the vector search's filter (`search_filtered_ef`), across the ef sweep. This is the in-process form of a two-system pipeline: the graph DB returns ids and the vector DB filters on them, minus both network hops. |
| `traverse+exact` | the same walk, then an exact scan of the reach |
| `cypher-post` | `CALL db.index.vector.queryNodes(...) YIELD node MATCH (node)<-[:T*1..2]-(x {id: X})`. This is what a user writes today; the traversal filters the top k after the search, so expect short results |
| `cypher-exact` | `MATCH (x {id: X})-[:T*1..2]->(n) ... ORDER BY vector.distance(...) LIMIT k` |

Statement text is parsed outside the timed region, while planning and execution are timed.
`tests/filtered_ann_graph_predicates.rs` checks that `cypher-exact` matches the
oracle and that `cypher-post` never returns a node outside the reach. No
numbers have been collected; the sweep needs a quiet host.

The gap between `cypher-post` and `traverse+auto` is the gap the planner
leaves today. The planner does not hand a traversal's reach to the vector
search as a filter, the way it does a property predicate (#1969) or a
hierarchy subtree (#445). That is planner work for #1970, not something this
benchmark does.

## Still required

Official YFCC-10M ground truth and tag inputs, competitor adapters on
symmetric network paths (including the Qdrant + Neo4j pipeline the
graph-predicate cell is meant to be compared against), matched-recall regret
reports, and published plots. The synthetic attribute and graph workloads and
their strategy baselines are here; none of those results are claimed.
