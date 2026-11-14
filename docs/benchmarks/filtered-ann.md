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

Still required for the full issue: official YFCC-10M ground truth and tag inputs,
graph-reachability predicates, competitor adapters on symmetric network paths,
matched-recall regret reports and published plots. This PR supplies the synthetic
workload and strategy baseline without claiming those results.
