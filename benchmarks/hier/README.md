# HIER — hierarchy-heavy complex queries

A benchmark category for **subsumption** (is `x` under `y`?) and **hierarchical roll-up**
(aggregate a measure over everything under `y`), the workload unified by
[arXiv:2606.24677](https://arxiv.org/abs/2606.24677) and implemented in Samyama as the OEH
index ([ADR-035](../../docs/ADR/ADR-035-oeh-hierarchy-index.md)).

## Why a new category

The suites Samyama already runs — LDBC SNB Interactive and BI, FinBench, Graphalytics —
contain essentially no subsumption or roll-up. They are social-network, financial and
graph-algorithm workloads; what hierarchies they have are shallow and incidental. A
hierarchy index is therefore *invisible* in those numbers no matter how good it is, which
is a property of the benchmarks, not of the index. HIER exists to make that axis
measurable.

## Running it

```bash
cargo run --release --example hier_benchmark              # full corpus
cargo run --release --example hier_benchmark -- --reps 20 --class H2
cargo bench --bench hierarchy_benchmark                   # index micro-benchmarks
python3 benchmarks/hier/generate_corpus.py                # regenerate queries.json
```

The dataset is **generated, deterministic and self-contained** — no download, no license
gate, identical on every machine. `benches/hier_common/mod.rs` builds it: 18,975 nodes and
33,974 edges across four hierarchy axes plus a 5,000-row fact table.

| Axis | Chain | Covering edge | Shape | Encoding | Subtree sizes |
|---|---|---|---|---|---|
| Time | Day ⊑ Month ⊑ Quarter ⊑ Year | `IN_PERIOD` | tree | nested-set | 1 … 353 |
| Geography | Zip ⊑ City ⊑ State ⊑ Country | `LOCATED_IN` | tree | nested-set | 1 … 446 |
| Ontology | Term ⊑ Term, 5 levels × fanout 6 | `IS_A` | tree | nested-set | 1, 7, 43, 259, 1555, 9331 |
| Threat | Technique ⊑ Technique, 2 parents each | `MAPS_TO` | multi-parent DAG | chain | 1 … 36 |

A fourth encoding, **near-tree** (#371), covers posets that are trees apart from a small
fraction of extra parent edges — real administrative geography, which the all-or-nothing
`is_tree()` test previously threw away. It is not exercised by this generated dataset; see
[`results/real-ontologies.md`](results/real-ontologies.md) for GeoNames.

## Correctness is the gate

Every query runs twice: once against a store with the four hierarchies declared, once
against an identical store with none. **The unindexed run is the ground truth**, and a
disagreement fails the run with a non-zero exit. A speedup number from a benchmark that
does not check the answer is a measurement of how fast it can be wrong.

Where the baseline is not simply the same query with the index off, it is one of:

1. **Prefix** — this dataset encodes each ontology and geography code as its own path from
   the root, so "under `T05`" is exactly "code starts with `T05`". That is ground truth
   computed with no hierarchy machinery at all — neither index nor traversal.
2. **Traversal** — a variable-length expansion, used where the code is not a prefix
   (calendar quarters) and on the DAG axis.

**Latest run: 108 / 108 agree.** 4 further queries are specified but blocked (below).

## Results

18,975 nodes, 5 reps, median per query. `speedup = baseline / indexed`.

| Class | n | Agree | Indexed (ms) | Baseline (ms) | Median speedup | Total-time |
|---|---:|---:|---:|---:|---:|---:|
| H1 order test | 15 | 15/15 | 0.239 | 0.471 | 2.0× | 1.44× |
| H2 single roll-up | 24 | 24/24 | 0.005 | 0.029 | 5.3× | **36.30×** |
| H3 level roll-up | 9 | 9/9 | 0.092 | 0.617 | 6.7× | 2.00× |
| H4 cross-hierarchy conjunction | 12 | 12/12 | 9.368 | 1.079 | 0.1× | **0.21×** |
| H5 hierarchy × traversal | 10 | 10/10 | 0.079 | 0.171 | 2.2× | 2.23× |
| H6 anti-subsumption | 10 | 10/10 | 4.234 | 1.930 | 0.5× | **0.49×** |
| H7 lowest common ancestor | 10 | 10/10 | 4.154 | 0.390 | 0.1× | **0.09×** |
| H8 top-k over roll-up | 8 | 8/8 | 0.114 | 0.608 | 5.3× | 2.80× |
| H10 temporal roll-up windows | 10 | 10/10 | 0.014 | 0.093 | 6.5× | 5.56× |
| **All** | **108** | **108/108** | 0.151 | 0.386 | 2.6× | **0.45×** |

**Two speedup columns, and a third thing the columns do not say.** The median
column is the per-class ratio of medians; the total-time column is the sum of
the right column over the sum of the left, which is what the corpus costs.

**The two columns are not always index-on against index-off.** 85 of the 112
queries carry a separate hand-written `baseline`; only 27 run the same text
twice with the index toggled. Those are different questions and the table mixes
them:

| Comparison | n | Total time | Medians | Slower |
|---|---:|---:|---:|---:|
| **same text, index on vs off** | 27 | **35.26×** | 11.83× | 2 |
| index-written vs hand-written | 81 | 0.43× | 0.52× | 32 |
| all rows (this table) | 108 | 0.45× | 2.56× | 34 |

Read the first row for what the index does: **35× on total time**, with 2 of 27
slower. The second row says how the two spellings compare, which is a fact about
the corpus's queries rather than about the index — and it is where H4, H6 and H7
lose:

- **H7** writes `hierarchy_lca(a, b)` inside a `WHERE` evaluated once per
  `:Term`, against a hand-written single path join. The call does not depend on
  the row.
- **H6**'s alternative is `NOT (d.code STARTS WITH "T")` — a string-prefix test
  that works only because this synthetic corpus encodes ancestry in the code. It
  is not a traversal and would not exist on a real ontology.
- **H4** conjoins two hierarchies against a var-length path join.

An earlier version of this page reported the mixed 0.45× as the index's own
result. It is not; `CH-BENCH-HIER` now publishes the two separately as
`index_on_same_query_total_time` and `index_written_vs_hand_written_total_time`.

The class table above was re-measured on documented hardware (2026-09-21). The
version it replaced was from a 2026-08-14 host that no longer exists and
reported H4 1.1×, H6 0.3×, H7 1.7×, All 3.0×. **The host changed as well as the
code**, so the difference is not attributable to either alone.

**Against Neo4j** on an identical graph (`samyama-graph-competitor-benchmarks/benchmarks/hier/`):
H2 **1124×**, H10 **144×**, H3 **88×**, H1 **9.1×**, H5 **8.2×** — 94× across the 58 queries
expressible on both engines. No class in *that comparison* loses — and those five classes
are exactly the ones the index wins. H4, H6 and H7 are not expressible on Neo4j, so the
three classes where the index is a net cost are absent from it by construction.

Index sizes and build cost:

| Index | Encoding | Nodes | Structural B/node | Roll-up bytes | Build |
|---|---|---:|---:|---:|---:|
| `cal` | nested-set | 2,824 | 12.0 | 1.95 MB | 1.6 ms |
| `geo` | nested-set | 1,784 | 12.0 | 1.15 MB | 1.0 ms |
| `onto` | nested-set | 9,331 | 12.0 | 7.46 MB | 5.9 ms |
| `threat` | chain | 36 | 28.9 | 4.8 KB | 0.05 ms |

### The headline: roll-up is flat in subtree size

`cargo bench --bench hierarchy_benchmark`, balanced tree, one root per distinct subtree
size:

| Subtree | 1 | 8 | 57 | 400 | 2,801 | 19,608 | 137,257 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sum` | 16.0 ns | 16.1 ns | 16.8 ns | 17.8 ns | 19.5 ns | 18.6 ns | 16.6 ns |
| `max` | 14.9 ns | 14.9 ns | 14.9 ns | 14.9 ns | 15.3 ns | 15.0 ns | 14.9 ns |

Five orders of magnitude of subtree, flat. `sum` shows the slight O(log n) rise a Fenwick
tree should; `max` is flat because a sparse table answers a range fold in O(1). This is
what "index-resident" means, and it is why H2 and H10 win by two to four orders of
magnitude while the engine aggregation pays O(subtree) every time.

## What did not win, and why

Reporting only H2 would be dishonest. Two classes are **slower** with the index:

**H6 (anti-subsumption), 0.3×.** Two causes, both real:

1. Half of H6 is a set difference — `subsumes(d, r) AND NOT subsumes(d, s)` — which needs
   two predicates and so takes the standard plan. The single-predicate half was fixed by
   #375 and H6 improved from 15.4 ms to 6.7 ms, but the class average is still dragged by
   the difference queries. Only the
   roll-up and descendant-scan rewrites ship. So these queries run the `subsumes()`
   function form, which binds the root with a cartesian product and then pays a
   per-row index lookup: ~500 ns/row against ~120 ns/row for a native predicate.
2. The prefix baseline is unusually strong *because of how this dataset is built*. Real
   ontologies do not encode ancestry in the identifier, so "code starts with X" is not
   available on Gene Ontology or GeoNames — it is available here only because the generator
   makes codes paths. Against Neo4j, which has no such shortcut, H1 is **9.1×**.

**H4 (cross-hierarchy conjunction), 1.1×** — the remaining half of #350. At this scale the 5,000-row fact scan dominates
both plans, so a three-axis conjunction costs about the same either way. The result worth
reporting for H4 is not speed but *expressibility*: one query ranges over ontology, time
and geography with three O(1) predicates, which no per-silo hierarchy index answers at all.
A time-series engine's continuous aggregate cannot express it; a 2-hop reachability index
has no roll-up to compose with. Making it *fast* needs a plan that starts from the
hierarchy and drives into the fact table, which is not a plan the current detector emits.

**H7 (LCA), 1.6×.** The index answers LCA in O(depth) from the intervals, but the query
shape again pays a cartesian product to bind both endpoints.

## The DAG double-count trap

On the `MAPS_TO` axis a node is reachable along many paths. The traversal baseline must say
`count(DISTINCT e)` or `WITH DISTINCT d` — written the obvious way it over-counts, and the
corpus contains a pair demonstrating exactly that. The index cannot make this mistake:
chain decomposition partitions the node set, so per-chain suffix folds visit every
descendant exactly once by construction. This is the correctness argument for the chain
encoding, not a performance one.

## Known engine gaps this benchmark surfaced

Found while building the corpus; all pre-date ADR-035 and none are caused by it.

1. **[#345] `ORDER BY sum(...) DESC` does not sort.** Repeating a `sum()` aggregate in
   `ORDER BY` leaves the rows in natural order, so `LIMIT k` silently returns the wrong
   top-k. Ordering by the *alias* is correct, and `count()` happens to work. The H8
   queries therefore order without a `LIMIT`, since the class exists to measure roll-up
   called in a loop rather than to test truncation.
2. **[#346] Inline property maps inside `EXISTS { }` never match.**
   `EXISTS { MATCH (d)-[:IS_A]->(r:T {code: "AB"}) }` returns nothing, so `NOT EXISTS`
   returns every row. Not specific to variable-length patterns — a single hop reproduces
   it — and the same constraint written as a `WHERE` inside the subquery works.
3. **[#347] `NOT x STARTS WITH "y"`** parses as though `NOT` binds the operand.
   `NOT (x STARTS WITH "y")` is correct. A precedence bug.
4. **[#348] `CALL … YIELD` variables are not in scope for a following `WHERE`**
   (`Variable not found: node`); a `WHERE` directly after `YIELD` is a parse error. This
   blocks class **H9**, hierarchy-filtered vector search. The four H9 queries are kept in
   `queries.json` with a `skip` reason so the class stays visible rather than quietly
   vanishing from the table.

## Corpus

`queries.json` — 112 queries, generated by `generate_corpus.py`. Regenerate after editing
the generator; the JSON is committed so a run needs no Python.

| Class | Queries | What it asks |
|---|---:|---|
| H1 | 15 | order test, four axes, every depth |
| H2 | 24 | single roll-up, subtree sizes 1 → 9,331 |
| H3 | 9 | level roll-up (group by hierarchy level) |
| H4 | 12 | ontology × time × geography in one query |
| H5 | 10 | subsumption predicate composed with traversal |
| H6 | 10 | anti-subsumption and subtree set difference |
| H7 | 10 | lowest common ancestor |
| H8 | 8 | top-k over roll-up (roll-up in a loop) |
| H9 | 4 | hierarchy-filtered vector search — **blocked**, see above |
| H10 | 10 | temporal roll-up windows |

## Not yet covered

Stated rather than skipped, and tracked in #353:

- **Neo4j / TigerGraph cross-engine baselines.** The corpus is Cypher and the dataset is
  generated, so porting it is mechanical, but it has not been run. `samyama-graph-competitor-benchmarks`
  is the place for it.
- **TimescaleDB continuous aggregates** for H10, the paper's head-to-head. The reference
  implementation matches TimescaleDB to the unit (day 704,800; month 21,168,000); that
  comparison has not been reproduced in-engine.
- ~~**Real ontologies at scale**~~ — **done, 2026-08-14**: see
  [`results/real-ontologies.md`](results/real-ontologies.md). NCBI Taxonomy builds at 2.9M
  nodes in 6.8 s at exactly 12 B/node; Gene Ontology declines as predicted. Two findings
  worth reading: **GeoNames declines despite being 98.7% a tree** (#371 — there is no
  encoding between "perfect tree" and "give up"), and **MONDO contains an `is_a` cycle**
  (#372). Of five real ontologies, one builds, three decline, one is cyclic — the paper's
  "low-width multi-parent DAGs are rare" is understated; in this sample there were none.
- **2-hop / PLL space-and-build comparison.** The paper's "half the space, 6–7× faster
  build at query parity" claim is not reproduced here because Samyama has no PLL
  implementation to compare against.
