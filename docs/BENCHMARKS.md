# Samyama Graph — Benchmarks

## What is on this page, and what is not

**On this page: Samyama's own numbers**, each with the commit and host it was
measured on. Every figure here has a reproducer in this repository that you can
run yourself:

| Suite | Run it with |
|---|---|
| LDBC SNB Interactive | `cargo bench --bench ldbc_benchmark` (data: `scripts/download_ldbc_snb.sh`) |
| LDBC SNB BI | `cargo bench --bench ldbc_bi_benchmark` |
| FinBench | `cargo bench --bench finbench_benchmark` |
| Graphalytics | `cargo bench --bench graphalytics_benchmark` |
| HIER (hierarchy) | `cargo run --release --example hier_benchmark` (corpus: `benchmarks/hier/`) |
| Memory footprint | `cargo bench --bench memory_footprint` |
| Cardinality accuracy | `cargo bench --bench cardinality_accuracy` |
| Ingestion profile | `cargo bench --bench ingest_profile` |
| Operator micro-benchmarks | `cargo bench --bench aggregate_grouping`, `--bench varlength_expand`, `--bench property_access`, `--bench aggregate_throughput` |

**Reading the operator micro-benchmarks.** These report ns per row for a single
operator, and each prints the operator the planner actually chose. Check that
column before drawing a conclusion: the planner rewrites several shapes, and a
case that was rewritten measures the rewrite rather than the operator named in
the row. `aggregate_grouping` reported 88 ns/row for LDBC IC5's shape until that
column was added — `RETURN f.x, count(i)` over an expand becomes
`AdjacencyCountAggregate`, which reads degrees off the adjacency index and never
groups anything. The honest figure for that shape was 12× higher.

**Not on this page: cross-engine comparisons.** Results against Neo4j,
FalkorDB and TigerGraph are maintained internally alongside the competitor
configurations and licence terms those runs depend on, and are published
selectively rather than continuously. The Neo4j figures quoted in the HIER
section below come from that work.

The split follows from where a reader can act: a number you can reproduce
belongs next to the code that produces it; a number that required another
vendor's licensed software to obtain does not.

## Comparing two numbers

**A before/after claim requires both figures from one back-to-back session on
one machine.** Not "the same machine" — the same *sitting*.

This is not caution for its own sake. On a 16-core workstation, the same
binary at the same commit ran LDBC IC9 in **2,822 ms in the morning and
4,912 ms the same evening**, with nothing else on the CPU; two consecutive
runs of one binary differed by **24%** (#529). Comparing an evening run
against a morning baseline produced apparent 2–4x regressions in three
queries, none of which existed.

So:

- **Check the calibration line first.** Every run prints a fixed CPU-bound
  loop's duration at the start and again at the end, with the load average
  and mean core frequency. Two runs whose calibration differs were taken on
  hosts of different speed, whatever their milliseconds say. If the closing
  figure differs from the opening one by more than 10%, the run says so and
  its own numbers are not internally comparable either.
- **Quote the ratio, not the milliseconds**, when the point is an
  improvement. A ratio measured back to back survives a slow host; an
  absolute figure does not survive being read next to one taken elsewhere.
- **`SLT-2` is a ratio against a competitor.** A competitor figure measured
  in a different session is not a baseline for one of ours.


Two suites are documented here: the LDBC SNB Interactive results below, and **HIER**, the
hierarchy category introduced with ADR-035.

## HIER — hierarchy-heavy complex queries

Subsumption (`is x under y?`) and hierarchical roll-up over time, geography and ontology —
the workload of [arXiv:2606.24677](https://arxiv.org/abs/2606.24677), which the LDBC and
FinBench suites do not exercise. 112 queries across ten classes over a generated,
self-contained 18,975-node / 33,974-edge dataset with four hierarchy axes.

```bash
cargo run --release --example hier_benchmark   # corpus, index on vs off
cargo bench --bench hierarchy_benchmark        # index micro-benchmarks
```

Every query is answered twice — once with the hierarchy indexes declared, once without —
and the unindexed run is the ground truth. **112/112 agree.** Class H9
(hierarchy-filtered vector search) was specified but skipped behind a `CALL … YIELD`
composition gap; #443 removed that skip and its four queries now run and agree.

| Class | n | Speedup | Class | n | Speedup |
|---|---:|---:|---|---:|---:|
| H1 order test | 15 | 1.1× | H6 anti-subsumption | 10 | 0.3× |
| H2 single roll-up | 24 | **8596×** | H7 lowest common ancestor | 10 | 1.7× |
| H3 level roll-up | 9 | 5.9× | H8 top-k over roll-up | 8 | 5.3× |
| H4 cross-hierarchy | 12 | 1.1× | H10 temporal windows | 10 | 108.5× |
| H5 hierarchy × traversal | 10 | **27.4×** | H9 hierarchy × vector | 4 | 0.9× |
| | | | **All** | **112** | **see note** |

> **Provenance.** The per-class speedups above were measured before #443 unblocked H9, so they
> cover 108 of the 112 queries. H9 itself measures ~0.9× — vector × hierarchy composes but is not
> ordered by selectivity (#445). The class figures need a re-run on the documented hardware; the
> 112/112 agreement above is from the current corpus and is not hardware-dependent.

Against **Neo4j** on an identical graph: H2 **1124×**, H10 144×, H3 88×, H1 9.1×, H5 8.2× —
**94× across the 58 queries expressible on both engines**, with no class losing. Without the
index Samyama is 1.6× *slower* than Neo4j on the same set, so the index is the
differentiator rather than the engine.

Roll-up latency is flat in subtree size — 16.0 ns at 1 node, 16.6 ns at 137,257 — which is
what makes H2 and H10 win by orders of magnitude. See
[`benchmarks/hier/README.md`](../benchmarks/hier/README.md) for the full accounting,
including the engine gaps the corpus surfaced.

## LDBC SNB Interactive

Samyama Graph's own results on the [LDBC Social Network Benchmark (SNB) Interactive](https://ldbcouncil.org/benchmarks/snb/) read workload (IS1–IS7 short reads, IC1–IC14 complex reads), at two scale factors. In-process (embedded) timing, 1 warm-up + 3 timed runs, median latency. Provenance: commit `31a7e77`, id-indexes built on all anchor labels.

> **SF1 is verified. SF10 is not.**
>
> The SF1 column below was re-run under the fixed harness (#450), which reports an `EMPTY` status and
> exits non-zero if any read returns zero rows. Result: **21/21 passed, 0 empty, 0 errors** — so every
> figure is a measurement of an actual traversal rather than of an empty result set. This is the first
> clean 21/21: IC14 timed out at 120 s until #539.
>
> Provenance: commit `4f0253e`, **Vultr voc-c-16c-32gb dedicated CPU** (16 vCPU / 31 GB, AMD EPYC-Rome,
> fixed 1996 MHz), 1 warm-up + 3 timed runs, median reported, dataset from
> `scripts/download_ldbc_snb.sh`. Host calibration was flat across the run — 43 ms opening and closing,
> 1.00x (#529).
>
> Substitution parameters were **derived from the dataset** at the median of the KNOWS-degree
> distribution rather than taken from the benchmark's built-in defaults (#505): anchor degree 23
> against a median of 23 and a maximum of 977. Reproduce with
> `cargo bench --bench ldbc_benchmark -- --runs 3 --derive-params 50`, which prints the provenance of
> every parameter above the table.
>
> **The SF10 column is still unverified** and should not be quoted. It predates #450, when the harness
> counted a zero-row read as a pass, so it cannot be told apart from an empty result. It needs the same
> treatment.
>
> The SF1 figures moved from the previous table but stayed the same order of magnitude, which is worth
> saying plainly: the old numbers were not fabricated, they were merely unprovable. Differences are
> explained by hardware — the earlier run was a macOS i9-9980HK.

| Scale | Nodes | Edges | Load |
|---|---|---|---|
| **SF1** | 3,727,429 | 21,140,212 | 78.5 s |
| **SF10** | 29,987,835 | 176,623,433 | 575 s |

> **SF1 counts, and how they were obtained.** Counted from the source CSVs in
> `social_network-sf1-CsvBasic-LongDateFormatter/`, independent of the loader: the 8 entity
> files sum to **3,727,429** rows, and the 25 relationship files sum to 21,161,452. The
> loader reports **21,140,212** edges; the 21,240 difference is exactly
> `person_email_emailaddress` (10,620) plus `person_speaks_language` (10,620), which are
> attributes stored as node properties rather than as edges. Both figures account to the row.
>
> The previously documented 3,181,724 / 17,256,038 matched neither, and was 17% and 22% low
> (#500). Anything divided by those counts was wrong by the same margin.
>
> The load time above is unchanged: it was measured on the host recorded at the foot of this
> section, and substituting a figure from a different machine would trade a stale number for
> an unprovenanced one.

## Short reads (IS1–IS7)

| Query | Name | SF1 | SF10 |
|---|---|---|---|
| IS1 | Person Profile | 0.03 ms | 0.02 ms |
| IS2 | Recent Posts by Person | 0.08 ms | 1.10 ms |
| IS3 | Friends of Person | 0.08 ms | 1.80 ms |
| IS4 | Post Content | 0.01 ms | 0.01 ms |
| IS5 | Post Creator | 0.02 ms | 0.02 ms |
| IS6 | Forum of Post | 0.03 ms | 0.06 ms |
| IS7 | Replies to Post | 1.2 ms | 11.50 ms |

## Complex reads (IC1–IC14)

| Query | Name | SF1 | previous SF1 | SF10 |
|---|---|---|---|---|
| IC1 | Transitive Friends by Name | **56.0 ms** | 58.9 ms | 14.0 s |
| IC2 | Recent Friend Posts | **5.6 ms** | 8.1 ms | 306 ms |
| IC3 | Friends in Countries | **443 ms** | 862 ms | 15.7 s |
| IC4 | Popular Tags in Period | **8.8 ms** | 13.9 ms | 527 ms |
| IC5 | New Forum Members | **713 ms** | 1203 ms | 31.1 s |
| IC6 | Tag Co-occurrence | **161 ms** | 185 ms | 31.5 s |
| IC7 | Recent Likers | **0.04 ms** | 0.09 ms | 1.70 ms |
| IC8 | Recent Replies | **0.12 ms** | 0.16 ms | 4.00 ms |
| IC9 | Recent FoF Posts | **673 ms** | 1131 ms | 26.3 s |
| IC10 | Friend Recommendation | **85.9 ms** | 104 ms | 2.3 s |
| IC11 | Job Referral | **30.9 ms** | 32.2 ms | 4.5 s |
| IC12 | Expert Reply | **56.0 ms** | 84.7 ms | 3.2 s |
| IC13 | Single Shortest Path | **42.6 ms** | 43.4 ms | 37.00 ms |
| IC14 | Trusted Connection Paths | **49.4 ms** | 49.8 ms | 696 ms |

**Whole suite: 9.7 s, 21/21 passed, 0 empty, 0 errors.**

### What the "previous SF1" column is

The measurement immediately before this one, on the same host with the same
derived parameters (#505), so the two columns differ only by engine changes.
Both were taken with the host calibration reported and matching, which is what
makes them comparable at all (#529).

This round: `Expand` applies a pattern's target labels **during** the adjacency
walk, by probing the label index with the target's id, instead of collecting
every incident edge and then `retain`-ing the ones that match. The old test was
`get_node(id).has_label(label)` per edge — a `Vec` index, a version-chain walk,
a 128-byte `Node`, and a `HashSet<Label>` probe that hashes a *string* — and at
2.22M edges visited per IC9 run it was **26.7% of a CPU profile**, the largest
single symbol, ahead of every property read (#592). IC3, IC5 and IC9 each drop
about 40%.

Before that: `Value` became **56 bytes instead of 144** by boxing the
`Node`/`Edge` payloads, shrinking every binding in every record (#570).

Before that: the aggregate's group table stopped storing a `Value` per group,
taking an entry from ~320 bytes to ~40 and IC5's `Aggregate` down 23%.

Before that: `Sort` locates its key columns once instead of once per input row
(#568), worth 5% on IC9, which spends a fifth of itself sorting. And before
that: `Expand` holds its variable names as `Arc<str>`, so binding one on
an output row is a refcount bump rather than two heap allocations, and it
refills its edge buffer instead of allocating a new one per source record
(#564). IC5 was a third faster on that alone.

The rounds before, which is what the previous column and the ones before it
reflect: records cloned with room for the bindings about to be added (#562);
`id()` predicates anchoring a scan instead of filtering one (#538); aggregates
grouping on identity and resolving their keys once per group (#521); property
columns located once per query rather than once per row (#557); and `Filter`
deciding whether to go parallel from the predicate's cost rather than the batch
size (#559).

Together, over this sequence, the suite went from **24.2 s to 9.7 s** on the
same host at the same derived parameters — a **60% reduction**, almost all of it
in per-row constants rather than in algorithms. The one exception is the last:
moving the label test into the walk removes work rather than making it cheaper.

Nothing here is a parameter change. The parameter shift that made several
queries look slower — deriving substitution parameters from the dataset at the
median of the KNOWS-degree distribution, rather than using the benchmark's
built-in anchors — happened in the round before, and those harder parameters are
still in force.


## Notes

- **Samyama is extremely fast on point and short reads** — IS1/IS4/IS5 are sub-0.1 ms at both scales (in-process index-free adjacency).
- **Complex multi-hop reads at scale are a known optimization area.** Several deep-traversal queries (IC1/IC3/IC5/IC6/IC9) grow super-linearly from SF1 to SF10 and are the focus of active planner/executor work — tracked in [issue #296](https://github.com/samyama-ai/samyama-graph/issues/296).
- Queries are LDBC-SNB-inspired Cypher adaptations; the runnable benchmark is `benches/ldbc_benchmark.rs` (`cargo bench --bench ldbc_benchmark -- --params-file <params.json> --data-dir <dataset>`).
- SF1 measured on a Vultr 12 vCPU / 23 GB AMD EPYC-Rome instance at commit `b20ab99`, under the fixed harness (21/21 passed, 0 empty). SF10 on a single 192 GB cloud VM, pre-#450 and unverified.

_These are Samyama's own numbers, published for transparency. We're actively improving the complex-read path._
