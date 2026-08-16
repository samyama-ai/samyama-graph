# Samyama Graph — Benchmarks

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

> **These numbers are unverified and should not be quoted.** They were produced by a harness that
> counted a read returning **zero rows as a pass** (#449), so it could not distinguish a fast query
> from one that measured nothing. #450 fixed that, adding an `EMPTY` status and making the run exit
> non-zero on any empty read. Which cells below are affected cannot be determined retroactively, so
> the table needs a re-run on the documented hardware under the fixed harness before it is used.

| Scale | Nodes | Edges | Load |
|---|---|---|---|
| **SF1** | 3,181,724 | 17,256,038 | 78.5 s |
| **SF10** | 29,987,835 | 176,623,433 | 575 s |

## Short reads (IS1–IS7)

| Query | Name | SF1 | SF10 |
|---|---|---|---|
| IS1 | Person Profile | 0.04 ms | 0.02 ms |
| IS2 | Recent Posts by Person | 1.00 ms | 1.10 ms |
| IS3 | Friends of Person | 0.24 ms | 1.80 ms |
| IS4 | Post Content | 0.03 ms | 0.01 ms |
| IS5 | Post Creator | 0.03 ms | 0.02 ms |
| IS6 | Forum of Post | 0.06 ms | 0.06 ms |
| IS7 | Replies to Post | 0.54 ms | 11.50 ms |

## Complex reads (IC1–IC14)

| Query | Name | SF1 | SF10 |
|---|---|---|---|
| IC1 | Transitive Friends by Name | 319 ms | 14.0 s |
| IC2 | Recent Friend Posts | 18.20 ms | 306 ms |
| IC3 | Friends in Countries | 1.3 s | 15.7 s |
| IC4 | Popular Tags in Period | 37.50 ms | 527 ms |
| IC5 | New Forum Members | 1.4 s | 31.1 s |
| IC6 | Tag Co-occurrence | 1.5 s | 31.5 s |
| IC7 | Recent Likers | 0.52 ms | 1.70 ms |
| IC8 | Recent Replies | 0.63 ms | 4.00 ms |
| IC9 | Recent FoF Posts | 2.5 s | 26.3 s |
| IC10 | Friend Recommendation | 199 ms | 2.3 s |
| IC11 | Job Referral | 133 ms | 4.5 s |
| IC12 | Expert Reply | 224 ms | 3.2 s |
| IC13 | Single Shortest Path | 7.10 ms | 37.00 ms |
| IC14 | Trusted Connection Paths | 34.70 ms | 696 ms |

## Notes

- **Samyama is extremely fast on point and short reads** — IS1/IS4/IS5 are sub-0.1 ms at both scales (in-process index-free adjacency).
- **Complex multi-hop reads at scale are a known optimization area.** Several deep-traversal queries (IC1/IC3/IC5/IC6/IC9) grow super-linearly from SF1 to SF10 and are the focus of active planner/executor work — tracked in [issue #296](https://github.com/samyama-ai/samyama-graph/issues/296).
- Queries are LDBC-SNB-inspired Cypher adaptations; the runnable benchmark is `benches/ldbc_benchmark.rs` (`cargo bench --bench ldbc_benchmark -- --params-file <params.json> --data-dir <dataset>`).
- SF1 measured on a macOS i9-9980HK (32 GB); SF10 on a single 192 GB cloud VM.

_These are Samyama's own numbers, published for transparency. We're actively improving the complex-read path._
