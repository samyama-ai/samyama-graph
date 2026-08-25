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
| openCypher TCK | `cargo run --release --example tck_runner -- --features <openCypher>/tck/features` |
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

**LDBC SNB Interactive at SF10, against Neo4j on the same host** (2026-08-19,
AWS r6a.8xlarge): 12 of 14 complex reads within 5× of Neo4j and **6 faster**,
with IC6 timing out and IC11 at 11.5×. That is a cross-engine result, so it
lives in the private repo with the competitor configs it depends on —
`benchmarks/ldbc-snb-interactive/SF10-2026-08-19-SAME-HOST.md`. It supersedes
the "85–170× slower on complex reads" figure, which came from a run whose
Samyama and competitor columns were measured on *different machines*.

**Not on this page either: the scorecard.** Every figure here is one suite's
result. The single file that says where the product stands against all 254
requirements — measured, unmeasured, or measured only by proxy — is
`SCORECARD.json`, assembled by the conformance harness from run envelopes. It
lives with the cross-engine results for the same reason they do. This page
stays the reproducible own-engine record; the scorecard is what a claim gets
checked against.

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

## openCypher TCK — Cypher conformance

The measured figure for `LANG-01`. A `~90% OpenCypher coverage` claim was
withdrawn in #437 as unmeasured; this replaces it with a number that has a
reproducer, and that has since been re-measured on a corpus three times larger
(see below).

```
cargo run --release --example tck_runner -- --features <openCypher>/tck/features
```

| | |
|---|---:|
| scenarios in the TCK | 3,897 |
| **evaluated** by the harness | **3,761** (96.5%) |
| pass | 3,384 |
| skipped | 136 |
| **pass rate, of evaluated** | **90.0%** |
| gate `CH-TCK ≥ 85%` | **met** |
| gate *≥ best competitor* | **met** — 10.3 points ahead |

Measured at `4118d37`; the CI floor is a **count**, currently `--min-pass 3384`,
and rises with each merge that earns it.

### The corpus tripled, and the old figure was measured on a third of it

The table above replaces `1,244 evaluated / 81.9%`. Nothing regressed — the
pass *count* went from 1,019 to 3,384. The harness had been skipping every
`Scenario Outline`: 274 of them, expanding to ~2,280 concrete cases, and the
harder ones, because an outline is how the TCK enumerates a feature's edge
cases once its happy path is established (#756).

Any pass rate quoted without its coverage describes an unknown fraction of the
corpus, which is why both numbers appear together here and in the runner's own
output.

### Cross-engine, same corpus and comparator

The same 3,766 scenarios were run against Neo4j, Memgraph and FalkorDB through
this runner — one scenario list, one comparator, a per-engine driver that only
executes and renders:

| Engine | 1,249-scenario set (2026-08-19) | 3,766-scenario set |
|---|---:|---:|
| **Samyama** `4118d37` | 81.9% | **89.7%** |
| Neo4j 5 | 98.9% | 79.4% |
| Memgraph | 89.8% | 65.9% |
| FalkorDB | 89.1% | 65.7% |

**Every engine falls on the wider corpus, and Neo4j falls nearly twenty
points.** That is the evidence the expansion is sound rather than malformed: a
corrupted corpus would have collapsed a mature engine to something implausible,
not to 79.4%.

Scope, because the number is easy to over-quote: this is **conformance**, not
performance or scale. The competitor figures are one run of one container image
each from 2026-08-24 and are a fixed baseline, not a live measurement. Kuzu is
absent — schema-first, so the TCK's schema-free fixtures do not load. Four
engines are not the field.

The second output was a set of bug reports about this harness. A reference
implementation failing a scenario is far more likely to be our defect than
theirs, and three were: the TCK's *control query* was being run as setup, so
27 scenarios were scored against the wrong query for every engine; escape
sequences were un-backslashed rather than interpreted, so `'Foo\nFoo'` became
`FoonFoo` on the expected side; and the value cursor indexed bytes rather than
characters, mangling every UTF-8 literal. Fixing them moved **our** number
932 → 950 with no engine change. Full comparison in
`samyama-graph-competitor-benchmarks/benchmarks/opencypher-tck/`.

**Five tests in this repo asserted rules Cypher does not have.** Four were the
same one: that `WHERE` after an `OPTIONAL MATCH` filters rows, when it scopes
to the optional match and nulls them (#667). Each passed, each was
self-consistent with the engine, and one was the stated justification for the
code producing the wrong answer. None had been checked against anything
outside this repo.

That is what a suite converges on when it only ever checks the engine against
itself, and it is the strongest argument for re-running the cross-engine
comparison on a cadence: it is the only mechanism here that catches the class.
The corrections were made against Neo4j's *actual output*, not against a
reading of the spec.

**A rising pass rate is not evidence of a correct change.** `IN` is
three-valued in Cypher, and the intuitive fix — "if either side contains a
null anywhere, the answer is unknown" — gains 8 scenarios and loses 4. That
nets to **+4 and reads as progress**. The four it breaks are cases where a
length mismatch settles the comparison without ever reaching the null
(`[1] IN [[1, null]]` is `false`). Nothing in the headline shows that; only
diffing the failure manifest does, which is why every step on this page is
checked that way (#647).

**The largest single engine step was a naming rule.** Cypher names an
unaliased result column after the expression as written; the planner
reconstructed it from the AST in **ten separate places that did not agree**.
`RETURN 1 + 1` produced `col_0` — a column no client can select by key — and
`count(*)` produced `count()`, because `*` is not an argument expression and
the text that would say so was discarded at parse time. Recording the source
text and deriving the name once moved 43 scenarios (#635, #636). The size of
that jump measures how ordinary the broken case was, not how clever the fix
is.

**One of these was a reachable crash.** `RETURN 9223372036854775808` did not
return an error — it panicked, from a string any client can send, which on
the HTTP or protocol server stops the process (#633, #634).

**Three more harness points, not engine points.** The
runner discarded every `Background:` block — its parse loop skips lines
before the first `Scenario:` — so all 29 scenarios in Match5 ran against an
empty graph, returned no rows, and were scored as **wrong answers**. The
engine had been charged with 26 defects it did not have, and `wrong_result`
— the class this page calls the most damaging — was 25 too high (#627). Run
by hand against the fixture, those queries return exactly what the TCK
expects. Stated plainly because the direction is flattering: 61.0% → 63.0%
here is the measurement getting more accurate, not the engine getting
better.

**A storage defect was worth 36 scenarios.** `create_node` takes one label
and always inserts it, so a pattern with no label had to invent one: CREATE
passed `""` and MERGE passed the string `"Node"`. Both reached the label
index and the catalog, so an unlabelled node reported a label it did not
have and `MATCH (n:Node)` matched nodes nobody had labelled (#625, #626).
Unlabelled nodes are the default shape across the TCK, which is the whole
reason one storage bug moved the rate 58.1% → 61.0%.

**Coverage moved further than the rate.** 65.0% → 76.7% of scenarios are now
judged rather than skipped, because 197 of them were being skipped for
"setup did not parse" — every TCK fixture is written as a run of `CREATE`
clauses, which did not parse. Making them parse then exposed a worse bug:
`CREATE (a), (b), (a)-[:R]->(b)` created **four** nodes instead of two. The
rate rising from 46.6% to 57.2% *while* 195 harder scenarios joined the
denominator is the more useful way to read this.

**Every step here was checked by diffing the failure manifest, not by
comparing the headline.** Completing the clause pipeline (#624) raised the
rate *and* regressed four TCK negative scenarios: Merge5 [22], [23], [28]
and [29] had been passing because the grammar rejected the clause **order**,
so an error came out for an unrelated reason. Routing MERGE through the new
path made them run, and the Cypher rules they assert — exactly one
relationship type, no variable-length relationship, no new labels on a bound
variable, no null property — turned out not to exist anywhere in the engine.
A rising pass rate hides that; a manifest diff does not.

**The largest single step was not a feature.** Every statement rule in the
grammar encoded one permitted clause order, with writes at the end — so
`MATCH (n) SET n.x = 1 WITH n RETURN n.x` was a syntax error. Underneath that
was the reason: a pass-through operator's default `next_mut` delegates to
`next`, which reads its input read-only, so a materialising operator severed
mutability for everything below it. A write below a `WITH` silently did not
happen. Fixing the operator tree, not the grammar, is what let clause order
become free (#617, #622).

**This number was nondeterministic until 2026-08-18.** Running the TCK five
times at a fixed commit gave 484, 485, 486 and 487 passes while `errored`
stayed constant — three scenarios were changing their *answer* between
processes, because `RandomState` seeds each process's hash maps differently
and that order was reaching results. Any single figure published before then,
including the 486 previously on this page, was one sample of a distribution.
The three defects behind it are fixed (#610); the count is stable now, and
`--failures-manifest` exists so the check is one diff rather than an
inference.

**Both numbers have to be quoted together.** The pass rate says what the engine
gets right among the scenarios this harness can judge; the coverage says how
many it can judge at all. A harness that counted its own unimplemented steps as
passes would report a flattering number, and one that counted them as failures
would mislead the other way — so they are separated, and the skip reasons are
published:

| skipped | reason |
|---:|---|
| 274 | `Scenario Outline` — the harness does not expand `Examples` tables |
| 39 | user-defined procedures |
| 34 | query parameters |
| 19 | named fixture graphs (`binary-tree-N`) |
| 7 | the scenario's setup still does not parse |
| 3 | the scenario's setup did not run |

"setup did not parse" fell from **197 to 7**: they were ordinary `CREATE`
statements the parser rejected, and fixing that moved them into the judged set
— which is where most of the coverage gain came from. `Scenario Outline`
expansion is now the single largest remaining skip category and is a harness
gap rather than an engine one.

Weakest areas, among features with at least 5 evaluated scenarios — all at 0%:
`Boolean5`, `Comparison3`, `Create3`, `Create6`, `Match6`, `Merge6`, `Set4`,
`Set5`, `Temporal5`, `Temporal6`, `Union1`, `Union2`; then `Pattern1` at 4% and
`Match5` at 12%.

### How this relates to the hand-written sweeps

Four hand-written sweeps in `examples/cypher_probe*.rs` (168 cases) pass
100%, 100%, 100% and 29/30. That is not in tension with 57.2% — those sweeps
were written to probe areas suspected of being wrong, and every case in them was
either already correct or has since been fixed. They found seven silent
wrong answers; they were never a coverage measurement. **The TCK is the coverage
measurement**, and it says there is a great deal left.

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
| IS2 | Recent Posts by Person | 0.04 ms | 1.10 ms |
| IS3 | Friends of Person | 0.06 ms | 1.80 ms |
| IS4 | Post Content | 0.01 ms | 0.01 ms |
| IS5 | Post Creator | 0.02 ms | 0.02 ms |
| IS6 | Forum of Post | 0.03 ms | 0.06 ms |
| IS7 | Replies to Post | 1.1 ms | 11.50 ms |

## Complex reads (IC1–IC14)

| Query | Name | SF1 | previous SF1 | SF10 |
|---|---|---|---|---|
| IC1 | Transitive Friends by Name | **55.3 ms** | 59.5 ms | 14.0 s |
| IC2 | Recent Friend Posts | **4.3 ms** | 9.7 ms | 306 ms |
| IC3 | Friends in Countries | **464 ms** | 862 ms | 15.7 s |
| IC4 | Popular Tags in Period | **7.8 ms** | 13.9 ms | 527 ms |
| IC5 | New Forum Members | **765 ms** | 1203 ms | 31.1 s |
| IC6 | Tag Co-occurrence | **157 ms** | 185 ms | 31.5 s |
| IC7 | Recent Likers | **0.05 ms** | 0.09 ms | 1.70 ms |
| IC8 | Recent Replies | **0.13 ms** | 0.16 ms | 4.00 ms |
| IC9 | Recent FoF Posts | **713 ms** | 1131 ms | 26.3 s |
| IC10 | Friend Recommendation | **90.2 ms** | 104 ms | 2.3 s |
| IC11 | Job Referral | **30.0 ms** | 32.2 ms | 4.5 s |
| IC12 | Expert Reply | **56.9 ms** | 84.7 ms | 3.2 s |
| IC13 | Single Shortest Path | **40.2 ms** | 43.4 ms | 37.00 ms |
| IC14 | Trusted Connection Paths | **46.2 ms** | 49.8 ms | 696 ms |

**Whole suite: 10.1 s, 21/21 passed, 0 empty, 0 errors.**

Run-to-run variance on this host is about ±5% (9.6–10.1 s across the runs
taken this round, all at the same 43 ms calibration). The figure above is the
most recent, taken at load 0.63 — the quietest of them. A difference smaller
than that is not a result.

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
