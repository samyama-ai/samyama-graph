# Evaluation sets from a knowledge graph

`samyama eval-gen` turns a `.sgsnap` snapshot into an evaluation set: questions
with gold answers, difficulty labels and unanswerable items (ML-14, #1772). It
is the generator KG-08 and EVAL-04 also point at. Before it, 2 of 21 KG
repositories shipped a question set, each written by hand.

```bash
samyama eval-gen football.sgsnap --out football.sgqueries            # 500 questions, seed 0
samyama eval-gen football.sgsnap --out f.sgqueries --count 100 --seed 7
samyama verify   football.sgsnap --queries football.sgqueries         # re-check every gold answer
```

Exit codes: `0` written (even if short of `--count`: the report says why),
`64` usage, `66` the snapshot cannot be opened, `74` the output cannot be
written, `1` anything else.

## What comes out

A question catalog in the `samyama.queries/1` format (`.sgqueries`, #1159), so
`verify`, `catalog-gate` and the KG-08 conformance check all read it unchanged.
Each entry carries:

| Field | Meaning |
|---|---|
| `question` | Templated English naming the labels, properties and relationship types exactly |
| `difficulty` | `easy`, `medium` or `hard` |
| `cypher` + `params` | The query that answers it. Values are **bound parameters**, never spliced into the text (#1156) |
| `answer` | The gold answer as `{"columns": [...], "rows": [[...]]}` |
| `rows`, `hash`, `work` | As in any catalog: what `verify` and `run_template` check |
| `unanswerable` | `true` for a question whose correct answer is "nothing" |

`generated_by` records the engine version and the seed, e.g.
`samyama eval-gen 1.16.0 seed=0`. A catalog from `eval-gen` is private
(`publishable: false`), like any catalog not built with `catalog-build --release`.

## The questions

The graph's own schema fills a fixed set of templates: its labels, an identifying
property per label, the numeric properties, and the relationships that actually
occur between labels.

| Template | Difficulty | Example (football-kg) |
|---|---|---|
| `easy-count` | easy | How many Goal nodes are in the graph? |
| `easy-lookup` | easy | What is the stage of the Match whose match_id is "M-2007-13"? |
| `easy-extreme` | easy | What is the smallest count_tournaments of any Player? |
| `medium-one-hop` | medium | Which Tournament (by name) does the Match whose match_id is "M-2002-10" have an IN_TOURNAMENT relationship to? |
| `medium-incoming` | medium | Which Goal (by goal_id) have a SCORED_IN relationship to the Match whose match_id is "M-2015-22"? |
| `medium-count-neighbours` | medium | How many distinct Country does the Tournament whose name is "1999 FIFA Women's World Cup" have a HOSTED_BY relationship to? |
| `medium-count-pattern` | medium | How many Goal have at least one SCORED_BY relationship to a Player? |
| `hard-two-hop` | hard | Which Team (by name) are reached from the Goal whose goal_id is "G-0487" by a SCORED_IN relationship to a Match and then a HOME_TEAM relationship? |
| `hard-top` | hard | *(from a drug graph; football-kg has no ranking whose counts differ)* Which 5 Drug (by name) have the most TREATS relationships to a Disease, and how many each? Ties are broken by name. |
| `hard-average-over-hop` | hard | What is the average year of the Tournament that the Match whose match_id is "M-1950-21" has an IN_TOURNAMENT relationship to? |
| `unanswerable-missing-node` | easy | What is the name of the Match whose match_id is "M-1986-1134"? |
| `unanswerable-missing-relationship` | medium | Which Team (by name) does the Goal whose goal_id is "G-3194" have an AWAY_TEAM relationship to? |

The questions are stiff on purpose. A gold answer is only trustworthy if the
question is unambiguous about which query it means, so the wording names schema
elements exactly instead of paraphrasing them.

### The mix

The default is 500 questions: 10% unanswerable (50), and of the rest 35% easy,
35% medium and 30% hard (157 / 157 / 136). Within each difficulty, candidates
are drawn round-robin across its templates, so no single template fills a
bucket. A graph whose schema allows fewer templates has a narrower mix. See
regulatory-affairs below.

### What is kept, and what is not

Every candidate is run before it is kept. It is dropped if:

- it returns no rows, or more than 20 (a list of hundreds is a retrieval task,
  not a question);
- its answer is null (e.g. the average over no values);
- it repeats another question (same query, same values);
- it is a `hard-top` ranking whose counts all tie, because the order would then
  come from the tie-break alone;
- it is meant to be unanswerable but returns rows.

An **identifying property** must be present on every node of the label, each
with a different value, checked over the whole label and not just a sample. So
"the Match whose match_id is …" always names one node. A label without one is
still counted and aggregated, but never looked up.

**Unanswerable items** are near misses, not obvious fakes. A value with digits
has its last number changed (`M-1986-34` → `M-1986-1134`). A value of several
words takes its last word from another key (`Lionel Messi` + `Kylian Mbappe` →
`Lionel Mbappe`). A relationship type that occurs in the graph is asked between
labels it never joins. A suite without such items cannot tell a system that
declines from one that guesses. Each one is run, and kept only if it really
returns nothing.

### Reproducible

The same snapshot and the same seed give the same file, byte for byte. Labels,
properties and node ids are read in sorted order (a label's node set is a
`HashSet`, whose order changes from run to run). Every choice comes from a
SplitMix64 generator written in `src/snapshot/eval_gen.rs` rather than a crate,
so a dependency upgrade cannot change a published suite.

## Measured runs

Release build of `23a204b1`. Intel Core i9-9880H, 16 threads, 32 GB, macOS 15.7.
Seed 0, default count. Time is the wall clock for the whole command, restoring
the snapshot included.

| KG (snapshot SHA-256 prefix) | Nodes / edges | Labels / rel. types | Questions | Time | `verify` | Rerun |
|---|---|---|---|---|---|---|
| legal-judgments (`b93e77666a87`) | 4,462 / 8,363 | 5 / 4 | 500 of 500 | 1.2 s | 500 reproduced | byte-identical |
| football (`090ebc8ec623`) | 16,150 / 12,384 | 8 / 8 | 500 of 500 | 0.6 s | 500 reproduced | byte-identical |
| regulatory-affairs (`53dc9d40e374`) | 28,520 / 25,332 | 3 / 2 | 500 of 500 | 3.5 s | 500 reproduced | byte-identical |

All three meet the KG-08 catalog shape (at least 30 questions, difficulty
labels, unanswerable items). Regulatory-affairs has three labels and two
relationship types and no numeric properties on its targets, so all of its 136
hard questions are two-hop; it has nothing to rank or average.

## Limits

- **Templated, not paraphrased.** Paraphrases (`paraphrases` in the catalog)
  are left empty. Adding them with a model is a separate step, and it would
  have to keep the question's meaning exactly.
- **Answers are for this snapshot.** A gold answer is what this graph says, not
  what is true in the world. A new snapshot needs a new set, and `verify`
  catches a stale one.
- **Large labels cost a scan.** A lookup on a property without an index scans
  the label, and checking that a key is unique reads every node of the label
  once. This is offline work, measured above in seconds on graphs of tens of
  thousands of nodes; it has not been measured at hundreds of millions.
- **Not checked for contamination.** Overlap between a generated set and
  public benchmarks is ML-12 (#1762), a separate check.
