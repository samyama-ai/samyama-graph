# Legal Judgments Knowledge Graph — Case Study

589 judgments of the Supreme Court of India (2016) — judges, parties, cited legal
sections and topics modelled as a property graph. Questions a legal researcher would
ask ("which sections are cited most?", "which judges sit together most often?", "which
laws span the widest range of subjects?") become single Cypher traversals.

Reproduces a public reference demo (PostgreSQL + Apache AGE + pgvector) by Shreyas Rao
on Samyama — one engine instead of three.

![Legal Judgments demo](demo.gif)

```bash
cd case_studies/legal-judgments && ./run.sh   # validate every query against the snapshot
RECORD=1 ./run.sh                              # also regenerate demo.gif
```

## The graph

**Scale:** 4,462 nodes · 8,363 edges (imported from a small snapshot in seconds)

| Node label | Count | Key properties |
|------------|-------|----------------|
| Topic | 2,291 | text, category |
| Party | 1,102 | name |
| Case | 589 | id, title, year, month |
| Act | 446 | name |
| Judge | 34 | name |

**Relationships (4):**

| Relationship | Pattern | Count |
|---|---|---|
| `ABOUT` | Case → Topic | 3,041 |
| `CITES` | Case → Act (property: `section`) | 2,749 |
| `PARTY_IN` | Party → Case (property: `role`) | 1,309 |
| `DECIDED` | Judge → Case | 1,264 |

The `section` lives on the `CITES` edge, so section-level questions
("how many judgments cite IPC §302?") are answerable — reproducing the reference's
headline result exactly.

## Benchmark — head-to-head vs Apache AGE

The same 4,462-node graph was loaded into **both** Samyama and Apache AGE (`apache/age`, AGE 1.7.0 on
PostgreSQL 18.1) on the same machine, and the same queries run against each — median of 40 warm
round-trips (Apache AGE via a persistent psycopg2 connection; Samyama via HTTP):

| Query | Apache AGE | Samyama | Samyama is |
|---|---|---|---|
| Judges by case count | 34 ms | **0.94 ms** | **36× faster** |
| Most-cited sections | 23 ms | **1.5 ms** | **15× faster** |
| Laws by topic breadth (2-hop) | 155 ms | **19 ms** | **8× faster** |
| Co-sitting judges (2-hop) | 25 ms | **16 ms** | **1.5× faster** |

**Why:** Apache AGE runs Cypher *inside* PostgreSQL via a `cypher('graph', $$…$$)` function, so Postgres
parses and plans the query **from scratch on every call** — `EXPLAIN ANALYZE` shows planning 8.7 ms +
execution 5.5 ms, i.e. most of AGE's time is planning, repeated each call. Samyama parses once and caches
the plan (a warm query is just execution), and executes over a native in-memory graph rather than
translating Cypher → SQL over Postgres rows. The gap is largest on aggregation-heavy queries (15–36×) and
narrows to 1.5× on the 2-hop join, where both engines do real traversal work.

*Method: same host, warm connection, median of 40 round-trips; two client transports (Postgres binary vs
HTTP). A like-for-like local comparison at this dataset size — not a large-scale benchmark.*

## Showcase queries

See [`queries.cypher`](queries.cypher). Every query passes the **Definition-of-Done gate**
(the build fails if any returns zero rows), so nothing in the demo is staged:

| # | Query | Result | Gate |
|---|---|---|:---:|
| 1 | Most productive judges | Dipak Misra — 104 | ✅ |
| 2 | Most-cited legal sections | IPC §302 — 57 | ✅ |
| 3 | Judges who most often sit together | Kurian Joseph & Rohinton F. Nariman — 55 | ✅ |
| 4 | Laws cited together | Indian Evidence Act + IPC — 21 | ✅ |
| 5 | Laws spanning the widest range of topics | Constitution of India — all 11 categories | ✅ |
| 6 | Docket by topic category | 11 categories, 3,041 labels | ✅ |

Queries 1–2 match the reference demo's **published numbers exactly**: top judge Dipak Misra
104; IPC §302 cited in 57 judgments; Constitution Article 32 in 36. (Per-query timings are in
the Benchmark table above.)

## Data & license

Source: [`Shreyasrao/Indian-law-supreme-court-judgements-2016`](https://huggingface.co/datasets/Shreyasrao/Indian-law-supreme-court-judgements-2016)
(revision `e928c72019d6`). Originally from the Indian Supreme Court Judgments registry
on AWS Open Data, managed by Dattam Labs.

**License:** CC-BY-4.0.

Snapshot built by [`examples/legal_judgments_loader.rs`](../../examples/legal_judgments_loader.rs)
from the 9 node/edge CSVs, and published as `legal-judgments.sgsnap` on a release.
