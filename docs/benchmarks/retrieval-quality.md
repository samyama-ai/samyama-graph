# Retrieval quality: BM25, dense and fusion on BEIR / MS MARCO (#1988)

```sh
python3 scripts/prepare_beir_datasets.py scifact          # download + embed (all-MiniLM-L6-v2)
cargo bench --bench retrieval_quality_benchmark -- --dataset data/beir/scifact --exact --csv rq.csv
cargo bench --bench retrieval_quality_benchmark -- --dataset data/beir/msmarco --split dev --csv rq.csv
cargo bench --bench retrieval_quality_benchmark -- --synthetic   # no data: exercises every row
```

With no arguments the bench prints `SKIP:` and exits 0, as `ann_benchmark`
does, so the nightly sweep can run it without data.

`ann_benchmark` measures recall against exact nearest neighbours: how well
HNSW approximates brute force. That is a proxy. This bench asks the question
the proxy stands in for -- are the answers relevant? -- by scoring them
against BEIR's human relevance judgments, the way Elasticsearch, OpenSearch,
Weaviate and Qdrant publish their retrieval quality.

No numbers have been collected in this PR. CI compiles the bench and runs
`tests/retrieval_quality_metrics.rs`; CI timings are not results.

## Data

`scripts/prepare_beir_datasets.py` fetches the BEIR zips (NFCorpus, SciFact,
FiQA, TREC-COVID, NQ, and MS MARCO whose judged split is `dev`) and, unless
`--no-embed`, embeds every document and query with **one** sentence-transformers
model (`all-MiniLM-L6-v2` by default; `--model BAAI/bge-base-en-v1.5
--query-prefix "Represent this sentence for searching relevant passages: "`
for bge). Vectors go to `dense/` in the TEXMEX `.fvecs` layout
`benches/ann_common` already reads, with id files giving the row order, the
metric (`cosine`, unit-normalised) and the model name. Every system -- this
engine and each competitor -- searches those same vectors, so a quality
difference between systems is not a model difference.

A document is `title + " " + text`; BM25 indexes that string and the model
embeds it. A query reaches the full-text index as its alphanumeric runs
joined by spaces: BEIR queries are natural language, and a stray `"` or `*`
would otherwise become a phrase or prefix query.

## Configurations

| config | what runs |
|---|---|
| `bm25` | the full-text index: `CREATE FULLTEXT INDEX` over the document text, BM25 with k1 = 1.2, b = 0.75 |
| `dense-hnsw` | the HNSW index at each `--ef` (default 100, 200, 400), m = 16, ef_construction = 200 |
| `dense-exact` | brute force over the same vectors (`--exact`). The ceiling HNSW approximates; not timed |
| `rrf-client:bm25+dense-hnsw` | reciprocal rank fusion (k = 60) of the two lists above |
| `rrf-client:bm25+dense-exact` | the same over the exact list (`--exact`) |

Reading the rows together answers the issue's question -- which matters more,
ANN approximation or fusion: `dense-exact` minus `dense-hnsw` is what the
index costs in quality at that `ef`, and `rrf-client` minus the better of its
two inputs is what fusion buys.

**The RRF rows are client-side fusion, not #1279's in-engine fusion.** The
harness takes the engine's BM25 list and its dense list, both `--depth` deep,
and fuses them itself, as a user without `db.hybrid.search` would. Their
latency is the two searches back to back plus the fusion; a client issuing
the two in parallel would see the larger of the two instead. The in-engine
hybrid row (`CALL db.hybrid.search`, #2284) is added once that PR is on `dev`.

**SPLADE sparse is pending.** #1978 added `vector.similarity.sparseDot`, a
query-time function over stored sparse vectors, but no sparse index: a SPLADE
row today would be a full scan of the corpus per query, which measures the
scan rather than sparse retrieval and is unusable at NQ or MS MARCO size. The
prepare script does not produce SPLADE vectors either. Both are added when a
sparse index lands.

`--path direct` (the default) calls the indexes directly; `--path cypher`
sends `db.index.fulltext.queryNodes` and `db.index.vector.queryNodes`
through `QueryEngine`, adding parsing, planning and materialisation. Both are
in-process; neither includes a network hop.

## Metrics

As BEIR reports them through `pytrec_eval`, in `benches/retrieval_common`:

- **nDCG@10**, linear gain: DCG = sum of grade / log2(rank + 1) over the top
  10; the ideal DCG uses the judged grades sorted descending. Negative grades
  count 0. TREC-COVID's graded qrels (0/1/2) are where the grading shows.
- **recall@100**: relevant documents (grade > 0) in the top 100 over all
  relevant ones.
- **MRR@10**: reciprocal rank of the first relevant document in the top 10.
- **p50 / p99 / mean latency** per query, one client thread.

Only queries with at least one relevant judgment are scored. A returned
document whose id equals the query id is dropped first, which is BEIR's
default `ignore_identical_ids`. One known difference from trec_eval: trec_eval
re-sorts a run by score and breaks ties by document id; this takes the ranking
as the system returned it, so a run with tied scores can differ in the fourth
decimal.

`tests/retrieval_quality_metrics.rs` checks each formula, the fusion and its
tie-breaking, the BEIR parsers and the synthetic corpus against hand-computed
values, and runs the two Cypher statements the bench sends against a small
store.

## Competitors

```sh
docker compose -f benches/retrieval_quality/docker-compose.yml up -d
python3 benches/retrieval_quality/competitors.py --dataset data/beir/scifact --exact --csv rq.csv
```

`competitors.py` loads the same prepared directory into Elasticsearch 8.15.3,
OpenSearch 2.17.1, Weaviate 1.27.6 and Qdrant 1.12.5 (tags pinned in the
compose file), runs the same configurations with the same HNSW parameters,
depth and query text, fuses with the same RRF, scores with the same formulas,
and appends the same CSV columns, so the files concatenate. **It is not run in
CI.**

What differs is the system under test: each analyzer (`english` for
Elasticsearch/OpenSearch by default, `--analyzer` to change it; Weaviate's
`word` tokenisation has no stemming; this engine lowercases and strips a short
list of suffixes, see `src/index/fulltext.rs`) and each HNSW. Qdrant has no
BM25 of its own, so it reports dense rows only. Each system's own in-engine
hybrid (Elasticsearch's `rrf` retriever, OpenSearch's hybrid query, Weaviate
`hybrid`, Qdrant's query fusion) is not measured: client-side fusion for every
system keeps the fusion constant so the rows compare retrieval.

Competitor latency is over HTTP on loopback with JSON encoding; this bench's
is in-process. Compare the quality columns across systems; compare latency
only with that difference in mind.

## CSV columns

`dataset, system, config, path, embedding_model, metric, corpus, queries, k,
depth, m, ef_construction, ef, rrf_k, ndcg_at_k, recall_at_depth, mrr_at_k,
p50_us, p99_us, mean_us, errors, calibration_ms`

`ef` is the search width: HNSW `ef` here, `num_candidates` in Elasticsearch,
`ef_search` in OpenSearch, the class `ef` in Weaviate, `hnsw_ef` in Qdrant.
Columns that do not apply to a row (HNSW parameters on `bm25`, latency on
exact rows, calibration on competitor rows) are empty, not zero.

## Still required

Collected numbers for the six datasets on a quiet host; the in-engine hybrid
row once #2284 merges; a SPLADE row once a sparse index exists; and, if
wanted, each competitor's own hybrid next to the client-side one.
