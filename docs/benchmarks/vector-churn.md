# Vector churn: recall under insert, delete and update (#1986)

```sh
# Random churn, the default (and what a run with no arguments does at 10000x32)
cargo bench --bench vector_churn_benchmark -- --synthetic 100000x128 --cycles 20 --csv churn.csv

# Drift: a sliding window over a moving distribution
cargo bench --bench vector_churn_benchmark -- --mode drift --synthetic 100000x128 --csv churn.csv

# A Big-ANN 2023 streaming-track runbook over real vectors (.fbin or .fvecs)
cargo bench --bench vector_churn_benchmark -- --runbook <runbook.yaml> --runbook-name <dataset> \
    --base base.fbin --query query.fbin --metric l2 --csv churn.csv
```

Run on a quiet benchmark host, in `--release`, and compare the calibration
line before comparing timings. CI compiles the target and checks the
harness's workload generation, runbook reader and scoring
(`tests/vector_churn_harness.rs`); CI timings are not published results. No
numbers have been collected in the PR that added this.

## Workloads

| workload | each cycle | live set |
|---|---|---|
| `churn` | a random `--churn` share of live vectors: half deleted and replaced by new vectors under new ids, half given a new vector in place | constant |
| `drift` | the oldest `--churn` share deleted, as many new vectors inserted; synthetic data drawn from cluster centres that move `--drift` over the run, queries drawn from the time span then live | constant |
| runbook | the file's `insert`, `delete`, `replace` and `search` steps, in step order | as the runbook says |

`drift` is research idea #31 from the issue. Over a real dataset without a
runbook, rows are taken in file order, so `drift` is a sliding window over
the file and moves only as far as the file's order does.

A runbook step is read as big-ann-benchmarks' streaming runner reads it:
`insert start..end` adds rows `start..end` under ids equal to their row
numbers, `delete start..end` removes those ids, and `replace` gives ids
`tags_start..tags_end` the vectors of rows `ids_start..ids_end`. Only the rows
the runbook reaches are read from the base file.

## What each search step reports

| column | meaning |
|---|---|
| `recall` | recall@k of the churned index against exact search over the live set, computed outside the timed region |
| `fresh_recall`, `recall_gap` | recall@k of an index built from scratch on the same live set, and fresh minus churned. A positive gap is what churn cost; the absolute recall alone mixes that with how hard the live set is |
| `superseded`, `dead_share` | dead entries still in the graph (`VectorIndex::superseded`), and their share of all entries |
| `compactions` | rebuilds the index did on its own, cumulative. It rebuilds when a fifth of the graph is dead (#1968) |
| `path` | `scan` when the index answered by linear scan instead of the graph: always at 128 live vectors or fewer, and with dead entries present once `ef * 2m` reaches the live count. A `scan` step's recall says nothing about the graph |
| `search_qps`, `search_p50_us`, `search_p99_us` | the scored searches, single client |
| `insert_ops_s`, `delete_ops_s`, `update_ops_s` | writes since the previous search step, per second of time spent in them |
| `write_p99_us`, `write_max_us` | write latency; a write that triggered a rebuild shows in the max |
| `probe_p50_us`, `probe_p99_us` | unscored searches run after every `--probe-every` writes: search latency while writes are going on |
| `resident_bytes`, `rss_bytes` | `VectorIndex::resident_bytes`, and process RSS (Linux only) |

## Scope

The index is driven directly through `VectorIndex::add`, `remove` and
`search_ef`, single client, in process. It measures how the index holds up
under maintenance, not the store's write path, Cypher, or the network.
Searches and writes are interleaved on one thread, not concurrent: the
store serialises writes against an index behind a lock, so concurrent
readers would measure that lock rather than the index.

Not done here, and still required for the issue:

- **Published Big-ANN results.** The runbook reader is in place, but no run
  over MSTuring-30M (clustered runbook) or the final runbook has been made,
  and the runbooks' published per-step ground truth is not read: truth is
  recomputed by brute force at each search step, which at 30M vectors is
  expensive. `u8bin`/`i8bin` datasets are not read.
- **Other systems.** Qdrant, Milvus, Weaviate, pgvector and FAISS
  (`remove_ids`) as the floor belong in `samyama-graph-competitor-benchmarks`,
  which measures every system over its network API on the same host. This
  bench's CSV columns are meant to be read alongside those results.
