# Modern-embedding scale benchmark (#1984)

SIFT and GloVe results do not transfer to LLM embeddings. This protocol uses
VectorDBBench's cases so samyama-graph's rows line up with its public
leaderboard, and carries the evidence row for NDS-05.

| case | VectorDBBench case type | vectors | dim | metric |
|---|---|---|---|---|
| `cohere-1m` | `Performance768D1M` | 1,000,000 | 768 | cosine |
| `cohere-10m` | `Performance768D10M` | 10,000,000 | 768 | cosine |
| `openai-500k` | `Performance1536D500K` | 500,000 | 1536 | cosine |
| `openai-5m` | `Performance1536D5M` | 5,000,000 | 1536 | cosine |

`cohere-100k` and `openai-50k` exist for smoke runs.

No benchmark numbers have been collected yet. CI compiles the bench and tests
its arithmetic; CI timings are not results.

## Two measurements

**In-process** — `benches/vector_scale_benchmark.rs`. The engine's ceiling,
without the network:

```sh
pip install numpy pyarrow
python3 scripts/download_vectordbbench_datasets.py cohere-1m      # -> data/vdbb/cohere-1m
cargo bench --bench vector_scale_benchmark -- --dataset data/vdbb/cohere-1m \
    --price-per-hour 0.40 --instance "r6i.4xlarge spot us-east-1" --json cohere-1m.json
```

**Through VectorDBBench** — `benches/vectordbbench/`, a client that runs the
server over HTTP inside VectorDBBench. This is the row that compares with the
leaderboard; see its README for the four registration edits.

## What the bench reports

| figure | how |
|---|---|
| load time | nodes created with their vectors, before any index |
| build time | `CREATE VECTOR INDEX` over the loaded nodes |
| recall@100 | VectorDBBench's figure, against the dataset's ground truth (1,000 neighbours per query) |
| recall@10 | NDS-05's figure, from a separate k = 10 search at the same width |
| serial p99 | one client thread, k = 100, at the operating width |
| max QPS | the best of a client-thread sweep (`1,2,4,…,cores`) at the operating width; points with failed queries are not eligible |
| peak RSS | `VmHWM`, Linux only; reported as unknown elsewhere |
| $ / million queries | `--price-per-hour` ÷ (max QPS × 3600) × 10⁶ — host time only |

The **operating width** is the fastest `ef` of the serial sweep whose
recall@100 reaches `--recall-target` (0.95). If none does, the highest-recall
width is used and the cost line says the target was missed. `--operating-ef`
overrides the choice.

## Memory check

Before anything is read, the bench estimates what the run needs — four f32
copies of every vector (the base set it scores against, the node property,
the HNSW graph's copy, the index's persistence copy), up to `2·M` 40-byte
links, and a 512-byte per-node allowance — and compares it with
`MemAvailable`, keeping 10% free. A run that will not fit stops with the
numbers instead of swapping through an hour of loading. At 10M × 768-d that
estimate is about 130 GiB; a server, without the bench's base copy, holds the
three copies the issue counts (~92 GB) plus the graph. `--limit N` measures a
prefix (ground truth is then recomputed by brute force); `--skip-memory-check`
runs anyway.

## NDS-05

NDS-05 is recall@10 ≥ 0.95 at 10M vectors. The bench prints one line for it,
naming the vector count and `ef` it was measured at:

- at ≥ 10M: `NDS-05: MET` or `NDS-05: NOT MET`;
- below 10M: `NDS-05: NOT EVIDENCE`, whatever the recall. A 1M result that
  clears 0.95 says nothing about 10M.

## Not done

- **No numbers.** Nothing has been run at 1M, 5M or 10M.
- **10M does not fit a commodity host yet.** It needs the quantization work
  (fp16 halves the index copies) and the persistence work (startup rebuild of
  10M) the issue names as prerequisites.
- **The VectorDBBench client has not been run against a live server** and has
  not been proposed upstream.
- **Spot pricing is an input**, not looked up: pass the price of the host the
  run used.
- **Competitors.** The same cases for the systems in the recall–QPS harness
  issue (#1983) are run by VectorDBBench itself, not by this repository.
