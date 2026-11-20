# samyama-graph client for VectorDBBench (#1984)

A [VectorDBBench](https://github.com/zilliztech/VectorDBBench) client that runs
samyama-graph over its HTTP API, so its Cohere and OpenAI rows are measured the
same way as every other system on VectorDBBench's leaderboard: same datasets,
same ground truth, same load / serial-latency / concurrency procedure, over
the network.

It lives here until it is proposed upstream. To use it, copy it into a
VectorDBBench checkout (a fork, or a local clone) and register it.

## Install

```sh
git clone https://github.com/zilliztech/VectorDBBench && cd VectorDBBench
pip install -e .
cp -r <samyama-graph>/benches/vectordbbench/samyama_graph vectordb_bench/backend/clients/
```

Then four edits:

1. `vectordb_bench/backend/clients/__init__.py`, in `class DB(Enum)`:
   ```python
   SamyamaGraph = "SamyamaGraph"
   ```
2. same file, in `init_cls`:
   ```python
   if self == DB.SamyamaGraph:
       from .samyama_graph.samyama_graph import SamyamaGraph
       return SamyamaGraph
   ```
3. same file, in `config_cls`:
   ```python
   if self == DB.SamyamaGraph:
       from .samyama_graph.config import SamyamaGraphConfig
       return SamyamaGraphConfig
   ```
   and in `case_config_cls`:
   ```python
   if self == DB.SamyamaGraph:
       from .samyama_graph.config import SamyamaGraphIndexConfig
       return SamyamaGraphIndexConfig
   ```
4. `vectordb_bench/cli/vectordbbench.py`:
   ```python
   from ..backend.clients.samyama_graph.cli import SamyamaGraph
   cli.add_command(SamyamaGraph)
   ```

The client needs only `requests`, which VectorDBBench already installs.

## Run

Start a fresh server per case (release build), then:

```sh
cargo run --release            # HTTP on :8080

vectordbbench samyamagraph --url http://127.0.0.1:8080 \
    --case-type Performance768D1M --m 16 --ef-construction 200 --ef-search 128 \
    --db-label r6i.4xlarge-spot
```

Cases the issue names: `Performance768D1M` and `Performance768D10M` (Cohere,
768-d cosine), `Performance1536D500K` and `Performance1536D5M` (OpenAI,
1536-d cosine). Sweep `--ef-search` for a recall–QPS curve; VectorDBBench
searches at k = 100, so the client never searches narrower than 100.

## What the client sends

All through `POST /api/query`:

| step | statement |
|---|---|
| insert | `UNWIND $rows AS r CREATE (:<collection> {id: r.id, embedding: r.v})`, cut to fit the 2 MiB request-body limit (about 97 rows at 768-d) |
| optimize | `CREATE VECTOR INDEX FOR (n:<collection>) ON (n.embedding) OPTIONS {dimensions, similarity: 'cosine', m, ef_construction}` |
| search | `CALL db.index.vector.queryNodes('<collection>', 'embedding', $q, 100, {ef: N}) YIELD node RETURN node.id`, read-only, `cache: false` |

The index is built in `optimize`, after the load, so VectorDBBench's
`load_duration` covers ingest plus build.

## Caveats

- **Not run against a live server in the PR that added it.** It is written
  against the HTTP API as it stands in `src/http` (`QueryRequest` fields
  `params`, `read_only`, `cache`; the `records` array in the response). The
  first real run is the check.
- `drop_old` deletes the collection's nodes; an existing vector index on the
  label is left in place. Start each case on a fresh server.
- Ingest through `/api/query` is JSON, about 20 bytes a float, so load time
  here includes serialisation that a binary protocol would not pay. That is
  part of what a client of this server pays today, so it stays in the number;
  `benches/vector_scale_benchmark.rs` reports the in-process load and build
  separately for comparison.
- 10M × 768-d needs roughly 92 GB for three f32 copies before the graph
  (#1984). Check the host first: `cargo bench --bench vector_scale_benchmark
  -- --dataset <dir>` prints the estimate and refuses a run that will not fit.
