# samyama — Python SDK for the Samyama Graph Database

Python bindings for [Samyama](https://github.com/samyama-ai/samyama-graph), a
property-graph database with a Cypher query engine. The extension module is
built from Rust with PyO3 on top of `crates/samyama-sdk`, so the embedded mode
runs the engine in-process — no server, no network.

## Install

```bash
pip install samyama
```

Wheels are `abi3` and work on CPython 3.8+.

The package also installs `samyama_aio` (asyncio wrapper), `samyama_nb`
(notebook helpers) and `samyama_mcp` (an MCP server, entry point
`samyama-mcp-serve`).

## Connect

Two modes. Embedded runs the engine inside your process:

```python
from samyama import SamyamaClient

client = SamyamaClient.embedded()
```

Remote talks to a running Samyama server over HTTP:

```python
from samyama import SamyamaClient

client = SamyamaClient.connect(
    "http://localhost:8080",
    timeout_seconds=30.0,          # whole-request deadline
    connect_timeout_seconds=5.0,   # TCP connect only
    max_retries=2,
    retry_base_delay_ms=100,
)
```

Both return the same `SamyamaClient`, with the same query methods. Graph
algorithms (`page_rank`, `wcc`, `scc`, `bfs`, `dijkstra`, `pca`,
`triangle_count`) and the vector methods (`create_vector_index`, `add_vector`,
`vector_search`) are embedded-only and raise `RuntimeError` on a remote client.

## First query

```python
from samyama import SamyamaClient

client = SamyamaClient.embedded()

client.query('CREATE (:Person {name: "Alice"})-[:KNOWS]->(:Person {name: "Bob"})')

result = client.query_readonly(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
)

print(result.columns)   # ['a.name', 'b.name']
for row in result.records:
    print(row)          # ['Alice', 'Bob']
print(len(result))      # 1
```

`query` is read-write, `query_readonly` is read-only. Both take an optional
`graph` argument, which defaults to `"default"`; this build serves a single
graph and rejects any other name.

A `QueryResult` exposes `columns`, `records`, `nodes`, `edges`, and `len()`.

Other methods on the client: `status()` (returns a `ServerStatus` with
`status`, `version`, `nodes`, `edges`), `ping()`, `list_graphs()`,
`delete_graph(graph="default")`.

## Snapshots

A `.sgsnap` file is a whole graph in one file. In embedded mode the client
loads and saves one itself, so a notebook, an evaluation harness or an agent
needs no server and no network:

```python
from samyama import SamyamaClient

client = SamyamaClient.open_snapshot("pubmed.sgsnap")   # embedded, ready to query
client.query_readonly("MATCH (a:Article) RETURN count(a) AS n")

client.query('CREATE (:Note {text: "checked"})')
report = client.export_snapshot("pubmed-annotated.sgsnap")
print(report["nodes_exported"], report["bytes_written"], report["dropped"])
```

`import_snapshot(path)` adds a snapshot to a client's graph and returns a report
with the keys of the server's import response (`nodes_imported`,
`edges_imported`, `nodes_merged`, `merges_by_key`, `dedup_keys_unused`, ...)
plus `duration_ms`. Pass `dedup_keys=["drugbank_id"]` to merge incoming nodes
into existing ones that share a key value; `open_snapshot` takes the same
argument. `export_snapshot` replaces any file at `path`, and its `dropped` list
says what the format does not carry.

**Vectors added with `add_vector` are not carried.** `add_vector` puts a vector
in the vector index only, not on the node as a property, and a snapshot holds
node properties and index *declarations*. After `open_snapshot` the index exists
but is empty, and `vector_search` returns nothing; the export's `dropped` list
does not report it yet. Re-add the vectors after opening.

All three are embedded-only: a remote client raises `RuntimeError`, because a
server imports and exports its own snapshots (`POST /api/snapshot/import` and
`POST /api/snapshot/export`). A file that is missing
or is not a snapshot raises `RuntimeError` and leaves the graph unchanged.

## asyncio

`samyama_aio` wraps the same client, running each call on a worker thread so
the event loop is not blocked:

```python
import asyncio
from samyama_aio import AsyncSamyamaClient

async def main():
    client = await AsyncSamyamaClient.embedded()
    await client.query('CREATE (:Person {name: "Alice"})')
    result = await client.query_readonly("MATCH (n:Person) RETURN n.name")
    print(result.records)   # [['Alice']]

asyncio.run(main())
```

`AsyncSamyamaClient.connect(url, **kwargs)` takes the same keyword arguments as
`SamyamaClient.connect`.

## More

- [`docs/SDK-API-CLI-ARCHITECTURE.md`](https://github.com/samyama-ai/samyama-graph/blob/main/docs/SDK-API-CLI-ARCHITECTURE.md)
  — how the SDKs, the HTTP API and the CLI fit together.
- [`sdk/README.md`](https://github.com/samyama-ai/samyama-graph/blob/main/sdk/README.md)
  — the other two SDKs (TypeScript, Rust).
- [`samyama_mcp/README.md`](https://github.com/samyama-ai/samyama-graph/blob/main/sdk/python/samyama_mcp/README.md)
  — the MCP server.

Apache-2.0.
