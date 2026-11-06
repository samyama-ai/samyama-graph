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

## Natural-language queries

`nlq` turns a question into Cypher. It returns the query and does not run it;
a generated query that writes is refused.

```python
client = SamyamaClient.embedded()
# ... load a graph ...
cypher = client.nlq("Who does Alice know?", provider="openai")  # key from OPENAI_API_KEY
result = client.query_readonly(cypher)
```

In embedded mode the translation happens in this process: the model is shown
this graph's schema, as the server would show it. The provider is read from
the same environment variables the server reads, and the keyword arguments
take their place for one call:

| argument       | environment variable it replaces                     |
|----------------|------------------------------------------------------|
| `provider`     | `NLQ_PROVIDER` — `openai`, `gemini`, `ollama`, `claudecode` or `mock`; no default |
| `model`        | `NLQ_MODEL` (default `gpt-4o`)                       |
| `api_key`      | the provider's own: `OPENAI_API_KEY`, `GEMINI_API_KEY` |
| `api_base_url` | `NLQ_API_BASE_URL`                                   |

No server is involved, but the provider still is: `openai` and `gemini` send
the question and the schema summary to that service. `mock` answers a fixed
query without leaving the process, for tests.

A remote client asks the server (`POST /api/nlq`), which chooses its own
provider, so passing any of these arguments to it raises `ValueError`.

## More

- [`docs/SDK-API-CLI-ARCHITECTURE.md`](https://github.com/samyama-ai/samyama-graph/blob/main/docs/SDK-API-CLI-ARCHITECTURE.md)
  — how the SDKs, the HTTP API and the CLI fit together.
- [`sdk/README.md`](https://github.com/samyama-ai/samyama-graph/blob/main/sdk/README.md)
  — the other two SDKs (TypeScript, Rust).
- [`samyama_mcp/README.md`](https://github.com/samyama-ai/samyama-graph/blob/main/sdk/python/samyama_mcp/README.md)
  — the MCP server.

Apache-2.0.
