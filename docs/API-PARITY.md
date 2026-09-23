# Feature parity matrix (API-02)

Version: 1.0.0

**Generated, not hand-maintained.** Produced by `cargo run --release --example feature_matrix`, which searches the real source of each surface for specific evidence (an OpenAPI path, a RESP command string, an SDK method, an MCP tool guard) and reports what it found — the same file this generator is defined in has the exact evidence string used for every cell. Regenerate after any change to a surface; a cell that goes stale means the evidence string changed shape (e.g. a method was renamed) and the generator's needle needs updating to match, which is a smaller and more visible failure than a hand-written table silently drifting.

**Not generated: whether a present capability actually works.** This is a static probe over source text, not a live call through each surface — a route that exists but is broken at runtime reads as present here. `examples/api_contract.rs` catches OpenAPI/server drift the same way for the HTTP surface specifically; nothing today drives a live Cypher query through all six surfaces and compares the answers. That would be the natural follow-up.

| Capability | HTTP | RESP | Rust SDK | Python SDK | TypeScript SDK | MCP |
|---|---|---|---|---|---|---|
| **Cypher — read-only query**<!-- cypher_read --> | Yes — `POST /api/query` | Yes — `GRAPH.RO_QUERY` | Yes — `SamyamaClient::query_readonly` | Yes — `.query_readonly()` | Yes — `.queryReadonly()` | Yes — `cypher_query` tool |
| **Cypher — read-write query**<!-- cypher_write --> | Yes — `POST /api/query` | Yes — `GRAPH.QUERY` | Yes — `SamyamaClient::query` | Yes — `.query()` | Yes — `.query()` | **No — blocked by design** (write keywords rejected before execution) |
| **Multi-statement transactions (begin / commit / rollback)**<!-- transactions --> | Yes — `/api/tx/begin`, `/api/tx/{id}/commit`, `/api/tx/{id}/rollback` | No — no `GRAPH.TX*` command is registered | No — not on the `SamyamaClient` trait | No method exposed | No method exposed | No — every tool is one query, and write tools don't exist anyway |
| **Graph algorithms (PageRank, BFS, WCC, SCC, Dijkstra, MST, …)**<!-- graph_algorithms --> | No endpoint | No command | **Embedded-only** — `AlgorithmClient` (pagerank/wcc/scc/bfs/dijkstra/maxflow/mst/triangles/cdlp/clustering-coeff/pca) | **Embedded-only** — `.page_rank()`, `.wcc()`, `.scc()`, `.bfs()`, `.dijkstra()`, `.pca()`, `.triangle_count()` all call `require_embedded()` | No method | **Embedded-only, conditionally** — `pagerank`/`shortest_path`/`communities` tools register only if the wrapped client exposes `page_rank` |
| **Vector index create / add / k-NN search**<!-- vector_search --> | Yes — `/api/vector-search`, `/api/vector/indexes` | No command | **Embedded-only** — `VectorClient` | **Embedded-only** — `.vector_search()` calls `require_embedded()` | No method | **Embedded-only, conditionally** — `find_similar_*` tools |
| **Portable snapshot export / import (.sgsnap)**<!-- snapshot --> | Yes — `/api/snapshot/export`, `/api/snapshot/import` | No command | **Embedded-only** — `EmbeddedClient::export_snapshot`/`import_snapshot`(not on the `SamyamaClient` trait, so `RemoteClient` has neither) | No method exposed | No method | No tool |
| **Bulk import (CSV / JSON / Parquet)**<!-- bulk_import --> | Yes — `/api/import/csv`, `/api/import/json`, `/api/import/parquet` | No command | No method exposed | No method exposed | **Partial** — `.importCsv()`, `.importJson()`; no `importParquet` | No tool |
| **Natural-language-to-Cypher (NLQ)**<!-- nlq --> | Yes — `/api/nlq` | No command | **Embedded-only** — `EmbeddedClient::nlq_pipeline()` returns the `NLQPipeline`; not on the shared trait | No method exposed | No method | No — tools are generated from schema instead (see note) |
| **Schema / index discovery**<!-- schema_introspection --> | Yes — `/api/schema` | Via Cypher only — `SHOW INDEXES`/`SHOW CONSTRAINTS` over `GRAPH.QUERY` | Via Cypher only — no dedicated method on the trait | Via Cypher only — no dedicated method | Yes — `.schema()` | Yes — `schema_info` tool |
| **Multi-graph management (list / delete)**<!-- multi_graph --> | Yes — `/api/tenants`, `/api/tenants/{id}` | Yes — `GRAPH.LIST`, `GRAPH.DELETE` | Yes — `.list_graphs()`, `.delete_graph()` | Yes — `.list_graphs()`, `.delete_graph()` | Yes — `.listGraphs()`, `.deleteGraph()` | No — the server is bound to one graph at construction |

Legend: plain **Yes** = a first-class method/endpoint/tool exists. **Embedded-only** = real, but only when the client is built in-process (`SamyamaClient.embedded()`), not against a remote server. **Via Cypher only** = reachable by sending Cypher through the surface's normal query call, not as a named method. **Partial** = some but not all of the capability's formats/operations are covered. **No — blocked by design** = deliberately absent, not missing.

## Notes

**Cypher — read-write query.** MCP has no write path anywhere: every generated and custom tool is funnelled through `is_readonly_cypher`, which rejects CREATE/SET/DELETE/MERGE/DROP/FOREACH/LOAD/CALL before the query reaches the graph (`escape.py`). That is a deliberate safety choice, documented in `sdk/python/samyama_mcp/README.md` ("Security — read-only by construction"), not an oversight.

**Multi-statement transactions (begin / commit / rollback).** No SDK exposes a transaction handle. A caller on the Rust, Python or TypeScript SDK, or over RESP, gets exactly the atomicity of one query; the only way to hold a transaction open across statements today is raw HTTP against `/api/tx/*`.

**Graph algorithms (PageRank, BFS, WCC, SCC, Dijkstra, MST, …).** There is no HTTP or RESP path to these at all — `/optimize/*` is the metaheuristic solver (simulated annealing etc. for scheduling/routing problems), a different feature, not graph analytics. The Rust and Python SDKs both gate algorithm methods behind `require_embedded()` (`crates/samyama-sdk/src/algo.rs`: "extension trait ... (EmbeddedClient only)"; `sdk/python/src/lib.rs`: "Algorithm methods are only available in embedded mode"), so a client built with `SamyamaClient.connect(url)` cannot call them — only `SamyamaClient.embedded()` can. The MCP server inherits that split: `AlgorithmToolGenerator` only registers tools when `hasattr(self.client, "page_rank")`, i.e. only when it wraps an embedded Python client.

**Vector index create / add / k-NN search.** Same split as the algorithms row: the HTTP API serves it directly, the Rust and Python SDKs serve it only in embedded mode, and MCP's `VectorToolGenerator` mirrors that by checking `hasattr(self.client, "vector_search")` before registering `find_similar_*` tools.

**Bulk import (CSV / JSON / Parquet).** The TypeScript SDK covers two of the three formats the HTTP API serves; there is no evidence of a `importParquet` method.

**Natural-language-to-Cypher (NLQ).** MCP does not need this the way the other surfaces might: instead of translating a question to Cypher, it generates a typed tool per label/edge/algorithm/vector-index so the agent calls those directly. Whether that substitutes for NLQ is exactly AI-03's open question, not this one's.

**Schema / index discovery.** "Via Cypher only" means the capability exists but not as a named method — a caller sends `SHOW INDEXES` / `SHOW CONSTRAINTS` through the same query call every other Cypher statement uses. That is a real capability, just not a discoverable one from a method list or an IDE's autocomplete.

**Multi-graph management (list / delete).** MCP has no tool for this because a server is bound to one graph at construction (`SamyamaMCPServer(client, graph="...")`); managing which graphs exist is an operator action taken before the server starts, not something the agent it serves does.

## Summary

| Capability | Full | Scoped/partial | Absent |
|---|---|---|---|
| Cypher — read-only query | 6 | 0 | 0 |
| Cypher — read-write query | 6 | 0 | 0 |
| Multi-statement transactions (begin / commit / rollback) | 1 | 0 | 5 |
| Graph algorithms (PageRank, BFS, WCC, SCC, Dijkstra, MST, …) | 0 | 3 | 3 |
| Vector index create / add / k-NN search | 1 | 3 | 2 |
| Portable snapshot export / import (.sgsnap) | 1 | 1 | 4 |
| Bulk import (CSV / JSON / Parquet) | 1 | 0 | 5 |
| Natural-language-to-Cypher (NLQ) | 1 | 1 | 4 |
| Schema / index discovery | 3 | 0 | 3 |
| Multi-graph management (list / delete) | 5 | 0 | 1 |

---

_10 capabilities x 6 surfaces, generated from this commit's source tree (not a live probe). Regenerate: `cargo run --release --example feature_matrix`._
