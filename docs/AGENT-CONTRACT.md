# Agent ergonomics contract (AI-16)

**Contract version: 1.0.0** — first published version. See [Changelog](#changelog).

This document describes what an agent (an LLM tool-caller, e.g. Claude via
MCP) can rely on when it talks to Samyama through the **MCP server**
(`sdk/python/samyama_mcp/`). It covers, per AI-16: **tool schemas**, **error
format**, **pagination**, **result envelopes**, and **idempotency**.

It is written from the code as it stands, not from intent. Where the code
does not guarantee something, this document says so instead of promising it
— see [What is not guaranteed](#what-is-not-guaranteed) at the end. A
contract that claims more than the code does is worse than no contract: an
agent that trusts it will be wrong in a way it cannot detect.

**Scope.** This covers `SamyamaMCPServer` and the tools its five generators
(`GenericToolGenerator`, `NodeToolGenerator`, `EdgeToolGenerator`,
`AlgorithmToolGenerator`, `VectorToolGenerator`) and `ToolConfig.custom_tools`
register. It does not cover the HTTP API, the RESP protocol, or the Rust/
Python/TypeScript SDKs directly — those are a different contract (see
`docs/API-PARITY.md` for what each surface can do).

---

## 1. Tool schemas

Every tool is a plain Python function decorated with `mcp.tool()`
(`FastMCP`, pinned in `sdk/python/pyproject.toml` as `fastmcp>=2.0` — no
upper bound). FastMCP derives each tool's JSON Schema (`inputSchema`)
by introspecting the function's **signature and type annotations**; nothing
in this repository writes a schema by hand.

This was verified by installing `fastmcp` (resolved to 4.0.5, the newest
version satisfying the repo's `>=2.0` pin at the time of writing) into a
throwaway environment and registering a function shaped like the generators'
output. For:

```python
def search(query: str, limit: int = 25) -> str:
    """Search docstring.

    Args:
        query: Text to search for.
        limit: Maximum results (default 25, max 100).
    """
```

FastMCP produced:

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["query"],
  "properties": {
    "query": {"type": "string", "description": "Text to search for."},
    "limit": {"type": "integer", "default": 25,
              "description": "Maximum results (default 25, max 100)."}
  }
}
```

From that, and from reading every generator in `sdk/python/samyama_mcp/generators/`:

- **Type mapping** follows the Python annotation on each parameter: `str` →
  `"type": "string"`, `int` → `"type": "integer"`, `float` → `"type":
  "number"`, `list` → `"type": "array"` (untyped — the generators never
  annotate list element types, so `items` is empty; see `find_similar`'s
  `query_vector: list` in `vector_tools.py`).
- **Optional vs. required** is decided by whether the parameter has a
  default. Every generator gives `limit`, `direction`, `max_hops`, `k`,
  `label`, `edge_type`, `filter_property`, `filter_value` a default, so
  they are optional; the identifying parameter (`query`, `value`,
  `source_id`, `target_id`, `query_vector`) has none, so it is required.
- **Per-parameter descriptions** come from a Google-style `Args:` block in
  the docstring, parsed by FastMCP into each property's `description`. A
  generator whose docstring has no `Args:` section (e.g. `schema_info` in
  `generic_tools.py`) produces parameters with no `description` field —
  confirmed above for `find_similar`, whose docstring in the probe omitted
  `Args:` and whose schema came back with no `description` on either
  property.
- **The tool's own description** is the docstring's first paragraph only
  (verified above: `"Search docstring.\n\n    Args: ..."` produced the tool
  description `"Search docstring."`) — the `Args:` block documents
  parameters, not the tool.
- `additionalProperties: false` — an agent that sends an extra key gets a
  schema-validation rejection before the tool body runs.
- **Custom tools** (`ToolConfig.custom_tools`, `server.py:
  _register_custom_tool`) get the same treatment: the registration code
  builds `__annotations__` and an `inspect.Signature` from the YAML
  `parameters` list (`type: str | int | float`, optional `default`) before
  handing the function to `mcp.tool()`, so a curated tool's schema is
  derived exactly the same way a generated one's is — there is no second
  code path.

**Not guaranteed by this clause:** the `fastmcp` dependency is pinned as
`>=2.0` with no ceiling, so the exact schema shape (property ordering,
whether `additionalProperties` stays `false`, how docstrings are parsed) is
whatever the resolved FastMCP version does at install time, not something
this repository fixes. An agent should not depend on schema details beyond
what a JSON Schema `type`/`required`/`description` reader would extract.

---

## 2. Error format

Every generator function wraps its body in `try: ... except Exception as
exc: return self._json({"error": str(exc)})` (see `base.py`'s `_json`
helper and every `_make_*` closure in `node_tools.py`, `edge_tools.py`,
`algorithm_tools.py`, `vector_tools.py`, and `generic_tools.py`'s
`cypher_query`). Custom tools do the same in `server.py:
_register_custom_tool`.

This means **a tool failure is not an MCP protocol-level error.** The tool
call succeeds at the transport layer; the payload is a JSON string whose
only content is `{"error": "<str(exception)>"}`. An agent must:

1. Parse the tool's string return value as JSON.
2. Check for an `"error"` key.

There is no error **code**, no error **taxonomy**, and no distinction
between "not found" (e.g. `get_{label}_by_{prop}` when nothing matches,
which returns `{"error": "{label} with {prop}={value!r} not found"}`),
"invalid input" (e.g. `find_{type}_connections` on an invalid identifier,
`{"error": "Invalid Cypher identifier: ...")}`), a rejected write
(`cypher_query` on a write statement: `{"error": "Write operations are not
allowed."}`), and an underlying query engine failure (whatever
`str(exc)` happens to say for that exception type). All four look
identical in shape: a one-key object. An agent that wants to react
differently to "not found" versus "rejected" versus "engine error" today
has to pattern-match the message text, which is not covered by this
contract and can change wording without notice.

`schema_info()` is the one tool with no `try/except` — it has no failure
mode to catch, since it only serializes an already-computed `GraphSchema`
object (`schema.to_dict()`); it cannot reach a state where the graph or the
query engine could reject it.

**Not guaranteed by this clause:** no request ID, no correlation ID, no
machine-readable error code. An agent cannot distinguish error kinds
without string-matching, and that matching is not specified here because
the messages are not contractual — they are whatever `str(exc)` or the
literal Python string the generator wrote happens to be this version.

---

## 3. Pagination

There is no cursor, no page token, and no `has_more` flag anywhere in
`samyama_mcp`. What bounds a result differs by tool family:

| Tool family | Bound | Evidence |
|---|---|---|
| `search_{label}` | `LIMIT min(int(limit), 100)`, default 25 | `node_tools.py: _make_search` |
| `get_{label}_by_{prop}` | none needed — matches on a unique-ish key, returns 0/1/N rows as-is | `node_tools.py: _make_get_by` |
| `count_{label}` | N/A — returns a single count | `node_tools.py: _make_count` |
| `find_{type}_connections` | `LIMIT min(int(limit), 100)`, default 25 | `edge_tools.py: _make_find_connections` |
| `traverse_{type}` | `LIMIT min(int(limit), 100)` and hop depth capped at `min(int(max_hops), 5)` | `edge_tools.py: _make_traverse` |
| `find_similar_{label}` | `k` capped at `min(int(k), 100)` | `vector_tools.py: _make_find_similar` |
| `pagerank` | `top_n` slices the ranked list in Python (`[: int(top_n)]`) — **no upper cap on `top_n` itself** | `algorithm_tools.py: _make_pagerank` |
| `communities` (WCC) | returns every component's size (`size_distribution`), truncated to the top 20 by size — the full component **count** and **membership** are not bounded | `algorithm_tools.py: _make_communities` |
| **`cypher_query`** | **none** — runs whatever `LIMIT` (if any) the agent's own Cypher contains | `generic_tools.py: cypher_query` |
| **Custom tools** (`ToolConfig.custom_tools`) | **none** — whatever the YAML `cypher_template` contains; the framework injects no `LIMIT` | `server.py: _register_custom_tool` |
| `schema_info` | returns the whole discovered schema; `CypherSchemaDiscovery` itself samples node properties from at most `PROPERTY_SAMPLE = 1000` nodes per label (`schema.py`), but places no cap on the number of labels, edge types, or indexes reported | `schema.py` |

**Stated plainly, because an undocumented limit is worse than a documented
absence: the two most powerful tools — `cypher_query` and any curated
custom tool — have no result-size bound at all.** Nothing below the MCP
layer imposes one either: the Python SDK's `query_readonly` binding
(`sdk/python/src/lib.rs`) takes no row-limit parameter, and the HTTP body
limit the engine enforces (`DefaultBodyLimit::max(64 * 1024 * 1024 *
1024)`, `src/http/server.rs`) is 64 GiB — not a pagination mechanism, a
crash-prevention ceiling. An agent that runs `cypher_query("MATCH (n)
RETURN n")` against a graph with millions of nodes gets all of them, in one
JSON string, in one tool response.

**What an agent should do about it:** for the generated tools, rely on the
stated caps (100 rows, 5 hops) — they are enforced server-side and cannot
be exceeded by passing a larger `limit`. For `cypher_query` and custom
tools, **the agent is responsible for writing its own `LIMIT` clause**;
nothing here will add one, truncate the response, or warn that it was
omitted.

---

## 4. Result envelopes

Every generated and custom tool has the Python signature `(...) -> str`.
The function body always returns `json.dumps(...)` (via `self._json` or
directly). FastMCP therefore has two layers to describe:

**a. The string itself.** Its shape depends on the tool:

| Tool | Success shape | Failure shape |
|---|---|---|
| `search_{label}`, `find_{type}_connections`, `traverse_{type}`, `cypher_query` | JSON array of row objects (`[{"col": val, ...}, ...]`), `[]` if no rows | `{"error": "<message>"}` |
| `get_{label}_by_{prop}` | a single row object if exactly one match, else a JSON array | `{"error": "<message>"}` |
| `count_{label}` | `{"count": N}` | `{"error": "<message>"}` |
| `pagerank` | JSON array of `{"node_id", "pagerank_score", "labels"}` | `{"error": "<message>"}` |
| `shortest_path` (BFS) | `{"path": [...], "distance"/"cost": ...}` (whatever `client.bfs()` returns) | `{"error": "No path found."}` or `{"error": "<message>"}` |
| `communities` (WCC) | `{"component_count", "largest_component", "size_distribution"}` | `{"error": "<message>"}` |
| `find_similar_{label}` | JSON array of `{"node_id", "similarity", ...node properties}` | `{"error": "<message>"}` |
| `schema_info` | the full `GraphSchema.to_dict()` shape (`total_nodes`, `total_edges`, `node_types[]`, `edge_types[]`, `indexes[]`, `vector_indexes[]`) | (no failure path — see §2) |

**There is no envelope shared across tools** — no common `{"data": ...,
"meta": ...}` wrapper, no `status` field, no schema version tag inside the
payload. A row's key set is whatever `RETURN` clause the generator built
(`id(n) AS _id, n.prop AS prop, ...`), so it varies per label/edge type,
and an agent has to read a tool's description (or `schema_info`) to know
what keys to expect from it.

**b. The MCP transport layer double-wraps it.** Because every tool
declares `-> str`, FastMCP auto-derives an **output schema** of
`{"result": {"type": "string"}}` and sets `x-fastmcp-wrap-result: true`
(verified against the installed `fastmcp` 4.0.5 — `to_mcp_tool().outputSchema`
for a `-> str` function returns exactly `{'properties': {'result':
{'type': 'string'}}, 'required': ['result'], 'type': 'object',
'x-fastmcp-wrap-result': True}`). Concretely: the tool's return value (a
JSON-encoded string) is delivered as MCP `TextContent`, **and** — for a
client that reads `structuredContent` instead — as `{"result": "<the same
JSON string, still encoded as a string>"}`. An agent reading structured
content must parse JSON **twice**: once to unwrap `result`, once to parse
what `result` contains. This is a FastMCP behavior triggered by every
generator's `-> str` return annotation, not something `samyama_mcp` opted
into deliberately — no code in this repository sets an explicit output
schema.

---

## 5. Idempotency

**Every tool registered by this server is read-only, and therefore safe to
retry.** This was verified by reading the call path of every generator and
of custom tools:

- `NodeToolGenerator`, `EdgeToolGenerator`, `AlgorithmToolGenerator`,
  `VectorToolGenerator` — every Cypher statement they build is executed
  through `client.query_readonly(...)`, never `client.query(...)`.
- `GenericToolGenerator.cypher_query` accepts arbitrary agent-supplied
  Cypher, but first checks `is_readonly_cypher(cypher)`
  (`escape.py`) and rejects anything containing `CREATE`, `DELETE`,
  `DETACH`, `SET`, `REMOVE`, `MERGE`, `DROP`, `FOREACH`, `LOAD`, or `CALL`
  as a bare token (string literals are stripped before the check, so a
  quoted word doesn't trigger it) with `{"error": "Write operations are not
  allowed."}` — and only calls `client.query_readonly(...)` if it passes.
- `server.py: _register_custom_tool` applies the **same**
  `is_readonly_cypher` check to every curated YAML tool before running it
  through `client.query_readonly(...)`. There is no separate, less-guarded
  path for custom tools.

So: **an agent can retry any tool call, any number of times, after a
timeout or a transient failure, without a side-effect risk that exists
today.** There is no tool in this server whose repeated execution could
create duplicate nodes, double-apply a write, or otherwise diverge from a
single call's effect — because no tool writes.

**What this is not:** it is not a general idempotency guarantee enforced by
the framework, an idempotency key, or a dedup mechanism. It is a
consequence of the current tool set having no write tools at all — stated
directly in `sdk/python/samyama_mcp/README.md`'s "Security — read-only by
construction" section ("The server never exposes a write path"). If a
future generator or a future `custom_tools` entry ever registers a
tool that calls `client.query(...)` instead of `client.query_readonly(...)`,
none of today's retry-safety follows it automatically — nothing in
`ToolGenerator`, `SamyamaMCPServer`, or FastMCP itself tracks call
identity, deduplicates repeated calls, or marks a tool as
idempotent/non-idempotent as metadata an agent could inspect. **An agent
should not assume "no result means safe to retry" once a write tool exists
— it should assume the opposite until that tool's documentation says
otherwise.** This contract's version number (§ top) exists so a client can
pin to "idempotency holds for every tool" and be warned by a version bump,
rather than by a write tool silently appearing in a later release.

---

## What is not guaranteed

Stated here rather than left implicit, per the instruction that an
undocumented absence should be said plainly rather than discovered by an
agent the hard way:

- **No cursor-based pagination anywhere** (§3). `cypher_query` and custom
  tools have no server-side row cap at all.
- **No structured error codes or taxonomy** (§2) — only a message string,
  not contractual wording.
- **No request/correlation ID** on either success or error responses.
- **No rate limiting** visible in this server's code — nothing here
  throttles repeated tool calls.
- **No auth boundary inside the MCP layer.** `server.run()` serves over
  **stdio only** (`FastMCP.run()` with no transport argument, and
  `samyama-mcp-serve`'s CLI has no flag that changes it — see `cli.py`);
  the process boundary is the only isolation. Whoever can start the
  `samyama-mcp-serve` process and read its stdio can call every tool
  the server registers, subject only to §5's read-only constraint.
- **No schema version negotiation.** The tool set is derived once, at
  `SamyamaMCPServer.__init__`, from whatever `CypherSchemaDiscovery`
  finds at that moment. If the underlying graph's schema changes while
  the server is running (a label added, a property renamed), the tool
  set does not update and nothing signals that it is stale.
- **No output schema beyond `{"result": string}`** (§4b) — an agent
  cannot get a typed, per-tool structured result without parsing the
  inner JSON string itself.
- **No upper bound on `pagerank`'s `top_n` or on `communities`'
  component count/membership** (§3) — only their *display* is
  truncated in places, not the underlying computation.

---

## Changelog

- **1.0.0** (2026-09-23) — first published version. Documents tool
  schemas (FastMCP signature-derived), error format (flat `{"error":
  ...}` string, not a protocol-level error), pagination (per-tool caps
  on generated tools; none on `cypher_query`/custom tools), result
  envelopes (per-tool JSON shapes plus FastMCP's `{"result": string}`
  double-wrap for `-> str` tools), and idempotency (every tool is
  read-only today, verified call-path by call-path; not a framework
  guarantee that survives a future write tool).
