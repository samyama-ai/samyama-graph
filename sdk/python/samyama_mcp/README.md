# `samyama_mcp` — auto-generate MCP tools from a Samyama graph

`samyama_mcp` turns any Samyama graph into a [Model Context Protocol](https://modelcontextprotocol.io)
(MCP) server. It **introspects the graph's schema** and **generates a tool per node label, edge type,
algorithm, and vector index** — so an MCP client (e.g. Claude) can query the graph in natural language
without anyone hand-writing tools. You can also layer **curated Cypher-backed tools** on top via YAML.

```
graph schema ──discover──▶ GraphSchema ──generators──▶ MCP tools ──FastMCP (stdio)──▶ MCP client
```

---

## Quickstart

### CLI (`samyama-mcp-serve`)

```bash
# Serve a tenant on a running engine, auto-generating tools from its schema:
samyama-mcp-serve --graph legal --url http://localhost:8080

# Add curated tools from a YAML config, and give the server a name:
samyama-mcp-serve --graph legal --url http://localhost:8080 \
                  --config mcp_server/config.yaml --name "Samyama Legal KG"

# Inspect what would be generated, without serving:
samyama-mcp-serve --graph legal --url http://localhost:8080 --list-tools

# Fully self-contained: build a tiny in-memory social graph and serve it:
samyama-mcp-serve --demo social
```

### Python

```python
from samyama import SamyamaClient
from samyama_mcp import SamyamaMCPServer

client = SamyamaClient.connect("http://localhost:8080")   # or SamyamaClient.embedded()
SamyamaMCPServer(client, graph="legal", server_name="Samyama Legal KG").run()
```

---

## How it works

`SamyamaMCPServer` orchestrates three steps:

1. **Schema discovery** — `CypherSchemaDiscovery(client, graph).discover()` introspects the live graph
   via Cypher and returns a `GraphSchema`:
   - `node_types` — each label with its count and `PropertyInfo` (name, type, `indexed`)
   - `edge_types` — each relationship type with source/target labels and count
   - `indexes`, `vector_indexes`, `total_nodes`, `total_edges`
2. **Tool generation** — the schema is filtered by `exclude_labels`, then each enabled generator
   registers tools on a `FastMCP` instance (see the table below).
3. **Serve** — `.run()` starts the server over the **stdio** transport.

### Tool families (generators)

| Generator | Enabled by | Tools it generates | Example |
|---|---|---|---|
| `GenericToolGenerator` | always on | `cypher_query`, `schema_info` | `cypher_query("MATCH (n) RETURN count(n)")` |
| `NodeToolGenerator` | `include_node_tools` | `search_{label}`, `get_{label}_by_{prop}`, `count_{label}` | `count_case`, `search_judge`, `get_case_by_id` |
| `EdgeToolGenerator` | `include_edge_tools` | `find_{rel}_connections`, `traverse_{rel}` | `find_decided_connections`, `traverse_cites` |
| `AlgorithmToolGenerator` | `include_algorithm_tools` | `pagerank`, `communities`, `shortest_path`, … | `pagerank(...)` |
| `VectorToolGenerator` | `include_vector_tools` | `find_similar_{label}` (embedded only), `retrieve_evidence_{label}` (per vector index) | `find_similar_case(...)`, `retrieve_evidence_case(...)` |

So a legal graph with `Case/Judge/Party/Act/Topic` nodes and `DECIDED/CITES/PARTY_IN/ABOUT` edges
yields `count_case`, `search_judge`, `find_decided_connections`, `traverse_cites`, `cypher_query`, … —
all without writing a line of tool code.

---

## Configuration — `ToolConfig`

`ToolConfig` controls which generators run and adds curated tools. Load it from YAML with
`ToolConfig.from_yaml(path)` (the CLI's `--config`), or use `ToolConfig.default()`.

```yaml
# mcp_server/config.yaml
include_node_tools: true
include_edge_tools: true
include_vector_tools: true
include_algorithm_tools: true

# Skip noisy or internal labels:
exclude_labels: [InternalAudit]

# Curated, domain-specific tools backed by a Cypher template:
custom_tools:
  - name: top_judges
    description: "Judges who authored the most judgments. Returns judge name and case count."
    cypher_template: |
      MATCH (j:Judge)-[:DECIDED]->(c:Case)
      RETURN j.name AS judge, count(DISTINCT c) AS cases
      ORDER BY cases DESC LIMIT {limit}
    parameters:
      - {name: limit, type: int, default: 10}
```

### Custom (curated) tools

Each `custom_tools` entry becomes a first-class MCP tool:

- `cypher_template` is filled with the tool's `parameters` via `str.format` (`{limit}` above).
- String parameters are **escaped** before substitution; the final query is checked by the
  **read-only guard** (below) before it runs.
- `parameters` support `type: str | int | float` and an optional `default`; the generated tool
  exposes them with correct types so the MCP client sees a proper signature.

Curated tools are **layered on top of** the auto-generated ones — you get both.

---

## Security — read-only by construction

Every generated and custom tool runs through `escape.py`:

- **`is_readonly_cypher`** rejects any query containing write operations (`CREATE`, `DELETE`, `SET`,
  `MERGE`, `DROP`, …) — write attempts return `{"error": "Write operations are not allowed."}`.
- **`escape_string`** / **`validate_identifier`** sanitize user-supplied values and identifiers used
  in generated Cypher.

The server never exposes a write path.

---

## Register a new KG in 3 steps

1. **Import** your data into a tenant on a running engine (e.g. graph `legal`).
2. *(Optional)* write a `config.yaml` with `custom_tools` for the domain questions you care about.
3. **Serve** it:
   ```bash
   samyama-mcp-serve --graph legal --url http://localhost:8080 --config config.yaml --name "Legal KG"
   ```
   Register that command with your MCP client (e.g. `claude mcp add legal -- samyama-mcp-serve …`) and
   ask questions in natural language.

---

## Module reference

| Module | Responsibility |
|---|---|
| `server.py` | `SamyamaMCPServer` — discovery + registration + `run()` |
| `schema.py` | `CypherSchemaDiscovery`, `GraphSchema`, `NodeType`, `EdgeType`, `VectorIndex` |
| `config.py` | `ToolConfig`, `CustomTool`, `from_yaml()` |
| `generators/` | one module per tool family (node / edge / algorithm / vector / generic) |
| `escape.py` | read-only guard + value/identifier escaping |
| `cli.py` | the `samyama-mcp-serve` entry point |
