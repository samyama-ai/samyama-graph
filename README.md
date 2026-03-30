# Samyama Graph Database

![Version](https://img.shields.io/badge/version-0.6.1-blue)
![Rust](https://img.shields.io/badge/rust-1.85-orange)
![Tests](https://img.shields.io/badge/tests-1814_passing-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-87.8%25-brightgreen)
![Bugs](https://img.shields.io/badge/bugs-0-brightgreen)
![Vulnerabilities](https://img.shields.io/badge/vulnerabilities-0-brightgreen)
![Quality Gate](https://img.shields.io/badge/quality_gate-passed-brightgreen)
![Maintainability](https://img.shields.io/badge/maintainability-A-brightgreen)
![LOC](https://img.shields.io/badge/LOC-44K-informational)
![License](https://img.shields.io/badge/license-Apache_2.0-blue)

**Samyama** (Sanskrit: the union of focused query, sustained analysis, and unified insight) is a high-performance graph-vector database written in **Rust**. It combines a **property graph engine** (~90% OpenCypher), **vector search** (HNSW), **graph algorithms**, and **natural language querying** in a single binary. Zero GC pauses, 1,814 tests, Apache-2.0.

### Scale Benchmark

| Dataset | Nodes | Edges | RAM | Instance Cost |
|---------|-------|-------|-----|--------------|
| **PubMed/MEDLINE** | **66.2M** | **1.04B** | 130 GB | **$2.50 total** |
| Biomedical Trifecta | 7.9M | 28.0M | 2 GB | Mac Mini M4 |
| Cricket (Cricsheet) | 37K | 1.39M | < 1 GB | Mac Mini M4 |

### See it in action

> Graph Simulation — Cricket KG (36K nodes, 1.4M edges) with live activity particles

[![Samyama Graph Simulation](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/simulation-preview.gif)](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/samyama-cricket-demo.mp4)

*Click for full demo (1:56) — Dashboard, Cypher Queries, and Graph Simulation*

### LDBC Benchmark Results (v0.6.0, Mac Mini M4)

| Benchmark | Queries | Pass Rate | Dataset |
|-----------|---------|-----------|---------|
| **SNB Interactive** | 21 reads | **21/21 (100%)** | SF1: 3.18M nodes, 17.26M edges |
| **SNB Business Intelligence** | 20 analytical | **16/16 run (100%)** (BI-17+ timeout) | SF1 (same dataset) |
| **Graphalytics** | 6 algorithms x 2 datasets | **12/12 (100%)** | LDBC XS reference graphs |
| **FinBench** | 12 CR + 6 SR + 3 RW + 19 W | **40/40 (100%)** | Synthetic: 7.7K nodes, 42.2K edges |

See [docs/ldbc/](docs/ldbc/) for detailed per-query results, latency tables, and analysis.

### What's New in v0.6.1

- **HTTP Tenant Management API**: Full CRUD for tenants via REST endpoints (`POST /api/tenants`, `GET /api/tenants`, `GET /api/tenants/{id}`, `DELETE /api/tenants/{id}`).
- **samyama-mcp-serve**: Auto-generate [MCP (Model Context Protocol)](https://modelcontextprotocol.io/) servers from any graph schema. Discovers labels, edge types, and properties, then generates typed tools for AI agents. Install via `pip install samyama[mcp]` and run `samyama-mcp-serve --demo` for instant agent tool access.
- **Snapshot format (.sgsnap)**: Portable gzip JSON-lines snapshot export/import for graph tenants, enabling backup and migration across instances.
- **Cricket dataset loader**: Load 21K Cricsheet T20/ODI/Test matches (36K nodes, 1.4M edges) via `cargo run --release --example cricket_loader`.
- **AACT clinical trials loader**: Full AACT dataset loader for clinical trial analysis (575K studies, 7.7M nodes, 27M edges).
- **Index scan fix**: Inline MATCH properties `{prop: val}` now trigger IndexScan when a matching index exists, avoiding full label scans.

## Key Features

- **OpenCypher Query Engine**: ~90% OpenCypher coverage — MATCH, CREATE, DELETE, SET, MERGE, OPTIONAL MATCH, UNION, WITH, UNWIND, aggregations, and 30+ built-in functions.
- **RESP Protocol**: Drop-in compatibility with any Redis client (`redis-cli`, Jedis, ioredis).
- **Vector Search**: Built-in HNSW indexing for millisecond semantic search and Graph RAG.
- **NLQ (Natural Language Queries)**: Ask questions in plain English — the LLM translates to Cypher automatically.
- **Graph Algorithms**: Native PageRank, BFS, Dijkstra, WCC, SCC, CDLP, LCC, MaxFlow, MST, SSSP, Triangle Counting.
- **Optimization Solvers**: 15+ metaheuristic algorithms (Jaya, Rao, GWO, PSO, Firefly, Cuckoo, ABC, NSGA-II) for in-database optimization.
- **Multi-Tenancy**: Tenant-level isolation with per-tenant quotas via RocksDB column families.
- **High Availability**: Raft consensus (via `openraft`) for cluster replication and automatic failover.
- **Persistence**: RocksDB storage with Write-Ahead Log and checkpointing.
- **Cost-Based Query Planner**: Graph-native plan enumeration with triple-level statistics (GraphCatalog), predicate pushdown, direction reversal, ExpandInto, and plan cache. EXPLAIN/PROFILE for plan visualization.
- **Late Materialization**: Scan operators produce lightweight `NodeRef` tokens instead of full clones — 4x traversal speedup on multi-hop queries.
- **Two-Phase Bulk Loading**: `create_node_stub()` + `create_edge_stub()` reduce per-edge memory from 709 to 52 bytes (13.6x), enabling billion-edge graphs on commodity hardware.
- **HTTP Tenant API**: REST endpoints for tenant CRUD (create, list, get, delete) alongside the RESP protocol.
- **MCP Server Generation**: Auto-generate MCP servers from graph schema for AI agent integration (`samyama-mcp-serve`).
- **Snapshot Persistence**: Portable `.sgsnap` format with automatic persistence — imported snapshots survive server restart.

## Getting Started

### Build

```bash
git clone https://github.com/samyama-ai/samyama-graph
cd samyama-graph
cargo build --release
```

### Run the Server

```bash
./target/release/samyama
```

This starts the RESP server on port 6379 and the HTTP API on port 8080.

### Connect

```bash
redis-cli -p 6379

# Create nodes
GRAPH.QUERY mygraph "CREATE (n:Person {name: 'Alice', age: 30})"

# Query
GRAPH.QUERY mygraph "MATCH (n:Person) RETURN n"

# Explain a query plan
GRAPH.QUERY mygraph "EXPLAIN MATCH (n:Person) WHERE n.age > 25 RETURN n"
```

## Examples

Samyama ships with domain-specific demos that showcase the full feature set.

### Core Infrastructure

| Example | Command | Description |
|---------|---------|-------------|
| Persistence | `cargo run --example persistence_demo` | RocksDB persistence, WAL, multi-tenancy, recovery |
| Cluster | `cargo run --example cluster_demo` | 3-node Raft cluster with leader election and failover |
| Full Benchmark | `cargo run --example full_benchmark` | Scale test up to 1M+ nodes |

### Industry Demos (with NLQ + Agentic Enrichment)

Each demo builds a domain-specific knowledge graph, runs Cypher queries, executes graph algorithms, and demonstrates natural language querying via the NLQ pipeline.

| Example | Command | What it demonstrates |
|---------|---------|---------------------|
| Banking / Fraud Detection | `cargo run --example banking_demo` | Customer segmentation, fraud patterns, money laundering detection, OFAC screening |
| Clinical Trials | `cargo run --example clinical_trials_demo` | Patient-trial matching (vector search), drug interactions (PageRank), site optimization (NSGA-II) |
| Supply Chain | `cargo run --example supply_chain_demo` | Disruption analysis, cold-chain monitoring, port optimization (Jaya), alternative suppliers (vector search) |
| Smart Manufacturing | `cargo run --example smart_manufacturing_demo` | Digital twin, failure cascade analysis, production scheduling (Cuckoo Search), energy optimization |
| Social Network | `cargo run --example social_network_demo` | Follower graphs, mutual connections, influence analysis (PageRank), community detection (WCC) |
| Knowledge Graph | `cargo run --example knowledge_graph_demo` | Document lineage, expert finding (vector search), topic clustering, knowledge hub identification |
| Enterprise SOC | `cargo run --example enterprise_soc_demo` | Threat intel, MITRE ATT&CK mapping, attack path analysis (Dijkstra), lateral movement simulation |
| Agentic Enrichment | `cargo run --example agentic_enrichment_demo` | Generation-Augmented Knowledge (GAK) — LLM generates Cypher to enrich the graph autonomously |

### Data Loaders

| Example | Command | Description |
|---------|---------|-------------|
| LDBC SNB | `cargo run --example ldbc_loader` | Load LDBC SNB SF1 dataset (3.18M nodes, 17.26M edges) |
| FinBench | `cargo run --example finbench_loader` | Load/generate LDBC FinBench dataset |
| Cricket | `cargo run --release --example cricket_loader` | Load 21K Cricsheet matches (36K nodes, 1.4M edges) |
| AACT Clinical Trials | `cargo run --release --example aact_loader` | Full AACT dataset (575K studies, 7.7M nodes, 27M edges) |

### AI Agent Integration

| Example | Command | Description |
|---------|---------|-------------|
| MCP Server | `samyama-mcp-serve --demo` | Auto-generate MCP server from graph schema for AI agents (Python, `pip install samyama[mcp]`) |

## Cypher Support

**~90% OpenCypher coverage.** See [docs/CYPHER_COMPATIBILITY.md](docs/CYPHER_COMPATIBILITY.md) for the full matrix.

### Supported Clauses

`MATCH`, `OPTIONAL MATCH`, `WHERE`, `RETURN`, `RETURN DISTINCT`, `ORDER BY`, `SKIP`, `LIMIT`, `CREATE`, `DELETE`, `DETACH DELETE`, `SET`, `REMOVE`, `MERGE` (with `ON CREATE SET` / `ON MATCH SET`), `WITH`, `UNWIND`, `UNION` / `UNION ALL`, `EXPLAIN`, `EXISTS` subqueries

### Supported Functions

| Category | Functions |
|----------|-----------|
| String | `toUpper`, `toLower`, `trim`, `replace`, `substring`, `left`, `right`, `reverse`, `toString` |
| Numeric | `abs`, `ceil`, `floor`, `round`, `sqrt`, `sign`, `toInteger`, `toFloat` |
| Aggregation | `count`, `sum`, `avg`, `min`, `max`, `collect` |
| List/Collection | `size`, `length`, `head`, `last`, `tail`, `keys`, `range` |
| Graph | `id`, `labels`, `type`, `exists`, `coalesce`, `startsWith`, `endsWith`, `contains` |

### Operators

Arithmetic (`+`, `-`, `*`, `/`, `%`), comparison (`=`, `<>`, `<`, `>`, `<=`, `>=`), logical (`AND`, `OR`, `NOT`, `XOR`), string (`STARTS WITH`, `ENDS WITH`, `CONTAINS`, `=~`), null (`IS NULL`, `IS NOT NULL`), list (`IN`).

Cross-type coercion: Integer/Float promotion and String/Boolean coercion for LLM-generated queries. Null propagation follows Neo4j three-valued logic.

## Architecture

```
src/
├── graph/           # Property graph model (Node, Edge, PropertyValue, GraphStore)
├── query/           # OpenCypher engine
│   ├── cypher.pest  #   PEG grammar (Pest)
│   ├── parser.rs    #   Parser → AST
│   └── executor/    #   Volcano iterator model (scan, filter, expand, project, aggregate, sort, limit)
├── protocol/        # RESP3 server (Tokio TCP)
├── persistence/     # RocksDB + WAL + multi-tenancy
├── raft/            # Raft consensus (openraft)
├── nlq/             # Natural Language Query pipeline (OpenAI, Gemini, Ollama, Claude Code)
├── vector/          # HNSW vector index
├── snapshot/        # Portable .sgsnap export/import
└── sharding/        # Tenant-level sharding
```

Key design decisions are documented as [Architecture Decision Records](docs/ADR/).

## Companion Crates

- **[samyama-graph-algorithms](crates/samyama-graph-algorithms/)**: PageRank, BFS, Dijkstra, WCC, SCC, MaxFlow, MST, Triangle Counting
- **[samyama-optimization](crates/samyama-optimization/)**: 15+ metaheuristic solvers for single and multi-objective optimization

## Benchmarks

Run with `cargo bench`. See [docs/performance/](docs/performance/) for detailed results.

| Operation | Throughput | Notes |
|-----------|-----------|-------|
| Node insertion | ~3.4M nodes/sec | At 1K batch, single-threaded |
| Label scan | <1 us | 100-node label groups |
| 1-hop traversal | ~22 us | MATCH-WHERE-RETURN pattern |
| Cypher parse | <8 us | Multi-hop patterns with aggregation |

## Documentation

- [LDBC Benchmark Results](docs/ldbc/) — SNB Interactive, SNB BI, Graphalytics, FinBench
- [Architecture](docs/ARCHITECTURE.md)
- [Cypher Compatibility](docs/CYPHER_COMPATIBILITY.md)
- [ACID Guarantees](docs/ACID_GUARANTEES.md)
- [Benchmarks](docs/performance/BENCHMARKS.md)
- [Architecture Decision Records](docs/ADR/)
- [Technology Comparisons](docs/TECHNOLOGY_COMPARISONS.md)

## Testing

1814 unit tests, integration tests via Python scripts, and 8 domain-specific example demos.

```bash
cargo test                     # Run all tests
cargo bench                    # Run benchmarks
cargo clippy -- -D warnings    # Lint
cargo fmt -- --check           # Format check
```

## License

Apache License 2.0
