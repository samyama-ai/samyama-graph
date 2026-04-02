<p align="center">
  <h1 align="center">Samyama</h1>
  <p align="center">
    <strong>The graph database that queried 1 billion edges for $2.50</strong>
  </p>
  <p align="center">
    <a href="https://github.com/samyama-ai/samyama-graph/releases/tag/v0.7.0"><img src="https://img.shields.io/badge/version-0.7.0-blue" alt="Version"></a>
    <a href="https://github.com/samyama-ai/samyama-graph/actions"><img src="https://img.shields.io/badge/tests-1877_passing-brightgreen" alt="Tests"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>
    <a href="https://samyama-ai.github.io/samyama-graph-book/"><img src="https://img.shields.io/badge/book-read_the_docs-orange" alt="Book"></a>
  </p>
</p>

---

We loaded the entire PubMed corpus — every article published since 1966 — plus ClinicalTrials.gov, Reactome pathways, and DrugBank into **one graph**. Then we asked:

> *"What drugs are most tested in cancer clinical trials?"*

```cypher
MATCH (m:MeSHTerm)<-[:ANNOTATED_WITH]-(a:Article)
      -[:REFERENCED_IN]->(t:ClinicalTrial)-[:TESTS]->(i:Intervention)
WHERE m.name = 'Neoplasms'
RETURN i.name, count(DISTINCT t) AS trials
ORDER BY trials DESC LIMIT 5
```

| Drug | Trials | Time |
|------|--------|------|
| Placebo | 521 | |
| **Pembrolizumab** | **137** | |
| Carboplatin | 106 | |
| Paclitaxel | 106 | |
| Cyclophosphamide | 98 | **5.2 seconds** |

One query. Four databases. 74 million nodes. 1 billion edges. A single machine.

[See all 100 benchmark queries →](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html)

---

## What is Samyama?

A graph-vector database written in Rust. OpenCypher queries, Redis protocol, vector search, graph algorithms — one binary, no JVM, no GC pauses.

```bash
# Install and run (30 seconds)
git clone https://github.com/samyama-ai/samyama-graph && cd samyama-graph
cargo build --release
./target/release/samyama    # RESP on :6379, HTTP on :8080
```

```bash
# Connect with any Redis client
redis-cli -p 6379
GRAPH.QUERY mydb "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
GRAPH.QUERY mydb "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name"
```

---

## Why Samyama?

**If your data has relationships, you need a graph database.** If your graph database can't handle a billion edges on a single machine, you need Samyama.

| What | How |
|------|-----|
| **74M nodes, 1B edges** | Loaded PubMed + ClinicalTrials.gov + Reactome + DrugBank on one r6a.8xlarge ($2.50 spot) |
| **96/100 queries pass** | Point lookups, multi-hop traversals, cross-KG aggregations — [all verified](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) |
| **Parallel everything** | Rayon: PageRank 3.1x, LCC 9.1x, Triangle Count 6x. Parallel scan, filter, compaction |
| **975 QPS concurrent** | 16-client read workload, p99 < 25ms, zero errors across 67K queries |
| **LDBC certified** | SNB Interactive 21/21, FinBench 40/40, Graphalytics 12/12 |

---

## The 30-Second Tour

**Cypher queries** — ~90% OpenCypher. MATCH, CREATE, MERGE, aggregations, path finding, 30+ functions.

```cypher
MATCH (a:Person)-[:KNOWS*1..3]->(b:Person)
WHERE a.name = 'Alice'
RETURN b.name, length(shortestPath(a, b))
```

**Graph algorithms** — PageRank, WCC, SCC, BFS, Dijkstra, LCC, CDLP, Triangle Count. All rayon-parallelized.

```cypher
CALL pagerank('social') YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC LIMIT 10
```

**Vector search** — HNSW indexing for semantic search and Graph RAG.

```cypher
CREATE VECTOR INDEX ON :Paper(embedding) OPTIONS {dimensions: 384, similarity: 'cosine'}
CALL vector.search('Paper', 'embedding', [0.1, 0.2, ...], 10) YIELD node, score
```

**Natural language** — Ask questions in English. The LLM translates to Cypher.

```
NLQ "Who are Alice's friends of friends that work at Google?"
→ MATCH (a:Person {name:'Alice'})-[:KNOWS]->()-[:KNOWS]->(fof)-[:WORKS_AT]->(c:Company {name:'Google'}) RETURN fof.name
```

**AI agents** — Auto-generated MCP servers from your graph schema.

```bash
pip install samyama[mcp]
samyama-mcp-serve --demo cricket    # Instant AI agent tools for any graph
```

---

## Benchmarks

### Scale: 74M Nodes, 1 Billion Edges

| KG | Source | Nodes | Edges |
|----|--------|-------|-------|
| PubMed/MEDLINE | NLM | 66.2M | 1.04B |
| Clinical Trials | ClinicalTrials.gov | 7.8M | 27M |
| Pathways | Reactome | 119K | 835K |
| Drug Interactions | DrugBank + ChEMBL + SIDER | 245K | 388K |

Loaded in 31 minutes from snapshots. **96 of 100 queries return real data** across all four KGs. [Full results →](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html)

### Cross-KG Query Highlights

| Query | Time | Result |
|-------|------|--------|
| Cancer → Trial interventions | 5.2s | Pembrolizumab #1 (137 trials) |
| Diabetes → Trial interventions | 2.4s | Metformin #1 (70 trials) |
| Metformin → Trial adverse events | 2.1s | Diarrhoea (185 trials) — known side effect confirmed |
| Cancer trial sites by country | 3.8s | US 4,062 · China 1,170 · France 827 |
| NCI-funded → Trial drugs | 19.4s | Cyclophosphamide (517) · Radiation (362) |
| Aspirin articles → Trials | 1.5s | NCT00000491 "Aspirin MI study" |

### LDBC Compliance

| Benchmark | Pass Rate | Dataset |
|-----------|-----------|---------|
| SNB Interactive | **21/21 (100%)** | SF1: 3.18M nodes, 17.26M edges |
| SNB BI | **16/16 (100%)** | SF1 |
| Graphalytics | **12/12 (100%)** | XS reference graphs |
| FinBench | **40/40 (100%)** | 7.7K nodes, 42.2K edges |

### Concurrent Performance

| Workload | 1 client | 16 clients | Scaling |
|----------|----------|------------|---------|
| Pure read | 145 QPS | 975 QPS | 6.7x |
| Mixed 80/20 | 181 QPS | 722 QPS | 4.0x |
| Write-heavy | 279 QPS | 482 QPS | 1.7x |

---

## Demo

> Cricket KG — 36K nodes, 1.4M edges, live graph simulation

[![Samyama Graph Simulation](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/simulation-preview.gif)](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/samyama-cricket-demo.mp4)

*Click for full demo (1:56)*

---

## Examples

### Domain Knowledge Graphs

| Domain | Command | Nodes | Edges |
|--------|---------|-------|-------|
| Banking & Fraud | `cargo run --example banking_demo` | — | Fraud patterns, money laundering, OFAC |
| Clinical Trials | `cargo run --example clinical_trials_demo` | — | Patient-trial matching, drug interactions |
| Supply Chain | `cargo run --example supply_chain_demo` | — | Disruption analysis, port optimization |
| Manufacturing | `cargo run --example smart_manufacturing_demo` | — | Digital twin, failure cascades |
| Social Network | `cargo run --example social_network_demo` | — | Influence, communities, recommendations |
| Enterprise SOC | `cargo run --example enterprise_soc_demo` | — | MITRE ATT&CK, attack paths, threat intel |

### Data Loaders

| Dataset | Command | Scale |
|---------|---------|-------|
| LDBC SNB SF1 | `cargo run --example ldbc_loader` | 3.2M nodes, 17.3M edges |
| Clinical Trials | `cargo run --release --example aact_loader` | 7.8M nodes, 27M edges |
| Drug Interactions | `cargo run --release --example druginteractions_loader` | 245K nodes, 388K edges |
| Cricket | `cargo run --release --example cricket_loader` | 36K nodes, 1.4M edges |
| FinBench | `cargo run --example finbench_loader` | 7.7K nodes, 42K edges |

---

## Architecture

```
samyama
├── graph/         Property graph model (Node, Edge, GraphStore, CSR adjacency)
├── query/         OpenCypher engine
│   ├── cypher.pest    PEG grammar
│   ├── executor/      Volcano iterator + WCO LeapFrog TrieJoin
│   └── planner.rs     Cost-based graph-native query planner
├── protocol/      RESP3 server (Redis-compatible, Tokio async)
├── persistence/   RocksDB + WAL + multi-tenancy
├── vector/        HNSW vector index
├── snapshot/      Portable .sgsnap v2 (CSR + ColumnStore)
├── raft/          Distributed consensus (openraft)
└── nlq/           Natural language → Cypher (OpenAI, Gemini, Ollama, Claude)
```

**Companion crates:**
- [samyama-graph-algorithms](crates/samyama-graph-algorithms/) — PageRank, BFS, Dijkstra, WCC, SCC, LCC, CDLP, Triangle Count (all rayon-parallelized)
- [samyama-optimization](crates/samyama-optimization/) — 15+ metaheuristic solvers (Jaya, Rao, GWO, NSGA-II, TLBO)
- [samyama-sdk](crates/samyama-sdk/) — Rust SDK with embedded and remote clients

---

## Documentation

| Resource | Link |
|----------|------|
| **The Book** | [samyama-ai.github.io/samyama-graph-book](https://samyama-ai.github.io/samyama-graph-book/) |
| Biomedical Benchmark | [100 queries, 96 pass](https://samyama-ai.github.io/samyama-graph-book/biomedical_benchmark.html) |
| Cypher Compatibility | [docs/CYPHER_COMPATIBILITY.md](docs/CYPHER_COMPATIBILITY.md) |
| LDBC Results | [docs/ldbc/](docs/ldbc/) |
| Architecture Decisions | [docs/ADR/](docs/ADR/) |
| API Spec | [api/openapi.yaml](api/openapi.yaml) |

---

## Enterprise Edition

Everything above is open source (Apache 2.0). [Samyama Enterprise](https://samyama.dev) adds:

- GPU acceleration (wgpu + CUDA)
- OpenTelemetry OTLP metrics
- Prometheus + Grafana monitoring
- Backup & disaster recovery
- ADMIN commands + audit trail
- Ed25519 signed license tokens

[Contact us →](https://samyama.dev/contact)

---

## License

Apache License 2.0 — use it in production, contribute back if you'd like.

**Samyama** (Sanskrit: संयम) — the union of focused query, sustained analysis, and unified insight.
