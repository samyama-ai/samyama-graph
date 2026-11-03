<p align="center">
  <h1 align="center">Samyama Graph</h1>
  <p align="center">A Rust-native graph-vector database for GraphRAG, knowledge graphs, and billion-edge analytics.</p>
  <p align="center">
    <strong>99.9% of the openCypher TCK's evaluated scenarios pass · LDBC SNB Interactive 21/21 and BI 20/20, no timeouts · 1B edges on one machine</strong>
  </p>
  <p align="center">
    <a href="https://github.com/samyama-ai/samyama-graph/releases"><img src="https://img.shields.io/badge/version-1.14.0-blue" alt="Version"></a>
    <a href="https://github.com/samyama-ai/samyama-graph/actions/workflows/ci.yml"><img src="https://github.com/samyama-ai/samyama-graph/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>
    <a href="https://graph.samyama.cloud/book/"><img src="https://img.shields.io/badge/book-read_the_docs-orange" alt="Book"></a>
    <a href="https://chat.whatsapp.com/Jjjkb3uWRDi1YMdfffaD9d"><img src="https://img.shields.io/badge/community-WhatsApp-25D366?logo=whatsapp&logoColor=white" alt="WhatsApp Community"></a>
  </p>
</p>

---

## What it is

Samyama Graph stores, queries, searches and analyses connected data in one
system: graph traversal, openCypher-style querying, HNSW vector search, graph
algorithms and Redis-compatible access, with no separate vector store to keep in
sync.

People use it for GraphRAG, knowledge graphs, AI-agent memory, biomedical and
clinical graphs, and fraud, investigation and dependency analysis. The two demos
below are the short version of why.

## Quickstart

### Docker

The image is public on GitHub Container Registry and needs no registry account.
`latest` is the newest release; pin a version such as `:1.14.0` for a
reproducible setup.

**1 — Write `docker-compose.yml`** in an empty folder:

```yaml
services:
  samyama-graph:
    image: ghcr.io/samyama-ai/samyama-graph:latest
    container_name: samyama-graph
    restart: unless-stopped
    ports:
      - "6379:6379"   # RESP (Redis protocol)
      - "8080:8080"   # HTTP API
    environment:
      # Lets the hosted visualizer (step 4) call this server from your browser.
      SAMYAMA_CORS_ORIGINS: https://graph.samyama.cloud
    volumes:
      - samyama-data:/app/samyama_data
volumes:
  samyama-data:
```

**2 — Start it**

```bash
docker compose up -d
docker logs -f samyama-graph     # Ctrl-C to stop following the log
```

**3 — Run your first query**

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'content-type: application/json' \
  -d '{"query":"CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"}) RETURN a.name, b.name","graph":"default"}'
```

```json
{"columns":["a.name","b.name"],"edges":[],"nodes":[],"records":[["Alice","Bob"]]}
```

Runtime values go in `params`, never in the query text — they are bound, so a
value is never re-read as Cypher:

```bash
curl -X POST http://localhost:8080/api/query \
  -H 'content-type: application/json' \
  -d '{"query":"MATCH (p:Person) WHERE p.name = $name RETURN p.name","params":{"name":"Alice"}}'
```

**4 — Explore it visually (optional).** Open <https://graph.samyama.cloud/>,
click **Home**, enter `http://localhost:8080` and **Connect**. Without the
`SAMYAMA_CORS_ORIGINS` line above the browser refuses the call — deliberately,
because `/api/query` runs arbitrary Cypher including `DELETE`
([#1328](https://github.com/samyama-ai/samyama-graph/issues/1328)).

**5 — Stop or reset.** `docker compose down` stops it; `docker compose down -v`
also deletes the graph data in the volume.

The server needs no credential by default. For a bearer token, TLS or
auto-embedding, see [docs/DATA-HANDLING.md](docs/DATA-HANDLING.md).

### Build from source

`zstd-sys` generates its bindings with `bindgen`, which needs libclang; without
it the build fails part-way with a misleading `'stddef.h' file not found`.

```bash
# Debian / Ubuntu
sudo apt-get install -y build-essential cmake pkg-config libssl-dev clang libclang-dev
# Fedora / RHEL
sudo dnf install -y gcc gcc-c++ cmake pkgconf-pkg-config openssl-devel clang clang-devel
# macOS — the Xcode Command Line Tools already provide clang
xcode-select --install
```

Then, with a stable Rust toolchain from [rustup](https://rustup.rs/):

```bash
git clone https://github.com/samyama-ai/samyama-graph && cd samyama-graph
cargo build --release
./target/release/samyama    # RESP on :6379, HTTP on :8080

# Connect with any Redis client
redis-cli -p 6379
GRAPH.QUERY mydb "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
GRAPH.QUERY mydb "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name"
```

### Clients

| Language | Install | Connect |
|----------|---------|---------|
| Python | `pip install samyama` | `SamyamaClient.connect("http://localhost:8080")`, or `SamyamaClient.embedded()` to run the engine in-process |
| TypeScript | `npm install samyama-sdk` | `SamyamaClient.connectHttp("http://localhost:8080")` (ESM, Node 18+) |
| Rust | `samyama-sdk = { git = "https://github.com/samyama-ai/samyama-graph", tag = "v1.14.0" }` — `samyama-sdk` is not on crates.io yet | `RemoteClient` over HTTP, or `EmbeddedClient::new()` in-process |

Any Redis client also works over RESP on port 6379. Details:
[Python](sdk/python/README.md) · [TypeScript](sdk/typescript/README.md) ·
[Rust](crates/samyama-sdk/README.md).

## Two demos

### 1. Four public datasets, one graph, one query

We loaded the entire PubMed corpus plus ClinicalTrials.gov, Reactome pathways and
DrugBank into **one graph** — 74M nodes, 1B edges on a single machine — then
asked *"what drugs are most tested in cancer clinical trials?"*

```cypher
MATCH (m:MeSHTerm)<-[:ANNOTATED_WITH]-(a:Article)
      -[:REFERENCED_IN]->(t:ClinicalTrial)-[:TESTS]->(i:Intervention)
WHERE m.name = 'Neoplasms'
RETURN i.name, count(DISTINCT t) AS trials
ORDER BY trials DESC LIMIT 5
```

| Drug | Trials |
|------|--------|
| **Placebo** | **521** |
| Pembrolizumab | 137 |
| Carboplatin | 106 |
| Paclitaxel | 106 |
| Cyclophosphamide | 98 |

**10.3 seconds.** No ETL between the four sources — they are joined at query
time on shared identifiers.

That is query `XK02`, measured 2026-04-02 on one r6a.8xlarge and not re-measured
since; 96 of the 100 queries in that suite return real data. Per-row provenance
is in [`verified-results.csv`](https://graph.samyama.cloud/book/data/benchmark/verified-results.csv)
and [docs/BENCHMARKS.md](docs/BENCHMARKS.md#biomedical-scale-and-cross-kg-queries).

[All 100 queries →](https://graph.samyama.cloud/book/biomedical_benchmark.html)

### 2. A real knowledge graph in one command

[`case_studies/`](case_studies) downloads a public knowledge graph, imports it,
runs showcase Cypher and renders the session as a narrated GIF. Every showcase
query is gated to return real rows
([Definition of Done](case_studies/DEFINITION_OF_DONE.md)).

```bash
cargo build --release && pip install rich requests
cd case_studies/cricket && ./run.sh      # fetch snapshot → import → validate → demo
```

> Cricket KG — 36,619 nodes, 1,392,017 edges, live graph simulation

[![Samyama Graph Simulation](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/simulation-preview.gif)](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/samyama-cricket-demo.mp4)

*Click for the full demo (1:56).*

Ten more ship the same way — drug interactions, pathways, vector search over
DBMS research, IMDB, surveillance, health determinants, health systems, legal
judgments, bank model risk and football.
[Browse the catalogue →](case_studies)

## What it can do

**Cypher** — MATCH, CREATE, MERGE, aggregations, path finding, 30+ functions.
99.9% of the openCypher TCK's evaluated scenarios pass (3,845 of 3,847, at 98.7%
coverage of the 3,897-scenario corpus, measured 2026-09-15) —
[docs/CYPHER-COMPATIBILITY.md](docs/CYPHER-COMPATIBILITY.md).

```cypher
MATCH p = shortestPath((a:Person)-[:KNOWS*1..3]->(b:Person))
WHERE a.name = 'Alice'
RETURN b.name, length(p)
```

**Vector search** — HNSW indexing for semantic search and GraphRAG, in the same
query language as the traversal. `quantization: 'fp16'` halves the index's
memory.

```cypher
CREATE VECTOR INDEX paper_idx FOR (p:Paper) ON (p.embedding)
  OPTIONS {dimensions: 384, similarity: 'cosine'}

CALL db.index.vector.queryNodes('Paper', 'embedding', [0.1, 0.2, 0.3], 10)
  YIELD node, score
```

**Graph algorithms** — PageRank, WCC, SCC, BFS, Dijkstra, LCC, CDLP, triangle
count. PageRank, LCC, CDLP and triangle counting are Rayon-parallel and scale
with cores; WCC, betweenness and closeness run on one thread.

```cypher
CALL pagerank('social') YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC LIMIT 10
```

**Natural language and AI agents** — `NLQ "Who are Alice's friends of friends
that work at Google?"` translates English to Cypher through an LLM. For agents,
MCP servers are generated from your graph schema: `pip install samyama[mcp]`,
then `samyama-mcp-serve --demo cricket`.

**Operational basics** — `BEGIN` / `COMMIT` / `ROLLBACK` over RESP and HTTP with
a published isolation table ([docs/ACID-GUARANTEES.md](docs/ACID-GUARANTEES.md));
any read query out as Arrow or Parquet via `POST /api/query/export`
([docs/BI-CONNECTIVITY.md](docs/BI-CONNECTIVITY.md)); Prometheus metrics at
`/metrics` with a [Grafana dashboard](ops/grafana/); GPU acceleration via wgpu or
CUDA (`--features gpu` / `--features cuda`).

## Measured results

Every headline number is re-measured on a schedule by a conformance harness that
publishes `SCORECARD.json`, and a regression gate blocks the release tag on a
stale or red verdict. Run the suites yourself with `cargo bench --bench <name>`
([`benches/`](benches)); the vector, optimization and micro/MVCC suites are
self-contained, LDBC needs a data download.

| Suite | Result |
|-------|--------|
| openCypher TCK | 3,845 / 3,847 evaluated scenarios, 98.7% corpus coverage (2026-09-15) |
| LDBC SNB Interactive (SF1) | 21/21 complete, 21/21 return rows (`CH-BENCH-LDBC`) |
| LDBC SNB BI (SF1) | 20/20 complete, 0 timeouts (`CH-BENCH-LDBC`) |
| LDBC Graphalytics | 12/12 agree with the LDBC reference (`CH-BENCH-GALX`) |
| HIER (subsumption and roll-up) | 108/108 agree — ground LDBC and FinBench do not cover. Provenance records a dirty tree at commit `30d0731`; 4 further corpus queries are specified but uncontrolled and are not in that denominator |
| Memory | 200 resident bytes per edge on SNB SF10, 176M edges (`CH-MEM-01`, against a 256 B/edge target) |
| Cross-KG biomedical | 96 / 100 queries return real data — the first demo above |

These run in-tree; LDBC certification is a formal third-party process we have not
been through. Full results, provenance and caveats:
**[docs/BENCHMARKS.md](docs/BENCHMARKS.md)**.

## Knowledge graphs

Twenty-six public knowledge graphs are built for Samyama, each in its own
repository with its loader and source-data notes. They are what the demos above
and the research below are measured on.

| How to get one | Count |
|---|---|
| Prebuilt download — HuggingFace dataset and/or `.sgsnap` snapshot | 17 |
| Build it yourself from the repository's loader | 8 |
| Not published yet | 1 |

The biomedical set — `pubmed-kg` (66M nodes / 1.04B edges), `clinicaltrials-kg`,
`pathways-kg`, `druginteractions-kg`, `surveillance-kg`,
`health-determinants-kg`, `health-systems-kg` and more — is what the federation
demo composes. Beyond biomedicine there are security and IT-ops graphs
(`mitre-attack-kg`, `nvd-cve-kg`, `d3fend-kg`), industrial and financial ones
(`assetops-kg`, `bank-model-risk-kg`, `powergrid-kg`, `telecom-kg`), and demo
graphs (`cricket-kg`, `imdb-kg`, `football-kg`).

**[docs/KG-CATALOG.md](docs/KG-CATALOG.md)** has every graph with its scale, data
licence, download link and a query to start with. Each figure there is taken from
that graph's own repository rather than restated, and every download link is
checked on the date the page records.

## Papers

Samyama's design and results are written up in eighteen arXiv preprints —
fourteen `cs.DB`, two `cs.CL`, one `cs.LG` — under one
[author listing](https://arxiv.org/a/mandarapu_m_1.html). Four describe the
system and its applications directly:

| Paper | arXiv |
|---|---|
| Samyama: A Unified Graph-Vector Database with In-Database Optimization, Agentic Enrichment, and Hardware Acceleration | [2603.08036](https://arxiv.org/abs/2603.08036) |
| Open Biomedical Knowledge Graphs at Scale: Construction, Federation, and AI Agent Access | [2603.15080](https://arxiv.org/abs/2603.15080) |
| Knowledge Graphs as the Missing Data Layer for LLM-Based Industrial Asset Operations | [2605.26874](https://arxiv.org/abs/2605.26874) |
| Graph-Grounded Optimization: Rao-Family Metaheuristics, Classical OR, and SLM-Driven Formulation over Knowledge Graphs | [2605.12204](https://arxiv.org/abs/2605.12204) |

The rest are focused results that came out of building the engine — when q-error
predicts plan regret, filtered ANN as a phase transition, when to repair a graph
ANN index (a matched-budget negative result), LSM bloom-filter tuning, a
decode-throughput law for columnar scans, and a reference semantics for GQL and
SQL/PGQ path patterns.

Two agent-access results are worth calling out, because they compare MCP tools
against text-to-Cypher on the same questions: **BiomedQA** (40 questions) scores
MCP 98% · text-to-Cypher 85% · GPT-4o 75%, and **AssetOpsBench** (139 scenarios)
scores MCP 99% · text-to-Cypher 83% · GPT-4 65%.

**[Publication tracker →](https://graph.samyama.cloud/book/publication_tracker.html)**
for the full list with revisions and venue status, and
[a plain-language summary →](https://graph.samyama.cloud/book/research_paper_summary.html).
Reproduction packages live in one repository per paper under the
[`samyama-ai`](https://github.com/orgs/samyama-ai/repositories) organization.

## The ecosystem

This repository is the engine. Around it:

| | What it is |
|---|---|
| **The Book** | [graph.samyama.cloud/book](https://graph.samyama.cloud/book/) — the long-form documentation, benchmarks, KG catalog and papers |
| **Visualizer** | [graph.samyama.cloud](https://graph.samyama.cloud/) — hosted browser UI; point it at your own server |
| **Knowledge graphs** | 26 repositories, one per graph — [catalog](docs/KG-CATALOG.md) |
| **[graphrag-rs](https://github.com/samyama-ai/graphrag-rs)** | Ingests a folder of docs, extracts entities and relationships with an LLM, then serves the result as an MCP server. **Builds** a graph, where `samyama[mcp]` **serves** one that exists |
| **[biomedqa](https://github.com/samyama-ai/biomedqa)** | The 40-question pharmacology benchmark above, across three KGs |
| **[optimization_algorithms](https://github.com/samyama-ai/optimization_algorithms)** | Rao-family metaheuristics, on PyPI as `rao-algorithms` — the optimizers behind `algo.or.solve` |
| **Samyama Enterprise** | [samyama.dev](https://samyama.dev) — see below |

A fuller map, including the private repositories and how versions are kept in
step, is in
[ecosystem architecture](https://graph.samyama.cloud/book/ecosystem_architecture.html).

## Enterprise Edition

Everything in this repository is open source (Apache 2.0) — including GPU
acceleration, Prometheus metrics and the request audit log (`--audit-log`).
[Samyama Enterprise](https://samyama.dev) adds OpenTelemetry OTLP metrics, backup
and disaster recovery, ADMIN commands, and Ed25519 signed license tokens.
[Contact us →](https://samyama.dev/contact)

## Documentation

| Resource | Link |
|----------|------|
| **The Book** | [graph.samyama.cloud/book](https://graph.samyama.cloud/book/) |
| All documentation | [docs/README.md](docs/README.md) |
| Examples and loaders — 127 programs, 19 domain demos, 15 data loaders | [examples/README.md](examples/README.md) |
| Cypher compatibility | [docs/CYPHER-COMPATIBILITY.md](docs/CYPHER-COMPATIBILITY.md) |
| Benchmarks | [docs/BENCHMARKS.md](docs/BENCHMARKS.md) |
| Knowledge graph catalog | [docs/KG-CATALOG.md](docs/KG-CATALOG.md) |
| Security, auth, TLS and data handling | [docs/DATA-HANDLING.md](docs/DATA-HANDLING.md) |
| Migrating from Neo4j | [docs/MIGRATING-FROM-NEO4J.md](docs/MIGRATING-FROM-NEO4J.md) |
| Migrating from FalkorDB or RedisGraph | [docs/MIGRATING-FROM-FALKORDB.md](docs/MIGRATING-FROM-FALKORDB.md) |
| Failure modes | [docs/FAILURE-MODES.md](docs/FAILURE-MODES.md) |
| Release notes | [docs/release-notes/](docs/release-notes/) |
| Project layout | [CONTRIBUTING.md](CONTRIBUTING.md#project-layout) |
| API spec | [openapi/openapi.yaml](openapi/openapi.yaml) |
| Troubleshooting | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |

## Contributing

Contributions are welcome — bug reports, docs, tests and code. See
**[CONTRIBUTING.md](CONTRIBUTING.md)** for setup, build and test commands and the
pull request workflow. Found a bug?
[Open an issue](https://github.com/samyama-ai/samyama-graph/issues/new/choose).
Questions? [Join the community chat](https://chat.whatsapp.com/Jjjkb3uWRDi1YMdfffaD9d).
Every issue gets a first response within 2 business days
([SUPPORT.md](SUPPORT.md)); what is accepted for a date is in
[ROADMAP.md](ROADMAP.md).

## License

Apache License 2.0 — use it in production, contribute back if you'd like.

**Samyama** (Sanskrit: संयम) — the union of focused query, sustained analysis, and unified insight.
