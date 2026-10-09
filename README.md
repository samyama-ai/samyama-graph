<p align="center">
  <h1 align="center">Samyama Graph</h1>
  <p align="center">A Rust-native graph-vector database for GraphRAG, knowledge graphs, and billion-edge analytics.</p>
  <p align="center">
    <strong>99.9% of the openCypher TCK's evaluated scenarios pass · SNB Interactive 21/21 and SNB BI 20/20, no timeouts · 1B edges on one machine</strong>
  </p>
  <p align="center">
    <a href="https://github.com/samyama-ai/samyama-graph/releases"><img src="https://img.shields.io/badge/version-1.11.0-blue" alt="Version"></a>
    <a href="https://github.com/samyama-ai/samyama-graph/actions/workflows/ci.yml"><img src="https://github.com/samyama-ai/samyama-graph/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-blue" alt="License"></a>
    <a href="https://graph.samyama.cloud/book/"><img src="https://img.shields.io/badge/book-read_the_docs-orange" alt="Book"></a>
    <a href="https://chat.whatsapp.com/Jjjkb3uWRDi1YMdfffaD9d"><img src="https://img.shields.io/badge/community-WhatsApp-25D366?logo=whatsapp&logoColor=white" alt="WhatsApp Community"></a>
  </p>
</p>

---

## What is Samyama Graph?

Samyama Graph is a Rust-native graph-vector database that lets developers store, query, search, and analyze connected data in one system.

It brings together graph traversal, OpenCypher-style querying, vector search, graph algorithms, and Redis-compatible access, making it useful for GraphRAG, knowledge graphs, AI agent memory, and large-scale relationship analytics.

## Quickstart

### Option 1 — Docker

Pulling the image needs no registry account: it is public on GitHub Container
Registry. `latest` is the newest release; pin a version such as `:1.11.0` for a
reproducible setup.

**Step 1 — Write `docker-compose.yml`** in an empty folder:

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
      # Lets the hosted visualizer (Step 4) call this server from your browser.
      SAMYAMA_CORS_ORIGINS: https://graph.samyama.cloud
    volumes:
      - samyama-data:/app/samyama_data
volumes:
  samyama-data:
```

**Step 2 — Start it**

```bash
docker compose up -d
docker logs -f samyama-graph     # Ctrl-C to stop following the log
```

**Step 3 — Run your first query**

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

**Step 4 — Explore it visually (optional).** Open the hosted visualizer at
<https://graph.samyama.cloud/>. The engine itself needs no account, but the
visualizer asks you to sign up or sign in. Then click **Home**, enter
`http://localhost:8080` and click **Connect**. Without the `SAMYAMA_CORS_ORIGINS`
line from Step 1 the browser refuses the call — deliberately, because
`/api/query` runs arbitrary Cypher including `DELETE`
([#1328](https://github.com/samyama-ai/samyama-graph/issues/1328)).

**Step 5 — Stop or reset**

```bash
docker compose down        # stop
docker compose down -v     # stop and delete all graph data in the volume
```

The server needs no credential by default. To require a bearer token, serve
TLS, or enable auto-embedding, see [docs/DATA-HANDLING.md](docs/DATA-HANDLING.md).
To load a real dataset, pick one from the [case studies](#case-studies--prove-it-yourself).

### Option 2 — Build from source

`zstd-sys` generates its bindings with `bindgen`, which needs libclang; without it
the build fails part-way with a misleading `'stddef.h' file not found`.

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

## Clients

| Language | Install | Connect |
|----------|---------|---------|
| Python | `pip install samyama` | `SamyamaClient.connect("http://localhost:8080")`, or `SamyamaClient.embedded()` to run the engine in-process |
| TypeScript | `npm install samyama-sdk` | `SamyamaClient.connectHttp("http://localhost:8080")` (ESM, Node 18+) |
| Rust | `samyama-sdk = { git = "https://github.com/samyama-ai/samyama-graph", tag = "v1.11.0" }` — not on crates.io yet | `RemoteClient` over HTTP, or `EmbeddedClient::new()` in-process |

Any Redis client also works over RESP on port 6379. Details:
[Python](sdk/python/README.md) · [TypeScript](sdk/typescript/README.md) · [Rust](crates/samyama-sdk/README.md).

## What can you build with Samyama Graph?

- **GraphRAG systems** that combine vector search with graph traversal
- **Knowledge graph applications** for enterprise, research, healthcare, and operations data
- **AI agent memory** where entities, tools, actions, and context are stored as a graph
- **Biomedical and clinical graphs** across papers, trials, pathways, drugs, and conditions
- **Fraud, investigation, infrastructure and dependency graphs** for pattern and impact analysis

We loaded the entire PubMed corpus plus ClinicalTrials.gov, Reactome pathways, and DrugBank into **one graph**, then asked *"What drugs are most tested in cancer clinical trials?"*

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

**10.3 seconds.** One query. Four databases. 74 million nodes. 1 billion edges. A single machine.

That is query `XK02` in
[`verified-results.csv`](https://graph.samyama.cloud/book/data/benchmark/verified-results.csv):
10,250.2 ms, measured 2026-04-02 on one r6a.8xlarge and not re-measured since.
Provenance of each row is in [docs/BENCHMARKS.md](docs/BENCHMARKS.md#biomedical-scale-and-cross-kg-queries).

[See all 100 benchmark queries →](https://graph.samyama.cloud/book/biomedical_benchmark.html)

## Demo

> Cricket KG — 36K nodes, 1.4M edges, live graph simulation

[![Samyama Graph Simulation](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/simulation-preview.gif)](https://github.com/samyama-ai/samyama-graph/releases/download/kg-snapshots-v2/samyama-cricket-demo.mp4)

*Click for the full demo (1:56).*

## Case Studies — prove it yourself

[`case_studies/`](case_studies) downloads a real public knowledge graph, imports
it, runs showcase Cypher, and renders the session as a narrated GIF — one
command. Every showcase query is gated to return real rows
([Definition of Done](case_studies/DEFINITION_OF_DONE.md)).

```bash
cargo build --release && pip install rich requests
cd case_studies/cricket && ./run.sh          # fetch snapshot → import → validate → demo
```

| Domain | Scale (nodes / edges) | Highlight |
|--------|-------|-----------|
| [cricket](case_studies/cricket) | 37K / 1.4M | dismissal-rivalry networks, venues, awards |
| [drug-interactions](case_studies/drug-interactions) | 245K / 388K | polypharmacy shared-target risk, CYP hubs |
| [pathways](case_studies/pathways) | 119K / 835K | protein hubs (TP53), pathway crosstalk |
| [dbms-research](case_studies/dbms-research) | 19K · 2 HNSW | **vector search** — semantic "nearest topics" |
| [imdb-movies](case_studies/imdb-movies) | 1.94M / 2.63M | top-rated films, director–actor pairs, genre trends |

Plus surveillance, health-determinants, health-systems and football — [browse the catalogue →](case_studies)

## Why Samyama Graph?

| What | How |
|------|-----|
| **74M nodes, 1B edges** | PubMed + ClinicalTrials.gov + Reactome + DrugBank on one r6a.8xlarge |
| **96 of 100 queries return real data** | Point lookups, multi-hop traversals and cross-KG aggregations, measured 2026-04-02 on one r6a.8xlarge and not re-measured since — [all 100 queries](https://graph.samyama.cloud/book/biomedical_benchmark.html) |
| **Four algorithms scale with cores** | PageRank, LCC, CDLP and triangle counting are Rayon-parallel; WCC, betweenness and closeness run on one thread (`CH-ALGO-PARALLEL`, ALGO-09) |
| **LDBC suites run in-tree** | SNB Interactive 21/21 and SNB BI 20/20 at SF1, no timeouts; Graphalytics 12/12 against the LDBC reference answers |
| **200 resident bytes per edge** | Measured on LDBC SNB SF10 (176M edges) by `CH-MEM-01`, against a 256 B/edge target |
| **Transactions with a published isolation table** | `BEGIN` / `COMMIT` / `ROLLBACK` over RESP and HTTP — [`docs/ACID_GUARANTEES.md`](docs/ACID_GUARANTEES.md) |
| **Every headline number re-measured on a schedule** | A conformance harness publishes `SCORECARD.json`, and a regression gate blocks the release tag on a stale or red verdict |

## The 30-Second Tour

**Cypher** — MATCH, CREATE, MERGE, aggregations, path finding, 30+ functions.
99.9% of the openCypher TCK's evaluated scenarios pass (3,845 of 3,847, at 98.7%
coverage of the corpus, measured 2026-09-15) — see [`docs/CYPHER_COMPATIBILITY.md`](docs/CYPHER_COMPATIBILITY.md).

```cypher
MATCH p = shortestPath((a:Person)-[:KNOWS*1..3]->(b:Person))
WHERE a.name = 'Alice'
RETURN b.name, length(p)
```

**Graph algorithms** — PageRank, WCC, SCC, BFS, Dijkstra, LCC, CDLP, triangle count.

```cypher
CALL pagerank('social') YIELD nodeId, score
RETURN nodeId, score ORDER BY score DESC LIMIT 10
```

**Vector search** — HNSW indexing for semantic search and GraphRAG; `quantization: 'fp16'` halves the index's memory.

```cypher
CREATE VECTOR INDEX paper_idx FOR (p:Paper) ON (p.embedding) OPTIONS {dimensions: 384, similarity: 'cosine'}
CALL vector.search('Paper', 'embedding', [0.1, 0.2, 0.3], 10) YIELD node, score
```

**Natural language** — ask in English; an LLM translates to Cypher.

```
NLQ "Who are Alice's friends of friends that work at Google?"
```

**AI agents** — MCP servers generated from your graph schema: `pip install samyama[mcp]`, then `samyama-mcp-serve --demo cricket`.

**Out to pandas** — any read query as Arrow or Parquet via `POST /api/query/export` ([docs/BI-CONNECTIVITY.md](docs/BI-CONNECTIVITY.md)).

## Benchmarks

Run them with `cargo bench --bench <name>` ([`benches/`](benches)); the vector,
optimization and micro/MVCC suites are self-contained, LDBC needs a data download.
Full results, provenance and caveats: **[docs/BENCHMARKS.md](docs/BENCHMARKS.md)**.

| Benchmark | Result |
|-----------|--------|
| LDBC SNB Interactive (SF1) | 21/21 complete, 21/21 return rows (`CH-BENCH-LDBC`, 2026-08-28) |
| LDBC SNB BI (SF1) | 20/20 complete, 0 timeouts (`CH-BENCH-LDBC`) |
| LDBC Graphalytics | 12/12 agree with the LDBC reference (`CH-BENCH-GALX`) |
| Cross-KG biomedical | see the table below |

These run in-tree; LDBC certification is a formal third-party process we have not been through.

**HIER** ([`benchmarks/hier/`](benchmarks/hier)) covers subsumption and hierarchical roll-up, which LDBC and FinBench do not. Latest: **108/108 agree** — `benchmarks/hier/results/PROVENANCE.json`, engine commit `30d0731` with uncommitted changes (`"dirty": true`), committed 2026-09-21; 4 further corpus queries are specified but uncontrolled and are not in that denominator.

| ID | Query | Time | First row (CSV) |
|----|-------|------|-----------------|
| XK02 | Cancer → Trial interventions | 10.3s | Placebo (521 trials) |
| XK07 | Cancer trial sites by country | 4.2s | United States (4,062) |
| XK08 | NCI-funded → Trial interventions | 20.5s | Placebo (933) |

*Cross-KG times from `verified-results.csv`, 2026-04-02, one r6a.8xlarge.*

## Examples and loaders

[`examples/`](examples) holds 124 programs, among them 19 domain demos and 15 data
loaders — banking fraud, clinical trials, supply chain, manufacturing, SOC,
LDBC, FinBench, cricket, IMDB and more. Run them all with
`./scripts/run_all_examples.sh --batch`, or one with `cargo run --example banking_demo`.
**Guide:** [`examples/README.md`](examples/README.md).

## Related repositories

- **KGs:** [pubmed-kg](https://github.com/samyama-ai/pubmed-kg), [clinicaltrials-kg](https://github.com/samyama-ai/clinicaltrials-kg), [druginteractions-kg](https://github.com/samyama-ai/druginteractions-kg), [pathways-kg](https://github.com/samyama-ai/pathways-kg), [cricket-kg](https://github.com/samyama-ai/cricket-kg), [imdb-kg](https://github.com/samyama-ai/imdb-kg), [football-kg](https://github.com/samyama-ai/football-kg), [assetops-kg](https://github.com/samyama-ai/assetops-kg)
- **Benchmarks:** [biomedqa](https://github.com/samyama-ai/biomedqa) — 40-question pharmacology benchmark across three KGs
- **Companions:** [graphrag-rs](https://github.com/samyama-ai/graphrag-rs) — doc-to-KG + MCP server; [optimization_algorithms](https://github.com/samyama-ai/optimization_algorithms) — PyPI `rao-algorithms`

## Documentation

| Resource | Link |
|----------|------|
| **The Book** | [graph.samyama.cloud/book](https://graph.samyama.cloud/book/) |
| All documentation | [docs/README.md](docs/README.md) |
| Cypher compatibility | [docs/CYPHER_COMPATIBILITY.md](docs/CYPHER_COMPATIBILITY.md) |
| Benchmarks | [docs/BENCHMARKS.md](docs/BENCHMARKS.md) |
| Security, auth, TLS and data handling | [docs/DATA-HANDLING.md](docs/DATA-HANDLING.md) |
| Migrating from Neo4j | [docs/MIGRATING-FROM-NEO4J.md](docs/MIGRATING-FROM-NEO4J.md) |
| Failure modes | [docs/FAILURE-MODES.md](docs/FAILURE-MODES.md) |
| Project layout | [CONTRIBUTING.md](CONTRIBUTING.md#project-layout) |
| API spec | [openapi/openapi.yaml](openapi/openapi.yaml) |
| Troubleshooting | [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) |

## Enterprise Edition

Everything in this repository is open source (Apache 2.0), including GPU
acceleration (wgpu, plus CUDA — build with `--features gpu` or `--features cuda`),
Prometheus metrics at `/metrics` with a [Grafana dashboard](ops/grafana/), and a
request audit log (`--audit-log`). [Samyama Enterprise](https://samyama.dev) adds:

- OpenTelemetry OTLP metrics
- Backup & disaster recovery
- ADMIN commands
- Ed25519 signed license tokens

[Contact us →](https://samyama.dev/contact)

## Contributing

Contributions are welcome — bug reports, docs, tests, and code. See
**[CONTRIBUTING.md](CONTRIBUTING.md)** for setup, build/test commands and the
pull request workflow. Found a bug? [Open an issue](https://github.com/samyama-ai/samyama-graph/issues/new/choose).
Questions? [Join the community chat](https://chat.whatsapp.com/Jjjkb3uWRDi1YMdfffaD9d).

## License

Apache License 2.0 — use it in production, contribute back if you'd like.

**Samyama** (Sanskrit: संयम) — the union of focused query, sustained analysis, and unified insight.
