# Samyama Graph — Documentation Index

**Last updated:** 2026-10-29 · **Engine version:** see [Releases](https://github.com/samyama-ai/samyama-graph/releases) — documents below state the version they were verified against

## Primary sources

Most architectural and design information now lives outside this directory:

| Source | What it covers |
|---|---|
| **[The book](https://graph.samyama.cloud/book)** | Narrative architecture, benchmarks, papers, deployment. Sources in `samyama-cloud/book/src/`. |
| **[Engineering Compendium](https://graph.samyama.cloud/book/) → `samyama-cloud/wiki/topics/engineering-compendium.md`** | 38-topic master technical reference (storage, in-memory layout, indexing, query engine, concurrency, distributed, compute). Each entry: where used → how it works → alternatives → honest evaluation → ADR linkage. |
| **[ADR/](./ADR/)** | Architecture Decision Records — the "why" behind every architectural choice (numbered, dated, status-tracked). |

If you're new to the codebase, start with the book; if you're touching a subsystem, read its ADR and the matching Engineering Compendium topic.

## Files in this directory

Every markdown file in this directory is listed below. If you add one, add a
line here — an index that lists some of a directory is worse than no index,
because a reader who does not find a document assumes it does not exist.

That is now a check rather than a request: `tests/docs_index_lists_every_document.rs`
reads this directory and fails if a document or sub-directory is unlisted, if a
relative link here points at nothing, or if the engine's current version has no
release notes. The list was hand-maintained until 2026-09-25 and had drifted to
eight of eighteen documents (#1450).

### Start here

- **[TROUBLESHOOTING.md](./TROUBLESHOOTING.md)** — what to do when something does not work.
- **[GLOSSARY.md](./GLOSSARY.md)** — domain and engine terms (MVCC, HNSW, NLQ, MCP, GAK).
- **[MIGRATING-FROM-NEO4J.md](./MIGRATING-FROM-NEO4J.md)** — bringing queries and data across, and the compatibility report that tells you what will run.
- **[MIGRATING-FROM-FALKORDB.md](./MIGRATING-FROM-FALKORDB.md)** — FalkorDB and RedisGraph: the RESP differences (graph key, parameters, reply shape, `--compact`), the Cypher to rewrite, and how to move the data.
- **[LEAVING-SAMYAMA.md](./LEAVING-SAMYAMA.md)** — getting your data out. Documented on purpose: an exit you cannot find is not an exit.
- **[KG-CATALOG.md](./KG-CATALOG.md)** — every public knowledge graph built for Samyama: what it holds, its scale, schema and data licence, where to download it, and a query to start with.
- **[MOVING-DATA.md](./MOVING-DATA.md)** — `samyama convert`: which formats convert to and from `.sgsnap`, the RDF mapping file and presets, loss reports and exit codes.

### The query language

- **[CYPHER-COMPATIBILITY.md](./CYPHER-COMPATIBILITY.md)** — OpenCypher coverage against the TCK, with the pass rate quoted beside its coverage.
- **[GQL-GAP-ANALYSIS.md](./GQL-GAP-ANALYSIS.md)** — a historical planning analysis of possible GQL gaps, explicitly not a GQL conformance claim.
- **[FULL-TEXT-SEARCH.md](./FULL-TEXT-SEARCH.md)** — the full-text index and how it is queried.
- **[INDEX-CONTRACT.md](./INDEX-CONTRACT.md)** — the six clauses every index structure implements (build, maintain under MVCC, cost, plan, persist, snapshot), and how to add one.
- **[GDS-COMPATIBILITY.md](./GDS-COMPATIBILITY.md)** — which `gds.*` names resolve here, and which deliberately do not.
- **[ALGORITHM-CONVENTIONS.md](./ALGORITHM-CONVENTIONS.md)** — directedness, weights, self-loops, disconnected components, tie-breaking.

### Guarantees and behaviour under stress

- **[ACID-GUARANTEES.md](./ACID-GUARANTEES.md)** — the transaction model: MVCC, the logical WAL, what durability `SAMYAMA_FSYNC` does and does not buy, and why there is no Raft replication to rely on.
- **[FAILURE-MODES.md](./FAILURE-MODES.md)** — how the engine behaves when things go wrong.
- **[SOC2-CONTROL-SET.md](./SOC2-CONTROL-SET.md)** — SOC 2 gap analysis (documented, not certified): access control, audit, change management, backup and incident response, each control with its evidence and its gap.
- **[BI-CONNECTIVITY.md](./BI-CONNECTIVITY.md)** — connecting BI tools.
- **[ORCHESTRATION.md](./ORCHESTRATION.md)** — loading the graph from a scheduled pipeline: a retry-safe, parameter-bound load with validation, as reference pipelines for Airflow and Dagster that the test suite runs against a real server.
- **[ORCHESTRATION-AIRFLOW.md](./ORCHESTRATION-AIRFLOW.md)** — how to run the reference DAG in Airflow 3: credential, Connection, params, what a failure means.
- **[ORCHESTRATION-DAGSTER.md](./ORCHESTRATION-DAGSTER.md)** — how to run the reference assets and checks in Dagster.

### Data, provenance and privacy

- **[DATA-HANDLING.md](./DATA-HANDLING.md)** — what leaves the machine and when; the authentication, TLS, audit and at-rest-encryption surfaces.
- **[DATA-PROVENANCE.md](./DATA-PROVENANCE.md)** — how model-generated and ingested data are kept apart.
- **[pii-waivers.json](./pii-waivers.json)** — findings from the published-snapshot PII scan that have been reviewed and accepted, each with its reason.

### Interfaces and operations

- **[SDK-API-CLI-ARCHITECTURE.md](./SDK-API-CLI-ARCHITECTURE.md)** — how the Python / TypeScript SDKs, the CLI, and the HTTP / RESP surfaces connect.
- **[AGENT-CONTRACT.md](./AGENT-CONTRACT.md)** — the versioned MCP tool contract: schemas, error format, pagination, result envelopes, idempotency.
- **[VERSIONING.md](./VERSIONING.md)** — what a version number promises: semver for the HTTP API, RESP and the SDKs, the N-1 SDK guarantee and the CI job that checks it, and two minor releases of notice before anything is removed.
- **[deprecations.json](./deprecations.json)** — every deprecation: when it was announced, what replaces it, and when it was removed; checked by `tests/deprecations_follow_the_policy.rs`.
- **[API-PARITY.md](./API-PARITY.md)** — generated capability-by-surface matrix (HTTP, RESP, Rust / Python / TypeScript SDKs, MCP); regenerate with `cargo run --release --example feature_matrix`.
- **[CONSTRAINED-CLIENTS.md](./CONSTRAINED-CLIENTS.md)** — writing to an Edge node from an ESP32 or Arduino-class device over RESP, and why that surface has no authentication.
- **[BENCHMARKS.md](./BENCHMARKS.md)** — LDBC SNB Interactive results (SF1 and SF10).
- **[SUPPLY-CHAIN-GUARDIAN-DEMO.md](./SUPPLY-CHAIN-GUARDIAN-DEMO.md)** — walkthrough of `supply_chain_demo`: disruption analysis, PageRank, Jaya rerouting and vector search on built-in data, and which of its results are artefacts of that data.

### Sub-directories

- **[ADR/](./ADR/)** — Architecture Decision Records. A record of why a decision was made *when it was made*; they are not refreshed, and old ones are not stale.
- **[release-notes/](./release-notes/)** — per-release notes, current back to [1.7.0](./release-notes/1.7.0.md). Earlier tags (1.0.x, 1.1.0, 1.7.1) have none and are not back-filled.
- **[demos/](./demos/)** — recordings used by the top-level README.

### Retired

The following documents were retired in 2026-05-19 because their content is now better maintained in the Engineering Compendium and the book:

| Retired | Where to look now |
|---|---|
| `ARCHITECTURE.md` | Book Part III (chapters 9–16) + Engineering Compendium §1–§5 |
| `TECH_STACK.md` | Book *Technology Choices* + ADR-001 (Rust), ADR-002 (RocksDB), ADR-003 (RESP), ADR-004 (Raft), ADR-006 (Tokio) |
| `TECHNOLOGY_COMPARISONS.md` | Engineering Compendium *alternatives we considered* sections per topic |

The retirements were content-redundancy cleanups, not architectural changes — see git history for the prior versions.

Retired 2026-10-14 because it no longer described anything in the repository (#1839):

| Retired | Where to look now |
|---|---|
| `optimization/CASE_STUDY_HEALTHCARE.md` | It documented `examples/healthcare_demo.rs`, a Jaya hospital-staffing example that was replaced on 2026-02-06 and deleted on 2026-02-07. For the solvers, see [`crates/samyama-optimization/README.md`](../crates/samyama-optimization/README.md) and [ADR-026](./ADR/ADR-026-rao-optimization-crate.md); for a healthcare allocation problem that runs today, `healthcare_allocation_demo` in [`examples/README.md`](../examples/README.md). |
