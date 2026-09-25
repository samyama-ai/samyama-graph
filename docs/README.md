# Samyama Graph — Documentation Index

**Last updated:** 2026-09-25 · **Engine version:** see [Releases](https://github.com/samyama-ai/samyama-graph/releases) — documents below state the version they were verified against

## Primary sources

Most architectural and design information now lives outside this directory:

| Source | What it covers |
|---|---|
| **[The book](https://graph.samyama.cloud/book)** | Narrative architecture, benchmarks, papers, deployment. Sources in `samyama-cloud/book/src/`. |
| **[Engineering Compendium](https://graph.samyama.cloud/book/) → `samyama-cloud/wiki/topics/engineering-compendium.md`** | 38-topic master technical reference (storage, in-memory layout, indexing, query engine, concurrency, distributed, compute). Each entry: where used → how it works → alternatives → honest evaluation → ADR linkage. |
| **[ADR/](./ADR/)** | 30 Architecture Decision Records — the "why" behind every architectural choice (numbered, dated, status-tracked). |

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
- **[LEAVING-SAMYAMA.md](./LEAVING-SAMYAMA.md)** — getting your data out. Documented on purpose: an exit you cannot find is not an exit.

### The query language

- **[CYPHER_COMPATIBILITY.md](./CYPHER_COMPATIBILITY.md)** — OpenCypher coverage against the TCK, with the pass rate quoted beside its coverage.
- **[FULL-TEXT-SEARCH.md](./FULL-TEXT-SEARCH.md)** — the full-text index and how it is queried.
- **[GDS-COMPATIBILITY.md](./GDS-COMPATIBILITY.md)** — which `gds.*` names resolve here, and which deliberately do not.
- **[ALGORITHM-CONVENTIONS.md](./ALGORITHM-CONVENTIONS.md)** — directedness, weights, self-loops, disconnected components, tie-breaking.

### Guarantees and behaviour under stress

- **[ACID_GUARANTEES.md](./ACID_GUARANTEES.md)** — the transaction model: MVCC, the logical WAL, Raft replication, and what each does not promise.
- **[FAILURE-MODES.md](./FAILURE-MODES.md)** — how the engine behaves when things go wrong.
- **[BI-CONNECTIVITY.md](./BI-CONNECTIVITY.md)** — connecting BI tools.

### Data, provenance and privacy

- **[DATA-HANDLING.md](./DATA-HANDLING.md)** — what leaves the machine and when; the authentication, TLS, audit and at-rest-encryption surfaces.
- **[DATA-PROVENANCE.md](./DATA-PROVENANCE.md)** — how model-generated and ingested data are kept apart.
- **[pii-waivers.json](./pii-waivers.json)** — findings from the published-snapshot PII scan that have been reviewed and accepted, each with its reason.

### Interfaces and operations

- **[SDK_API_CLI_ARCHITECTURE.md](./SDK_API_CLI_ARCHITECTURE.md)** — how the Python / TypeScript SDKs, the CLI, and the HTTP / RESP surfaces connect.
- **[AGENT-CONTRACT.md](./AGENT-CONTRACT.md)** — the versioned MCP tool contract: schemas, error format, pagination, result envelopes, idempotency.
- **[API-PARITY.md](./API-PARITY.md)** — generated capability-by-surface matrix (HTTP, RESP, Rust / Python / TypeScript SDKs, MCP); regenerate with `cargo run --release --example feature_matrix`.
- **[CONSTRAINED-CLIENTS.md](./CONSTRAINED-CLIENTS.md)** — writing to an Edge node from an ESP32 or Arduino-class device over RESP, and why that surface has no authentication.
- **[BENCHMARKS.md](./BENCHMARKS.md)** — LDBC SNB Interactive results (SF1 and SF10).
- **[SUPPLY_CHAIN_GUARDIAN_DEMO.md](./SUPPLY_CHAIN_GUARDIAN_DEMO.md)** — an end-to-end demo combining ingestion, federation, NLQ and optimization.

### Sub-directories

- **[ADR/](./ADR/)** — Architecture Decision Records. A record of why a decision was made *when it was made*; they are not refreshed, and old ones are not stale.
- **[optimization/](./optimization/)** — optimization case study (see also `crates/samyama-optimization/` and ADR-026).
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
