# Samyama Graph — Documentation Index

**Last Updated:** 2026-05-19 · **Engine version:** v1.0.0 (shipped 2026-04-11)

## Primary sources

Most architectural and design information now lives outside this directory:

| Source | What it covers |
|---|---|
| **[The book](https://graph.samyama.cloud/book)** | Narrative architecture, benchmarks, papers, deployment. Sources in `samyama-cloud/book/src/`. |
| **[Engineering Compendium](https://graph.samyama.cloud/book/) → `samyama-cloud/wiki/topics/engineering-compendium.md`** | 38-topic master technical reference (storage, in-memory layout, indexing, query engine, concurrency, distributed, compute). Each entry: where used → how it works → alternatives → honest evaluation → ADR linkage. |
| **[ADR/](./ADR/)** | 30 Architecture Decision Records — the "why" behind every architectural choice (numbered, dated, status-tracked). |

If you're new to the codebase, start with the book; if you're touching a subsystem, read its ADR and the matching Engineering Compendium topic.

## Files in this directory

### Core
- **[ACID_GUARANTEES.md](./ACID_GUARANTEES.md)** — ACID model in v1.0.0: MVCC (RC + SI + conflict detection), Samyama logical WAL, Raft replication.
- **[CYPHER_COMPATIBILITY.md](./CYPHER_COMPATIBILITY.md)** — OpenCypher coverage (~90%) vs Neo4j and FalkorDB; supported clauses, functions, remaining gaps.
- **[REQUIREMENTS.md](./REQUIREMENTS.md)** — functional and non-functional requirements specification.
- **[GLOSSARY.md](./GLOSSARY.md)** — domain and engine terms (MVCC, HNSW, NLQ, MCP, GAK, etc.).
- **[SDK_API_CLI_ARCHITECTURE.md](./SDK_API_CLI_ARCHITECTURE.md)** — how Python / TypeScript SDKs, the CLI, and HTTP / RESP clients connect.
- **[SUPPLY_CHAIN_GUARDIAN_DEMO.md](./SUPPLY_CHAIN_GUARDIAN_DEMO.md)** — end-to-end demo combining ingestion, federation, NLQ, and optimization.

### Sub-directories
- **[ADR/](./ADR/)** — Architecture Decision Records (30 ADRs).
- **[product/](./product/)** — product management artifacts (personas, workflows, test cases).
- **[test-results/](./test-results/)** — test execution reports.
- **[optimization/](./optimization/)** — optimization case study + workflow notes (see also `crates/samyama-optimization/` and ADR-026).
- **[ldbc/](./ldbc/)** — LDBC benchmark results (SNB Interactive, FinBench, Graphalytics).
- **[archive/](./archive/)** — historical documents and earlier-phase records.

### Retired

The following documents were retired in 2026-05-19 because their content is now better maintained in the Engineering Compendium and the book:

| Retired | Where to look now |
|---|---|
| `ARCHITECTURE.md` | Book Part III (chapters 9–16) + Engineering Compendium §1–§5 |
| `TECH_STACK.md` | Book *Technology Choices* + ADR-001 (Rust), ADR-002 (RocksDB), ADR-003 (RESP), ADR-004 (Raft), ADR-006 (Tokio) |
| `TECHNOLOGY_COMPARISONS.md` | Engineering Compendium *alternatives we considered* sections per topic |

The retirements were content-redundancy cleanups, not architectural changes — see git history for the prior versions.
