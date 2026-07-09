# Contributing to Samyama Graph

First off — thank you for taking the time to contribute! 🙌 Samyama Graph is an
open-source, Rust-native graph-vector database, and it gets better with every
issue, example, benchmark, and pull request from the community.

This guide explains how to get set up and how to send changes we can merge quickly.

---

## Ways to contribute

You don't have to write Rust to help:

- 🐛 **Report bugs** or 💡 **request features** — open an [issue](../../issues/new/choose).
- 📖 **Improve docs, examples, or case studies** — often the highest-leverage contribution.
- 🔌 **Build integrations** — LangChain, LlamaIndex, Python/TS SDK helpers, importers.
- ⚡ **Extend the engine** — Cypher coverage, graph algorithms, vector search, connectors.
- 🌱 **Pick a [`good first issue`](../../labels/good%20first%20issue)** if you're new here.

New contributors: start with an issue labelled `good first issue` or `help wanted`,
and feel free to comment to claim it before you start.

---

## Getting set up

### Prerequisites

- **Rust** (stable, edition 2021) via [rustup](https://rustup.rs).
- A **C/C++ toolchain + LLVM/libclang** — the `rocksdb` crate compiles RocksDB from
  source, so you need `clang`/`libclang` and a working C++ compiler:
  - **Linux:** `build-essential clang libclang-dev cmake`
  - **macOS:** `xcode-select --install` (Clang ships with the Command Line Tools)
  - **Windows:** Visual Studio Build Tools (C++ workload) + [LLVM](https://releases.llvm.org/) + CMake
- **Docker** (optional) if you'd rather run the prebuilt image than build locally.

### Build & run

```bash
git clone https://github.com/samyama-ai/samyama-graph && cd samyama-graph
cargo build --release
./target/release/samyama        # RESP on :6379, HTTP on :8080
```

Connect with any Redis client:

```bash
redis-cli -p 6379
GRAPH.QUERY mydb "CREATE (a:Person {name:'Alice'})-[:KNOWS]->(b:Person {name:'Bob'})"
```

### Run tests and examples

```bash
cargo test --workspace                 # full test suite
cargo run --example banking_demo       # any of the demos in examples/
cargo bench --bench vector_benchmark   # self-contained benchmarks (see benches/)
```

---

## Before you open a pull request

Please make sure these pass locally — they're the same gates we expect on review:

```bash
cargo fmt --all                                       # formatting
cargo clippy --all-targets --all-features -- -D warnings   # lints
cargo test --workspace                                # tests
cargo deny check                                      # license + advisory audit (see deny.toml)
```

If your change affects performance, include a relevant `cargo bench` result in the PR.

---

## Project layout

Samyama Graph is a Cargo workspace:

| Path | What it is |
|------|-----------|
| `src/` | The `samyama` engine (server binary + core library) |
| `crates/samyama-graph-algorithms/` | PageRank, BFS, Dijkstra, WCC, SCC, LCC, CDLP, Triangle Count (rayon-parallel) |
| `crates/samyama-optimization/` | Metaheuristic solvers (Jaya, Rao, GWO, NSGA-II, TLBO, …) |
| `crates/samyama-sdk/` | Rust SDK (embedded + remote clients) |
| `cli/` | Command-line interface |
| `examples/` | Runnable domain demos + data loaders |
| `benches/` | Benchmarks (vector, LDBC, micro, MVCC, …) |
| `case_studies/` | One-command, download-and-run public knowledge graphs |
| `docs/` | The Book source, ADRs, compatibility notes |

Core engine modules (inside `src/`): `graph/` (property graph model), `query/`
(OpenCypher engine — grammar, executor, planner), `protocol/` (RESP3 server),
`persistence/` (RocksDB + WAL + multi-tenancy), `vector/` (HNSW), `snapshot/`
(`.sgsnap` format), `raft/` (consensus), `nlq/` (natural language → Cypher).

See [`ROADMAP.md`](ROADMAP.md) for where the project is headed.

---

## Pull request process

1. **Branch from `main`** with a descriptive name: `feat/…`, `fix/…`, `docs/…`, `perf/…`.
2. **Keep PRs focused and small** — easier to review, faster to merge.
3. **Reference the issue** it addresses (`Fixes #123`).
4. **Describe what changed and why.** Include a repro, test, or benchmark where relevant.
5. Ensure the local checks above are green.
6. A maintainer ([@sandeepkunkunuru](https://github.com/sandeepkunkunuru), see
   [`CODEOWNERS`](CODEOWNERS)) will review. Address feedback with follow-up commits.

### Commit messages

We favour a lightweight [Conventional Commits](https://www.conventionalcommits.org)
style — `feat:`, `fix:`, `docs:`, `perf:`, `refactor:`, `test:`, `chore:` — so the
history stays readable and release notes are easy to assemble.

---

## Questions & community

- 💬 [WhatsApp community](https://chat.whatsapp.com/Jjjkb3uWRDi1YMdfffaD9d) — questions, help, updates
- 🗣️ [GitHub Discussions](../../discussions) — design questions, ideas, Q&A
- 📚 [The Book](https://graph.samyama.cloud/book/) — full documentation

---

## Code of Conduct

By participating, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Samyama Graph is licensed under the [Apache License 2.0](LICENSE). By submitting a
contribution, you agree that it will be licensed under the same terms.
