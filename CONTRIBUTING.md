# Contributing to Samyama Graph

First off — thank you for taking the time to contribute! Samyama is an
open-source (Apache-2.0) distributed graph + vector database written in Rust,
and contributions of all sizes are welcome: bug reports, docs, tests, examples,
and code.

This guide explains how to get set up and how to get a change merged.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Ways to Contribute](#ways-to-contribute)
- [Development Setup](#development-setup)
- [Building, Testing, and Linting](#building-testing-and-linting)
- [Making a Change](#making-a-change)
- [Commit Message Convention](#commit-message-convention)
- [Opening a Pull Request](#opening-a-pull-request)
- [Where to Start](#where-to-start)
- [Project Layout](#project-layout)

## Code of Conduct

Please be respectful and constructive in all interactions. Assume good intent,
keep discussions technical, and help make this a welcoming project for
newcomers.

## Ways to Contribute

You do **not** need to write Rust to be useful here:

- **Report a bug** — open an issue with steps to reproduce, expected vs. actual
  behavior, and your OS / Rust version (`rustc --version`).
- **Improve docs** — the `docs/` directory, the `README`, and inline doc
  comments can always be clearer.
- **Add or improve tests** — the suite is large but coverage gaps exist; extra
  test cases for existing modules are always welcome.
- **Add an example / case study** — see `examples/` and `case_studies/`.
- **Fix a bug or add a feature** — see [Where to Start](#where-to-start).

If in doubt, **open an issue first** to discuss the change before investing time
in a large PR.

## Development Setup

You will need a recent stable Rust toolchain (installed via
[rustup](https://rustup.rs/)).

Fork the repository on GitHub, then:

```bash
# Clone your fork
git clone https://github.com/<your-username>/samyama-graph.git
cd samyama-graph

# Add the upstream repo so you can stay in sync
git remote add upstream https://github.com/samyama-ai/samyama-graph.git
git fetch upstream
```

Keep your `main` in sync with upstream before starting new work:

```bash
git checkout main
git merge --ff-only upstream/main
git push origin main
```

## Building, Testing, and Linting

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Optimized build

# Test (full suite)
cargo test
cargo test graph::node         # A specific module
cargo test -- --nocapture      # Show test output

# Benchmarks (Criterion + domain suites in benches/)
cargo bench

# Code quality — these must pass before you open a PR
cargo fmt -- --check           # Formatting
cargo clippy -- -D warnings    # Lints (warnings are treated as errors)
```

Integration tests require a running server:

```bash
cargo run                      # RESP on 127.0.0.1:6379, HTTP on :8080
# in another terminal:
cd tests/integration
python3 test_resp_basic.py
```

For more detail on the architecture and available example demos, see
[`CLAUDE.md`](CLAUDE.md) and [`docs/`](docs/).

## Making a Change

1. Create a topic branch off the latest upstream `main`:

   ```bash
   git checkout -b fix/short-description upstream/main
   ```

   Use a descriptive prefix: `fix/`, `feat/`, `docs/`, `test/`, `refactor/`.

2. Make your change. Keep the diff focused — one logical change per PR.

3. Before committing, make sure the checks pass:

   ```bash
   cargo fmt -- --check
   cargo clippy -- -D warnings
   cargo test
   ```

4. Add or update tests for any behavior you change.

## Commit Message Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
The type prefix helps generate changelogs and communicates intent:

```
<type>(<optional scope>): <short summary>
```

Common types used in this repo:

| Type       | Use for                                          |
|------------|--------------------------------------------------|
| `feat`     | A new feature                                    |
| `fix`      | A bug fix                                         |
| `docs`     | Documentation only                               |
| `test`     | Adding or fixing tests                            |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `chore`    | Build, tooling, or maintenance                   |
| `ci`       | CI configuration                                 |

Examples from the project history:

```
feat(vector): add HNSW rebuild after snapshot import
fix(scripts): correct example runner path
docs(quickstart): clarify RESP connection steps
```

## Opening a Pull Request

1. Push your branch to your fork:

   ```bash
   git push origin fix/short-description
   ```

2. Open a Pull Request against `samyama-ai/samyama-graph`'s `main` branch.

3. In the PR description, explain **what** changed and **why**, and link any
   related issue (e.g. `Closes #123`).

4. Ensure CI is green. A maintainer (see [`CODEOWNERS`](CODEOWNERS)) will review
   your change. Please respond to review feedback by pushing additional commits
   to the same branch.

Small, well-scoped PRs are reviewed and merged faster than large ones.

## Where to Start

Good first contributions, roughly easiest to hardest:

1. **Docs & examples** — fix inaccuracies or fill gaps in `docs/` and `README`.
2. **Tests** — add cases for an under-tested module.
3. **Cypher functions** — the supported function list is in
   [`CLAUDE.md`](CLAUDE.md); standard OpenCypher has more that could be added
   (parser + executor + tests).
4. **Open issues / roadmap items** — see [`ROADMAP.md`](ROADMAP.md) and known
   gaps noted in `docs/CYPHER_COMPATIBILITY.md`.

## Project Layout

```
src/
├── graph/         # Property graph model (store, node, edge, property)
├── query/         # OpenCypher engine (parser, planner, executor)
├── protocol/      # RESP (Redis-compatible) protocol server
├── persistence/   # RocksDB storage, WAL, multi-tenancy
├── raft/          # High availability (openraft)
├── nlq/           # Natural-language-to-Cypher pipeline
├── vector/        # HNSW vector index
├── snapshot/      # Portable .sgsnap export/import
└── sharding/      # Tenant-level sharding

benches/           # Criterion + domain benchmarks
examples/          # Runnable demos and data loaders
tests/             # Integration tests
docs/              # Architecture docs, ADRs, compatibility notes
```

---

Thanks again for contributing to Samyama Graph! 🙏
