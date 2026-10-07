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
- [Releasing](#releasing)

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
[rustup](https://rustup.rs/)) and a few system packages. `zstd-sys` generates its
bindings with `bindgen`, which needs libclang — without it the build fails part-way
through with a misleading `'stddef.h' file not found`, so install these first:

```bash
# Debian / Ubuntu
sudo apt-get install -y build-essential cmake pkg-config libssl-dev clang libclang-dev

# Fedora / RHEL
sudo dnf install -y gcc gcc-c++ cmake pkgconf-pkg-config openssl-devel clang clang-devel

# macOS — the Xcode Command Line Tools already provide clang
xcode-select --install
```

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

### What CI runs, and when

| Workflow | Trigger | What it does | Typical time |
|---|---|---|---|
| **CI** (`ci.yml`) | every PR, push to `main` | `cargo test --workspace` (dev profile) and `cargo check --all-targets` | ~7 min |
| **Nightly sweep** (`nightly.yml`) | 02:30 UTC, or manual dispatch | `scripts/verify-sweep.sh` — tests, every example, every bench, every case study | ~50 min |
| **GPU CI** (`gpu-ci.yml`) | manual dispatch only | GPU-path tests on a self-hosted runner | — |

**`test (ubuntu-latest)` is a required check on `main`.** A PR cannot merge until
it passes.

Tests run in the **dev** profile, not release. Release compilation of the test
binaries is what costs the wall-clock time; the tests themselves run in under a
second either way. If your change needs release-profile timings, it belongs in
the nightly sweep.

`cargo fmt --check` and `cargo clippy -D warnings` are **not** currently gated —
the tree has ~5,000 format diffs and ~680 clippy warnings, so either gate would
be red from its first run. Adopting them is tracked in
[#487](https://github.com/samyama-ai/samyama-graph/issues/487). Please do not
reformat files you are not otherwise touching; it makes review harder and the
mechanical reformat is planned as a single reviewable commit.

You can run the fast lane locally before pushing:

```bash
cargo test --workspace          # what the PR check runs
./scripts/verify-sweep.sh       # what the nightly runs
```

The nightly's bench timings come from a shared hosted runner and are **not
comparable between runs** — they answer "does it still run", not "how fast".
Published numbers come from a recorded host.

## Where to Start

Good first contributions, roughly easiest to hardest:

1. **Docs & examples** — fix inaccuracies or fill gaps in `docs/` and `README`.
2. **Tests** — add cases for an under-tested module.
3. **Cypher functions** — the supported function list is in
   [`CLAUDE.md`](CLAUDE.md); standard OpenCypher has more that could be added
   (parser + executor + tests).
4. **Open issues** — see the
   [issue tracker](https://github.com/samyama-ai/samyama-graph/issues), where
   each issue is a concrete unit of work, and known gaps noted in
   `docs/CYPHER_COMPATIBILITY.md`.

## Project Layout

```
src/                  # The server engine — the `samyama` library crate and its server binary
├── agent/            # Agentic enrichment: tool-using agents that plan and write graph updates
├── algo/             # Adapter exposing GraphStore to the samyama-graph-algorithms crate (GraphView)
├── embed/            # Auto-embed pipelines: text splitting + embedding generation for RAG
├── export/           # Result export: Arrow/Parquet, CSV, GraphML
├── graph/            # Property graph model (node, edge, property, catalog, CSR store)
├── http/             # HTTP/REST API and Web UI (tenants, transactions, vector, optimize)
├── index/            # Property, unique, composite, full-text and hierarchy (OEH) indexes; cardinality sketches
├── migrate/          # Importers for other engines' export formats (Neo4j APOC JSON)
├── nlq/              # Natural-language-to-Cypher pipeline (LLM-backed)
├── optimization/     # CypherProblem — graph-grounded problems for the samyama-optimization crate
├── persistence/      # RocksDB storage, WAL, multi-tenancy, health checks
├── protocol/         # RESP (Redis-compatible) protocol server
├── query/            # OpenCypher engine (PEG grammar, parser, binder, validator, planner, executor)
├── raft/             # High availability via openraft (cluster, network, state machine, storage)
├── rdf/              # RDF triple/quad store, namespace and schema mapping, serialization
├── sharding/         # Tenant-level sharding (router, proxy)
├── snapshot/         # Portable .sgsnap export/import, encryption, verification, publish gate
├── sparql/           # SPARQL 1.1 parser, algebra, optimizer, executor
├── vector/           # HNSW vector index and index manager
├── allocator.rs      # Names the allocator the server binary installs (ADR-038); does not install it
├── auth.rs           # Credentials, roles and tenant binding for the HTTP and RESP listeners
├── compat.rs         # Reports which queries of a corpus this engine accepts, and why the rest fail
├── pii.rs            # Scans a snapshot for personal identifiers before it is published
├── provenance.rs     # Where a fact came from, under what licence, and what it was derived from
├── schema_doc.rs     # Derives a schema from a snapshot and renders it as a diagram
├── lib.rs            # Library crate root — re-exports the modules above
└── main.rs           # Server binary entry point

api/               # OpenAPI 3 specification for the HTTP/REST surface (openapi.yaml)
benches/           # Criterion microbenchmarks and LDBC / FinBench / Graphalytics harnesses
benchmarks/        # Benchmark inputs and corpora (LDBC parameter files, Neo4j idiom and APOC export
                   #   fixtures, hierarchy corpus generator) — data, not code; the harnesses are in benches/
case_studies/      # End-to-end domain datasets with loaders and queries (health, legal, sport, finance, …)
cli/               # `samyama-cli` binary crate — connects to a running server via samyama-sdk
crates/            # Workspace member crates (the algorithm and optimization code lives here, not in src/)
├── samyama-graph-algorithms/  # Graph algorithms (PageRank, WCC, BFS, Dijkstra, …)
├── samyama-optimization/      # Metaheuristic optimizers (Jaya, Rao, TLBO, BMR/BWR, NSGA-II, …)
├── samyama-gpu/               # GPU-accelerated graph algorithms and vector operations
└── samyama-sdk/               # Rust client SDK — embedded and remote modes
docs/              # Architecture docs, ADRs, compatibility notes
examples/          # Runnable demos and data loaders
ops/               # Operational assets (Grafana dashboard JSON)
scripts/           # CI gates, coverage, release checks, dataset downloads
sdk/               # Python and TypeScript client SDKs (outside the cargo workspace)
tests/             # Integration tests
```

## Releasing

Releases publish three of the six workspace crates to crates.io, the
`samyama` package to PyPI, the `samyama-sdk` package to npm, and a multi-arch
Docker image to GHCR. A merge to `main` never publishes anything on its own —
CI validates, a published GitHub Release publishes. Cutting that Release is
itself two GitHub Actions runs, not a manual `git tag`.

### Versioning

Every crate shares one version, defined once in `[workspace.package]` in the
root `Cargo.toml` and inherited via `version.workspace = true`. `sdk/python`
and `sdk/typescript` are outside the cargo workspace (`sdk/python` is in the
root `exclude` list; `sdk/typescript` isn't cargo at all) so neither can
inherit — both are bumped to the same number by hand, or by
`prepare-release.yml` below.

Every publish workflow refuses to run if these numbers disagree with each
other or with the release tag. Registries do not allow a version to be
published twice, so this is checked before anything is uploaded rather than
discovered halfway through.

### Cutting a release

1. **Actions → Prepare Release → Run workflow**, enter the new version (e.g.
   `1.7.2`, no `v`). This bumps all three version sources, refreshes
   `Cargo.lock`, and opens a PR titled `release: v1.7.2`.
2. Review and merge that PR like any other — it changes nothing but version
   numbers and the lockfile.
3. The merge itself is the trigger: `tag-release.yml` sees the new,
   never-tagged version on `main`, re-checks that all three sources still
   agree, tags `v1.7.2`, and publishes a GitHub Release with generated notes.
4. That Release fires `publish-crate.yml`, `publish-pypi.yml`,
   `publish-npm.yml`, and `publish-docker.yml` — in parallel, unattended.

No one runs `git tag` or fills in the GitHub Release form by hand. If any one
publish workflow fails, rerun that workflow alone — there is no need to cut
another release, and every one of the four is safe to rerun (each skips or
`--skip-existing`s whatever already landed).

**RELEASE_PAT.** A PR opened by `prepare-release.yml`'s default token sits in
GitHub's "approval-required" state and never fires `ci.yml`'s own
`pull_request` trigger — the very check branch protection requires before
merge. Add a fine-grained PAT scoped to this repo only, with `Contents: write`
+ `Pull requests: write` and nothing else, as the `RELEASE_PAT` secret, and
that goes away. Without it the workflow still runs and the PR still opens —
someone just has to click "approve and run" on the CI check once per release.

### crates.io scope and publish order

Only `samyama-gpu`, `samyama-optimization`, and `samyama-graph-algorithms` are
published. `samyama`, `samyama-sdk`, and `samyama-cli` are deliberately not:
crates.io names can be yanked but never released back, and none of the three
had any external consumer as of this decision (0 reverse dependencies on the
two crates that do exist — see samyama-cloud#146). Revisit if a real consumer
asks for `cargo add samyama-sdk` or `cargo install samyama-cli`.

crates.io resolves every path dependency's `version` against the real index,
so a crate cannot even be *packaged* until everything it depends on is
published — optional dependencies included. `samyama-graph-algorithms` has a
path dependency on `samyama-gpu`, so gpu publishes first regardless of the
`gpu` feature being off by default:

```
samyama-gpu → samyama-optimization → samyama-graph-algorithms
```

For the same reason the dry run cannot be hoisted into a single pass over all
three: crate N only resolves once crate N-1 is genuinely on the index. Each
crate is validated immediately before it is uploaded.

### Python wheels

`sdk/python` is a PyO3 extension, so every platform needs a compiled wheel. It
builds with `abi3-py38`, so one wheel per platform serves CPython 3.8+ and the
matrix has no Python-version axis. `publish-pypi.yml` builds manylinux
x86_64/aarch64, macOS x86_64/arm64, Windows x64, plus an sdist.

The sdist is self-contained: maturin walks up to the repository root and
vendors the whole cargo workspace into it, `crates/` sources included, so
building from it does not require the crates to be on crates.io. It is around
4 MB. Installing from it still needs a Rust toolchain on the target machine,
which is why the wheel matrix matters.

### TypeScript SDK

`sdk/typescript` is pure TypeScript with no native binding, so there is no
platform matrix — one build, one package. `publish-npm.yml` builds and
publishes it as `samyama-sdk` on npm.

### Docker image

`publish-docker.yml` builds `ghcr.io/samyama-ai/samyama-graph` for
linux/amd64 and linux/arm64 and runs a real smoke test — starts the container
and executes a Cypher query through the RESP protocol — before the run is
considered successful. It only fires on a semver tag (`refs/tags/v*`), so the
data-release tags this repo also uses (`kg-snapshots-*`) don't produce a
misleading image build.

GHCR packages default to **private** on first publish. Flip
`ghcr.io/samyama-ai/samyama-graph` to public in the package settings after the
first real release, or `docker pull` fails for anyone not authenticated.

### Credentials

| Registry | Mechanism | Where |
|---|---|---|
| PyPI | Trusted Publishing (GitHub OIDC) | `pypi` environment — no stored token |
| crates.io | Trusted Publishing (GitHub OIDC via `rust-lang/crates-io-auth-action`) | `crates` environment — no stored token |
| npm | Trusted Publishing (GitHub OIDC) | `npm` environment — no stored token |
| GHCR | `GITHUB_TOKEN` (built in) | — |
| release PRs | `RELEASE_PAT` (optional; see above) | repo secret |

No registry credential is a stored, long-lived secret. That is a direct fix
for how this pipeline broke the first time: `CRATES_IO_TOKEN` went stale in
April, every release after that failed on `403 Forbidden`, and nobody
noticed for four months because nothing was watching the token's expiry.
OIDC has no expiry to miss.

Each trusted publisher is registered on the registry's side (PyPI, crates.io,
npm), naming this exact repository, workflow filename, and environment. All
three fields are matched exactly — renaming a workflow file or an environment
means re-registering it there.

### Trying it without publishing

Every publish workflow accepts `workflow_dispatch`. `publish-crate.yml` and
`publish-npm.yml` take a `dry_run` input (default on) that validates without
uploading. `publish-pypi.yml` takes a `target` input — `testpypi`, `pypi`, or
`none`. `publish-docker.yml` and `prepare-release.yml`/`tag-release.yml` are
safe to dispatch directly; the former only pushes on a real tag ref, and
cutting a release always stops at a PR for review.

Locally:

```bash
cargo package -p samyama-optimization      # any crate whose deps are published
cd sdk/python && maturin build --release   # a wheel for the current platform
cd sdk/typescript && npm publish --dry-run # validates without needing to be logged in
```


---

Thanks again for contributing to Samyama Graph! 🙏
