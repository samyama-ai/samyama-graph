# Repository scripts

This directory holds the checks used by CI and release workflows, local
verification tools, and dataset helpers. The automated scripts are called from
`.github/workflows/ci.yml`, `.github/workflows/nightly.yml`,
`.github/workflows/tag-release.yml`, and `.github/workflows/triage-sla.yml`.

## Automated checks

| Script | What it checks | Typical invocation |
|---|---|---|
| `api_stability.py` | Replays the recorded HTTP request/response contract and fails if it changes. CI runs `--check`; changing the contract requires an intentional re-record. | `python3 scripts/api_stability.py --check` |
| `check_dockerfile_rust_pin.py` | Checks that the Rust version pinned by the Dockerfile satisfies the resolved dependencies. | `python3 scripts/check_dockerfile_rust_pin.py` |
| `check_lint_ratchet.sh` | Prevents the Clippy warning count from exceeding its configured ceiling. | `scripts/check_lint_ratchet.sh 796` |
| `check_thread_determinism.sh` | Compares openCypher TCK results across thread counts. CI supplies the TCK feature directory. | `scripts/check_thread_determinism.sh /path/to/tck/features` |
| `snapshot_property_audit.py` | Reports, for every node label and edge type in a `.sgsnap`, how many records carry each property, how many are empty and how many distinct values there are, and flags columns that are always empty, constant, or hold only a few values (#1815). It reads the file directly, needs no engine, and exits 2 on a snapshot it cannot fully read (truncated, unknown record type, missing fields). | `python3 scripts/snapshot_property_audit.py football.sgsnap --flagged-only` |
| `test_snapshot_property_audit.py` | Tests the property audit against snapshots built to have the defects it exists to find. | `python3 scripts/test_snapshot_property_audit.py` |
| `test_check_release_gate.py` | Tests the release-gate validator. | `python3 scripts/test_check_release_gate.py` |
| `check_release_gate.py` | Blocks a release tag when its CH-REGRESS verdict is missing, stale, red, or does not cover the commit being tagged. | `python3 scripts/check_release_gate.py RELEASE-GATE.json --commit SHA` |
| `triage_sla.py` | Reports the first-response time of every issue opened from outside the maintainer team, and fails if one opened since `SUPPORT.md` was published waited more than 2 business days. `triage-sla.yml` runs it on weekdays; it needs a token. | `GITHUB_TOKEN=$(gh auth token) python3 scripts/triage_sla.py` |
| `test_triage_sla.py` | Tests the triage-SLA rules offline: business days, explained responses, and delayed policy publication through a merge. | `python3 scripts/test_triage_sla.py` |
| `verify-sweep.sh` | Runs tests, examples, benchmarks, and case studies, recording a result for each. The nightly workflow runs it. | `scripts/verify-sweep.sh --only tests` |

The full sweep accepts `--provision`, `--out DIR`, `--jobs N`, and repeatable
`--only` stages (`tests`, `examples`, `benches`, `casestudies`). See
`scripts/verify-sweep.sh --help` for the current options. It treats an example
that reports missing input and an unpublished case-study snapshot as skips;
panics and failed checks remain failures.

The API corpus lives at
[`tests/fixtures/api_corpus/corpus.json`](../tests/fixtures/api_corpus/corpus.json).
To update it, use `--record --i-am-changing-the-contract` and include the
contract change in the commit message.

## Local verification

### `coverage.sh`

Runs workspace tests under `cargo-llvm-cov` (excluding `samyama-python`) and
writes reports under `coverage/`. If `cargo-llvm-cov` is missing, the script
installs it; it also requests the `llvm-tools-preview` Rust component.

```sh
scripts/coverage.sh              # HTML report; opens it on macOS
scripts/coverage.sh --summary    # terminal summary
scripts/coverage.sh --json       # coverage/coverage.json
scripts/coverage.sh --lcov       # coverage/lcov.info
```

### Other local tools

| Script | What it does | Typical invocation |
|---|---|---|
| `regression-test.sh` | Runs the broader regression sequence: tests, feature checks, example and benchmark builds, GPU smoke runs where available, Clippy, and formatting. `--quick` skips the longer stages. | `scripts/regression-test.sh --quick` |
| `crash_consistency.py` | Force-kills the server during writes, restarts it, and reports whether acknowledged data survived consistently. This is a process-level probe, not an in-process unit test. | `python3 scripts/crash_consistency.py --cycles 50 --json out.json` |
| `run_all_examples.sh` | Starts a local server and runs the example demos interactively or with `--batch`; `--port N` changes the server port. It clears its named demo data directories between examples. | `scripts/run_all_examples.sh --batch` |

### Live comparisons

| Script | What it does | Typical invocation |
|---|---|---|
| `falkordb_corpus_run.py` | Runs `benchmarks/compat/falkordb-idioms.cypher` on a FalkorDB and a Samyama server and reports how many idioms run on each, with every failure's error. Produces the "actually runs" figure in `docs/MIGRATING-FROM-FALKORDB.md`. Needs Docker and a fresh dedicated `--ephemeral` Samyama server for each run; existing data or schema is refused before setup. The docstring has the server commands. | `python3 scripts/falkordb_corpus_run.py --falkordb 6391 --samyama 6392 --import-dir /tmp/fk-import` |

## Dataset helpers

| Script | Data |
|---|---|
| `download_ldbc_snb.sh` | Downloads and extracts LDBC SNB Interactive SF1 data used by the loader and SNB benchmarks. |
| `download_finbench.sh` | Generates the synthetic LDBC FinBench files through the FinBench loader. |
| `download_graphalytics.sh` | Downloads LDBC Graphalytics datasets; defaults to XS, with `--size S` or `--size all` for larger datasets. The S-size archives require `zstd`. |

## Requirements

The shell scripts require Bash. Python tools use the Python 3 standard library;
checks that build or run Samyama need the repository's Rust toolchain. Dataset
helpers need network access; the Graphalytics S-size download also needs
`zstd`. Coverage installs `cargo-llvm-cov` if needed.

Corpus runner preflight regressions: `python3 -m unittest discover -s scripts -p test_falkordb_corpus_run.py`.

The `triage SLA role lookup (workflow token)` CI job also checks the role API
with the same read permissions as the scheduled SLA workflow. Its read-only
fixtures default to maintainer `Raghuvarma905` and outside user `octocat`.
If their roles change, update repository variables `TRIAGE_SLA_MAINTAINER` and
`TRIAGE_SLA_OUTSIDE_USER`. The token stays in the environment and is never printed.
