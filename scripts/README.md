# Repository scripts

This directory holds the checks used by CI and release workflows, local
verification tools, and dataset helpers. The automated scripts are called from
`.github/workflows/ci.yml`, `.github/workflows/nightly.yml`, and
`.github/workflows/tag-release.yml`.

## Automated checks

| Script | What it checks | Typical invocation |
|---|---|---|
| `api_stability.py` | Replays the recorded HTTP request/response contract and fails if it changes. CI runs `--check`; changing the contract requires an intentional re-record. | `python3 scripts/api_stability.py --check` |
| `check_dockerfile_rust_pin.py` | Checks that the Rust version pinned by the Dockerfile satisfies the resolved dependencies. | `python3 scripts/check_dockerfile_rust_pin.py` |
| `check_lint_ratchet.sh` | Prevents the Clippy warning count from exceeding its configured ceiling. | `scripts/check_lint_ratchet.sh 796` |
| `check_thread_determinism.sh` | Compares openCypher TCK results across thread counts. CI supplies the TCK feature directory. | `scripts/check_thread_determinism.sh /path/to/tck/features` |
| `test_check_release_gate.py` | Tests the release-gate validator. | `python3 scripts/test_check_release_gate.py` |
| `check_release_gate.py` | Blocks a release tag when its CH-REGRESS verdict is missing, stale, red, or does not cover the commit being tagged. | `python3 scripts/check_release_gate.py RELEASE-GATE.json --commit SHA` |
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
