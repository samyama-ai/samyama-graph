# Samyama Scripts

Utility scripts for verification, data downloads, and testing.

## Verification

### `verify-sweep.sh`

Runs the full sweep — **tests, examples, benches, case studies** — and writes a
one-line-per-item summary of each, so two runs can be diffed directly.

```sh
scripts/verify-sweep.sh                 # against this checkout
scripts/verify-sweep.sh --provision     # also install toolchain + system deps
scripts/verify-sweep.sh --out DIR       # where results and logs go
scripts/verify-sweep.sh --jobs 4        # cargo -j (default: nproc-2)
```

This catches the classes a plain `cargo test` does not: an example that panics
instead of saying what data it needs, a bench that reports success while
measuring nothing, a case study whose snapshot URL was never filled in.

It distinguishes **breakage from absence**, and the exit code follows that
distinction — an example exiting non-zero because it needs a dataset
(`NEEDS-INPUT`) and a case study whose snapshot is unpublished (`SKIP`) are not
failures. A panic, a failed bench, or a failed test is.

Resumable: each stage drops a marker, so a run interrupted partway resumes
where it stopped. On a cloud host, launch it under `setsid nohup` so a dropped
SSH connection does not take the run with it.


## Requirements

The Python scripts here use only the standard library, so there is nothing to
install beyond Python 3.
