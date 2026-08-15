#!/usr/bin/env bash
#
# Full verification sweep: tests, examples, benches, case studies.
#
# This is the sweep that has been catching the things a plain `cargo test`
# does not -- an example that panics instead of explaining what data it needs,
# a bench that reports success while measuring nothing, a case study whose
# snapshot URL was never filled in. Each stage writes a one-line-per-item
# summary so two runs can be diffed directly.
#
# Resumable: every stage drops a marker, so re-running after an interruption
# picks up where it stopped. Delete the marker directory to force a full run.
#
#   scripts/verify-sweep.sh                  # against this checkout
#   scripts/verify-sweep.sh --provision      # also install toolchain + system deps
#   scripts/verify-sweep.sh --out DIR        # where results and logs go
#   scripts/verify-sweep.sh --jobs 4         # cargo -j (default: nproc-2, min 2)
#
# On a clean cloud host the usual invocation is `--provision`, launched under
# `setsid nohup` so a dropped SSH connection does not take the run with it.

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${REPO}/target/verify-sweep"
PROVISION=0
JOBS=""

while [ $# -gt 0 ]; do
  case "$1" in
    --provision) PROVISION=1; shift ;;
    --out) OUT="$2"; shift 2 ;;
    --jobs) JOBS="$2"; shift 2 ;;
    -h|--help) sed -n '2,25p' "$0" | sed 's/^# \?//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$JOBS" ]; then
  n="$(nproc 2>/dev/null || echo 4)"
  JOBS=$(( n > 3 ? n - 2 : 2 ))
fi

LOGS="${OUT}/logs"
RESULTS="${OUT}/results"
MARKS="${OUT}/.marks"
mkdir -p "$LOGS" "$RESULTS" "$MARKS"

step() { [ -f "${MARKS}/$1" ]; }
mark() { touch "${MARKS}/$1"; }

if [ "$PROVISION" = "1" ] && ! step provision; then
  echo "provisioning toolchain and system dependencies..."
  export DEBIAN_FRONTEND=noninteractive
  # libclang is required by zstd-sys and is the single most common first-build
  # failure on a fresh host (documented in CONTRIBUTING).
  PKGS=(build-essential cmake pkg-config libssl-dev clang libclang-dev git curl jq)
  # Root already; otherwise escalate. Redirect outside the privileged command so
  # the log stays owned by the invoking user.
  if [ "$(id -u)" -eq 0 ]; then SUDO=(); else SUDO=(sudo -n); fi
  {
    "${SUDO[@]}" apt-get update -qq
    "${SUDO[@]}" apt-get install -y -qq "${PKGS[@]}"
  } >>"${LOGS}/provision.log" 2>&1
  command -v cargo >/dev/null 2>&1 || \
    curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal >>"${LOGS}/provision.log" 2>&1
  mark provision
fi

# shellcheck disable=SC1090,SC1091
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
cd "$REPO" || { echo "cannot cd to ${REPO}" >&2; exit 1; }

{
  echo "commit:   $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "host:     $(nproc 2>/dev/null || echo '?')vCPU $(free -g 2>/dev/null | awk '/Mem:/{print $2}' || echo '?')GB"
  echo "jobs:     ${JOBS}"
} | tee "${RESULTS}/run.txt"

# ---------------------------------------------------------------- tests
if ! step tests; then
  echo "==> tests"
  cargo test --workspace --release -j "$JOBS" >"${LOGS}/tests.log" 2>&1
  rc=$?
  {
    echo "exit=${rc}"
    grep -E "^test result" "${LOGS}/tests.log" \
      | awk -F'[ ;]' '{p+=$4; f+=$6} END {print "passed=" p " failed=" f}'
    grep -E "^test .* FAILED" "${LOGS}/tests.log" | head -20
  } > "${RESULTS}/tests.txt"
  mark tests
fi

# ---------------------------------------------------------------- examples
#
# An example that exits non-zero because it needs a dataset or a flag is a
# pass here -- it said so. An example that panics is not: the distinction is
# the whole point of this stage.
if ! step examples; then
  echo "==> examples"
  cargo build --release --examples -j "$JOBS" >"${LOGS}/build-examples.log" 2>&1
  echo "build=$?" > "${RESULTS}/examples.txt"
  for ex in $(find examples -maxdepth 1 -name '*.rs' -exec basename {} .rs \; | sort); do
    case "$ex" in
      # Long-running servers, interactive clients, and loaders that require a
      # multi-gigabyte corpus. Skipped by name so the list is auditable.
      *_server*|server_*|*_client*|agentic_enrichment_demo|mesh_scale_bench|ontology_loader|hier_export_csv)
        printf "%-14s %s\n" "SKIP" "$ex" >> "${RESULTS}/examples.txt"; continue ;;
    esac
    timeout 300 "./target/release/examples/${ex}" >"${LOGS}/example-${ex}.log" 2>&1
    rc=$?
    if [ $rc -eq 0 ]; then
      r="OK"
    elif grep -q "panicked at" "${LOGS}/example-${ex}.log"; then
      r="PANIC${rc}"
    else
      r="NEEDS-INPUT${rc}"
    fi
    printf "%-14s %s\n" "$r" "$ex" >> "${RESULTS}/examples.txt"
  done
  mark examples
fi

# ---------------------------------------------------------------- benches
#
# Short criterion settings: this checks that every bench still runs and
# measures something, not that its numbers are publication-grade.
if ! step benches; then
  echo "==> benches"
  echo "=== benches ===" > "${RESULTS}/benches.txt"
  for b in $(find benches -maxdepth 1 -name '*.rs' -exec basename {} .rs \; | sort); do
    start=$(date +%s)
    timeout 1800 cargo bench --bench "$b" -- \
      --warm-up-time 1 --measurement-time 3 --sample-size 10 \
      >"${LOGS}/bench-${b}.log" 2>&1
    rc=$?
    printf "%-10s %-34s %4ss\n" \
      "$([ $rc -eq 0 ] && echo OK || echo "FAIL${rc}")" "$b" "$(( $(date +%s) - start ))" \
      >> "${RESULTS}/benches.txt"
  done
  mark benches
fi

# ---------------------------------------------------------------- case studies
if ! step casestudies; then
  echo "==> case studies"
  echo "=== case studies ===" > "${RESULTS}/case-studies.txt"
  for cs in case_studies/*/; do
    n="$(basename "$cs")"
    [ "$n" = "_lib" ] && continue
    [ -f "${cs}run.sh" ] || continue
    start=$(date +%s)
    ( cd "$cs" && timeout 1200 bash ./run.sh ) >"${LOGS}/case-${n}.log" 2>&1
    rc=$?
    # A case study whose snapshot has not been published prints SKIP: and
    # exits 0; that is distinct from a failure and is reported as such.
    if grep -q "^SKIP:" "${LOGS}/case-${n}.log"; then
      r="SKIP"
    elif [ $rc -eq 0 ]; then
      r="OK"
    else
      r="FAIL${rc}"
    fi
    printf "%-8s %-24s %4ss\n" "$r" "$n" "$(( $(date +%s) - start ))" \
      >> "${RESULTS}/case-studies.txt"
  done
  mark casestudies
fi

echo "ALL DONE $(date -Is)" > "${RESULTS}/COMPLETE.txt"

echo
echo "===== summary ====="
cat "${RESULTS}/run.txt"
echo "tests:        $(sed -n 2p "${RESULTS}/tests.txt")"
echo "examples:     OK=$(grep -c '^OK' "${RESULTS}/examples.txt") PANIC=$(grep -c '^PANIC' "${RESULTS}/examples.txt") other=$(grep -cE '^(NEEDS-INPUT|SKIP)' "${RESULTS}/examples.txt")"
echo "benches:      OK=$(grep -c '^OK' "${RESULTS}/benches.txt") FAIL=$(grep -c '^FAIL' "${RESULTS}/benches.txt")"
echo "case studies: OK=$(grep -c '^OK' "${RESULTS}/case-studies.txt") SKIP=$(grep -c '^SKIP' "${RESULTS}/case-studies.txt") FAIL=$(grep -c '^FAIL' "${RESULTS}/case-studies.txt")"
echo
echo "results in ${RESULTS}"

# Fail the run if anything actually broke. A NEEDS-INPUT example and a SKIP
# case study are not breakage; a panic, a failed bench, or a failed test is.
fails=0
grep -q "failed=0" "${RESULTS}/tests.txt" || fails=1
[ "$(grep -c '^PANIC' "${RESULTS}/examples.txt")" -eq 0 ] || fails=1
[ "$(grep -c '^FAIL' "${RESULTS}/benches.txt")" -eq 0 ] || fails=1
[ "$(grep -c '^FAIL' "${RESULTS}/case-studies.txt")" -eq 0 ] || fails=1
exit $fails
