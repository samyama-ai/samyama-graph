#!/usr/bin/env bash
# LANG-14: the same query on the same data returns identical results at any
# thread count (#611).
#
# CH-DETERM varies the *process*, which catches a `RandomState` seed leaking
# into results. It says nothing about a parallel operator emitting rows in a
# different order at 8 threads than at 1 — which is the failure this checks,
# and the one that matters as morsel-driven parallel expand lands under PERF-06.
#
# Compares full failure manifests as sets. A scenario that passes at 1 thread
# and fails at 32 changes the manifest, and any difference fails the build.
#
# Writes `summary.json` into the out-dir so the harness can report LANG-14 from the
# same run CI gates on, rather than asserting it a second way and hoping the two agree.
#
#   usage: check_thread_determinism.sh <path to tck/features> [out-dir]
set -euo pipefail

FEATURES="${1:?usage: $0 <tck/features> [out-dir]}"
OUT="${2:-$(mktemp -d)}"
THREAD_COUNTS=(1 8 32)

mkdir -p "$OUT"
for t in "${THREAD_COUNTS[@]}"; do
  RAYON_NUM_THREADS="$t" cargo run --release --quiet --example tck_runner -- \
    --features "$FEATURES" --failures-manifest "$OUT/manifest-$t.txt" >/dev/null
  if [ ! -s "$OUT/manifest-$t.txt" ]; then
    echo "FAIL: no manifest produced at $t threads — the run did not complete"
    exit 1
  fi
done

base="${THREAD_COUNTS[0]}"
status=0
for t in "${THREAD_COUNTS[@]:1}"; do
  if ! diff -u "$OUT/manifest-$base.txt" "$OUT/manifest-$t.txt" > "$OUT/diff-$base-$t.txt"; then
    echo "FAIL: results differ between $base and $t threads."
    echo "  A query's answer must not depend on how many threads ran it."
    head -40 "$OUT/diff-$base-$t.txt"
    status=1
  fi
done

entries=$(wc -l < "$OUT/manifest-$base.txt" | tr -d ' ')
counts=""
for t in "${THREAD_COUNTS[@]}"; do
  counts="$counts${counts:+,}{\"threads\":$t,\"manifest_entries\":$(wc -l < "$OUT/manifest-$t.txt" | tr -d ' ')}"
done
cat > "$OUT/summary.json" <<JSON
{
  "thread_counts": [$(IFS=,; echo "${THREAD_COUNTS[*]}")],
  "identical": $([ "$status" -eq 0 ] && echo true || echo false),
  "manifest_entries": $entries,
  "runs": [$counts]
}
JSON

if [ "$status" -eq 0 ]; then
  echo "OK: identical failure manifests at ${THREAD_COUNTS[*]} threads ($entries entries)"
fi
echo "summary: $OUT/summary.json"
exit "$status"
