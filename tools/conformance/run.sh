#!/usr/bin/env bash
# Run the GPML conformance suite against this checkout and compare to the baseline.
#
# The suite lives in its own repository and is pinned by commit. It stays external on
# purpose: it is the yardstick, and vendoring it into the repository it measures would
# let the two drift together until the measurement agrees with the engine by
# construction.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SUITE_REPO="${SUITE_REPO:-https://github.com/samyama-ai/gpml-conformance.git}"
SUITE_COMMIT="${SUITE_COMMIT:-$(python3 -c "import json;print(json.load(open('$ROOT/tools/conformance/baseline.json'))['suite_commit'])")}"
WORK="${WORK:-$ROOT/target/conformance}"
SAMYAMA_BIN="${SAMYAMA_BIN:-$ROOT/target/release/samyama}"

echo "== suite $SUITE_COMMIT"
mkdir -p "$WORK"
if [ ! -d "$WORK/suite/.git" ]; then
  git clone -q "$SUITE_REPO" "$WORK/suite"
fi
git -C "$WORK/suite" fetch -q origin
git -C "$WORK/suite" checkout -q "$SUITE_COMMIT"

if [ ! -x "$SAMYAMA_BIN" ]; then
  echo "== building the engine"
  (cd "$ROOT" && cargo build --release --bin samyama)
fi

echo "== python environment"
if [ ! -d "$WORK/venv" ]; then
  python3 -m venv "$WORK/venv"
  "$WORK/venv/bin/pip" install -q requests
fi

echo "== gate B: the reference must reproduce the standard's published answers"
"$WORK/venv/bin/python" -m pytest "$WORK/suite/tests/test_gate_b_paper_examples.py" -q \
  2>/dev/null || "$WORK/venv/bin/pip" install -q pytest && \
  "$WORK/venv/bin/python" -m pytest "$WORK/suite/tests/test_gate_b_paper_examples.py" -q

echo "== gate A: the adapter must transport the graph"
( cd "$WORK/suite" && SAMYAMA_BIN="$SAMYAMA_BIN" CF_WORKDIR="$WORK/data" \
    "$WORK/venv/bin/python" tools/verify_adapter.py samyama-graph )

echo "== the map"
# Every other engine is simply unreachable here, and the runner records that rather
# than failing: this job measures one engine, and the check below refuses a map with
# no samyama-graph row in it.
( cd "$WORK/suite" && SAMYAMA_BIN="$SAMYAMA_BIN" CF_WORKDIR="$WORK/data" \
    "$WORK/venv/bin/python" src/run_map.py )

echo "== against the baseline"
python3 "$ROOT/tools/conformance/check.py" "$WORK/suite/results/map.json"
