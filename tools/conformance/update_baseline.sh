#!/usr/bin/env bash
# Rewrite the baseline from the last run. Review the diff before committing it: a
# change here is a change in what this engine computes for the ISO path patterns, and
# it is the one file in this repository where that is visible in a pull request.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
WORK="${WORK:-$ROOT/target/conformance}"
MAP="${1:-$WORK/suite/results/map.json}"
python3 - "$MAP" "$WORK/suite" "$ROOT/tools/conformance/baseline.json" <<'PY'
import datetime, json, subprocess, sys
map_path, suite_dir, out_path = sys.argv[1:4]
m = json.load(open(map_path))
ours = sorted((c for c in m["cells"] if c["engine"] == "samyama-graph"),
              key=lambda c: c["case"])
if not ours:
    sys.exit("the map has no samyama-graph row; nothing to record")
old = json.load(open(out_path))
old.update({
    "suite_commit": subprocess.run(["git", "-C", suite_dir, "rev-parse", "HEAD"],
                                   capture_output=True, text=True).stdout.strip(),
    "engine_version": next(e["version"] for e in m["engines"]
                           if e["name"] == "samyama-graph"),
    "recorded": datetime.date.today().isoformat(),
    "verdicts": {c["case"]: c["verdict"] for c in ours},
})
json.dump(old, open(out_path, "w"), indent=2, sort_keys=True)
print(f"wrote {out_path}: {len(old['verdicts'])} constructs")
PY
