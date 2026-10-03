#!/usr/bin/env python3
"""Compare a conformance run against the committed baseline.

The baseline is the reviewable artifact. A diff in it is a semantics change, and the
point of this check is that such a change has to be *reviewed* rather than discovered
later by someone running the suite by hand -- which is how #1140 was found, and what
#1142 existed to prevent.

Both directions fail, for different reasons and with different messages:

  worse   a cell that conformed and no longer does, or that was refused and is now
          answered wrongly. The regression this gate exists for.
  better  a cell that improved. Not a problem, and still a failure: an improvement
          nobody recorded means the baseline is no longer a description of the engine,
          and the next real regression hides behind the stale line.

Exit 0 only when every cell matches. Usage:
    python3 tools/conformance/check.py <map.json> [baseline.json]
"""
from __future__ import annotations

import json
import os
import sys

# How good a verdict is, for deciding which direction a cell moved. INEXPRESSIBLE is
# not on this scale -- it means the dialect cannot state the construct, which is a fact
# about the language rather than a grade -- so a move into or out of it is reported as
# a change without a direction.
RANK = {"DIVERGES": 0, "ENGINE_UNAVAILABLE": 0, "LOAD_FAILED": 0,
        "NONDETERMINISTIC": 0, "REJECTS": 1, "CONFORMS": 2}
ENGINE = "samyama-graph"


def main(argv: list[str]) -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    map_path = argv[1] if len(argv) > 1 else "results/map.json"
    base_path = argv[2] if len(argv) > 2 else os.path.join(here, "baseline.json")

    base = json.load(open(base_path))
    got = json.load(open(map_path))
    cells = {c["case"]: c for c in got["cells"] if c["engine"] == ENGINE}
    if not cells:
        print(f"FAIL: the map has no {ENGINE} row. The engine did not start, or the "
              f"adapter could not reach it; either way nothing was measured and a "
              f"green result here would mean nothing.", file=sys.stderr)
        return 1

    want = base["verdicts"]
    worse, better, changed, missing, added = [], [], [], [], []

    for case, expected in sorted(want.items()):
        cell = cells.get(case)
        if cell is None:
            missing.append(case)
            continue
        actual = cell["verdict"]
        if actual == expected:
            continue
        a, b = RANK.get(actual), RANK.get(expected)
        if a is None or b is None:
            changed.append((case, expected, actual, cell.get("detail", "")))
        elif a < b:
            worse.append((case, expected, actual, cell.get("detail", "")))
        else:
            better.append((case, expected, actual))

    added = sorted(set(cells) - set(want))

    for case, exp, act, detail in worse:
        print(f"WORSE    {case}: {exp} -> {act}")
        if detail:
            print(f"         {str(detail)[:160]}")
    for case, exp, act in better:
        print(f"BETTER   {case}: {exp} -> {act}")
    for case, exp, act, _ in changed:
        print(f"CHANGED  {case}: {exp} -> {act}  (no ordering between these)")
    for case in missing:
        print(f"MISSING  {case}: in the baseline, not in this run")
    for case in added:
        print(f"NEW      {case}: in this run, not in the baseline")

    total = len(worse) + len(better) + len(changed) + len(missing) + len(added)
    if not total:
        print(f"ok: {len(want)} constructs match the baseline "
              f"(suite {base['suite_commit'][:8]}, recorded {base['recorded']})")
        return 0

    print()
    if worse:
        print(f"{len(worse)} cell(s) got worse. That is a semantics regression.")
    if better or changed or missing or added:
        print("Cells also moved in ways that are not regressions. They still fail: a "
              "baseline that no longer describes the engine cannot catch the next "
              "regression, because the stale line hides it.")
    print("If the change is intended, review the diff and run "
          "tools/conformance/update_baseline.sh.")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
