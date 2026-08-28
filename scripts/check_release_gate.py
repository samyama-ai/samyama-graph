#!/usr/bin/env python3
"""Refuse to cut a release tag unless CH-REGRESS vouched for this build.

spec 19 §2 condition 3: CH-REGRESS green on its own nightly bare-metal run
**and blocking the release tag**. The evidence lives in
`samyama-graph-competitor-benchmarks`, which the tag workflow cannot read, so
the nightly on the fixed host publishes its verdict to this repo's
`release-gate` branch and this script reads it from there.

A verdict is a fact about a run, not about a commit, which is why it lives on
its own branch rather than on main: otherwise every nightly would be a
source-code change.

**Missing and stale block the tag exactly as red does.** "We could not tell" is
not "it is fine" -- treating those as the same thing is the failure the whole
harness is built to avoid, and a release is the last place to start.

    python3 scripts/check_release_gate.py RELEASE-GATE.json [--max-age-days 3]
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("verdict", type=Path)
    ap.add_argument("--max-age-days", type=int, default=3,
                    help="older than this and the nightly is not running")
    a = ap.parse_args()

    if not a.verdict.exists():
        print(f"::error::no {a.verdict}. The nightly "
              f"(harness/nightly/ch-regress-nightly.sh in the benchmarks repo) "
              f"publishes CH-REGRESS's verdict to the release-gate branch; "
              f"without it nothing has vouched for this build's latency.")
        return 1

    try:
        g = json.loads(a.verdict.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"::error::{a.verdict} is not readable JSON: {e}")
        return 1

    try:
        measured = datetime.fromisoformat(str(g["measured_at"]).replace("Z", "+00:00"))
    except (KeyError, ValueError) as e:
        print(f"::error::{a.verdict} has no usable measured_at: {e}")
        return 1

    age = (datetime.now(timezone.utc) - measured).days
    if age > a.max_age_days:
        print(f"::error::the CH-REGRESS verdict is {age} days old "
              f"(limit {a.max_age_days}) — the nightly has not run. "
              f"A gate with gaps nobody notices is not a gate.")
        return 1

    if g.get("status") != "pass":
        print(f"::error::CH-REGRESS is {g.get('status')!r}: {g.get('note')}")
        return 1

    print(f"CH-REGRESS pass, {age}d old, host {g.get('host')!r}, "
          f"engine {g.get('engine_commit')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
