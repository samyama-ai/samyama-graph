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

**A green CH-REGRESS run reports `partial`, not `pass`**, so this accepts both.
The envelope's status is derived pessimistically (benchmarks repo,
`harness/envelope.py`):

    error      if any measurement errored
    fail       if any measurement failed        <- a real regression lands here
    partial    if any measurement is partial or unmeasured
    pass       otherwise

CH-REGRESS marks each of its 62 per-query timings `partial`, because a timing
has no pass/fail target of its own -- so the run-level status is `partial`
however green the suite is, and `pass` is unreachable. Requiring it blocked
every release from 2026-08-28 (when this check landed) until this fix: twelve
consecutive verdicts, none of them `pass`, and no tag cut in that window.

The benchmarks repo's own `harness/gate.py` documents the trap it fell into
here: *"testing for `"pass"` would be a condition that can never hold however
green the suite is ... It is an easy shape to write and an invisible one to
read, because 'not met' looks like work remaining rather than like a broken
test."*

Accepting `partial` does **not** weaken the gate against regressions: by the
rollup above, a slower query produces `fail`, which is still refused.

It does leave one narrower gap. `partial` also covers "a measurement declined
to judge", and the verdict file carries no per-measurement detail to tell that
apart from the ordinary timing case. Closing it needs the nightly to publish
that detail -- see the note at the bottom of this file.

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

    # `pass` and `partial` are both green; everything else refuses. See the
    # module docstring for why `pass` alone was unreachable.
    status = g.get("status")
    if status not in ("pass", "partial"):
        print(f"::error::CH-REGRESS is {status!r}: {g.get('note')}")
        return 1

    print(f"CH-REGRESS {status}, {age}d old, host {g.get('host')!r}, "
          f"engine {g.get('engine_commit')}")
    return 0


# FOLLOW-UP, for the benchmarks repo rather than here.
#
# `harness/nightly/publish_verdict.py` reduces the envelope to seven fields and
# keeps no per-measurement detail, so this script cannot apply the definition
# `harness/gate.py` settled on -- *green is no failed measurement and a headline
# that actually judged*. Publishing two more fields (whether any measurement
# failed, and whether the headline was withheld) would let this check refuse a
# run that declined to judge, which today it cannot see.


if __name__ == "__main__":
    sys.exit(main())
