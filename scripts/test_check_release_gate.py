#!/usr/bin/env python3
"""The release gate must refuse for *three* reasons, not one -- and must not
refuse for a fourth that is not a reason at all.

A gate that only rejects a red verdict is not a gate. The two cases that
actually happen are the nightly having stopped (stale) and never having run
(missing), and both look exactly like "nothing is wrong" if the check only
compares a status string.

Run: python3 scripts/test_check_release_gate.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
CHECK = HERE / "check_release_gate.py"


def run(verdict: dict | None, name: str = "RELEASE-GATE.json") -> int:
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / name
        if verdict is not None:
            p.write_text(json.dumps(verdict))
        return subprocess.run(
            [sys.executable, str(CHECK), str(p)], capture_output=True, text=True
        ).returncode


def stamp(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


CASES = [
    ("a fresh pass lets the tag through", 0,
     {"status": "pass", "measured_at": stamp(0), "host": "vm-1"}),
    # The case this suite was missing, and the reason the gate never opened.
    # CH-REGRESS marks each of its 62 per-query timings `partial` -- a timing
    # has no pass/fail target -- so the run-level status is `partial` however
    # green the suite is. Requiring `pass` blocked every release for the whole
    # life of the check. A regression is `fail` by the envelope's rollup, so
    # accepting `partial` does not weaken it.
    ("a fresh partial lets the tag through", 0,
     {"status": "partial", "measured_at": stamp(0), "host": "vm-1",
      "note": "62 timings partial; no target of their own"}),
    ("a fresh fail blocks", 1,
     {"status": "fail", "measured_at": stamp(0), "note": "IC6 3.2x slower"}),
    ("a fresh error blocks", 1,
     {"status": "error", "measured_at": stamp(0), "note": "harness crashed"}),
    # Stale applies to partial exactly as it does to pass.
    ("a stale partial blocks", 1,
     {"status": "partial", "measured_at": stamp(9), "host": "vm-1"}),
    # The nightly stopped a week ago. Nothing is red; nothing has looked.
    ("a stale pass blocks", 1,
     {"status": "pass", "measured_at": stamp(9), "host": "vm-1"}),
    ("an unmeasured verdict blocks", 1,
     {"status": "unmeasured", "measured_at": stamp(0)}),
    # The nightly has never run, or the branch is gone.
    ("a missing verdict blocks", 1, None),
    ("a verdict with no measured_at blocks", 1, {"status": "pass"}),
    ("three days is inside the window", 0,
     {"status": "pass", "measured_at": stamp(3), "host": "vm-1"}),
    ("four days is not", 1,
     {"status": "pass", "measured_at": stamp(4), "host": "vm-1"}),
]

if __name__ == "__main__":
    ok = True
    for name, want, verdict in CASES:
        got = run(verdict)
        good = got == want
        ok &= good
        print(f"{'ok  ' if good else 'FAIL'} {name}"
              + ("" if good else f"  -- wanted rc={want}, got rc={got}"))
    print("\nPASS" if ok else "\nFAIL")
    sys.exit(0 if ok else 1)
