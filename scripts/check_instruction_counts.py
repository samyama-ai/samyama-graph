#!/usr/bin/env python3
"""PERF-18 instruction-count gate (#2207).

    cargo bench --bench instruction_counts -- --save-summary=json
    python3 scripts/check_instruction_counts.py              # compare
    python3 scripts/check_instruction_counts.py --update     # accept the new counts

Reads gungraun's summary.json files under target/gungraun and compares each
benchmark's instruction count (Callgrind `Ir`) with
benches/instruction_counts.json. Fails when:

- a count grows more than `tolerance_pct` (3%) over the baseline;
- a benchmark in the baseline did not run, or one ran that the baseline does
  not list -- a renamed benchmark must not pass as a removed one;
- the Rust or Valgrind version differs from the one the baseline was taken
  with. A new compiler changes the code it emits, so the counts move without a
  change to this repository; the CI job pins both, and moving either means
  re-running with --update in the same pull request.

A count that falls more than the tolerance is reported, not failed: the
improvement is real, and lowering the baseline (--update) is what stops the
next pull request from spending it.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / "benches/instruction_counts.json"


def tool_version(cmd: list[str]) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as e:
        sys.exit(f"FAIL: cannot run {cmd[0]}: {e}")


def measured(summaries: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    for f in sorted(summaries.rglob("summary.json")):
        d = json.loads(f.read_text())
        if "instruction_counts" not in d.get("module_path", ""):
            continue
        ir = [p["data"]["total"]["metrics"]["Ir"]["values"]["new"]
              for p in d.get("profiles", []) if p.get("tool") == "Callgrind"]
        if len(ir) != 1:
            sys.exit(f"FAIL: {f} has {len(ir)} Callgrind profiles, expected one")
        out[f"{d['function_name']}/{d['id']}"] = int(ir[0])
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--summaries", default=os.environ.get("GUNGRAUN_HOME",
                    str(Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")) / "gungraun")))
    ap.add_argument("--update", action="store_true")
    args = ap.parse_args()

    counts = measured(Path(args.summaries))
    if not counts:
        sys.exit(f"FAIL: no instruction_counts summaries under {args.summaries}; "
                 "run `cargo bench --bench instruction_counts -- --save-summary=json` first")
    rustc = tool_version(["rustc", "--version"])
    valgrind = tool_version(["valgrind", "--version"])

    if args.update:
        old = json.loads(BASELINE.read_text()) if BASELINE.exists() else {}
        BASELINE.write_text(json.dumps({
            "rustc": rustc, "valgrind": valgrind,
            "tolerance_pct": old.get("tolerance_pct", 3),
            "counts": dict(sorted(counts.items())),
        }, indent=2) + "\n")
        print(f"wrote {BASELINE.relative_to(ROOT)}: {len(counts)} benchmarks, {rustc}, {valgrind}")
        return 0

    base = json.loads(BASELINE.read_text())
    tol = base["tolerance_pct"]
    problems: list[str] = []
    for what, have, want in (("rustc", rustc, base["rustc"]), ("valgrind", valgrind, base["valgrind"])):
        if have != want:
            problems.append(f"{what} is `{have}`, the baseline was taken with `{want}`: "
                            "counts are only comparable under the same toolchain (re-run with --update)")
    for name in sorted(set(base["counts"]) - set(counts)):
        problems.append(f"{name}: in the baseline but did not run")
    for name in sorted(set(counts) - set(base["counts"])):
        problems.append(f"{name}: ran but is not in the baseline (re-run with --update)")

    print(f"{'benchmark':40s} {'baseline':>14s} {'now':>14s} {'change':>8s}")
    improved = []
    for name in sorted(set(counts) & set(base["counts"])):
        was, now = base["counts"][name], counts[name]
        pct = (now - was) / was * 100
        print(f"{name:40s} {was:>14,d} {now:>14,d} {pct:>+7.2f}%")
        if pct > tol:
            problems.append(f"{name}: {now:,d} instructions, {pct:+.2f}% over the baseline {was:,d} "
                            f"(limit +{tol}%)")
        elif pct < -tol:
            improved.append(f"{name} ({pct:+.2f}%)")
    if improved:
        print(f"NOTE  fewer instructions than the baseline by more than {tol}%: {', '.join(improved)}. "
              "Run with --update to lower the baseline so the gain is kept.")
    if problems:
        print("FAIL  PERF-18 instruction counts:\n  " + "\n  ".join(problems))
        return 1
    print(f"PASS  PERF-18 instruction counts: {len(counts)} benchmarks within +{tol}% of the baseline")
    return 0


if __name__ == "__main__":
    sys.exit(main())
