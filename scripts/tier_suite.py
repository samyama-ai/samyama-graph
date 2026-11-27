#!/usr/bin/env python3
"""Run one hardware tier's test suite, as docs/HARDWARE-TIERS.md defines it (HW-01, #1676).

    python3 scripts/tier_suite.py --list
    python3 scripts/tier_suite.py --tier edge --dry-run
    python3 scripts/tier_suite.py --tier edge
    python3 scripts/tier_suite.py --tier workstation --with-optional

The tier's tests are read from the feature manifest in docs/HARDWARE-TIERS.md,
the table between the `tier-manifest` markers. There is no second copy here: a
copy would go stale, and a runner that agreed with its own copy would pass a
tier the docs had since changed.

Exit codes:

* 0: every required test of the tier passed (and every optional one, with
  `--with-optional`);
* 1: a test failed, or the manifest could not be read;
* 2: the tier has nothing to run, so a job for it cannot pass by testing nothing.

Capabilities the manifest marks `gap` are printed on every run. A green run
means the required tests passed, not that the tier is complete.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOC = ROOT / "docs" / "HARDWARE-TIERS.md"
TIERS = ("server", "workstation", "edge", "micro", "browser")
STATUSES = {"required", "optional", "unsupported", "gap", "—"}
START, END = "<!-- tier-manifest:start -->", "<!-- tier-manifest:end -->"
# `name` is tests/name.rs; `crate+feature::filter` is a test in another crate.
CRATE_TEST = re.compile(r"^(?P<crate>[a-z0-9_-]+)(?:\+(?P<features>[a-z0-9_,-]+))?::(?P<filter>\S+)$")
ROOT_TEST = re.compile(r"^[a-z0-9_]+$")


@dataclass
class Capability:
    name: str
    tests: list[str]
    status: dict[str, str]
    note: str


@dataclass
class Plan:
    tier: str
    commands: list[list[str]] = field(default_factory=list)
    env: dict[str, str] = field(default_factory=dict)
    run: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    gaps: list[str] = field(default_factory=list)
    unsupported: list[str] = field(default_factory=list)


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def parse_manifest(text: str) -> list[Capability]:
    """The capabilities in the manifest table. Raises ValueError on a malformed table."""
    if START not in text or END not in text:
        raise ValueError(f"no {START} ... {END} block")
    rows = [l for l in text.split(START, 1)[1].split(END, 1)[0].splitlines() if l.strip().startswith("|")]
    if len(rows) < 3:
        raise ValueError("the manifest table has no rows")
    header = _cells(rows[0])
    want = ["Capability", "Tests"] + [f"{t.capitalize()} tier" for t in TIERS] + ["Note"]
    if header != want:
        raise ValueError(f"manifest header is {header}, expected {want}")
    caps = []
    for line in rows[2:]:
        cells = _cells(line)
        if len(cells) != len(want):
            raise ValueError(f"row has {len(cells)} cells, expected {len(want)}: {line}")
        name, tests_cell, *statuses, note = cells
        tests = re.findall(r"`([^`]+)`", tests_cell)
        status = dict(zip(TIERS, statuses))
        for tier, s in status.items():
            if s not in STATUSES:
                raise ValueError(f"{name}: {tier} has status {s!r}, not one of {sorted(STATUSES)}")
        for t in tests:
            if not (ROOT_TEST.match(t) or CRATE_TEST.match(t)):
                raise ValueError(f"{name}: test {t!r} is neither `name` nor `crate+feature::filter`")
        caps.append(Capability(name, tests, status, note))
    return caps


def plan(caps: list[Capability], tier: str, with_optional: bool) -> Plan:
    """The cargo commands that run `tier`'s suite, without running them."""
    p = Plan(tier)
    root_tests: list[str] = []
    crate_runs: list[tuple[str, str | None, str]] = []
    for c in caps:
        s = c.status[tier]
        if s == "gap":
            p.gaps.append(f"{c.name}: {c.note}" if c.note else c.name)
        elif s == "unsupported":
            p.unsupported.append(c.name)
        if s == "required" or (s == "optional" and with_optional):
            p.run.append(c.name)
            for t in c.tests:
                m = CRATE_TEST.match(t)
                if m:
                    key = (m["crate"], m["features"], m["filter"])
                    if key not in crate_runs:
                        crate_runs.append(key)
                elif t not in root_tests:
                    root_tests.append(t)
        elif s == "optional":
            p.skipped.append(c.name)
    if root_tests:
        cmd = ["cargo", "test", "--no-fail-fast"]
        for t in root_tests:
            cmd += ["--test", t]
        p.commands.append(cmd)
    for crate, features, flt in crate_runs:
        cmd = ["cargo", "test", "--no-fail-fast", "-p", crate]
        if features:
            cmd += ["--features", features]
        p.commands.append(cmd + ["--", flt])
        if features and "gpu" in features.split(","):
            # The parity tests return early and pass without a GPU; asked to run
            # them, a host with none must fail rather than report a pass.
            p.env["SAMYAMA_REQUIRE_GPU"] = "1"
    return p


def describe(p: Plan) -> str:
    out = [f"{p.tier} tier"]
    out.append("  runs: " + (", ".join(p.run) if p.run else "nothing"))
    if p.skipped:
        out.append("  optional, not run: " + ", ".join(p.skipped) + " (--with-optional runs them)")
    if p.unsupported:
        out.append("  unsupported: " + ", ".join(p.unsupported))
    for g in p.gaps:
        out.append(f"  GAP: {g}")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tier", choices=TIERS)
    ap.add_argument("--list", action="store_true", help="describe every tier and exit")
    ap.add_argument("--dry-run", action="store_true", help="print the commands, do not run them")
    ap.add_argument("--with-optional", action="store_true", help="also run the optional tests (GPU)")
    args = ap.parse_args(argv)
    if not args.list and not args.tier:
        ap.error("give --tier or --list")

    try:
        caps = parse_manifest(DOC.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        print(f"FAIL: cannot read the manifest in {DOC.relative_to(ROOT)}: {e}")
        return 1

    if args.list:
        for t in TIERS:
            print(describe(plan(caps, t, args.with_optional)))
        return 0

    p = plan(caps, args.tier, args.with_optional)
    print(describe(p))
    if not p.commands:
        print(f"\nFAIL: the {args.tier} tier has nothing to run")
        return 2
    env = dict(os.environ, **p.env)
    failed = []
    for cmd in p.commands:
        shown = " ".join(f"{k}={v}" for k, v in p.env.items() if "gpu" in " ".join(cmd)) + " " + " ".join(cmd)
        print(f"\n$ {shown.strip()}", flush=True)
        if args.dry_run:
            continue
        if subprocess.run(cmd, cwd=ROOT, env=env).returncode != 0:
            failed.append(" ".join(cmd))
    if args.dry_run:
        return 0
    if failed:
        print(f"\nFAIL: the {args.tier} tier's suite failed:\n  " + "\n  ".join(failed))
        return 1
    print(f"\nok: every {'required and optional' if args.with_optional else 'required'} "
          f"test of the {args.tier} tier passed"
          + (f"; {len(p.gaps)} gap(s) remain, listed above" if p.gaps else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
