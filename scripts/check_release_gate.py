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
        [--commit SHA] [--repo PATH]

**A pass must be about the code being shipped.** Status and age alone let a
green verdict from an older main vouch for a release that added engine changes
after it -- which is what v1.8.0 would have done on 2026-09-14: the live
verdict measured `0eb6a55`, and the release carried #1198 and #1194 on top.
With `--commit` (the commit being tagged), the verdict's `engine_commit` must be
an ancestor of it, and no commit between the two -- other than the tagged
commit itself, the release PR being tagged -- may touch engine code
(`ENGINE_PATHS`). A docs-only merge after the nightly does not matter; an engine
change does, and the tag waits for the next nightly.

This relies on the release PR landing as one commit, which is how this
repository merges (squash). A release merged as several commits would show its
own version bumps as engine changes and be refused -- the safe direction.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


# What a CH-REGRESS run measures. Benches are excluded: they change what is
# measured, not what is shipped.
ENGINE_PATHS = ("src", "crates", "Cargo.toml", "Cargo.lock")


def git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)


def provenance_error(engine: str | None, commit: str, repo: Path) -> str | None:
    """Why the verdict does not cover `commit`, or None if it does."""
    if not engine:
        return ("the verdict names no engine_commit, so nothing says which build "
                "it measured")
    if git(repo, "cat-file", "-e", f"{engine}^{{commit}}").returncode != 0:
        return f"the verdict's engine_commit {engine} is not a commit in this repository"
    if git(repo, "cat-file", "-e", f"{commit}^{{commit}}").returncode != 0:
        return f"the commit being tagged, {commit}, is not in this repository"
    if git(repo, "merge-base", "--is-ancestor", engine, commit).returncode != 0:
        return (f"the verdict measured {engine}, which is not in the history of "
                f"{commit}: it vouches for a different line of development")
    # Everything after the measured commit, except the tagged commit itself.
    between = git(repo, "log", "--format=%h %s", f"{engine}..{commit}^", "--", *ENGINE_PATHS)
    if between.returncode != 0:
        return f"could not list the commits between {engine} and {commit}: {between.stderr.strip()}"
    changed = [l for l in between.stdout.splitlines() if l.strip()]
    if changed:
        listing = "; ".join(changed[:10]) + (" ..." if len(changed) > 10 else "")
        return (f"{len(changed)} commit(s) changed engine code after the measured "
                f"{engine} and before the release: {listing}. The next nightly "
                f"has to measure them first")
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("verdict", type=Path)
    ap.add_argument("--max-age-days", type=int, default=3,
                    help="older than this and the nightly is not running")
    ap.add_argument("--commit",
                    help="the commit being tagged; the verdict must have measured "
                         "it, or an ancestor with no engine change since")
    ap.add_argument("--repo", type=Path, default=Path("."),
                    help="the git checkout holding --commit's history")
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

    if a.commit:
        why = provenance_error(g.get("engine_commit"), a.commit, a.repo)
        if why:
            print(f"::error::CH-REGRESS passed, but not on this build: {why}.")
            return 1

    print(f"CH-REGRESS pass, {age}d old, host {g.get('host')!r}, "
          f"engine {g.get('engine_commit')}"
          + (f", covers {a.commit}" if a.commit else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
