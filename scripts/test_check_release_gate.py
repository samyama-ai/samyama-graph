#!/usr/bin/env python3
"""The release gate must refuse for every reason the evidence can be wrong.

A gate that only rejects a red verdict is not a gate. The cases that actually
happen are the nightly having stopped (stale), never having run (missing), and
-- with `--commit` -- having measured a build other than the one being tagged:
an older main, with engine changes merged after it. All of them look exactly
like "nothing is wrong" if the check only compares a status string.

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


def run(verdict: dict | None, name: str = "RELEASE-GATE.json", extra: list[str] | None = None) -> int:
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / name
        if verdict is not None:
            p.write_text(json.dumps(verdict))
        return subprocess.run(
            [sys.executable, str(CHECK), str(p), *(extra or [])], capture_output=True, text=True
        ).returncode


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True,
                          text=True, check=True).stdout.strip()


def commit(repo: Path, path: str, text: str, msg: str) -> str:
    f = repo / path
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(text)
    git(repo, "add", path)
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", msg)
    return git(repo, "rev-parse", "--short", "HEAD")


def provenance_cases() -> list[tuple[str, int, dict, list[str]]]:
    """A small history: main with an engine commit, a docs commit, another
    engine commit, a side branch, and a release commit on top."""
    repo = Path(tempfile.mkdtemp())
    git(repo, "init", "-q", "-b", "main")
    measured = commit(repo, "src/lib.rs", "v1\n", "engine: the build the nightly measured")
    after_docs = commit(repo, "docs/guide.md", "words\n", "docs only")
    engine_later = commit(repo, "src/store.rs", "faster\n", "engine change after the nightly")
    git(repo, "checkout", "-q", "-b", "side", measured)
    side = commit(repo, "src/lib.rs", "other\n", "a line that never reached main")
    git(repo, "checkout", "-q", "main")
    release = commit(repo, "Cargo.toml", 'version = "1.8.0"\n', "release: v1.8.0")
    # A release cut from just after the docs commit, with no engine change since the measurement.
    git(repo, "checkout", "-q", "-b", "clean", after_docs)
    clean_release = commit(repo, "Cargo.toml", 'version = "1.8.0"\n', "release: v1.8.0")
    git(repo, "checkout", "-q", "main")
    fresh = lambda engine: {"status": "pass", "measured_at": stamp(0), "host": "vm-1", "engine_commit": engine}
    at = lambda c: ["--commit", c, "--repo", str(repo)]
    return [
        ("a pass on the release's parent covers the release", 0, fresh(engine_later), at(release)),
        ("a pass with only docs merged since covers the release", 0, fresh(measured), at(clean_release)),
        # The v1.8.0 case: green on an older main, engine changes merged after it.
        ("a pass on an older main with engine changes since blocks", 1, fresh(measured), at(release)),
        ("a pass on a commit outside the release's history blocks", 1, fresh(side), at(release)),
        ("a pass that names no engine_commit blocks", 1,
         {"status": "pass", "measured_at": stamp(0), "host": "vm-1"}, at(release)),
        ("a pass naming a commit the repository does not have blocks", 1, fresh("deadbee"), at(release)),
        ("without --commit, provenance is not checked", 0, fresh(measured), []),
    ]


def stamp(days_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


CASES = [
    ("a fresh pass lets the tag through", 0,
     {"status": "pass", "measured_at": stamp(0), "host": "vm-1"}),
    ("a fresh fail blocks", 1,
     {"status": "fail", "measured_at": stamp(0), "note": "IC6 3.2x slower"}),
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
    for name, want, verdict, extra in [(n, w, v, []) for n, w, v in CASES] + provenance_cases():
        got = run(verdict, extra=extra)
        good = got == want
        ok &= good
        print(f"{'ok  ' if good else 'FAIL'} {name}"
              + ("" if good else f"  -- wanted rc={want}, got rc={got}"))
    print("\nPASS" if ok else "\nFAIL")
    sys.exit(0 if ok else 1)
