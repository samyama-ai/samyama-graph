#!/usr/bin/env python3
"""SEC-SEMGREP: Samyama's own static rules hold, and the findings do not grow (#2209).

    python3 scripts/check_semgrep.py            # self-test, then gate src/
    python3 scripts/check_semgrep.py --list     # print every finding, gate nothing

The rules are in `.semgrep.yml`; this holds each to a ceiling in
`scripts/semgrep_ceilings.json`, the same shape as the clippy ratchet
(`check_lint_ratchet.sh`): the findings that existed when a rule was added are
counted, and a change that adds one fails. A change that removes some should
lower the ceiling in the same commit, which is how the count only goes down.
Raising a ceiling is a decision, made in the commit that does it, with a reason.

Two ways this gate could pass while checking nothing, and both fail it:

* **Semgrep reports an error.** A rule that times out on a file skips that
  file silently -- measured: the `format!` rule on `src/query/executor/mod.rs`
  at the default timeout. Any error fails the gate.
* **A rule stops matching.** A Semgrep upgrade or a rule edit that matches
  nothing passes every ceiling. So the rules are first run over
  `scripts/semgrep-fixture/`, where each line marked `planted` must be
  reported and nothing else may be. (Not under `tests/`: Semgrep ignores any
  `tests/` directory by default, and the fixture was invisible there.)
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONFIG = ROOT / ".semgrep.yml"
CEILINGS = ROOT / "scripts" / "semgrep_ceilings.json"
FIXTURE = ROOT / "scripts" / "semgrep-fixture"
TIMEOUT_S = "300"


def scan(cwd: Path, target: str) -> dict:
    exe = shutil.which("semgrep")
    if not exe:
        sys.exit("FAIL: semgrep is not installed (pip install semgrep); the gate cannot run")
    out = subprocess.run(
        [exe, "--metrics=off", "--config", str(CONFIG), "--json", "--quiet",
         "--timeout", TIMEOUT_S, target],
        cwd=cwd, capture_output=True, text=True,
    )
    try:
        doc = json.loads(out.stdout)
    except ValueError:
        sys.exit(f"FAIL: semgrep produced no JSON (exit {out.returncode}):\n{out.stderr[-2000:]}")
    if doc.get("errors"):
        lines = [f"  {e.get('type')}: {(e.get('message') or '').strip()[:300]}" for e in doc["errors"]]
        sys.exit("FAIL: semgrep reported errors, so some code was not checked:\n" + "\n".join(lines))
    return doc


def rule(r: dict) -> str:
    return r["check_id"].rsplit(".", 1)[-1]


def source_line(r: dict) -> str:
    """The flagged line, read from the file: Semgrep's own `lines` field is
    withheld ("requires login") when it is run without a Semgrep account."""
    try:
        return (ROOT / r["path"]).read_text().splitlines()[r["start"]["line"] - 1].strip()[:100]
    except (OSError, IndexError):
        return ""


def self_test() -> None:
    fixture = FIXTURE / "src" / "http" / "planted.rs"
    planted = {i for i, line in enumerate(fixture.read_text().splitlines(), 1) if "// planted:" in line}
    found = {r["start"]["line"] for r in scan(FIXTURE, "src")["results"]}
    missed, extra = sorted(planted - found), sorted(found - planted)
    if missed or extra:
        sys.exit(f"FAIL: the rules do not match their fixture ({fixture.relative_to(ROOT)}): "
                 f"planted lines not reported {missed}, unplanted lines reported {extra}")
    print(f"self-test: {len(planted)} planted findings reported, nothing else")


def main() -> int:
    if "--list" in sys.argv:
        for r in scan(ROOT, "src")["results"]:
            print(f"{rule(r)}  {r['path']}:{r['start']['line']}  {source_line(r)}")
        return 0

    self_test()
    ceilings: dict[str, int] = json.loads(CEILINGS.read_text())
    results = scan(ROOT, "src")["results"]
    counts = Counter(rule(r) for r in results)
    rules_in_config = set(re.findall(r"^\s*- id: (\S+)", CONFIG.read_text(), re.M))
    if rules_in_config != set(ceilings):
        print(f"FAIL: rules {sorted(rules_in_config)} and ceilings {sorted(ceilings)} differ")
        return 1

    failed = False
    for name in sorted(ceilings):
        n, cap = counts.get(name, 0), ceilings[name]
        if n > cap:
            failed = True
            print(f"FAIL  {name}: {n} findings, ceiling {cap}")
            for r in results:
                if rule(r) == name:
                    print(f"        {r['path']}:{r['start']['line']}  {source_line(r)}")
        elif n < cap:
            print(f"PASS  {name}: {n} findings, ceiling {cap} -- lower it to {n} in "
                  f"{CEILINGS.relative_to(ROOT)} to keep the gain")
        else:
            print(f"PASS  {name}: {n} findings, ceiling {cap}")
    if failed:
        print("\nNew code should not add to these. Fix the new finding, or raise the ceiling "
              "in the same commit and say why.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
