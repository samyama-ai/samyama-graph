#!/usr/bin/env python3
"""SEC-API-FUZZ: every documented HTTP route survives generated input (#2209).

    python3 scripts/api_fuzz.py --bin target/release/samyama
    python3 scripts/api_fuzz.py --bin ... --update   # rewrite the known list

Starts the server, then Schemathesis (pinned in CI) sends schema-derived and
schema-violating requests to every operation in `openapi/openapi.yaml`.

Hard failures, never allowed:

* **a 5xx** (ST101, ST102): bad input reached a panic or an unhandled error;
* **a request that got no answer** (a Schemathesis error: timeout, reset);
* **the server dead, or a panic in its log**, after the run.

The rest -- a status code or content type the spec does not document, a
schema-valid request rejected -- is a disagreement between the spec and the
server. Those that exist today are listed per operation in
`scripts/api_fuzz_known.json`; a new (operation, check) pair fails, and a pair
no longer seen is reported so the list only shrinks. Fixing one is either a
spec change (document the 400) or a server change, decided per case.

The gate also fails if it tested fewer operations than the spec declares less
the excluded ones, so a spec it cannot load or a filter that matches nothing
does not pass as "no failures".

Open transactions hold the graph's write lock (`POST /api/tx/begin`, see the
spec), and the fuzzer opens them and walks away; the server runs with a 2 s
transaction timeout so one abandoned transaction does not stall the run.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPEC = ROOT / "openapi" / "openapi.yaml"
KNOWN = ROOT / "scripts" / "api_fuzz_known.json"
SEED = "7"
# The SSE stream never ends, so every request to it times out by design.
EXCLUDE = [r"^/optimize/solve/\{id\}/stream$"]
HARD = {"ST101", "ST102"}
METHODS = ("get", "put", "post", "delete", "patch")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def declared_operations() -> int:
    paths = re.findall(r"^  (/\S*):\s*$", SPEC.read_text(), re.M)
    n = 0
    text = SPEC.read_text()
    for i, p in enumerate(paths):
        if any(re.search(rx, p) for rx in EXCLUDE):
            continue
        start = text.index(f"\n  {p}:")
        nxt = text.find(f"\n  {paths[i + 1]}:", start) if i + 1 < len(paths) else len(text)
        n += len(re.findall(rf"^    ({'|'.join(METHODS)}):", text[start:nxt], re.M))
    return n


def alive(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status", timeout=10) as r:
            return r.status == 200
    except OSError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--examples", default="100")
    ap.add_argument("--update", action="store_true")
    args = ap.parse_args()
    st = shutil.which("schemathesis") or sys.exit("FAIL: schemathesis is not installed")

    work = Path(tempfile.mkdtemp(prefix="api-fuzz-"))
    http = free_port()
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("SAMYAMA_", "NLQ_", "OTEL_"))}
    env["SAMYAMA_TX_TIMEOUT_SECS"] = "2"
    log = open(work / "server.log", "w")
    server = subprocess.Popen(
        [str(Path(args.bin).resolve()), "--ephemeral", "--port", str(free_port()), "--http-port", str(http)],
        cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    try:
        deadline = time.time() + 90
        while not alive(http):
            if server.poll() is not None or time.time() > deadline:
                sys.exit(f"FAIL: the server did not start:\n{(work / 'server.log').read_text()[-3000:]}")
            time.sleep(0.5)

        cmd = [st, "run", str(SPEC), "--url", f"http://127.0.0.1:{http}", "--checks", "all",
               "--phases", "examples,fuzzing", "--max-examples", args.examples, "--seed", SEED,
               "--workers", "1", "--request-timeout", "20", "--report", "junit",
               "--report-dir", str(work / "report")]
        for rx in EXCLUDE:
            cmd += ["--exclude-path-regex", rx]
        st_env = dict(os.environ, SCHEMATHESIS_HOOKS="api_fuzz_hooks",
                      PYTHONPATH=os.pathsep.join(filter(None, [str(ROOT / "scripts"), os.environ.get("PYTHONPATH")])))
        run = subprocess.run(cmd, capture_output=True, text=True, cwd=work, env=st_env)
        reports = list((work / "report").glob("*.xml"))
        if not reports:
            sys.exit(f"FAIL: Schemathesis wrote no report (exit {run.returncode}):\n{run.stdout[-3000:]}{run.stderr[-2000:]}")
        still_alive = alive(http)
    finally:
        server.terminate()
        server.wait(timeout=30)
        log.close()

    seen: dict[str, set[str]] = {}
    hard: list[str] = []
    tested = 0
    for tc in ET.parse(reports[0]).getroot().iter("testcase"):
        tested += 1
        op = tc.attrib["name"]
        for child in tc:
            text = (child.attrib.get("message") or "") + "\n" + (child.text or "")
            if child.tag == "error":
                hard.append(f"{op}: no answer -- {text.strip().splitlines()[0][:200]}")
            elif child.tag == "failure":
                codes = set(re.findall(r"\bST\d{3}\b", text))
                for c in sorted(codes & HARD):
                    hard.append(f"{op}: {c} server error\n{text.strip()[:1200]}")
                seen.setdefault(op, set()).update(codes - HARD)
    panics = sorted({l.split(") ", 1)[-1] for l in (work / "server.log").read_text().splitlines()
                     if "panicked" in l})

    failed = False
    want = declared_operations()
    print(f"operations tested: {tested} (spec declares {want} after exclusions)")
    if tested < want:
        failed = True
        print(f"FAIL  only {tested} of {want} operations were tested")
    for h in hard:
        failed = True
        print(f"FAIL  {h}")
    if not still_alive:
        failed = True
        print("FAIL  the server no longer answers /api/status after the run")
    for p in panics[:10]:
        failed = True
        print(f"FAIL  server log: {p[:300]}")

    if args.update:
        KNOWN.write_text(json.dumps({k: sorted(v) for k, v in sorted(seen.items())}, indent=2) + "\n")
        print(f"wrote {KNOWN.relative_to(ROOT)}: {sum(len(v) for v in seen.values())} known disagreements")
        return 1 if failed else 0

    known = {k: set(v) for k, v in json.loads(KNOWN.read_text()).items()}
    for op in sorted(seen):
        for c in sorted(seen[op] - known.get(op, set())):
            failed = True
            print(f"FAIL  new spec disagreement: {op} {c}")
    gone = [(op, c) for op in sorted(known) for c in sorted(known[op] - seen.get(op, set()))]
    for op, c in gone:
        print(f"note  {op} {c} no longer seen -- remove it from {KNOWN.relative_to(ROOT)}")
    total = sum(len(v) for v in seen.values())
    print(f"spec disagreements: {total} seen, {sum(len(v) for v in known.values())} known")
    if failed:
        print(f"\nfull Schemathesis output and server log: {work}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
