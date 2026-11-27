#!/usr/bin/env python3
"""CH-HW-SIZE, runnable: the release binary's size per build profile (HW-05, #1623).

    python3 scripts/binary_size.py --build                 # build both profiles, weigh, compare
    python3 scripts/binary_size.py --build --smoke         # and check what each build serves
    python3 scripts/binary_size.py --binary edge=path/to/samyama --smoke
    python3 scripts/binary_size.py --build --record --reason "why it grew"

Profiles (docs/BINARY-SIZE.md):

* `server`: the default features, `cargo build --release --bin samyama`;
* `edge`: `--no-default-features --features edge`, without Arrow, Parquet and
  OpenTelemetry.

Both are built with the repository's own release profile (`lto = true`,
`codegen-units = 1`), because that is the binary that ships; a faster build
would weigh something else. Sizes are MiB (1024 * 1024 bytes), as the harness
reports them.

Two different checks, on purpose:

* **budgets** (server <= 50, edge <= 15): HW-05's targets. Reported for each
  profile; a miss fails the run only with `--enforce-budgets`, because the edge
  build misses its budget today and a gate that is red from the first day is a
  gate that gets switched off;
* **growth** (spec 05 section 5): a profile more than 10% larger than its
  recorded baseline fails, unless the baseline was re-recorded with a written
  reason. Baselines are per target triple: a macOS binary and a Linux one differ,
  and comparing them would measure the platform. A triple with no baseline is
  reported, not failed; record one with `--record`.

`--smoke` starts each binary and checks what it serves: the edge build must
answer 501 for Parquet and Arrow export and for Parquet import, refuse
`LOAD PARQUET` by name, and still export CSV; the server build must export
Parquet, so the refusals are shown to belong to the edge build alone.

Exit codes: 0 pass; 1 a check failed; 2 a build failed or a binary is missing.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / "scripts" / "binary_size_baseline.json"
MIB = 1024 * 1024
BUDGETS_MIB = {"server": 50, "edge": 15}
# The same limit in whole percent, so the check is exact integer arithmetic:
# 44 MiB on a 40 MiB baseline is 10% and passes, which 44/40 - 1 in floating
# point (0.10000000000000009) would not.
GROWTH_PERCENT = 10
PROFILES = {
    "server": [],
    "edge": ["--no-default-features", "--features", "edge"],
}


def target_triple() -> str:
    try:
        out = subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True).stdout
        for line in out.splitlines():
            if line.startswith("host:"):
                return line.split(":", 1)[1].strip()
    except (OSError, subprocess.CalledProcessError):
        pass
    return f"{platform.machine()}-{platform.system().lower()}"


def build(profile: str, out_dir: Path) -> Path:
    cmd = ["cargo", "build", "--release", "--locked", "--bin", "samyama", *PROFILES[profile]]
    print(f"$ {' '.join(cmd)}", flush=True)
    if subprocess.run(cmd, cwd=ROOT).returncode != 0:
        raise SystemExit(2)
    built = ROOT / "target" / "release" / ("samyama.exe" if os.name == "nt" else "samyama")
    # Both profiles write the same path; keep a copy so the second build does
    # not replace the first one's binary before it is weighed and smoked.
    dest = out_dir / f"samyama-{profile}"
    shutil.copy2(built, dest)
    print(f"kept the {profile} binary at {dest}", flush=True)
    return dest


def too_big(size: int, baseline: int) -> bool:
    """More than GROWTH_PERCENT larger than the baseline, exactly."""
    return size * 100 > baseline * (100 + GROWTH_PERCENT)


def check_budgets(sizes: dict[str, int]) -> list[tuple[str, float, int, bool]]:
    """(profile, MiB, budget, within) for every profile with a budget."""
    return [(p, b / MIB, BUDGETS_MIB[p], b / MIB <= BUDGETS_MIB[p]) for p, b in sizes.items() if p in BUDGETS_MIB]


def check_growth(sizes: dict[str, int], baseline: dict, triple: str) -> tuple[list[str], list[str]]:
    """(failures, notes) against the recorded baseline for `triple`."""
    fails, notes = [], []
    base = baseline.get(triple)
    if not base:
        notes.append(f"no baseline for {triple}; record one with --record")
        return fails, notes
    for p, b in sizes.items():
        entry = base.get("profiles", {}).get(p)
        if not entry:
            notes.append(f"{p}: no baseline for this profile on {triple}")
            continue
        growth = b / entry["bytes"] - 1
        over = too_big(b, entry["bytes"])
        line = f"{p}: {b / MIB:.2f} MiB vs baseline {entry['bytes'] / MIB:.2f} MiB ({growth:+.1%})"
        (fails if over else notes).append(line + (" -- more than 10% growth" if over else ""))
    return fails, notes


def record(sizes: dict[str, int], baseline: dict, triple: str, reason: str | None) -> dict:
    """The baseline with `triple`'s profiles replaced by `sizes`. Growth past the
    limit needs a written reason, which is kept beside the number."""
    old = baseline.get(triple, {}).get("profiles", {})
    grew = [p for p, b in sizes.items() if p in old and too_big(b, old[p]["bytes"])]
    if grew and not reason:
        raise ValueError(f"{', '.join(grew)} grew more than 10%; re-record with --reason \"why\"")
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    profiles = dict(old)
    for p, b in sizes.items():
        profiles[p] = {"bytes": b, "commit": commit} | ({"reason": reason} if reason else {})
    out = dict(baseline)
    out[triple] = {"profiles": profiles}
    return out


# --- smoke: what each build serves -------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _post(port: int, path: str, body: bytes, ctype: str) -> tuple[int, str]:
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, method="POST",
                                 headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def _json(port: int, path: str, obj: dict) -> tuple[int, str]:
    return _post(port, path, json.dumps(obj).encode(), "application/json")


def smoke(profile: str, binary: Path) -> list[str]:
    """Start `binary` and check what it serves; the failures, if any."""
    work = Path(tempfile.mkdtemp(prefix=f"binary-size-{profile}-"))
    http = _free_port()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SAMYAMA_", "NLQ_", "OTEL_"))}
    log = open(work / "server.log", "w")
    server = subprocess.Popen([str(binary), "--ephemeral", "--port", str(_free_port()), "--http-port", str(http)],
                              cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    fails: list[str] = []
    try:
        deadline = time.time() + 90
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{http}/api/status", timeout=5):
                    break
            except OSError:
                if server.poll() is not None or time.time() > deadline:
                    return [f"{profile}: the server did not start; log in {work}"]
                time.sleep(0.5)

        _json(http, "/api/query", {"query": "CREATE (:P {name: 'a'}), (:P {name: 'b'})"})
        q = "MATCH (p:P) RETURN p.name AS name ORDER BY name"

        def expect(what: str, got: tuple[int, str], status: int, *needles: str) -> None:
            code, text = got
            if code != status or not all(n in text for n in needles):
                fails.append(f"{profile}: {what}: expected {status} naming {list(needles)}, got {code}: {text[:200]}")

        boundary = "----binarysizesmoke"
        multipart = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"x.parquet\"\r\n\r\n"
                     f"PAR1 not a parquet file\r\n--{boundary}--\r\n").encode()
        expect("CSV export", _json(http, "/api/query/export", {"query": q, "format": "csv"}), 200, "name")
        if profile == "edge":
            for fmt, what in (("parquet", "Parquet export"), ("arrow", "Arrow export")):
                expect(f"{fmt} export", _json(http, "/api/query/export", {"query": q, "format": fmt}),
                       501, what, "`arrow` feature")
            expect("Parquet import",
                   _post(http, "/api/import/parquet?label=Row", multipart, f"multipart/form-data; boundary={boundary}"),
                   501, "Parquet import", "`arrow` feature")
            code, text = _json(http, "/api/query", {"query": "LOAD PARQUET FROM 'x.parquet' AS row RETURN row"})
            if code < 400 or "LOAD PARQUET" not in text or "`arrow` feature" not in text:
                fails.append(f"{profile}: LOAD PARQUET was not refused by name: {code}: {text[:200]}")
        else:
            code, _ = _json(http, "/api/query/export", {"query": q, "format": "parquet"})
            if code != 200:
                fails.append(f"{profile}: Parquet export answered {code}; the default build must serve it")
        if profile == "edge":
            # A refused import must create nothing.
            code, text = _json(http, "/api/query", {"query": "MATCH (r:Row) RETURN count(r) AS n"})
            try:
                rows = json.loads(text)["records"]
            except (ValueError, KeyError, TypeError):
                rows = None
            if code != 200 or rows != [[0]]:
                fails.append(f"{profile}: after the refused Parquet import, count(:Row) is not 0: {code}: {text[:200]}")
    finally:
        server.terminate()
        try:
            server.wait(timeout=30)
        except subprocess.TimeoutExpired:
            server.kill()
        log.close()
    return fails


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build", action="store_true", help="build every profile with the release profile")
    ap.add_argument("--binary", action="append", default=[], metavar="PROFILE=PATH",
                    help="weigh an existing binary instead of building it")
    ap.add_argument("--smoke", action="store_true", help="start each binary and check what it serves")
    ap.add_argument("--record", action="store_true", help=f"write the sizes to {BASELINE.name}")
    ap.add_argument("--reason", help="why a recorded size grew more than 10%%")
    ap.add_argument("--enforce-budgets", action="store_true", help="fail when a profile misses its budget")
    ap.add_argument("--json-out", help="write the measurements here")
    args = ap.parse_args(argv)

    binaries: dict[str, Path] = {}
    for spec in args.binary:
        profile, _, path = spec.partition("=")
        if profile not in PROFILES or not path:
            ap.error(f"--binary takes PROFILE=PATH with PROFILE one of {sorted(PROFILES)}")
        binaries[profile] = Path(path)
    if args.build:
        out_dir = Path(tempfile.mkdtemp(prefix="binary-size-"))
        for profile in PROFILES:
            if profile not in binaries:
                binaries[profile] = build(profile, out_dir)
    if not binaries:
        ap.error("give --build or --binary")
    for p, b in binaries.items():
        if not b.is_file():
            print(f"FAIL: {p}: no binary at {b}")
            return 2

    triple = target_triple()
    sizes = {p: b.stat().st_size for p, b in binaries.items()}
    failed = False
    print(f"\nbinary size on {triple} (MiB = 1024 * 1024 bytes)")
    for p, mib, budget, ok in check_budgets(sizes):
        print(f"  {p:7s} {mib:7.2f} MiB   budget {budget:3d}   {'within' if ok else 'OVER'}")
        if not ok and args.enforce_budgets:
            failed = True

    baseline = json.loads(BASELINE.read_text()) if BASELINE.is_file() else {}
    fails, notes = check_growth(sizes, baseline, triple)
    for n in notes:
        print(f"  note  {n}")
    for f in fails:
        failed = True
        print(f"  FAIL  {f}")

    smoke_fails: list[str] = []
    if args.smoke:
        for p, b in binaries.items():
            got = smoke(p, b)
            smoke_fails += got
            print(f"  smoke {p}: {'ok' if not got else 'FAILED'}")
        for f in smoke_fails:
            failed = True
            print(f"  FAIL  {f}")

    if args.record:
        try:
            BASELINE.write_text(json.dumps(record(sizes, baseline, triple, args.reason), indent=2, sort_keys=True) + "\n")
        except ValueError as e:
            print(f"FAIL: {e}")
            return 1
        print(f"  recorded {BASELINE.relative_to(ROOT)} for {triple}")

    if args.json_out:
        Path(args.json_out).write_text(json.dumps({
            "target": triple, "bytes": sizes, "budgets_mib": BUDGETS_MIB,
            "growth_failures": fails, "smoke_failures": smoke_fails}, indent=2) + "\n")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
