#!/usr/bin/env python3
"""REL-04 — kill -9 the server mid-write and check what survives.

The requirement: "kill -9 at 1,000 random points during mixed workload;
recovery yields a state consistent with some prefix of the commit log, every
time". This runs that loop and reports what it finds, rather than asserting a
pass.

**The check is one-sided on purpose.** Losing an unacknowledged write is
correct -- the client never heard it land. Losing an *acknowledged* one is not:
the server answered, the client moved on, and the write is gone. So the probe
records the id of every write the server acknowledged, and after restart asks
which of those are present. Anything missing is lost acknowledged data. The
reverse direction is checked too: a node present that was never acknowledged
means the server persisted something it had not agreed to.

**Prefix, not count.** "43 of 50 survived" is consistent with a prefix and also
with 43 scattered survivors, which is a different and worse failure: a state no
ordering of the commit log can produce. The ids are written in order, so the
surviving set is compared against the longest prefix of that order.

Timing is the reason this is a script and not a test. `kill -9` has to hit a
real process at a real moment; an in-process test can only simulate the crash
it is trying to observe.

    python3 scripts/crash_consistency.py --cycles 50 --json out.json

`--mode` decides what an acknowledgement is: `auto` takes the 200 on the
statement, `tx` takes the 200 on `POST /api/tx/:id/commit`. They are different
promises and the default runs half the cycles each.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

#: The server under test. `--binary` pins it to a copy, which a long sweep
#: needs: `cargo build` replaces `target/release/samyama` in place, so a build
#: started while a sweep is running silently moves the sweep onto a different
#: engine half way through -- and the result would be attributed to whichever
#: commit was checked out at the end.
DEFAULT_BINARY = REPO / "target" / "release" / "samyama"
BINARY = DEFAULT_BINARY


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def post(port: int, path: str, body: dict, timeout: float = 10.0) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def query(port: int, cypher: str, timeout: float = 10.0, tx: str | None = None) -> dict:
    body = {"query": cypher}
    if tx is not None:
        body["tx"] = tx
    return post(port, "/api/query", body, timeout)


def start(data_dir: Path, resp_port: int, http_port: int) -> subprocess.Popen:
    proc = subprocess.Popen(
        [str(BINARY), "--port", str(resp_port), "--http-port", str(http_port),
         "--data-path", str(data_dir)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        # Its own process group, so kill -9 takes the whole server and not just
        # the parent: a surviving child holding the data directory makes the
        # next start fail for a reason that has nothing to do with recovery.
        start_new_session=True,
    )
    return proc


def wait_ready(port: int, proc: subprocess.Popen, deadline_s: float = 30.0) -> bool:
    end = time.time() + deadline_s
    while time.time() < end:
        if proc.poll() is not None:
            return False
        try:
            query(port, "RETURN 1", timeout=2.0)
            return True
        except Exception:
            time.sleep(0.05)
    return False


def kill9(proc: subprocess.Popen) -> None:
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait(timeout=30)


def longest_prefix(acked: list[int], survived: set[int]) -> int:
    n = 0
    for i in acked:
        if i in survived:
            n += 1
        else:
            break
    return n


def one_cycle(cycle: int, rng: random.Random, mode: str,
              min_delay: float, max_delay: float) -> dict:
    """One kill point: write until the crash, then restart and compare."""
    data_dir = Path(tempfile.mkdtemp(prefix="samyama-crash-"))
    resp_port, http_port = free_port(), free_port()
    proc = start(data_dir, resp_port, http_port)
    try:
        if not wait_ready(http_port, proc):
            return {"cycle": cycle, "error": "server did not start"}

        # A mixed workload: creates, a property update, and a read, so the
        # crash lands somewhere other than the middle of a single CREATE every
        # time.
        #
        # `mode` decides what an acknowledgement *is*. Auto-commit takes the
        # 200 on the CREATE; `tx` takes the 200 on `POST /api/tx/:id/commit`,
        # which is the acknowledgement samyama-graph#1275 is about -- COMMIT
        # answering before the write is persisted. The two are different claims
        # and a probe that only makes the first cannot test the second.
        # **The kill lands while a write is in flight.** An earlier version of
        # this loop wrote N times and then killed, so every crash happened in
        # the gap between two requests -- the one moment the server has nothing
        # half-done. The writer runs on its own thread and the kill fires after
        # a random delay, so the signal arrives wherever it arrives.
        acked: list[int] = []
        in_flight: list[int] = []
        stop = threading.Event()

        def writer() -> None:
            i = 0
            while not stop.is_set():
                i += 1
                in_flight.append(i)
                try:
                    if mode == "tx":
                        tx = post(http_port, "/api/tx/begin", {})["tx"]
                        query(http_port, f"CREATE (:Crash {{seq: {i}}})", tx=tx)
                        if i % 5 == 0:
                            query(http_port, f"MATCH (n:Crash {{seq: {i}}}) "
                                             f"SET n.touched = true", tx=tx)
                        post(http_port, f"/api/tx/{tx}/commit", {})
                    else:
                        query(http_port, f"CREATE (:Crash {{seq: {i}}})", timeout=10.0)
                        if i % 5 == 0:
                            query(http_port, f"MATCH (n:Crash {{seq: {i}}}) "
                                             f"SET n.touched = true")
                    acked.append(i)
                    if i % 7 == 0:
                        query(http_port, "MATCH (n:Crash) RETURN count(n)")
                except Exception:
                    return

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        kill_delay = rng.uniform(min_delay, max_delay)
        time.sleep(kill_delay)
        kill9(proc)
        stop.set()
        t.join(timeout=30)

        proc2 = start(data_dir, resp_port, http_port)
        try:
            if not wait_ready(http_port, proc2):
                return {"cycle": cycle, "error": "server did not restart",
                        "acked": len(acked)}
            res = query(http_port, "MATCH (n:Crash) RETURN n.seq AS seq", timeout=60.0)
            survived = set()
            for row in res.get("data", res.get("records", [])):
                v = row.get("seq") if isinstance(row, dict) else row[0]
                if isinstance(v, (int, float)):
                    survived.add(int(v))
        finally:
            kill9(proc2)

        # A write that was in flight when the signal landed is neither
        # acknowledged nor refused: it may or may not have reached disk, and
        # both outcomes are correct. Counting it as lost would report a
        # failure on every cycle by construction, which is the fastest way to
        # make a real one invisible.
        acked_set = set(acked)
        undecided = set(in_flight) - acked_set
        return {
            "cycle": cycle,
            "mode": mode,
            "kill_delay_s": round(kill_delay, 3),
            "acked": len(acked),
            "undecided_at_kill": len(undecided),
            "survived": len(survived),
            "acked_lost": sorted(acked_set - survived)[:20],
            "acked_lost_count": len(acked_set - survived),
            "unacked_present": sorted(survived - acked_set - undecided)[:20],
            "unacked_present_count": len(survived - acked_set - undecided),
            "longest_acked_prefix_intact": longest_prefix(acked, survived),
        }
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycles", type=int, default=50,
                    help="kill points (REL-04's H1 target is 1000)")
    ap.add_argument("--mode", choices=["auto", "tx", "both"], default="both",
                    help="what counts as an acknowledgement: the 200 on the "
                         "statement (auto), the 200 on COMMIT (tx), or half the "
                         "cycles each (both)")
    # How long the writer runs before the signal. The upper bound is also most
    # of the cost of a cycle: everything written has to be recovered on
    # restart, so a longer delay buys a deeper crash and a slower sweep.
    # REL-04 asks for 1000 kill points, and 1000 shallow ones say more about
    # "every time" than 100 deep ones.
    ap.add_argument("--min-delay", type=float, default=0.05)
    ap.add_argument("--max-delay", type=float, default=0.8)
    ap.add_argument("--binary", type=str,
                    help="server binary to run; pin a copy for a long sweep so "
                         "a rebuild cannot change the engine under it")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--json", type=str)
    a = ap.parse_args()

    global BINARY
    if a.binary:
        BINARY = Path(a.binary).resolve()
    if not BINARY.exists():
        print(f"{BINARY} does not exist; cargo build --release --bin samyama",
              file=sys.stderr)
        return 2

    rng = random.Random(a.seed)
    results = []
    for c in range(1, a.cycles + 1):
        mode = a.mode if a.mode != "both" else ("tx" if c % 2 == 0 else "auto")
        r = one_cycle(c, rng, mode, a.min_delay, a.max_delay)
        results.append(r)
        print(f"  cycle {c}: {r}", file=sys.stderr)

    ran = [r for r in results if "error" not in r]
    errored = [r for r in results if "error" in r]
    lost = [r for r in ran if r["acked_lost_count"] > 0]
    phantom = [r for r in ran if r["unacked_present_count"] > 0]
    not_a_prefix = [
        r for r in ran
        if r["longest_acked_prefix_intact"] != r["survived"] - r["unacked_present_count"]
    ]

    doc = {
        "binary": str(BINARY),
        "mode": a.mode,
        "kill_delay_range_s": [a.min_delay, a.max_delay],
        "cycles_requested": a.cycles,
        "cycles_ran": len(ran),
        "cycles_errored": len(errored),
        "target_cycles": 1000,
        "cycles_losing_acknowledged_writes": len(lost),
        "cycles_with_unacknowledged_writes_present": len(phantom),
        "cycles_whose_surviving_set_is_not_a_prefix": len(not_a_prefix),
        "acknowledged_writes_total": sum(r["acked"] for r in ran),
        "acknowledged_writes_lost_total": sum(r["acked_lost_count"] for r in ran),
        "worst_cycle_acked_lost": max((r["acked_lost_count"] for r in ran), default=0),
        # Split out, because the two modes make different claims: an
        # auto-commit ack and a COMMIT ack are not the same promise, and a
        # combined figure would let a clean half hide a broken one.
        "by_mode": {
            m: {
                "cycles": sum(1 for r in ran if r["mode"] == m),
                "acknowledged_writes": sum(r["acked"] for r in ran if r["mode"] == m),
                "acknowledged_writes_lost": sum(
                    r["acked_lost_count"] for r in ran if r["mode"] == m),
                "cycles_losing_acknowledged_writes": sum(
                    1 for r in ran if r["mode"] == m and r["acked_lost_count"] > 0),
            }
            for m in sorted({r["mode"] for r in ran})
        },
        "cycles": results,
    }
    out = json.dumps(doc, indent=2)
    if a.json:
        Path(a.json).write_text(out, encoding="utf-8")
        print(f"wrote {a.json}", file=sys.stderr)
    else:
        print(out)

    print(
        f"\n{len(ran)}/{a.cycles} cycles ran; "
        f"{len(lost)} lost acknowledged writes "
        f"({doc['acknowledged_writes_lost_total']} of "
        f"{doc['acknowledged_writes_total']} writes); "
        f"{len(not_a_prefix)} recovered to a state that is not a prefix.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
