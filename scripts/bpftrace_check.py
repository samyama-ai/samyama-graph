#!/usr/bin/env python3
"""CH-OBS (#2219): the shipped bpftrace scripts work against a live server.

    sudo python3 scripts/bpftrace_check.py --bin target/release/samyama

Linux, as root, with bpftrace and strace installed. Starts the server with
SAMYAMA_FSYNC=1 and checks, against the scripts' own output:

1. `bpftrace -l` lists every probe in src/usdt.rs's PROBES on the binary.
2. ops/bpftrace/samyama-tail-latency.bt attributes an injected disk stall:
   strace delays every fsync/fdatasync by 300 ms, and the script must report
   the write's persist span as slow with most of it in `fsync`. A CPU-bound
   read must be reported as slow with most of it `oncpu`.
3. ops/bpftrace/samyama-mmap-faults.bt reports the page faults of a large
   write against its query and persist spans.

The assertions are ratios within one measurement (fsync / span, oncpu /
span), never absolute times, so a slower runner moves both sides.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BT = ROOT / "ops/bpftrace"
DELAY_US = 300_000


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def query(port: int, q: str) -> None:
    req = urllib.request.Request(f"http://127.0.0.1:{port}/api/query", method="POST",
                                 data=json.dumps({"query": q}).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        r.read()


class Trace:
    """A bpftrace script attached to the server, stopped with SIGINT like Ctrl-C."""

    def __init__(self, script: Path, pid: int, *args: str):
        self.out = tempfile.TemporaryFile("w+")
        self.proc = subprocess.Popen(["bpftrace", "-p", str(pid), str(script), *args],
                                     stdout=self.out, stderr=subprocess.STDOUT)
        deadline = time.time() + 60
        while "Attaching" not in self.text():
            if self.proc.poll() is not None or time.time() > deadline:
                sys.exit(f"FAIL: {script.name} did not attach:\n{self.text()[-3000:]}")
            time.sleep(0.3)
        time.sleep(2)  # probes attach after the banner

    def text(self) -> str:
        self.out.seek(0)
        return self.out.read()

    def stop(self) -> str:
        self.proc.send_signal(signal.SIGINT)
        self.proc.wait(timeout=60)
        return self.text()


def fields(line: str) -> dict[str, int]:
    out = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)}
    out["total"] = int(re.search(r"(\d+) ms", line).group(1))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    args = ap.parse_args()
    if os.geteuid() != 0:
        sys.exit("FAIL: run as root (bpftrace and strace attach to the server)")
    binary = Path(args.bin).resolve()
    problems: list[str] = []

    # 1. Every documented probe is visible to bpftrace.
    usdt = (ROOT / "src/usdt.rs").read_text()
    documented = set(re.findall(r'^\s*\("(\w+)",', usdt, re.M))
    listed = subprocess.run(["bpftrace", "-l", f"usdt:{binary}:samyama:*"],
                            capture_output=True, text=True).stdout
    seen = set(re.findall(r":samyama:(\w+)$", listed, re.M))
    if not documented or documented != seen:
        problems.append(f"bpftrace -l lists {sorted(seen)}; src/usdt.rs documents {sorted(documented)}")
    print(f"probes listed by bpftrace: {sorted(seen)}")

    work = Path(tempfile.mkdtemp(prefix="bpftrace-check-"))
    http = free_port()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SAMYAMA_", "NLQ_", "OTEL_"))}
    env["SAMYAMA_FSYNC"] = "1"
    log = open(work / "server.log", "w")
    server = subprocess.Popen([str(binary), "--port", str(free_port()), "--http-port", str(http),
                               "--data-path", str(work / "data")],
                              cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    try:
        deadline = time.time() + 90
        while True:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{http}/api/status", timeout=5).read()
                break
            except OSError:
                if server.poll() is not None or time.time() > deadline:
                    sys.exit(f"FAIL: the server did not start:\n{(work / 'server.log').read_text()[-3000:]}")
                time.sleep(0.3)

        # 2. Tail latency: an injected fsync stall, and a CPU-bound read.
        t = Trace(BT / "samyama-tail-latency.bt", server.pid, "100")
        strace = subprocess.Popen(["strace", "-f", "-qq", "-p", str(server.pid), "-o", "/dev/null",
                                   "-e", "trace=fsync,fdatasync",
                                   "-e", f"inject=fsync,fdatasync:delay_enter={DELAY_US}"])
        time.sleep(2)
        for i in range(3):
            query(http, f"CREATE (:Slow {{i: {i}}})")
        strace.terminate()
        strace.wait(timeout=30)
        query(http, "UNWIND range(1, 2000000) AS i WITH i WHERE i % 7 = 0 RETURN count(i)")
        out = t.stop()
        print(out[:4000])
        persists = [fields(l) for l in out.splitlines() if l.startswith("SLOW persist")]
        if len(persists) < 3:
            problems.append(f"tail-latency: {len(persists)} slow persist lines for 3 writes with a "
                            f"{DELAY_US // 1000} ms fsync stall")
        for p in persists:
            if p["fsync"] < 0.8 * p["total"]:
                problems.append(f"tail-latency: a persist stalled in fsync was attributed {p}")
        reads = [fields(l) for l in out.splitlines() if l.startswith("SLOW query") and "UNWIND" in l]
        if not reads:
            problems.append("tail-latency: the CPU-bound read was not reported as slow")
        elif reads[0]["oncpu"] < 0.8 * reads[0]["total"]:
            problems.append(f"tail-latency: a CPU-bound read was attributed {reads[0]}")
        if "ERROR" in out:
            problems.append("tail-latency: bpftrace reported an error")

        # 3. Page faults of a large write, against its spans.
        t = Trace(BT / "samyama-mmap-faults.bt", server.pid, "500")
        query(http, "UNWIND range(1, 100000) AS i CREATE (:F {i: i, s: toString(i)})")
        out = t.stop()
        print(out[:3000])
        if not re.search(r"^FAULTS query .*CREATE \(:F", out, re.M):
            problems.append("mmap-faults: no fault line for the 100,000-node write's query span")
        if not re.search(r"^FAULTS persist .*mutations=\d+", out, re.M):
            problems.append("mmap-faults: no fault line for the 100,000-node write's persist span")
        if "ERROR" in out:
            problems.append("mmap-faults: bpftrace reported an error")
    finally:
        server.terminate()
        server.wait(timeout=30)
        log.close()

    if problems:
        print("FAIL  CH-OBS bpftrace:\n  " + "\n  ".join(problems))
        return 1
    print("PASS  CH-OBS bpftrace: every probe listed; fsync stall and CPU-bound read attributed; "
          "page faults attributed to the write's spans")
    return 0


if __name__ == "__main__":
    sys.exit(main())
