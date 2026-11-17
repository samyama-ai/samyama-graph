#!/usr/bin/env python3
"""SEC-MAC (#2218, REL-16): the AppArmor profile, under a real workload.

    sudo python3 scripts/apparmor_check.py --bin target/release/samyama

Linux with AppArmor enabled, as root (it installs the binary and loads the
profile). Three signals, as #2218 asks:

1. The policy is active: the server's /proc/<pid>/attr/current names the
   `samyama` profile in enforce mode.
2. The work still works: under the profile the server takes writes, builds an
   index, restarts on its own data, and exports and re-imports a snapshot --
   with no AppArmor denial logged for it. Checking only /api/status is the
   "health=ok confinement illusion" the issue warns about.
3. Escapes are refused and logged: a confined process -- root, so file
   permissions are no obstacle and only AppArmor is -- cannot read /etc/shadow
   or start /bin/sh, and the kernel logs a denial for each.

`--profile` swaps the profile file, which is how the gate is shown to catch a
profile that is too tight or too loose.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROFILE = ROOT / "ops/security/apparmor/usr.bin.samyama"
TARGET = Path("/usr/local/bin/samyama")
DATA = Path("/var/lib/samyama")
COUNT = "MATCH (n:P) RETURN count(n) AS c"


def sh(*cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def mark(name: str) -> str:
    """Write a marker into the kernel log; denials() reads what follows it."""
    tag = f"samyama-apparmor-check:{name}:{time.time_ns()}"
    Path("/dev/kmsg").write_text(tag + "\n")
    return tag


def denials(since: str) -> list[str]:
    """AppArmor denials for the samyama profile logged after marker `since`."""
    out = sh("dmesg", check=False).stdout
    after = out[out.rfind(since):] if since in out else ""
    return [l for l in after.splitlines() if 'apparmor="DENIED"' in l and 'profile="samyama"' in l]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(port: int, path: str, body: bytes, ctype: str = "application/json") -> tuple[int, bytes]:
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body, method="POST",
                                 headers={"Content-Type": ctype})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def cypher(port: int, q: str) -> dict:
    code, body = call(port, "/api/query", json.dumps({"query": q}).encode())
    if code != 200:
        sys.exit(f"FAIL: {q!r} returned {code} under the profile: {body[:500]!r}")
    return json.loads(body)


class Server:
    def __init__(self, data: Path):
        self.http = free_port()
        self.log = tempfile.NamedTemporaryFile("w+", prefix="samyama-", suffix=".log")
        env = {k: v for k, v in os.environ.items() if not k.startswith(("SAMYAMA_", "NLQ_", "OTEL_"))}
        self.proc = subprocess.Popen(
            [str(TARGET), "--port", str(free_port()), "--http-port", str(self.http), "--data-path", str(data)],
            cwd=str(data.parent), env=env, stdout=self.log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        deadline = time.time() + 90
        while True:
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.http}/api/status", timeout=5):
                    break
            except OSError:
                if self.proc.poll() is not None or time.time() > deadline:
                    self.log.seek(0)
                    sys.exit(f"FAIL: the server did not start under the profile:\n{self.log.read()[-3000:]}")
                time.sleep(0.3)

    def label(self) -> str:
        return Path(f"/proc/{self.proc.pid}/attr/current").read_text().strip("\x00\n ")

    def stop(self) -> None:
        self.proc.terminate()
        self.proc.wait(timeout=30)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--profile", default=str(PROFILE))
    args = ap.parse_args()
    if os.geteuid() != 0:
        sys.exit("FAIL: run as root (it installs the binary and loads the profile)")
    if sh("aa-enabled", check=False).stdout.strip() != "Yes":
        sys.exit("FAIL: AppArmor is not enabled on this host")

    shutil.copy2(args.bin, TARGET)
    TARGET.chmod(0o755)
    loaded = sh("apparmor_parser", "-r", "-W", args.profile, check=False)
    if loaded.returncode:
        sys.exit(f"FAIL: apparmor_parser rejected the profile:\n{loaded.stderr}")
    shutil.rmtree(DATA, ignore_errors=True)
    data, data2 = DATA / "graph", DATA / "graph2"
    data.mkdir(parents=True)
    data2.mkdir()
    since = mark("workload")
    problems: list[str] = []

    # 1 + 2: the policy is active and the workload runs under it.
    s = Server(data)
    label = s.label()
    print(f"server label: {label}")
    if label != "samyama (enforce)":
        problems.append(f"the server runs as {label!r}, not 'samyama (enforce)': the profile did not attach")
    cypher(s.http, "UNWIND range(1, 5000) AS i CREATE (:P {i: i, name: 'n' + toString(i)})")
    cypher(s.http, "CREATE INDEX ON :P(i)")
    cypher(s.http, "MATCH (a:P), (b:P) WHERE b.i = a.i + 1 AND a.i <= 1000 CREATE (a)-[:NEXT]->(b)")
    code, snap = call(s.http, "/api/snapshot/export", b"{}")
    if code != 200 or len(snap) < 1000:
        problems.append(f"snapshot export under the profile: {code}, {len(snap)} bytes")
    s.stop()

    s = Server(data)
    if cypher(s.http, COUNT)["records"][0][0] != 5000:
        problems.append("the data did not survive a restart under the profile")
    s.stop()

    s = Server(data2)
    boundary = "samyamaboundary7"
    body = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"g.sgsnap\"\r\n"
            f"Content-Type: application/octet-stream\r\n\r\n").encode() + snap + f"\r\n--{boundary}--\r\n".encode()
    code, resp = call(s.http, "/api/snapshot/import", body, f"multipart/form-data; boundary={boundary}")
    if code != 200:
        problems.append(f"snapshot import under the profile: {code} {resp[:300]!r}")
    elif cypher(s.http, COUNT)["records"][0][0] != 5000:
        problems.append("the imported snapshot does not hold the 5000 nodes")
    s.stop()

    time.sleep(1)
    workload_denials = denials(since)
    for d in workload_denials:
        problems.append(f"denied during the workload (the profile is too tight): {d}")
    print(f"workload: writes, index, restart, snapshot export + import; {len(workload_denials)} denials")

    # 3: escapes, as root, so only AppArmor stands in the way.
    escape_since = mark("escapes")
    open_shadow = sh("aa-exec", "-p", "samyama", "--", str(TARGET), "verify", "/etc/shadow", check=False)
    unconfined = sh(str(TARGET), "verify", "/etc/shadow", check=False)
    if "ermission denied" not in open_shadow.stderr + open_shadow.stdout:
        problems.append(f"a confined root process read /etc/shadow: {open_shadow.stderr.strip()[:300]}")
    if "ermission denied" in unconfined.stderr + unconfined.stdout:
        problems.append("/etc/shadow is unreadable to root even unconfined, so the escape test proves nothing")
    # A builtin would print even from a confined shell, so the test starts a
    # real program from inside the profile: that exec is the one it must refuse.
    spawn = sh("aa-exec", "-p", "samyama", "--", "/bin/sh", "-c", "/usr/bin/id", check=False)
    if "uid=" in spawn.stdout:
        problems.append("a process under the samyama profile started a program")
    time.sleep(1)
    logged = denials(escape_since)
    if not any('name="/etc/shadow"' in d for d in logged):
        problems.append("no kernel denial logged for reading /etc/shadow")
    if not any('operation="exec"' in d for d in logged):
        problems.append("no kernel denial logged for starting a program")
    print(f"escapes: /etc/shadow -> {open_shadow.stderr.strip()[:120]!r}; program start -> exit {spawn.returncode}; "
          f"{len(logged)} denials logged")

    if problems:
        print("FAIL  SEC-MAC:\n  " + "\n  ".join(problems))
        return 1
    print("PASS  SEC-MAC: profile enforced on the server, full workload with no denial, "
          "/etc/shadow and starting a program refused and logged")
    return 0


if __name__ == "__main__":
    sys.exit(main())
