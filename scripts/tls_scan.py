#!/usr/bin/env python3
"""SEC-TLS (#2209, REL-12): scan the HTTP listener's TLS with testssl.sh.

    python3 scripts/tls_scan.py --bin target/release/samyama --testssl testssl/testssl.sh
    python3 scripts/tls_scan.py --self-test     # the verdict logic alone, no scan
    python3 scripts/tls_scan.py --judge report.json   # the verdict on an existing testssl.sh report

Starts the server with a freshly generated self-signed P-256 certificate
(REL-09's --tls-cert/--tls-key), runs testssl.sh against it, and fails when:

- SSLv2, SSLv3, TLS 1.0 or TLS 1.1 is offered;
- any finding is HIGH or CRITICAL, other than the certificate's own findings
  (IGNORED_PREFIXES below), and the overall grade when its only cap is the
  untrusted chain;
- the scan reports a problem, or does not report on every protocol in
  REQUIRED_IDS -- a scan that did not run must not pass as a clean one.

The certificate is generated here and is self-signed, so testssl.sh rates its
chain of trust CRITICAL. That rates the test's certificate, not the server:
an operator supplies their own. What this gate measures is the configuration
the server negotiates with whatever certificate it is given.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

OLD_PROTOCOLS = ("SSLv2", "SSLv3", "TLS1", "TLS1_1")
REQUIRED_IDS = OLD_PROTOCOLS + ("TLS1_2", "TLS1_3")
FAIL_SEVERITIES = ("HIGH", "CRITICAL")
# Findings about the certificate itself (trust, chain, OCSP/CRL, CAA, ...).
# The certificate is the test's, generated a second ago; see the docstring.
IGNORED_PREFIXES = ("cert", "intermediate_cert", "DNS_CAArecord", "OCSP_stapling")


def verdict(findings: list[dict]) -> list[str]:
    """Every reason the scan fails; empty when it passes."""
    problems: list[str] = []
    by_id = {f.get("id", ""): f for f in findings}
    for pid in REQUIRED_IDS:
        if pid not in by_id:
            problems.append(f"{pid}: not reported -- the scan did not test this protocol")
    for pid in OLD_PROTOCOLS:
        f = by_id.get(pid)
        if f and not f.get("finding", "").startswith("not offered"):
            problems.append(f"{pid}: {f.get('finding')} (must not be offered)")
    # The overall grade is capped to T for an untrusted chain alone; any other
    # cap reason is a finding about the configuration and still fails.
    caps = [f.get("finding", "") for f in findings if f.get("id", "").startswith("grade_cap_reason")]
    trust_only = bool(caps) and all("chain of trust" in c for c in caps)
    for f in findings:
        fid, sev = f.get("id", ""), f.get("severity", "")
        if fid == "overall_grade" and trust_only:
            continue
        if fid == "scanProblem" or sev == "FATAL":
            problems.append(f"{fid}: {sev}: {f.get('finding')} (the scan itself failed)")
        elif sev in FAIL_SEVERITIES and fid not in OLD_PROTOCOLS and not fid.startswith(IGNORED_PREFIXES):
            problems.append(f"{fid}: {sev}: {f.get('finding')}")
    return problems


def self_test() -> int:
    """The verdict must fail on each planted defect and pass the clean set."""
    clean = [{"id": p, "severity": "OK", "finding": "not offered"} for p in OLD_PROTOCOLS]
    clean += [{"id": "TLS1_2", "severity": "OK", "finding": "offered"},
              {"id": "TLS1_3", "severity": "OK", "finding": "offered with final"},
              {"id": "cert_chain_of_trust", "severity": "CRITICAL", "finding": "self signed"},
              {"id": "overall_grade", "severity": "CRITICAL", "finding": "T"},
              {"id": "grade_cap_reason_1", "severity": "INFO",
               "finding": "Grade capped to T. Issues with chain of trust (self signed)"}]
    assert verdict(clean) == [], verdict(clean)
    planted = {
        "TLS 1.0 offered": [{"id": "TLS1", "severity": "LOW", "finding": "offered (deprecated)"}],
        "a HIGH vulnerability": [{"id": "heartbleed", "severity": "HIGH", "finding": "VULNERABLE"}],
        "a scan problem": [{"id": "scanProblem", "severity": "FATAL", "finding": "can't connect"}],
        "a grade cap other than trust": [{"id": "grade_cap_reason_2", "severity": "INFO",
                                          "finding": "Grade capped to B. TLS 1.1 offered"}],
    }
    for name, extra in planted.items():
        found = verdict([f for f in clean if f["id"] != extra[0]["id"]] + extra)
        assert found, f"the verdict passed {name}"
    assert verdict([f for f in clean if f["id"] != "TLS1_3"]), "a missing protocol passed"
    print("PASS  tls_scan verdict self-test (clean set passes, 5 planted defects fail)")
    return 0


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def alive(port: int) -> bool:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(f"https://127.0.0.1:{port}/api/status", timeout=10, context=ctx) as r:
            return r.status == 200
    except OSError:
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin")
    ap.add_argument("--testssl")
    ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--judge", help="a testssl.sh --jsonfile report to judge, without starting a server")
    args = ap.parse_args()
    if args.self_test:
        return self_test()
    if args.judge:
        problems = verdict(json.loads(Path(args.judge).read_text()))
        print("FAIL  SEC-TLS:\n  " + "\n  ".join(problems) if problems else "PASS  SEC-TLS")
        return 1 if problems else 0
    if not (args.bin and args.testssl):
        ap.error("--bin and --testssl are required unless --self-test")
    self_test()

    work = Path(tempfile.mkdtemp(prefix="tls-scan-"))
    cert, key = work / "cert.pem", work / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
                    "-nodes", "-days", "2", "-subj", "/CN=localhost",
                    "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                    "-keyout", str(key), "-out", str(cert)], check=True, capture_output=True)

    http = free_port()
    env = {k: v for k, v in os.environ.items() if not k.startswith(("SAMYAMA_", "NLQ_", "OTEL_"))}
    log = open(work / "server.log", "w")
    server = subprocess.Popen(
        [str(Path(args.bin).resolve()), "--ephemeral", "--port", str(free_port()), "--http-port", str(http),
         "--tls-cert", str(cert), "--tls-key", str(key)],
        cwd=work, env=env, stdout=log, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
    report = work / "testssl.json"
    try:
        deadline = time.time() + 90
        while not alive(http):
            if server.poll() is not None or time.time() > deadline:
                sys.exit(f"FAIL: the server did not start over TLS:\n{(work / 'server.log').read_text()[-3000:]}")
            time.sleep(0.5)
        run = subprocess.run([str(Path(args.testssl).resolve()), "--quiet", "--color", "0", "--warnings", "off",
                              "--jsonfile", str(report), f"127.0.0.1:{http}"],
                             capture_output=True, text=True, cwd=work)
        print(run.stdout[-6000:])
    finally:
        server.terminate()
        server.wait(timeout=30)
        log.close()

    if not report.exists():
        sys.exit(f"FAIL: testssl.sh wrote no report (exit {run.returncode}):\n{run.stderr[-3000:]}")
    findings = json.loads(report.read_text())
    problems = verdict(findings)
    offered = [f["id"] for f in findings if f.get("id") in REQUIRED_IDS and f.get("finding", "").startswith("offered")]
    if problems:
        print("FAIL  SEC-TLS:\n  " + "\n  ".join(problems))
        return 1
    print(f"PASS  SEC-TLS: {len(findings)} findings, protocols offered: {', '.join(offered)}; "
          "no old protocol, no HIGH/CRITICAL outside the test certificate's own")
    return 0


if __name__ == "__main__":
    sys.exit(main())
