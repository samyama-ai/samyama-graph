#!/usr/bin/env python3
"""The Dockerfile's Rust must be new enough for what the lockfile resolves to.

v1.10.0 shipped with no container image. The Dockerfile pinned
`rust:1.85-bookworm`; this cycle's TLS work pulled `aws-lc-sys` and the ICU
stack, and `cargo build --release` inside the image stopped with

    error: rustc 1.85.1 is not supported by the following packages:
      encoding_rs@0.8.41 requires rustc 1.88
      icu_collections@2.3.0 requires rustc 1.88   [...]

CI could not have caught it. CI builds with `dtolnay/rust-toolchain@stable`,
which is whatever stable is that day; only the image is pinned. The two drift
apart silently, and the first thing that notices is a release with no image --
which is also the moment it is most expensive to find out.

This reads the pin out of the Dockerfile and the highest `rust-version` out of
the resolved dependency tree, and fails when the pin is lower. It needs no
container build: `cargo metadata --locked` answers from the lockfile in
seconds.

    python3 scripts/check_dockerfile_rust_pin.py
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = ROOT / "Dockerfile"


def parts(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in v.split(".")) 


def pinned() -> tuple[str, tuple[int, ...]]:
    """The builder stage's Rust version, e.g. `FROM rust:1.90-bookworm`."""
    text = DOCKERFILE.read_text(encoding="utf-8")
    m = re.search(r"^FROM\s+rust:(\d+(?:\.\d+){0,2})", text, re.M)
    if not m:
        sys.exit(f"{DOCKERFILE.name}: no `FROM rust:<version>` line found. If the "
                 f"builder image changed, this check needs updating rather than deleting.")
    return m.group(1), parts(m.group(1))


def required() -> tuple[str, str]:
    """The highest `rust-version` in the resolved tree, and who asks for it."""
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=ROOT, capture_output=True, text=True,
    )
    if out.returncode != 0:
        sys.exit(f"cargo metadata failed: {out.stderr.strip()[:400]}")
    best, who = None, ""
    for p in json.loads(out.stdout)["packages"]:
        rv = p.get("rust_version")
        if not rv:
            continue
        if best is None or parts(rv) > parts(best):
            best, who = rv, f"{p['name']} {p['version']}"
    return best or "0", who


def main() -> int:
    pin_s, pin = pinned()
    need_s, who = required()
    need = parts(need_s)
    # Compare on (major, minor): the image tag `rust:1.90-bookworm` carries no
    # patch, and a crate asking for 1.88.0 is satisfied by any 1.88.x.
    if pin[:2] < need[:2]:
        print(f"::error::Dockerfile pins rust {pin_s}, but the resolved tree needs "
              f"{need_s} ({who}). The image build will stop with 'rustc {pin_s} is not "
              f"supported by the following packages'. Raise the `FROM rust:` pin.")
        return 1
    print(f"Dockerfile pins rust {pin_s}; the resolved tree needs {need_s} ({who}) — ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
