#!/usr/bin/env python3
"""Every path the Dockerfile COPYs out of the repo must exist.

v1.14.0 shipped with no container image. #1833 moved `cli/` into
`crates/samyama-cli`; the Dockerfile still said `COPY cli ./cli`, so
`publish-docker` failed on the release tag and the image was published only
after #2056 fixed it.

Nothing could have caught it. The pull request that broke the build did not
touch the Dockerfile -- it moved a directory the Dockerfile named -- so a gate
keyed on Dockerfile changes would not have fired. And nothing builds the image
on a pull request or in the nightly sweep, so the first build of a release is
the release itself.

This is the cheap half of that gap: it reads the Dockerfile's own COPY sources
and checks each one exists, in well under a second, on every pull request. It
does not prove the image builds -- only a build does that -- but it catches the
class of break that actually happened, and it catches it in the PR that causes
it rather than at the tag.

    python3 scripts/check_dockerfile_copy_sources.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE = ROOT / "Dockerfile"


def copy_sources(text: str) -> list[tuple[int, str]]:
    """(line number, source path) for each repo path a COPY reads.

    `COPY --from=<stage>` is skipped: those read an earlier build stage, not the
    repository. Flags are dropped, and the final token is the destination.
    """
    found: list[tuple[int, str]] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line.upper().startswith("COPY "):
            continue
        tokens = line.split()[1:]
        if any(t.startswith("--from=") for t in tokens):
            continue
        tokens = [t for t in tokens if not t.startswith("--")]
        # Last token is the destination inside the image.
        for src in tokens[:-1]:
            found.append((lineno, src))
    return found


def main() -> int:
    if not DOCKERFILE.is_file():
        print(f"FAIL: no Dockerfile at {DOCKERFILE}", file=sys.stderr)
        return 2

    sources = copy_sources(DOCKERFILE.read_text())
    if not sources:
        print("FAIL: found no COPY sources — has the Dockerfile changed shape?", file=sys.stderr)
        return 2

    missing = [(n, s) for n, s in sources if not (ROOT / s).exists()]

    for lineno, src in sources:
        mark = "ok" if (ROOT / src).exists() else "MISSING"
        print(f"  Dockerfile:{lineno:<3} {src:<24} {mark}")

    if missing:
        print(file=sys.stderr)
        for lineno, src in missing:
            print(
                f"FAIL: Dockerfile:{lineno} copies `{src}`, which does not exist. "
                f"If a directory moved, update the Dockerfile in the same change.",
                file=sys.stderr,
            )
        return 1

    print(f"\n{len(sources)} COPY sources, all present.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
