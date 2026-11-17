#!/usr/bin/env python3
"""Record how much the project is used, so growth is judged by movement (DX-14, #1788).

DX-14 asks for "a monthly report of stars, downloads (crates/PyPI/npm/Docker)
and issue volume, so DX work is judged by movement rather than by feel". A
number nobody writes down cannot show movement; this writes them down.

For one repository it reports, from public APIs that need no login:

  * GitHub: stars, forks, and open issues (pull requests are not counted)
  * crates.io: all-time downloads and downloads in the last 90 days, for every
    crate in the repository that is meant to be published
  * PyPI: downloads in the last month (via pypistats.org)
  * npm: downloads in the last month

The package names are read from the repository's own manifests (`Cargo.toml`,
`sdk/python/pyproject.toml`, `sdk/typescript/package.json`) so they cannot drift
out of date. A crate with `publish = false` is not reported; a package that is
named in a manifest but not on the registry is reported as "not published", not
as zero.

Every number is fetched on its own. A source that is down, rate limited, or
answers with something unexpected shows as "unavailable" with the reason, and
the rest of the report is still produced. An unknown number is never printed as
0: a 0 that really means "I could not look" would read as "nobody downloaded it".

The container image is on ghcr.io, which shows no pull count to a caller without
credentials, so it is listed as not measured.

Movement. Pass the previous run's `--json` output as `--previous` and each
cumulative counter (stars, forks, all-time downloads) gets a change column.
Windowed counters (the last month, the last 90 days) already describe a period,
so they are not differenced.

    python3 scripts/adoption_report.py
    python3 scripts/adoption_report.py --json --out 2026-11.json
    python3 scripts/adoption_report.py --previous 2026-10.json

Set GITHUB_TOKEN to raise GitHub's rate limit. It is sent only to api.github.com
(a redirect to another host or scheme is not followed) and never printed.

Exit status: 0 when the report was produced; 2 when it could not be: the GitHub
numbers (the core of the report) are unavailable, or an option or manifest is
unusable.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPO = "samyama-ai/samyama-graph"
USER_AGENT = "samyama-adoption-report (https://github.com/samyama-ai/samyama-graph)"
TIMEOUT = 20            # for each wait on the network
TOTAL_TIMEOUT = 30      # for a whole request, however slowly the body is dripped in
MAX_BODY = 5_000_000
MAX_FILE = 5_000_000
# crates.io asks for at most one request a second.
CRATES_INTERVAL = 1.0

# owner/name as GitHub allows them: an owner has letters, digits and hyphens; a
# repository name may also hold dots and underscores but is not "." or "..".
# Matched with fullmatch, because `$` also accepts a trailing newline.
REPO_OK = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]*/(?!\.{1,2}\Z)[A-Za-z0-9._-]+")
# A token is one word of printable ASCII; anything else is not a token and must
# not be put into a header, where an error message would then repeat it.
TOKEN_OK = re.compile(r"[\x21-\x7e]+")
# A crate, PyPI or npm package name, optionally npm-scoped (@scope/name).
NAME_OK = re.compile(r"(@[a-z0-9][a-z0-9._-]*/)?[A-Za-z0-9][A-Za-z0-9._-]*")


class Unavailable(Exception):
    """One number could not be read; the message says why."""


class NotFound(Unavailable):
    """The registry answered 404: the package is not there."""


class ConfigError(Exception):
    """An option, a manifest or the previous report cannot be used."""


@dataclass
class Metric:
    source: str            # "github", "crates.io", "pypi", "npm", "ghcr.io"
    name: str              # repository or package
    metric: str            # e.g. "stars", "downloads_total"
    value: int | None      # None means unknown, never zero
    cumulative: bool       # a running total, so a change since last time is meaningful
    note: str = ""         # why the value is unknown


# --- fetching ---------------------------------------------------------------

class _SameSiteRedirects(urllib.request.HTTPRedirectHandler):
    """Follow a redirect only to the same scheme and host.

    urllib copies every request header, Authorization included, onto the
    redirected request, and will follow https to http and one host to another. A
    token must reach api.github.com and nowhere else, so any other redirect is
    left as a 3xx answer, which the caller reports as unavailable.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old, new = urllib.parse.urlsplit(req.full_url), urllib.parse.urlsplit(newurl)
        if (old.scheme, old.netloc) != (new.scheme, new.netloc):
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_OPENER = urllib.request.build_opener(_SameSiteRedirects)


def _read_body(resp, deadline: float) -> bytes:
    """Read at most MAX_BODY + 1 bytes, giving up at the deadline.

    A socket timeout restarts at every byte, so a server that drips one byte at a
    time would otherwise hold the run for as long as it likes.
    """
    chunks, size = [], 0
    while size <= MAX_BODY:
        if time.monotonic() > deadline:
            raise TimeoutError(f"took longer than {TOTAL_TIMEOUT} s")
        # read1 returns what has arrived; read(n) waits for all n bytes, so a
        # body dripped in a byte at a time would never reach the deadline check.
        chunk = getattr(resp, "read1", resp.read)(65536)
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
    return b"".join(chunks)[:MAX_BODY + 1]


def http_get(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    """GET a URL; return (status, body). Network trouble raises Unavailable."""
    host = url
    deadline = time.monotonic() + TOTAL_TIMEOUT
    try:
        host = urllib.parse.urlsplit(url).hostname or url
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **headers})
        try:
            with _OPENER.open(req, timeout=TIMEOUT) as resp:
                status, body = resp.status, _read_body(resp, deadline)
        except urllib.error.HTTPError as e:
            status, body = e.code, _read_body(e, deadline)
    except (urllib.error.URLError, OSError) as e:
        raise Unavailable(f"could not reach {host}: {getattr(e, 'reason', e)}") from None
    except ValueError:
        # http.client's message for a bad header quotes the header, token and all.
        raise Unavailable(f"could not reach {host}: not a request that can be sent") from None
    if len(body) > MAX_BODY:
        raise Unavailable(f"{host} sent more than {MAX_BODY:,} bytes")
    return status, body


def get_json(fetch, url: str, headers: dict[str, str] | None = None) -> object:
    status, body = fetch(url, headers or {})
    host = urllib.parse.urlsplit(url).hostname
    if status == 404:
        raise NotFound("not found (404)")
    if status == 401:
        raise Unavailable(f"{host} rejected the credentials (HTTP 401); check GITHUB_TOKEN")
    if status in (403, 429):
        raise Unavailable(f"{host} refused the request (HTTP {status}, rate limited?); try again later")
    if status != 200:
        raise Unavailable(f"{host} answered HTTP {status}")
    try:
        return json.loads(body.decode("utf-8"))
    except (ValueError, RecursionError):      # includes bad JSON and bad UTF-8
        raise Unavailable(f"{host} did not answer with JSON") from None


def count(obj: object, *path: str) -> int:
    """obj[path...] as a count: a non-negative integer, or Unavailable."""
    here = obj
    for key in path:
        if not isinstance(here, dict) or key not in here:
            raise Unavailable(f"unexpected response: no {'.'.join(path)}")
        here = here[key]
    if isinstance(here, bool) or not isinstance(here, int) or here < 0:
        raise Unavailable(f"unexpected response: {'.'.join(path)} is not a count")
    return here


def attempt(source: str, name: str, metric: str, cumulative: bool, read,
            missing: str = "not published (404)") -> Metric:
    try:
        return Metric(source, name, metric, read(), cumulative)
    except NotFound:
        return Metric(source, name, metric, None, cumulative, missing)
    except Unavailable as e:
        return Metric(source, name, metric, None, cumulative, str(e))


# --- the sources ------------------------------------------------------------

def github_metrics(fetch, repo: str, token: str | None) -> list[Metric]:
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    quoted = urllib.parse.quote(repo, safe="/")
    cache: dict[str, object] = {}
    gone = "repository not found (404)"

    def repo_obj() -> object:
        if "repo" not in cache:
            cache["repo"] = get_json(fetch, f"https://api.github.com/repos/{quoted}", headers)
        return cache["repo"]

    def open_issues() -> int:
        q = urllib.parse.quote(f"repo:{repo} is:issue is:open")
        found = get_json(fetch, f"https://api.github.com/search/issues?q={q}&per_page=1", headers)
        if isinstance(found, dict) and found.get("incomplete_results") is True:
            raise Unavailable("GitHub says this count may be incomplete (incomplete_results)")
        return count(found, "total_count")

    return [
        attempt("github", repo, "stars", True, lambda: count(repo_obj(), "stargazers_count"), gone),
        attempt("github", repo, "forks", True, lambda: count(repo_obj(), "forks_count"), gone),
        attempt("github", repo, "open_issues", False, open_issues, gone),
    ]


def crate_metrics(fetch, name: str) -> list[Metric]:
    quoted = urllib.parse.quote(name, safe="")
    cache: dict[str, object] = {}

    def crate() -> object:
        if "c" not in cache:
            cache["c"] = get_json(fetch, f"https://crates.io/api/v1/crates/{quoted}")
        return cache["c"]

    return [
        attempt("crates.io", name, "downloads_total", True, lambda: count(crate(), "crate", "downloads")),
        attempt("crates.io", name, "downloads_90d", False, lambda: count(crate(), "crate", "recent_downloads")),
    ]


def pypi_metrics(fetch, name: str) -> list[Metric]:
    url = f"https://pypistats.org/api/packages/{urllib.parse.quote(name, safe='')}/recent"
    return [attempt("pypi", name, "downloads_30d", False,
                    lambda: count(get_json(fetch, url), "data", "last_month"))]


def npm_metrics(fetch, name: str) -> list[Metric]:
    url = f"https://api.npmjs.org/downloads/point/last-month/{urllib.parse.quote(name, safe='@')}"
    return [attempt("npm", name, "downloads_30d", False,
                    lambda: count(get_json(fetch, url), "downloads"))]


def ghcr_metrics(repo: str) -> list[Metric]:
    return [Metric("ghcr.io", repo.lower(), "pulls", None, True,
                   "not measured: ghcr.io shows no pull count to a caller without credentials")]


# --- what the repository publishes --------------------------------------------

def read_small(path: Path) -> str:
    """The text of a regular file of at most MAX_FILE bytes.

    Not a device (/dev/zero never ends) and not a pipe (opening one can block
    forever), and not a file big enough to exhaust memory.
    """
    try:
        info = path.stat()
        if not stat.S_ISREG(info.st_mode):
            raise ConfigError(f"{path}: not a regular file")
        if info.st_size > MAX_FILE:
            raise ConfigError(f"{path}: larger than {MAX_FILE:,} bytes")
        with path.open("rb") as f:
            data = f.read(MAX_FILE + 1)
        if len(data) > MAX_FILE:
            raise ConfigError(f"{path}: larger than {MAX_FILE:,} bytes")
        return data.decode("utf-8")
    except OSError as e:
        raise ConfigError(f"{path}: {e.strerror or e}") from None
    except UnicodeDecodeError:
        raise ConfigError(f"{path}: not UTF-8 text") from None


def _read_toml(path: Path) -> dict:
    try:
        return tomllib.loads(read_small(path))
    except ValueError as e:                 # tomllib.TOMLDecodeError is a ValueError
        raise ConfigError(f"{path}: {e}") from None


def _name(value: object, where: Path) -> str:
    if not isinstance(value, str) or not NAME_OK.fullmatch(value):
        raise ConfigError(f"{where}: package name {value!r} is not a usable name")
    return value


def packages(root: Path) -> dict[str, list[str]]:
    """Package names the repository means to publish, from its own manifests."""
    # A package a manifest fails to name is an error, not a package left out: a
    # report that quietly omits one would still look complete.
    crates: list[str] = []
    root_manifest = root / "Cargo.toml"
    root_data = _read_toml(root_manifest) if root_manifest.exists() else {}
    inherited = root_data.get("workspace", {}).get("package", {}).get("publish") \
        if isinstance(root_data.get("workspace"), dict) and isinstance(root_data["workspace"].get("package"), dict) else None
    for manifest in [root_manifest, *sorted(root.glob("crates/*/Cargo.toml"))]:
        if not manifest.exists():
            continue
        data = root_data if manifest == root_manifest else _read_toml(manifest)
        if "package" not in data:
            if manifest == root_manifest:
                continue                # a workspace root with no package of its own
            raise ConfigError(f"{manifest}: no [package] table")
        pkg = data["package"]
        if not isinstance(pkg, dict) or "name" not in pkg:
            raise ConfigError(f"{manifest}: [package] has no name")
        publish = pkg.get("publish")
        if isinstance(publish, dict) and publish.get("workspace") is True:
            publish = inherited         # `publish.workspace = true` takes the workspace's value
        name = _name(pkg["name"], manifest)
        if publish is False or publish == []:
            continue                    # meant to stay unpublished
        crates.append(name)

    pypi: list[str] = []
    pyproject = root / "sdk" / "python" / "pyproject.toml"
    if pyproject.exists():
        data = _read_toml(pyproject)
        if "project" in data:
            project = data["project"]
            if not isinstance(project, dict) or "name" not in project:
                raise ConfigError(f"{pyproject}: [project] has no name")
            pypi.append(_name(project["name"], pyproject))

    npm: list[str] = []
    package_json = root / "sdk" / "typescript" / "package.json"
    if package_json.exists():
        try:
            data = json.loads(read_small(package_json))
        except (ValueError, RecursionError) as e:
            raise ConfigError(f"{package_json}: {e}") from None
        if not isinstance(data, dict):
            raise ConfigError(f"{package_json}: not a JSON object")
        if data.get("private") is not True:
            if "name" not in data:
                raise ConfigError(f"{package_json}: no name")
            npm.append(_name(data["name"], package_json))
    # One crate named in two manifests is still one crate.
    return {"crates": list(dict.fromkeys(crates)), "pypi": list(dict.fromkeys(pypi)),
            "npm": list(dict.fromkeys(npm))}


# --- putting it together --------------------------------------------------------

def collect(fetch, repo: str, pkgs: dict[str, list[str]], token: str | None = None,
            pause=time.sleep) -> list[Metric]:
    out = github_metrics(fetch, repo, token)
    for i, name in enumerate(pkgs["crates"]):
        if i:
            pause(CRATES_INTERVAL)
        out += crate_metrics(fetch, name)
    for name in pkgs["pypi"]:
        out += pypi_metrics(fetch, name)
    for name in pkgs["npm"]:
        out += npm_metrics(fetch, name)
    return out + ghcr_metrics(repo)


def key(m: Metric | dict) -> tuple[str, str, str]:
    g = (lambda k: m[k]) if isinstance(m, dict) else (lambda k: getattr(m, k))
    source, name = g("source"), g("name")
    # GitHub and ghcr.io names are case-insensitive, so Acme/Widget is acme/widget.
    return source, name.lower() if source in ("github", "ghcr.io") else name, g("metric")


def load_previous(path: Path, repo: str, now: datetime) -> dict[tuple[str, str, str], int]:
    """Counts from an earlier `--json` report, keyed by (source, name, metric).

    The report must be this repository's and from the past: a change worked out
    against another repository's numbers, or against a report that cannot have
    been made yet, would look like movement and mean nothing.
    """
    try:
        data = json.loads(read_small(path))
    except ConfigError as e:
        raise ConfigError(f"--previous {e}") from None
    except (ValueError, RecursionError) as e:
        raise ConfigError(f"--previous {path}: {e}") from None
    if not isinstance(data, dict) or not isinstance(data.get("metrics"), list):
        raise ConfigError(f"--previous {path}: not a report from this script (no \"metrics\" list)")
    metrics = data["metrics"]
    other = data.get("repository")
    if not isinstance(other, str) or other.lower() != repo.lower():
        raise ConfigError(f"--previous {path}: it is a report for {other!r}, not {repo!r}")
    try:
        made = datetime.strptime(str(data.get("generated_at")), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        raise ConfigError(f"--previous {path}: no valid generated_at") from None
    if made > now:
        raise ConfigError(f"--previous {path}: generated_at {data['generated_at']} is in the future")
    out: dict[tuple[str, str, str], int] = {}
    seen: set[tuple[str, str, str]] = set()
    for i, m in enumerate(metrics):
        if not isinstance(m, dict) or not all(isinstance(m.get(k), str) for k in ("source", "name", "metric")):
            raise ConfigError(f"--previous {path}: metrics[{i}] is not a metric")
        v = m.get("value")
        if v is not None and (isinstance(v, bool) or not isinstance(v, int) or v < 0):
            raise ConfigError(f"--previous {path}: metrics[{i}].value is not a count")
        if key(m) in seen:
            raise ConfigError(f"--previous {path}: {key(m)[0]} {key(m)[1]} {key(m)[2]} appears twice")
        seen.add(key(m))
        if v is not None:
            out[key(m)] = v
    return out


def change(m: Metric, previous: dict[tuple[str, str, str], int] | None) -> int | None:
    """Movement since the last report: only for a running total known both times."""
    if previous is None or not m.cumulative or m.value is None:
        return None
    before = previous.get(key(m))
    return None if before is None else m.value - before


def report_dict(repo: str, metrics: list[Metric], previous, now: datetime) -> dict:
    return {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repository": repo,
        "metrics": [{"source": m.source, "name": m.name, "metric": m.metric, "value": m.value,
                     "cumulative": m.cumulative, "change": change(m, previous), "note": m.note}
                    for m in metrics],
    }


def render(rep: dict) -> str:
    def cell(m: dict) -> tuple[str, ...]:
        if m["value"] is not None:
            value = f"{m['value']:,}"
        else:
            value = "not published" if m["note"].startswith("not published") else "unavailable"
        c = m["change"]
        moved = "" if c is None else ("0" if c == 0 else f"{c:+,}")
        return (m["source"], m["name"], m["metric"], value, moved)

    head = ("source", "package", "metric", "value", "change")
    body = [cell(m) for m in rep["metrics"]]
    widths = [max(len(h), *(len(r[i]) for r in body)) for i, h in enumerate(head)]

    def line(cells) -> str:
        return "  ".join(c.ljust(w) if i < 3 else c.rjust(w) for i, (c, w) in enumerate(zip(cells, widths))).rstrip()

    out = [f"Adoption report: {rep['repository']}", f"generated {rep['generated_at']}", "",
           line(head), *map(line, body)]
    # A package that is simply not published is already said in the table. What
    # is left is what could not be read, once per package and reason.
    unknown = dict.fromkeys((m["source"], m["name"], m["note"]) for m in rep["metrics"]
                            if m["value"] is None and not m["note"].startswith("not published"))
    if unknown:
        out += ["", "Not measured:"]
        out += [f"  {source} {name}: {note}" for source, name, note in unknown]
    return "\n".join(out)


def emit(text: str, out: Path | None) -> None:
    if out is not None:
        try:
            out.write_text(text + "\n", encoding="utf-8")
        except OSError as e:
            raise ConfigError(f"--out {out}: {e.strerror or e}") from None
        return
    try:
        print(text)
        sys.stdout.flush()
    except BrokenPipeError:
        try:       # leave nothing for the interpreter's flush at exit to fail on
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except (OSError, ValueError):
            pass


def main(argv: list[str] | None = None, *, fetch=http_get, now: datetime | None = None,
         root: Path = ROOT, pause=time.sleep) -> int:
    ap = argparse.ArgumentParser(description="Record stars, forks, issue volume and package downloads.")
    ap.add_argument("--repo", default=None,
                    help=f"owner/name on GitHub (default: $GITHUB_REPOSITORY, else {DEFAULT_REPO})")
    ap.add_argument("--previous", type=Path, metavar="FILE",
                    help="an earlier --json report, to show the change in running totals")
    ap.add_argument("--json", action="store_true", help="print the report as JSON")
    ap.add_argument("--out", type=Path, metavar="FILE", help="write the report to FILE instead of stdout")
    args = ap.parse_args(argv)
    # A reason from the operating system can hold characters the terminal cannot print.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="backslashreplace")

    when = now or datetime.now(timezone.utc)
    repo, origin = args.repo, "--repo"
    if repo is None:
        repo, origin = (os.environ["GITHUB_REPOSITORY"], "GITHUB_REPOSITORY") \
            if os.environ.get("GITHUB_REPOSITORY") else (DEFAULT_REPO, "the default repository")
    try:
        if not REPO_OK.fullmatch(repo):
            raise ConfigError(f"{origin} {repo!r} is not owner/name")
        token = os.environ.get("GITHUB_TOKEN", "").strip() or None
        if token and not TOKEN_OK.fullmatch(token):
            # Say what is wrong without repeating it.
            raise ConfigError("GITHUB_TOKEN is not a usable token (one word of printable ASCII)")
        previous = load_previous(args.previous, repo, when) if args.previous else None
        pkgs = packages(root)
    except ConfigError as e:
        print(f"adoption_report: {e}", file=sys.stderr)
        return 2

    metrics = collect(fetch, repo, pkgs, token, pause)
    rep = report_dict(repo, metrics, previous, when)
    # The GitHub numbers are the core of the report. Without them there is nothing to
    # track, and a file of "unavailable" must not be mistaken for a month's record.
    if not any(m.source == "github" and m.metric == "stars" and m.value is not None for m in metrics):
        stars = next(m for m in metrics if m.metric == "stars")
        print(f"adoption_report: could not read the GitHub numbers: {stars.note}", file=sys.stderr)
        return 2
    try:
        emit(json.dumps(rep, indent=2) if args.json else render(rep), args.out)
    except ConfigError as e:
        print(f"adoption_report: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
