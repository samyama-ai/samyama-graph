#!/usr/bin/env python3
"""The adoption report must never print a number it does not have.

A report that shows `0` where it really means "I could not look" reads as
"nobody downloaded it", and a month of those is worse than no report: it looks
like a record. So the cases here are mostly about what the script does when a
source misbehaves -- 404, rate limit, timeout, an answer that is not JSON or not
the shape we expect -- and about the numbers it does print being the ones read.

Everything runs offline against fake responses, except the network layer itself,
which is pointed at a throwaway server on 127.0.0.1.

Run: python3 scripts/test_adoption_report.py
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "adoption_report.py"

spec = importlib.util.spec_from_file_location("adoption_report", SCRIPT)
mod = importlib.util.module_from_spec(spec)
sys.modules["adoption_report"] = mod        # a dataclass looks its own module up by name
spec.loader.exec_module(mod)

NOW = datetime(2026, 11, 2, 4, 30, 0, tzinfo=timezone.utc)
REPO = "acme/widget"


def j(obj) -> tuple[int, bytes]:
    return 200, json.dumps(obj).encode()


class Fake:
    """A stand-in for http_get: routes are URL prefixes."""

    def __init__(self, routes=None):
        self.routes = dict(routes or {})
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, url, headers):
        self.calls.append((url, dict(headers)))
        for prefix, resp in self.routes.items():
            if url.startswith(prefix):
                if isinstance(resp, Exception):
                    raise resp
                return resp
        return 404, b"{}"


def good_routes(**more):
    routes = {
        f"https://api.github.com/repos/{REPO}": j({"stargazers_count": 185, "forks_count": 18}),
        "https://api.github.com/search/issues": j({"total_count": 217}),
        "https://crates.io/api/v1/crates/widget": j({"crate": {"downloads": 340, "recent_downloads": 81}}),
        "https://pypistats.org/api/packages/widget/recent": j({"data": {"last_month": 997}}),
        "https://api.npmjs.org/downloads/point/last-month/widget-sdk": j({"downloads": 593}),
    }
    for prefix in more:                       # an override replaces what it covers
        for default in [k for k in routes if k.startswith(prefix)]:
            del routes[default]
    routes.update(more)
    return routes


def make_root(td: Path, crates=("widget",), pypi="widget", npm="widget-sdk") -> Path:
    root = Path(td) / "repo"
    (root / "crates").mkdir(parents=True)
    (root / "Cargo.toml").write_text("[workspace]\nmembers = []\n")
    for c in crates:
        (root / "crates" / c).mkdir(parents=True, exist_ok=True)
        (root / "crates" / c / "Cargo.toml").write_text(f'[package]\nname = "{c}"\nversion = "1.0.0"\n')
    if pypi:
        (root / "sdk" / "python").mkdir(parents=True)
        (root / "sdk" / "python" / "pyproject.toml").write_text(f'[project]\nname = "{pypi}"\n')
    if npm:
        (root / "sdk" / "typescript").mkdir(parents=True)
        (root / "sdk" / "typescript" / "package.json").write_text(json.dumps({"name": npm}))
    return root


def run(argv=(), fetch=None, root=None, env=None, repo=REPO):
    """main() in-process; returns (exit code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    saved = {k: os.environ.get(k) for k in ("GITHUB_TOKEN", "GITHUB_REPOSITORY")}
    os.environ.pop("GITHUB_TOKEN", None)
    os.environ.pop("GITHUB_REPOSITORY", None)
    os.environ.update(env or {})
    try:
        with tempfile.TemporaryDirectory() as td, contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                rc = mod.main((["--repo", repo] if repo and "--repo" not in argv else []) + list(argv),
                              fetch=fetch or Fake(good_routes()), now=NOW,
                              root=root or make_root(td), pause=lambda s: None)
            except SystemExit as e:
                rc = e.code
    finally:
        for k, v in saved.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v
    return rc, out.getvalue(), err.getvalue()


def metrics_of(argv=(), fetch=None, root=None):
    rc, out, err = run(["--json", *argv], fetch, root)
    assert rc == 0, (rc, err)
    return {(m["source"], m["name"], m["metric"]): m for m in json.loads(out)["metrics"]}


# --- the numbers that are printed are the numbers that were read ------------

def t_every_source_is_read_and_reported():
    m = metrics_of()
    assert m[("github", REPO, "stars")]["value"] == 185
    assert m[("github", REPO, "forks")]["value"] == 18
    assert m[("github", REPO, "open_issues")]["value"] == 217
    assert m[("crates.io", "widget", "downloads_total")]["value"] == 340
    assert m[("crates.io", "widget", "downloads_90d")]["value"] == 81
    assert m[("pypi", "widget", "downloads_30d")]["value"] == 997
    assert m[("npm", "widget-sdk", "downloads_30d")]["value"] == 593


def t_a_real_zero_is_reported_as_zero():
    routes = good_routes(**{"https://api.npmjs.org/downloads/point/last-month/widget-sdk": j({"downloads": 0})})
    assert metrics_of(fetch=Fake(routes))[("npm", "widget-sdk", "downloads_30d")]["value"] == 0


def t_the_report_names_when_it_was_made():
    rc, out, _ = run(["--json"])
    assert json.loads(out)["generated_at"] == "2026-11-02T04:30:00Z"
    assert "generated 2026-11-02T04:30:00Z" in run()[1]


def t_the_container_image_is_listed_as_not_measured():
    g = metrics_of()[("ghcr.io", REPO, "pulls")]
    assert g["value"] is None and "not measured" in g["note"]
    assert "ghcr.io" in run()[1]


# --- an unknown number is never a zero ----------------------------------------

BAD_BODIES = {
    "http 500": (500, b"oops"),
    "http 403": (403, b"{}"),
    "http 429": (429, b"{}"),
    "not json": (200, b"<html>maintenance</html>"),
    "not utf-8": (200, b"\xff\xfe\x00"),
    "empty": (200, b""),
    "a list": j([1, 2, 3]),
    "null": j(None),
    "no data": j({"data": {}}),
    "string count": j({"downloads": "593", "data": {"last_month": "997"}, "crate": {"downloads": "1"}}),
    "negative": j({"downloads": -1, "data": {"last_month": -1}, "crate": {"downloads": -1, "recent_downloads": -1}}),
    "boolean": j({"downloads": True, "data": {"last_month": True}, "crate": {"downloads": True, "recent_downloads": True}}),
    "float": j({"downloads": 5.5, "data": {"last_month": 5.5}, "crate": {"downloads": 5.5, "recent_downloads": 5.5}}),
    "null count": j({"downloads": None, "data": {"last_month": None}, "crate": {"downloads": None, "recent_downloads": None}}),
    "too deep": (200, b"[" * 100000 + b"]" * 100000),
    "network": mod.Unavailable("could not reach host: timed out"),
}


def t_a_bad_answer_is_unavailable_never_zero():
    for label, bad in BAD_BODIES.items():
        routes = good_routes(**{
            "https://crates.io/": bad, "https://pypistats.org/": bad, "https://api.npmjs.org/": bad})
        m = metrics_of(fetch=Fake(routes))
        for k in (("crates.io", "widget", "downloads_total"), ("crates.io", "widget", "downloads_90d"),
                  ("pypi", "widget", "downloads_30d"), ("npm", "widget-sdk", "downloads_30d")):
            assert m[k]["value"] is None and m[k]["note"], (label, k, m[k])
        assert m[("github", REPO, "stars")]["value"] == 185, label      # the rest still arrives


def t_a_404_means_not_published():
    routes = good_routes(**{"https://crates.io/": (404, b'{"errors":[]}')})
    m = metrics_of(fetch=Fake(routes))[("crates.io", "widget", "downloads_total")]
    assert m["value"] is None and m["note"] == "not published (404)"
    rc, out, _ = run(fetch=Fake(routes))
    row = next(l for l in out.splitlines() if l.startswith("crates.io") and "downloads_total" in l)
    assert "not published" in row and "unavailable" not in row, row


def t_nothing_unknown_is_printed_as_a_number():
    for label, bad in BAD_BODIES.items():
        routes = good_routes(**{"https://crates.io/": bad, "https://pypistats.org/": bad})
        _, out, _ = run(fetch=Fake(routes))
        for line in out.splitlines():
            if line.startswith(("crates.io", "pypi")):
                assert " 0" not in line.replace("  ", " ") or "unavailable" in line or "not published" in line, (label, line)
                assert ("unavailable" in line or "not published" in line), (label, line)


def t_a_null_recent_downloads_does_not_hide_the_total():
    routes = good_routes(**{"https://crates.io/": j({"crate": {"downloads": 340, "recent_downloads": None}})})
    m = metrics_of(fetch=Fake(routes))
    assert m[("crates.io", "widget", "downloads_total")]["value"] == 340
    assert m[("crates.io", "widget", "downloads_90d")]["value"] is None


def t_open_issues_failing_does_not_lose_the_stars():
    routes = good_routes(**{"https://api.github.com/search/issues": (403, b"{}")})
    rc, out, _ = run(["--json"], fetch=Fake(routes))
    m = {x["metric"]: x for x in json.loads(out)["metrics"] if x["source"] == "github"}
    assert rc == 0 and m["stars"]["value"] == 185 and m["forks"]["value"] == 18
    assert m["open_issues"]["value"] is None and "rate limited" in m["open_issues"]["note"]


def t_without_the_github_numbers_there_is_no_report():
    # A file of "unavailable" must not be mistaken for a month's record.
    for bad in ((500, b"x"), (403, b"{}"), mod.Unavailable("could not reach api.github.com: down"),
                (200, b"not json"), j({"forks_count": 3}), j({"stargazers_count": -4})):
        routes = good_routes(**{f"https://api.github.com/repos/{REPO}": bad})
        rc, out, err = run(fetch=Fake(routes))
        assert rc == 2 and out == "" and "GitHub" in err and "Traceback" not in err, (bad, rc, out, err)


def t_a_repository_that_does_not_exist_exits_2():
    rc, out, err = run(fetch=Fake({f"https://api.github.com/repos/{REPO}": (404, b"{}")}))
    assert rc == 2 and "not found" in err.lower() and out == "", (rc, out, err)


# --- what the repository publishes -----------------------------------------------

def t_packages_come_from_the_manifests():
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=("b-crate", "a-crate"), pypi="py-pkg", npm="@acme/js-sdk")
        assert mod.packages(root) == {"crates": ["a-crate", "b-crate"], "pypi": ["py-pkg"], "npm": ["@acme/js-sdk"]}


def t_the_root_package_is_included_and_a_bare_workspace_is_not():
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=(), pypi=None, npm=None)
        assert mod.packages(root)["crates"] == []
        (root / "Cargo.toml").write_text('[package]\nname = "rootcrate"\nversion.workspace = true\n[workspace]\n')
        assert mod.packages(root)["crates"] == ["rootcrate"]


def t_unpublished_by_design_is_not_reported():
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=("keep", "no", "none"), pypi=None, npm=None)
        (root / "crates" / "no" / "Cargo.toml").write_text('[package]\nname = "no"\npublish = false\n')
        (root / "crates" / "none" / "Cargo.toml").write_text('[package]\nname = "none"\npublish = []\n')
        assert mod.packages(root)["crates"] == ["keep"]
        (root / "crates" / "none" / "Cargo.toml").write_text('[package]\nname = "none"\npublish = ["my-registry"]\n')
        assert mod.packages(root)["crates"] == ["keep", "none"]
        (root / "sdk" / "typescript").mkdir(parents=True)
        (root / "sdk" / "typescript" / "package.json").write_text('{"name": "internal", "private": true}')
        assert mod.packages(root)["npm"] == []


def t_missing_manifests_are_fine():
    with tempfile.TemporaryDirectory() as td:
        (Path(td) / "empty").mkdir()
        assert mod.packages(Path(td) / "empty") == {"crates": [], "pypi": [], "npm": []}
        rc, out, _ = run(root=Path(td) / "empty")
        assert rc == 0 and "github" in out


def t_an_unusable_manifest_exits_2_not_a_traceback():
    cases = {
        "bad toml": ("crates/x/Cargo.toml", "[package\nname ="),
        "bad json": ("sdk/typescript/package.json", "{not json"),
        "bad name": ("crates/x/Cargo.toml", '[package]\nname = "../etc/passwd"\n'),
        "name with space": ("crates/x/Cargo.toml", '[package]\nname = "a b"\n'),
        "non-ascii name": ("crates/x/Cargo.toml", '[package]\nname = "café"\n'),
        "name not text": ("crates/x/Cargo.toml", "[package]\nname = 5\n"),
        "empty name": ("crates/x/Cargo.toml", '[package]\nname = ""\n'),
    }
    for label, (rel, text) in cases.items():
        with tempfile.TemporaryDirectory() as td:
            root = make_root(td, crates=(), pypi=None, npm=None)
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text)
            fake = Fake(good_routes())
            rc, out, err = run(fetch=fake, root=root)
            assert rc == 2 and out == "" and "adoption_report:" in err and "Traceback" not in err, (label, rc, err)
            assert fake.calls == [], label          # refused before any request


def t_scoped_npm_names_are_requested_correctly():
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=(), pypi=None, npm="@acme/js-sdk")
        fake = Fake(good_routes())
        run(fetch=fake, root=root)
    urls = [u for u, _ in fake.calls if "npmjs" in u]
    assert urls == ["https://api.npmjs.org/downloads/point/last-month/@acme%2Fjs-sdk"], urls


# --- requests ----------------------------------------------------------------------

def t_crates_io_is_asked_politely_one_request_a_second():
    pauses = []
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=("a", "b", "c"), pypi=None, npm=None)
        mod.collect(Fake(good_routes()), REPO, mod.packages(root), None, pauses.append)
    assert pauses == [mod.CRATES_INTERVAL, mod.CRATES_INTERVAL], pauses
    assert mod.CRATES_INTERVAL >= 1.0


def t_each_crate_is_fetched_once_for_both_of_its_numbers():
    fake = Fake(good_routes())
    run(fetch=fake)
    assert sum("crates.io" in u for u, _ in fake.calls) == 1
    assert sum(u.startswith(f"https://api.github.com/repos/{REPO}") for u, _ in fake.calls) == 1


def t_the_token_goes_only_to_github_and_is_never_printed():
    secret = "ghp_" + "a1B2" * 9
    fake = Fake(good_routes())
    rc, out, err = run(["--json"], fetch=fake, env={"GITHUB_TOKEN": secret})
    assert rc == 0 and secret not in out and secret not in err
    for url, headers in fake.calls:
        if url.startswith("https://api.github.com/"):
            assert headers.get("Authorization") == f"Bearer {secret}", url
        else:
            assert "Authorization" not in headers, url
    fake = Fake(good_routes())
    run(fetch=fake, env={"GITHUB_TOKEN": ""})
    assert all("Authorization" not in h for _, h in fake.calls)


def t_the_repository_is_checked_before_it_goes_into_a_url():
    for bad in ("nope", "a/b/c", "../x", "a b/c", "a/b?x=1", "a/b#frag", "/a/b", "a/", "", "a/b\n"):
        fake = Fake(good_routes())
        rc, out, err = run(["--repo", bad], fetch=fake)
        assert rc == 2 and "owner/name" in err and fake.calls == [], (bad, rc, err)


def t_the_repository_defaults_from_the_environment():
    fake = Fake({"https://api.github.com/repos/other/thing": j({"stargazers_count": 1, "forks_count": 2}),
                 "https://api.github.com/search/issues": j({"total_count": 3})})
    with tempfile.TemporaryDirectory() as td:
        rc, out, _ = run(["--json"], fetch=fake, root=make_root(td, (), None, None),
                         env={"GITHUB_REPOSITORY": "other/thing"}, repo=None)
    assert json.loads(out)["repository"] == "other/thing"
    assert any("q=repo%3Aother/thing%20is%3Aissue" in u for u, _ in fake.calls), fake.calls


def t_the_issue_search_counts_issues_not_pull_requests():
    fake = Fake(good_routes())
    run(fetch=fake)
    q = next(u for u, _ in fake.calls if "search/issues" in u)
    assert "is%3Aissue" in q and "is%3Aopen" in q, q


# --- movement since last time --------------------------------------------------------

PREV_HEAD = {"repository": REPO, "generated_at": "2026-10-02T04:30:00Z"}


def previous_file(td, **changes):
    now = metrics_of()
    rep = {"repository": REPO, "generated_at": "2026-10-02T04:30:00Z",
           "metrics": [{**m, "value": changes.get(m["metric"], m["value"])} for m in now.values()]}
    p = Path(td) / "prev.json"
    p.write_text(json.dumps(rep))
    return p


def t_the_change_is_shown_for_running_totals_only():
    with tempfile.TemporaryDirectory() as td:
        prev = previous_file(td, stars=180, forks=18, downloads_total=300, downloads_30d=900, open_issues=200)
        rc, out, _ = run(["--json", "--previous", str(prev)])
    m = {(x["source"], x["metric"]): x for x in json.loads(out)["metrics"]}
    assert m[("github", "stars")]["change"] == 5
    assert m[("github", "forks")]["change"] == 0
    assert m[("crates.io", "downloads_total")]["change"] == 40
    # a window already describes a period; differencing it would mean nothing
    assert m[("pypi", "downloads_30d")]["change"] is None
    assert m[("github", "open_issues")]["change"] is None
    assert m[("crates.io", "downloads_90d")]["change"] is None


def t_a_fall_is_a_negative_change_and_shown_with_its_sign():
    with tempfile.TemporaryDirectory() as td:
        prev = previous_file(td, stars=190)
        rc, out, _ = run(["--previous", str(prev)])
    line = next(l for l in out.splitlines() if l.startswith("github") and "stars" in l)
    assert line.rstrip().endswith("-5"), line
    with tempfile.TemporaryDirectory() as td:
        rc, out, _ = run(["--previous", str(previous_file(td, stars=180))])
    assert next(l for l in out.splitlines() if " stars " in l).rstrip().endswith("+5")


def t_no_change_is_not_shown_when_either_side_is_unknown():
    with tempfile.TemporaryDirectory() as td:
        prev = previous_file(td, stars=None)
        rc, out, _ = run(["--json", "--previous", str(prev)])
        m = {(x["source"], x["metric"]): x for x in json.loads(out)["metrics"]}
        assert m[("github", "stars")]["change"] is None
        # unknown now: the old number must not be turned into a fake drop to zero
        routes = good_routes(**{"https://crates.io/": (500, b"x")})
        prev = previous_file(td, downloads_total=300)
        rc, out, _ = run(["--json", "--previous", str(prev)], fetch=Fake(routes))
        m = {(x["source"], x["metric"]): x for x in json.loads(out)["metrics"]}
        assert m[("crates.io", "downloads_total")]["change"] is None
        # a package the previous report never had
        (Path(td) / "p2.json").write_text(json.dumps({**PREV_HEAD, "metrics": []}))
        rc, out, _ = run(["--json", "--previous", str(Path(td) / "p2.json")])
        assert all(x["change"] is None for x in json.loads(out)["metrics"])


def t_a_previous_report_that_cannot_be_used_exits_2():
    good = {"source": "github", "name": "a/b", "metric": "stars", "value": 5}
    cases = {
        "missing file": None,
        "not json": "{oops",
        "a list": "[]",
        "no metrics": json.dumps(PREV_HEAD),
        "metrics not a list": json.dumps({**PREV_HEAD, "metrics": {}}),
        "entry not an object": json.dumps({**PREV_HEAD, "metrics": [5]}),
        "entry without a name": json.dumps({**PREV_HEAD, "metrics": [{"source": "x", "metric": "y", "value": 1}]}),
        "value is text": json.dumps({**PREV_HEAD, "metrics": [{**good, "value": "5"}]}),
        "value is negative": json.dumps({**PREV_HEAD, "metrics": [{**good, "value": -5}]}),
        "value is a boolean": json.dumps({**PREV_HEAD, "metrics": [{**good, "value": True}]}),
        "value is a float": json.dumps({**PREV_HEAD, "metrics": [{**good, "value": 1.5}]}),
        "too deep": "[" * 100000 + "]" * 100000,
    }
    for label, text in cases.items():
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "prev.json"
            if text is not None:
                p.write_text(text)
            fake = Fake(good_routes())
            rc, out, err = run(["--previous", str(p)], fetch=fake)
            assert rc == 2 and out == "" and "--previous" in err and "Traceback" not in err, (label, rc, err)
            assert fake.calls == [], label


def t_a_null_value_in_the_previous_report_is_allowed():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "prev.json"
        p.write_text(json.dumps({**PREV_HEAD, "metrics": [{"source": "github", "name": REPO, "metric": "stars", "value": None}]}))
        assert run(["--previous", str(p)])[0] == 0


def t_a_report_can_be_fed_back_as_the_next_previous():
    with tempfile.TemporaryDirectory() as td:
        first = Path(td) / "first.json"
        assert run(["--json", "--out", str(first)])[0] == 0
        rc, out, _ = run(["--json", "--previous", str(first)])
    assert rc == 0
    m = {(x["source"], x["metric"]): x for x in json.loads(out)["metrics"]}
    assert m[("github", "stars")]["change"] == 0


# --- output -----------------------------------------------------------------------------

def t_json_output_is_valid_and_complete():
    data = json.loads(run(["--json"])[1])
    assert set(data) == {"generated_at", "repository", "metrics"}
    for m in data["metrics"]:
        assert set(m) == {"source", "name", "metric", "value", "cumulative", "change", "note"}
        assert m["value"] is None or (isinstance(m["value"], int) and m["value"] >= 0)


def t_out_writes_a_file_and_prints_nothing():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "r.txt"
        rc, out, err = run(["--out", str(p)])
        assert rc == 0 and out == "" and "stars" in p.read_text() and p.read_text().endswith("\n")


def t_an_unwritable_out_path_exits_2_after_a_good_fetch():
    for bad in ("/nonexistent/dir/r.txt", "/"):
        rc, out, err = run(["--out", bad])
        assert rc == 2 and "--out" in err and "Traceback" not in err, (bad, rc, err)


def t_the_table_lines_up():
    _, out, _ = run()
    lines = out.splitlines()
    head = next(l for l in lines if l.startswith("source"))
    rows = [l.ljust(len(head)) for l in lines[lines.index(head) + 1:]
            if l.startswith(("github", "crates", "pypi", "npm", "ghcr"))]
    assert len(rows) >= 5
    end = head.index("value") + len("value")
    for row in rows:                                   # the value column is right-aligned
        assert row[end - 1] != " " and row[end:end + 2] == "  ", (head, row)
        assert row[head.index("package") - 1] == " " and row[head.index("package")] != " ", (head, row)


def t_not_measured_lists_each_reason_once():
    routes = good_routes(**{"https://crates.io/": (500, b"x")})
    with tempfile.TemporaryDirectory() as td:
        _, out, _ = run(fetch=Fake(routes), root=make_root(td, ("a", "b"), None, None))
    notes = out.split("Not measured:")[1]
    assert notes.count("crates.io a:") == 1 and notes.count("crates.io b:") == 1, notes


def t_bad_options_exit_2():
    for bad in (["--nope"], ["--previous"], ["--out"], ["stray"]):
        rc, out, err = run(bad)
        assert rc == 2 and out == "", (bad, rc)


def t_a_closed_pipe_is_not_an_error():
    code = ("import sys; sys.path.insert(0, %r); import adoption_report as a; a.emit('x' * 2_000_000, None)" % str(HERE))
    proc = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    proc.stdout.read(10)
    proc.stdout.close()
    err = proc.stderr.read().decode()
    assert proc.wait() == 0 and err == "", err[-300:]


# --- the network layer, against a real local server -----------------------------------------

class QuietServer(HTTPServer):
    def handle_error(self, request, client_address):     # a client that gave up is not news
        pass


class Handler(BaseHTTPRequestHandler):
    seen: list[dict] = []

    def log_message(self, *a):
        pass

    def do_GET(self):
        Handler.seen.append(dict(self.headers))
        if self.path == "/ok":
            body, status = b'{"n": 5}', 200
        elif self.path == "/gone":
            body, status = b'{"errors": []}', 404
        elif self.path == "/boom":
            body, status = b"server error", 500
        elif self.path == "/big":
            body, status = b"x" * 50, 200
        elif self.path == "/slow":
            time.sleep(1.0)
            body, status = b"{}", 200
        else:
            body, status = b"{}", 200
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@contextlib.contextmanager
def server():
    srv = QuietServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        yield f"http://127.0.0.1:{srv.server_port}"
    finally:
        srv.shutdown()
        srv.server_close()


def t_http_get_returns_status_and_body_for_any_status():
    with server() as base:
        assert mod.http_get(base + "/ok", {}) == (200, b'{"n": 5}')
        assert mod.http_get(base + "/gone", {})[0] == 404
        assert mod.http_get(base + "/boom", {}) == (500, b"server error")


def t_http_get_identifies_itself_and_forwards_headers():
    Handler.seen.clear()
    with server() as base:
        mod.http_get(base + "/ok", {"X-Test": "yes"})
    assert "samyama-adoption-report" in Handler.seen[0]["User-Agent"]
    assert Handler.seen[0]["X-Test"] == "yes"


def t_http_get_refuses_an_oversized_body():
    old = mod.MAX_BODY
    mod.MAX_BODY = 10
    try:
        with server() as base:
            try:
                mod.http_get(base + "/big", {})
            except mod.Unavailable as e:
                assert "more than" in str(e)
            else:
                raise AssertionError("an oversized body was accepted")
    finally:
        mod.MAX_BODY = old


def t_http_get_times_out_instead_of_hanging():
    old = mod.TIMEOUT
    mod.TIMEOUT = 0.3
    try:
        with server() as base:
            started = time.monotonic()
            try:
                mod.http_get(base + "/slow", {})
            except mod.Unavailable as e:
                assert "could not reach" in str(e)
            else:
                raise AssertionError("no timeout")
            assert time.monotonic() - started < 0.95
    finally:
        mod.TIMEOUT = old


def t_http_get_reports_a_refused_connection():
    srv = HTTPServer(("127.0.0.1", 0), Handler)
    port = srv.server_port
    srv.server_close()          # nothing is listening on this port now
    for url in (f"http://127.0.0.1:{port}/x", "http://no-such-host.invalid/x", "not a url", "ftp://x/y", ""):
        try:
            mod.http_get(url, {})
        except mod.Unavailable as e:
            assert "could not reach" in str(e) and "Traceback" not in str(e), (url, e)
        else:
            raise AssertionError(f"{url!r} did not fail")


def t_get_json_turns_statuses_into_the_right_exception():
    with server() as base:
        assert mod.get_json(mod.http_get, base + "/ok") == {"n": 5}
        for path, kind in (("/gone", mod.NotFound), ("/boom", mod.Unavailable)):
            try:
                mod.get_json(mod.http_get, base + path)
            except kind as e:
                assert type(e) is kind, (path, e)
            else:
                raise AssertionError(path)


# --- review of the first draft: the token, redirects, wrong baselines, silent omissions ----

def t_the_token_is_never_put_into_an_error_message():
    # A trailing newline is common when a token is read from a file. http.client's
    # error for it quotes the header, token included.
    secret = "ghp_TOPSECRET123"
    for raw in (secret + "\n", secret + " \n", "  " + secret, secret + "\r\n"):
        fake = Fake(good_routes())
        rc, out, err = run(["--json"], fetch=fake, env={"GITHUB_TOKEN": raw})
        assert rc == 0 and secret not in out + err, (raw, rc, err)          # whitespace is trimmed
        assert all(h.get("Authorization") == f"Bearer {secret}" for u, h in fake.calls if "github" in u), raw
    for bad in (secret + " x", "tok\x01en", "tok\ten", "tökén", "a\nb"):      # (an env var cannot hold NUL)
        fake = Fake(good_routes())
        rc, out, err = run(fetch=fake, env={"GITHUB_TOKEN": bad})
        assert rc == 2 and "GITHUB_TOKEN" in err and out == "" and fake.calls == [], (bad, rc, err)
        assert "ken" not in err.replace("token", "").replace("TOKEN", ""), err       # not echoed
    # even if one reached the network layer, http.client's text is not repeated
    try:
        mod.http_get("http://127.0.0.1:9/", {"Authorization": "Bearer " + secret + "\n"})
    except mod.Unavailable as e:
        assert secret not in str(e), str(e)


def t_a_redirect_to_another_host_does_not_carry_the_token_along():
    seen = {}

    class Target(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            seen["auth"] = self.headers.get("Authorization")
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

    target = QuietServer(("127.0.0.1", 0), Target)
    threading.Thread(target=target.serve_forever, daemon=True).start()

    class Redirect(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            where = {"/other-host": f"http://localhost:{target.server_port}/x",
                     "/other-port": f"http://127.0.0.1:{target.server_port}/x",
                     "/downgrade": "http://127.0.0.1:1/x",
                     "/same": f"http://127.0.0.1:{self.server.server_port}/final"}.get(self.path)
            if self.path == "/final":
                self.send_response(200)
                self.send_header("Content-Length", "6")
                self.end_headers()
                self.wfile.write(b'{"a":1}')
                seen["final_auth"] = self.headers.get("Authorization")
                return
            self.send_response(302)
            self.send_header("Location", where)
            self.send_header("Content-Length", "0")
            self.end_headers()

    redirector = QuietServer(("127.0.0.1", 0), Redirect)
    threading.Thread(target=redirector.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{redirector.server_port}"
    try:
        for path in ("/other-host", "/other-port"):
            seen.clear()
            status, _ = mod.http_get(base + path, {"Authorization": "Bearer ghp_X"})
            assert status == 302 and "auth" not in seen, (path, status, seen)       # not followed
            try:
                mod.get_json(mod.http_get, base + path, {"Authorization": "Bearer ghp_X"})
            except mod.Unavailable as e:
                assert "302" in str(e)
            else:
                raise AssertionError("a refused redirect was read as an answer")
        seen.clear()
        status, body = mod.http_get(base + "/same", {"Authorization": "Bearer ghp_X"})   # same site: allowed
        assert status == 200 and seen.get("final_auth") == "Bearer ghp_X", (status, seen)
    finally:
        target.shutdown()
        redirector.shutdown()


def t_an_incomplete_issue_count_is_not_reported_as_exact():
    routes = good_routes(**{"https://api.github.com/search/issues": j({"total_count": 5, "incomplete_results": True})})
    m = metrics_of(fetch=Fake(routes))[("github", REPO, "open_issues")]
    assert m["value"] is None and "incomplete" in m["note"], m
    routes = good_routes(**{"https://api.github.com/search/issues": j({"total_count": 5, "incomplete_results": False})})
    assert metrics_of(fetch=Fake(routes))[("github", REPO, "open_issues")]["value"] == 5


def t_a_previous_report_for_another_repository_is_refused():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "p.json"
        for head in ({"repository": "other/x", "generated_at": "2026-10-02T04:30:00Z"},
                     {"generated_at": "2026-10-02T04:30:00Z"},
                     {"repository": 5, "generated_at": "2026-10-02T04:30:00Z"}):
            p.write_text(json.dumps({**head, "metrics": []}))
            rc, out, err = run(["--previous", str(p)])
            assert rc == 2 and "--previous" in err and "not 'acme/widget'" in err and out == "", (head, rc, err)


def t_the_repository_name_is_compared_without_regard_to_case():
    with tempfile.TemporaryDirectory() as td:
        first = Path(td) / "first.json"
        assert run(["--json", "--out", str(first), "--repo", "Acme/Widget"],
                   fetch=Fake({"https://api.github.com/repos/Acme/Widget": j({"stargazers_count": 100, "forks_count": 1}),
                               "https://api.github.com/search/issues": j({"total_count": 1})}))[0] == 0
        rc, out, _ = run(["--json", "--previous", str(first)],
                         fetch=Fake({f"https://api.github.com/repos/{REPO}": j({"stargazers_count": 105, "forks_count": 1}),
                                     "https://api.github.com/search/issues": j({"total_count": 1})}))
    m = {(x["source"], x["metric"]): x for x in json.loads(out)["metrics"]}
    assert rc == 0 and m[("github", "stars")]["change"] == 5, m[("github", "stars")]


def t_a_previous_report_with_the_same_metric_twice_is_refused():
    entry = {"source": "github", "name": REPO, "metric": "stars"}
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "p.json"
        for names in ((REPO, REPO), (REPO, REPO.upper()), (REPO, REPO.title())):
            p.write_text(json.dumps({**PREV_HEAD, "metrics": [{**entry, "name": names[0], "value": 100},
                                                               {**entry, "name": names[1], "value": 180}]}))
            rc, out, err = run(["--previous", str(p)])
            assert rc == 2 and "appears twice" in err, (names, rc, err)


def t_a_previous_report_from_the_future_or_undated_is_refused():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "p.json"
        for when in ("2099-01-01T00:00:00Z", "2026-11-02T04:30:01Z", "yesterday", "2026-10-02", "", None, 5, "2026-10-02T04:30:00+00:00"):
            body = {"repository": REPO, "metrics": []}
            if when is not None:
                body["generated_at"] = when
            p.write_text(json.dumps(body))
            rc, out, err = run(["--previous", str(p)])
            assert rc == 2 and "generated_at" in err, (when, rc, err)
        p.write_text(json.dumps({"repository": REPO, "generated_at": "2026-11-02T04:30:00Z", "metrics": []}))
        assert run(["--previous", str(p)])[0] == 0                       # made at this very moment: fine


def t_a_crate_named_twice_is_one_crate():
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=("widget",), pypi=None, npm=None)
        (root / "Cargo.toml").write_text('[package]\nname = "widget"\nversion = "1.0.0"\n')
        assert mod.packages(root)["crates"] == ["widget"]
        fake = Fake(good_routes())
        rc, out, _ = run(["--json"], fetch=fake, root=root)
    keys = [(m["source"], m["name"], m["metric"]) for m in json.loads(out)["metrics"]]
    assert len(keys) == len(set(keys)), keys
    assert sum("crates.io" in u for u, _ in fake.calls) == 1


def t_a_manifest_that_does_not_name_its_package_is_an_error_not_an_omission():
    cases = {
        "crate without a name": ("crates/x/Cargo.toml", '[package]\nversion = "1"\n'),
        "package is not a table": ("crates/x/Cargo.toml", 'package = "x"\n'),
        "crate without [package]": ("crates/x/Cargo.toml", '[workspace]\nmembers = []\n'),
        "root package without a name": ("Cargo.toml", '[package]\nversion = "1"\n'),
        "project without a name": ("sdk/python/pyproject.toml", '[project]\nversion = "1"\n'),
        "project is not a table": ("sdk/python/pyproject.toml", 'project = "x"\n'),
        "package.json is a list": ("sdk/typescript/package.json", '[{"name": "n"}]'),
        "package.json is a string": ("sdk/typescript/package.json", '"n"'),
        "package.json has no name": ("sdk/typescript/package.json", '{"version": "1"}'),
        "package.json name is a number": ("sdk/typescript/package.json", '{"name": 5}'),
    }
    for label, (rel, text) in cases.items():
        with tempfile.TemporaryDirectory() as td:
            root = make_root(td, crates=(), pypi=None, npm=None)
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text)
            fake = Fake(good_routes())
            rc, out, err = run(fetch=fake, root=root)
            assert rc == 2 and out == "" and "Traceback" not in err and fake.calls == [], (label, rc, err)


def t_publish_workspace_true_follows_the_workspace():
    with tempfile.TemporaryDirectory() as td:
        root = make_root(td, crates=("priv", "pub"), pypi=None, npm=None)
        (root / "Cargo.toml").write_text('[workspace]\nmembers = []\n[workspace.package]\npublish = false\n')
        (root / "crates" / "priv" / "Cargo.toml").write_text('[package]\nname = "priv"\npublish.workspace = true\n')
        (root / "crates" / "pub" / "Cargo.toml").write_text('[package]\nname = "pub"\npublish = true\n')
        assert mod.packages(root)["crates"] == ["pub"]
        (root / "Cargo.toml").write_text('[workspace]\nmembers = []\n')       # nothing inherited: published
        assert mod.packages(root)["crates"] == ["priv", "pub"]


def t_a_slow_drip_cannot_hold_the_run():
    # A socket timeout restarts at every byte. One byte every 0.2 s kept a request
    # alive for ever with a 0.3 s per-read timeout.
    class Drip(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            for _ in range(100):
                try:
                    self.wfile.write(b"x")
                    self.wfile.flush()
                except OSError:
                    return
                time.sleep(0.2)

    srv = QuietServer(("127.0.0.1", 0), Drip)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    old = (mod.TIMEOUT, mod.TOTAL_TIMEOUT)
    mod.TIMEOUT, mod.TOTAL_TIMEOUT = 0.5, 0.8
    try:
        started = time.monotonic()
        try:
            mod.http_get(f"http://127.0.0.1:{srv.server_port}/", {})
        except mod.Unavailable as e:
            assert "took longer" in str(e), str(e)
        else:
            raise AssertionError("a dripping body was read to the end")
        assert time.monotonic() - started < 2.0
    finally:
        mod.TIMEOUT, mod.TOTAL_TIMEOUT = old
        srv.shutdown()


def t_files_that_are_not_ordinary_small_files_are_refused():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        fifo = td / "fifo"
        os.mkfifo(fifo)
        big = td / "big.json"
        big.write_bytes(b" " * (mod.MAX_FILE + 1))
        for label, path in (("/dev/zero", "/dev/zero"), ("a pipe", fifo), ("a directory", td), ("too big", big)):
            started = time.monotonic()
            rc, out, err = run(["--previous", str(path)])
            assert rc == 2 and "--previous" in err and "Traceback" not in err and out == "", (label, rc, err)
            assert time.monotonic() - started < 3, label
    # ...and a manifest that is not an ordinary file is an error, not an omission.
    for rel in ("Cargo.toml", "crates/x/Cargo.toml", "sdk/python/pyproject.toml", "sdk/typescript/package.json"):
        with tempfile.TemporaryDirectory() as td:
            root = make_root(td, crates=(), pypi=None, npm=None)
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            if (root / rel).exists():
                (root / rel).unlink()
            os.mkfifo(root / rel)
            started = time.monotonic()
            rc, out, err = run(root=root)
            assert rc == 2 and "not a regular file" in err and time.monotonic() - started < 3, (rel, rc, err)
            (root / rel).unlink()
            (root / rel).mkdir()                       # a directory with that name
            rc, out, err = run(root=root)
            assert rc == 2 and "not a regular file" in err, (rel, "dir", rc, err)


def t_an_invalid_environment_repository_names_the_environment():
    for bad in ("acme/widget\n", "nope", "../x", "a/b/c"):
        rc, out, err = run(env={"GITHUB_REPOSITORY": bad}, repo=None)
        assert rc == 2 and "GITHUB_REPOSITORY" in err and "--repo" not in err and out == "", (bad, rc, err)
    rc, out, err = run(["--repo", "nope"], env={"GITHUB_REPOSITORY": "acme/widget"})
    assert rc == 2 and "--repo 'nope'" in err        # the option wins, and is blamed when it is the bad one


def t_a_rejected_token_says_so():
    routes = good_routes(**{f"https://api.github.com/repos/{REPO}": (401, b'{"message":"Bad credentials"}')})
    rc, out, err = run(fetch=Fake(routes), env={"GITHUB_TOKEN": "ghp_expired"})
    assert rc == 2 and "rejected the credentials" in err and "GITHUB_TOKEN" in err and "ghp_expired" not in err, (rc, err)


def t_a_reason_the_terminal_cannot_print_does_not_crash():
    odd = mod.Unavailable("could not reach host: Název se nepodařilo přeložit \u65e5\u672c")
    routes = good_routes(**{"https://pypistats.org/": odd})
    with tempfile.TemporaryDirectory() as td:
        env = {**os.environ, "PYTHONIOENCODING": "ascii", "PYTHONUTF8": "0"}
        code = ("import sys; sys.path.insert(0, %r); import adoption_report as a, datetime as d, pathlib;"
                "f=lambda u,h:(_ for _ in ()).throw(a.Unavailable('reason: \u65e5\u672c')) if 'pypistats' in u else "
                "(200, b'{\"stargazers_count\": 1, \"forks_count\": 2, \"total_count\": 3, \"downloads\": 4, \"crate\": {\"downloads\": 5, \"recent_downloads\": 6}}');"
                "sys.exit(a.main(['--repo','a/b'], fetch=f, root=pathlib.Path(%r), pause=lambda s: None))"
                % (str(HERE), str(make_root(td))))
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    assert out.returncode == 0 and "Traceback" not in out.stderr, (out.returncode, out.stderr[-300:])
    assert "\\u65e5" in out.stdout, out.stdout


TESTS = [v for k, v in sorted(globals().items()) if k.startswith("t_") and callable(v)]

if __name__ == "__main__":
    ok = True
    for t in TESTS:
        try:
            t()
            print(f"ok   {t.__name__[2:].replace('_', ' ')}")
        except Exception as e:  # report every failing case, not just the first
            ok = False
            print(f"FAIL {t.__name__[2:].replace('_', ' ')}  -- {type(e).__name__}: {e}")
    print("\nPASS" if ok else "\nFAIL")
    sys.exit(0 if ok else 1)
