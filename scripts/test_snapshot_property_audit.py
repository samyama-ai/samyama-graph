#!/usr/bin/env python3
"""The property audit must flag the columns #1815 found, and only those.

The three defects it exists for all look like a healthy snapshot to every other
check: the node counts are right and the labels are right. What gives them away
is what is *inside* a property -- a column that is empty on every node, or holds
two distinct values across thousands. So the cases here build small snapshots
with exactly that shape, and the opposite: a column that merely repeats because
the data is small, or because it is a boolean, must not be flagged.

Run: python3 scripts/test_snapshot_property_audit.py
"""

from __future__ import annotations

import contextlib
import gzip
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "snapshot_property_audit.py"

spec = importlib.util.spec_from_file_location("snapshot_property_audit", SCRIPT)
audit_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_mod)


def snapshot(nodes=(), edges=(), header=None) -> list[str]:
    """Snapshot lines: header, then nodes, then edges."""
    nodes, edges = list(nodes), list(edges)
    head = {"format": "sgsnap", "version": 2, "node_count": len(nodes),
            "edge_count": len(edges)} if header is None else header
    lines = [json.dumps(head)]
    for i, (labels, props) in enumerate(nodes, 1):
        lines.append(json.dumps({"t": "n", "id": i, "labels": labels, "props": props}))
    for i, (etype, props) in enumerate(edges, 1):
        lines.append(json.dumps({"t": "e", "id": i, "src": 1, "tgt": 2, "type": etype, "props": props}))
    return lines


def flags(lines, min_records=100, few=3) -> dict[tuple[str, str], list[str]]:
    """{(group, property): flags} for every property in the snapshot."""
    result = audit_mod.audit(lines)
    return {(r["group"], r["property"]): r["flags"]
            for r in audit_mod.rows(result, min_records, few)}


def many(label, make, n=150):
    return [([label], make(i)) for i in range(n)]


def write(td: Path, lines: list[str], name="s.sgsnap", gz=True) -> Path:
    p = td / name
    data = ("\n".join(lines) + "\n").encode()
    p.write_bytes(gzip.compress(data) if gz else data)
    return p


def truncated_gzip(path: Path) -> Path:
    """Starts with the gzip magic bytes and then stops being gzip."""
    path.write_bytes(b"\x1f\x8bgarbage")
    return path


def cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(SCRIPT), *args], capture_output=True, text=True)


# --- the flags --------------------------------------------------------------

def t_always_empty_is_flagged():
    # health-systems Country.income_level: on every node, empty on every node.
    f = flags(snapshot(many("Country", lambda i: {"name": f"C{i}", "income_level": ""})))
    assert f[("(Country)", "income_level")] == ["always-empty"], f
    assert f[("(Country)", "name")] == [], f


def t_every_kind_of_empty_counts():
    for blank in ("", "   ", None, [], {}):
        f = flags(snapshot(many("X", lambda i: {"v": blank}, n=5)))
        assert f[("(X)", "v")] == ["always-empty"], (blank, f)


def t_two_values_over_many_nodes_is_flagged():
    # sider SideEffect.name: the term *type* written where the name belongs.
    f = flags(snapshot(many("SideEffect", lambda i: {"name": "PT" if i % 2 else "LLT"})))
    assert f[("(SideEffect)", "name")] == ["few-values"], f


def t_one_value_over_many_nodes_is_constant():
    f = flags(snapshot(many("System", lambda i: {"kind": "production"})))
    assert f[("(System)", "kind")] == ["constant"], f


def t_a_healthy_column_is_not_flagged():
    f = flags(snapshot(many("Drug", lambda i: {"name": f"drug-{i}"})))
    assert f[("(Drug)", "name")] == [], f


def t_a_small_column_may_repeat():
    # 40 nodes cannot say much about a column; below --min-records stay quiet.
    f = flags(snapshot(many("T", lambda i: {"c": "a" if i % 2 else "b"}, n=40)))
    assert f[("(T)", "c")] == [], f
    f = flags(snapshot(many("T", lambda i: {"c": "a" if i % 2 else "b"}, n=40)), min_records=10)
    assert f[("(T)", "c")] == ["few-values"], f


def t_a_boolean_column_is_not_few_values():
    # Two values is what a boolean is. A *constant* boolean is still worth a look.
    f = flags(snapshot(many("G", lambda i: {"own_goal": bool(i % 2), "pen": False})))
    assert f[("(G)", "own_goal")] == [], f
    assert f[("(G)", "pen")] == ["constant"], f


def t_partly_empty_is_not_always_empty():
    f = flags(snapshot(many("P", lambda i: {"v": "" if i < 10 else f"x{i}"})))
    assert f[("(P)", "v")] == [], f


def t_thresholds_are_configurable():
    lines = snapshot(many("P", lambda i: {"v": f"x{i % 5}"}))
    assert flags(lines, few=3)[("(P)", "v")] == []
    assert flags(lines, few=5)[("(P)", "v")] == ["few-values"]


# --- what is counted --------------------------------------------------------

def t_one_and_string_one_and_true_are_different_values():
    lines = snapshot(many("V", lambda i: {"v": (1, "1", True)[i % 3]}))
    result = audit_mod.audit(lines)
    assert result["groups"]["(V)"]["v"].distinct == 3


def t_unhashable_values_are_handled():
    lines = snapshot(many("V", lambda i: {"tags": [i % 2, "a"], "m": {"k": i % 2}, "f": 1.5 * (i % 2)}))
    result = audit_mod.audit(lines)
    for prop in ("tags", "m", "f"):
        assert result["groups"]["(V)"][prop].distinct == 2, prop


def t_a_multi_label_node_counts_under_each_label():
    result = audit_mod.audit(snapshot([(["A", "B"], {"p": "x"})]))
    assert result["group_size"] == {"(A)": 1, "(B)": 1}, result["group_size"]
    assert result["nodes"] == 1


def t_edges_are_grouped_by_type():
    result = audit_mod.audit(snapshot(edges=[("KNOWS", {"since": 2020}), ("KNOWS", {}), ("LIKES", {"w": ""})]))
    assert result["group_size"] == {"[:KNOWS]": 2, "[:LIKES]": 1}, result["group_size"]
    assert result["groups"]["[:KNOWS]"]["since"].present == 1
    assert result["edges"] == 3


def t_a_property_missing_on_some_nodes_shows_in_present():
    result = audit_mod.audit(snapshot([(["A"], {"p": "x"}), (["A"], {})]))
    col = result["groups"]["(A)"]["p"]
    assert (result["group_size"]["(A)"], col.present) == (2, 1)


def t_index_and_hierarchy_records_are_ignored():
    lines = snapshot(many("A", lambda i: {"p": i}, n=3))
    lines.insert(1, json.dumps({"t": "i", "label": "A", "property": "p"}))
    lines.insert(2, json.dumps({"t": "h", "name": "h"}))
    assert audit_mod.audit(lines)["nodes"] == 3


def t_distinct_stops_being_tracked_past_the_cap():
    old = audit_mod.MAX_TRACKED
    audit_mod.MAX_TRACKED = 10
    try:
        col = audit_mod.audit(snapshot(many("A", lambda i: {"p": i}, n=50)))["groups"]["(A)"]["p"]
        assert col.saturated and col.distinct == 11, (col.saturated, col.distinct)
        assert audit_mod.flags_for(col, 1, 3) == []   # a saturated column is never "few"
    finally:
        audit_mod.MAX_TRACKED = old


# --- the command line -------------------------------------------------------

def t_cli_reads_gzip_and_plain_and_flags():
    lines = snapshot(many("Country", lambda i: {"name": f"C{i}", "region": ""}))
    with tempfile.TemporaryDirectory() as td:
        for gz in (True, False):
            p = write(Path(td), lines, gz=gz)
            out = cli(str(p), "--flagged-only")
            assert out.returncode == 0, out.stderr
            assert "always-empty" in out.stdout and "1 of 2 properties flagged" in out.stdout, out.stdout


def t_strict_exits_1_only_when_something_is_flagged():
    bad = snapshot(many("C", lambda i: {"r": ""}))
    good = snapshot(many("C", lambda i: {"r": f"v{i}"}))
    with tempfile.TemporaryDirectory() as td:
        assert cli(str(write(Path(td), bad)), "--strict").returncode == 1
        assert cli(str(write(Path(td), good)), "--strict").returncode == 0
        assert cli(str(write(Path(td), bad))).returncode == 0   # report only by default


def t_json_output_parses():
    lines = snapshot(many("C", lambda i: {"r": ""}))
    with tempfile.TemporaryDirectory() as td:
        out = cli(str(write(Path(td), lines)), "--json")
    data = json.loads(out.stdout)
    assert data["nodes"] == 150 and data["properties"][0]["flags"] == ["always-empty"], data


def t_a_header_count_that_disagrees_is_reported():
    lines = snapshot(many("C", lambda i: {"r": i}, n=5), header={"format": "sgsnap", "node_count": 9, "edge_count": 0})
    with tempfile.TemporaryDirectory() as td:
        out = cli(str(write(Path(td), lines)))
    assert "header says node_count 9, the file holds 5" in out.stdout, out.stdout


def t_unreadable_input_exits_2():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        cases = {
            "missing file": td / "nope.sgsnap",
            "no header": write(td, [json.dumps({"t": "n", "id": 1, "labels": [], "props": {}})], "a"),
            "not json": write(td, ["{not json"], "b"),
            "empty": write(td, [], "c"),
            "not gzip": truncated_gzip(td / "d"),
            "props not an object": write(td, snapshot() + [json.dumps({"t": "n", "labels": ["A"], "props": 3})], "e"),
        }
        for name, path in cases.items():
            out = cli(str(path))
            assert out.returncode == 2 and out.stderr, (name, out.returncode, out.stderr)
            assert "Traceback" not in out.stderr, (name, out.stderr)


# --- review of #2027: thresholds against the tracking cap, malformed records --

def with_cap(cap, fn):
    old = audit_mod.MAX_TRACKED
    audit_mod.MAX_TRACKED = cap
    try:
        return fn()
    finally:
        audit_mod.MAX_TRACKED = old


def t_few_at_the_cap_flags_a_column_exactly_at_it_and_not_one_past():
    # The cap is where distinct values stop being counted. --few may reach it,
    # so a column with exactly `cap` values is flagged and `cap + 1` is not.
    def run():
        at = audit_mod.audit(snapshot(many("A", lambda i: {"p": i % 10}, n=200)))["groups"]["(A)"]["p"]
        past = audit_mod.audit(snapshot(many("A", lambda i: {"p": i % 11}, n=200)))["groups"]["(A)"]["p"]
        assert not at.saturated and audit_mod.flags_for(at, 1, 10) == ["few-values"]
        assert past.saturated and audit_mod.flags_for(past, 1, 10) == []
    with_cap(10, run)


def t_few_above_the_cap_is_refused_not_silently_ignored():
    # Before: --few 11 with a cap of 10 said nothing was wrong, and --strict exited 0.
    def run():
        with tempfile.TemporaryDirectory() as td:
            p = write(Path(td), snapshot(many("A", lambda i: {"p": i % 11}, n=200)))
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    audit_mod.main([str(p), "--few", "11", "--min-records", "1", "--strict"])
            except SystemExit as e:
                assert e.code == 2, e.code
            else:
                raise AssertionError("--few above the cap was accepted")
    with_cap(10, run)


def t_few_and_min_records_must_be_in_range():
    with tempfile.TemporaryDirectory() as td:
        p = str(write(Path(td), snapshot(many("A", lambda i: {"p": i}, n=3))))
        for bad in (["--few", "1"], ["--few", "0"], ["--few", "100001"], ["--min-records", "0"]):
            out = cli(p, *bad)
            assert out.returncode == 2 and "must" in out.stderr and "Traceback" not in out.stderr, (bad, out)
        assert cli(p, "--few", "100000").returncode == 0


def t_labels_must_be_an_array_of_strings():
    for bad in (123, "Person", {"a": 1}, [1, 2], ["ok", None], True):
        lines = snapshot() + [json.dumps({"t": "n", "id": 1, "labels": bad, "props": {"x": 1}})]
        try:
            audit_mod.audit(lines)
        except audit_mod.SnapshotError as e:
            assert "line 2" in str(e) and "labels" in str(e), (bad, e)
        else:
            raise AssertionError(f"labels {bad!r} was accepted")


def t_labels_cli_exits_2_not_a_traceback():
    with tempfile.TemporaryDirectory() as td:
        for bad in (123, "Person"):
            lines = snapshot() + [json.dumps({"t": "n", "id": 1, "labels": bad, "props": {"x": 1}})]
            out = cli(str(write(Path(td), lines)))
            assert out.returncode == 2 and "labels" in out.stderr and "Traceback" not in out.stderr, (bad, out)


def t_missing_or_null_labels_are_still_allowed():
    for rec in ({"t": "n", "props": {"x": 1}}, {"t": "n", "labels": None, "props": {"x": 1}},
                {"t": "n", "labels": [], "props": {"x": 1}}):
        result = audit_mod.audit(snapshot() + [json.dumps(rec)])
        assert list(result["groups"]) == ["(no label)"], (rec, result["groups"])


def t_an_edge_with_no_type_is_grouped_not_dropped():
    result = audit_mod.audit(snapshot() + [json.dumps({"t": "e", "id": 1, "src": 1, "tgt": 2, "props": {"w": 1}})])
    assert list(result["groups"]) == ["[no type]"], result["groups"]


def t_an_edge_type_must_be_a_string():
    for bad in (["KNOWS"], 5, {"a": 1}):
        lines = snapshot() + [json.dumps({"t": "e", "id": 1, "src": 1, "tgt": 2, "type": bad, "props": {}})]
        try:
            audit_mod.audit(lines)
        except audit_mod.SnapshotError as e:
            assert "type" in str(e), e
        else:
            raise AssertionError(f"edge type {bad!r} was accepted")


def t_a_header_count_that_is_not_a_number_is_ignored():
    # Before: "node_count": "many" crashed the report with a ValueError.
    for claimed in ("many", None, True, 2.5, [1]):
        lines = snapshot(many("C", lambda i: {"r": i}, n=5), header={"format": "sgsnap", "node_count": claimed})
        with tempfile.TemporaryDirectory() as td:
            out = cli(str(write(Path(td), lines)))
        assert out.returncode == 0 and "warning" not in out.stdout and "Traceback" not in out.stderr, (claimed, out)


def t_a_corrupt_deflate_stream_exits_2():
    body = ("\n".join(snapshot(many("A", lambda i: {"p": f"value-{i}"}, n=500))) + "\n").encode()
    raw = bytearray(gzip.compress(body))
    for i in range(len(raw) // 2, len(raw) // 2 + 40):
        raw[i] ^= 0xFF
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "c.sgsnap"
        p.write_bytes(bytes(raw))
        out = cli(str(p))
    assert out.returncode == 2 and out.stderr and "Traceback" not in out.stderr, out


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
