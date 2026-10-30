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
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "snapshot_property_audit.py"

spec = importlib.util.spec_from_file_location("snapshot_property_audit", SCRIPT)
audit_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_mod)


HEADER = {"format": "sgsnap", "version": 2}


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
    for bad in (123, "Person", {"a": 1}, [1, 2], ["ok", None], True, None):
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






def t_an_edge_type_must_be_a_string():
    for bad in (["KNOWS"], 5, {"a": 1}):
        lines = snapshot() + [json.dumps({"t": "e", "id": 1, "src": 1, "tgt": 2, "type": bad, "props": {}})]
        try:
            audit_mod.audit(lines)
        except audit_mod.SnapshotError as e:
            assert "type" in str(e), e
        else:
            raise AssertionError(f"edge type {bad!r} was accepted")




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






def t_falsy_props_exit_2_even_with_strict():
    with tempfile.TemporaryDirectory() as td:
        for bad in ([], False, 0, ""):
            lines = snapshot() + [json.dumps({"t": "n", "id": 1, "labels": ["A"], "props": bad})]
            out = cli(str(write(Path(td), lines)), "--strict")
            assert out.returncode == 2 and "props" in out.stderr and "Traceback" not in out.stderr, (bad, out)


def t_a_deeply_nested_value_exits_2():
    deep = "[" * 200_000 + "]" * 200_000
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "d.sgsnap"
        p.write_text(json.dumps(HEADER) + "\n"
                     + '{"t": "n", "labels": ["A"], "props": {"x": ' + deep + "}}\n")
        out = cli(str(p))
    assert out.returncode == 2 and "line 2" in out.stderr and "Traceback" not in out.stderr, out


def t_a_directory_and_invalid_utf8_exit_2():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        bad = json.dumps(HEADER).encode() + b'\n{"t":"n","labels":["A"],"props":{"x":"\xff"}}\n'
        (td / "p").write_bytes(bad)
        (td / "g").write_bytes(gzip.compress(bad))
        for path in (td, td / "p", td / "g"):
            out = cli(str(path))
            assert out.returncode == 2 and out.stderr and "Traceback" not in out.stderr, (path.name, out)


def t_every_record_must_have_a_known_type():
    # Before: a missing or unknown "t" fell through silently, so a snapshot
    # whose records were all misspelt audited as empty and exited 0.
    props = {"labels": ["Country"], "props": {"income_level": ""}}
    for rec in ({**props}, {"t": None, **props}, {"t": "", **props}, {"t": "node", **props},
                {"t": "N", **props}, {"t": "z", **props}, {"t": 5, **props}, {"t": True, **props},
                {"t": ["n"], **props}, {"t": {"a": 1}, **props}):
        try:
            audit_mod.audit(snapshot() + [json.dumps(rec)])
        except audit_mod.SnapshotError as e:
            assert "line 2" in str(e) and "record type" in str(e), (rec, e)
        else:
            raise AssertionError(f"record {rec!r} was accepted")


def t_an_unknown_type_is_reported_as_the_type_not_as_something_else():
    # The first thing wrong with the record is what the message should say.
    rec = {"t": "node", "labels": 123, "props": []}
    try:
        audit_mod.audit(snapshot() + [json.dumps(rec)])
    except audit_mod.SnapshotError as e:
        assert "record type" in str(e), e
    else:
        raise AssertionError("accepted")


def t_unknown_record_type_exits_2_even_with_strict():
    line = '{"t":"node","labels":["Country"],"props":{"income_level":""}}'
    with tempfile.TemporaryDirectory() as td:
        out = cli(str(write(Path(td), snapshot() + [line])), "--strict")
    assert out.returncode == 2 and "record type" in out.stderr and "Traceback" not in out.stderr, out


def t_index_and_hierarchy_records_need_no_props():
    lines = snapshot(many("A", lambda i: {"p": i}, n=2))
    lines.insert(1, json.dumps({"t": "i", "definitions": [{"label": "A"}]}))
    lines.insert(2, json.dumps({"t": "h", "name": "tax", "edge_types": ["IS_A"], "props": [1]}))
    result = audit_mod.audit(lines)
    assert result["nodes"] == 2 and list(result["groups"]) == ["(A)"], result["groups"]


def t_a_label_called_no_label_is_not_the_unlabeled_group():
    # Before both were "(no label)": 60 nodes with an always-empty column and 60
    # unlabeled nodes with real values merged into one group of 120, and the
    # empty column stopped being flagged.
    lines = snapshot([(["no label"], {"income": ""})] * 60 + [([], {"income": f"v{i}"}) for i in range(60)])
    result = audit_mod.audit(lines)
    assert result["group_size"] == {"(no label)": 60, audit_mod.UNLABELED: 60}, result["group_size"]
    f = flags(lines, min_records=50)
    assert f[("(no label)", "income")] == ["always-empty"], f
    assert f[(audit_mod.UNLABELED, "income")] == [], f


def t_two_halves_do_not_add_up_to_reach_min_records():
    # 60 + 60 must not read as 120 and clear --min-records 100.
    lines = snapshot([(["no label"], {"c": "a"})] * 60 + [([], {"c": "a"})] * 60)
    assert all(v == [] for v in flags(lines).values()), flags(lines)
    assert flags(lines, min_records=60)[("(no label)", "c")] == ["constant"]






def mixed_snapshot():
    """One healthy column and one always-empty column."""
    return snapshot(many("Country", lambda i: {"name": f"C{i}", "income_level": ""}))


def t_json_lists_every_property_without_flagged_only():
    with tempfile.TemporaryDirectory() as td:
        data = json.loads(cli(str(write(Path(td), mixed_snapshot())), "--json").stdout)
    assert [r["property"] for r in data["properties"]] == ["income_level", "name"], data
    assert (data["properties_total"], data["properties_flagged"]) == (2, 1), data


def t_json_flagged_only_lists_only_flagged_properties():
    # Before: --json ignored --flagged-only and emitted the healthy column too.
    with tempfile.TemporaryDirectory() as td:
        out = cli(str(write(Path(td), mixed_snapshot())), "--json", "--flagged-only")
    data = json.loads(out.stdout)
    assert [r["property"] for r in data["properties"]] == ["income_level"], data
    assert all(r["flags"] for r in data["properties"]), data
    assert (data["properties_total"], data["properties_flagged"]) == (2, 1), data


def t_json_flagged_only_with_nothing_flagged_is_empty():
    healthy = snapshot(many("Drug", lambda i: {"name": f"drug-{i}"}))
    with tempfile.TemporaryDirectory() as td:
        out = cli(str(write(Path(td), healthy)), "--json", "--flagged-only", "--strict")
    data = json.loads(out.stdout)
    assert out.returncode == 0 and data["properties"] == [] and data["properties_total"] == 1, (out, data)


def t_strict_judges_the_whole_table_not_what_flagged_only_shows():
    with tempfile.TemporaryDirectory() as td:
        p = str(write(Path(td), mixed_snapshot()))
        for extra in ([], ["--flagged-only"], ["--json"], ["--json", "--flagged-only"]):
            assert cli(p, "--strict", *extra).returncode == 1, extra


def t_a_repeated_label_counts_the_node_once():
    # Before: ["A", "A"] bumped group (A) twice, so one node read as two records.
    result = audit_mod.audit(snapshot([(["A", "A"], {"p": "x"}), (["A", "B", "A"], {"p": "y"})]))
    assert result["group_size"] == {"(A)": 2, "(B)": 1}, result["group_size"]
    assert result["groups"]["(A)"]["p"].present == 2
    assert result["nodes"] == 2


def t_an_integer_too_long_to_convert_exits_2():
    # Python refuses integers over 4,300 digits with a plain ValueError.
    huge = json.dumps(HEADER) + '\n{"t":"n","labels":["A"],"props":{"x":' + "9" * 5000 + "}}\n"
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "h.sgsnap"
        p.write_text(huge)
        out = cli(str(p))
    assert out.returncode == 2 and "line 2" in out.stderr and "Traceback" not in out.stderr, out




def t_a_binary_file_gets_a_plain_message():
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "b.sgsnap"
        p.write_bytes(bytes(range(256)) * 4)
        out = cli(str(p))
    assert out.returncode == 2 and "not UTF-8 text" in out.stderr and "decrypted" in out.stderr, out


# --- review of #2027, round 6: what a clean report is allowed to mean ---------

def refuses(lines, needle, *extra):
    """The audit must refuse these lines, via the API and via the CLI with --strict."""
    try:
        audit_mod.audit(lines)
    except audit_mod.SnapshotError as e:
        assert needle in str(e), (needle, str(e))
    else:
        raise AssertionError(f"accepted; wanted an error mentioning {needle!r}")
    with tempfile.TemporaryDirectory() as td:
        out = cli(str(write(Path(td), lines)), "--strict", *extra)
    assert out.returncode == 2 and needle in out.stderr and "Traceback" not in out.stderr, (needle, out)


def t_a_header_only_file_is_not_a_clean_snapshot():
    # A file that stops after the header read as "0 of 0 properties flagged" and exit 0.
    head = {**HEADER, "node_count": 3, "edge_count": 0}
    refuses([json.dumps(head)], "node_count 3 but the file holds 0")
    refuses([json.dumps(head)], "node_count 3 but the file holds 0", "--json")


def t_a_header_count_that_disagrees_exits_2():
    nodes = many("C", lambda i: {"r": i}, n=5)
    refuses(snapshot(nodes, header={**HEADER, "node_count": 9, "edge_count": 0}), "node_count 9 but the file holds 5")
    refuses(snapshot(nodes, [("R", {})], header={**HEADER, "node_count": 5, "edge_count": 4}), "edge_count 4 but the file holds 1")
    # ...and the same lines with the right counts are fine.
    assert audit_mod.audit(snapshot(nodes, [("R", {})]))["nodes"] == 5


def t_a_header_count_that_is_not_a_number_is_ignored():
    # Before: "node_count": "many" crashed the report with a ValueError.
    for claimed in ("many", None, True, 2.5, [1]):
        lines = snapshot(many("C", lambda i: {"r": i}, n=5), header={**HEADER, "node_count": claimed})
        with tempfile.TemporaryDirectory() as td:
            out = cli(str(write(Path(td), lines)))
        assert out.returncode == 0 and "Traceback" not in out.stderr, (claimed, out)


def t_the_header_version_must_be_1_or_2():
    node = [(["A"], {"p": 1})]
    for ok in (1, 2):
        assert audit_mod.audit(snapshot(node, header={**HEADER, "version": ok, "node_count": 1}))["nodes"] == 1
    for bad in (0, 3, 99, "2", 2.0, True, None, [2]):
        refuses(snapshot(node, header={**HEADER, "version": bad, "node_count": 1}), "unsupported snapshot version")
    head = {"format": "sgsnap", "node_count": 1}
    refuses(snapshot(node, header=head), "unsupported snapshot version")   # no version at all


def t_required_fields_are_required():
    # The engine's SnapshotNode / SnapshotEdge have no defaults for these.
    node = {"t": "n", "id": 1, "labels": ["A"], "props": {"x": 1}}
    edge = {"t": "e", "id": 1, "src": 1, "tgt": 2, "type": "R", "props": {"w": 1}}
    for rec, key in [(node, "id"), (node, "labels"), (node, "props"),
                     (edge, "id"), (edge, "src"), (edge, "tgt"), (edge, "type"), (edge, "props")]:
        for how in ("missing", "null"):
            r = {k: v for k, v in rec.items() if k != key}
            if how == "null":
                r[key] = None
            refuses(snapshot() + [json.dumps(r)], key)
    for rec, key in [(node, "id"), (edge, "id"), (edge, "src"), (edge, "tgt")]:
        for bad in (-1, 1.5, "1", True, [1]):
            refuses(snapshot() + [json.dumps({**rec, key: bad})], key)


def t_no_labels_is_legal_but_a_missing_labels_key_is_not():
    result = audit_mod.audit(snapshot([([], {"x": 1})]))
    assert list(result["groups"]) == [audit_mod.UNLABELED], result["groups"]
    assert audit_mod.audit(snapshot([([], {})]))["group_size"] == {audit_mod.UNLABELED: 1}


def t_props_must_be_an_object():
    # Before: `rec.get("props") or {}` turned [], false, 0 and "" into "no properties".
    for bad in ([], False, 0, "", 3, "x", [1], True, 1.5, None):
        refuses(snapshot() + [json.dumps({"t": "n", "id": 1, "labels": ["A"], "props": bad})], "props")
        refuses(snapshot() + [json.dumps({"t": "e", "id": 1, "src": 1, "tgt": 2, "type": "R", "props": bad})], "props")
    assert audit_mod.audit(snapshot([(["A"], {})]))["nodes"] == 1    # {} is fine


def t_odd_group_names_stay_separate():
    result = audit_mod.audit(snapshot([([""], {"p": 1}), ([], {"p": 1}), (["<unlabeled>"], {"p": 1}),
                                       (["a)"], {"p": 1}), (["a"], {"p": 1})],
                                      [("", {"w": 1}), ("<unlabeled>", {"w": 1}), ("no type", {"w": 1})]))
    assert result["group_size"] == {"()": 1, audit_mod.UNLABELED: 1, "(<unlabeled>)": 1, "(a))": 1, "(a)": 1,
                                    "[:]": 1, "[:<unlabeled>]": 1, "[:no type]": 1}, result["group_size"]
    assert not audit_mod.UNLABELED.startswith(("(", "[:"))


def t_nan_and_infinity_are_refused():
    for const in ("NaN", "Infinity", "-Infinity"):
        line = '{"t":"n","id":1,"labels":["A"],"props":{"x":%s}}' % const
        refuses(snapshot() + [line], const.lstrip("-"))


def t_a_lone_surrogate_is_refused_wherever_it_appears():
    # serde_json, which the engine reads with, rejects it; so does this.
    s = "\\ud800"
    node = lambda labels='["A"]', props='{"x":1}': '{"t":"n","id":1,"labels":%s,"props":%s}' % (labels, props)
    for line in (node(props='{"x":"%s"}' % s), node(props='{"x":["a","%s"]}' % s), node(props='{"x":{"k":"%s"}}' % s),
                 node(props='{"x":{"%s":1}}' % s), node(props='{"%s":1}' % s), node(labels='["%s"]' % s),
                 '{"t":"e","id":1,"src":1,"tgt":2,"type":"%s","props":{}}' % s):
        refuses(snapshot() + [line], "lone surrogate")
    # A good emoji (a surrogate *pair*) is fine.
    ok = '{"t":"n","id":1,"labels":["A"],"props":{"x":"\\ud83d\\ude00"}}'
    assert audit_mod.audit(snapshot([(["A"], {"x": "\U0001F600"})]))["nodes"] == 1
    assert audit_mod.audit([json.dumps({**HEADER, "node_count": 1}), ok])["nodes"] == 1


def t_a_surrogate_is_refused_even_in_a_column_past_the_cap():
    def run():
        lines = snapshot(many("A", lambda i: {"p": f"v{i}"}, n=50)) + ['{"t":"n","id":99,"labels":["A"],"props":{"p":"\\ud800"}}']
        lines[0] = json.dumps({**HEADER, "node_count": 51})
        refuses(lines, "lone surrogate")
    with_cap(10, run)


def t_strings_are_trimmed_before_counting_distinct_values():
    # The engine trims every string on import (codec.rs), so these are one value.
    lines = snapshot(many("SE", lambda i: {"name": ("PT", "PT ", " PT", "PT  ", "\tPT\n")[i % 5]}, n=300))
    col = audit_mod.audit(lines)["groups"]["(SE)"]["name"]
    assert col.distinct == 1 and flags(lines)[("(SE)", "name")] == ["constant"]
    # ...also inside a list or a map, which the engine decodes recursively.
    lines = snapshot(many("SE", lambda i: {"l": [("a", " a")[i % 2]], "m": {"k": ("b", "b ")[i % 2]}}, n=300))
    assert audit_mod.audit(lines)["groups"]["(SE)"]["l"].distinct == 1
    assert audit_mod.audit(lines)["groups"]["(SE)"]["m"].distinct == 1
    # Different text stays different, and so do different types.
    lines = snapshot(many("SE", lambda i: {"v": ("PT", "LLT")[i % 2]}, n=300))
    assert audit_mod.audit(lines)["groups"]["(SE)"]["v"].distinct == 2


def t_distinct_values_are_kept_as_digests_not_text():
    # 848 MB peak on 110k nodes with four 2 KB text columns, when the text was kept.
    col = audit_mod.audit(snapshot(many("A", lambda i: {"d": ("x" * 5000) + str(i)}, n=200)))["groups"]["(A)"]["d"]
    assert col.distinct == 200 and all(isinstance(v, bytes) and len(v) == 16 for v in col.values)


def t_a_carriage_return_inside_a_line_is_whitespace_not_a_line_break():
    # The engine splits lines on \n only; so must this, or valid JSON is rejected
    # and every later line number shifts.
    lines = snapshot(many("A", lambda i: {"p": i}, n=3))
    lines[1] = lines[1].replace('{"t"', '{\r"t"', 1).replace('"id"', '\r"id"', 1)
    with tempfile.TemporaryDirectory() as td:
        for gz in (True, False):
            out = cli(str(write(Path(td), lines, gz=gz)))
            assert out.returncode == 0 and "3 nodes" in out.stdout, (gz, out.stderr, out.stdout)


def t_windows_line_endings_are_read():
    lines = snapshot(many("A", lambda i: {"p": i}, n=3))
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "w.sgsnap"
        p.write_bytes(("\r\n".join(lines) + "\r\n").encode())
        out = cli(str(p))
    assert out.returncode == 0 and "3 nodes" in out.stdout, out


def t_a_closed_pipe_is_not_an_error():
    # `... | head` closes stdout early; that must not print a traceback.
    nodes = [([f"L{i}"], {"a": "x", "b": "y"}) for i in range(1500)]
    with tempfile.TemporaryDirectory() as td:
        p = write(Path(td), snapshot(nodes))
        for extra in ([], ["--json"]):
            proc = subprocess.Popen([sys.executable, str(SCRIPT), str(p), *extra],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc.stdout.read(20)
            proc.stdout.close()
            err = proc.stderr.read().decode()
            rc = proc.wait()
            assert rc == 0 and err == "", (extra, rc, err[-300:])


def t_table_columns_line_up_with_their_headings():
    # Widths came from the data only, so a short name left the heading overhanging.
    for nodes in (many("A", lambda i: {"x": i}, n=5),
                  many("Country", lambda i: {"name": f"C{i}", "income_level": ""}, n=1234)):
        with tempfile.TemporaryDirectory() as td:
            out = cli(str(write(Path(td), snapshot(nodes)))).stdout.splitlines()
        i = next(k for k, l in enumerate(out) if l.startswith("group"))
        head, body = out[i], [l for l in out[i + 1:] if l.strip() and "properties flagged" not in l]
        assert body
        for col in ("records", "present", "empty", "distinct"):
            end = head.index(col) + len(col)
            for row in body:
                assert row[end - 1] != " " and row[end:end + 2].strip() == "", (col, head, row)
        for col in ("group", "property"):
            start = head.index(col)
            for row in body:
                assert row[start] != " " and (start == 0 or row[start - 1] == " "), (col, head, row)


def t_flagged_only_in_the_text_table_hides_healthy_rows():
    with tempfile.TemporaryDirectory() as td:
        p = str(write(Path(td), mixed_snapshot()))
        everything, flagged = cli(p).stdout, cli(p, "--flagged-only").stdout
    assert "income_level" in everything and "name" in everything
    assert "income_level" in flagged and "name" not in flagged and "1 of 2 properties flagged" in flagged


def t_a_count_past_the_cap_is_shown_as_a_lower_bound():
    def run():
        result = audit_mod.audit(snapshot(many("A", lambda i: {"p": i}, n=50)))
        table = audit_mod.rows(result, 1, 3)
        assert table[0]["distinct"] == 11 and table[0]["distinct_is_lower_bound"] is True
        assert "11+" in audit_mod.render(Path("s.sgsnap"), result, table, False)
        exact = audit_mod.rows(audit_mod.audit(snapshot(many("A", lambda i: {"p": i % 5}, n=50))), 1, 3)
        assert exact[0]["distinct_is_lower_bound"] is False
    with_cap(10, run)


def t_a_saturated_column_is_never_classified_even_for_a_huge_few():
    # --few cannot exceed the cap on the command line, but flags_for is a library call.
    def run():
        col = audit_mod.audit(snapshot(many("A", lambda i: {"p": i % 11}, n=200)))["groups"]["(A)"]["p"]
        assert col.saturated
        assert audit_mod.flags_for(col, 1, 1000) == []
    with_cap(10, run)


def t_output_with_characters_the_terminal_cannot_print_does_not_crash():
    nodes = [(["Zürich", "日本"], {"café": "x"})]
    env = {**os.environ, "PYTHONIOENCODING": "ascii", "PYTHONUTF8": "0"}
    with tempfile.TemporaryDirectory() as td:
        p = str(write(Path(td), snapshot(nodes)))
        for extra in ([], ["--json"], ["--flagged-only"]):
            out = subprocess.run([sys.executable, str(SCRIPT), p, *extra], capture_output=True, text=True, env=env)
            assert out.returncode == 0 and "Traceback" not in out.stderr, (extra, out.stderr[-200:])
        assert "Z\\xfcrich" in subprocess.run([sys.executable, str(SCRIPT), p], capture_output=True, text=True, env=env).stdout


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
