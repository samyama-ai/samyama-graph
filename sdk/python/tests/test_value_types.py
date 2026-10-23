"""Every value type arrives through the Python SDK as the engine sent it.

API-04 (#1728). The cases are ``sdk/fixtures/value_types.json``, shared with
the Rust and TypeScript suites so all three SDKs are held to one table. Each
value is read as a column of its own, inside a list, inside a map, and (when
the fixture says it can be stored) as a node property.

The embedded client always runs. The remote client runs when ``SAMYAMA_URL``
names a server, buffered and streamed.

The comparison is strict about type: ``1`` and ``1.0`` and ``True`` are equal
in Python, so plain ``==`` would pass a float that came back as an integer.
"""

import json
import os
import pathlib
import uuid

import pytest

import samyama

FIXTURE = pathlib.Path(__file__).resolve().parents[2] / "fixtures" / "value_types.json"
CASES = json.loads(FIXTURE.read_text(encoding="utf-8"))["cases"]
ENGINE_URL = os.environ.get("SAMYAMA_URL")
SDK = "python"


def same(got, want):
    """Equal in value and in type, all the way down."""
    if type(got) is not type(want):
        return False
    if isinstance(want, list):
        return len(got) == len(want) and all(same(g, w) for g, w in zip(got, want))
    if isinstance(want, dict):
        return got.keys() == want.keys() and all(same(got[k], want[k]) for k in want)
    return got == want


def read_shapes(case):
    """Each read-only way a value comes back, with what it must decode to."""
    expr, want = case["cypher"], json.loads(case["json"])
    return [
        ("column", f"RETURN {expr} AS v", want),
        ("list", f"WITH {expr} AS x RETURN [x] AS v", [want]),
        ("map", f"WITH {expr} AS x RETURN {{k: x}} AS v", {"k": want}),
    ]


def property_query(case):
    """Store the value on a node of a label no other case or run uses."""
    label = "RoundTripPy" + uuid.uuid4().hex
    return f"CREATE (n:{label} {{v: {case['cypher']}}}) RETURN n.v AS v"


def only_value(result, query):
    records = result.records
    assert len(records) == 1 and len(records[0]) == 1, f"{query}: one row, one column"
    return records[0][0]


def check(got, want, case, where):
    assert same(got, want), f"{where}: {case['name']}: got {got!r}, want {want!r}"


def test_no_case_is_lossy_in_python():
    # Python's int is unbounded and its float is a double, so every value the
    # engine sends fits. A case marked lossy here would need its own handling.
    assert [c["name"] for c in CASES if SDK in c.get("lossy", {})] == []


def test_the_fixture_names_are_unique():
    names = [c["name"] for c in CASES]
    assert len(set(names)) == len(names)


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_embedded_round_trip(case):
    client = samyama.SamyamaClient.embedded()
    for shape, query, want in read_shapes(case):
        check(only_value(client.query(query), query), want, case, f"embedded {shape}")
    if case["storable"]:
        query = property_query(case)
        check(only_value(client.query(query), query), json.loads(case["json"]), case, "embedded property")


@pytest.mark.skipif(not ENGINE_URL, reason="set SAMYAMA_URL to run against a server")
@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_remote_round_trip(case):
    client = samyama.SamyamaClient.connect(ENGINE_URL)
    for shape, query, want in read_shapes(case):
        check(only_value(client.query(query), query), want, case, f"remote {shape}")
        rows = list(client.query_stream(query))
        assert len(rows) == 1, f"stream {shape}: one row"
        check(rows[0]["v"], want, case, f"stream {shape}")
    if case["storable"]:
        query = property_query(case)
        check(only_value(client.query(query), query), json.loads(case["json"]), case, "remote property")
