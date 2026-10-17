"""`samyama_arrow` decodes what `/api/query/export` sends (#1097).

These run with no server: `_post` is replaced, and the bytes it returns are real
Arrow IPC and Parquet built by pyarrow. So they cover the half that is this
module's own -- decoding, the pandas conversion, and how failures surface --
and not the wire contract.

That contract is covered already, and deliberately elsewhere:
`tests/arrow_parquet_export.rs` checks the writer with Arrow's own readers, and
`tests/integration/test_arrow_export.py` checks that pandas opens what the
running server writes and gets the same values back. A unit test that built its
fixture with pyarrow and read it with pyarrow could not tell those things.

The fixture carries the types spec 09 names -- nulls, unicode, floats, nested
lists -- because those are the ones that break exporters.
"""
import json
import urllib.error

import pytest

pa = pytest.importorskip("pyarrow", reason="the Arrow helper needs pyarrow")

import samyama_arrow


def fixture_table():
    return pa.table(
        {
            "ord": pa.array([1, 2, 3], pa.int64()),
            "title": pa.array(["café — naïve 日本語 🌏", None, "plain"], pa.string()),
            "score": pa.array([1.5, None, -0.25], pa.float64()),
            "live": pa.array([True, False, None], pa.bool_()),
            "tags": pa.array([["a", "b"], [], None], pa.list_(pa.string())),
            "embedding": pa.array([[0.5, 1.5], None, [0.0, 0.0]], pa.list_(pa.float64())),
        }
    )


def ipc_bytes(tbl):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    return sink.getvalue().to_pybytes()


def parquet_file_bytes(tbl):
    pq = pytest.importorskip("pyarrow.parquet")
    sink = pa.BufferOutputStream()
    pq.write_table(tbl, sink)
    return sink.getvalue().to_pybytes()


@pytest.fixture
def captured(monkeypatch):
    """Replace `_post`, recording the request and returning Arrow IPC bytes."""
    seen = {}

    def fake_post(base_url, body, timeout):
        seen.update(base_url=base_url, body=body, timeout=timeout)
        return ipc_bytes(fixture_table())

    monkeypatch.setattr(samyama_arrow, "_post", fake_post)
    return seen


def test_table_decodes_the_arrow_stream(captured):
    tbl = samyama_arrow.table("MATCH (n) RETURN n")
    assert tbl.num_rows == 3
    assert tbl.column_names == ["ord", "title", "score", "live", "tags", "embedding"]
    # Unicode, nulls, floats and nested lists survive the round trip.
    assert tbl.column("title")[0].as_py() == "café — naïve 日本語 🌏"
    assert tbl.column("title")[1].as_py() is None
    assert tbl.column("score")[2].as_py() == -0.25
    assert tbl.column("tags")[0].as_py() == ["a", "b"]
    assert tbl.column("tags")[1].as_py() == []
    assert tbl.column("tags")[2].as_py() is None
    assert tbl.column("embedding")[0].as_py() == [0.5, 1.5]


def test_table_sends_the_query_graph_and_format(captured):
    samyama_arrow.table("MATCH (n) RETURN n", graph="people")
    assert captured["body"] == {
        "query": "MATCH (n) RETURN n",
        "graph": "people",
        "format": "arrow",
    }


def test_graph_defaults_to_default(captured):
    samyama_arrow.table("RETURN 1")
    assert captured["body"]["graph"] == "default"


def test_base_url_defaults_and_can_be_overridden(captured):
    samyama_arrow.table("RETURN 1")
    assert captured["base_url"] == samyama_arrow.DEFAULT_BASE_URL
    samyama_arrow.table("RETURN 1", base_url="http://elsewhere:9999")
    assert captured["base_url"] == "http://elsewhere:9999"


def test_table_reads_parquet_in_memory(monkeypatch):
    tbl = fixture_table()
    monkeypatch.setattr(
        samyama_arrow, "_post", lambda b, body, t: parquet_file_bytes(tbl)
    )
    got = samyama_arrow.table("RETURN 1", via="parquet")
    # Parquet is read from a BytesIO, so no file is written anywhere.
    assert got.column("title")[0].as_py() == "café — naïve 日本語 🌏"
    assert got.column("tags")[0].as_py() == ["a", "b"]


def test_parquet_via_asks_the_server_for_parquet(monkeypatch):
    seen = {}

    def fake_post(base_url, body, timeout):
        seen.update(body)
        return parquet_file_bytes(fixture_table())

    monkeypatch.setattr(samyama_arrow, "_post", fake_post)
    samyama_arrow.table("RETURN 1", via="parquet")
    assert seen["format"] == "parquet"


def test_an_unknown_via_is_refused_before_any_request(monkeypatch):
    def explode(*_args, **_kwargs):
        raise AssertionError("no request should be made for an unknown format")

    monkeypatch.setattr(samyama_arrow, "_post", explode)
    with pytest.raises(ValueError, match="arrow"):
        samyama_arrow.table("RETURN 1", via="feather")


def test_dataframe_converts_through_pandas(captured):
    pytest.importorskip("pandas", reason="the DataFrame half needs pandas")
    df = samyama_arrow.dataframe("MATCH (n) RETURN n")
    assert list(df.columns) == ["ord", "title", "score", "live", "tags", "embedding"]
    assert len(df) == 3
    assert df["title"][0] == "café — naïve 日本語 🌏"


def test_parquet_bytes_returns_the_payload_undecoded(monkeypatch):
    monkeypatch.setattr(samyama_arrow, "_post", lambda b, body, t: b"PAR1-ish")
    assert samyama_arrow.parquet_bytes("RETURN 1") == b"PAR1-ish"


def test_a_refusal_surfaces_the_servers_message(monkeypatch):
    """A bare "HTTP Error 400" hides which query failed, so the body is read."""

    class FakeHTTPError(urllib.error.HTTPError):
        def __init__(self):
            super().__init__("http://x/api/query/export", 400, "Bad Request", {}, None)

        def read(self):
            return json.dumps({"error": "a write is not exportable"}).encode()

    def fake_urlopen(*_args, **_kwargs):
        raise FakeHTTPError()

    monkeypatch.setattr(samyama_arrow.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(samyama_arrow.ExportError) as caught:
        samyama_arrow.table("CREATE (n)")
    assert "a write is not exportable" in str(caught.value)
    assert "400" in str(caught.value)


def test_an_unreachable_server_says_so(monkeypatch):
    def fake_urlopen(*_args, **_kwargs):
        raise urllib.error.URLError("Connection refused")

    monkeypatch.setattr(samyama_arrow.urllib.request, "urlopen", fake_urlopen)
    with pytest.raises(samyama_arrow.ExportError, match="could not reach"):
        samyama_arrow.table("RETURN 1", base_url="http://nope:1")
