"""`SamyamaClient.query_arrow` and `SamyamaClient.export` (ML-01, #2267).

The client itself hands back a `pyarrow.Table`, in both modes:

* **embedded** -- the engine's export encoder runs in this process, so there is
  no server and no HTTP. These cases run a real engine.
* **remote** -- `POST /api/query/export`. Against a small local server that
  records what it was sent and answers with Arrow built by pyarrow, so the
  cases check the request the client builds and how it decodes and refuses.
  That the real endpoint and the embedded encoder give the same table is
  checked in Rust, on one shared store: `crates/samyama-sdk/tests/export_arrow.rs`.

These need the compiled extension (`maturin develop`), like `test_embedded.py`.
"""

import asyncio
import io
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pa = pytest.importorskip("pyarrow", reason="query_arrow returns a pyarrow.Table")
import pyarrow.parquet as pq

import samyama
from samyama import SamyamaClient
from samyama_aio import AsyncSamyamaClient

PEOPLE = (
    "MATCH (p:P) RETURN p.name AS name, p.age AS age, p.score AS score, "
    "p.active AS active, p.tags AS tags ORDER BY name"
)


@pytest.fixture
def people():
    c = SamyamaClient.embedded()
    c.query(
        "CREATE (:P {name: 'Ada', age: 36, score: 9.5, active: true, tags: ['a', 'b']}), "
        "(:P {name: 'Bo', age: 41, score: 7.25, active: false, tags: []}), "
        "(:P {name: 'Cy', age: 29, active: true, tags: ['c']})"
    )
    return c


def assert_people(tbl):
    assert isinstance(tbl, pa.Table)
    assert tbl.column_names == ["name", "age", "score", "active", "tags"]
    assert tbl.schema.field("name").type == pa.string()
    assert tbl.schema.field("age").type == pa.int64()
    assert tbl.schema.field("score").type == pa.float64()
    assert tbl.schema.field("active").type == pa.bool_()
    assert tbl.schema.field("tags").type == pa.list_(pa.string())
    assert tbl.to_pydict() == {
        "name": ["Ada", "Bo", "Cy"],
        "age": [36, 41, 29],
        "score": [9.5, 7.25, None],
        "active": [True, False, True],
        "tags": [["a", "b"], [], ["c"]],
    }


# ── embedded: no server ──────────────────────────────────────────────────────


def test_embedded_query_arrow_is_a_typed_table(people):
    assert_people(people.query_arrow(PEOPLE))


def test_the_table_converts_to_pandas_with_its_types(people):
    pd = pytest.importorskip("pandas")
    df = people.query_arrow(PEOPLE).to_pandas()
    assert list(df["name"]) == ["Ada", "Bo", "Cy"]
    assert str(df["age"].dtype) == "int64"
    assert pd.isna(df["score"][2])


def test_export_bytes_decode_to_the_same_table_in_both_formats(people):
    expected = people.query_arrow(PEOPLE)
    ipc = people.export(PEOPLE)
    assert isinstance(ipc, bytes)
    with pa.ipc.open_stream(pa.BufferReader(ipc)) as reader:
        assert reader.read_all().equals(expected)
    parquet = people.export(PEOPLE, format="parquet")
    assert parquet[:4] == b"PAR1"
    assert pq.read_table(io.BytesIO(parquet)).equals(expected)
    # Case-insensitive, as the endpoint is.
    assert people.export(PEOPLE, format="ARROW") == ipc


def test_an_empty_result_keeps_its_columns(people):
    tbl = people.query_arrow("MATCH (p:P) WHERE p.age > 100 RETURN p.name AS name, p.age AS age")
    assert tbl.num_rows == 0
    assert tbl.column_names == ["name", "age"]


def test_a_write_is_refused_and_nothing_changes(people):
    for q in ("CREATE (:P {name: 'Zed'})", "MATCH (p:P) DETACH DELETE p"):
        with pytest.raises(RuntimeError, match="read-only"):
            people.query_arrow(q)
        with pytest.raises(RuntimeError, match="read-only"):
            people.export(q, format="parquet")
    assert people.query_arrow("MATCH (p:P) RETURN count(p) AS n").to_pydict() == {"n": [3]}


def test_an_unknown_format_or_graph_is_refused_with_the_choices(people):
    with pytest.raises(ValueError, match=r"unknown export format 'csv'; use 'arrow' or 'parquet'"):
        people.export(PEOPLE, format="csv")
    with pytest.raises(RuntimeError, match="single graph"):
        people.query_arrow(PEOPLE, graph="other")


def test_without_pyarrow_the_error_names_the_package(people, monkeypatch):
    # `None` in sys.modules makes `import pyarrow` raise ImportError.
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    with pytest.raises(ImportError, match="pip install pyarrow"):
        people.query_arrow(PEOPLE)
    # `export` does not need pyarrow at all.
    assert len(people.export(PEOPLE)) > 0


def test_a_cancelled_token_stops_the_call_before_it_runs(people):
    token = samyama.CancelToken()
    token.cancel()
    with pytest.raises(samyama.RequestCancelled):
        people.query_arrow(PEOPLE, cancel=token)
    with pytest.raises(samyama.RequestCancelled):
        people.export(PEOPLE, cancel=token)


def test_the_async_client_returns_the_same_table():
    async def run():
        db = await AsyncSamyamaClient.embedded()
        await db.query("CREATE (:P {name: 'Ada', age: 36})")
        tbl = await db.query_arrow("MATCH (p:P) RETURN p.name AS name, p.age AS age")
        raw = await db.export("MATCH (p:P) RETURN p.name AS name", format="parquet")
        return tbl, raw

    tbl, raw = asyncio.run(run())
    assert tbl.to_pydict() == {"name": ["Ada"], "age": [36]}
    assert pq.read_table(io.BytesIO(raw)).to_pydict() == {"name": ["Ada"]}


# ── remote: POST /api/query/export ───────────────────────────────────────────


class ExportServer:
    """Answers POST /api/query/export with `status` and `body`, recording requests."""

    def __init__(self, status, body, content_type="application/vnd.apache.arrow.stream"):
        self.requests = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                outer.requests.append((self.path, json.loads(self.rfile.read(n) or b"{}")))
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def url(self):
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def close(self):
        self._server.shutdown()
        self._server.server_close()


def ipc_of(tbl):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as w:
        w.write_table(tbl)
    return sink.getvalue().to_pybytes()


def test_remote_query_arrow_posts_to_the_export_endpoint_and_decodes():
    sent = pa.table({"name": ["Ada", None], "age": pa.array([36, 41], pa.int64())})
    srv = ExportServer(200, ipc_of(sent))
    try:
        c = SamyamaClient.connect(srv.url, max_retries=0)
        got = c.query_arrow("MATCH (p:P) RETURN p.name AS name, p.age AS age")
        assert got.equals(sent)
        raw = c.export("RETURN 1 AS x", format="parquet")
        assert raw == ipc_of(sent), "export hands back the body undecoded"
    finally:
        srv.close()
    assert srv.requests == [
        (
            "/api/query/export",
            {"query": "MATCH (p:P) RETURN p.name AS name, p.age AS age", "graph": "default", "format": "arrow"},
        ),
        ("/api/query/export", {"query": "RETURN 1 AS x", "graph": "default", "format": "parquet"}),
    ]


def test_remote_refusal_carries_the_servers_message():
    body = json.dumps({"error": "unknown format 'xlsx'"}).encode()
    srv = ExportServer(400, body, content_type="application/json")
    try:
        c = SamyamaClient.connect(srv.url, max_retries=0)
        with pytest.raises(RuntimeError, match=r"400 from /api/query/export: unknown format 'xlsx'"):
            c.query_arrow("RETURN 1 AS x")
    finally:
        srv.close()
