"""`query_readonly` refuses writes, and `query_stream` reads the NDJSON stream
(#1628, #1632).

Against a small local HTTP server that records what it was sent and answers
with a canned body, so the cases cover the protocol edges (an error trailer, a
missing trailer, a client that stops early) that a healthy engine never
produces on demand.

These tests need the compiled extension, so they run in the nightly
`python-extension` job rather than in PR CI, the same as `test_embedded.py`.
"""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import samyama
from samyama_aio import AsyncSamyamaClient


class Server:
    """Answers every POST with `respond(handler)`, recording request bodies."""

    def __init__(self, respond):
        self.bodies = []
        self.headers = []
        self.finished = threading.Event()
        self.written = 0
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def do_POST(self):
                n = int(self.headers.get("Content-Length", 0))
                outer.bodies.append(json.loads(self.rfile.read(n) or b"{}"))
                outer.headers.append(dict(self.headers))
                try:
                    respond(self, outer)
                finally:
                    outer.finished.set()

            def log_message(self, *args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def url(self):
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def close(self):
        self._server.shutdown()
        self._server.server_close()


def ndjson(handler, lines):
    handler.send_response(200)
    handler.send_header("Content-Type", "application/x-ndjson")
    handler.end_headers()
    for line in lines:
        handler.wfile.write((json.dumps(line) + "\n").encode())


def client(server):
    return samyama.SamyamaClient.connect(server.url, max_retries=0)


def test_query_readonly_asks_the_server_to_refuse_a_write():
    def respond(h, _):
        if h.path == "/api/query":
            h.send_response(403)
            h.send_header("Content-Type", "application/json")
            h.end_headers()
            h.wfile.write(json.dumps({"error": "this request is read-only"}).encode())

    s = Server(respond)
    with pytest.raises(RuntimeError, match="read-only"):
        client(s).query_readonly("MATCH (n) DETACH DELETE n")
    assert s.bodies[-1]["read_only"] is True
    s.close()


def test_a_stream_yields_dicts_by_column_and_ends_at_the_trailer():
    rows = [{"row": [i, 2 * i]} for i in range(1, 1001)]
    s = Server(lambda h, _: ndjson(h, [{"columns": ["i", "twice"]}, *rows, {"done": True, "rows": 1000}]))
    stream = client(s).query_stream("UNWIND range(1, 1000) AS i RETURN i, i * 2 AS twice")
    assert stream.columns == ["i", "twice"]
    got = list(stream)
    assert got == [{"i": i, "twice": 2 * i} for i in range(1, 1001)]
    assert s.headers[0].get("accept") == "application/x-ndjson"
    assert s.bodies[0]["read_only"] is True
    s.close()


def test_an_error_trailer_raises_after_the_rows_before_it():
    s = Server(lambda h, _: ndjson(h, [{"columns": ["i"]}, {"row": [1]}, {"error": "boom", "rows": 1}]))
    stream = client(s).query_stream("RETURN 1")
    assert next(stream) == {"i": 1}
    with pytest.raises(RuntimeError, match="boom"):
        next(stream)
    s.close()


def test_a_body_without_a_trailer_is_incomplete():
    s = Server(lambda h, _: ndjson(h, [{"columns": ["i"]}, {"row": [1]}]))
    stream = client(s).query_stream("RETURN 1")
    assert next(stream) == {"i": 1}
    with pytest.raises(RuntimeError, match="without a trailer"):
        next(stream)
    s.close()


def test_closing_early_stops_the_server_writing():
    """The handler writes until the socket refuses; closing the stream must
    make it refuse, or the handler would write forever."""

    def respond(h, srv):
        ndjson(h, [{"columns": ["i"]}])
        try:
            while True:
                srv.written += 1
                h.wfile.write((json.dumps({"row": [srv.written, "x" * 200]}) + "\n").encode())
        except (BrokenPipeError, ConnectionResetError):
            pass

    s = Server(respond)
    with client(s).query_stream("UNWIND range(1, 1000000000) AS i RETURN i") as stream:
        for expected in range(1, 6):
            assert next(stream)["i"] == expected
    assert s.finished.wait(60), "the server kept writing after the client closed"
    assert s.written < 1_000_000
    s.close()


def test_the_async_client_iterates_a_stream():
    s = Server(lambda h, _: ndjson(h, [{"columns": ["i"]}, {"row": [1]}, {"row": [2]}, {"done": True}]))

    async def run():
        db = await AsyncSamyamaClient.connect(s.url, max_retries=0)
        stream = await db.query_stream("UNWIND [1, 2] AS i RETURN i")
        return [row async for row in stream]

    assert asyncio.run(run()) == [{"i": 1}, {"i": 2}]
    s.close()


def test_the_embedded_client_iterates_its_result():
    db = samyama.SamyamaClient.embedded()
    db.query("CREATE (:T {v: 1}), (:T {v: 2})")
    rows = list(db.query_stream("MATCH (n:T) RETURN n.v AS v ORDER BY v"))
    assert rows == [{"v": 1}, {"v": 2}]
