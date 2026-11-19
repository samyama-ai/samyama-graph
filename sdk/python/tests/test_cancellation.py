"""A request in flight can be stopped, and the pool is the caller's to set (API-06, #1741).

Every blocking call used to be ``block_on`` with the GIL released: until the
server answered, nothing could reach the thread. Ctrl-C waited for the query,
and ``samyama_aio`` documented that a cancelled task left its query running
on the worker thread. These cases stand up servers that never answer, or
answer slowly, and check from the **server's** side of the socket that a
cancelled request really goes away -- the connection is closed -- rather than
trusting the exception the client raises.

They need the compiled extension, so like ``test_connection.py`` they run in
the nightly ``python-extension`` job, not in PR CI.
"""

import asyncio
import json
import signal
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import samyama
from samyama import CancelToken, RequestCancelled


class Silent:
    """Accepts connections, reads nothing back, and notices when one closes.

    A cancelled request must close its connection: that is what tells a
    streaming server to stop, and what frees the socket. `closed` is set when
    the client hangs up.
    """

    def __init__(self):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self._listener.settimeout(0.05)
        self.accepted = 0
        self.closed = threading.Event()
        self._stop = threading.Event()
        threading.Thread(target=self._run, daemon=True).start()

    @property
    def url(self):
        host, port = self._listener.getsockname()
        return f"http://{host}:{port}"

    def _run(self):
        while not self._stop.is_set():
            try:
                sock, _ = self._listener.accept()
            except socket.timeout:
                continue
            self.accepted += 1
            threading.Thread(target=self._hold, args=(sock,), daemon=True).start()

    def _hold(self, sock):
        sock.settimeout(0.05)
        while not self._stop.is_set():
            try:
                if sock.recv(65536) == b"":
                    self.closed.set()
                    return
            except socket.timeout:
                continue
            except OSError:
                self.closed.set()
                return

    def close(self):
        self._stop.set()
        self._listener.close()


@pytest.fixture
def silent():
    s = Silent()
    yield s
    s.close()


STATUS = {
    "status": "healthy",
    "version": "test",
    "storage": {"nodes": 0, "edges": 0},
    "cache": {"hits": 0, "misses": 0, "size": 0},
}


class Answering:
    """A keep-alive HTTP server that counts the connections it is given and
    streams query rows slowly, one every `row_delay` seconds, with no end."""

    def __init__(self, row_delay=0.05):
        owner = self
        self.ports = []
        self.stream_closed = threading.Event()

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def setup(self):
                super().setup()
                owner.ports.append(self.client_address[1])

            def _json(self, body):
                data = json.dumps(body).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                self._json(STATUS)

            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()

                def chunk(obj):
                    line = (json.dumps(obj) + "\n").encode()
                    self.wfile.write(b"%x\r\n%s\r\n" % (len(line), line))
                    self.wfile.flush()

                try:
                    chunk({"columns": ["i"], "snapshot_version": 1, "engine_version": "test"})
                    i = 0
                    while True:
                        chunk({"row": [i]})
                        i += 1
                        time.sleep(row_delay)
                except OSError:
                    owner.stream_closed.set()
                    self.close_connection = True

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._server.daemon_threads = True
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def url(self):
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    @property
    def connections(self):
        return len(set(self.ports))

    def close(self):
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def answering():
    s = Answering()
    yield s
    s.close()


#: The client's own timeout, as a backstop. Cancellation is expected to land
#: in well under a second; if it is broken, the call ends here with a timeout
#: -- the wrong exception, so the test fails -- instead of hanging the run.
BACKSTOP_SECONDS = 5.0


def bounded(url, **kw):
    """No retry, and only the backstop timeout: cancellation has to end a call
    to `Silent` long before the timeout does."""
    return samyama.SamyamaClient.connect(
        url, timeout_seconds=BACKSTOP_SECONDS, max_retries=0, **kw
    )


# ── cancellation ────────────────────────────────────────────────────────────


def test_a_token_stops_a_query_the_server_never_answers(silent):
    client = bounded(silent.url)
    token = CancelToken()
    threading.Timer(0.3, token.cancel).start()
    start = time.monotonic()
    with pytest.raises(RequestCancelled):
        client.query("RETURN 1", cancel=token)
    took = time.monotonic() - start
    assert 0.25 <= took < 1.5, f"cancel took {took:.2f}s to land"
    assert token.cancelled
    assert silent.closed.wait(2), "the cancelled request kept its connection open"


def test_every_cancellable_call_takes_a_token(silent):
    client = bounded(silent.url)
    calls = [
        lambda t: client.query("RETURN 1", cancel=t),
        lambda t: client.query_readonly("RETURN 1", cancel=t),
        lambda t: client.query_stream("RETURN 1", cancel=t),
        lambda t: client.nlq("how many nodes?", cancel=t),
    ]
    for call in calls:
        token = CancelToken()
        threading.Timer(0.1, token.cancel).start()
        with pytest.raises(RequestCancelled):
            call(token)


def test_a_cancelled_token_stops_the_next_call_before_it_is_sent(silent):
    client = bounded(silent.url)
    token = CancelToken()
    token.cancel()
    with pytest.raises(RequestCancelled):
        client.query("RETURN 1", cancel=token)
    time.sleep(0.2)
    assert silent.accepted == 0, "a request went out on a cancelled token"


def test_a_stream_can_be_cancelled_between_rows_and_the_server_stops(answering):
    client = samyama.SamyamaClient.connect(answering.url)
    token = CancelToken()
    rows = 0
    with pytest.raises(RequestCancelled):
        for _row in client.query_stream("UNWIND range(1, 1000000) AS i RETURN i", cancel=token):
            rows += 1
            if rows == 3:
                token.cancel()
            # The server never ends this stream: rows still arriving after
            # cancel() is the failure, reported rather than looped on.
            assert rows < 50, "rows kept coming after cancel()"
    assert rows == 3
    assert answering.stream_closed.wait(2), "the server kept streaming to a cancelled reader"


def test_ctrl_c_interrupts_a_blocked_call(silent):
    """`KeyboardInterrupt` would end the pytest session, so the SIGINT handler
    raises a marker exception instead; the point is that Python's signal
    handler gets to run at all while the call is blocked."""

    class Interrupted(Exception):
        pass

    def handler(signum, frame):
        raise Interrupted()

    previous = signal.signal(signal.SIGINT, handler)
    try:
        client = bounded(silent.url)
        threading.Timer(0.3, lambda: signal.raise_signal(signal.SIGINT)).start()
        start = time.monotonic()
        with pytest.raises(Interrupted):
            client.query("RETURN 1")
        assert time.monotonic() - start < 1.5
    finally:
        signal.signal(signal.SIGINT, previous)
    assert silent.closed.wait(2), "the interrupted request kept its connection open"


def test_cancelling_an_asyncio_task_stops_the_request_underneath(silent):
    from samyama_aio import AsyncSamyamaClient

    async def run():
        db = await AsyncSamyamaClient.connect(
            silent.url, timeout_seconds=BACKSTOP_SECONDS, max_retries=0
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(db.query("RETURN 1"), 0.3)

    asyncio.run(run())
    # Before #1741 the worker thread waited on the server forever and the
    # connection stayed open; now the token reaches it and it hangs up.
    # Well inside the backstop: a connection closed only by the 5 s timeout
    # is the failure this test exists to catch.
    assert silent.closed.wait(2), "the cancelled task left its request running"


# ── pooling ─────────────────────────────────────────────────────────────────


def test_connections_are_reused_by_default(answering):
    client = samyama.SamyamaClient.connect(answering.url)
    for _ in range(4):
        client.status()
    assert answering.connections == 1, f"{answering.connections} connections for 4 calls"


def test_pool_max_idle_per_host_zero_turns_reuse_off(answering):
    client = samyama.SamyamaClient.connect(answering.url, pool_max_idle_per_host=0)
    for _ in range(4):
        client.status()
    assert answering.connections == 4, f"{answering.connections} connections for 4 calls"


@pytest.mark.parametrize("bad", [0, -1.0, float("nan"), float("inf")])
def test_a_nonsense_pool_idle_timeout_is_rejected(bad):
    with pytest.raises(RuntimeError):
        samyama.SamyamaClient.connect("http://127.0.0.1:1", pool_idle_timeout_seconds=bad)


def test_no_pool_idle_timeout_is_accepted():
    samyama.SamyamaClient.connect("http://127.0.0.1:1", pool_idle_timeout_seconds=None)
