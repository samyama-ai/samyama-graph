"""A stalled server does not hang the Python caller (API-06, #1326).

`SamyamaClient.connect` takes `timeout_seconds`, `connect_timeout_seconds`,
`max_retries` and `retry_base_delay_ms` and hands them to the Rust client.
Reading the signature says the options exist; it does not say they reach the
transport. These cases stand up a server that **accepts the connection and then
says nothing** -- the failure the issue is about -- and measure what the
binding does.

A server that refuses the connection would not test this: a refused connect
fails fast with or without a deadline. Silence is the case that matters.

These tests need the compiled extension, so they run in the nightly
`python-extension` job rather than in PR CI, the same as `test_embedded.py`.
"""

import socket
import threading
import time

import pytest

import samyama


class BlackHole:
    """A TCP listener that accepts connections and never replies.

    Accepted sockets are kept open rather than dropped: closing one would hand
    the client an EOF, which is an answer, not silence.
    """

    def __init__(self):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self._listener.settimeout(0.05)
        self._held = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    @property
    def url(self):
        host, port = self._listener.getsockname()
        return f"http://{host}:{port}"

    @property
    def connections(self):
        with self._lock:
            return len(self._held)

    def _run(self):
        while not self._stop.is_set():
            try:
                sock, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            with self._lock:
                self._held.append(sock)

    def close(self):
        self._stop.set()
        self._thread.join(timeout=1)
        with self._lock:
            for s in self._held:
                s.close()
        self._listener.close()


@pytest.fixture
def black_hole():
    server = BlackHole()
    yield server
    server.close()


def test_a_silent_server_does_not_hang_the_caller(black_hole):
    client = samyama.SamyamaClient.connect(
        black_hole.url, timeout_seconds=0.3, max_retries=0
    )
    started = time.monotonic()
    with pytest.raises(Exception):
        client.query("RETURN 1")
    elapsed = time.monotonic() - started
    # The bound rather than an exact time: a slow host may take longer to give
    # up, but it may not fail to give up. Without the deadline this call never
    # returns, so the test hangs instead of failing -- which is why the elapsed
    # time is asserted and not only the exception.
    assert elapsed < 5.0, f"took {elapsed:.2f}s against a 0.3 s deadline"
    assert black_hole.connections == 1


def test_every_remote_method_is_bounded(black_hole):
    # One deadline for the transport, not one per call site: check the calls
    # that do not go through `query` as well.
    client = samyama.SamyamaClient.connect(
        black_hole.url, timeout_seconds=0.3, max_retries=0
    )
    for call in (
        lambda: client.query_readonly("RETURN 1"),
        client.status,
        client.ping,
    ):
        started = time.monotonic()
        with pytest.raises(Exception):
            call()
        assert time.monotonic() - started < 5.0


def test_a_timeout_is_retried_the_configured_number_of_times(black_hole):
    # Counted at the server, one connection per attempt. Timing alone would
    # pass for a client that never retried and simply waited longer.
    client = samyama.SamyamaClient.connect(
        black_hole.url,
        timeout_seconds=0.15,
        max_retries=2,
        retry_base_delay_ms=10,
    )
    with pytest.raises(Exception):
        client.query("RETURN 1")
    assert black_hole.connections == 3, "expected one attempt plus two retries"


@pytest.mark.parametrize("bad", [0.0, -1.0, float("inf"), float("nan")])
def test_a_nonsense_timeout_is_rejected(bad):
    # Zero or a negative deadline would fail every request; infinity is
    # `None` spelled in a way nobody means. Refuse them at construction.
    with pytest.raises(RuntimeError, match="timeout"):
        samyama.SamyamaClient.connect("http://127.0.0.1:1", timeout_seconds=bad)


def test_no_timeout_is_an_explicit_choice():
    # `None` restores the unbounded behaviour on purpose; it must be accepted,
    # not mistaken for a missing argument.
    client = samyama.SamyamaClient.connect(
        "http://127.0.0.1:1", timeout_seconds=None, connect_timeout_seconds=None
    )
    assert "remote" in repr(client)
