"""Async surface for the Samyama Python SDK (API-12).

``samyama.SamyamaClient`` is a compiled extension whose methods block. In an
asyncio application every one of those calls stops the event loop for as long
as the query runs, so a web handler that reads the graph stalls every other
request in the same process.

This module is the asyncio-shaped view of the same client. Each method runs the
blocking call on a worker thread and awaits it, so the loop keeps running::

    from samyama_aio import AsyncSamyamaClient

    async with await AsyncSamyamaClient.embedded() as db:
        result = await db.query("MATCH (n) RETURN count(n) AS n")

Why this is not just a wrapper
------------------------------

Moving a call to a thread does nothing on its own if the call holds the GIL,
and until samyama-graph#1436 every method here did: measured on a 1.28 s query,
another Python thread advanced **once** instead of the ~1,000 times it would
have if it were running. The extension now releases the GIL around the blocking
work, and the same measurement gives 1,310. Without that change this module
would look asynchronous and still stall the loop -- which is the more dangerous
of the two states, because the stall is no longer visible in the call site.

``tests/test_async_client.py`` asserts the loop keeps ticking during a query
rather than asserting that the methods are coroutines. A coroutine that blocks
is the thing being ruled out, so checking the shape would check the wrong half.

Cancellation
------------

``asyncio.CancelledError`` propagates to the caller as usual, but the worker
thread is **not** interrupted: Python cannot interrupt a thread inside a C
call, and the engine has no query-cancellation API (samyama-graph#1393). So a
cancelled ``await`` returns control to the loop while the query continues to
completion in the background. That is documented here rather than papered
over: a caller who needs a bound should use ``asyncio.timeout`` for
responsiveness and know the work is still running underneath.
"""

from __future__ import annotations

import asyncio
import functools
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, TypeVar

try:  # pragma: no cover - import shape differs only by build
    from samyama import SamyamaClient
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "samyama_aio needs the compiled `samyama` extension. Build it with "
        "`maturin develop` in sdk/python."
    ) from exc

__all__ = ["AsyncSamyamaClient"]

_T = TypeVar("_T")


async def _to_thread(fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:
    """`asyncio.to_thread`, with a fallback for Python 3.8.

    The package declares `requires-python = ">=3.8"` and `to_thread` arrived in
    3.9, so the fallback is load-bearing rather than defensive -- without it
    this module would raise AttributeError on a version the SDK claims to
    support.
    """
    if sys.version_info >= (3, 9):
        return await asyncio.to_thread(fn, *args, **kwargs)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, functools.partial(fn, *args, **kwargs))


class AsyncSamyamaClient:
    """An awaitable view of :class:`samyama.SamyamaClient`.

    Construct with :meth:`embedded` or :meth:`connect`; both are coroutines
    because both do real work (an embedded client builds a store, a remote one
    opens a connection), and a constructor that blocks would put the stall back
    in exactly the place this class exists to remove it from.
    """

    __slots__ = ("_client",)

    def __init__(self, client: SamyamaClient) -> None:
        self._client = client

    # ── construction ────────────────────────────────────────────────────────

    @classmethod
    async def embedded(cls) -> "AsyncSamyamaClient":
        """In-process client. No server."""
        return cls(await _to_thread(SamyamaClient.embedded))

    @classmethod
    async def connect(cls, url: str, **kwargs: Any) -> "AsyncSamyamaClient":
        """Client against a running server at `url`."""
        return cls(await _to_thread(SamyamaClient.connect, url, **kwargs))

    @property
    def sync(self) -> SamyamaClient:
        """The blocking client underneath.

        Exposed on purpose. Not every call belongs on a thread -- a cheap one
        in a startup path is better done directly than paid for with a context
        switch -- and hiding it would push callers to build a second client.
        """
        return self._client

    # ── context management ──────────────────────────────────────────────────

    async def __aenter__(self) -> "AsyncSamyamaClient":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        # Nothing to release: the extension owns its store and its connection,
        # and drops them when the object does. The context manager exists for
        # the shape API-12 asks for, and `close` is deliberately absent rather
        # than present and empty -- a no-op close reads as a resource being
        # released.
        return None

    # ── queries ─────────────────────────────────────────────────────────────

    async def query(self, cypher: str, graph: str = "default") -> Any:
        return await _to_thread(self._client.query, cypher, graph)

    async def query_readonly(self, cypher: str, graph: str = "default") -> Any:
        return await _to_thread(self._client.query_readonly, cypher, graph)

    async def status(self) -> Any:
        return await _to_thread(self._client.status)

    async def ping(self) -> str:
        return await _to_thread(self._client.ping)

    async def list_graphs(self) -> List[str]:
        return await _to_thread(self._client.list_graphs)

    async def delete_graph(self, graph: str = "default") -> None:
        return await _to_thread(self._client.delete_graph, graph)

    # ── algorithms ──────────────────────────────────────────────────────────

    async def page_rank(
        self,
        label: Optional[str] = None,
        edge_type: Optional[str] = None,
        damping: float = 0.85,
        iterations: int = 20,
        tolerance: float = 1e-6,
    ) -> Dict[int, float]:
        return await _to_thread(
            self._client.page_rank, label, edge_type, damping, iterations, tolerance
        )

    async def wcc(
        self, label: Optional[str] = None, edge_type: Optional[str] = None
    ) -> Dict[str, Any]:
        return await _to_thread(self._client.wcc, label, edge_type)

    async def scc(
        self, label: Optional[str] = None, edge_type: Optional[str] = None
    ) -> Dict[str, Any]:
        return await _to_thread(self._client.scc, label, edge_type)

    async def bfs(
        self,
        source: int,
        target: Optional[int] = None,
        label: Optional[str] = None,
        edge_type: Optional[str] = None,
    ) -> Any:
        return await _to_thread(self._client.bfs, source, target, label, edge_type)

    async def dijkstra(
        self,
        source: int,
        target: Optional[int] = None,
        label: Optional[str] = None,
        edge_type: Optional[str] = None,
        weight_property: Optional[str] = None,
    ) -> Any:
        return await _to_thread(
            self._client.dijkstra, source, target, label, edge_type, weight_property
        )

    async def triangle_count(
        self, label: Optional[str] = None, edge_type: Optional[str] = None
    ) -> int:
        return await _to_thread(self._client.triangle_count, label, edge_type)

    # ── vectors ─────────────────────────────────────────────────────────────

    async def create_vector_index(
        self, label: str, property: str, dimensions: int, metric: str = "cosine"
    ) -> None:
        return await _to_thread(
            self._client.create_vector_index, label, property, dimensions, metric
        )

    async def add_vector(
        self, label: str, property: str, node_id: int, vector: Sequence[float]
    ) -> None:
        return await _to_thread(
            self._client.add_vector, label, property, node_id, list(vector)
        )

    async def vector_search(
        self, label: str, property: str, query_vector: Sequence[float], k: int
    ) -> List[Tuple[int, float]]:
        return await _to_thread(
            self._client.vector_search, label, property, list(query_vector), k
        )

    def __repr__(self) -> str:
        return f"AsyncSamyamaClient({self._client!r})"
