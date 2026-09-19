"""Type information for the `samyama` extension module.

The Python SDK is a PyO3 extension: its surface is defined in Rust and
compiled, so a type checker importing `samyama` sees an opaque module and
reports nothing -- no signatures, no return types, no error on a call that
cannot work. API-12 asks for Python typing, and `CH-SDK-RT` measured
`python_ships_type_information: False` for exactly this reason.

These stubs are hand-written against `sdk/python/src/lib.rs` and can drift from
it. `tests/test_stub_matches_extension.py` compares them against the compiled
module at test time, so a method added, renamed or re-signed in Rust without a
stub change fails rather than silently going untyped.

Every signature here carries the defaults declared in the Rust
`#[pyo3(signature = ...)]` attributes, because a default that disagrees
between the stub and the extension is worse than no stub: a caller omitting an
argument gets one value and is told another.
"""

from typing import Any, Final

__all__ = ["SamyamaClient", "QueryResult", "ServerStatus"]

class QueryResult:
    """One query's answer: a table, plus the graph elements it referenced."""

    @property
    def columns(self) -> list[str]: ...
    @property
    def records(self) -> list[list[Any]]:
        """Rows, each a list positionally aligned with `columns`."""
        ...
    @property
    def nodes(self) -> list[dict[str, Any]]: ...
    @property
    def edges(self) -> list[dict[str, Any]]: ...
    def __len__(self) -> int:
        """The number of records, not of nodes."""
        ...
    def __repr__(self) -> str: ...

class ServerStatus:
    @property
    def status(self) -> str: ...
    @property
    def version(self) -> str: ...
    @property
    def nodes(self) -> int: ...
    @property
    def edges(self) -> int: ...
    def __repr__(self) -> str: ...

class SamyamaClient:
    """Embedded or remote. `embedded()` and `connect()` are the constructors.

    The algorithm methods are embedded-only; calling one on a remote client
    raises, because they run against the store directly rather than over the
    wire.
    """

    @staticmethod
    def embedded() -> SamyamaClient:
        """An in-process store. No server, no socket."""
        ...
    @staticmethod
    def connect(
        url: str,
        timeout_seconds: float | None = 30.0,
        connect_timeout_seconds: float | None = 5.0,
        max_retries: int = 2,
        retry_base_delay_ms: int = 100,
    ) -> SamyamaClient:
        """A client for a server already running at `url`.

        `timeout_seconds` bounds a whole request; `None` means no timeout, which
        is what this used to do unconditionally (samyama-graph#1326).
        """
        ...

    def query(self, cypher: str, graph: str = "default") -> QueryResult: ...
    def query_readonly(self, cypher: str, graph: str = "default") -> QueryResult:
        """Refuses a write rather than performing one."""
        ...
    def status(self) -> ServerStatus: ...
    def ping(self) -> str: ...
    def delete_graph(self, graph: str = "default") -> None: ...
    def list_graphs(self) -> list[str]: ...

    # Algorithms. Embedded only.
    def page_rank(
        self,
        label: str | None = None,
        edge_type: str | None = None,
        damping: float = 0.85,
        iterations: int = 20,
        tolerance: float = 1e-6,
    ) -> list[tuple[int, float]]: ...
    def wcc(
        self, label: str | None = None, edge_type: str | None = None
    ) -> list[tuple[int, int]]: ...
    def scc(
        self, label: str | None = None, edge_type: str | None = None
    ) -> list[tuple[int, int]]: ...
    def bfs(
        self,
        source: int,
        target: int,
        label: str | None = None,
        edge_type: str | None = None,
    ) -> Any: ...
    def dijkstra(
        self,
        source: int,
        target: int,
        label: str | None = None,
        edge_type: str | None = None,
        weight_property: str | None = None,
    ) -> Any:
        """Refuses a graph with a negative weight rather than skipping the edge
        and answering about a different graph (samyama-graph#1303)."""
        ...
    def pca(
        self,
        properties: list[str],
        label: str | None = None,
        n_components: int = 2,
    ) -> Any: ...
    def triangle_count(
        self, label: str | None = None, edge_type: str | None = None
    ) -> int: ...

    # Vector search.
    def create_vector_index(
        self, label: str, property: str, dimensions: int, metric: str = "cosine"
    ) -> None: ...
    def add_vector(
        self, label: str, property: str, node_id: int, vector: list[float]
    ) -> None: ...
    def vector_search(
        self, label: str, property: str, query_vector: list[float], k: int = 10
    ) -> list[tuple[int, float]]: ...
    def __repr__(self) -> str: ...
