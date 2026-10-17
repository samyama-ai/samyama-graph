"""Query results as Arrow tables and pandas frames, with no file in between (ML-01, INT-05).

`POST /api/query/export` streams a read query's result as Arrow IPC or writes it
as Parquet (#1101). This is the client half #1097 asked for: one call that hands
back a `pyarrow.Table` or a `pandas.DataFrame`, so a notebook or a training
script does not round-trip through a temporary file.

    from samyama_arrow import table, dataframe

    tbl = table("MATCH (p:Person) RETURN p.name AS name, p.age AS age")
    df = dataframe("MATCH (p:Person) RETURN p.name AS name", graph="people")

Why this is HTTP and not the extension module
---------------------------------------------

The Arrow/Parquet export exists only on the HTTP surface -- neither
`samyama-sdk` nor the pyo3 extension exposes it -- and reaching it from Python
needs no compiled extension. That matters for testing: building the extension
needs maturin and a full release compile, so the suites that depend on it are
excluded from the pull-request lane. This module is importable and testable
without any of that.

`urllib.request` from the standard library, deliberately: the SDK's runtime
dependencies are `fastmcp` and `pyyaml`, and a dataframe helper is a poor reason
to add an HTTP client to them.

pyarrow and pandas are optional
-------------------------------

Neither is a dependency of this SDK. Each is imported inside the function that
needs it and the ImportError names the package to install, rather than failing
at `import samyama_arrow` for someone who only wanted the rest of it -- the same
arrangement `samyama_nb` uses for pandas.

Reads only
----------

The endpoint refuses writes, as `/api/query/export` documents: a write whose
result is streamed to a column store leaves a partially consumed stream unable
to say whether the write happened. Use `query` on the client for those.
"""

from __future__ import annotations

import io
import json
import os
import urllib.error
import urllib.request
from typing import Any, Optional

__all__ = ["table", "dataframe", "parquet_bytes", "DEFAULT_BASE_URL", "ExportError"]

#: Where the server is, unless told otherwise. `SAMYAMA_HTTP` matches the
#: variable the integration suite already reads.
DEFAULT_BASE_URL = os.environ.get("SAMYAMA_HTTP", "http://localhost:8080")


class ExportError(RuntimeError):
    """The server refused the export, or could not be reached."""


def _post(base_url: str, body: dict, timeout: float) -> bytes:
    """POST `body` to the export endpoint and return the raw response bytes.

    Separated out so tests can exercise the decoding without a server.
    """
    request = urllib.request.Request(
        base_url.rstrip("/") + "/api/query/export",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        # The handler answers a refusal as JSON, and that message is the useful
        # part -- a bare "HTTP Error 400: Bad Request" hides which query failed.
        detail = exc.read().decode("utf-8", "replace").strip()
        try:
            detail = json.loads(detail).get("error", detail)
        except ValueError:
            pass
        raise ExportError(f"{exc.code} from /api/query/export: {detail}") from exc
    except urllib.error.URLError as exc:
        raise ExportError(f"could not reach {base_url}: {exc.reason}") from exc


def table(
    query: str,
    *,
    graph: str = "default",
    base_url: Optional[str] = None,
    via: str = "arrow",
    timeout: float = 300.0,
) -> Any:
    """Run `query` and return its result as a `pyarrow.Table`.

    `via` picks the wire format: `"arrow"` (IPC stream, the default) or
    `"parquet"`. Both are decoded in memory. The result is the same table either
    way, so the choice only matters if you want the Parquet encoder exercised --
    which is why both are reachable rather than just the default.
    """
    if via not in ("arrow", "parquet"):
        raise ValueError(f"via must be 'arrow' or 'parquet', not {via!r}")
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - exercised by hand, not in CI
        raise ImportError(
            "samyama_arrow.table needs pyarrow: pip install pyarrow"
        ) from exc

    payload = _post(
        base_url if base_url is not None else DEFAULT_BASE_URL,
        {"query": query, "graph": graph, "format": via},
        timeout,
    )
    if via == "arrow":
        with pa.ipc.open_stream(pa.BufferReader(payload)) as reader:
            return reader.read_all()
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(payload))


def dataframe(
    query: str,
    *,
    graph: str = "default",
    base_url: Optional[str] = None,
    via: str = "arrow",
    timeout: float = 300.0,
) -> Any:
    """Run `query` and return its result as a `pandas.DataFrame`.

    `table(...).to_pandas()`, which needs pandas as well as pyarrow.
    """
    try:
        import pandas  # noqa: F401
    except ImportError as exc:  # pragma: no cover - exercised by hand, not in CI
        raise ImportError(
            "samyama_arrow.dataframe needs pandas: pip install pandas"
        ) from exc
    return table(
        query, graph=graph, base_url=base_url, via=via, timeout=timeout
    ).to_pandas()


def parquet_bytes(
    query: str,
    *,
    graph: str = "default",
    base_url: Optional[str] = None,
    timeout: float = 300.0,
) -> bytes:
    """Run `query` and return its result as Parquet bytes, undecoded.

    For writing a `.parquet` file without pyarrow installed, or for handing the
    bytes to something that reads Parquet itself.
    """
    return _post(
        base_url if base_url is not None else DEFAULT_BASE_URL,
        {"query": query, "graph": graph, "format": "parquet"},
        timeout,
    )
