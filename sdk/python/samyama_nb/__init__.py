"""Jupyter integration for Samyama (INT-09).

INT-09 asks for "Jupyter magic + a pandas/polars-native result type". Neither
existed: no magic was registered anywhere under `sdk/`, and `QueryResult` hands
back a list of lists, which a notebook renders as a list of lists.

    %load_ext samyama_nb
    %cypher_connect --embedded

    %%cypher
    MATCH (j:Judge)-[:DECIDED]->(c:Case)
    RETURN j.name AS judge, count(c) AS cases
    ORDER BY cases DESC LIMIT 5

The cell returns a `pandas.DataFrame`, so the next cell can plot it.

Reads by default, writes on request
-----------------------------------

`%%cypher` runs `query_readonly`. A notebook is re-run top to bottom, often by
somebody who did not write it, and a cell that silently writes turns a
re-execution into a mutation — twice the nodes, and no error to notice. Writes
need `%%cypher --write`, which is one word and makes the intent visible in the
cell that carries it.

The magic returns rather than prints
------------------------------------

`df = %cypher MATCH (n) RETURN n` works, and so does a bare `%%cypher` cell
whose value Jupyter displays. A magic that printed a formatted table would look
better in one cell and be useless in the next.

pandas is optional
------------------

When pandas is not installed the result is a list of dicts — the same data,
one row per record, still keyed by column name. That is a smaller thing to
receive, not a different one, and it means `samyama_nb` does not drag a
dataframe library into an SDK whose other users may not want one.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

__all__ = [
    "to_dataframe",
    "rows",
    "SamyamaMagics",
    "load_ipython_extension",
]


def rows(result: Any) -> List[Dict[str, Any]]:
    """`result` as a list of dicts, one per record, keyed by column name.

    The shape everything else here is built from, and the fallback when pandas
    is absent.
    """
    columns = list(getattr(result, "columns", []) or [])
    out = []
    for record in getattr(result, "records", []) or []:
        # A record shorter than the header is a bug upstream, but truncating
        # silently would hide it; zip to the shorter and let the missing column
        # be missing rather than inventing a None for it.
        out.append({c: v for c, v in zip(columns, record)})
    return out


def to_dataframe(result: Any) -> Union["Any", List[Dict[str, Any]]]:
    """A `pandas.DataFrame` of `result`, or a list of dicts without pandas.

    The columns come from the result rather than from the data, so a query that
    returned no rows still gives a frame with the right headers. A zero-row
    frame with no columns is indistinguishable from a failure when you are
    looking at it in a notebook.
    """
    data = rows(result)
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        return data
    return pd.DataFrame(data, columns=list(getattr(result, "columns", []) or []))


def _connect(arg: str):
    """Build a client from the argument to `%cypher_connect`."""
    from samyama import SamyamaClient  # noqa: PLC0415

    arg = (arg or "").strip()
    if not arg or arg == "--embedded":
        return SamyamaClient.embedded()
    return SamyamaClient.connect(arg)


class SamyamaMagics:
    """`%cypher_connect`, `%cypher` and `%%cypher`.

    Written as a plain class with the IPython decorators applied in
    `load_ipython_extension` rather than as a `@magics_class`, so that the
    module imports — and `to_dataframe` stays usable — on a machine without
    IPython. An SDK helper that cannot be imported outside a notebook is one
    that cannot be unit-tested.
    """

    def __init__(self, shell=None):
        self.shell = shell
        self.client = None

    # ── %cypher_connect ─────────────────────────────────────────────────────

    def cypher_connect(self, line: str = "") -> str:
        """`%cypher_connect [--embedded | <url>]`"""
        self.client = _connect(line)
        return repr(self.client)

    # ── %cypher / %%cypher ──────────────────────────────────────────────────

    def cypher(self, line: str = "", cell: Optional[str] = None):
        """Run Cypher. `--write` opts into a mutating query."""
        write = "--write" in line
        flags = ("--write",)
        head = " ".join(w for w in line.split() if w not in flags)
        query = f"{head}\n{cell}" if cell else head
        query = query.strip()

        if not query:
            raise ValueError(
                "no query. Use `%cypher MATCH (n) RETURN n` or a `%%cypher` cell."
            )
        if self.client is None:
            raise RuntimeError(
                "not connected. Run `%cypher_connect --embedded` for an in-process "
                "graph, or `%cypher_connect http://host:8080` for a server."
            )

        # Read by default. See the module docstring: a notebook is re-run, and
        # a cell that silently writes turns a re-execution into a mutation.
        result = (
            self.client.query(query) if write else self.client.query_readonly(query)
        )
        return to_dataframe(result)


def load_ipython_extension(ipython) -> None:
    """`%load_ext samyama_nb`."""
    from IPython.core.magic import register_cell_magic, register_line_magic  # noqa: PLC0415

    magics = SamyamaMagics(ipython)

    @register_line_magic("cypher_connect")
    def _connect_magic(line):  # noqa: ANN001
        return magics.cypher_connect(line)

    @register_line_magic("cypher")
    def _cypher_line(line):  # noqa: ANN001
        return magics.cypher(line)

    @register_cell_magic("cypher")
    def _cypher_cell(line, cell):  # noqa: ANN001
        return magics.cypher(line, cell)

    # Reachable from a cell for anything the magics do not cover, and the
    # handle the tests drive.
    ipython.push({"samyama_magics": magics})
