"""The `%%cypher` magic returns a DataFrame, and does not write by accident.

INT-09 asks for "Jupyter magic + a pandas/polars-native result type". Both
halves are asserted here against a real IPython shell rather than by calling
the functions directly: `%load_ext samyama_nb` registering the magics is the
part that breaks, and calling `SamyamaMagics.cypher()` in a test would pass
while `%%cypher` in a notebook raised `UsageError: Cell magic not found`.

The case worth reading is `test_a_cell_does_not_write_unless_it_says_so`. A
notebook is re-run top to bottom, often by somebody who did not write it, and a
cell that silently writes turns a re-execution into a mutation — twice the
nodes, no error, and nothing in the cell to suggest it.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

IPython = pytest.importorskip("IPython", reason="the magic needs IPython")
pd = pytest.importorskip("pandas", reason="the DataFrame half of INT-09 needs pandas")

from samyama_nb import rows, to_dataframe  # noqa: E402


@pytest.fixture()
def shell():
    """A real IPython shell with the extension loaded."""
    from IPython.core.interactiveshell import InteractiveShell

    sh = InteractiveShell.instance()
    sh.run_line_magic("load_ext", "samyama_nb")
    sh.run_line_magic("cypher_connect", "--embedded")
    yield sh
    InteractiveShell.clear_instance()


def test_the_extension_registers_both_magics(shell):
    # The half that a direct call to `SamyamaMagics.cypher` would not check.
    assert "cypher" in shell.magics_manager.magics["line"]
    assert "cypher" in shell.magics_manager.magics["cell"]
    assert "cypher_connect" in shell.magics_manager.magics["line"]


def test_a_cell_returns_a_dataframe_with_the_query_s_columns(shell):
    shell.run_cell_magic(
        "cypher", "--write", "CREATE (:Judge {name: 'A'}), (:Judge {name: 'B'})"
    )
    df = shell.run_cell_magic(
        "cypher", "", "MATCH (j:Judge) RETURN j.name AS judge ORDER BY judge"
    )
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["judge"]
    assert list(df["judge"]) == ["A", "B"]


def test_a_line_magic_returns_the_same_thing(shell):
    shell.run_cell_magic("cypher", "--write", "CREATE (:N {v: 1})")
    df = shell.run_line_magic("cypher", "MATCH (n:N) RETURN n.v AS v")
    assert isinstance(df, pd.DataFrame)
    assert list(df["v"]) == [1]


def test_a_cell_does_not_write_unless_it_says_so(shell):
    # The case this design exists for.
    with pytest.raises(Exception) as exc:
        shell.run_cell_magic("cypher", "", "CREATE (:Sneaky {v: 1})")
    assert "read" in str(exc.value).lower() or "write" in str(exc.value).lower(), (
        f"the refusal should say why, got: {exc.value}"
    )

    # And nothing was written.
    df = shell.run_cell_magic("cypher", "", "MATCH (n:Sneaky) RETURN count(n) AS c")
    assert list(df["c"]) == [0]


def test_an_empty_result_keeps_its_columns(shell):
    # A zero-row frame with no columns is indistinguishable from a failure when
    # you are looking at it in a notebook.
    df = shell.run_cell_magic(
        "cypher", "", "MATCH (n:NothingHere) RETURN n.a AS a, n.b AS b"
    )
    assert list(df.columns) == ["a", "b"]
    assert len(df) == 0


def test_running_before_connecting_says_so(shell):
    from samyama_nb import SamyamaMagics

    m = SamyamaMagics()
    with pytest.raises(RuntimeError) as exc:
        m.cypher("MATCH (n) RETURN n")
    assert "cypher_connect" in str(exc.value), "the error should name the fix"


def test_an_empty_query_is_refused(shell):
    with pytest.raises(ValueError):
        shell.run_line_magic("cypher", "")


def test_rows_pairs_each_record_with_the_column_names():
    # `to_dataframe` builds on this, and it is worth pinning on its own because
    # it is the fallback when pandas is absent.
    class Result:
        columns = ["a", "b"]
        records = [[1, 2], [3, 4]]

    assert rows(Result()) == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]


def test_to_dataframe_preserves_column_order():
    class Result:
        columns = ["z", "a", "m"]
        records = [[1, 2, 3]]

    df = to_dataframe(Result())
    assert list(df.columns) == ["z", "a", "m"], (
        "a frame ordered by anything other than the query's RETURN clause makes "
        "the notebook disagree with the query above it"
    )
