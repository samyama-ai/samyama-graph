"""Snapshots from Python, in embedded mode (#2092, API-05, AI-18).

`SamyamaClient.open_snapshot`, `import_snapshot` and `export_snapshot` let a
notebook, an evaluation harness or an agent loop load and save a `.sgsnap`
file with the graph in-process: no server, no CLI, no socket.

Needs the compiled extension (`maturin develop`), like `test_embedded.py`.
"""

import os
import shutil
import subprocess

import pytest

import samyama

ROWS = "MATCH (p:Person) RETURN p.name AS name, p.age AS age, p.score AS score, " \
       "p.active AS active, p.tags AS tags ORDER BY name"
KNOWS = "MATCH (a:Person)-[k:KNOWS]->(b:Person) RETURN a.name AS a, b.name AS b, " \
        "k.since AS since ORDER BY a, b"


def build_graph():
    """Three people, two cities, every property type a snapshot carries."""
    c = samyama.SamyamaClient.embedded()
    c.query('CREATE (:Person {name: "Alice", age: 30, score: 1.5, active: true, tags: ["a", "b"]})')
    c.query('CREATE (:Person {name: "Bob", age: 25, score: -2.25, active: false, tags: []})')
    c.query('CREATE (:Person {name: "Chen", age: 41, score: 0.0, active: true, tags: ["c"]})')
    c.query('CREATE (:City {name: "Pune", code: "PNQ"})')
    c.query('CREATE (:City {name: "Oslo", code: "OSL"})')
    c.query('MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) CREATE (a)-[:KNOWS {since: 2019}]->(b)')
    c.query('MATCH (a:Person {name: "Bob"}), (b:Person {name: "Chen"}) CREATE (a)-[:KNOWS {since: 2021}]->(b)')
    c.query('MATCH (a:Person {name: "Alice"}), (b:City {name: "Pune"}) CREATE (a)-[:LIVES_IN]->(b)')
    return c


def rows(client, cypher):
    return client.query_readonly(cypher).records


def inet_sockets():
    """This process's open internet sockets, as `lsof` lines.

    `None` when `lsof` is not available, so the check skips rather than
    passing on a machine that cannot look.
    """
    if shutil.which("lsof") is None:
        return None
    out = subprocess.run(
        ["lsof", "-a", "-i", "-n", "-P", "-p", str(os.getpid())],
        capture_output=True, text=True,
    )
    return [line for line in out.stdout.splitlines()[1:] if line.strip()]


# --- the round trip -----------------------------------------------------------


def test_export_then_open_snapshot_gives_back_the_same_graph(tmp_path):
    path = str(tmp_path / "people.sgsnap")
    original = build_graph()

    written = original.export_snapshot(path)
    assert written["nodes_exported"] == 5
    assert written["edges_exported"] == 3
    assert written["bytes_written"] == os.path.getsize(path) > 0
    assert sorted(written["labels"]) == ["City", "Person"]
    assert sorted(written["edge_types"]) == ["KNOWS", "LIVES_IN"]
    assert isinstance(written["dropped"], list)
    assert written["duration_ms"] >= 0

    restored = samyama.SamyamaClient.open_snapshot(path)
    assert repr(restored) == "SamyamaClient(mode='embedded')"
    status = restored.status()
    assert (status.nodes, status.edges) == (5, 3)

    # Every property type, and the relationships with theirs, come back
    # exactly: same query, same answer.
    assert rows(restored, ROWS) == rows(original, ROWS)
    assert rows(restored, ROWS)[0] == ["Alice", 30, 1.5, True, ["a", "b"]]
    assert rows(restored, KNOWS) == [["Alice", "Bob", 2019], ["Bob", "Chen", 2021]]
    lives = "MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN p.name, c.code"
    assert rows(restored, lives) == [["Alice", "PNQ"]]


def test_import_snapshot_reports_what_it_imported(tmp_path):
    path = str(tmp_path / "people.sgsnap")
    build_graph().export_snapshot(path)

    client = samyama.SamyamaClient.embedded()
    report = client.import_snapshot(path)
    assert report["nodes_imported"] == 5
    assert report["edges_imported"] == 3
    assert report["nodes_merged"] == 0
    assert sorted(report["labels"]) == ["City", "Person"]
    assert sorted(report["edge_types"]) == ["KNOWS", "LIVES_IN"]
    assert report["duration_ms"] >= 0
    # The same keys as the server's POST .../snapshot/import response.
    for key in ("merges_refused_conflicting", "edges_deduplicated", "parallel_edges_kept",
                "merge_groups", "largest_merge_group", "merges_by_key",
                "property_conflicts_by_key", "property_conflict_samples",
                "dedup_keys_unused", "indexes_restored"):
        assert key in report, key
    assert rows(client, ROWS) == rows(build_graph(), ROWS)


def test_the_round_trip_opens_no_network_socket(tmp_path):
    before = inet_sockets()
    if before is None:
        pytest.skip("lsof is not available to list this process's sockets")
    path = str(tmp_path / "people.sgsnap")
    build_graph().export_snapshot(path)
    restored = samyama.SamyamaClient.open_snapshot(path)
    restored.import_snapshot(path)
    assert len(rows(restored, ROWS)) == 6
    assert inet_sockets() == before == [], "an embedded snapshot must not touch the network"


# --- importing into a graph that already holds something --------------------


def test_a_second_import_adds_to_the_graph(tmp_path):
    path = str(tmp_path / "people.sgsnap")
    build_graph().export_snapshot(path)
    client = samyama.SamyamaClient.open_snapshot(path)
    client.import_snapshot(path)
    status = client.status()
    assert (status.nodes, status.edges) == (10, 6), "without dedup_keys every node is added"


def test_dedup_keys_merge_nodes_that_share_a_key(tmp_path):
    path = str(tmp_path / "people.sgsnap")
    build_graph().export_snapshot(path)
    client = samyama.SamyamaClient.open_snapshot(path)

    report = client.import_snapshot(path, dedup_keys=["name"])
    assert report["nodes_merged"] == 5
    assert report["nodes_imported"] == 0
    assert report["merges_by_key"] == [{"key": "name", "merged": 5}]
    assert report["merge_groups"] == 5
    assert report["largest_merge_group"] == 2, "each node merged with its one copy"
    assert client.status().nodes == 5
    assert rows(client, ROWS) == rows(build_graph(), ROWS)


def test_open_snapshot_takes_dedup_keys_too(tmp_path):
    # Two Alices in one file: one node once merged on `name`.
    c = samyama.SamyamaClient.embedded()
    c.query('CREATE (:Person {name: "Alice", age: 30})')
    c.query('CREATE (:Person {name: "Alice", age: 30})')
    path = str(tmp_path / "twins.sgsnap")
    c.export_snapshot(path)

    assert samyama.SamyamaClient.open_snapshot(path).status().nodes == 2
    assert samyama.SamyamaClient.open_snapshot(path, dedup_keys=["name"]).status().nodes == 1


def test_a_dedup_key_that_merged_nothing_is_reported(tmp_path):
    path = str(tmp_path / "people.sgsnap")
    build_graph().export_snapshot(path)
    client = samyama.SamyamaClient.open_snapshot(path)
    report = client.import_snapshot(path, dedup_keys=["no_such_property"])
    assert report["nodes_merged"] == 0
    assert report["dedup_keys_unused"] == [
        {"key": "no_such_property", "incoming_nodes_with_key": 0}
    ]


# --- refusals -----------------------------------------------------------------


def test_a_missing_file_is_an_error_and_changes_nothing(tmp_path):
    client = build_graph()
    with pytest.raises(RuntimeError, match="import_snapshot"):
        client.import_snapshot(str(tmp_path / "nowhere.sgsnap"))
    with pytest.raises(RuntimeError):
        samyama.SamyamaClient.open_snapshot(str(tmp_path / "nowhere.sgsnap"))
    assert client.status().nodes == 5


def test_a_file_that_is_not_a_snapshot_is_an_error_and_changes_nothing(tmp_path):
    bad = tmp_path / "not-a-snapshot.sgsnap"
    bad.write_bytes(b"this is not a snapshot\n" * 100)
    client = build_graph()
    with pytest.raises(RuntimeError, match="import_snapshot"):
        client.import_snapshot(str(bad))
    assert (client.status().nodes, client.status().edges) == (5, 3)
    assert rows(client, ROWS) == rows(build_graph(), ROWS)


def test_another_graph_name_is_refused_not_ignored(tmp_path):
    path = tmp_path / "people.sgsnap"
    client = build_graph()
    with pytest.raises(ValueError, match='single graph, "default"'):
        client.export_snapshot(str(path), graph="other")
    assert not path.exists(), "a refused export writes nothing"
    client.export_snapshot(str(path))
    with pytest.raises(ValueError, match='single graph, "default"'):
        client.import_snapshot(str(path), graph="other")
    assert client.status().nodes == 5


def test_allow_conflicts_without_dedup_keys_is_refused(tmp_path):
    path = str(tmp_path / "people.sgsnap")
    build_graph().export_snapshot(path)
    client = samyama.SamyamaClient.embedded()
    with pytest.raises(ValueError, match="allow_conflicts only applies with dedup_keys"):
        client.import_snapshot(path, allow_conflicts=True)
    assert client.status().nodes == 0


def test_a_remote_client_is_told_where_to_go():
    # Refused before any request: nothing listens at this address.
    remote = samyama.SamyamaClient.connect("http://127.0.0.1:9")
    for call in (lambda: remote.import_snapshot("x.sgsnap"),
                 lambda: remote.export_snapshot("x.sgsnap")):
        with pytest.raises(RuntimeError, match="only available in embedded mode"):
            call()


def test_export_replaces_an_existing_file(tmp_path):
    path = tmp_path / "people.sgsnap"
    path.write_bytes(b"old contents")
    build_graph().export_snapshot(str(path))
    assert samyama.SamyamaClient.open_snapshot(str(path)).status().nodes == 5


# --- asyncio ------------------------------------------------------------------


def test_the_async_client_has_the_same_snapshot_methods(tmp_path):
    import asyncio
    from samyama_aio import AsyncSamyamaClient

    path = str(tmp_path / "people.sgsnap")

    async def main():
        src = AsyncSamyamaClient(build_graph())
        written = await src.export_snapshot(path)
        opened = await AsyncSamyamaClient.open_snapshot(path, dedup_keys=None)
        again = await opened.import_snapshot(path, dedup_keys=["name"])
        return written, (await opened.status()).nodes, again

    written, nodes, again = asyncio.run(main())
    assert written["nodes_exported"] == 5
    assert nodes == 5
    assert again["nodes_merged"] == 5
