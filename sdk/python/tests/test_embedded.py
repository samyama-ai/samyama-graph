"""Tests for the Samyama Python SDK (embedded mode)."""

import samyama


def test_embedded_create():
    """Test creating an embedded client."""
    client = samyama.SamyamaClient.embedded()
    assert repr(client) == "SamyamaClient(mode='embedded')"


def test_ping():
    """Test ping."""
    client = samyama.SamyamaClient.embedded()
    assert client.ping() == "PONG"


def test_status():
    """Test status."""
    client = samyama.SamyamaClient.embedded()
    status = client.status()
    assert status.status == "healthy"
    assert status.nodes == 0
    assert status.edges == 0


def test_create_and_query():
    """Test creating nodes and querying them."""
    client = samyama.SamyamaClient.embedded()

    # Create nodes
    client.query('CREATE (n:Person {name: "Alice", age: 30})')
    client.query('CREATE (n:Person {name: "Bob", age: 25})')

    # Query
    result = client.query_readonly("MATCH (n:Person) RETURN n.name, n.age")
    assert len(result) == 2
    assert result.columns == ["n.name", "n.age"]

    # Status should reflect 2 nodes
    status = client.status()
    assert status.nodes == 2


def test_list_graphs():
    """Test listing graphs."""
    client = samyama.SamyamaClient.embedded()
    graphs = client.list_graphs()
    assert graphs == ["default"]


def test_delete_graph():
    """Test deleting a graph."""
    client = samyama.SamyamaClient.embedded()
    client.query('CREATE (n:Person {name: "Alice"})')
    assert client.status().nodes == 1

    client.delete_graph()
    assert client.status().nodes == 0


def _ages(client):
    result = client.query_readonly("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
    return [tuple(row) for row in result.records]


def test_checkpoint_diff_and_rollback():
    """A checkpoint lists what changed since it and undoes it (#2093)."""
    client = samyama.SamyamaClient.embedded()
    client.query('CREATE (n:Person {name: "Alice", age: 30})')

    v = client.checkpoint()
    client.query('MATCH (n:Person {name: "Alice"}) SET n.age = 31')
    client.query('CREATE (n:Person {name: "Bob", age: 25})')

    diff = client.diff_since(v)
    assert diff["from_version"] == v
    assert len(diff["nodes_added"]) == 1
    assert diff["nodes_deleted"] == []
    [change] = diff["property_changes"]
    assert (change["kind"], change["key"], change["old"], change["new"]) == ("node", "age", 30, 31)

    stats = client.rollback_to(v)
    assert stats["nodes_removed"] == 1
    assert stats["properties_reverted"] == 1
    assert stats["failures"] == []
    assert _ages(client) == [("Alice", 30)]

    # Rolled back to, so released.
    assert client.release_checkpoint(v) is False


def test_branch_rolls_back_unless_committed():
    """`with client.branch()` undoes the block unless it commits (#2093)."""
    client = samyama.SamyamaClient.embedded()
    client.query('CREATE (n:Person {name: "Alice", age: 30})')

    with client.branch() as b:
        client.query('MATCH (n:Person) SET n.age = 99')
        assert len(b.diff()["property_changes"]) == 1
    assert not b.open
    assert _ages(client) == [("Alice", 30)]

    with client.branch() as b:
        client.query('MATCH (n:Person) SET n.age = 31')
        b.commit()
    assert _ages(client) == [("Alice", 31)]

    # An exception still rolls back, and is not swallowed.
    try:
        with client.branch():
            client.query('CREATE (n:Person {name: "Bob", age: 25})')
            raise KeyError("boom")
    except KeyError:
        pass
    else:
        raise AssertionError("the exception was suppressed")
    assert _ages(client) == [("Alice", 31)]


def test_checkpoint_refused_on_remote():
    """Checkpoints are embedded-only; a remote client refuses before sending."""
    client = samyama.SamyamaClient.connect("http://127.0.0.1:1")
    try:
        client.checkpoint()
    except RuntimeError as e:
        assert "embedded" in str(e)
    else:
        raise AssertionError("a remote client took a checkpoint")
