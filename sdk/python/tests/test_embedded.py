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
