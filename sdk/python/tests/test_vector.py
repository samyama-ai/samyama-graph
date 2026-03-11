"""Tests for vector search methods in the Samyama Python SDK."""

from samyama import SamyamaClient


def test_create_vector_index():
    client = SamyamaClient.embedded()
    client.create_vector_index("Doc", "embedding", 4, "cosine")


def test_add_and_search_vectors():
    client = SamyamaClient.embedded()
    client.create_vector_index("Doc", "embedding", 4, "cosine")
    client.query("CREATE (d:Doc {title: 'Alpha'})")
    client.query("CREATE (d:Doc {title: 'Beta'})")

    # Get node IDs
    result = client.query_readonly("MATCH (d:Doc) RETURN id(d) ORDER BY d.title")
    node_ids = [row[0] for row in result.records]

    client.add_vector("Doc", "embedding", node_ids[0], [1.0, 0.0, 0.0, 0.0])
    client.add_vector("Doc", "embedding", node_ids[1], [0.0, 1.0, 0.0, 0.0])

    results = client.vector_search("Doc", "embedding", [1.0, 0.1, 0.0, 0.0], 2)
    assert len(results) == 2
    assert results[0][0] == node_ids[0]  # Alpha should be closest


def test_vector_search_l2_metric():
    client = SamyamaClient.embedded()
    client.create_vector_index("Item", "vec", 3, "l2")
    client.query("CREATE (i:Item {name: 'A'})")
    result = client.query_readonly("MATCH (i:Item) RETURN id(i)")
    nid = result.records[0][0]
    client.add_vector("Item", "vec", nid, [1.0, 2.0, 3.0])
    results = client.vector_search("Item", "vec", [1.0, 2.0, 3.0], 1)
    assert len(results) == 1
    assert results[0][0] == nid
