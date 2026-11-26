"""Tests for samyama_mcp.generators — tool generation."""

import json
import pytest
from unittest.mock import MagicMock

from samyama_mcp.schema import (
    EdgeType,
    GraphSchema,
    NodeType,
    PropertyInfo,
    VectorIndex,
)
from samyama_mcp.generators.generic_tools import GenericToolGenerator
from samyama_mcp.generators.node_tools import NodeToolGenerator
from samyama_mcp.generators.edge_tools import EdgeToolGenerator
from samyama_mcp.generators.algorithm_tools import AlgorithmToolGenerator
from samyama_mcp.generators.vector_tools import VectorToolGenerator


# ── Helpers ───────────────────────────────────────────────────────────


class FakeResult:
    def __init__(self, columns, records):
        self.columns = columns
        self.records = records


class FakeMCP:
    """Minimal FastMCP stand-in that tracks registered tools."""

    def __init__(self):
        self.tools: dict[str, callable] = {}

    def tool(self):
        def decorator(fn):
            self.tools[fn.__name__] = fn
            return fn
        return decorator


def _make_schema():
    return GraphSchema(
        node_types=[
            NodeType(
                label="Person",
                count=100,
                properties=[
                    PropertyInfo(name="name", type="String", indexed=True, samples=["Alice"]),
                    PropertyInfo(name="age", type="Integer", indexed=False, samples=[30]),
                    PropertyInfo(name="email", type="String", indexed=True, samples=["a@b.com"]),
                ],
            ),
            NodeType(
                label="Company",
                count=20,
                properties=[
                    PropertyInfo(name="name", type="String", indexed=True, samples=["TechCorp"]),
                    PropertyInfo(name="industry", type="String", indexed=False, samples=["Tech"]),
                ],
            ),
        ],
        edge_types=[
            EdgeType(
                type="KNOWS",
                count=500,
                source_labels=["Person"],
                target_labels=["Person"],
            ),
            EdgeType(
                type="WORKS_AT",
                count=100,
                source_labels=["Person"],
                target_labels=["Company"],
            ),
        ],
        indexes=[("Person", "name"), ("Person", "email"), ("Company", "name")],
        total_nodes=120,
        total_edges=600,
    )


def _make_client():
    client = MagicMock()
    client.query_readonly = MagicMock(
        return_value=FakeResult(["_id", "name", "age"], [[1, "Alice", 30]])
    )
    return client


@pytest.fixture
def schema():
    return _make_schema()


@pytest.fixture
def client():
    return _make_client()


@pytest.fixture
def mcp():
    return FakeMCP()


# ── GenericToolGenerator ─────────────────────────────────────────────


class TestGenericTools:
    def test_registers_two_tools(self, client, schema, mcp):
        gen = GenericToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        assert names == ["cypher_query", "schema_info"]
        assert "cypher_query" in mcp.tools
        assert "schema_info" in mcp.tools

    def test_cypher_query_readonly(self, client, schema, mcp):
        gen = GenericToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["cypher_query"]("MATCH (n) RETURN n")
        assert isinstance(result, str)
        client.query_readonly.assert_called()

    def test_cypher_query_rejects_write(self, client, schema, mcp):
        gen = GenericToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["cypher_query"]("CREATE (n:Person)")
        data = json.loads(result)
        assert "error" in data

    def test_schema_info_returns_json(self, client, schema, mcp):
        gen = GenericToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["schema_info"]()
        data = json.loads(result)
        assert data["total_nodes"] == 120
        assert len(data["node_types"]) == 2


# ── NodeToolGenerator ────────────────────────────────────────────────


class TestNodeTools:
    def test_registers_expected_tools(self, client, schema, mcp):
        gen = NodeToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        # Person: search, get_by_name, get_by_email, count = 4
        # Company: search, get_by_name, count = 3
        assert "search_person" in names
        assert "get_person_by_name" in names
        assert "get_person_by_email" in names
        assert "count_person" in names
        assert "search_company" in names
        assert "get_company_by_name" in names
        assert "count_company" in names
        assert len(names) == 7

    def test_search_builds_correct_cypher(self, client, schema, mcp):
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["search_person"]("Alice")
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "Person" in cypher_arg
        assert "CONTAINS" in cypher_arg
        assert "Alice" in cypher_arg

    def test_search_escapes_input(self, client, schema, mcp):
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["search_person"]('test"injection')
        cypher_arg = client.query_readonly.call_args[0][0]
        # The double-quote must be escaped with backslash
        assert 'test\\"injection' in cypher_arg

    def test_get_by_returns_single(self, client, schema, mcp):
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["get_person_by_name"]("Alice")
        data = json.loads(result)
        assert data["name"] == "Alice"

    def test_get_by_not_found(self, client, schema, mcp):
        client.query_readonly.return_value = FakeResult(["_id", "name"], [])
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["get_person_by_name"]("Nobody")
        data = json.loads(result)
        assert "error" in data

    def test_count_no_filter(self, client, schema, mcp):
        client.query_readonly.return_value = FakeResult(["count"], [[100]])
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["count_person"]()
        data = json.loads(result)
        assert data["count"] == 100

    def test_count_with_filter(self, client, schema, mcp):
        client.query_readonly.return_value = FakeResult(["count"], [[5]])
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["count_person"]("age", "30")
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "age" in cypher_arg
        assert "30" in cypher_arg

    def test_count_rejects_bad_property(self, client, schema, mcp):
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["count_person"]("bad-prop", "val")
        data = json.loads(result)
        assert "error" in data

    def test_limit_capped_at_100(self, client, schema, mcp):
        gen = NodeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["search_person"]("x", limit=999)
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "LIMIT 100" in cypher_arg


# ── EdgeToolGenerator ────────────────────────────────────────────────


class TestEdgeTools:
    def test_registers_expected_tools(self, client, schema, mcp):
        gen = EdgeToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        # 2 edge types × 2 tools each = 4
        assert "find_knows_connections" in names
        assert "traverse_knows" in names
        assert "find_works_at_connections" in names
        assert "traverse_works_at" in names
        assert len(names) == 4

    def test_find_connections_outgoing(self, client, schema, mcp):
        gen = EdgeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["find_knows_connections"](
            "Person", "name", "Alice", direction="outgoing"
        )
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "Person" in cypher_arg
        assert "KNOWS" in cypher_arg
        assert "->" in cypher_arg

    def test_find_connections_incoming(self, client, schema, mcp):
        gen = EdgeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["find_knows_connections"](
            "Person", "name", "Alice", direction="incoming"
        )
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "->" in cypher_arg  # still has direction

    def test_find_connections_validates_label(self, client, schema, mcp):
        gen = EdgeToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["find_knows_connections"](
            "bad-label", "name", "Alice"
        )
        data = json.loads(result)
        assert "error" in data

    def test_traverse_builds_var_length(self, client, schema, mcp):
        gen = EdgeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["traverse_knows"]("Person", "name", "Alice", max_hops=2)
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "*1..2" in cypher_arg

    def test_traverse_caps_hops_at_5(self, client, schema, mcp):
        gen = EdgeToolGenerator(client, "default", schema)
        gen.register(mcp)
        mcp.tools["traverse_knows"]("Person", "name", "Alice", max_hops=99)
        cypher_arg = client.query_readonly.call_args[0][0]
        assert "*1..5" in cypher_arg


# ── AlgorithmToolGenerator ───────────────────────────────────────────


class TestAlgorithmTools:
    def test_registers_when_embedded(self, schema, mcp):
        client = MagicMock()
        client.page_rank = MagicMock(return_value={1: 0.5, 2: 0.3})
        client.bfs = MagicMock(return_value={"path": [1, 2], "cost": 1})
        client.wcc = MagicMock(
            return_value={"components": {"0": [1, 2, 3]}, "component_count": 1}
        )
        client.query_readonly = MagicMock(
            return_value=FakeResult(["_id", "labels"], [[1, ["Person"]]])
        )
        gen = AlgorithmToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        assert "pagerank" in names
        assert "shortest_path" in names
        assert "communities" in names

    def test_skipped_for_remote(self, schema, mcp):
        client = MagicMock(spec=["query", "query_readonly"])
        # No page_rank attribute → not embedded
        gen = AlgorithmToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        assert names == []

    def test_pagerank_calls_client(self, schema, mcp):
        client = MagicMock()
        client.page_rank = MagicMock(return_value={1: 0.5, 2: 0.3})
        client.query_readonly = MagicMock(
            return_value=FakeResult(["_id", "labels"], [[1, ["Person"]]])
        )
        gen = AlgorithmToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["pagerank"]()
        data = json.loads(result)
        assert len(data) == 2
        client.page_rank.assert_called_once()

    def test_shortest_path_no_path(self, schema, mcp):
        client = MagicMock()
        client.page_rank = MagicMock()
        client.bfs = MagicMock(return_value=None)
        client.wcc = MagicMock()
        client.query_readonly = MagicMock()
        gen = AlgorithmToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["shortest_path"](1, 99)
        data = json.loads(result)
        assert "error" in data

    def test_communities_summary(self, schema, mcp):
        client = MagicMock()
        client.page_rank = MagicMock()
        client.bfs = MagicMock()
        client.wcc = MagicMock(
            return_value={
                "components": {"0": [1, 2, 3], "1": [4, 5]},
                "component_count": 2,
            }
        )
        client.query_readonly = MagicMock()
        gen = AlgorithmToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["communities"]()
        data = json.loads(result)
        assert data["component_count"] == 2
        assert data["largest_component"] == 3


# ── VectorToolGenerator ──────────────────────────────────────────────


class TestVectorTools:
    def test_skipped_without_vector_indexes(self, client, schema, mcp):
        gen = VectorToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        assert names == []

    def test_registers_for_vector_indexes(self, client, mcp):
        schema = GraphSchema(
            node_types=[
                NodeType(label="Doc", count=10, properties=[]),
            ],
            vector_indexes=[VectorIndex(label="Doc", property="embedding")],
        )
        client.vector_search = MagicMock(return_value=[(1, 0.1), (2, 0.3)])
        client.query_readonly = MagicMock(
            return_value=FakeResult(["_id", "labels"], [[1, ["Doc"]]])
        )
        gen = VectorToolGenerator(client, "default", schema)
        names = gen.register(mcp)
        assert "find_similar_doc" in names

    def test_find_similar_calls_vector_search(self, client, mcp):
        schema = GraphSchema(
            node_types=[NodeType(label="Doc", count=10)],
            vector_indexes=[VectorIndex(label="Doc", property="embedding")],
        )
        client.vector_search = MagicMock(return_value=[(1, 0.1)])
        client.query_readonly = MagicMock(
            return_value=FakeResult(["_id", "labels"], [[1, ["Doc"]]])
        )
        gen = VectorToolGenerator(client, "default", schema)
        gen.register(mcp)
        result = mcp.tools["find_similar_doc"]([0.1, 0.2, 0.3], k=5)
        data = json.loads(result)
        assert len(data) == 1
        assert data[0]["similarity"] == 0.9
        client.vector_search.assert_called_once_with("Doc", "embedding", [0.1, 0.2, 0.3], 5)


# ── retrieve_evidence (#2098) ─────────────────────────────────────────

# Rows as `ai.rag.retrieve` returns them for the tool's RETURN: a seed with a
# source, and a node one hop out reached against its edge's direction.
_RAG_COLUMNS = ["node_id", "labels", "seed_id", "score", "hops", "source", "hop_sequence"]
_RAG_ROWS = [
    [1, ["Chunk"], 1, 0.97, 0, {"__source_uri": "https://example.org/a.pdf", "__chunk_id": "a#3"}, []],
    [2, ["Entity"], 1, 0.97, 1, None, [[2, "MENTIONS", 1]]],
]


def _rag_schema(label="Chunk", prop="emb"):
    return GraphSchema(
        node_types=[NodeType(label=label, count=3)],
        vector_indexes=[VectorIndex(label=label, property=prop)],
    )


class TestRetrieveEvidence:
    def _tool(self, client, mcp, schema=None):
        client.query_readonly = MagicMock(return_value=FakeResult(_RAG_COLUMNS, _RAG_ROWS))
        VectorToolGenerator(client, "default", schema or _rag_schema()).register(mcp)
        return mcp.tools["retrieve_evidence_chunk"]

    def test_registers_without_an_embedded_client(self, mcp):
        # One Cypher call, so a remote client gets it too; find_similar needs
        # `vector_search`, which only the embedded client has.
        client = MagicMock(spec=["query", "query_readonly"])
        names = VectorToolGenerator(client, "default", _rag_schema()).register(mcp)
        assert names == ["retrieve_evidence_chunk"]

    def test_registers_beside_find_similar(self, client, mcp):
        client.vector_search = MagicMock()
        names = VectorToolGenerator(client, "default", _rag_schema()).register(mcp)
        assert names == ["find_similar_chunk", "retrieve_evidence_chunk"]

    def test_calls_the_procedure(self, client, mcp):
        self._tool(client, mcp)([1.0, 0.0], k=4, hops=2)
        cypher, graph = client.query_readonly.call_args[0]
        assert graph == "default"
        assert cypher.startswith("CALL ai.rag.retrieve('Chunk', 'emb', [1.0, 0.0], 4, 2) YIELD ")

    def test_returns_the_envelope(self, client, mcp):
        data = json.loads(self._tool(client, mcp)([1.0, 0.0]))
        assert data["sources"] == [
            {"node_id": 1, "labels": ["Chunk"],
             "provenance": {"source_uri": "https://example.org/a.pdf", "chunk_id": "a#3"}},
            {"node_id": 2, "labels": ["Entity"], "provenance": None},
        ]
        assert data["lineage_paths"] == [
            {"path_id": 0, "node_id": 1, "seed_node_id": 1, "seed_vector_score": 0.97,
             "hops": 0, "hop_sequence": []},
            {"path_id": 1, "node_id": 2, "seed_node_id": 1, "seed_vector_score": 0.97,
             "hops": 1, "hop_sequence": [{"src": 2, "edge_type": "MENTIONS", "dst": 1}]},
        ]

    @pytest.mark.parametrize("vector,k,hops,error", [
        ([], 5, 1, "query_vector"),
        ([float("nan"), 1.0], 5, 1, "query_vector"),
        ([1.0, 0.0], 5, 4, "hops"),
        ([1.0, 0.0], 5, -1, "hops"),
    ])
    def test_refuses_bad_arguments_without_a_query(self, client, mcp, vector, k, hops, error):
        data = json.loads(self._tool(client, mcp)(vector, k=k, hops=hops))
        assert error in data["error"]
        client.query_readonly.assert_not_called()

    def test_k_is_clamped(self, client, mcp):
        tool = self._tool(client, mcp)
        tool([1.0], k=1000)
        assert ", 100, 1)" in client.query_readonly.call_args[0][0]
        tool([1.0], k=0)
        assert ", 1, 1)" in client.query_readonly.call_args[0][0]

    def test_engine_error_is_returned(self, client, mcp):
        tool = self._tool(client, mcp)
        client.query_readonly.side_effect = RuntimeError("no vector index on :Chunk(emb)")
        assert json.loads(tool([1.0]))["error"] == "no vector index on :Chunk(emb)"

    def test_no_tool_for_a_name_that_is_not_an_identifier(self, client, mcp):
        names = VectorToolGenerator(client, "default", _rag_schema(prop="e') RETURN 1 //")).register(mcp)
        assert not any(n.startswith("retrieve_evidence") for n in names)
