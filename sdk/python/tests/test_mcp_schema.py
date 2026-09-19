"""Tests for samyama_mcp.schema — schema discovery."""

import pytest
from unittest.mock import MagicMock

from samyama_mcp.schema import (
    CypherSchemaDiscovery,
    GraphSchema,
    NodeType,
    PropertyInfo,
    _infer_type,
)


# ── Helpers ───────────────────────────────────────────────────────────


class FakeResult:
    """Lightweight stand-in for QueryResult."""

    def __init__(self, columns, records):
        self.columns = columns
        self.records = records


def _make_mock_client():
    """Build a mock client whose query_readonly returns canned results."""
    client = MagicMock()

    def query_readonly(cypher, graph="default"):
        # Node labels
        if "DISTINCT labels(n)" in cypher:
            return FakeResult(
                ["label", "cnt"],
                [[["Person"], 100], [["Company"], 20]],
            )
        # Property keys
        if "keys(n)" in cypher and "Person" in cypher:
            return FakeResult(["keys"], [[["name", "age", "email"]]])
        if "keys(n)" in cypher and "Company" in cypher:
            return FakeResult(["keys"], [[["name", "industry"]]])
        # Property samples
        if "n.name" in cypher and "Person" in cypher:
            return FakeResult(["val"], [["Alice"], ["Bob"], ["Carol"]])
        if "n.age" in cypher:
            return FakeResult(["val"], [[30], [25], [35]])
        if "n.email" in cypher:
            return FakeResult(["val"], [["a@example.com"], ["b@example.com"]])
        if "n.name" in cypher and "Company" in cypher:
            return FakeResult(["val"], [["TechCorp"], ["DataInc"]])
        if "n.industry" in cypher:
            return FakeResult(["val"], [["Technology"], ["Data"]])
        # Edge types
        if "DISTINCT type(r)" in cypher or "type(r)" in cypher:
            return FakeResult(
                ["type", "source", "target", "cnt"],
                [
                    ["KNOWS", ["Person"], ["Person"], 500],
                    ["WORKS_AT", ["Person"], ["Company"], 100],
                ],
            )
        # Indexes
        if "SHOW INDEXES" in cypher:
            return FakeResult(
                ["label", "property"],
                [["Person", "name"], ["Person", "email"]],
            )
        # Constraints
        if "SHOW CONSTRAINTS" in cypher:
            return FakeResult(["label", "property", "type"], [])
        return FakeResult([], [])

    client.query_readonly = MagicMock(side_effect=query_readonly)
    return client


@pytest.fixture
def mock_client():
    return _make_mock_client()


# ── CypherSchemaDiscovery ────────────────────────────────────────────


class TestDiscoverNodeTypes:
    def test_discovers_two_labels(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert len(schema.node_types) == 2

    def test_person_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        assert person.count == 100

    def test_person_properties(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        prop_names = [p.name for p in person.properties]
        assert "name" in prop_names
        assert "age" in prop_names
        assert "email" in prop_names

    def test_company_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        company = next(nt for nt in schema.node_types if nt.label == "Company")
        assert company.count == 20


class TestDiscoverEdgeTypes:
    def test_discovers_two_edge_types(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert len(schema.edge_types) == 2

    def test_knows_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        knows = next(et for et in schema.edge_types if et.type == "KNOWS")
        assert knows.count == 500

    def test_knows_endpoints(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        knows = next(et for et in schema.edge_types if et.type == "KNOWS")
        assert "Person" in knows.source_labels
        assert "Person" in knows.target_labels

    def test_works_at_endpoints(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        wa = next(et for et in schema.edge_types if et.type == "WORKS_AT")
        assert "Person" in wa.source_labels
        assert "Company" in wa.target_labels


class TestDiscoverIndexes:
    def test_index_count(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert len(schema.indexes) == 2

    def test_index_entries(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert ("Person", "name") in schema.indexes
        assert ("Person", "email") in schema.indexes


class TestIndexedPropertyMarking:
    def test_name_is_indexed(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        name_prop = next(p for p in person.properties if p.name == "name")
        assert name_prop.indexed is True

    def test_age_not_indexed(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        person = next(nt for nt in schema.node_types if nt.label == "Person")
        age_prop = next(p for p in person.properties if p.name == "age")
        assert age_prop.indexed is False


class TestTotals:
    def test_total_nodes(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert schema.total_nodes == 120

    def test_total_edges(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        assert schema.total_edges == 600


# ── Type inference ───────────────────────────────────────────────────


class TestInferType:
    def test_string(self):
        assert _infer_type(["hello"]) == "String"

    def test_integer(self):
        assert _infer_type([42]) == "Integer"

    def test_float(self):
        assert _infer_type([3.14]) == "Float"

    def test_boolean(self):
        assert _infer_type([True]) == "Boolean"

    def test_list(self):
        assert _infer_type([[1, 2, 3]]) == "Array"

    def test_dict(self):
        assert _infer_type([{"a": 1}]) == "Map"

    def test_empty(self):
        assert _infer_type([]) == "Unknown"


class TestSchemaToDict:
    def test_roundtrip(self, mock_client):
        schema = CypherSchemaDiscovery(mock_client).discover()
        d = schema.to_dict()
        assert d["total_nodes"] == 120
        assert d["total_edges"] == 600
        assert len(d["node_types"]) == 2
        assert len(d["edge_types"]) == 2
        assert len(d["indexes"]) == 2


# ── The schema is the whole schema, not one node's worth of it ─────────
#
# Each test here corresponds to a way discovery used to under-report, and each
# one fails against the previous implementation. What they have in common is
# that the loss was silent: the caller -- an LLM planning a query, or the tool
# generator -- got a schema that looked complete and was not.


class _Canned:
    """A client answering a fixed map of substring -> records."""

    def __init__(self, answers):
        self.answers = answers
        self.asked: list[str] = []

    def query_readonly(self, cypher, graph="default"):
        self.asked.append(cypher)
        for needle, records in self.answers.items():
            if needle in cypher:
                return FakeResult(["c"], records)
        return FakeResult(["c"], [])


class TestEveryLabelIsANodeType:
    """`:Person:Employee` is two node types, not one."""

    def _schema(self):
        client = _Canned({
            "DISTINCT labels(n)": [[["Person", "Employee"], 10], [["Company"], 4]],
            "keys(n)": [[["name"]]],
            "type(r)": [],
        })
        return CypherSchemaDiscovery(client).discover()

    def test_the_second_label_is_not_dropped(self):
        labels = {nt.label for nt in self._schema().node_types}
        assert labels == {"Person", "Employee", "Company"}, (
            "a node labelled :Person:Employee was reported as Person only, so "
            "no tool was generated for Employee and a model was told the label "
            f"does not exist; got {labels}"
        )

    def test_a_node_counts_towards_each_of_its_labels(self):
        by_label = {nt.label: nt.count for nt in self._schema().node_types}
        assert by_label["Person"] == 10
        assert by_label["Employee"] == 10

    def test_total_nodes_does_not_double_count(self):
        # 10 nodes with two labels plus 4 with one is 14 nodes, not 24. Summing
        # `NodeType.count` would report more nodes than the graph has.
        assert self._schema().total_nodes == 14


class TestEveryEndpointLabelIsRecorded:
    def test_edge_endpoints_keep_the_whole_label_set(self):
        client = _Canned({
            "DISTINCT labels(n)": [[["Person", "Employee"], 10]],
            "keys(n)": [[["name"]]],
            "type(r)": [["WORKS_AT", ["Person", "Employee"], ["Company"], 7]],
        })
        schema = CypherSchemaDiscovery(client).discover()
        et = schema.edge_types[0]
        assert set(et.source_labels) == {"Person", "Employee"}, (
            f"WORKS_AT starts at a :Person:Employee and the schema records only "
            f"{et.source_labels}"
        )


class TestPropertiesComeFromMoreThanOneNode:
    """Two nodes of a label need not carry the same keys."""

    def _client(self):
        return _Canned({
            "DISTINCT labels(n)": [[["Person"], 3]],
            # Three nodes: the first has no email, the third has no age.
            "keys(n)": [[["name"]], [["name", "email"]], [["name", "age"]]],
            "type(r)": [],
            "n.": [["v"]],
        })

    def test_a_key_the_first_node_lacks_is_still_in_the_schema(self):
        schema = CypherSchemaDiscovery(self._client()).discover()
        props = {p.name for p in schema.node_types[0].properties}
        assert props == {"name", "email", "age"}, (
            "discovery read one node's keys, so a property that node did not "
            f"set was missing from the schema entirely; got {props}"
        )

    def test_the_sample_size_is_reported(self):
        # The answer is still a sample, and the caller is told how large it was
        # rather than left to assume it was exhaustive.
        nt = CypherSchemaDiscovery(self._client()).discover().node_types[0]
        assert nt.properties_sampled_from == 3

    def test_the_sample_size_survives_serialization(self):
        d = CypherSchemaDiscovery(self._client()).discover().to_dict()
        assert d["node_types"][0]["properties_sampled_from"] == 3
