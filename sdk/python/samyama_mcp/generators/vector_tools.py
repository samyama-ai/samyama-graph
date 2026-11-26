"""Vector tools: similarity search and evidence retrieval for labels with
vector indexes."""

from __future__ import annotations

import math

from samyama_mcp.escape import validate_identifier
from samyama_mcp.generators.base import ToolGenerator, to_dicts

#: Provenance keys ``ai.rag.retrieve`` reports, and the names the envelope
#: gives them (the reserved ``__`` prefix dropped).
_SOURCE_KEYS = (
    "__source_uri", "__source_version", "__retrieved_at", "__license",
    "__redistributable", "__chunk_id", "__span", "__extractor_version",
)
#: The engine's bound on ``hops`` (``rag_ops::MAX_HOPS``).
MAX_HOPS = 3


def envelope(columns: list[str], records: list[list]) -> dict:
    """The evidence envelope (#2098) from ``ai.rag.retrieve`` rows.

    ``sources``: one entry per retrieved node, ``provenance`` ``None`` when the
    node records none -- unknown, not "no source needed".
    ``lineage_paths``: for each node, the seed it was reached from, that
    seed's score, and every hop as ``{src, edge_type, dst}`` in the edge's own
    direction, which may be against the walk's.
    Row order is the engine's, which is deterministic.
    """
    sources, paths = [], []
    for i, row in enumerate(records):
        r = dict(zip(columns, row))
        prov = r.get("source")
        sources.append({
            "node_id": r["node_id"],
            "labels": r["labels"],
            "provenance": (
                {k[2:]: prov[k] for k in _SOURCE_KEYS if k in prov}
                if prov else None
            ),
        })
        paths.append({
            "path_id": i,
            "node_id": r["node_id"],
            "seed_node_id": r["seed_id"],
            "seed_vector_score": r["score"],
            "hops": r["hops"],
            "hop_sequence": [
                {"src": src, "edge_type": etype, "dst": dst}
                for src, etype, dst in r["hop_sequence"]
            ],
        })
    return {"sources": sources, "lineage_paths": paths}


class VectorToolGenerator(ToolGenerator):
    """Generates, per vector index, ``find_similar_{label}`` (embedded clients
    only) and ``retrieve_evidence_{label}`` (any client: it is one Cypher
    call)."""

    def register(self, mcp) -> list[str]:
        if not self.schema.vector_indexes:
            return []

        names: list[str] = []
        for vi in self.schema.vector_indexes:
            makers = [self._make_retrieve_evidence]
            if hasattr(self.client, "vector_search"):
                makers.insert(0, self._make_find_similar)
            for make in makers:
                fn = make(vi.label, vi.property)
                if fn is None:
                    continue
                mcp.tool()(fn)
                names.append(fn.__name__)
        return names

    def _make_retrieve_evidence(self, label: str, prop: str):
        client = self.client
        graph = self.graph
        # Both go into the call as string literals; a name that is not a plain
        # identifier gets no tool rather than an escaping scheme to get wrong.
        try:
            validate_identifier(label)
            validate_identifier(prop)
        except ValueError:
            return None

        def retrieve_evidence(query_vector: list, k: int = 5, hops: int = 1) -> str:
            try:
                vec = [float(x) for x in query_vector]
                if not vec or not all(math.isfinite(x) for x in vec):
                    return self._json({"error": "query_vector must be a non-empty list of finite numbers"})
                safe_k = max(1, min(int(k), 100))
                hops = int(hops)
                if not 0 <= hops <= MAX_HOPS:
                    return self._json({"error": f"hops must be between 0 and {MAX_HOPS}"})
                cypher = (
                    f"CALL ai.rag.retrieve('{label}', '{prop}', "
                    f"[{', '.join(repr(x) for x in vec)}], {safe_k}, {hops}) "
                    # The id columns, not node/seed/path: those are closed
                    # to a principal whose retrieval rules withhold
                    # properties (#2233), and the envelope needs no more.
                    "YIELD node_id, labels, seed_id, score, hops, source, hop_sequence "
                    "RETURN node_id, labels, seed_id, score, hops, source, hop_sequence"
                )
                result = client.query_readonly(cypher, graph)
                return self._json(envelope(result.columns, result.records))
            except Exception as exc:
                return self._json({"error": str(exc)})

        retrieve_evidence.__name__ = f"retrieve_evidence_{label.lower()}"
        retrieve_evidence.__doc__ = (
            f"Retrieve {label} nodes similar to a query vector (property: "
            f"{prop}) and the graph around them, with the evidence for each: "
            f"its recorded source, the matching seed and its score, and the "
            f"exact path walked from it.\n\n"
            f"Returns JSON {{sources: [...], lineage_paths: [...]}}. Cite "
            f"sources; a null provenance means the source is unknown.\n\n"
            f"Args:\n"
            f"    query_vector: List of floats representing the query.\n"
            f"    k: Number of seed matches (default 5, max 100).\n"
            f"    hops: Edges to walk out from each seed (default 1, max {MAX_HOPS})."
        )
        return retrieve_evidence

    def _make_find_similar(self, label: str, prop: str):
        client = self.client
        graph = self.graph

        def find_similar(query_vector: list, k: int = 10) -> str:
            """Find nodes with similar vector embeddings using k-NN search.

            Args:
                query_vector: The query vector (list of floats).
                k: Number of nearest neighbours to return (default 10).

            Returns:
                JSON array of similar nodes with similarity scores.
            """
            try:
                safe_k = min(int(k), 100)
                neighbours = client.vector_search(
                    label, prop, query_vector, safe_k
                )

                results = []
                for node_id, distance in neighbours:
                    rows = to_dicts(
                        client.query_readonly(
                            f"MATCH (n:{label}) WHERE id(n) = {node_id} "
                            f"RETURN id(n) AS _id, labels(n) AS labels",
                            graph,
                        )
                    )
                    entry = {
                        "node_id": node_id,
                        "similarity": round(1.0 - distance, 4),
                    }
                    if rows:
                        entry.update(rows[0])
                    results.append(entry)
                return self._json(results)
            except Exception as exc:
                return self._json({"error": str(exc)})

        find_similar.__name__ = f"find_similar_{label.lower()}"
        find_similar.__doc__ = (
            f"Find {label} nodes similar to a query vector "
            f"(property: {prop}).\n\n"
            f"Args:\n"
            f"    query_vector: List of floats representing the query.\n"
            f"    k: Number of nearest neighbours (default 10, max 100)."
        )
        return find_similar
