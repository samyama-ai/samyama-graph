"""samyama-graph client for VectorDBBench (#1984).

Copied into a VectorDBBench checkout as
`vectordb_bench/backend/clients/samyama_graph/samyama_graph.py`; see
../README.md for the three registration edits.

Everything goes through the server's HTTP Cypher endpoint, `POST /api/query`,
the same route the SDKs use:

- insert:   `UNWIND $rows AS r CREATE (:<label> {id: r.id, embedding: r.v})`
- optimize: `CREATE VECTOR INDEX ... FOR (n:<label>) ON (n.embedding) OPTIONS {...}`,
            which builds the HNSW graph from the rows already loaded. Built
            here rather than in `init`, so VectorDBBench's `load_duration`
            (insert + optimize) covers ingest and build, as it does for the
            systems that build after loading.
- search:   `CALL db.index.vector.queryNodes('<label>', 'embedding', $q, k, {ef: N})
             YIELD node RETURN node.id`, sent read-only with the result cache
            off, so a repeated query measures the index and not the cache.

`/api/vector-search` is not used: it has no per-query `ef`, and the width is
the one parameter a recall-QPS curve sweeps.
"""

import logging
from contextlib import contextmanager

import requests

from ..api import VectorDB
from .config import SamyamaGraphIndexConfig

log = logging.getLogger(__name__)

PROPERTY = "embedding"

# `/api/query` takes a JSON body of at most 2 MiB (axum's default limit). A
# float serialises to about 20 bytes of JSON, so a batch is cut to fit.
BODY_BUDGET = 1_500_000
BYTES_PER_VALUE = 20


def rows_per_request(dim: int) -> int:
    return max(1, BODY_BUDGET // (max(dim, 1) * BYTES_PER_VALUE))


class SamyamaGraph(VectorDB):
    name = "SamyamaGraph"
    # One `requests.Session` per process; VectorDBBench copies the client
    # into each search process and calls `init` there.
    thread_safe = False

    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: SamyamaGraphIndexConfig | None,
        collection_name: str = "VectorDBBenchItem",
        drop_old: bool = False,
        **kwargs,
    ):
        self.dim = dim
        self.db_config = db_config
        self.case_config = db_case_config or SamyamaGraphIndexConfig()
        # A label must be an identifier; VectorDBBench's names are.
        self.label = "".join(c for c in collection_name if c.isalnum() or c == "_") or "Item"
        self.session = None
        if drop_old:
            with self.init():
                self._drop()

    # ------------------------------------------------------------- plumbing

    @contextmanager
    def init(self):
        self.session = requests.Session()
        token = self.db_config.get("token")
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"
        try:
            yield
        finally:
            self.session.close()
            self.session = None

    def _query(self, text: str, params: dict | None = None, read_only: bool = False) -> dict:
        body = {
            "query": text,
            "graph": self.db_config.get("graph", "default"),
            "params": params or {},
            "read_only": read_only,
            "cache": False,
        }
        r = self.session.post(f"{self.db_config['url']}/api/query", json=body, timeout=600)
        if r.status_code != 200:
            raise RuntimeError(f"samyama-graph {r.status_code}: {r.text[:500]}")
        return r.json()

    def _drop(self):
        # Delete in slices so no single statement holds the write lock for
        # the whole collection. A vector index on the label survives; a fresh
        # server per case is the clean start, and the README says so.
        while True:
            out = self._query(
                f"MATCH (n:{self.label}) WITH n LIMIT 10000 DETACH DELETE n RETURN count(*) AS c"
            )
            records = out.get("records") or [[0]]
            if not records or not records[0] or int(records[0][0]) == 0:
                break

    # ------------------------------------------------------------ interface

    def insert_embeddings(
        self,
        embeddings: list[list[float]],
        metadata: list[int],
        labels_data: list[str] | None = None,
        tenant_labels_data: list[str] | None = None,
        **kwargs,
    ) -> tuple[int, Exception | None]:
        step = rows_per_request(self.dim)
        done = 0
        try:
            for start in range(0, len(embeddings), step):
                rows = [
                    {"id": int(i), "v": [float(x) for x in v]}
                    for i, v in zip(
                        metadata[start : start + step], embeddings[start : start + step], strict=True
                    )
                ]
                self._query(
                    f"UNWIND $rows AS r CREATE (:{self.label} {{id: r.id, {PROPERTY}: r.v}})",
                    {"rows": rows},
                )
                done += len(rows)
        except Exception as e:  # VectorDBBench expects the count and the error back
            log.warning(f"samyama-graph insert failed after {done} rows: {e}")
            return done, e
        return done, None

    def optimize(self, data_size: int | None = None):
        p = self.case_config.index_param()
        self._query(
            f"CREATE VECTOR INDEX FOR (n:{self.label}) ON (n.{PROPERTY}) OPTIONS "
            f"{{dimensions: {int(self.dim)}, similarity: '{p['similarity']}', "
            f"m: {int(p['m'])}, ef_construction: {int(p['ef_construction'])}}}"
        )

    def search_embedding(self, query: list[float], k: int = 100, *args, **kwargs) -> list[int]:
        ef = max(int(self.case_config.search_param()["ef"]), int(k))
        out = self._query(
            f"CALL db.index.vector.queryNodes('{self.label}', '{PROPERTY}', $q, {int(k)}, {{ef: {ef}}}) "
            f"YIELD node RETURN node.id AS id",
            {"q": [float(x) for x in query]},
            read_only=True,
        )
        return [int(row[0]) for row in out.get("records", [])]
