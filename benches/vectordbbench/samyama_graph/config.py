"""VectorDBBench configuration for samyama-graph (#1984).

Copied into a VectorDBBench checkout as
`vectordb_bench/backend/clients/samyama_graph/config.py`; see ../README.md.
"""

from pydantic import BaseModel, SecretStr

from ..api import DBCaseConfig, DBConfig, MetricType


class SamyamaGraphConfig(DBConfig):
    """Where the server is. `url` is its HTTP API (`cargo run` serves :8080)."""

    url: str = "http://127.0.0.1:8080"
    # Optional `Authorization: Bearer` token, when the server requires one.
    token: SecretStr | None = None
    # The graph (tenant) to load into. `default` unless the server runs with
    # a tenant manager.
    graph: str = "default"

    def to_dict(self) -> dict:
        return {
            "url": self.url.rstrip("/"),
            "token": self.token.get_secret_value() if self.token else None,
            "graph": self.graph,
        }


class SamyamaGraphIndexConfig(BaseModel, DBCaseConfig):
    """HNSW parameters, as `CREATE VECTOR INDEX ... OPTIONS` takes them."""

    metric_type: MetricType | None = None
    m: int = 16
    ef_construction: int = 200
    # Search width per query. VectorDBBench searches at k = 100, so a width
    # below 100 cannot return k answers; the client raises it to k.
    ef_search: int = 128

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "l2"
        if self.metric_type in (MetricType.IP, MetricType.DP):
            return "dot"
        return "cosine"

    def index_param(self) -> dict:
        return {
            "similarity": self.parse_metric(),
            "m": self.m,
            "ef_construction": self.ef_construction,
        }

    def search_param(self) -> dict:
        return {"ef": self.ef_search}
