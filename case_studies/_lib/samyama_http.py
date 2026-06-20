"""Tiny HTTP client for a running Samyama server.

Case studies talk to the server that `cargo build --release` produces over the
HTTP API (POST /api/query, POST /api/snapshot/import, GET /api/status). This
keeps the whole "clone → build → run" loop free of any Python-wheel build step —
all a user needs is `pip install rich requests`.

Also exposes a shared `queries.cypher` parser so the demo and the DoD validator
read the *same* query definitions (single source of truth).
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any

import requests


@dataclass
class QueryResult:
    columns: list[str]
    records: list[list[Any]]
    elapsed_ms: float

    @property
    def rows(self) -> int:
        return len(self.records)

    def scalar(self) -> Any:
        """Single cell, for COUNT-style queries."""
        if self.records and self.records[0]:
            return self.records[0][0]
        return None


class SamyamaClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8080", graph: str = "default"):
        self.base_url = base_url.rstrip("/")
        self.graph = graph

    def status(self) -> dict:
        r = requests.get(f"{self.base_url}/api/status", timeout=30)
        r.raise_for_status()
        return r.json()

    def node_count(self) -> int:
        return int(self.status().get("storage", {}).get("nodes", 0))

    def edge_count(self) -> int:
        return int(self.status().get("storage", {}).get("edges", 0))

    def query(self, cypher: str, graph: str | None = None) -> QueryResult:
        t0 = time.perf_counter()
        r = requests.post(
            f"{self.base_url}/api/query",
            json={"query": cypher, "graph": graph or self.graph},
            timeout=300,
        )
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        if r.status_code != 200:
            try:
                msg = r.json().get("error", r.text)
            except Exception:
                msg = r.text
            raise RuntimeError(f"query failed ({r.status_code}): {msg}")
        body = r.json()
        return QueryResult(
            columns=body.get("columns", []),
            records=body.get("records", []),
            elapsed_ms=elapsed_ms,
        )

    def import_snapshot(self, path: str, dedup_key: str | None = None) -> dict:
        params = {"dedup_key": dedup_key} if dedup_key else {}
        with open(path, "rb") as f:
            r = requests.post(
                f"{self.base_url}/api/snapshot/import",
                files={"file": f},
                params=params,
                timeout=3600,
            )
        r.raise_for_status()
        return r.json()


@dataclass
class ShowcaseQuery:
    title: str
    insight: str
    cypher: str


def parse_queries(path: str) -> list[ShowcaseQuery]:
    """Parse a queries.cypher file.

    Format — each showcase query is introduced by a marker comment and ends at a
    semicolon, so the demo narration and the validator never drift apart:

        // @query Top dismissal rivalries | Who gets whom out most across all formats?
        MATCH (bowler:Player)-[d:DISMISSED]->(batsman:Player)
        RETURN bowler.name AS bowler, batsman.name AS victim, count(d) AS n
        ORDER BY n DESC LIMIT 5;
    """
    queries: list[ShowcaseQuery] = []
    title = insight = None
    buf: list[str] = []
    marker = re.compile(r"^\s*//\s*@query\s+(?P<title>.+?)\s*(\|\s*(?P<insight>.+))?\s*$")
    for raw in open(path, encoding="utf-8"):
        m = marker.match(raw)
        if m:
            title = m.group("title").strip()
            insight = (m.group("insight") or "").strip()
            buf = []
            continue
        if raw.lstrip().startswith("//"):
            continue  # ordinary comment
        buf.append(raw)
        if ";" in raw:
            cypher = "".join(buf).strip().rstrip(";").strip()
            if cypher and title:
                queries.append(ShowcaseQuery(title=title, insight=insight or "", cypher=cypher))
            title = insight = None
            buf = []
    return queries
