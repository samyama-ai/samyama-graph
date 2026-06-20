#!/usr/bin/env python3
"""Narrated DBMS-Research KG demo with a vector-search finale.

The first steps run the structure queries from queries.cypher; the finale picks a
seed research problem, reads its embedding straight from the graph, and asks
Samyama's HNSW index for the nearest open problems — semantic "find work like
this" with no external embedding service (the query vector comes from the graph).
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "_lib"))

import requests
from demo_lib import (console, pause, run_query, show_scale, step, takeaway,
                      title_card)
from rich.table import Table
from samyama_http import SamyamaClient, parse_queries

BASE = os.environ.get("SG_BASE_URL", "http://127.0.0.1:8080")
GRAPH = os.environ.get("SG_GRAPH", "default")
# The query vector is a real Problem embedding extracted once from this very
# snapshot and committed alongside the demo, so the semantic search reproduces
# offline with no embedding service and no runtime vector round-trip.
SEED = json.load(open(os.path.join(os.path.dirname(__file__), "seed_embedding.json")))


def vector_search(query_vec, label="Problem", k=7, retries=8):
    """Hit the /api/vector-search HTTP endpoint, retrying while the HNSW index
    finishes rebuilding right after import."""
    for _ in range(retries):
        try:
            r = requests.post(f"{BASE}/api/vector-search", timeout=60, json={
                "graph": GRAPH, "label": label,
                "property_key": "embedding", "query_vector": query_vec, "k": k,
            })
            if r.status_code == 200:
                return r.json().get("results", [])
        except requests.exceptions.RequestException:
            pass
        time.sleep(1.5)
    return []


def main():
    client = SamyamaClient(BASE, GRAPH)
    title_card(
        "DBMS Research Knowledge Graph",
        "1000+ open database-research problems — searchable by meaning",
        "data: dbms_research corpus · OpenAI embeddings (1024-dim) on every Problem",
    )
    step(1, "Snapshot imported into Samyama")
    show_scale(client)

    queries = parse_queries(os.path.join(os.path.dirname(__file__), "queries.cypher"))
    for i, q in enumerate(queries, start=1):
        step(i + 1, q.title)
        run_query(client, q, i)

    # --- vector-search finale -------------------------------------------------
    step(len(queries) + 2, "Vector search: find research problems like this one")
    title, emb = SEED["title"], SEED["embedding"]
    console.print(f'  seed problem: [bold white]"{title}"[/bold white]')
    console.print("  [dim]→ POST /api/vector-search  (1024-dim embedding, cosine)[/dim]")
    pause()
    results = vector_search(emb, label="Problem", k=7)
    tbl = Table(show_edge=False, box=None, header_style="bold magenta")
    tbl.add_column("score", justify="right")
    tbl.add_column("nearest open problem")
    shown = 0
    for it in results:
        name = it["node"].get("properties", {}).get("title", "")
        if name == title:
            continue   # skip the seed itself
        tbl.add_row(f'{it.get("score", 0):.3f}', name)
        shown += 1
        if shown >= 5:
            break
    console.print(tbl)
    console.print(f"  [green]→[/green] semantically nearest open problems by embedding")
    pause()

    takeaway("Graph structure + vector semantics in one engine — citation networks,\n"
             "author graphs, and semantic 'find research like this' over the same nodes.")


if __name__ == "__main__":
    main()
