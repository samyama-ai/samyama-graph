# DBMS Research KG — Case Study (Vector Search)

1,000+ open, hard database-research problems — with their topics, formal bounds,
papers, authors and computation models — as a graph, where **every problem and
topic also carries a 1024-dim embedding**. This is the case study for Samyama's
**vector search**: graph structure and semantic similarity over the *same* nodes,
in one engine.

![DBMS research demo](demo.gif)

```bash
cd case_studies/dbms-research && ./run.sh        # validate the structure queries
RECORD=1 ./run.sh                                # also regenerate demo.gif (incl. vector search)
```

## The graph

**Scale:** 18,751 nodes · 38,539 edges · **2 HNSW vector indices** (Problem &
Topic embeddings, 1024-dim, cosine) rebuilt automatically on import.

| Node label | Count | | Node label | Count |
|------------|-------|-|------------|-------|
| Person | 3,939 | | Institution | 1,402 |
| FutureDirection | 3,825 | | Problem | 1,053 |
| Paper | 2,954 | | Venue | 480 |
| Algorithm | 2,807 | | Topic | 35 |
| Bound | 2,106 | | Concept | 34 |

**Relationships:** `AUTHORED_BY` (9.1K), `CITES` (6.1K), `APPEARED_AT` (5.8K),
`STATE_OF_ART` (4.1K), `CLOSING_REQUIRES` (3.8K), `HAS_BOUND` (2.1K), `IN_MODEL`,
`AFFILIATED_WITH`, `ACTIVE_ON`, `IN_TOPIC`, `RESTS_ON`, `RELATED_TO`.

## Showcase

**Structure queries** ([`queries.cypher`](queries.cypher), DoD-gated): topics by
open-problem count → most-cited papers → most prolific authors → best-characterised
problems (by formal bounds).

**Vector-search finale** ([`demo.py`](demo.py)): the demo takes a seed problem
("Cardinality estimation error propagation through plans") whose 1024-dim
embedding is committed alongside the demo ([`seed_embedding.json`](seed_embedding.json),
extracted once from this snapshot) and posts it to `POST /api/vector-search` —
Samyama's HNSW index returns the **research topics** the problem is semantically
nearest to. No external embedding service: the query vector ships with the demo,
so the semantic search reproduces offline.

> Vector search is also covered directly in the engine suite — see
> `benches/vector_benchmark.rs` and `tests/vector_search_test.rs`.

## Data & license

Source: [`dbms_research`](https://github.com/samyama-ai/dbms_research) — Samyama's
open corpus of database-research problems; embeddings via OpenAI text-embedding.
Snapshot `dbms-research.sgsnap` on release
[`kg-snapshots-v7`](https://github.com/samyama-ai/samyama-graph/releases/tag/kg-snapshots-v7)
(sha256 pinned in [`case.env`](case.env)).
