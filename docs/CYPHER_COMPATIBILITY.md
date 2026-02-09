# Cypher Compatibility Matrix

**Last Updated:** 2026-02-08
**Version:** Samyama v0.5.0-alpha.1

This document tracks the compatibility of Samyama's OpenCypher implementation against the industry standard (Neo4j) and modern competitors (FalkorDB).

## 🚦 Summary

Samyama provides a **functional Cypher engine** with support for pattern matching, CRUD operations, aggregations, sorting, and vector/algorithm extensions.

*   **Supported:** Pattern matching, CRUD (CREATE/DELETE/SET), Aggregations (COUNT/SUM/AVG/MIN/MAX/COLLECT), ORDER BY, LIMIT, Vector Indexing, Graph Algorithms, Optimization Solvers.
*   **Partial:** `SKIP` not implemented, `RETURN DISTINCT` not implemented.
*   **Unsupported:** Pipelining (`WITH`), Upserts (`MERGE`), `OPTIONAL MATCH`, `UNION`, String/List/Scalar functions.

## 📊 Feature Matrix

| Feature Category | Feature | Samyama | FalkorDB | Neo4j | Notes |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Read Operations** | `MATCH` | ✅ | ✅ | ✅ | Basic pattern matching works. |
| | `OPTIONAL MATCH` | ❌ | ✅ | ✅ | Returns `null` for missing patterns. |
| | `WHERE` | ✅ | ✅ | ✅ | **Supported**: Precedence issues fixed via Pratt Parser. |
| | `RETURN` | ✅ | ✅ | ✅ | Projections work. |
| | `RETURN DISTINCT` | ❌ | ✅ | ✅ | Deduplication not implemented. |
| | `ORDER BY` | ✅ | ✅ | ✅ | **Supported**: In-memory sorting implemented. |
| | `SKIP` / `LIMIT` | ⚠️ | ✅ | ✅ | `LIMIT` works; `SKIP` not implemented. |
| **Write Operations** | `CREATE` | ✅ | ✅ | ✅ | Nodes, edges, chained patterns with properties. |
| | `DELETE` | ✅ | ✅ | ✅ | Node and edge deletion supported. |
| | `SET` | ✅ | ✅ | ✅ | Property updates work. |
| | `REMOVE` | ❌ | ✅ | ✅ | Label/Property removal not implemented. |
| | `MERGE` | ❌ | ✅ | ✅ | **Critical Gap**: No upsert capability. |
| **Aggregations** | `count()` | ✅ | ✅ | ✅ | Global and grouped aggregation supported. |
| | `sum()` | ✅ | ✅ | ✅ | Numeric summation via AggregateOperator. |
| | `avg()` | ✅ | ✅ | ✅ | Numeric average via AggregateOperator. |
| | `min()`, `max()` | ✅ | ✅ | ✅ | Min/Max via AggregateOperator. |
| | `COLLECT` | ✅ | ✅ | ✅ | List aggregation via AggregateOperator. |
| | `GROUP BY` | ✅ | ✅ | ✅ | Implicit grouping in `RETURN` supported. |
| **Query Structure** | `WITH` | ❌ | ✅ | ✅ | Pipelining results to next query stage. |
| | `UNWIND` | ❌ | ✅ | ✅ | List expansion. |
| | `UNION` | ❌ | ✅ | ✅ | Combining result sets. |
| **Functions** | String Functions | ❌ | ✅ | ✅ | e.g., `toUpper`, `substring`. |
| | Scalar Functions | ❌ | ✅ | ✅ | e.g., `coalesce`, `head`. |
| | List Functions | ❌ | ✅ | ✅ | e.g., `nodes()`, `relationships()`. |
| **Vector / AI** | `CREATE VECTOR INDEX` | ✅ | ⚠️ | ⚠️ | **Native Syntax**. Falkor/Neo4j use procedures or separate indices. |
| | `CALL db.index.vector...` | ✅ | ⚠️ | ⚠️ | Optimized for RAG. |
| | `algo.pageRank` | ✅ | ✅ | ✅ | Iterative ranking. |
| | `algo.wcc` | ✅ | ✅ | ✅ | Weakly Connected Components. |
| | `algo.scc` | ✅ | ✅ | ✅ | Strongly Connected Components (Tarjan's). |
| | `algo.bfs` / `shortestPath` | ✅ | ✅ | ✅ | Unweighted shortest path. |
| | `algo.dijkstra` / `weightedPath` | ✅ | ❌ | ✅ | Weighted shortest path. |
| | `algo.maxFlow` | ✅ | ❌ | ❌ | Edmonds-Karp Max Flow. |
| | `algo.mst` | ✅ | ❌ | ❌ | Prim's Minimum Spanning Tree. |
| | `algo.triangleCount` | ✅ | ❌ | ❌ | Topology analysis. |
| | `algo.or.solve` | ✅ | ❌ | ❌ | **Unique**: In-Database Optimization (Single & Multi-Objective). |

## 🛠 Known Issues

1.  **Missing Features**: `MERGE`, `WITH`, `OPTIONAL MATCH`, `UNION` are high-priority gaps.
2.  **Missing Clauses**: `SKIP`, `RETURN DISTINCT` not yet implemented.
3.  **No String/List/Scalar Functions**: Built-in functions like `toUpper()`, `substring()`, `coalesce()`, `nodes()` are not yet available.

## 📅 Roadmap for Compatibility

Remaining gaps to reach "Bronze" compatibility tier (usable for general apps):

1.  **WITH (Pipelining)**: Allow multi-stage query plans.
2.  **MERGE (Upsert)**: Get-or-create semantics.
3.  **OPTIONAL MATCH**: Return `null` for unmatched patterns.
4.  **UNION**: Combine result sets from multiple queries.
5.  **SKIP / DISTINCT**: Pagination and deduplication support.
6.  **String/List Functions**: `toUpper`, `toLower`, `substring`, `nodes()`, `relationships()`, etc.
