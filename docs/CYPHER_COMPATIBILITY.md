# Cypher Compatibility Matrix

**Last Updated:** 2026-01-26
**Version:** Samyama v0.3.1

This document tracks the compatibility of Samyama's OpenCypher implementation against the industry standard (Neo4j) and modern competitors (FalkorDB).

## 🚦 Summary

Samyama is currently in an **MVP State** regarding Cypher support. We prioritize high-throughput ingestion and vector search over full query language compliance.

*   **Supported:** Basic pattern matching, CRUD operations, Vector Indexing.
*   **Partial/Buggy:** Filtering logic (precedence issues).
*   **Unsupported:** Aggregations, Sorting, Pipelining (`WITH`), Upserts (`MERGE`).

## 📊 Feature Matrix

| Feature Category | Feature | Samyama | FalkorDB | Neo4j | Notes |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Read Operations** | `MATCH` | ✅ | ✅ | ✅ | Basic pattern matching works. |
| | `OPTIONAL MATCH` | ❌ | ✅ | ✅ | Returns `null` for missing patterns. |
| | `WHERE` | ⚠️ | ✅ | ✅ | **Bug**: Precedence issues (e.g., `AND` binds tighter than `>=`). Requires parentheses. |
| | `RETURN` | ✅ | ✅ | ✅ | Projections work. |
| | `RETURN DISTINCT` | ❌ | ✅ | ✅ | Deduplication not implemented. |
| | `ORDER BY` | ❌ | ✅ | ✅ | Parsed but execution fails. |
| | `SKIP` / `LIMIT` | ⚠️ | ✅ | ✅ | `LIMIT` works; `SKIP` not implemented. |
| **Write Operations** | `CREATE` | ✅ | ✅ | ✅ | Fast node/edge creation. |
| | `DELETE` | ✅ | ✅ | ✅ | Basic deletion works. |
| | `SET` | ✅ | ✅ | ✅ | Property updates work. |
| | `REMOVE` | ❌ | ✅ | ✅ | Label/Property removal not implemented. |
| | `MERGE` | ❌ | ✅ | ✅ | **Critical Gap**: No upsert capability. |
| **Aggregations** | `count()` | ❌ | ✅ | ✅ | Runtime error in projection. |
| | `sum()`, `avg()`, `max()` | ❌ | ✅ | ✅ | Not implemented. |
| | `GROUP BY` | ❌ | ✅ | ✅ | Implicit grouping in `RETURN` not supported. |
| **Query Structure** | `WITH` | ❌ | ✅ | ✅ | Pipelining results to next query stage. |
| | `UNWIND` | ❌ | ✅ | ✅ | List expansion. |
| | `UNION` | ❌ | ✅ | ✅ | Combining result sets. |
| **Functions** | String Functions | ❌ | ✅ | ✅ | e.g., `toUpper`, `substring`. |
| | Scalar Functions | ❌ | ✅ | ✅ | e.g., `coalesce`, `head`. |
| | List Functions | ❌ | ✅ | ✅ | e.g., `nodes()`, `relationships()`. |
| **Vector / AI** | `CREATE VECTOR INDEX` | ✅ | ⚠️ | ⚠️ | **Native Syntax**. Falkor/Neo4j use procedures or separate indices. |
| | `CALL db.index.vector...` | ✅ | ⚠️ | ⚠️ | Optimized for RAG. |

## 🛠 Known Issues

1.  **Parser Precedence**: The current parser treats all binary operators with flat left-to-right precedence.
    *   *Fails:* `WHERE n.age > 10 AND n.city = 'NY'`
    *   *Workaround:* `WHERE (n.age > 10) AND (n.city = 'NY')`
2.  **Aggregation Runtime**: Using `count(n)` in `RETURN` clause causes a runtime panic ("Unsupported projection expression").
3.  **Sorting**: `ORDER BY` clause is parsed but ignored or causes execution errors.

## 📅 Roadmap for Compatibility

To reach "Bronze" compatibility tier (usable for general apps):

1.  **Phase 8.1 (Parser Refactor)**: Migrate to Pratt Parser to fix operator precedence.
2.  **Phase 8.2 (Aggregations)**: Implement `AggregateOperator` (Hash/Stream) for `count`, `sum`, etc.
3.  **Phase 8.3 (Sorting)**: Implement `SortOperator` (In-memory sort).
4.  **Phase 8.4 (Pipelining)**: Implement `WITH` to allow multi-stage query plans.
