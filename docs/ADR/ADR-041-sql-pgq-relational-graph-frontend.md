# ADR-041: Read-Only `SQL/PGQ`-Style Relational-Graph Frontend (`GRAPH_TABLE` & Virtual Graph Views)

## Status
**Proposed**

## Date
2026-11-25

## Objective

Provide a thin, read-only analytical SQL frontend — supporting a documented `SQL/PGQ`-style `GRAPH_TABLE` operator (reusing Samyama's existing GPML path restrictors and selectors) plus virtual node-label and edge-type relational views — that lowers directly into Samyama's existing `LogicalPlanNode` IR (`ADR-015`) and Apache Arrow result exporter (`src/export/mod.rs`), without introducing a second storage engine or relational OLTP write path.

---

## Context

### 1. Why Unify Relational Set Algebra and Graph Pattern Matching in One Engine

Property-graph workloads routinely combine two distinct algebraic shapes:

1. **Topological & Path-Centric Operations (Graph-Native Advantage):**
   - Multi-hop reachability, path-mode semantics (`WALK`, `TRAIL`, `ACYCLIC`, `SIMPLE` with `ALL`, `ANY`, `ALL SHORTEST`, `ANY SHORTEST` selectors in `src/query/dialect.rs`), cyclic motif intersection (`LogicalPlanNode::TrieJoin` via `src/query/executor/leapfrog.rs`), hierarchy subsumption and O(1) subtree roll-ups (`ADR-035`, `src/query/executor/hierarchy_detector.rs`), and HNSW vector kNN (`src/query/executor/vector_ops.rs`).
   - Expressing variable-length paths or cyclic triangles in pure relational SQL requires cascading self-joins or `WITH RECURSIVE` common table expressions that explode intermediate cardinality.
2. **Dimensional Set & Tabular Analytics (Relational SQL Advantage):**
   - Multi-column `GROUP BY` / `HAVING` threshold filtering, `CASE WHEN` / `FILTER (WHERE ...)` cohort pivoting, window analytics (`ROW_NUMBER()`, `SUM(...) OVER (PARTITION BY ... ORDER BY ...)`), CTE composition, and flat projections expected by BI tools, notebooks, and dataframe libraries.
   - Expressing multi-stage dimensional rollups in OpenCypher requires procedural `WITH ... WITH ...` variable piping, where omitting an identifier silently drops it from scope or alters grouping cardinality.

Today, users who need both shapes either pay an ETL tax (exporting graph projections to DuckDB/Postgres/Parquet) or rewrite analytical SQL dashboards into procedural Cypher `WITH` chains.

### 2. What Already Exists in Samyama (`origin/main`)

Samyama already contains almost all of the storage, planning, and columnar export machinery needed to execute analytical `SQL/PGQ` queries natively:

- **Columnar Property Storage & Late Materialization (`ADR-012`, `ADR-021`):** Node and edge properties live in sparse typed columns alongside CSR adjacency lists; scans carry lightweight `Value::NodeRef(NodeId)` handles and resolve properties lazily.
- **Language-Independent Logical IR (`ADR-015`, `src/query/executor/logical_plan.rs`):** `LogicalPlanNode` (`LabelScan`, `IndexLookup`, `Expand`, `ExpandInto`, `TrieJoin`, `Filter`, `Join`, `CartesianProduct`, `AdjacencyCountAggregate`) decouples surface syntax from physical Volcano/vectorized execution (`src/query/executor/physical_planner.rs`, `operator.rs`) and cost-based plan enumeration (`plan_enumerator.rs`, `cost_model.rs`).
- **GPML Path Modes & Dialect Pass (`src/query/dialect.rs`):** `Dialect::Gql` already implements GPML path-mode defaults (`PathRestrictor::Walk` when unprefixed; bare `*` quantifier minimum `0` instead of OpenCypher's `1`) and re-runs `crate::query::validate::validate` so unbounded `WALK` quantifiers are rejected cleanly rather than bypassing cycle safety checks (`#1141`, `#1648`).
- **Multi-Frontend Precedent (`src/sparql/`):** Samyama already hosts a second query frontend (`SPARQL 1.1` in `src/sparql/{mod,algebra,executor,http}.rs`) over the same server binary and tenant lifecycle.
- **Deterministic Apache Arrow Unification (`src/export/mod.rs`, `#1097`) & Streaming (`ADR-039`):** `RecordBatch` already converts to `arrow::record_batch::RecordBatch` with explicit, lossless column unification (`Boolean`, `Int64`, `Float64`, `Utf8`, `List<Int64|Float64|Utf8>`, `Null`, ISO-8601 text preservation for zoned temporals, and JSON serialization for composite graph values).

---

## Decision

### 1. Scope: Read-Only Analytical `SQL/PGQ`-Inspired Subset (`src/sql/`)

We will introduce a read-only `src/sql/` frontend crate/module built on `sqlparser` (Apache-2.0) plus a small `GRAPH_TABLE` recursive-descent hook delegating inner `MATCH` patterns to Samyama's existing Pest GPML/Cypher pattern parser (`src/query/parser.rs`) under `Dialect::Gql` (`src/query/dialect.rs`).

#### Supported Query Forms (Phase 1 MVP)

1. **`GRAPH_TABLE` in `FROM` / `JOIN` Clauses:**
   ```sql
   SELECT
       gt.therapeutic_area,
       COUNT(DISTINCT gt.trial_id) AS trial_count,
       AVG(gt.enrollment)          AS avg_enrollment
   FROM GRAPH_TABLE (current_graph
       MATCH (s:Sponsor {country: 'US'})-[:SPONSORS]->(t:ClinicalTrial)
             -[:STUDIES]->(d:Disease)
       WHERE t.phase = 'PHASE3'
       COLUMNS (
           d.therapeutic_area AS therapeutic_area,
           t.nct_id           AS trial_id,
           t.enrollment       AS enrollment
       )
   ) AS gt
   GROUP BY gt.therapeutic_area
   HAVING COUNT(DISTINCT gt.trial_id) >= 5
   ORDER BY trial_count DESC
   LIMIT 20;
   ```
   - Every item in `COLUMNS (<expr> AS <alias>)` projects a scalar or list expression over variables bound in `MATCH` into a flat relational column on `Record`.
   - Unprefixed variable-length patterns inside `GRAPH_TABLE` follow `Dialect::Gql` (`WALK` + `*0..`, validated via `crate::query::validate::validate` so unbounded `WALK` without a single-path selector errors deterministically per `#1648`). Explicit restrictors (`TRAIL`, `ACYCLIC`, `SIMPLE`, `WALK`) and selectors (`ALL`, `ANY`, `ALL SHORTEST`, `ANY SHORTEST`) work identically to `#1145`.
2. **Virtual Node-Label and Edge-Type Relational Views:**
   - Every node label `L` in the active tenant's `GraphStore` is queryable as a read-only virtual table exposing `_node_id BIGINT NOT NULL` plus the union of property keys present on label `L` in `ColumnStore`.
   - Every edge type `E` is queryable as a read-only virtual table exposing `_edge_id BIGINT`, `_src_id BIGINT NOT NULL`, `_dst_id BIGINT NOT NULL`, plus edge property keys.
   - `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS` synthesize schema metadata on demand from `GraphStore` catalog statistics (`schema_doc.rs` / `store.rs`), enabling BI tools and SQL clients to introspect labels, edge types, and inferred column types without persistent catalog duplication.
3. **Outer Relational Clauses:**
   - `SELECT [DISTINCT]`, scalar projections (`CASE WHEN`, `COALESCE`, arithmetic, string/temporal functions), `WHERE`, `[INNER | LEFT OUTER | CROSS] JOIN`, `WHERE [NOT] EXISTS (...)`, `GROUP BY`, `HAVING`, non-recursive `WITH cte AS (...)`, `UNION [ALL]`, `ORDER BY [ASC | DESC] [NULLS FIRST | LAST]`, `LIMIT`, `OFFSET`, and `EXPLAIN`.
   - **Phase 2 Analytical Extensions:** Window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM`/`AVG`/`COUNT` `OVER (PARTITION BY ... ORDER BY ...)`) and `COUNT(*) FILTER (WHERE ...)`.

### 2. Explicit Non-Goals (Anti-Scope Guardrails)

To keep the engine fast, compact, and maintainable, the following are **strictly out of scope**:

1. **No Second OLTP Relational Storage Engine:** No heap tables, no SQL DDL (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`), no SQL DML (`INSERT`, `UPDATE`, `DELETE`, `MERGE`), no foreign-key constraints, no triggers, and no stored procedures (`PL/pgSQL`). Mutations remain governed exclusively by Samyama's graph write APIs (`MutQueryExecutor`, `.sgsnap` import, bulk loaders). Any SQL write/DDL statement is rejected at parse/bind time with a structured read-only error code (`src/query/error_code.rs`).
2. **No `Cypher -> SQL` Transpiler:** Lowering is strictly one-way (`SQL/PGQ -> LogicalPlanNode`), never translating Cypher into external SQL.
3. **No Unverified ISO Conformance Claims:** Neither ISO/IEC 9075-16:2023 (`SQL/PGQ`) nor ISO/IEC 39075:2024 (`GQL`) currently has an official public conformance test suite. Documentation, error messages, and release notes must refer to this feature strictly as a **documented, test-evidenced `SQL/PGQ`-style `GRAPH_TABLE` subset**, reporting exact pass/coverage counts on Samyama's differential test suite (mirroring how openCypher TCK pass rate and coverage are reported in `CLAUDE.md`).

### 3. Lowering Architecture & Known Correctness Guards

```text
SQL / SQL-PGQ Query String
   │
   ├─► sqlparser AST + GRAPH_TABLE Pattern Hook (Dialect::Gql + validate())
   │
   ├─► Binder (resolves tenant labels, edge views, COLUMNS aliases, INFORMATION_SCHEMA)
   │
   ├─► Logical Lowering (src/query/executor/logical_plan.rs)
   │     • Virtual Label Table  ──► LabelScan / IndexLookup + Filter
   │     • GRAPH_TABLE(MATCH..) ──► PlanEnumerator (Expand / ExpandInto / TrieJoin / OEH / HNSW)
   │     • SQL JOIN             ──► LogicalPlanNode::Join (hash/index) or Filter
   │     • GROUP BY / HAVING    ──► Physical Aggregate + post-Aggregate Filter
   │
   └─► Volcano / Streaming Execution (ADR-007, ADR-039) + Budget Enforcement (budget.rs)
         │
         └─► RecordBatch / ArrowBatch (src/export/mod.rs)
```

Two engine invariants require explicit enforcement in the SQL binder/planner:

1. **`ADR-017` Non-Unique Property Grouping Guard:** `LogicalPlanNode::AdjacencyCountAggregate` computes per-node degree and is only valid when `GROUP BY` keys functionally include unique node identity (`_node_id` or a `UNIQUE`-indexed property). Grouping on a non-unique property (e.g., `GROUP BY s.country`) **must not** lower directly to `AdjacencyCountAggregate` without a post-degree re-aggregation by `s.country`.
2. **Three-Valued `NULL` Logic & `ORDER BY` Null Ordering:** SQL `=` / `<>` / `IN` / `NOT IN` over nullable `ColumnStore` properties must preserve three-valued logic (`TRUE`, `FALSE`, `UNKNOWN`), matching Samyama's existing cross-type null propagation, with explicit `NULLS LAST` (`ASC`) / `NULLS FIRST` (`DESC`) defaults when unspecified.

### 4. Protocol & BI Connectivity Surface

1. **HTTP & RESP Endpoints (Phase 1):**
   - `POST /api/sql` (tenant-scoped, read-only, supporting `application/json`, `text/csv`, and `application/vnd.apache.arrow.stream` via `src/export/mod.rs` and `ADR-039` backpressure streaming) and `GRAPH.SQL <graph> <query>` on RESP3, governed by the same `--auth-file` RBAC (`Read` role minimum) and `budget.rs` per-query memory/timeout ceilings as `GRAPH.RO_QUERY`.
2. **Standard BI Wire Interface Evaluation (Phase 2 — Single Standard Connector):**
   - Rather than maintaining per-vendor BI plugins (Tableau, PowerBI, Superset, Grafana, `dbt`), Phase 2 will benchmark and select **one** standard tabular wire protocol layered over `ArrowBatch` / `RecordBatch`:
     - **Option A — Apache Arrow Flight SQL (`arrow-flight`):** Zero-copy columnar `ArrowBatch` streaming (`src/export/mod.rs` + `ADR-039`), native JDBC/ODBC drivers via Arrow Flight SQL driver, ideal for high-throughput analytical scans and dataframe (`Polars`/`DuckDB`/`PyArrow`) ingestion.
     - **Option B — Read-Only PostgreSQL v3 Wire Protocol (`pgwire`):** Text/binary row framing speaking `SELECT` + `pg_catalog`/`INFORMATION_SCHEMA` introspection for zero-driver compatibility with `psql`, Metabase, Grafana, Superset, and `psycopg`.

---

## Phased Implementation Plan (Post-H1 Release Gate)

| Phase | Scope | Verification Gate |
| :---: | :--- | :--- |
| **Phase 0** | Ratify `ADR-041`; publish exact BNF grammar for supported `GRAPH_TABLE` + virtual-view subset and type-mapping matrix. | Grammar & unsupported-construct error catalog documented (`error_code.rs`). |
| **Phase 1** | Read-only `src/sql/` parser, binder, `LogicalPlanNode` lowering (`GRAPH_TABLE`, virtual node/edge tables, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `EXPLAIN`), and `/api/sql` + `GRAPH.SQL`. | Differential test suite (`>= 100` queries) verifying row-for-row equivalence against `OpenCypher`/`GQL` on LDBC SNB, FinBench, and HIER in CI debug profile (`cargo test --workspace --no-fail-fast`). |
| **Phase 2** | `INFORMATION_SCHEMA`, window functions, `FILTER (WHERE ...)`, and Arrow Flight SQL vs. `pgwire` BI connector spike + benchmark. | End-to-end headless integration test connecting a standard SQL/BI client (`DuckDB` / `PyArrow Flight` / `psql`) against a multi-tenant snapshot with RBAC and `budget.rs` limits enforced. |

---

## Consequences

### Positive
- Eliminates ETL exports for mixed graph-traversal + relational rollup workloads while reusing 100% of Samyama's CSR, `ColumnStore`, `TrieJoin`, `OEH`, HNSW, MVCC, and `ArrowBatch` infrastructure.
- Opens Samyama knowledge graphs directly to SQL-fluent analysts, data engineers, notebooks, and standard BI tools.
- Preserves a compact binary footprint and clean architecture by strictly refusing OLTP relational DDL/DML scope creep.

### Trade-offs & Risks
- Adds `sqlparser` compile-time dependency (~25K LoC pure Rust, zero C dependencies) and requires maintaining a clear boundary between supported analytical SQL constructs and rejected OLTP SQL constructs (`src/compat.rs` / `error_code.rs`).
- Users accustomed to full PostgreSQL extensions (`pg_catalog` internals, `REGEXP_MATCHES`, custom DDL) will hit explicit unsupported-feature errors unless expectations are set clearly via `INFORMATION_SCHEMA` and actionable error diagnostics.
