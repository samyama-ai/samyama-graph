# Benchmark Results - v0.5.0-alpha.1

**Date:** 2026-01-31
**Release:** v0.5.0-alpha.1 (CSR, Vectorized Execution, Columnar Storage)

## Environment
- **OS:** Darwin (macOS)
- **Mode:** Release

## Results

### 1. Ingestion Throughput
- **Node Ingestion:** ~359,426 nodes/sec
- **Edge Ingestion:** ~1,511,803 edges/sec

### 2. Vector Search (HNSW)
- **Latency (Avg):** 1.33ms
- **Throughput:** 752 QPS
- **Dimensions:** 64
- **Dataset:** 10,000 vectors

### 3. Graph Traversal (K-Hop via Cypher)
- **1-Hop Latency:** 164.11ms
- **2-Hop Latency:** 1.22s

## Analysis
- **Strengths:** Edge ingestion is high (>1.5M/sec), and vector search is performant.
- **Weaknesses:** K-Hop traversal via Cypher is slower than expected for an in-memory graph.
- **Bottleneck:** The `ExpandOperator` currently materializes full `Node` objects (including all properties) for every step of the traversal.
- **Next Steps:** Implement **Late Materialization**. Pass only `NodeId`s through the execution pipeline and hydrate properties lazily only when projected or filtered.

---

## Post Late Materialization Results

**Date:** 2026-02-07
**Changes:** NodeRef/EdgeRef late materialization, lightweight `get_outgoing_edge_targets()` API

### Updated Benchmark Numbers

#### Ingestion (unchanged)
- **Node Ingestion:** ~230K nodes/sec
- **Edge Ingestion:** ~1.1M edges/sec

#### Raw Storage Traversal (`late_materialization_bench`)
- **3-Hop Traversal (lightweight API):** 15µs
- **3-Hop Traversal (old clone API):** 17µs
- **Improvement:** 14% faster at storage level

#### Cypher Query Execution
- **1-Hop Latency:** 41ms (was 164ms — **4x improvement**)
- **2-Hop Latency:** 259ms (was 1.22s — **4.7x improvement**)

#### Materialization Cost
- **RETURN n (materialize 1000 nodes):** 1.96ms
- **RETURN n.name (property only):** 1.58ms
- **Saving:** 19% — late materialization avoids full node hydration when only properties are needed

### Bottleneck Analysis

| Component | Time | % of 1-Hop |
| :--- | :--- | :--- |
| Parse (Pest grammar) | ~20-25ms | ~55% |
| Plan (AST → operators) | ~15-20ms | ~40% |
| Execute (scan + expand + project) | <1ms | ~2% |
| **Total** | **~41ms** | **100%** |

**Key Insight:** Execution is now sub-millisecond. The parser and planner dominate query latency. Query AST caching will eliminate the parse overhead (~20-25ms) for repeated queries, bringing warm-cache 1-hop latency to ~16-20ms.

### Next Steps
- **Query AST Cache:** Cache parsed ASTs keyed by normalized query string (in progress)
- **Plan Cache:** Future work — requires decoupling plan from GraphStore references
- **Query Compilation (JIT):** Long-term — compile hot queries to native code
