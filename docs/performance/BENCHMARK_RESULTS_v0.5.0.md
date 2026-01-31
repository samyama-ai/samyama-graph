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
