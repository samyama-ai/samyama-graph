# Samyama Performance Benchmarks

**Date**: January 8, 2026
**Version**: 0.1.0 (Phase 8 Complete)
**Hardware**: macOS / M1 Max (implied)

## Executive Summary

Samyama demonstrates **production-ready performance** for Vector Search, Graph Algorithms, and now **Cypher Queries**. The Phase 8 optimization (Property Indices) delivered a **1350x speedup** for lookups.

## 1. Cypher Query Execution (OLTP)
*Target: Application Queries*

| Query Type | Previous Rate | **Current Rate** | **Speedup** |
|------------|---------------|------------------|-------------|
| **Simple Lookup** | 6 QPS | **8,127 QPS** | **1354x** |
| **Latency** | 164.73 ms | **0.12 ms** | - |

**Query**: `MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = 1 RETURN b.id`

**Conclusion**: The Cost-Based Optimizer correctly selects the `IndexScanOperator`, eliminating the full table scan.

## 2. Vector Search (HNSW)
*Target: AI/RAG Applications*

| Metric | Result |
|--------|--------|
| **Throughput** | **1,074 - 3,500 QPS** |
| **Latency (Avg)** | **0.2 - 0.9 ms** |
| **Dataset** | 10,000 nodes, 128 dimensions |

## 3. Graph Algorithms (Analytics)
*Target: Data Science / Insights*

| Algorithm | Graph Size | Execution Time |
|-----------|------------|----------------|
| **PageRank** | 10k Nodes | **100 - 350 ms** |
| **BFS** | 10k Nodes | **5 - 15 ms** |

## 4. Data Ingestion
*Target: Write Performance*

| Operation | Rate | Notes |
|-----------|------|-------|
| **Node Creation** | **180 - 700 nodes/sec** | Writes to Graph + Vector Index + Property Index |
| **Edge Creation** | **~1.5 Million edges/sec** | Very fast |

**Conclusion**: Write throughput for nodes is the tradeoff for rich indexing. It is acceptable for most transactional workloads but bulk loading should bypass indexing if possible (future optimization).

## Reproduction

To run these benchmarks yourself:

```bash
cargo run --release --example full_benchmark
```