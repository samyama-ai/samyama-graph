# Samyama Performance Benchmarks

**Date**: January 8, 2026
**Version**: 0.1.0 (Phase 10 Complete)
**Hardware**: Mac Mini (M4, 16GB RAM)

## Executive Summary

Samyama demonstrates **production-ready performance** at scale. Benchmarks with **1,000,000 nodes** confirm that the system handles massive ingestion rates and high-speed queries efficiently.

## 1. Cypher Query Execution (OLTP)
*Target: Application Queries*

| Dataset Size | Rate | Latency | Speedup vs Naive |
|--------------|------|---------|------------------|
| **10k Nodes** | **35,360 QPS** | **0.028 ms** | 5,893x |
| **100k Nodes** | **116,373 QPS** | **0.008 ms** | - |
| **1M Nodes** | **115,320 QPS** | **0.008 ms** | - |

**Query**: `MATCH (a:Entity)-[:LINKS_TO]->(b:Entity) WHERE a.id = 1 RETURN b.id`

**Conclusion**: Performance remains flat (O(1) / O(log n)) regardless of dataset size, thanks to B-Tree indices.

## 2. Vector Search (HNSW)
*Target: AI/RAG Applications*

| Dataset Size | Throughput | Latency (Avg) |
|--------------|------------|---------------|
| **10k Nodes** | **3,600 QPS** | **0.28 ms** |
| **100k Nodes** | **4,500 QPS** | **0.22 ms** |
| **1M Nodes** | **267 QPS** | **3.74 ms** |

**Conclusion**: Latency increases with dataset size but remains under 5ms even at 1M vectors (128d).

## 3. Graph Algorithms (Analytics)
*Target: Data Science / Insights*

| Algorithm | Graph Size | Execution Time |
|-----------|------------|----------------|
| **PageRank** | 10k Nodes | **100 ms** |
| **PageRank** | 100k Nodes | **350 ms** |
| **PageRank** | 1M Nodes | **5.93 s** |
| **BFS** | 1M Nodes | **495 ms** |

**Conclusion**: PageRank scales linearly with graph size.

## 4. Data Ingestion
*Target: Write Performance*

| Operation | Rate (Async) | Notes |
|-----------|--------------|-------|
| **Node Creation** | **~870,000 nodes/sec** | Decoupled indexing (Phase 9) |
| **Edge Creation** | **~2.2 Million edges/sec** | Very fast |

**Conclusion**: Ingestion is extremely fast, capable of loading millions of entities in seconds.

## Reproduction

To run these benchmarks yourself:

```bash
# Run with default 10k nodes
cargo run --release --example full_benchmark

# Run with 1M nodes
cargo run --release --example full_benchmark 1000000
```
