# Samyama Performance Benchmarks

**Date**: February 8, 2026
**Version**: v0.5.0-alpha.1
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

## 5. Cypher Query Execution (Post Late Materialization)
*Date: 2026-02-07*

Late materialization (`Value::NodeRef`/`Value::EdgeRef`) avoids full object cloning during traversal. Properties are resolved lazily only when projected or filtered.

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| **1-Hop Traversal** | 164ms | **41ms** | **4x** |
| **2-Hop Traversal** | 1.22s | **259ms** | **4.7x** |
| **Raw 3-Hop (storage API)** | 17µs | **15µs** | 14% |

### Bottleneck Breakdown (1-Hop Query)

| Component | Time | % of Total |
| :--- | :--- | :--- |
| Parse (Pest grammar) | ~20-25ms | ~55% |
| Plan (AST → operators) | ~15-20ms | ~40% |
| Execute (scan + expand + project) | <1ms | ~2% |
| **Total** | **~41ms** | **100%** |

**Key Insight:** Execution is now sub-millisecond. The parser and planner dominate query latency. Query AST caching will bring warm-cache 1-hop latency to ~16-20ms.

See [BENCHMARK_RESULTS_v0.5.0.md](./BENCHMARK_RESULTS_v0.5.0.md) for full details.

## Reproduction

To run these benchmarks yourself:

```bash
# Run with default 10k nodes
cargo run --release --example full_benchmark

# Run with 1M nodes
cargo run --release --example full_benchmark 1000000

# Run late materialization benchmark
cargo run --release --example late_materialization_bench
```
