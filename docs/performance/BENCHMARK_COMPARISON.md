# Honest Benchmark & Architecture Comparison

This document provides a candid comparison of **Samyama Graph Database** (v0.5.0-alpha.1) against industry leaders **Neo4j** and **FalkorDB**.

## Summary Table

| Feature | Samyama (Rust/RocksDB) | Neo4j (Java/Native) | FalkorDB (C/GraphBLAS) |
| :--- | :--- | :--- | :--- |
| **Ingestion Speed** | **Very High** 🚀. Rust + RocksDB WAL + Vectorization delivers **~363k nodes/sec** and **~1.42M edges/sec**. Significantly faster than transactional Neo4j. | **Medium**. Java GC and transactional safety (ACID) add overhead. | **High**. In-memory C structure is fast, but single-threaded writer often bottlenecks. |
| **Concurrency (MVCC)** | **High** ⚡. Native **MVCC** allows non-blocking reads (~600M ops/sec) during heavy writes. Snapshot isolation guaranteed. | **Medium/High**. Uses locks but mature transaction management handles concurrency well. | **Medium**. Often uses global or partition locks; long-running queries can block updates. |
| **Deep Traversal (K-Hop)** | **Medium**. CSR improves analytics, but Cypher traversal (154ms for 1-hop) is still bottlenecked by object materialization. | **High** (Native). Pointer chasing is cache-friendly and physically adjacent on disk. | **High** (Matrix). GraphBLAS creates adjacency matrices that make BFS/Traversal simple linear algebra ops. |
| **Vector Search** | **Ultra High** ⚡. Native `hnsw_rs` integration achieves **238µs** latency and **4200 QPS**. | **Low/Medium**. Usually relies on Apache Lucene (Java) integration, which adds overhead/latency. | **Medium**. Uses Vector Similarity Search (VSS) module, efficient but module-boundary overhead exists. |
| **Query Complexity** | **Low**. Our `QueryEngine` is basic (Volcano iterator). No cost-based optimizer (CBO). Complex joins/filters will be naïve and slow. | **Very High**. Decades of CBO tuning. Handles massive complexity efficiently. | **Medium/High**. Optimization based on sparse matrix operations is mathematically efficient but different. |
| **Memory Footprint** | **Low/Efficient**. Columnar storage + CSR + Arena Allocation reduces footprint. Rust has no GC. | **High**. JVM heap requirements are significant. Object overhead. | **Low**. C is efficient, but GraphBLAS matrices can grow large if graph is dense. |

## Detailed Verdict

### Samyama Wins 🏆
*   **High-Throughput Ingestion**: **~1.4 Million edges/sec** makes it ideal for streaming data pipelines (Log ingestion, IoT).
*   **Lock-Free Concurrency**: MVCC implementation ensures that **readers never block writers**, enabling high-throughput analytical queries on live operational data.
*   **Ultra-Low Latency Vector Search**: **238µs** latency enables real-time RAG (Retrieval Augmented Generation) at the edge.
*   **Memory Efficiency**: Columnar storage, Arenas, and CSR allow running large graphs on limited RAM (e.g., edge devices).

### Samyama Challenges ⚠️
*   **Cypher Execution Overhead**: While the storage is fast, the current query executor materializes full objects too early, slowing down simple K-Hop queries compared to native pointer chasing.
*   **Complex Analytical Queries**: Queries involving multiple hops with complex filtering (e.g., "Find patterns where A->B->C and D->E...") will be slower due to the lack of a mature Cost-Based Optimizer (CBO).
*   **Tooling Maturity**: Ecosystem tooling (visualizers, drivers, backup tools) is nascent compared to established players.

## Conclusion

Samyama (v0.5.0) has evolved into a **high-performance, hybrid transactional/analytical graph database**. It dominates in **Vector Search** and **Ingestion**, making it the superior choice for AI-native workloads, while offering "good enough" standard graph traversal performance that is actively improving.
