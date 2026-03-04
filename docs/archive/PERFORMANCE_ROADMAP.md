# Performance Roadmap: Making Samyama the World's Fastest Graph Database

This document outlines the strategic technical initiatives to optimize Samyama Graph Database for industry-leading throughput and sub-millisecond latency.

## 1. Memory Layout & Data Structures

### [DONE] Compressed Sparse Row (CSR) Implementation
- **Goal**: Move from `HashMap` lookups to dense integer arrays for graph topology.
- **Benefit**: Achieves superior CPU cache locality and enables pre-fetching. Can be 10x-50x faster for traversals.
- **Strategy**: Use a hybrid model. Maintain a mutable "Delta" store (HashMap/BTree) for recent writes and a dense "Stable" store (CSR). Periodically merge Delta into CSR.

### [DONE] Arena Allocation
- **Goal**: Allocate Node and Edge objects in contiguous memory blocks (Arenas).
- **Benefit**: Reduces heap fragmentation and allocator overhead. Node IDs become direct indices into these arrays.

## 2. Execution Model

### [DONE] Vectorized Execution (Batching)
- **Goal**: Transition from the Volcano (row-at-a-time) iterator to a batch-based iterator (e.g., 1024 rows per `next_batch()`).
- **Benefit**: Amortizes function call overhead and enables SIMD (Single Instruction, Multiple Data) optimizations.

### [DONE] Late Materialization
- **Goal**: Pass lightweight `NodeRef(NodeId)` / `EdgeRef(EdgeId)` through the execution pipeline instead of cloning full objects. Materialize only at projection.
- **Benefit**: Eliminates unnecessary Node/Edge cloning during traversal. Storage-level 3-hop traversal dropped from 17µs to 15µs (14%). Cypher 1-hop dropped from 164ms to 41ms (4x improvement) by avoiding per-step materialization.
- **Implementation**: `Value::NodeRef`, `Value::EdgeRef` variants; `resolve_property()` for lazy property access; `get_outgoing_edge_targets()` returning `(EdgeId, NodeId, NodeId, &EdgeType)` tuples.

### [IN PROGRESS] Query AST Cache
- **Goal**: Cache parsed Query ASTs keyed by whitespace-normalized query strings. Eliminate the ~20-25ms parse overhead for repeated queries.
- **Benefit**: Reduces warm-cache Cypher 1-hop from ~41ms to ~16-20ms (~50% reduction). Critical for RESP server, HTTP handler, and benchmark workloads where queries repeat.
- **Strategy**: `HashMap<String, Query>` with Mutex in `QueryEngine`. Simple size cap (1024 entries) with full eviction. Plan caching deferred — plans depend on mutable GraphStore state.

### Query Compilation (JIT)
- **Goal**: Use LLVM or Cranelift to compile Cypher ASTs directly into machine code at runtime.
- **Benefit**: Eliminates interpreter overhead. Logic like `WHERE n.age > 30` becomes a raw CPU comparison.

## 3. Storage I/O

### [DONE] Columnar Property Storage
- **Goal**: Adopt an Apache Arrow-style columnar layout for node/edge properties.
- **Benefit**: Queries that access specific properties (e.g., `RETURN n.price`) only read relevant data from disk/memory, avoiding "cache pollution" from unused fields.

### [DEFERRED] IO_URING Integration
- **Goal**: Utilize the Linux `io_uring` interface for asynchronous disk I/O.
- **Benefit**: Allows the storage engine to submit thousands of I/O requests in a single system call, maximizing SSD throughput.
- **Status**: Deferred (Linux-only, developer environment is macOS).

## 4. Concurrency & Throughput

### [IN PROGRESS] Multi-Version Concurrency Control (MVCC)
- **Goal**: Implement versioning for all graph elements.
- **Benefit**: **Reads never block writes, and writes never block reads.** Essential for high-concurrency enterprise workloads.

### Lock-Free Data Structures
- **Goal**: Replace global or heavy locks with lock-free primitives (using `Crossbeam` or `DashMap`) and shard-level locking.

## 5. Advanced Optimization

### Worst-Case Optimal (WCO) Joins
- **Goal**: Implement join algorithms like Leapfrog Trie Join.
- **Benefit**: Guarantees optimal performance for complex graph patterns (Triangles, Cliques) that traditional binary join trees handle poorly.

---

## Performance Targets
- **Ingestion**: > 2 Million nodes/sec.
- **Traversal**: > 100 Million edges/sec per core.
- **Latency**: < 1ms p99 for 3-hop neighborhood queries.
