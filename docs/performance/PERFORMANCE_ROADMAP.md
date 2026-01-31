# Performance Roadmap: Making Samyama the World's Fastest Graph Database

This document outlines the strategic technical initiatives to optimize Samyama Graph Database for industry-leading throughput and sub-millisecond latency.

## 1. Memory Layout & Data Structures

### [DONE] Compressed Sparse Row (CSR) Implementation
- **Goal**: Move from `HashMap` lookups to dense integer arrays for graph topology.
- **Benefit**: Achieves superior CPU cache locality and enables pre-fetching. Can be 10x-50x faster for traversals.
- **Strategy**: Use a hybrid model. Maintain a mutable "Delta" store (HashMap/BTree) for recent writes and a dense "Stable" store (CSR). Periodically merge Delta into CSR.

### Arena Allocation
- **Goal**: Allocate Node and Edge objects in contiguous memory blocks (Arenas).
- **Benefit**: Reduces heap fragmentation and allocator overhead. Node IDs become direct indices into these arrays.

## 2. Execution Model

### [DONE] Vectorized Execution (Batching)
- **Goal**: Transition from the Volcano (row-at-a-time) iterator to a batch-based iterator (e.g., 1024 rows per `next_batch()`).
- **Benefit**: Amortizes function call overhead and enables SIMD (Single Instruction, Multiple Data) optimizations.

### Query Compilation (JIT)
- **Goal**: Use LLVM or Cranelift to compile Cypher ASTs directly into machine code at runtime.
- **Benefit**: Eliminates interpreter overhead. Logic like `WHERE n.age > 30` becomes a raw CPU comparison.

## 3. Storage I/O

### [DONE] Columnar Property Storage
- **Goal**: Adopt an Apache Arrow-style columnar layout for node/edge properties.
- **Benefit**: Queries that access specific properties (e.g., `RETURN n.price`) only read relevant data from disk/memory, avoiding "cache pollution" from unused fields.

### IO_URING Integration
- **Goal**: Utilize the Linux `io_uring` interface for asynchronous disk I/O.
- **Benefit**: Allows the storage engine to submit thousands of I/O requests in a single system call, maximizing SSD throughput.

## 4. Concurrency & Throughput

### Multi-Version Concurrency Control (MVCC)
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
