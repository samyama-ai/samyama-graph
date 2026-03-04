# Performance Optimization Progress

## 2026-02-07: Phase 3 - Late Materialization

### Achievements
- **Late Materialization (NodeRef/EdgeRef)**:
    - Scan operators now produce `Value::NodeRef(NodeId)` instead of `Value::Node(id, node.clone())`.
    - `ExpandOperator` uses lightweight `get_outgoing_edge_targets()` returning `(EdgeId, NodeId, NodeId, &EdgeType)` tuples — no Edge clone.
    - Property access via `Value::resolve_property(prop, store)` — checks columnar store first, falls back to node/edge lookup.
    - Full materialization only at `ProjectOperator` for `RETURN n` (whole-variable expressions).
- **Benchmark Results**:
    - **1-Hop Traversal**: 41ms (was 164ms — **4x improvement**)
    - **2-Hop Traversal**: 259ms (was 1.22s — **4.7x improvement**)
    - **Raw 3-Hop (storage API)**: 15µs
    - **Execution phase**: Sub-millisecond (<1ms). Parser and planner now dominate latency (~95%).

### Next Steps
- **Query AST Cache**: Cache parsed ASTs keyed by normalized query string to eliminate ~20-25ms parse overhead.
- **Plan Cache**: Decouple plan from GraphStore references for caching.
- **JIT Query Compilation**: Long-term — compile hot queries to native code.

---

## 2026-01-31: Phase 2 - Advanced Memory & Concurrency

### Achievements
- **Memory Layout (Arena Allocation)**: 
    - Migrated core `GraphStore` from `HashMap<Id, Object>` to `Vec<Vec<Version>>` (Arena-style with versioning).
    - Implemented a **Free-List** for `NodeId` and `EdgeId` to enable efficient ID reuse after deletions.
    - Improved cache locality by using dense integer arrays for adjacency lists (`Vec<Vec<EdgeId>>`).
- **Concurrency (MVCC Foundation)**:
    - Added `version` field to `Node` and `Edge` structs.
    - Refactored `GraphStore` to support **Multi-Version Concurrency Control (MVCC)**. 
    - Readers now access consistent snapshots via `get_node_at_version`.
- **Query Engine Optimization**:
    - Refactored property evaluation to prioritize **Columnar Storage**, significantly reducing the need to hydrate full `Node` objects during traversals and filters.
- **Benchmark Results**:
    - **Edge Ingestion**: ~1.4M edges/sec.
    - **1-Hop Traversal**: 154ms (down from ~200ms).
    - **Vector Search**: ~790µs.

### Next Steps
- **JIT Query Compilation**: Explore LLVM integration for raw performance.
- **Lock-Free Data Structures**: Transition from heavy locks to lock-free primitives for the primary store.
- **Tombstone Cleanup**: Implement a background vacuum/compaction process for old MVCC versions and deleted slots.
