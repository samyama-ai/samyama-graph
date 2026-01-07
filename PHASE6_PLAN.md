# Phase 6: Vector Search & Graph RAG - Implementation Plan

## Executive Summary

**Goal**: Transform Samyama into a **Graph Vector Database** to support AI/LLM workloads (Graph RAG).

**Value Proposition**: Standard vector databases (Pinecone, Milvus) lack structural context. Standard graph databases lack semantic search. By combining both, we enable queries like *"Find legal precedents semantically similar to this case (Vector) that were cited by Judge Smith (Graph)."*

**Technology Strategy**:
- **Index Algorithm**: HNSW (Hierarchical Navigable Small World) via `hnsw_rs` (Pure Rust, high performance).
- **Distance Metrics**: Cosine Similarity (default), Euclidean, Dot Product.
- **Integration**: Vector indices managed alongside RocksDB column families.

## Requirements Coverage

| Requirement | Description |
|-------------|-------------|
| **REQ-VEC-001** | Support `Vector` (array of floats) as a native property type |
| **REQ-VEC-002** | Create/Delete vector indices on specific node labels and properties |
| **REQ-VEC-003** | Approximate Nearest Neighbor (ANN) search with HNSW |
| **REQ-VEC-004** | Cypher `CALL` procedure for vector search |
| **REQ-VEC-005** | Hybrid Query Execution (Vector Score + Graph Filters) |
| **REQ-VEC-006** | Persistence of vector indices (Snapshots/WAL) |

## Module Structure

```
src/
├── vector/                     # NEW Module
│   ├── mod.rs
│   ├── index.rs                # HNSW Index Wrapper
│   ├── manager.rs              # Manages indices for different labels/properties
│   ├── storage.rs              # Serialization/Persistence of indices
│   └── metric.rs               # Distance calculations (Cosine, L2)
├── graph/
│   ├── property.rs             # Update: Add PropertyValue::Vector(Vec<f32>)
│   └── ...
├── query/
│   ├── procedures.rs           # NEW: Handle CALL vector.search(...)
│   └── ...
```

## Implementation Roadmap (6 Weeks)

### Week 1: Core Vector Types & Property Support
**Goal**: Store and retrieve vectors on nodes.

1.  **Dependencies**: Add `hnsw_rs`, `rand`.
2.  **Data Model**: Update `PropertyValue` enum:
    ```rust
    pub enum PropertyValue {
        // ... existing
        Vector(Vec<f32>), // New
    }
    ```
3.  **Storage**: Update RocksDB serialization (`bincode`) to handle the new variant.
4.  **API**: Add `node.set_vector("embedding", vec![0.1, 0.2, ...])`.

### Week 2: HNSW Index Integration
**Goal**: Build the in-memory index structure.

1.  **Index Wrapper**: Implement `VectorIndex` struct wrapping `hnsw_rs::Hnsw`.
2.  **Index Management**: Create `VectorIndexManager` in `GraphStore`.
    *   Map `(Label, PropertyKey)` -> `VectorIndex`.
3.  **Synchronization**: Ensure when a Node is updated/deleted, the vector index is updated.

### Week 3: Persistence
**Goal**: Save/Load indices to disk.

1.  **Serialization**: HNSW indices are complex graph structures. Implement serialization to disk (separate files from RocksDB, e.g., `vectors/person_bio.idx`).
2.  **Startup**: Load indices into memory on server start.
3.  **Rebuild**: Tooling to rebuild index from RocksDB data if index file is corrupted.

### Week 4: Cypher Query Extension (Procedures)
**Goal**: Expose search to the user.

1.  **Syntax Extension**: Support `CALL` procedures in parser (if not already fully supported).
    ```cypher
    CALL db.index.vector.queryNodes('Person', 'bio_embedding', $query_vector, 10)
    YIELD node, score
    RETURN node.name, score
    ```
2.  **Procedure Registry**: Implement a registry for system procedures.
3.  **Execution**: Implement the physical operator for procedures.

### Week 5: Hybrid Query Execution (Graph RAG)
**Goal**: Combine Vector + Graph.

1.  **Composition**: Allow vector results to be fed into `MATCH`.
    ```cypher
    CALL db.index.vector.queryNodes('Paper', 'embedding', [...], 5) YIELD node, score
    MATCH (node)-[:AUTHORED_BY]->(author)
    RETURN node.title, author.name, score
    ```
2.  **Filtering**: Apply pre-filtering (filtering results *before* HNSW search - harder) or post-filtering (filtering *after* search - easier). Start with post-filtering.

### Week 6: Testing & Benchmarking
**Goal**: Verify accuracy and performance.

1.  **Recall Testing**: Verify HNSW returns correct nearest neighbors compared to brute force.
2.  **Performance**: Benchmark latency on 100k+ vectors (1536 dimensions - OpenAI standard).
3.  **Integration Tests**: End-to-end RAG flows.

## API Design

### Rust API
```rust
// Create an index
store.create_vector_index(
    "Person",       // Label
    "embedding",    // Property
    1536,           // Dimensions
    Distance::Cosine
)?;

// Search
let results = store.vector_search(
    "Person",
    "embedding",
    &query_vector,
    10 // Limit
)?; // Returns Vec<(NodeId, f32)>
```

### Cypher API
```cypher
// Create index (DDL)
CREATE VECTOR INDEX person_embed 
FOR (n:Person) ON (n.embedding)
OPTIONS { dimensions: 1536, similarity: 'cosine' }

// Query
WITH [0.1, 0.2, ...] AS query
CALL db.index.vector.queryNodes('Person', 'embedding', query, 10) 
YIELD node, score
WHERE score > 0.9
RETURN node.name
```

## Risks & Mitigation
1.  **Memory Usage**: HNSW is memory-hungry.
    *   *Mitigation*: Implement index sharding or use PQ (Product Quantization) compression features of `hnsw_rs` if available, or allow swapping to disk.
2.  **Write Amplification**: Updating vectors is expensive.
    *   *Mitigation*: Batch updates.
3.  **Dimension Mismatch**: User inserts 1536d vector into 768d index.
    *   *Mitigation*: Strict validation on write.

## Team & Resources
- **Engineers**: 1 Backend / Systems Engineer (Rust).
- **Duration**: 6 Weeks.
- **Dependencies**: `hnsw_rs` (v0.2+).

---
**Status**: Planned
**Version**: 1.0
