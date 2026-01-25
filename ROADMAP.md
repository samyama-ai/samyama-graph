# Samyama Graph Database Roadmap

This document outlines the development journey of Samyama, from its inception as a property graph engine to its current state as a distributed, AI-native Graph Vector Database.

---

## ✅ Completed Phases

### Phase 1: Core Property Graph Engine
**Goal**: Build the fundamental data structures for nodes, edges, and properties.
*   **Features**:
    *   In-memory `GraphStore` using HashMaps.
    *   Support for multiple labels and property types (String, Int, Float, Bool, etc.).
    *   Adjacency lists for O(1) traversal lookups.

### Phase 2: Query Engine & RESP Protocol
**Goal**: Enable interaction via standard tools.
*   **Features**:
    *   **OpenCypher Parser**: `MATCH`, `WHERE`, `RETURN`, `CREATE`, `ORDER BY`, `LIMIT`.
    *   **Volcano Executor**: Iterator-based query execution pipeline.
    *   **RESP Server**: Compatibility with Redis clients (`redis-cli`, Python/JS drivers).

### Phase 3: Persistence & Multi-Tenancy
**Goal**: Enterprise-grade durability and isolation.
*   **Features**:
    *   **RocksDB Storage**: Persistent storage with column families for Nodes/Edges/Indices.
    *   **WAL (Write-Ahead Log)**: Crash recovery and durability.
    *   **Multi-Tenancy**: Logical namespace isolation with resource quotas.

### Phase 4: High Availability (Raft)
**Goal**: Distributed consensus and failover.
*   **Features**:
    *   **Raft Consensus**: Leader election, log replication, and quorum safety via `openraft`.
    *   **Cluster Management**: Dynamic membership changes (add/remove nodes).

### Phase 5: RDF & Semantic Web
**Goal**: Interoperability with knowledge graphs.
*   **Features**:
    *   **Triple Store**: RDF data model support.
    *   **Serialization**: Turtle, N-Triples, RDF/XML support.

### Phase 6: Vector Search & AI Integration
**Goal**: Native AI support for RAG applications.
*   **Features**:
    *   **Vector Type**: Native `Vec<f32>` property support.
    *   **HNSW Indexing**: High-performance Approximate Nearest Neighbor search.
    *   **Graph RAG**: Hybrid queries combining vector similarity + graph traversal.
    *   **Cypher**: `CALL db.index.vector.queryNodes(...)`.

### Phase 7: Native Graph Algorithms
**Goal**: In-database analytics.
*   **Features**:
    *   **PageRank**: Node centrality scoring.
    *   **BFS/Dijkstra**: Shortest path algorithms.
    *   **WCC**: Community detection.
    *   **GraphView**: Optimized CSR-like projection for analytics speed.

### Phase 8: Query Optimization
**Goal**: Solve performance bottlenecks.
*   **Features**:
    *   **B-Tree Indices**: O(log n) property lookups.
    *   **Cost-Based Optimizer (CBO)**: Automatically selects indices over scans.
    *   **Performance**: Improved lookup speed by **5,800x** (115k QPS).

### Phase 9: Async Ingestion
**Goal**: Maximize write throughput.
*   **Features**:
    *   **Decoupled Architecture**: Writes are acked immediately; indexing happens in background.
    *   **Performance**: Restored ingestion to **>800k nodes/sec**.

### Phase 10: Tenant Sharding
**Goal**: Horizontal scalability.
*   **Features**:
    *   **Request Router**: Distributes tenants across different Raft groups.
    *   **Proxy Layer**: Forwards requests to correct shards transparently.

### Phase 11: Native Visualizer
**Goal**: Developer Experience.
*   **Features**:
    *   **Embedded Web UI**: Served directly from binary at port 8080.
    *   **Force-Directed Graph**: Interactive visualization.
    *   **Query Workbench**: Run Cypher directly in the browser.

### Phase 12: "Auto-RAG" Pipelines 🤖
**Goal**: Native AI support for automatic data processing.
*   **Features**:
    *   **Tenant-Level Config**: Each tenant can have its own LLM provider and embedding policy.
    *   **Externalized LLMs**: Support for OpenAI, Ollama, and Gemini.
    *   **Automatic Embedding**: Background tasks automatically generate embeddings when text properties matching policies are updated.
    *   **Native Integration**: Built directly into the async indexing pipeline.

---

## 🔮 Future Roadmap

### 1. Time-Travel / Temporal Queries ⏳

### 3. Graph-Level Sharding
**Goal**: Massive scale for single graphs.
*   **Plan**: Partition *single* large graphs across nodes using Min-Cut algorithms (Metis), enabling trillion-edge scale (complexity: High).
