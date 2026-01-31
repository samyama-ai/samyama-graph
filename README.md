# Samyama Graph Database

**Samyama** is a high-performance, distributed, AI-native Graph Vector Database written in **Rust**.

It bridges the gap between **Transactional Graph Databases**, **Vector Databases**, and **Graph Analytics Engines** by providing a single, unified engine that does it all.

![Samyama Analytics Visualization](./visualization.svg)

## 🚀 Key Features

*   **⚡ Speed**: 115,000+ Queries Per Second (QPS) for lookups; 800,000+ Nodes/sec ingestion.
*   **🧠 Vector Search**: Built-in HNSW indexing for millisecond-speed Semantic Search.
*   **🤖 Auto-Embed**: Automatically generate vector embeddings from text properties using LLMs.
*   **🗣️ NLQ**: Query your graph using plain English ("Who knows Alice?") instead of Cypher code.
*   **🕵️ Agents**: Autonomous agents that can enrich your graph data by calling external tools (e.g., Web Search).
*   **🕸️ Graph RAG**: Combine vector similarity ("Find nodes meaning X") with graph structure ("...connected to Y").
*   **📊 Analytics**: Native PageRank, BFS, Dijkstra, WCC, SCC, MaxFlow, MST, and Triangle Counting algorithms.
*   **🎯 Optimization**: Built-in metaheuristic solvers (15+ algorithms including Jaya, Rao, GWO, Firefly, Cuckoo, ABC, NSGA-II) for single and multi-objective resource allocation directly on the graph.
*   **🛡️ Reliability**: Raft Consensus for High Availability and RocksDB for persistence.
*   **⚖️ Scalability**: Native Tenant-Level Sharding for horizontal scaling.
*   **🎨 Visualization**: Built-in interactive Web UI.

---

## 🏁 Getting Started

### 1. Installation & Build

Samyama is distributed as a single binary. The Web Visualizer is **embedded** into this binary during compilation.

```bash
# Clone repository
git clone https://gitlab.com/samyama-ai/samyama-graph
cd samyama-graph

# Build release binary (This compiles the Rust code AND embeds the Web UI)
cargo build --release
```

### 2. Run the Server

Start the database server. This launches both the **RESP Protocol Server** (port 6379) and the **Web Visualizer** (port 8080).

```bash
./target/release/samyama
```

You should see:
```text
Samyama Graph Database v0.1.0
...
RESP server listening on 127.0.0.1:6379
Visualizer available at: http://localhost:8080
```

### 3. Use the Web Visualizer

Open **[http://localhost:8080](http://localhost:8080)** in your browser.

*   **Explore**: Run Cypher queries interactively.
*   **Visualize**: See your data as a force-directed node-link graph.
*   **Inspect**: Click nodes to view properties and vector embeddings.

### 4. Connect via CLI

You can use any standard Redis client (like `redis-cli`) to talk to Samyama.

```bash
redis-cli -p 6379

# Create a node
127.0.0.1:6379> GRAPH.QUERY mygraph "CREATE (n:Person {name: 'Alice', age: 30})"

# Query the graph
127.0.0.1:6379> GRAPH.QUERY mygraph "MATCH (n:Person) RETURN n"
```

---

## 🧪 Running Examples

We provide several fully functional examples to demonstrate different capabilities.

### 1. Banking / Fraud Detection Demo
Simulates a banking system with accounts, transactions, and fraud detection patterns.

```bash
cargo run --release --example banking_demo
```
*   **What it does**: Generates synthetic accounts/transactions, builds a graph, and runs queries to find circular money movement (potential money laundering).

### 2. Graph RAG (AI) Demo
Demonstrates how to combine Vector Search with Graph queries.

```bash
cargo run --release --example graph_rag_demo
```
*   **What it does**: Creates documents with 128d vector embeddings. Runs a hybrid query: *"Find documents semantically similar to this vector that were written by 'Alice'"*.

### 3. Distributed Cluster Demo
Simulates a 3-node Raft cluster with automatic failover.

```bash
cargo run --release --example cluster_demo
```
*   **What it does**: Spins up 3 in-memory Raft nodes, elects a leader, replicates writes, and simulates a node crash/recovery to prove high availability.

### 4. High-Scale Benchmark
Pushes the system to its limits (1M+ nodes).

```bash
# Run with default 10k nodes
cargo run --release --example full_benchmark

# Run with 1 Million nodes (Requires ~8GB RAM)
cargo run --release --example full_benchmark 1000000
```

### 5. Auto-Embed Demo
Demonstrates automatic embedding generation.

```bash
export GEMINI_API_KEY="your_key"
cargo run --release --example auto_embed_demo
```
*   **What it does**: Creates a document node with text. The system automatically calls Gemini to generate an embedding and indexes it for vector search.

### 6. NLQ (Natural Language Query) Demo
Query the database using English.

```bash
export GEMINI_API_KEY="your_key"
cargo run --release --example nlq_demo
```
*   **What it does**: Translates "Who knows Alice?" into a valid Cypher query using an LLM and executes it.

### 7. Agentic Enrichment Demo
Demonstrates an autonomous agent enriching data.

```bash
cargo run --release --example agent_demo
```
*   **What it does**: Simulates a new company node being created. An agent automatically wakes up, searches the web for info (mocked), and updates the node.

---

## 📚 Advanced Usage

### Vector Search (AI Integration)

Samyama supports `Vector` as a first-class property type.

1.  **Create Index**:
    ```cypher
    CREATE VECTOR INDEX doc_idx FOR (n:Doc) ON (n.embedding) 
    OPTIONS {dimensions: 1536, similarity: 'cosine'}
    ```

2.  **Insert Data**:
    ```cypher
    CREATE (n:Doc {content: "Hello", embedding: [0.1, 0.9, ...]})
    ```

3.  **Query**:
    ```cypher
    CALL db.index.vector.queryNodes('Doc', 'embedding', $query_vector, 5) 
    YIELD node, score
    RETURN node.content, score
    ```

### Graph Algorithms

Run analytics directly on your data without exporting it.

```cypher
// Calculate PageRank
CALL algo.pageRank('Person', 'KNOWS') 
YIELD node, score 
RETURN node.name, score 
ORDER BY score DESC LIMIT 10

// Find Shortest Path (BFS)
CALL algo.shortestPath($start_id, $end_id) YIELD path, cost
```

### In-Database Optimization

Use the graph as your optimization model. Automatically adjust node properties to minimize/maximize an objective.

```cypher
// Optimize Factory production to minimize cost
CALL algo.or.solve({
  algorithm: 'Jaya',
  label: 'Factory',
  property: 'production_rate',
  min: 10.0,
  max: 100.0,
  cost_property: 'unit_cost',
  budget: 50000.0
}) 
YIELD fitness, algorithm
```

---

## 🛠 Architecture

Samyama is built on a modern Rust stack:

*   **Storage**: **RocksDB** (LSM-Tree) with custom serialization (bincode).
*   **Consensus**: **Raft** (via `openraft`) for consistency and replication.
*   **Indexing**: 
    *   **HNSW** (`hnsw_rs`) for Vectors.
    *   **B-Tree** (`BTreeMap`) for Properties.
*   **Query Engine**: **Volcano Iterator Model** with a Cost-Based Optimizer.
*   **Networking**: **Tokio** (Async I/O) and **Axum** (HTTP).

## 📖 Documentation

For developers and contributors:
- [Benchmarks](./docs/BENCHMARKS.md)
- [Requirements](./docs/REQUIREMENTS.md)
- [Architecture](./docs/ARCHITECTURE.md)
- [Roadmap](./ROADMAP.md)

## License

Apache License 2.0