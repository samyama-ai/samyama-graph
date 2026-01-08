# Samyama Graph Database

A high-performance, distributed graph database with OpenCypher query support, Redis protocol compatibility, multi-tenancy, **Vector Search (Graph RAG)**, and **Native Graph Algorithms**.

## Status: Phase 7 - Graph Analytics ✅

### Implemented Requirements

**Phase 1 - Property Graph Model**:
- ✅ **REQ-GRAPH-001**: Property graph data model (Nodes, Edges, Properties, Labels)
- ✅ **REQ-MEM-001**: In-memory storage with memory-optimized structures

**Phase 2 - Query Engine & RESP Protocol**:
- ✅ **REQ-CYPHER-001**: OpenCypher query language support
- ✅ **REQ-CYPHER-009**: Query optimization (Volcano iterator model)
- ✅ **REQ-REDIS-001**: RESP3 protocol implementation

**Phase 3 - Persistence & Multi-Tenancy**:
- ✅ **REQ-PERSIST-001**: RocksDB persistence with Write-Ahead Logging (WAL)
- ✅ **REQ-TENANT-001**: Multi-tenant namespace isolation

**Phase 4 - High Availability**:
- ✅ **REQ-HA-001**: Raft consensus protocol for replication and failover

**Phase 5 - RDF/SPARQL Support (Foundation)**:
- ✅ **REQ-RDF-001**: RDF triple/quad store with indexing
- ✅ **REQ-RDF-003**: RDF serialization (Turtle, N-Triples, RDF/XML, JSON-LD)

**Phase 6 - Vector Search & AI Integration**:
- ✅ **REQ-VEC-001**: Native `Vector` (Vec<f32>) property support
- ✅ **REQ-VEC-002**: HNSW Indexing for approximate nearest neighbor search
- ✅ **REQ-VEC-005**: Hybrid Graph RAG query execution

**Phase 7 - Native Graph Algorithms ✅ (New)**:
- ✅ **REQ-ALGO-001**: PageRank (Centrality)
- ✅ **REQ-ALGO-002**: Breadth-First Search (BFS)
- ✅ **REQ-ALGO-003**: Dijkstra's Algorithm (Weighted Shortest Path)
- ✅ **REQ-ALGO-004**: Weakly Connected Components (Community Detection)
- ✅ **REQ-ALGO-005**: Cypher `CALL algo.*` procedure integration

### Test Results

```
✓ 176 tests passed (171 unit + 5 integration)
✓ 0 tests failed
✓ Test coverage: Core + Query + RESP + Persistence + HA + RDF + Vector + Algo
```

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/VaidhyaMegha/samyama_graph.git
cd samyama_graph

# Build
cargo build --release

# Run demos
cargo run --example graph_rag_demo
cargo run --example vector_benchmark
```

### Usage Examples

#### 1. Graph RAG (Vector + Graph Hybrid)

```cypher
CALL db.index.vector.queryNodes('Document', 'embedding', $query_vector, 5) YIELD node, score
MATCH (author:Person)-[:WROTE]->(node)
RETURN node.title, author.name, score
```

#### 2. Graph Algorithms (Analytics)

```cypher
// Calculate PageRank to find influential nodes
CALL algo.pageRank('Person', 'KNOWS') 
YIELD node, score 
RETURN node.name, score 
ORDER BY score DESC LIMIT 5

// Find shortest path
CALL algo.shortestPath($startNodeId, $endNodeId) 
YIELD path, cost 
RETURN cost
```

## Architecture

### Module Structure

```
src/
├── graph/              # Property Graph (Node, Edge, PropertyValue)
├── vector/             # Vector Indexing (HNSW, Distance Metrics)
├── algo/               # Graph Algorithms (PageRank, Pathfinding, WCC)
├── query/              # Cypher Engine (Parser, Planner, Operators)
├── protocol/           # RESP Protocol (Redis compatibility)
├── persistence/        # Storage Engine (RocksDB, WAL)
├── raft/               # High Availability (Consensus)
├── rdf/                # RDF Support
└── sparql/             # SPARQL Support
```

---

**Version**: 0.1.0
**Status**: Phase 7 Complete - Graph Analytics Engine
**Last Updated**: 2026-01-08
