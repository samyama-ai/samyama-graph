# Samyama Graph Database

A high-performance, distributed graph database with OpenCypher query support, Redis protocol compatibility, multi-tenancy, and **Vector Search (Graph RAG)**.

## Status: Phase 6 - Vector Search & AI Integration ✅

### Implemented Requirements

**Phase 1 - Property Graph Model (REQ-GRAPH-001 through REQ-GRAPH-008)**:
- ✅ **REQ-GRAPH-001**: Property graph data model
- ✅ **REQ-GRAPH-002**: Nodes with labels
- ✅ **REQ-GRAPH-003**: Edges (relationships) with types
- ✅ **REQ-GRAPH-004**: Properties on both nodes and edges
- ✅ **REQ-GRAPH-005**: Multiple property data types (String, Integer, Float, Boolean, DateTime, Array, Map, **Vector**)
- ✅ **REQ-GRAPH-006**: Multiple labels per node
- ✅ **REQ-GRAPH-007**: Directed edges
- ✅ **REQ-GRAPH-008**: Multiple edges between same pair of nodes

**Phase 2 - OpenCypher Query Engine (REQ-CYPHER-001 through REQ-CYPHER-009)**:
- ✅ **REQ-CYPHER-001**: OpenCypher query language support
- ✅ **REQ-CYPHER-002**: Pattern matching (MATCH clauses)
- ✅ **REQ-CYPHER-007**: WHERE clauses and filtering
- ✅ **REQ-CYPHER-008**: ORDER BY and LIMIT clauses
- ✅ **REQ-CYPHER-009**: Query optimization (Volcano iterator model)

**Phase 2 - RESP Protocol (REQ-REDIS-001 through REQ-REDIS-006)**:
- ✅ **REQ-REDIS-001**: RESP3 protocol implementation
- ✅ **REQ-REDIS-002**: Redis client connections via Tokio
- ✅ **REQ-REDIS-004**: Redis-compatible GRAPH.* commands
- ✅ **REQ-REDIS-006**: Compatible with standard Redis clients

**Phase 3 - Persistence & Multi-Tenancy (REQ-PERSIST-001, REQ-PERSIST-002, REQ-TENANT-001 through REQ-TENANT-008)**:
- ✅ **REQ-PERSIST-001**: RocksDB persistence with column families
- ✅ **REQ-PERSIST-002**: Write-Ahead Logging (WAL) for durability
- ✅ **REQ-TENANT-001**: Multi-tenant namespace isolation
- ✅ **REQ-TENANT-002**: Per-tenant data isolation
- ✅ **REQ-TENANT-003**: Resource quotas (nodes, edges, memory, storage, connections)
- ✅ **REQ-TENANT-004**: Quota enforcement
- ✅ **REQ-TENANT-005**: Tenant management (create, delete, list)
- ✅ **REQ-TENANT-006**: Enable/disable tenants
- ✅ **REQ-TENANT-007**: Usage tracking
- ✅ **REQ-TENANT-008**: Recovery and snapshot support

**Phase 4 - High Availability Foundation (REQ-HA-001 through REQ-HA-004)**:
- ✅ **REQ-HA-001**: Raft consensus protocol (foundation with openraft)
- ✅ **REQ-HA-002**: Leader election and role management
- ✅ **REQ-HA-003**: Cluster membership management
- ✅ **REQ-HA-004**: Health monitoring and quorum detection

**Phase 5 - RDF/SPARQL Support Foundation (REQ-RDF-001 through REQ-SPARQL-008)**:
- ✅ **REQ-RDF-001**: RDF data model (triples and quads)
- ✅ **REQ-RDF-002**: RDF triple/quad store with indexing
- ✅ **REQ-RDF-003**: RDF serialization formats (Turtle, N-Triples, RDF/XML, JSON-LD)
- ✅ **REQ-RDF-004**: Named graphs support

**Phase 6 - Vector Search & AI Integration ✅ (New)**:
- ✅ **REQ-VEC-001**: Native `Vector` (Vec<f32>) property support
- ✅ **REQ-VEC-002**: Vector index management (HNSW)
- ✅ **REQ-VEC-003**: Approximate Nearest Neighbor (ANN) search
- ✅ **REQ-VEC-004**: Cypher `CALL db.index.vector.queryNodes` support
- ✅ **REQ-VEC-005**: Hybrid Graph RAG query execution

### Test Results

```
✓ 174 tests passed (171 unit + 3 integration)
✓ 0 tests failed
✓ Test coverage: Core + Query + RESP + Persistence + HA + RDF + Vector/AI
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

#### 1. Graph RAG (Vector + Graph Hybrid Query)

```cypher
// 1. Create a vector index
CREATE VECTOR INDEX doc_idx FOR (n:Document) ON (n.embedding) 
OPTIONS {dimensions: 1536, similarity: 'cosine'}

// 2. Perform Hybrid Search
CALL db.index.vector.queryNodes('Document', 'embedding', $query_vector, 5) YIELD node, score
MATCH (author:Person)-[:WROTE]->(node)
RETURN node.title, author.name, score
```

#### 2. Rust API for Vectors

```rust
use samyama::{GraphStore, Label, PropertyValue, vector::DistanceMetric};

let mut store = GraphStore::new();

// Create index
store.create_vector_index("Person", "bio_embed", 128, DistanceMetric::Cosine).unwrap();

// Add node with vector
let mut props = HashMap::new();
props.insert("name".to_string(), "Alice".into());
props.insert("bio_embed".to_string(), PropertyValue::Vector(vec![0.1, 0.2, ...]));
store.create_node_with_properties(vec![Label::new("Person")], props);

// Search
let results = store.vector_search("Person", "bio_embed", &query_vec, 10).unwrap();
```

## Architecture

### Module Structure

```
src/
├── graph/              # Property Graph (Node, Edge, PropertyValue)
├── vector/             # Vector Indexing (HNSW, Distance Metrics)
├── query/              # Cypher Engine (Parser, Planner, Operators)
├── protocol/           # RESP Protocol (Redis compatibility)
├── persistence/        # Storage Engine (RocksDB, WAL, Multi-Tenancy)
├── raft/               # High Availability (Consensus, Clustering)
├── rdf/                # RDF Support (Triples, Quads, Serialization)
└── sparql/             # SPARQL Support (Algebra, Results)
```

## Roadmap

### Phase 1-4: Core Infrastructure ✅ (Complete)
- Property Graph, Cypher, RESP, Persistence, Multi-Tenancy, High Availability.

### Phase 5: RDF/SPARQL Foundation ✅ (Complete)
- Triple store, indexing, and serialization formats.

### Phase 6: Vector Search & AI Integration ✅ (Complete)
- Native vector types, HNSW indexing, and Graph RAG hybrid queries.

### Phase 7: Distributed Scaling (Optional)
- [ ] Graph-aware partitioning
- [ ] Distributed query execution
- [ ] Data rebalancing

---

**Version**: 0.1.0
**Status**: Phase 6 Complete - Graph Vector Database
**Last Updated**: 2026-01-08