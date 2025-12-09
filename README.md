# Samyama Graph Database

A high-performance, distributed graph database with OpenCypher query support, Redis protocol compatibility, and multi-tenancy.

## Status: Phase 4 - High Availability (Foundation) ✅

### Implemented Requirements

**Phase 1 - Property Graph Model (REQ-GRAPH-001 through REQ-GRAPH-008)**:
- ✅ **REQ-GRAPH-001**: Property graph data model
- ✅ **REQ-GRAPH-002**: Nodes with labels
- ✅ **REQ-GRAPH-003**: Edges (relationships) with types
- ✅ **REQ-GRAPH-004**: Properties on both nodes and edges
- ✅ **REQ-GRAPH-005**: Multiple property data types (String, Integer, Float, Boolean, DateTime, Array, Map)
- ✅ **REQ-GRAPH-006**: Multiple labels per node
- ✅ **REQ-GRAPH-007**: Directed edges
- ✅ **REQ-GRAPH-008**: Multiple edges between same pair of nodes

**Phase 1 - In-Memory Storage (REQ-MEM-001, REQ-MEM-003)**:
- ✅ **REQ-MEM-001**: In-memory graph storage
- ✅ **REQ-MEM-003**: Memory-optimized data structures (hash maps, adjacency lists)

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
- ✅ Raft state machine for graph operations
- ✅ Network layer framework for node communication
- ✅ Storage layer for Raft logs and metadata
- ✅ Cluster configuration and node management

### Test Results

```
✓ 119 tests passed (118 unit + 1 doc)
✓ 0 tests failed
✓ Test coverage: Core graph + Query engine + RESP protocol + Persistence + Multi-tenancy + HA/Raft
```

## Quick Start~/Downloads/samyama-graph-main/examples/banking_demo.rs (preview )

### Installation

```bash
# Clone repository
git clone https://github.com/VaidhyaMegha/samyama_graph.git
cd samyama_graph

# Build
cargo build --release

# Run demo
cargo run

# Run tests
cargo test
```

### Usage Examples

#### 1. Property Graph API

```rust
use samyama::{GraphStore, Label};

// Create a graph store
let mut store = GraphStore::new();

// Create nodes with properties
let alice = store.create_node("Person");
if let Some(node) = store.get_node_mut(alice) {
    node.set_property("name", "Alice");
    node.set_property("age", 30i64);
    node.set_property("city", "New York");
}

let bob = store.create_node("Person");
if let Some(node) = store.get_node_mut(bob) {
    node.set_property("name", "Bob");
    node.set_property("age", 25i64);
}

// Create edges (relationships)
let knows = store.create_edge(alice, bob, "KNOWS").unwrap();
if let Some(edge) = store.get_edge_mut(knows) {
    edge.set_property("since", 2020i64);
    edge.set_property("strength", 0.9);
}

// Query the graph
let persons = store.get_nodes_by_label(&Label::new("Person"));
println!("Found {} persons", persons.len());

// Traverse relationships
let alice_connections = store.get_outgoing_edges(alice);
for edge in alice_connections {
    println!("Alice -[{}]-> {}", edge.edge_type, edge.target);
}
```

#### 2. OpenCypher Queries

```rust
use samyama::{GraphStore, QueryEngine};

let mut store = GraphStore::new();
// ... create nodes and edges ...

let engine = QueryEngine::new();

// Simple pattern matching
let result = engine.execute("MATCH (n:Person) RETURN n", &store)?;
println!("Found {} persons", result.len());

// Filtering with WHERE
let result = engine.execute(
    "MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age",
    &store
)?;

// Edge traversal
let result = engine.execute(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    &store
)?;

// With LIMIT
let result = engine.execute(
    "MATCH (n:Person) RETURN n ORDER BY n.age DESC LIMIT 5",
    &store
)?;
```

#### 3. RESP Protocol Server

```rust
use samyama::{RespServer, ServerConfig, GraphStore};
use std::sync::Arc;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let config = ServerConfig::default(); // 127.0.0.1:6379
    let server = RespServer::new(config, store);

    server.start().await.unwrap();
}
```

Then connect with any Redis client:

```bash
redis-cli

# Execute Cypher queries
GRAPH.QUERY mygraph "MATCH (n:Person) RETURN n"
GRAPH.QUERY mygraph "MATCH (a)-[:KNOWS]->(b) WHERE a.age > 25 RETURN a, b"

# Management commands
GRAPH.LIST
GRAPH.DELETE mygraph

# Standard Redis commands
PING
ECHO "Hello"
INFO
```

#### 4. Persistence & Multi-Tenancy

```rust
use samyama::{PersistenceManager, ResourceQuotas, graph::{Node, NodeId, Label}};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create persistence manager
    let persist_mgr = PersistenceManager::new("./graph_data")?;

    // Create tenant with custom quotas
    let quotas = ResourceQuotas {
        max_nodes: Some(1_000_000),
        max_edges: Some(10_000_000),
        max_memory_bytes: Some(1024 * 1024 * 1024), // 1 GB
        max_storage_bytes: Some(10 * 1024 * 1024 * 1024), // 10 GB
        max_connections: Some(100),
        max_query_time_ms: Some(60_000), // 60 seconds
    };

    persist_mgr.tenants().create_tenant(
        "my_tenant".to_string(),
        "My Tenant".to_string(),
        Some(quotas),
    )?;

    // Create and persist a node
    let mut node = Node::new(NodeId::new(1), Label::new("Person"));
    node.set_property("name", "Alice");
    node.set_property("age", 30i64);

    // Automatically enforces quotas and writes to WAL + RocksDB
    persist_mgr.persist_create_node("my_tenant", &node)?;

    // Create checkpoint (flush WAL and storage)
    persist_mgr.checkpoint()?;

    // Recover from disk
    let (nodes, edges) = persist_mgr.recover("my_tenant")?;
    println!("Recovered {} nodes and {} edges", nodes.len(), edges.len());

    Ok(())
}
```

Run the persistence demo:

```bash
# Run the comprehensive demo
cargo run --example persistence_demo

# This demonstrates:
# - WAL (Write-Ahead Logging)
# - RocksDB persistence with column families
# - Multi-tenant data isolation
# - Resource quota enforcement
# - Recovery from disk
```

## Architecture

### Technology Stack (Phases 1-3)

- **Language**: Rust 2021 Edition
- **Core Dependencies**:
  - `serde`: Serialization/deserialization
  - `indexmap`: Ordered maps
  - `thiserror`: Error handling
- **Query Engine (Phase 2)**:
  - `pest`: PEG parser for OpenCypher
  - Volcano iterator model for query execution
  - Pattern matching and graph traversal
- **Networking (Phase 2)**:
  - `tokio`: Async runtime (v1.35+)
  - `bytes`: Zero-copy buffer management
  - RESP3 protocol implementation
  - `tracing`: Structured logging
- **Persistence (Phase 3)**:
  - `rocksdb`: LSM-tree storage engine (v0.22)
  - `bincode`: Binary serialization
  - `chrono`: Timestamp handling
  - Write-Ahead Logging for durability
  - Column families for multi-tenancy
  - LZ4/Zstd compression

### Data Structures

```
GraphStore (In-Memory)
├── nodes: HashMap<NodeId, Node>
├── edges: HashMap<EdgeId, Edge>
├── outgoing: HashMap<NodeId, Vec<EdgeId>>  // Adjacency list
├── incoming: HashMap<NodeId, Vec<EdgeId>>  // Reverse adjacency
├── label_index: HashMap<Label, HashSet<NodeId>>
└── edge_type_index: HashMap<EdgeType, HashSet<EdgeId>>
```

### Module Structure

```
src/
├── lib.rs              # Library entry point
├── main.rs             # Server application
├── graph/              # Phase 1: Property Graph
│   ├── mod.rs          # Graph module
│   ├── types.rs        # NodeId, EdgeId, Label, EdgeType
│   ├── property.rs     # PropertyValue (7 data types)
│   ├── node.rs         # Node implementation
│   ├── edge.rs         # Edge implementation
│   └── store.rs        # GraphStore (in-memory)
├── query/              # Phase 2: Query Engine
│   ├── mod.rs          # Query module
│   ├── ast.rs          # Abstract Syntax Tree
│   ├── parser.rs       # OpenCypher parser (Pest)
│   ├── cypher.pest     # OpenCypher grammar
│   └── executor/
│       ├── mod.rs      # Executor module
│       ├── operator.rs # Physical operators (Scan, Filter, Expand, Project, Limit)
│       ├── planner.rs  # Query planner
│       └── record.rs   # Record structures
├── protocol/           # Phase 2: RESP Protocol
│   ├── mod.rs          # Protocol module
│   ├── resp.rs         # RESP encoder/decoder
│   ├── server.rs       # Tokio TCP server
│   └── command.rs      # GRAPH.* command handler
└── persistence/        # Phase 3: Persistence & Multi-Tenancy
    ├── mod.rs          # Persistence module & PersistenceManager
    ├── wal.rs          # Write-Ahead Log implementation
    ├── storage.rs      # RocksDB persistent storage
    └── tenant.rs       # Multi-tenancy & resource quotas
```

## Performance Characteristics

### Operations (Current Implementation)

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| Create Node | O(1) | O(1) |
| Create Edge | O(1) | O(1) |
| Get Node by ID | O(1) | - |
| Get Edge by ID | O(1) | - |
| Get Nodes by Label | O(n) where n = nodes with label | - |
| Get Outgoing Edges | O(m) where m = outgoing edges | - |
| Delete Node | O(m) where m = connected edges | O(1) |

### Memory Usage

- **Node**: ~200 bytes + properties
- **Edge**: ~150 bytes + properties
- **Overhead**: ~20-30% for indices and adjacency lists

## Roadmap

### Phase 1: Core Property Graph ✅ (Complete)
- [x] Property graph model
- [x] In-memory storage
- [x] Basic CRUD operations
- [x] Adjacency lists for traversal
- [x] Label and edge type indices

### Phase 2: Query Engine & RESP Protocol ✅ (Complete)
- [x] OpenCypher query parser (Pest PEG)
- [x] Query execution engine (Volcano iterator model)
- [x] Physical operators (Scan, Filter, Expand, Project, Limit)
- [x] Query planner with basic optimization
- [x] RESP protocol server (Tokio)
- [x] GRAPH.* commands (QUERY, RO_QUERY, DELETE, LIST)
- [x] Redis client compatibility

### Phase 3: Persistence & Multi-Tenancy ✅ (Complete)
- [x] Write-Ahead Log (WAL) with sequence numbers and checksums
- [x] RocksDB integration with column families
- [x] Snapshot and checkpoint creation
- [x] Multi-tenant namespace isolation (tenant-prefixed keys)
- [x] Resource quotas (nodes, edges, memory, storage, connections, query time)
- [x] Tenant management (create, delete, list, enable/disable)
- [x] Usage tracking and quota enforcement
- [x] Recovery from persistent storage

### Phase 4: High Availability (Foundation) ✅ (Complete)
- [x] Raft consensus protocol foundation (openraft)
- [x] Raft state machine for graph operations
- [x] Leader election and role management
- [x] Cluster membership management
- [x] Health monitoring and quorum detection
- [x] Network layer framework
- [x] Raft storage layer (logs, metadata, snapshots)
- [x] 3-node cluster demonstration

### Phase 5: Distributed Scaling (Optional)
- [ ] Graph-aware partitioning
- [ ] Distributed query execution
- [ ] Data rebalancing

### Phase 6: Advanced Features (Optional)
- [ ] RDF data model support
- [ ] SPARQL query language
- [ ] Graph algorithms library

## Documentation

- [Requirements Specification](./REQUIREMENTS.md)
- [Feasibility & Implementation Plan](./FEASIBILITY_AND_PLAN.md)
- [Technology Stack Analysis](./docs/TECH_STACK.md)
- [System Architecture](./docs/ARCHITECTURE.md)
- [Architecture Decision Records](./docs/ADR/)
- [Product Management Artifacts](./docs/product/)

## Testing

### Run Tests

```bash
# All unit tests
cargo test

# Specific module
cargo test graph::node

# With output
cargo test -- --nocapture

# Test coverage (requires tarpaulin)
cargo tarpaulin --out Html
```

### Integration Tests

Test the RESP server with real network connections:

```bash
# Terminal 1: Start server
cargo run --release

# Terminal 2: Run integration tests
cd tests/integration
python3 test_resp_basic.py
python3 test_resp_visual.py
```

See [tests/integration/README.md](tests/integration/README.md) for detailed instructions.

### Test Categories

- **Unit Tests**: 118 tests covering all functionality
  - 35 tests: Property graph (Phase 1)
  - 49 tests: Query engine & RESP protocol (Phase 2)
  - 20 tests: Persistence & multi-tenancy (Phase 3)
  - 14 tests: High availability & Raft consensus (Phase 4)
- **Integration Tests**: 8 tests for RESP server
- **Doc Tests**: 1 test for library examples
- **Total**: 127 tests, 100% passing

### Test Results

Detailed test reports available in [docs/test-results/](docs/test-results/):
- [Phase 2 RESP Protocol Tests](docs/test-results/PHASE2_RESP_TESTS.md)
- Performance benchmarks
- Requirements coverage

## Contributing

This is currently a private repository under active development.

### Development Setup

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Clone and build
git clone https://github.com/VaidhyaMegha/samyama_graph.git
cd samyama_graph
cargo build

# Run tests
cargo test

# Check formatting
cargo fmt -- --check

# Run clippy
cargo clippy -- -D warnings
```

## License

Apache License 2.0

## Authors

Samyama Graph Database Team

## References

- [Property Graph Model](https://neo4j.com/blog/data-modeling-basics/)
- [OpenCypher Query Language](https://opencypher.org/)
- [Redis Protocol (RESP)](https://redis.io/docs/reference/protocol-spec/)
- [Raft Consensus](https://raft.github.io/)

---

**Version**: 0.1.0
**Status**: Phase 4 Complete - High Availability Foundation
**Last Updated**: 2025-10-16
