# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Samyama is a high-performance distributed graph database written in Rust with OpenCypher query support, Redis protocol (RESP) compatibility, and multi-tenancy. Currently at Phase 4 (High Availability Foundation).

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build (optimized)

# Run tests
cargo test                     # All tests (119 unit + 1 doc)
cargo test graph::node         # Specific module tests
cargo test -- --nocapture      # Tests with output

# Run examples
cargo run --example persistence_demo    # Phase 3: Persistence & multi-tenancy
cargo run --example cluster_demo        # Phase 4: Raft clustering
cargo run --example banking_demo        # Banking use case demo

# Start RESP server
cargo run                      # Starts on 127.0.0.1:6379

# Code quality
cargo fmt -- --check           # Check formatting
cargo clippy -- -D warnings    # Lint checks

# Integration tests (requires running server)
cd tests/integration
python3 test_resp_basic.py
python3 test_resp_visual.py
```

## Architecture

### Module Structure

```
src/
├── graph/           # Phase 1: Property Graph Model
│   ├── store.rs     # GraphStore - in-memory storage with indices
│   ├── node.rs      # Node with labels and properties
│   ├── edge.rs      # Directed edges with types
│   ├── property.rs  # PropertyValue (7 types: String, Integer, Float, Boolean, DateTime, Array, Map)
│   └── types.rs     # NodeId, EdgeId, Label, EdgeType
│
├── query/           # Phase 2: OpenCypher Query Engine
│   ├── parser.rs    # Pest-based OpenCypher parser
│   ├── cypher.pest  # OpenCypher grammar
│   ├── ast.rs       # Query AST
│   └── executor/
│       ├── planner.rs   # Query planner (AST → ExecutionPlan)
│       ├── operator.rs  # Physical operators (Volcano iterator model)
│       └── record.rs    # Record, RecordBatch, Value
│
├── protocol/        # Phase 2: RESP Protocol
│   ├── resp.rs      # RESP3 encoder/decoder
│   ├── server.rs    # Tokio TCP server
│   └── command.rs   # GRAPH.* command handler
│
├── persistence/     # Phase 3: Persistence & Multi-Tenancy
│   ├── storage.rs   # RocksDB with column families
│   ├── wal.rs       # Write-Ahead Log
│   └── tenant.rs    # Multi-tenancy & resource quotas
│
└── raft/            # Phase 4: High Availability
    ├── node.rs      # RaftNode using openraft
    ├── state_machine.rs  # GraphStateMachine
    ├── cluster.rs   # ClusterConfig, ClusterManager
    ├── network.rs   # Inter-node communication
    └── storage.rs   # Raft log storage
```

### Key Architectural Patterns

1. **Volcano Iterator Model (ADR-007)**: Query execution uses lazy, pull-based operators:
   - `NodeScanOperator` → `FilterOperator` → `ExpandOperator` → `ProjectOperator` → `LimitOperator`

2. **In-Memory Graph Storage**: O(1) lookups via HashMaps with adjacency lists for traversal:
   - `nodes: HashMap<NodeId, Node>`
   - `label_index: HashMap<Label, HashSet<NodeId>>`
   - `outgoing/incoming: HashMap<NodeId, Vec<EdgeId>>`

3. **Multi-Tenancy**: RocksDB column families with tenant-prefixed keys, per-tenant quotas

4. **Raft Consensus**: Uses `openraft` crate with custom `GraphStateMachine`

## Known Limitations

**Query Engine (not yet implemented):**
- CREATE, DELETE, SET, REMOVE (write operations via Cypher)
- MERGE, OPTIONAL MATCH, UNION, WITH, subqueries
- Aggregation functions (COUNT, SUM, AVG)

**Note**: The parser accepts CREATE clauses but the planner requires at least one MATCH clause. Write operations must use the Rust API directly (`graph.create_node()`, `persist_mgr.persist_create_node()`).

## API Patterns

### Graph Store
```rust
let mut graph = GraphStore::new();
let node_id = graph.create_node("Person");                    // Returns NodeId
graph.get_node_mut(node_id)?.set_property("name", "Alice");
graph.create_edge(source_id, target_id, "KNOWS")?;            // Returns EdgeId
let nodes: Vec<&Node> = graph.get_nodes_by_label(&Label::new("Person"));
let edges: Vec<&Edge> = graph.get_outgoing_edges(node_id);
```

### Query Engine
```rust
let engine = QueryEngine::new();
let result: RecordBatch = engine.execute("MATCH (n:Person) RETURN n", &graph)?;
for record in &result.records {
    if let Some(value) = record.get("n") { /* Value::Node(id, node) */ }
}
```

### Persistence
```rust
let persist_mgr = PersistenceManager::new("./data")?;
persist_mgr.tenants().create_tenant(id, name, Some(quotas))?;
persist_mgr.persist_create_node("tenant_id", &node)?;
persist_mgr.checkpoint()?;
let (nodes, edges) = persist_mgr.recover("tenant_id")?;
```

## Testing

- **Unit tests**: 119 tests across all modules
- **Integration tests**: Python scripts in `tests/integration/` for RESP server
- **Examples**: `persistence_demo.rs`, `cluster_demo.rs`, `banking_demo.rs`

Connect to RESP server with any Redis client:
```bash
redis-cli
GRAPH.QUERY mygraph "MATCH (n:Person) RETURN n"
GRAPH.LIST
PING
```
