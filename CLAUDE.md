# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Samyama is a high-performance distributed graph database written in Rust with ~90% OpenCypher query support, Redis protocol (RESP) compatibility, multi-tenancy, vector search, NLQ, and graph algorithms. Currently at Phase 4 (High Availability Foundation), version v0.5.4.

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build (optimized)

# Run tests (248 unit tests)
cargo test                     # All tests
cargo test graph::node         # Specific module tests
cargo test -- --nocapture      # Tests with output

# Benchmarks (Criterion)
cargo bench                    # 15 benchmarks across 5 groups

# Run examples
cargo run --example banking_demo              # Banking fraud detection + NLQ
cargo run --example clinical_trials_demo      # Clinical trials + vector search
cargo run --example supply_chain_demo         # Supply chain + optimization
cargo run --example smart_manufacturing_demo  # Digital twin + scheduling
cargo run --example social_network_demo       # Social network analysis
cargo run --example knowledge_graph_demo      # Enterprise knowledge graph
cargo run --example enterprise_soc_demo       # Security operations center
cargo run --example agentic_enrichment_demo   # GAK (Generation-Augmented Knowledge)
cargo run --example persistence_demo          # Persistence & multi-tenancy
cargo run --example cluster_demo              # Raft clustering

# Start RESP server
cargo run                      # RESP on 127.0.0.1:6379, HTTP on :8080

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
├── graph/           # Property Graph Model
│   ├── store.rs     # GraphStore - in-memory storage with indices + cardinality stats
│   ├── node.rs      # Node with labels and properties
│   ├── edge.rs      # Directed edges with types
│   ├── property.rs  # PropertyValue (String, Integer, Float, Boolean, DateTime, Array, Map, Null)
│   └── types.rs     # NodeId, EdgeId, Label, EdgeType
│
├── query/           # OpenCypher Query Engine (~90% coverage)
│   ├── parser.rs    # Pest-based OpenCypher parser
│   ├── cypher.pest  # PEG grammar (atomic keyword rules for word boundaries)
│   ├── ast.rs       # Query AST
│   └── executor/
│       ├── planner.rs   # Query planner (AST → ExecutionPlan)
│       ├── operator.rs  # Physical operators (Volcano iterator model)
│       └── record.rs    # Record, RecordBatch, Value (with late materialization)
│
├── protocol/        # RESP Protocol
│   ├── resp.rs      # RESP3 encoder/decoder
│   ├── server.rs    # Tokio TCP server
│   └── command.rs   # GRAPH.* command handler
│
├── persistence/     # Persistence & Multi-Tenancy
│   ├── storage.rs   # RocksDB with column families
│   ├── wal.rs       # Write-Ahead Log
│   └── tenant.rs    # Multi-tenancy & resource quotas
│
├── raft/            # High Availability
│   ├── node.rs      # RaftNode using openraft
│   ├── state_machine.rs  # GraphStateMachine
│   ├── cluster.rs   # ClusterConfig, ClusterManager
│   ├── network.rs   # Inter-node communication
│   └── storage.rs   # Raft log storage
│
├── nlq/             # Natural Language Query Pipeline
│   ├── mod.rs       # NLQPipeline (text_to_cypher, extract_cypher, is_safe_query)
│   └── client.rs    # NLQClient (OpenAI, Gemini, Ollama, Claude Code providers)
│
├── vector/          # HNSW Vector Index
└── sharding/        # Tenant-level sharding
```

### Key Architectural Patterns

1. **Volcano Iterator Model (ADR-007)**: Lazy, pull-based operators:
   - `NodeScanOperator` → `FilterOperator` → `ExpandOperator` → `ProjectOperator` → `LimitOperator`

2. **Late Materialization (ADR-012)**: Scan produces `Value::NodeRef(id)` not full clones. Properties resolved on demand via `resolve_property()`.

3. **In-Memory Graph Storage**: O(1) lookups via HashMaps with adjacency lists for traversal.

4. **Multi-Tenancy**: RocksDB column families with tenant-prefixed keys, per-tenant quotas.

5. **Raft Consensus**: Uses `openraft` crate with custom `GraphStateMachine`.

6. **Cross-Type Coercion**: Integer/Float promotion, String/Boolean coercion, Null propagation (three-valued logic).

## Cypher Support

**Supported clauses:** MATCH, OPTIONAL MATCH, CREATE, DELETE, SET, REMOVE, MERGE, WITH, UNWIND, UNION, RETURN DISTINCT, ORDER BY, SKIP, LIMIT, EXPLAIN, EXISTS subqueries.

**Supported functions (30+):** toUpper, toLower, trim, replace, substring, left, right, reverse, toString, toInteger, toFloat, abs, ceil, floor, round, sqrt, sign, count, sum, avg, min, max, collect, size, length, head, last, tail, keys, id, labels, type, exists, coalesce.

**Remaining gaps:** WITH projection barrier (partial), list slicing, pattern comprehensions, named paths, CASE expressions, collect(DISTINCT x).

## API Patterns

### Query Engine
```rust
// Read-only queries
let executor = QueryExecutor::new(&store);
let result: RecordBatch = executor.execute(&query)?;

// Write queries (CREATE, DELETE, SET, MERGE)
let mut executor = MutQueryExecutor::new(&mut store, tenant_id);
executor.execute(&query)?;

// EXPLAIN (no execution)
// Returns plan as RecordBatch with operator descriptions
```

### NLQ Pipeline
```rust
let pipeline = NLQPipeline::new(nlq_config)?;
let cypher = pipeline.text_to_cypher("Who knows Alice?", &schema_summary).await?;
// Returns clean Cypher with markdown fences stripped and safety validation
```

### Graph Store
```rust
let mut graph = GraphStore::new();
let node_id = graph.create_node("Person");
graph.get_node_mut(node_id)?.set_property("name", "Alice");
graph.create_edge(source_id, target_id, "KNOWS")?;
```

## Testing

- **248 unit tests** across all modules
- **15 Criterion benchmarks** (node insertion, label scan, traversal, WHERE filter, Cypher parse)
- **Integration tests**: Python scripts in `tests/integration/`
- **8 domain-specific example demos** with NLQ integration
