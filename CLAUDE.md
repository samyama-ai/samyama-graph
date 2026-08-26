# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Samyama is a high-performance distributed graph database written in Rust with Redis protocol (RESP) compatibility, multi-tenancy, vector search, NLQ, and graph algorithms. Currently at Phase 4 (High Availability Foundation), version v1.7.0.

Cypher conformance is **measured, not estimated**: 97.0% of the openCypher TCK's evaluated scenarios pass (3,650 of 3,762, 96.5% coverage, 2026-08-26). The `~90% OpenCypher support` this line used to claim was a self-assessment withdrawn in `#437` as unmeasured; the measured figure has now passed it, which settles nothing, because only one of the two can be reproduced. Quote the pass rate **with** its coverage: a high rate over a small evaluated set is how this measurement is usually inflated, and that is exactly what the earlier 87.1% turned out to be.

## Build & Development Commands

```bash
# Build
cargo build                    # Debug build
cargo build --release          # Release build (optimized)

# Run tests (1814 unit tests)
cargo test                     # All tests
cargo test graph::node         # Specific module tests
cargo test -- --nocapture      # Tests with output

# Benchmarks (all in benches/)
cargo bench                                    # All benchmarks
cargo bench --bench graph_benchmarks           # Criterion micro-benchmarks (15 benches)
cargo bench --bench full_benchmark             # Full suite (ingestion, vector, traversal, algorithms)
cargo bench --bench vector_benchmark           # HNSW vector search
cargo bench --bench graphalytics_benchmark     # LDBC Graphalytics algorithms
cargo bench --bench mvcc_benchmark             # MVCC & arena allocation
cargo bench --bench late_materialization_bench  # Late materialization traversal
cargo bench --bench graph_optimization_benchmark # Metaheuristic optimization solvers
cargo bench --bench ldbc_benchmark             # LDBC SNB Interactive queries (needs data)
cargo bench --bench ldbc_benchmark -- --explain          # print plans, do NOT run the queries
cargo bench --bench ldbc_benchmark -- --derive-params 50 # parameters valid for THIS extract
cargo bench --bench ldbc_bi_benchmark          # LDBC SNB BI queries (needs data)
cargo bench --bench finbench_benchmark         # LDBC FinBench queries (synthetic data)
cargo bench --bench hierarchy_benchmark        # OEH index: build, order test, roll-up vs subtree size

# Run examples
cargo run --example banking_demo              # Banking fraud detection + NLQ
cargo run --example clinical_trials_demo      # Clinical trials + vector search
cargo run --example supply_chain_demo         # Supply chain + optimization
cargo run --example smart_manufacturing_demo  # Digital twin + scheduling
cargo run --example social_network_demo       # Social network analysis
cargo run --example knowledge_graph_demo      # Enterprise knowledge graph
cargo run --example enterprise_soc_demo       # Security operations center
cargo run --example agentic_enrichment_demo   # GAK (Generation-Augmented Knowledge; needs `claude` CLI)
cargo run --example cluster_demo              # Raft clustering
cargo run --example ldbc_loader               # Load LDBC SNB SF1 dataset
cargo run --example finbench_loader           # Load/generate FinBench dataset
cargo run --release --example cricket_loader  # Load 21K Cricsheet matches
cargo run --release --example aact_loader     # Load AACT clinical trials dataset
cargo run --release --example imdb_loader     # Load IMDB movies/persons KG (needs --data-dir)
cargo run --release --example football_loader # Load Football KG (needs --data-dir)
cargo run --release --example hier_benchmark   # HIER corpus: 112 hierarchy queries, index on vs off
cargo run --release --example ontology_loader -- --format taxdump --path nodes.dmp  # real ontologies

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
├── snapshot/        # Portable .sgsnap export/import
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

**Remaining gaps:** full temporal arithmetic (CY-29), standalone WITH...RETURN.

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

**CI runs `cargo test --workspace --no-fail-fast` — a debug build, not `--release`.**
A suite that passes in release can still fail the gate: the engine is more than
an order of magnitude slower in debug. Run the profile CI runs before claiming a
suite is green, and say which profile a claim was measured in.

### Diagnosing a slow query

```bash
# Plans without executing. `--profile` runs the query, which is useless for the
# ones most worth looking at — a query that times out cannot be PROFILEd.
cargo bench --bench ldbc_benchmark -- --data-dir <sf1> --derive-params 50 --explain --query IC6

# Why the planner chose the anchor it chose. EXPLAIN shows only the winner.
SAMYAMA_EXPLAIN_ANCHORS=1 cargo bench --bench ldbc_benchmark -- ... --explain --query IC6
```

**Use `--derive-params` before believing anything.** The built-in defaults name
a person who exists in one particular extract and not in others. Against the
wrong extract every query runs fast and returns nothing, the anchor for the
pinned node costs zero rows, and the plans you are reading are plans for a
query that matches nothing. The bench's `0 empty` line in the summary is the
check that catches this; read it first.

**Do not assert wall-clock times in tests.** An absolute bound encodes the speed
of the machine and the build profile that wrote it. Assert a **ratio** against a
baseline measured in the same process instead — a scan against an anchored
lookup, a small graph against a large one, a query against a single hop. See
`tests/id_anchor.rs`, `tests/aggregate_identity_grouping.rs`,
`tests/bounded_sort_semantics.rs` and `tests/shortest_path_semantics.rs` for the
pattern, and #587 for what it cost to learn. A weak timing assertion is worse
than none: the one in `id_anchor` passed for a whole PR over a plan that was
running 329x too slow (#584).

Benchmarks are the opposite — always `--release`, on a quiet host, with the
calibration line compared before the timings (#529).

- **1814 unit tests** across all modules (87.8% coverage)
- **10 benchmark binaries** in `benches/` (Criterion micro-benchmarks + domain benchmarks)
- **Integration tests**: Python scripts in `tests/integration/`
- **8 domain-specific example demos** with NLQ integration
- **4 data loaders** (LDBC SNB, FinBench, Cricket, AACT) in `examples/`
