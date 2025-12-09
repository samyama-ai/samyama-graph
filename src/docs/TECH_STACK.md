# Samyama Graph Database - Technology Stack

## Executive Summary

After deep technical analysis, the recommended technology stack is:

- **Core Language**: Rust
- **Storage Engine**: Custom graph store with RocksDB for persistence
- **Network Protocol**: RESP (Redis Serialization Protocol) via Tokio
- **Query Engine**: Custom OpenCypher implementation
- **Consensus**: Raft (via openraft)
- **Serialization**: Apache Arrow / Cap'n Proto
- **Monitoring**: Prometheus + OpenTelemetry

---

## 1. Programming Language Selection

### Critical Requirements
- Memory safety (graph databases handle complex pointer structures)
- High performance (in-memory operations, tight loops)
- Concurrency support (thousands of concurrent queries)
- No garbage collection pauses (predictable latency)
- Systems-level control (memory layout optimization)

### Language Comparison Matrix

| Language | Performance | Memory Safety | Concurrency | GC Pauses | Ecosystem | Verdict |
|----------|-------------|---------------|-------------|-----------|-----------|---------|
| **Rust** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ✅ None | ⭐⭐⭐⭐ | **RECOMMENDED** |
| C++ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | ✅ None | ⭐⭐⭐⭐⭐ | Strong Alternative |
| Go | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ❌ Yes | ⭐⭐⭐⭐⭐ | Not Ideal |
| Java | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ❌ Yes | ⭐⭐⭐⭐⭐ | Not Ideal |
| Zig | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ✅ None | ⭐⭐ | Too Immature |

### Deep Dive: Rust vs C++

#### Why Rust Over C++?

**1. Memory Safety Without Runtime Cost**
```rust
// Rust prevents use-after-free at compile time
fn traverse_graph(node: &Node) {
    let edges = &node.edges;
    // Compiler ensures edges reference is valid
    for edge in edges {
        // No dangling pointers possible
    }
}
```

```cpp
// C++ - easy to create bugs
void traverse_graph(Node* node) {
    auto edges = node->edges;
    delete node; // Oops! edges now invalid
    for (auto& edge : edges) { // Undefined behavior
        // ...
    }
}
```

**2. Fearless Concurrency**
```rust
// Rust prevents data races at compile time
Arc<RwLock<GraphStore>> // Thread-safe by construction
// Compiler enforces:
// - Multiple readers OR single writer
// - No shared mutable state without synchronization
```

**3. Modern Tooling**
- Cargo: Superior dependency management (vs CMake/Conan chaos)
- Built-in testing, benchmarking, documentation
- Consistent formatting (rustfmt)
- Integrated linter (clippy)

**4. Error Handling**
```rust
// Explicit, type-safe error handling
Result<Vec<Node>, GraphError>
// Forces handling of errors at compile time
```

**5. Zero-Cost Abstractions**
- Iterator chains compile to same code as manual loops
- No runtime overhead for abstractions
- Monomorphization eliminates virtual dispatch

#### When C++ Might Be Better

- **Existing C++ expertise on team** (but Rust learning curve is 2-3 months)
- **Need to integrate with existing C++ libraries** (though Rust FFI is excellent)
- **Extreme low-level control** (Rust gives 99% of this)

**Verdict**: **Rust wins** for greenfield graph database due to safety + performance + modern tooling.

### Why Not Go?

Go is excellent for many systems, but has critical issues for graph databases:

**1. Garbage Collection Pauses**
```
Query response times:
- P50: 5ms
- P99: 200ms  ← GC pause
- P99.9: 500ms ← Long GC pause
```

For sub-10ms query latency requirements, GC pauses are unacceptable.

**2. Lack of Control Over Memory Layout**
```rust
// Rust: Explicit memory layout control
#[repr(C)]
struct Node {
    id: u64,           // 8 bytes, offset 0
    label: u32,        // 4 bytes, offset 8
    edges_offset: u32, // 4 bytes, offset 12
}
// Total: 16 bytes, cache-line aligned
```

Go doesn't provide this level of control, critical for cache efficiency in graph traversals.

**3. No SIMD Support**
Graph algorithms benefit from SIMD (processing multiple nodes/edges in parallel). Rust has excellent SIMD support; Go has minimal.

**Counter-argument**: Go's simplicity and productivity
**Response**: For a database system where performance is critical, the Rust complexity is worth it.

### Why Not Java?

**1. GC Pauses Even Worse**
- Even with ZGC, sub-millisecond pauses not guaranteed
- Large heaps (billions of nodes) → long GC cycles

**2. Memory Overhead**
```
Java object overhead: 12-16 bytes per object
Graph with 1B nodes: 12-16 GB wasted on headers alone
```

**3. JVM Warmup**
- Minutes to reach peak performance
- Unacceptable for database restart scenarios

**Counter-argument**: Neo4j uses Java successfully
**Response**: Neo4j predates Rust. If building today, Rust would likely be chosen.

---

## 2. Storage Engine Architecture

### Design Philosophy

```
┌─────────────────────────────────────────────┐
│  In-Memory Graph Store (Primary)           │
│  - Optimized for traversals                 │
│  - Custom data structures                   │
│  - Hot data                                  │
└─────────────────┬───────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│  Persistence Layer (RocksDB)                │
│  - Write-Ahead Log (WAL)                    │
│  - Snapshots                                │
│  - Cold data                                │
└─────────────────────────────────────────────┘
```

### In-Memory Graph Store

#### Core Data Structures

**Option 1: Adjacency List (Chosen for Phase 1)**
```rust
struct GraphStore {
    // Node storage: NodeId -> Node
    nodes: HashMap<NodeId, Node>,

    // Edge storage: EdgeId -> Edge
    edges: HashMap<EdgeId, Edge>,

    // Adjacency: NodeId -> [EdgeId]
    outgoing: HashMap<NodeId, Vec<EdgeId>>,
    incoming: HashMap<NodeId, Vec<EdgeId>>,

    // Indices: (Label, Property) -> [NodeId]
    indices: HashMap<(Label, Property), Vec<NodeId>>,
}

struct Node {
    id: NodeId,
    labels: SmallVec<[Label; 2]>, // Most nodes have 1-2 labels
    properties: PropertyMap,      // Column-oriented
}

struct Edge {
    id: EdgeId,
    source: NodeId,
    target: NodeId,
    edge_type: EdgeType,
    properties: PropertyMap,
}
```

**Rationale**:
- Simple to implement
- Cache-friendly for traversals
- Easy to update
- Proven pattern (RedisGraph uses similar)

**Option 2: Compressed Sparse Row (CSR) - Phase 2+**
```rust
struct CSRGraph {
    // Compact array-based storage
    // Better cache locality, less memory
    // But harder to update (requires rebuild)
    row_offsets: Vec<usize>,    // Node -> offset in edges array
    column_indices: Vec<NodeId>, // Edge targets
    edge_data: Vec<EdgeData>,    // Edge properties
}
```

**When to use**:
- Read-heavy workloads
- Static or slowly-changing graphs
- Maximum performance

**Trade-off**: Update performance vs query performance

#### Property Storage

**Columnar Design** (Chosen):
```rust
struct PropertyMap {
    // Column-oriented for compression and cache efficiency
    string_props: HashMap<PropertyKey, String>,
    int_props: HashMap<PropertyKey, i64>,
    float_props: HashMap<PropertyKey, f64>,
    bool_props: HashMap<PropertyKey, bool>,
    // ... other types
}
```

**vs Row-Oriented**:
```rust
// Row-oriented (Not chosen)
HashMap<PropertyKey, PropertyValue> // More flexible but less efficient
```

**Rationale**:
- Better compression (all integers together)
- Better cache locality for property scans
- SIMD-friendly
- Used by: Apache Arrow, ClickHouse

#### Index Structures

```rust
// Label index: Fast "MATCH (n:Person)" queries
HashMap<Label, RoaringBitmap> // Bitmap for fast set operations

// Property index: Fast "WHERE n.age > 30" queries
enum PropertyIndex {
    Hash(HashMap<PropertyValue, RoaringBitmap>), // Exact match
    BTree(BTreeMap<PropertyValue, RoaringBitmap>), // Range queries
    FullText(TantivyIndex), // Text search (Phase 2+)
}
```

**Bitmap Choice**: RoaringBitmap
- Compressed (up to 1000x vs raw bitmap)
- Fast set operations (AND, OR, NOT)
- Used by: Elasticsearch, Lucene

### Persistence Layer: RocksDB

**Why RocksDB?**

| Feature | RocksDB | LevelDB | LMDB | Sled |
|---------|---------|---------|------|------|
| Performance | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| Maturity | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| Features | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| WAL Support | ✅ | ✅ | ✅ | ✅ |
| Compression | ✅ Multiple | ✅ Snappy | ❌ | ✅ |
| Production Use | Meta, LinkedIn | Google | OpenLDAP | Limited |

**RocksDB Advantages**:
1. **Battle-tested**: Powers Facebook, LinkedIn, Netflix
2. **Tunable**: 100+ configuration options
3. **Column Families**: Separate namespaces (perfect for multi-tenancy)
4. **Write-optimized**: LSM tree design
5. **Compression**: Snappy, LZ4, Zstd
6. **Rust bindings**: Mature via `rust-rocksdb`

**RocksDB Schema Design**:
```
Column Families:
- "nodes":     NodeId -> Node (serialized)
- "edges":     EdgeId -> Edge (serialized)
- "wal":       Sequence -> Operation (for recovery)
- "metadata":  Config, schema, etc.
- "tenant_1":  Tenant-specific data
- "tenant_2":  ...
```

**Alternative Considered: Custom Storage**
- **Pros**: Perfect optimization for graph workload
- **Cons**: Years of development, bugs, maintenance burden
- **Verdict**: Use RocksDB, optimize later if needed

---

## 3. Network Protocol Layer

### RESP (Redis Serialization Protocol)

**Why RESP?**
1. **Simple**: Text-based (RESP2) or binary (RESP3)
2. **Proven**: Billions of connections handled daily
3. **Client Support**: Libraries in every language
4. **Human-readable**: Easy debugging with `redis-cli`
5. **Pipelining**: Batch multiple commands

**RESP3 Example**:
```
Client: *3\r\n$5\r\nGRAPH\r\n$5\r\nQUERY\r\n$25\r\nMATCH (n) RETURN n LIMIT 10\r\n

Server: *10\r\n
        %3\r\n
        +id\r\n:1\r\n
        +labels\r\n*1\r\n+Person\r\n
        +properties\r\n%2\r\n+name\r\n$5\r\nAlice\r\n+age\r\n:30\r\n
        ...
```

**Custom Command Namespace**:
```
GRAPH.QUERY <graph-name> <cypher-query>
GRAPH.RO_QUERY <graph-name> <cypher-query>
GRAPH.DELETE <graph-name>
GRAPH.SLOWLOG
GRAPH.EXPLAIN <graph-name> <cypher-query>
GRAPH.CONFIG GET/SET
```

**Implementation**: Tokio-based async server
```rust
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn handle_connection(socket: TcpStream, store: Arc<GraphStore>) {
    let (reader, writer) = socket.into_split();
    let mut resp_parser = RespParser::new(reader);
    let mut resp_writer = RespWriter::new(writer);

    loop {
        let cmd = resp_parser.parse_command().await?;
        let result = execute_command(cmd, &store).await;
        resp_writer.write_response(result).await?;
    }
}
```

### Additional Protocols (Phase 2+)

**HTTP/REST API**:
```
POST /v1/graphs/:graph_name/query
{
  "query": "MATCH (n:Person) RETURN n",
  "parameters": { "name": "Alice" }
}
```

**Why**: Web-friendly, easier for some clients

**gRPC** (Internal cluster communication):
```protobuf
service GraphService {
    rpc ExecuteQuery(QueryRequest) returns (QueryResponse);
    rpc Replicate(ReplicationLog) returns (ReplicationAck);
}
```

**Why**: Efficient binary protocol for node-to-node communication

---

## 4. Query Engine Architecture

### OpenCypher Implementation

**Architecture**:
```
Cypher Query
    ↓
┌──────────────┐
│   Parser     │  ← Use libcypher-parser or write custom
└──────┬───────┘
       ↓
┌──────────────┐
│     AST      │  Abstract Syntax Tree
└──────┬───────┘
       ↓
┌──────────────┐
│  Validator   │  Type checking, semantic analysis
└──────┬───────┘
       ↓
┌──────────────┐
│   Planner    │  Logical plan → Physical plan
└──────┬───────┘
       ↓
┌──────────────┐
│  Optimizer   │  Cost-based optimization
└──────┬───────┘
       ↓
┌──────────────┐
│   Executor   │  Volcano/Iterator model
└──────┬───────┘
       ↓
    Results
```

### Parser Options

**Option 1: libcypher-parser** (C library)
- **Pros**: Mature, TCK-compliant, used by RedisGraph
- **Cons**: FFI overhead, C dependency

**Option 2: Custom Rust Parser**
- **Pros**: Native Rust, full control, no FFI
- **Cons**: Months of development, TCK compliance hard

**Recommendation**: **Start with libcypher-parser**, migrate to custom if needed

```rust
use libcypher_parser_sys::*;

fn parse_cypher(query: &str) -> Result<AST, ParseError> {
    unsafe {
        let result = cypher_parse(query.as_ptr(), query.len(), ...);
        // Convert C AST to Rust AST
    }
}
```

### Execution Model: Volcano Iterator

```rust
trait PhysicalOperator {
    fn next(&mut self) -> Option<Record>;
}

// Example: Scan operator
struct NodeScanOperator {
    label: Label,
    iter: NodeIterator,
}

impl PhysicalOperator for NodeScanOperator {
    fn next(&mut self) -> Option<Record> {
        self.iter.next().map(|node| Record::from(node))
    }
}

// Example: Filter operator
struct FilterOperator {
    input: Box<dyn PhysicalOperator>,
    predicate: Predicate,
}

impl PhysicalOperator for FilterOperator {
    fn next(&mut self) -> Option<Record> {
        while let Some(record) = self.input.next() {
            if self.predicate.eval(&record) {
                return Some(record);
            }
        }
        None
    }
}
```

**Query Execution Pipeline**:
```
MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.age > 30 RETURN b.name

Execution Plan:
    ProjectOperator(b.name)
        ↑
    FilterOperator(a.age > 30)
        ↑
    ExpandOperator(KNOWS edge)
        ↑
    NodeScanOperator(Person label)
```

### Query Optimization (Phase 2)

**Cost-Based Optimizer**:
```rust
struct Statistics {
    node_count: HashMap<Label, usize>,
    edge_count: HashMap<EdgeType, usize>,
    property_cardinality: HashMap<Property, usize>,
}

fn estimate_cost(plan: &PhysicalPlan, stats: &Statistics) -> f64 {
    // Estimate based on:
    // - Selectivity
    // - Index availability
    // - Join order
}
```

**Optimizations**:
1. **Index selection**: Choose best index for WHERE clause
2. **Join reordering**: Start with most selective pattern
3. **Filter pushdown**: Apply filters early
4. **Lazy evaluation**: Don't compute unused properties

---

## 5. Concurrency and Async Runtime

### Tokio: The Async Runtime

**Why Tokio?**
- Industry standard for async Rust
- Excellent performance (M:N threading)
- Work-stealing scheduler
- Mature ecosystem

```rust
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let graph_store = Arc::new(RwLock::new(GraphStore::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let store = graph_store.clone();

        tokio::spawn(async move {
            handle_connection(socket, store).await;
        });
    }
}
```

### Concurrency Control

**Read-Write Lock Strategy**:
```rust
use tokio::sync::RwLock;

Arc<RwLock<GraphStore>>
// Readers: Shared lock (many concurrent readers)
// Writers: Exclusive lock (blocks all others)
```

**MVCC (Phase 2+)**: Multi-Version Concurrency Control
```rust
struct VersionedGraphStore {
    // Each transaction sees snapshot at specific timestamp
    versions: BTreeMap<Timestamp, GraphSnapshot>,
    current_version: AtomicU64,
}
```

**Benefits**:
- Readers never block readers
- Readers never block writers
- Better concurrency than locks

**Trade-off**: More memory overhead

---

## 6. Distributed Coordination (Phase 3+)

### Raft Consensus

**Why Raft?**
- Understandable (vs Paxos)
- Proven (etcd, TiKV use Raft)
- Strong consistency guarantees
- Excellent Rust implementation (openraft)

**openraft Library**:
```rust
use openraft::*;

struct GraphStateMachine;

impl RaftStateMachine for GraphStateMachine {
    fn apply(&mut self, entry: &Entry) -> Result<Response> {
        // Apply graph mutation to state machine
    }

    fn snapshot(&self) -> Result<Snapshot> {
        // Create graph snapshot for new followers
    }
}
```

**Raft Replication Flow**:
```
Client Write
    ↓
Leader receives write
    ↓
Leader appends to log
    ↓
Leader replicates to followers (parallel)
    ↓
Majority acknowledge
    ↓
Leader commits
    ↓
Leader responds to client
```

**Alternative: Multi-Paxos**
- More complex
- Slightly better performance
- **Verdict**: Raft's understandability wins

---

## 7. Serialization

### Requirements
- Fast serialization/deserialization
- Zero-copy deserialization (critical for performance)
- Schema evolution (versioning)
- Cross-language support (for clients)

### Comparison

| Format | Speed | Zero-Copy | Schema Evolution | Verdict |
|--------|-------|-----------|------------------|---------|
| **Cap'n Proto** | ⭐⭐⭐⭐⭐ | ✅ Yes | ✅ Yes | **RECOMMENDED** |
| **Apache Arrow** | ⭐⭐⭐⭐⭐ | ✅ Yes | ⚠️ Limited | Column data only |
| FlatBuffers | ⭐⭐⭐⭐⭐ | ✅ Yes | ✅ Yes | Similar to Cap'n Proto |
| Protocol Buffers | ⭐⭐⭐⭐ | ❌ No | ✅ Yes | Good but not zero-copy |
| bincode | ⭐⭐⭐⭐⭐ | ❌ No | ❌ No | Rust-only |

**Recommendation**:
- **Cap'n Proto**: For node/edge serialization (zero-copy critical)
- **Apache Arrow**: For columnar property data

**Cap'n Proto Example**:
```capnp
struct Node {
    id @0 :UInt64;
    labels @1 :List(Text);
    properties @2 :PropertyMap;
}

struct Edge {
    id @0 :UInt64;
    source @1 :UInt64;
    target @2 :UInt64;
    edgeType @3 :Text;
    properties @4 :PropertyMap;
}
```

**Benefits**:
- Zero-copy: Read directly from buffer without deserialization
- Fast: Microseconds vs milliseconds
- Compact: Similar size to hand-optimized binary

---

## 8. Monitoring and Observability

### Metrics: Prometheus

**Why Prometheus?**
- Industry standard for metrics
- Pull-based model
- PromQL query language
- Excellent Grafana integration

```rust
use prometheus::{Counter, Histogram, Registry};

lazy_static! {
    static ref QUERY_COUNTER: Counter =
        Counter::new("graph_queries_total", "Total queries").unwrap();

    static ref QUERY_DURATION: Histogram =
        Histogram::new("graph_query_duration_seconds", "Query duration").unwrap();
}

// In query execution:
let timer = QUERY_DURATION.start_timer();
let result = execute_query(query);
timer.observe_duration();
QUERY_COUNTER.inc();
```

**Key Metrics**:
- `graph_queries_total{type="read|write"}`
- `graph_query_duration_seconds{quantile="0.5|0.95|0.99"}`
- `graph_nodes_total`
- `graph_edges_total`
- `graph_memory_bytes`
- `graph_disk_bytes`

### Tracing: OpenTelemetry

**Why OpenTelemetry?**
- Vendor-neutral standard
- Distributed tracing
- Integrates with Jaeger, Zipkin, DataDog

```rust
use tracing::{info, span};
use tracing_opentelemetry::OpenTelemetryLayer;

#[tracing::instrument]
async fn execute_query(query: &str) -> Result<Response> {
    let span = span!(tracing::Level::INFO, "execute_query");
    let _enter = span.enter();

    info!("Parsing query");
    let ast = parse(query)?;

    info!("Planning query");
    let plan = create_plan(ast)?;

    info!("Executing query");
    let result = execute(plan)?;

    Ok(result)
}
```

**Distributed Trace Example**:
```
Request [500ms total]
  ├─ Parse Query [2ms]
  ├─ Plan Query [5ms]
  ├─ Execute Query [490ms]
      ├─ Node Scan [50ms]
      ├─ Edge Expand [200ms]
      │   └─ RPC to Node 2 [150ms]  ← Cross-node call
      └─ Filter [40ms]
```

### Logging: tracing + JSON

```rust
use tracing_subscriber::fmt::format::json;

tracing_subscriber::fmt()
    .json()
    .with_max_level(tracing::Level::INFO)
    .init();

// Structured logging
info!(
    query = %query_string,
    duration_ms = %duration,
    rows_returned = %count,
    "Query executed"
);
```

---

## 9. Testing Strategy

### Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let mut store = GraphStore::new();
        let node = store.create_node(vec!["Person"], props!{
            "name" => "Alice",
            "age" => 30
        });
        assert_eq!(node.labels, vec!["Person"]);
    }
}
```

### Integration Tests
```rust
#[tokio::test]
async fn test_cypher_query() {
    let server = TestServer::start().await;
    let client = RedisClient::connect("127.0.0.1:6379").await.unwrap();

    let result = client.cmd("GRAPH.QUERY")
        .arg("mygraph")
        .arg("CREATE (n:Person {name: 'Alice'}) RETURN n")
        .query_async()
        .await
        .unwrap();

    assert_eq!(result.nodes_created, 1);
}
```

### Property-Based Testing (proptest)
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_graph_invariants(
        ops in vec(graph_operation(), 0..1000)
    ) {
        let mut graph = Graph::new();
        for op in ops {
            graph.apply(op);
        }
        // Invariant: All edges reference valid nodes
        assert!(graph.validate_edge_references());
    }
}
```

### Performance Benchmarks (Criterion)
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_traversal(c: &mut Criterion) {
    let graph = create_test_graph(100_000, 500_000);

    c.bench_function("2-hop traversal", |b| {
        b.iter(|| {
            graph.traverse(black_box(start_node), black_box(2))
        });
    });
}

criterion_group!(benches, benchmark_traversal);
criterion_main!(benches);
```

### OpenCypher TCK (Technology Compatibility Kit)
- Official test suite for OpenCypher compliance
- Thousands of test cases
- Must pass for certification

---

## 10. Development Tooling

### Build System: Cargo

```toml
[package]
name = "samyama"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1.35", features = ["full"] }
rocksdb = "0.21"
serde = { version = "1.0", features = ["derive"] }
prometheus = "0.13"
tracing = "0.1"
openraft = "0.8"

[dev-dependencies]
criterion = "0.5"
proptest = "1.4"

[profile.release]
lto = true              # Link-time optimization
codegen-units = 1       # Single codegen unit for max optimization
opt-level = 3           # Maximum optimization

[profile.bench]
inherits = "release"
```

### CI/CD Pipeline

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: cargo test --all-features
      - run: cargo clippy -- -D warnings
      - run: cargo fmt -- --check

  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: cargo bench
      - uses: benchmark-action/github-action-benchmark@v1
```

### Code Quality Tools

- **rustfmt**: Automatic code formatting
- **clippy**: Linting and best practices
- **cargo-audit**: Security vulnerability scanning
- **cargo-deny**: Dependency licensing checks
- **cargo-tarpaulin**: Code coverage

---

## 11. Complete Technology Stack Summary

```
┌─────────────────────────────────────────────────────────┐
│                   Samyama Graph Database                │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Language & Runtime                                     │
│  - Rust 1.75+                                           │
│  - Tokio async runtime                                  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Network Layer                                          │
│  - RESP Protocol (primary)                              │
│  - HTTP/REST (secondary)                                │
│  - gRPC (cluster internal)                              │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Query Engine                                           │
│  - OpenCypher parser (libcypher-parser)                 │
│  - Custom query planner & optimizer                     │
│  - Volcano iterator execution model                     │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Storage Engine                                         │
│  - In-Memory: Custom graph structures                   │
│  - Adjacency lists (HashMap-based)                      │
│  - Columnar property storage                            │
│  - RoaringBitmap indices                                │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Persistence                                            │
│  - RocksDB (LSM tree storage)                           │
│  - Write-Ahead Log (WAL)                                │
│  - Snapshots (background, incremental)                  │
│  - Compression (LZ4/Zstd)                               │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Distributed Systems (Phase 3+)                         │
│  - Consensus: Raft (openraft)                           │
│  - Replication: Async, multi-leader                     │
│  - Sharding: Graph-aware partitioning                   │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Serialization                                          │
│  - Cap'n Proto (zero-copy)                              │
│  - Apache Arrow (columnar data)                         │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Observability                                          │
│  - Metrics: Prometheus                                  │
│  - Tracing: OpenTelemetry                               │
│  - Logging: tracing (JSON structured)                   │
│  - Dashboards: Grafana                                  │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Development                                            │
│  - Build: Cargo                                         │
│  - Testing: cargo test + proptest + Criterion           │
│  - CI/CD: GitHub Actions                                │
│  - Code Quality: clippy, rustfmt, cargo-audit           │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Deployment                                             │
│  - Containers: Docker                                   │
│  - Orchestration: Kubernetes                            │
│  - Cloud: AWS/GCP/Azure agnostic                        │
└─────────────────────────────────────────────────────────┘
```

---

## 12. Key Dependencies (Cargo.toml)

```toml
[dependencies]
# Async runtime
tokio = { version = "1.35", features = ["full"] }
tokio-util = "0.7"

# Storage
rocksdb = "0.21"
roaring = "0.10"  # RoaringBitmap

# Serialization
capnp = "0.18"
arrow = "50.0"
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"

# Networking
bytes = "1.5"
prost = "0.12"  # For gRPC

# Query engine
# libcypher-parser-sys = "0.1"  # C bindings
pest = "2.7"  # If writing custom parser

# Distributed systems
openraft = "0.8"
async-raft = "0.7"

# Observability
prometheus = "0.13"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json"] }
tracing-opentelemetry = "0.21"
opentelemetry = "0.21"

# Utilities
anyhow = "1.0"
thiserror = "1.0"
dashmap = "5.5"  # Concurrent HashMap
parking_lot = "0.12"  # Better RwLock
crossbeam = "0.8"

# Security
argon2 = "0.5"  # Password hashing
rustls = "0.21"  # TLS

[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }
proptest = "1.4"
quickcheck = "1.0"
mockall = "0.12"
```

---

## 13. Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| **Rust learning curve** | - Hire 1-2 Rust experts initially<br>- 2-3 month onboarding for experienced engineers<br>- Excellent documentation/books available |
| **RocksDB complexity** | - Use recommended defaults initially<br>- Hire RocksDB expert consultant<br>- Reference TiKV's usage patterns |
| **Cypher parser complexity** | - Start with libcypher-parser (proven)<br>- Subset implementation first<br>- Gradual TCK compliance |
| **Raft consensus bugs** | - Use battle-tested openraft library<br>- Extensive testing<br>- Chaos engineering |
| **Performance not meeting targets** | - Early benchmarking (Week 4+)<br>- Profiling from day 1<br>- Gradual optimization |

---

## 14. Alternatives Considered and Rejected

### Language Alternatives
- **C++**: Memory safety issues, complex build system
- **Go**: GC pauses unacceptable for latency requirements
- **Java**: Memory overhead, GC pauses, warmup time
- **Zig**: Too immature (though promising for future)

### Storage Alternatives
- **Custom LSM tree**: Too much work, RocksDB is proven
- **LMDB**: Read-optimized, worse write performance
- **Sled**: Immature, less proven in production

### Consensus Alternatives
- **Multi-Paxos**: More complex than Raft
- **Viewstamped Replication**: Less tooling/libraries
- **Custom replication**: Too risky, bugs likely

### Serialization Alternatives
- **JSON**: Too slow, too large
- **MessagePack**: Not zero-copy
- **Protocol Buffers**: Not zero-copy

---

## Conclusion

This technology stack represents a **modern, high-performance, safe foundation** for building Samyama Graph Database:

✅ **Rust**: Best choice for systems programming in 2024+
✅ **RocksDB**: Battle-tested persistence
✅ **Tokio**: Industry-standard async runtime
✅ **Raft**: Proven distributed consensus
✅ **Cap'n Proto**: Zero-copy serialization
✅ **Prometheus**: Standard observability

**Trade-offs Accepted**:
- Rust learning curve (worth it for safety + performance)
- RocksDB complexity (worth it vs building custom)
- RESP protocol limitations (acceptable for graph use case)

**Estimated Time to Production-Ready v1.0**: 12-18 months with this stack.

---

**Document Version**: 1.0
**Last Updated**: 2025-10-14
**Status**: Technology Stack Specification
