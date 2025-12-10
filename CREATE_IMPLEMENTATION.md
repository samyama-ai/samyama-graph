# Samyama Graph Database: CREATE Support Implementation

## Overview

This document describes the implementation of **Cypher CREATE support** in Samyama Graph Database. This feature allows users to create nodes and edges using standard OpenCypher syntax via the RESP protocol.

## Summary of Changes

| File | Changes |
|------|---------|
| `src/query/executor/mod.rs` | Added `MutQueryExecutor` for write operations |
| `src/query/executor/operator.rs` | Added `CreateNodesAndEdgesOperator`, fixed property setting |
| `src/query/executor/planner.rs` | Added `plan_create_only()` function, edge planning logic |
| `src/query/mod.rs` | Added `execute_mut()` method, comprehensive tests |
| `.gitignore` | Minor updates |

## Detailed Changes

---

### 1. `src/query/executor/mod.rs` - Mutable Query Executor

#### What Changed
Added a new `MutQueryExecutor` struct that takes a **mutable reference** to the `GraphStore`, enabling write operations.

#### Why It Was Needed
The original `QueryExecutor` only took an immutable reference (`&GraphStore`), which meant it could only read data. CREATE operations need to modify the graph, requiring mutable access.

#### Code Added

```rust
/// Query executor for write queries (CREATE, DELETE, SET, etc.)
/// Takes mutable reference to GraphStore to allow modifications
pub struct MutQueryExecutor<'a> {
    store: &'a mut GraphStore,
    planner: QueryPlanner,
}

impl<'a> MutQueryExecutor<'a> {
    /// Create a new mutable query executor for write operations
    pub fn new(store: &'a mut GraphStore) -> Self {
        Self {
            store,
            planner: QueryPlanner::new(),
        }
    }

    /// Execute a query (read or write) and return results
    pub fn execute(&mut self, query: &Query) -> ExecutionResult<RecordBatch> {
        let plan = {
            let store_ref: &GraphStore = self.store;
            self.planner.plan(query, store_ref)?
        };
        self.execute_plan_mut(plan)
    }

    fn execute_plan_mut(&mut self, mut plan: ExecutionPlan) -> ExecutionResult<RecordBatch> {
        let mut records = Vec::new();
        // Use next_mut() which allows operators to modify the graph store
        while let Some(record) = plan.root.next_mut(self.store)? {
            records.push(record);
        }
        Ok(RecordBatch {
            records,
            columns: plan.output_columns,
        })
    }
}
```

#### Key Design Decisions
- **Separation of concerns**: Read-only queries use `QueryExecutor`, write queries use `MutQueryExecutor`
- **Safety**: The read-only executor now rejects write queries with a clear error message
- **Volcano model preserved**: Both executors use the same pull-based iterator pattern

---

### 2. `src/query/executor/operator.rs` - Physical Operators

#### What Changed
1. Added `CreateNodesAndEdgesOperator` - a combined operator for CREATE patterns with edges
2. Fixed property setting to use the proper `set_property()` method

#### Why It Was Needed
When creating a pattern like `CREATE (a:Person)-[:KNOWS]->(b:Person)`:
1. Nodes must be created first
2. Edges must be created after nodes exist (to get their IDs)
3. The execution must happen in a single atomic operation

#### Code Added - CreateNodesAndEdgesOperator

```rust
/// Combined operator for CREATE patterns with both nodes and edges
/// Example: CREATE (a:Person)-[:KNOWS]->(b:Person)
pub struct CreateNodesAndEdgesOperator {
    node_operator: OperatorBox,
    edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)>,
    var_to_node_id: HashMap<String, NodeId>,
    created_edges: Vec<(EdgeId, Edge, Option<String>)>,
    phase: usize,  // 0 = creating nodes, 1 = creating edges, 2 = returning results
    result_index: usize,
    results: Vec<(Option<String>, Value)>,
}
```

#### Execution Phases

The operator executes in three phases:

```
Phase 0: Create Nodes
    ┌─────────────────────────────────────────┐
    │ For each node in pattern:               │
    │   1. Create node in GraphStore          │
    │   2. Map variable name → NodeId         │
    │   3. Store in results for return        │
    └─────────────────────────────────────────┘
                      │
                      ▼
Phase 1: Create Edges
    ┌─────────────────────────────────────────┐
    │ For each edge in pattern:               │
    │   1. Look up source NodeId by variable  │
    │   2. Look up target NodeId by variable  │
    │   3. Create edge between them           │
    │   4. Set edge properties                │
    └─────────────────────────────────────────┘
                      │
                      ▼
Phase 2: Return Results
    ┌─────────────────────────────────────────┐
    │ Return created nodes and edges          │
    │ as Record objects (Volcano iterator)    │
    └─────────────────────────────────────────┘
```

#### Bug Fix - Property Setting

Changed from direct property access to using the setter method:

```rust
// Before (incorrect - properties field was private)
node.properties.set(key.clone(), value.clone());

// After (correct - uses public API)
node.set_property(key.clone(), value.clone());
```

---

### 3. `src/query/executor/planner.rs` - Query Planner

#### What Changed
1. Added `is_write` flag to `ExecutionPlan`
2. Added `plan_create_only()` function for CREATE-only queries
3. Modified `plan()` to route CREATE queries appropriately

#### Why It Was Needed
The planner needed to:
- Distinguish between read and write queries
- Handle CREATE queries that have no MATCH clause
- Parse CREATE patterns with edges and properties

#### Code Added - ExecutionPlan.is_write

```rust
pub struct ExecutionPlan {
    pub root: OperatorBox,
    pub output_columns: Vec<String>,
    /// Whether this plan contains write operations (CREATE/DELETE/SET)
    pub is_write: bool,
}
```

#### Code Added - plan_create_only()

```rust
fn plan_create_only(&self, create_clause: &CreateClause) -> ExecutionResult<ExecutionPlan> {
    let pattern = &create_clause.pattern;

    let mut nodes_to_create: Vec<(Vec<Label>, HashMap<String, PropertyValue>, Option<String>)> = Vec::new();
    let mut edges_to_create: Vec<(String, String, EdgeType, HashMap<String, PropertyValue>, Option<String>)> = Vec::new();
    let mut output_columns: Vec<String> = Vec::new();

    for path in &pattern.paths {
        // Process start node
        let start = &path.start;
        nodes_to_create.push((start.labels.clone(), start.properties.clone().unwrap_or_default(), start.variable.clone()));

        let mut current_source_var = start.variable.clone();

        // Process path segments (edges and target nodes)
        for segment in &path.segments {
            let node = &segment.node;
            nodes_to_create.push((node.labels.clone(), node.properties.clone().unwrap_or_default(), node.variable.clone()));

            // Extract edge info and add to edges_to_create
            let edge = &segment.edge;
            if let (Some(source), Some(target)) = (&current_source_var, &node.variable) {
                edges_to_create.push((source.clone(), target.clone(), edge_type, edge_properties, edge.variable.clone()));
            }

            current_source_var = node.variable.clone();
        }
    }

    // Build operator chain
    let node_operator = Box::new(CreateNodeOperator::new(nodes_to_create));
    let final_operator = if edges_to_create.is_empty() {
        node_operator
    } else {
        Box::new(CreateNodesAndEdgesOperator::new(node_operator, edges_to_create))
    };

    Ok(ExecutionPlan {
        root: final_operator,
        output_columns,
        is_write: true,
    })
}
```

#### Pattern Parsing Logic

```
Input: CREATE (a:Person {name: "Alice"})-[:KNOWS {since: 2020}]->(b:Person {name: "Bob"})

Parsed as:
┌─────────────────────────────────────────────────────────────────┐
│ Path[0]:                                                        │
│   start_node:                                                   │
│     variable: "a"                                               │
│     labels: ["Person"]                                          │
│     properties: {name: "Alice"}                                 │
│                                                                 │
│   segments[0]:                                                  │
│     edge:                                                       │
│       types: ["KNOWS"]                                          │
│       properties: {since: 2020}                                 │
│       direction: Outgoing                                       │
│     node:                                                       │
│       variable: "b"                                             │
│       labels: ["Person"]                                        │
│       properties: {name: "Bob"}                                 │
└─────────────────────────────────────────────────────────────────┘

Produces:
  nodes_to_create = [
    (["Person"], {name: "Alice"}, Some("a")),
    (["Person"], {name: "Bob"}, Some("b"))
  ]
  edges_to_create = [
    ("a", "b", KNOWS, {since: 2020}, None)
  ]
```

---

### 4. `src/query/mod.rs` - Query Engine API

#### What Changed
1. Added `execute_mut()` method for write queries
2. Exported `MutQueryExecutor` from the module
3. Added comprehensive tests for CREATE operations

#### Code Added - execute_mut()

```rust
impl QueryEngine {
    /// Parse and execute a read-only Cypher query (MATCH, RETURN, etc.)
    pub fn execute(&self, query_str: &str, store: &GraphStore) -> Result<RecordBatch, _> {
        let query = parse_query(query_str)?;
        let executor = QueryExecutor::new(store);
        executor.execute(&query)
    }

    /// Parse and execute a write Cypher query (CREATE, DELETE, SET, etc.)
    pub fn execute_mut(&self, query_str: &str, store: &mut GraphStore) -> Result<RecordBatch, _> {
        let query = parse_query(query_str)?;
        let mut executor = MutQueryExecutor::new(store);
        executor.execute(&query)
    }
}
```

#### Tests Added

| Test | Description |
|------|-------------|
| `test_create_single_node` | Basic `CREATE (n:Person)` |
| `test_create_node_with_properties` | `CREATE (n:Person {name: "Alice", age: 30})` |
| `test_create_multiple_nodes` | Multiple sequential CREATE operations |
| `test_create_returns_error_on_readonly_executor` | Verifies read-only executor rejects CREATE |
| `test_create_edge_simple` | `CREATE (a:Person)-[:KNOWS]->(b:Person)` |
| `test_create_edge_with_properties` | `CREATE (a)-[:FRIENDS {since: 2020}]->(b)` |
| `test_create_chain_pattern` | `CREATE (a)-[:KNOWS]->(b)-[:LIKES]->(c)` |

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           QueryEngine                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  execute(query, &store)      │  execute_mut(query, &mut store)      ││
│  │  (read-only)                 │  (read/write)                        ││
│  └─────────────┬────────────────┴─────────────────┬────────────────────┘│
└────────────────┼──────────────────────────────────┼─────────────────────┘
                 │                                  │
                 ▼                                  ▼
        ┌────────────────┐                ┌────────────────────┐
        │ QueryExecutor  │                │ MutQueryExecutor   │
        │ (&GraphStore)  │                │ (&mut GraphStore)  │
        └───────┬────────┘                └─────────┬──────────┘
                │                                   │
                └───────────────┬───────────────────┘
                                │
                                ▼
                        ┌───────────────┐
                        │ QueryPlanner  │
                        │   .plan()     │
                        └───────┬───────┘
                                │
                ┌───────────────┼───────────────┐
                │               │               │
                ▼               ▼               ▼
         ┌──────────┐   ┌──────────────┐  ┌─────────────────────────┐
         │ MATCH    │   │ CREATE node  │  │ CREATE node + edge      │
         │ queries  │   │ only         │  │                         │
         └────┬─────┘   └──────┬───────┘  └───────────┬─────────────┘
              │                │                      │
              ▼                ▼                      ▼
    ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────────────┐
    │ NodeScanOperator │ │CreateNodeOperator│ │CreateNodesAndEdgesOperator│
    │ FilterOperator   │ │                  │ │ (nodes first, then edges) │
    │ ExpandOperator   │ │                  │ │                          │
    │ ProjectOperator  │ │                  │ │                          │
    │ LimitOperator    │ │                  │ │                          │
    └──────────────────┘ └──────────────────┘ └──────────────────────────┘
```

---

## Supported Cypher Syntax

### CREATE Nodes

```cypher
-- Simple node
CREATE (n:Person)

-- Node with properties
CREATE (n:Person {name: "Alice", age: 30})

-- Node with multiple labels
CREATE (n:Person:Employee {name: "Bob"})
```

### CREATE Edges

```cypher
-- Simple edge between new nodes
CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})

-- Edge with properties
CREATE (a:Person)-[:FRIENDS {since: 2020, strength: 0.9}]->(b:Person)

-- Chain pattern (multiple edges)
CREATE (a:Person)-[:KNOWS]->(b:Person)-[:LIKES]->(c:Movie)
```

---

## Test Results

```
test query::tests::test_create_single_node ... ok
test query::tests::test_create_node_with_properties ... ok
test query::tests::test_create_multiple_nodes ... ok
test query::tests::test_create_returns_error_on_readonly_executor ... ok
test query::tests::test_create_edge_simple ... ok
test query::tests::test_create_edge_with_properties ... ok
test query::tests::test_create_chain_pattern ... ok

Total: 130 tests passed (128 unit + 1 integration + 1 doc)
```

---

## What's NOT Implemented (Future Work)

| Feature | Status | Notes |
|---------|--------|-------|
| DELETE | Not implemented | Would need `DeleteNodeOperator`, `DeleteEdgeOperator` |
| SET | Not implemented | Would need `SetPropertyOperator` |
| MERGE | Not implemented | CREATE or MATCH semantics |
| MATCH + CREATE | Not implemented | e.g., `MATCH (a) CREATE (a)-[:KNOWS]->(b)` |

---

## Usage Examples

### Rust API

```rust
use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

let mut store = GraphStore::new();
let engine = QueryEngine::new();

// Create nodes and edge
engine.execute_mut(
    r#"CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"})"#,
    &mut store
)?;

// Query the created data
let result = engine.execute(
    "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name",
    &store
)?;
```

### RESP Protocol (redis-cli)

```bash
# Start Samyama server
cargo run --release

# In another terminal
redis-cli

# Create nodes and edge
GRAPH.QUERY mygraph "CREATE (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"})"

# Query the relationship
GRAPH.QUERY mygraph "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name"
```

---

## Conclusion

The CREATE implementation follows Samyama's existing architecture patterns:
- **Volcano iterator model** for operator execution
- **Separation of read/write** through distinct executor types
- **AST-based planning** with operator composition
- **Comprehensive testing** for all supported patterns

This implementation enables Samyama to be used as a complete graph database for knowledge graph applications, supporting both data ingestion (CREATE) and querying (MATCH/RETURN).
