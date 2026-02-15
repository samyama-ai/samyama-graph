# ADR-012: Late Materialization with NodeRef/EdgeRef

## Status
**Accepted**

## Date
2025-12-15

## Context

Query execution was cloning full `Node` and `Edge` objects at scan time, causing excessive memory allocation for large graphs. A typical query like `MATCH (n:Person) WHERE n.age > 30 RETURN n.name` would:

1. Clone every `Person` node (including all properties) into `Value::Node(id, node.clone())`
2. Filter most of them out in `FilterOperator`
3. Only use the `name` property in `ProjectOperator`

For a graph with 1M Person nodes each carrying 10 properties, this meant cloning ~1M full node objects just to return a single property from the ~10K that pass the filter. Most queries only need a few properties from each node, making full cloning wasteful.

Additionally, `JoinOperator` (used for multi-pattern MATCH queries) needed to compare nodes by identity, but `Value::Node` equality required comparing all properties -- both incorrect semantically and expensive computationally.

## Decision

**We will use late materialization with reference-based values throughout the query pipeline.**

### Reference Types

Scan operators produce lightweight reference values instead of cloned objects:

```rust
// Before: Full clone at scan time
Value::Node(NodeId, Node)        // Clones entire node with all properties
Value::Edge(EdgeId, Edge)        // Clones entire edge with all properties

// After: Reference only
Value::NodeRef(NodeId)                              // 8 bytes
Value::EdgeRef(EdgeId, NodeId, NodeId, EdgeType)    // ~40 bytes
```

### Lazy Property Resolution

Properties are resolved on demand via the graph store:

```rust
impl Value {
    pub fn resolve_property(&self, prop: &str, store: &GraphStore) -> Option<Value> {
        match self {
            Value::NodeRef(id) => store.get_node(*id)?.get_property(prop).map(Into::into),
            Value::EdgeRef(id, ..) => store.get_edge(*id)?.get_property(prop).map(Into::into),
            Value::Node(_, node) => node.get_property(prop).map(Into::into),
            _ => None,
        }
    }
}
```

### Materialization Points

Full materialization (cloning the entire node/edge) only happens when necessary:

| Operator | Input | Output | Materializes? |
|----------|-------|--------|---------------|
| `NodeScanOperator` | Store | `NodeRef(id)` | No |
| `ExpandOperator` | Store | `EdgeRef(id, src, tgt, type)` | No |
| `FilterOperator` | `NodeRef` | `NodeRef` (pass-through) | No |
| `JoinOperator` | `NodeRef` | `NodeRef` | No |
| `ProjectOperator` | `NodeRef` | `Node(id, node)` for `RETURN n` | Yes (only for variable expressions) |
| `ProjectOperator` | `NodeRef` | `Value::String` for `RETURN n.name` | No (uses resolve_property) |

### ExpandOperator Optimization

`ExpandOperator` uses `get_outgoing_edge_targets()` which returns `(EdgeId, NodeId, NodeId, &EdgeType)` tuples, avoiding any `Edge` clone:

```rust
fn expand(&mut self, source_id: NodeId) -> Vec<Record> {
    let targets = self.store.get_outgoing_edge_targets(source_id);
    targets.iter().map(|(eid, src, tgt, etype)| {
        let mut record = base_record.clone();
        record.set(edge_var, Value::EdgeRef(*eid, *src, *tgt, etype.clone()));
        record.set(target_var, Value::NodeRef(*tgt));
        record
    }).collect()
}
```

### Identity-Based Equality

`PartialEq` and `Hash` for `Value` compare by ID only, enabling efficient joins:

```rust
// These are considered equal for join purposes
Value::NodeRef(42) == Value::Node(42, any_node)  // true
Value::NodeRef(42) == Value::NodeRef(42)          // true
```

## Consequences

### Positive

- Reduced memory allocation by ~60% for typical queries (benchmarked on 100K-node graphs)
- O(1) property access via store lookup instead of scanning cloned property maps
- `PartialEq`/`Hash` compare by ID only, enabling efficient `JoinOperator` with `HashSet`-based matching
- `LIMIT` queries benefit most -- only N nodes are ever materialized regardless of scan size
- Pipeline stays lazy end-to-end, consistent with Volcano model (ADR-007)

### Negative

- Tests must use `resolve_property(prop, store)` instead of `as_node().1.get_property()` to access properties from scan results
- `JoinOperator` equality semantics require care: `NodeRef(id) == Node(id, _)` must hold true
- Store reference (`&GraphStore`) must be threaded through operators that resolve properties
- Debugging is slightly harder since `NodeRef` values don't show property data in debug output

### Neutral

- No change to the external query API or result format -- `RETURN n` still produces full node data
- Persistence layer is unaffected (operates on full objects)

## Alternatives Considered

### Alternative 1: Full Materialization (Status Quo)

Continue cloning full `Node`/`Edge` objects at scan time.

**Rejected because**:
- Excessive memory allocation for large graphs
- Wasted work for queries that filter most nodes or only access a few properties
- Incorrect equality semantics for joins (comparing all properties instead of identity)

### Alternative 2: Column-Oriented Storage Only

Store properties in columnar format and never materialize full objects.

**Rejected because**:
- Too complex for current phase -- requires redesigning `GraphStore` internals
- Not all access patterns benefit from columnar layout (e.g., `RETURN n` needs all properties)
- Can be layered on top of NodeRef approach later as an optimization

### Alternative 3: Materialization at Filter Operator

Materialize nodes before filtering so filters can access properties directly.

**Rejected because**:
- Filtering is the primary reduction step -- materializing before filtering defeats the purpose
- `resolve_property()` on `NodeRef` is O(1) via `HashMap` lookup, nearly as fast as direct property access

## Related Decisions

- [ADR-007: Volcano Iterator Model](./ADR-007-volcano-iterator-execution.md) - Late materialization preserves the lazy pull-based execution model
- [ADR-001: Use Rust](./ADR-001-use-rust-as-primary-language.md) - Rust's ownership model makes reference threading safe and explicit

---

**Last Updated**: 2025-12-15
**Status**: Accepted and Implemented
