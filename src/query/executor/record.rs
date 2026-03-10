//! # Records: The Data Unit of Query Execution
//!
//! A [`Record`] is a set of **variable bindings** -- a mapping from variable names (like
//! `n`, `r`, `m` in `MATCH (n)-[r]->(m)`) to [`Value`]s. It is the graph-database analog
//! of a "row" in a relational database, except that columns are named variables rather
//! than positional indices. Records flow through the Volcano iterator pipeline one at a
//! time, accumulating bindings as they pass through operators (e.g., `ExpandOperator` adds
//! the target node binding to an existing record that already contains the source node).
//!
//! ## Late Materialization (ADR-012)
//!
//! The most important optimization in this module is **late materialization**. Instead of
//! cloning an entire `Node` (with all its properties, labels, and metadata) when a scan
//! produces a result, we store only a `Value::NodeRef(id)` -- a 64-bit integer. The full
//! node data is resolved **on demand** via [`Value::resolve_property(prop, store)`] only
//! when a property is actually needed (e.g., in a WHERE filter or RETURN projection).
//!
//! This matters enormously for traversal queries. Consider `MATCH (a)-[:KNOWS]->(b)-[:KNOWS]->(c)`:
//! the ExpandOperator traverses through `b` nodes, but if the query only returns `c.name`,
//! the `b` nodes never need their properties loaded. Late materialization turns what would
//! be O(n * avg_properties) memory into O(n * 8 bytes).
//!
//! ## Semantic Equality: `NodeRef(id) == Node(id, _)`
//!
//! The [`Value`] enum has both lazy (`NodeRef`) and materialized (`Node`) variants for the
//! same logical entity. This creates a subtle correctness requirement: the `JoinOperator`
//! uses hash-based lookups to match records from two sides of a join. If the left side
//! produces `NodeRef(42)` and the right side produces `Node(42, <data>)`, they must be
//! considered **equal** and must produce the **same hash** -- otherwise the join silently
//! drops valid matches.
//!
//! This is why [`PartialEq`] and [`Hash`] are implemented **manually** instead of derived.
//! The derive macro would compare all fields (including the `Node` data), breaking the
//! semantic equivalence. The manual implementation compares only the identity (the `NodeId`),
//! and the hash function uses a discriminant tag (0 for nodes, 1 for edges) plus the ID,
//! ensuring the **hash consistency invariant**: if `a == b`, then `hash(a) == hash(b)`.
//!
//! [`RecordBatch`] is the final output container -- a vector of [`Record`]s plus column
//! names, returned to the caller after query execution completes.

use crate::graph::{Edge, Node, NodeId, EdgeId, EdgeType, PropertyValue, GraphStore};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// A single record flowing through the query pipeline
#[derive(Debug, Clone)]
pub struct Record {
    /// Variable bindings (variable name -> value)
    bindings: HashMap<String, Value>,
}

/// Value types that can be bound to variables in a query record.
///
/// The key design choice here is the **late materialization hierarchy**:
///
/// - **`NodeRef(id)`** -- a lazy reference. Stores only the 64-bit `NodeId`. Produced by
///   scan and expand operators. Extremely cheap to create (no heap allocation, no cloning).
///   Properties are resolved on demand via `resolve_property(prop, store)`.
///
/// - **`Node(id, node)`** -- a fully materialized node. Contains a clone of the `Node`
///   struct with all labels and properties. Produced by `ProjectOperator` when the RETURN
///   clause requests `RETURN n` (the entire node), triggering full materialization.
///
/// The same lazy/eager split exists for edges: `EdgeRef(id, src, tgt, type)` carries the
/// structural data (endpoints and type) without property clones, while `Edge(id, edge)`
/// is fully materialized.
///
/// `Property(PropertyValue)` wraps scalar values (strings, integers, floats, booleans,
/// datetimes, arrays, maps) that result from property access (`n.name`) or literal
/// expressions. `Path` stores ordered sequences of node/edge IDs for named path patterns
/// like `p = (a)-[]->(b)`. `Null` represents the absence of a value, following Cypher's
/// three-valued logic (true/false/null).
#[derive(Debug, Clone)]
pub enum Value {
    /// A fully materialized node
    Node(NodeId, Node),
    /// A lazy node reference (no property clone)
    NodeRef(NodeId),
    /// A fully materialized edge
    Edge(EdgeId, Edge),
    /// A lazy edge reference (structural data only, no property clone)
    EdgeRef(EdgeId, NodeId, NodeId, EdgeType),
    /// A property value
    Property(PropertyValue),
    /// A path (ordered sequence of node/edge IDs)
    Path {
        nodes: Vec<NodeId>,
        edges: Vec<EdgeId>,
    },
    /// Null
    Null,
}

// NodeRef(id) == Node(id, _) — compare by ID only for nodes/edges
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // Node variants compare by ID
            (Value::Node(id1, _), Value::Node(id2, _)) => id1 == id2,
            (Value::NodeRef(id1), Value::NodeRef(id2)) => id1 == id2,
            (Value::Node(id1, _), Value::NodeRef(id2)) | (Value::NodeRef(id2), Value::Node(id1, _)) => id1 == id2,
            // Edge variants compare by ID
            (Value::Edge(id1, _), Value::Edge(id2, _)) => id1 == id2,
            (Value::EdgeRef(id1, ..), Value::EdgeRef(id2, ..)) => id1 == id2,
            (Value::Edge(id1, _), Value::EdgeRef(id2, ..)) | (Value::EdgeRef(id2, ..), Value::Edge(id1, _)) => id1 == id2,
            // Property and Null
            (Value::Property(p1), Value::Property(p2)) => p1 == p2,
            // Path
            (Value::Path { nodes: n1, edges: e1 }, Value::Path { nodes: n2, edges: e2 }) => n1 == n2 && e1 == e2,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Use semantic tags so NodeRef and Node hash the same
        match self {
            Value::Node(id, _) | Value::NodeRef(id) => { 0u8.hash(state); id.hash(state); }
            Value::Edge(id, _) | Value::EdgeRef(id, ..) => { 1u8.hash(state); id.hash(state); }
            Value::Property(p) => { 2u8.hash(state); p.hash(state); }
            Value::Path { nodes, edges } => { 3u8.hash(state); nodes.hash(state); edges.hash(state); }
            Value::Null => { 4u8.hash(state); }
        }
    }
}

impl Record {
    /// Create a new empty record
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    /// Bind a variable to a value
    pub fn bind(&mut self, variable: String, value: Value) {
        self.bindings.insert(variable, value);
    }

    /// Get a bound value
    pub fn get(&self, variable: &str) -> Option<&Value> {
        self.bindings.get(variable)
    }

    /// Get all bindings
    pub fn bindings(&self) -> &HashMap<String, Value> {
        &self.bindings
    }

    /// Check if a variable is bound
    pub fn has(&self, variable: &str) -> bool {
        self.bindings.contains_key(variable)
    }

    /// Merge another record into this one
    pub fn merge(&mut self, other: Record) {
        self.bindings.extend(other.bindings);
    }

    /// Clone with only specified variables
    pub fn project(&self, variables: &[String]) -> Record {
        let mut new_record = Record::new();
        for var in variables {
            if let Some(value) = self.bindings.get(var) {
                new_record.bind(var.clone(), value.clone());
            }
        }
        new_record
    }
}

impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

impl Value {
    /// Get as node if this is a fully materialized node value
    pub fn as_node(&self) -> Option<(NodeId, &Node)> {
        match self {
            Value::Node(id, node) => Some((*id, node)),
            _ => None,
        }
    }

    /// Get as edge if this is a fully materialized edge value
    pub fn as_edge(&self) -> Option<(EdgeId, &Edge)> {
        match self {
            Value::Edge(id, edge) => Some((*id, edge)),
            _ => None,
        }
    }

    /// Get as property if this is a property value
    pub fn as_property(&self) -> Option<&PropertyValue> {
        match self {
            Value::Property(prop) => Some(prop),
            _ => None,
        }
    }

    /// Check if this is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Extract NodeId from any node variant (Node or NodeRef)
    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            Value::Node(id, _) | Value::NodeRef(id) => Some(*id),
            _ => None,
        }
    }

    /// Extract EdgeId from any edge variant (Edge or EdgeRef)
    pub fn edge_id(&self) -> Option<EdgeId> {
        match self {
            Value::Edge(id, _) => Some(*id),
            Value::EdgeRef(id, ..) => Some(*id),
            _ => None,
        }
    }

    /// Extract edge endpoints from any edge variant
    pub fn edge_endpoints(&self) -> Option<(NodeId, NodeId)> {
        match self {
            Value::Edge(_, edge) => Some((edge.source, edge.target)),
            Value::EdgeRef(_, src, tgt, _) => Some((*src, *tgt)),
            _ => None,
        }
    }

    /// Extract edge type from any edge variant
    pub fn edge_type(&self) -> Option<&EdgeType> {
        match self {
            Value::Edge(_, edge) => Some(&edge.edge_type),
            Value::EdgeRef(_, _, _, et) => Some(et),
            _ => None,
        }
    }

    /// Check if this represents a node (Node or NodeRef)
    pub fn is_node(&self) -> bool {
        matches!(self, Value::Node(..) | Value::NodeRef(..))
    }

    /// Check if this represents an edge (Edge or EdgeRef)
    pub fn is_edge(&self) -> bool {
        matches!(self, Value::Edge(..) | Value::EdgeRef(..))
    }

    /// Materialize a NodeRef into a full Node by looking it up in the store.
    /// Returns self unchanged if already materialized or not a node variant.
    pub fn materialize_node(self, store: &GraphStore) -> Self {
        match self {
            Value::NodeRef(id) => {
                if let Some(node) = store.get_node(id) {
                    Value::Node(id, node.clone())
                } else {
                    Value::Null
                }
            }
            other => other,
        }
    }

    /// Materialize an EdgeRef into a full Edge by looking it up in the store.
    /// Returns self unchanged if already materialized or not an edge variant.
    pub fn materialize_edge(self, store: &GraphStore) -> Self {
        match self {
            Value::EdgeRef(id, ..) => {
                if let Some(edge) = store.get_edge(id) {
                    Value::Edge(id, edge.clone())
                } else {
                    Value::Null
                }
            }
            other => other,
        }
    }

    /// Resolve a property from this value, using columnar store first, then
    /// falling back to materialized node/edge properties or store lookup for refs.
    pub fn resolve_property(&self, property: &str, store: &GraphStore) -> PropertyValue {
        match self {
            Value::Node(id, node) => {
                let prop = store.node_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else {
                    node.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                }
            }
            Value::NodeRef(id) => {
                let prop = store.node_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else if let Some(node) = store.get_node(*id) {
                    node.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                } else {
                    PropertyValue::Null
                }
            }
            Value::Edge(id, edge) => {
                let prop = store.edge_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else {
                    edge.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                }
            }
            Value::EdgeRef(id, ..) => {
                let prop = store.edge_columns.get_property(id.as_u64() as usize, property);
                if !prop.is_null() {
                    prop
                } else if let Some(edge) = store.get_edge(*id) {
                    edge.get_property(property).cloned().unwrap_or(PropertyValue::Null)
                } else {
                    PropertyValue::Null
                }
            }
            _ => PropertyValue::Null,
        }
    }
}

/// A batch of records (result set)
#[derive(Debug)]
pub struct RecordBatch {
    /// All records in the batch
    pub records: Vec<Record>,
    /// Column names for the result
    pub columns: Vec<String>,
}

impl RecordBatch {
    /// Create a new empty batch
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            records: Vec::new(),
            columns,
        }
    }

    /// Get number of records
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Add a record
    pub fn push(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Get a record by index
    pub fn get(&self, index: usize) -> Option<&Record> {
        self.records.get(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::Label;

    #[test]
    fn test_record_creation() {
        let record = Record::new();
        assert_eq!(record.bindings().len(), 0);
    }

    #[test]
    fn test_record_binding() {
        let mut record = Record::new();
        let node = Node::new(NodeId::new(1), Label::new("Person"));

        record.bind("n".to_string(), Value::Node(NodeId::new(1), node));

        assert!(record.has("n"));
        assert!(record.get("n").is_some());
    }

    #[test]
    fn test_record_merge() {
        let mut record1 = Record::new();
        let mut record2 = Record::new();

        record1.bind("a".to_string(), Value::Property(PropertyValue::Integer(1)));
        record2.bind("b".to_string(), Value::Property(PropertyValue::Integer(2)));

        record1.merge(record2);

        assert!(record1.has("a"));
        assert!(record1.has("b"));
    }

    #[test]
    fn test_record_project() {
        let mut record = Record::new();
        record.bind("a".to_string(), Value::Property(PropertyValue::Integer(1)));
        record.bind("b".to_string(), Value::Property(PropertyValue::Integer(2)));
        record.bind("c".to_string(), Value::Property(PropertyValue::Integer(3)));

        let projected = record.project(&vec!["a".to_string(), "c".to_string()]);

        assert!(projected.has("a"));
        assert!(!projected.has("b"));
        assert!(projected.has("c"));
    }

    #[test]
    fn test_value_types() {
        let node_val = Value::Node(NodeId::new(1), Node::new(NodeId::new(1), Label::new("Test")));
        assert!(node_val.as_node().is_some());
        assert!(node_val.as_edge().is_none());

        let prop_val = Value::Property(PropertyValue::String("test".to_string()));
        assert!(prop_val.as_property().is_some());

        let null_val = Value::Null;
        assert!(null_val.is_null());
    }

    #[test]
    fn test_record_batch() {
        let mut batch = RecordBatch::new(vec!["n".to_string(), "m".to_string()]);
        assert_eq!(batch.len(), 0);
        assert!(batch.is_empty());

        batch.push(Record::new());
        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    // ========== Batch 5: Additional Record Tests ==========

    #[test]
    fn test_as_edge() {
        let edge = crate::graph::Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("KNOWS"),
        );
        let val = Value::Edge(EdgeId::new(1), edge);
        let (eid, e) = val.as_edge().unwrap();
        assert_eq!(eid, EdgeId::new(1));
        assert_eq!(e.source, NodeId::new(10));
        assert_eq!(e.target, NodeId::new(20));

        // Non-edge variants return None
        assert!(Value::Null.as_edge().is_none());
        assert!(Value::NodeRef(NodeId::new(1)).as_edge().is_none());
    }

    #[test]
    fn test_node_id() {
        // From Node
        let node = Node::new(NodeId::new(5), Label::new("Person"));
        let val = Value::Node(NodeId::new(5), node);
        assert_eq!(val.node_id(), Some(NodeId::new(5)));

        // From NodeRef
        let val = Value::NodeRef(NodeId::new(7));
        assert_eq!(val.node_id(), Some(NodeId::new(7)));

        // Non-node variants
        assert!(Value::Null.node_id().is_none());
        assert!(Value::Property(PropertyValue::Integer(42)).node_id().is_none());
    }

    #[test]
    fn test_edge_id() {
        // From Edge
        let edge = crate::graph::Edge::new(
            EdgeId::new(3),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        let val = Value::Edge(EdgeId::new(3), edge);
        assert_eq!(val.edge_id(), Some(EdgeId::new(3)));

        // From EdgeRef
        let val = Value::EdgeRef(
            EdgeId::new(4),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(val.edge_id(), Some(EdgeId::new(4)));

        // Non-edge
        assert!(Value::Null.edge_id().is_none());
    }

    #[test]
    fn test_edge_endpoints() {
        // From Edge
        let edge = crate::graph::Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("E"),
        );
        let val = Value::Edge(EdgeId::new(1), edge);
        assert_eq!(val.edge_endpoints(), Some((NodeId::new(10), NodeId::new(20))));

        // From EdgeRef
        let val = Value::EdgeRef(
            EdgeId::new(1),
            NodeId::new(30),
            NodeId::new(40),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(val.edge_endpoints(), Some((NodeId::new(30), NodeId::new(40))));

        // Non-edge
        assert!(Value::Null.edge_endpoints().is_none());
    }

    #[test]
    fn test_edge_type_accessor() {
        let edge = crate::graph::Edge::new(
            EdgeId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("KNOWS"),
        );
        let val = Value::Edge(EdgeId::new(1), edge);
        assert_eq!(val.edge_type().unwrap().as_str(), "KNOWS");

        let val = Value::EdgeRef(
            EdgeId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            crate::graph::EdgeType::new("LIKES"),
        );
        assert_eq!(val.edge_type().unwrap().as_str(), "LIKES");

        assert!(Value::Null.edge_type().is_none());
    }

    #[test]
    fn test_is_node_is_edge() {
        let node = Node::new(NodeId::new(1), Label::new("A"));
        assert!(Value::Node(NodeId::new(1), node).is_node());
        assert!(Value::NodeRef(NodeId::new(1)).is_node());
        assert!(!Value::Null.is_node());
        assert!(!Value::Property(PropertyValue::Integer(1)).is_node());

        let edge = crate::graph::Edge::new(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert!(Value::Edge(EdgeId::new(1), edge).is_edge());
        assert!(Value::EdgeRef(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        ).is_edge());
        assert!(!Value::Null.is_edge());
    }

    #[test]
    fn test_materialize_node() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.get_node_mut(id).unwrap().set_property(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );

        // NodeRef materializes to Node
        let val = Value::NodeRef(id).materialize_node(&store);
        match &val {
            Value::Node(nid, node) => {
                assert_eq!(*nid, id);
                assert!(node.labels.contains(&Label::new("Person")));
            }
            _ => panic!("Expected Value::Node after materialization"),
        }

        // Already materialized stays the same
        let node = store.get_node(id).unwrap().clone();
        let val = Value::Node(id, node).materialize_node(&store);
        assert!(matches!(val, Value::Node(..)));

        // Non-existent NodeRef becomes Null
        let val = Value::NodeRef(NodeId::new(9999)).materialize_node(&store);
        assert!(val.is_null());

        // Non-node value is returned unchanged
        let val = Value::Property(PropertyValue::Integer(42)).materialize_node(&store);
        assert!(matches!(val, Value::Property(..)));
    }

    #[test]
    fn test_materialize_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let eid = store.create_edge(a, b, "KNOWS").unwrap();

        // EdgeRef materializes to Edge
        let val = Value::EdgeRef(
            eid, a, b, crate::graph::EdgeType::new("KNOWS"),
        ).materialize_edge(&store);
        match &val {
            Value::Edge(id, edge) => {
                assert_eq!(*id, eid);
                assert_eq!(edge.source, a);
                assert_eq!(edge.target, b);
            }
            _ => panic!("Expected Value::Edge after materialization"),
        }

        // Non-existent EdgeRef becomes Null
        let val = Value::EdgeRef(
            EdgeId::new(9999), a, b, crate::graph::EdgeType::new("X"),
        ).materialize_edge(&store);
        assert!(val.is_null());

        // Non-edge value is returned unchanged
        let val = Value::Null.materialize_edge(&store);
        assert!(val.is_null());
    }

    #[test]
    fn test_resolve_property_node() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.get_node_mut(id).unwrap().set_property(
            "name".to_string(),
            PropertyValue::String("Alice".to_string()),
        );

        // Resolve from Node (materialized)
        let node = store.get_node(id).unwrap().clone();
        let val = Value::Node(id, node);
        let prop = val.resolve_property("name", &store);
        assert_eq!(prop, PropertyValue::String("Alice".to_string()));

        // Missing property returns Null
        let prop = val.resolve_property("missing", &store);
        assert_eq!(prop, PropertyValue::Null);
    }

    #[test]
    fn test_resolve_property_noderef() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.get_node_mut(id).unwrap().set_property(
            "age".to_string(),
            PropertyValue::Integer(30),
        );

        let val = Value::NodeRef(id);
        let prop = val.resolve_property("age", &store);
        assert_eq!(prop, PropertyValue::Integer(30));

        // Non-existent NodeRef
        let val = Value::NodeRef(NodeId::new(9999));
        let prop = val.resolve_property("age", &store);
        assert_eq!(prop, PropertyValue::Null);
    }

    #[test]
    fn test_resolve_property_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");

        let mut props = std::collections::HashMap::new();
        props.insert("since".to_string(), PropertyValue::Integer(2020));
        let eid = store.create_edge_with_properties(a, b, "KNOWS", props).unwrap();

        // From Edge
        let edge = store.get_edge(eid).unwrap().clone();
        let val = Value::Edge(eid, edge);
        let prop = val.resolve_property("since", &store);
        assert_eq!(prop, PropertyValue::Integer(2020));
    }

    #[test]
    fn test_resolve_property_edgeref() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");

        let mut props = std::collections::HashMap::new();
        props.insert("weight".to_string(), PropertyValue::Float(0.5));
        let eid = store.create_edge_with_properties(a, b, "KNOWS", props).unwrap();

        let val = Value::EdgeRef(eid, a, b, crate::graph::EdgeType::new("KNOWS"));
        let prop = val.resolve_property("weight", &store);
        assert_eq!(prop, PropertyValue::Float(0.5));

        // Non-existent EdgeRef
        let val = Value::EdgeRef(
            EdgeId::new(9999), a, b, crate::graph::EdgeType::new("X"),
        );
        let prop = val.resolve_property("weight", &store);
        assert_eq!(prop, PropertyValue::Null);
    }

    #[test]
    fn test_resolve_property_non_node_edge() {
        let store = GraphStore::new();
        let val = Value::Null;
        assert_eq!(val.resolve_property("anything", &store), PropertyValue::Null);

        let val = Value::Property(PropertyValue::Integer(42));
        assert_eq!(val.resolve_property("x", &store), PropertyValue::Null);
    }

    #[test]
    fn test_record_batch_get() {
        let mut batch = RecordBatch::new(vec!["n".to_string()]);
        let mut r1 = Record::new();
        r1.bind("n".to_string(), Value::Property(PropertyValue::Integer(1)));
        let mut r2 = Record::new();
        r2.bind("n".to_string(), Value::Property(PropertyValue::Integer(2)));
        batch.push(r1);
        batch.push(r2);

        assert!(batch.get(0).is_some());
        assert!(batch.get(1).is_some());
        assert!(batch.get(2).is_none()); // out of bounds

        let r = batch.get(0).unwrap();
        assert_eq!(
            r.get("n").unwrap().as_property(),
            Some(&PropertyValue::Integer(1))
        );
    }

    #[test]
    fn test_record_bindings() {
        let mut r = Record::new();
        r.bind("x".to_string(), Value::Property(PropertyValue::Integer(1)));
        r.bind("y".to_string(), Value::Null);

        let bindings = r.bindings();
        assert_eq!(bindings.len(), 2);
        assert!(bindings.contains_key("x"));
        assert!(bindings.contains_key("y"));
    }

    #[test]
    fn test_record_default() {
        let r = Record::default();
        assert_eq!(r.bindings().len(), 0);
    }

    #[test]
    fn test_value_partial_eq_cross_variant() {
        // Node == NodeRef with same ID
        let node = Node::new(NodeId::new(5), Label::new("A"));
        let v1 = Value::Node(NodeId::new(5), node.clone());
        let v2 = Value::NodeRef(NodeId::new(5));
        assert_eq!(v1, v2);
        assert_eq!(v2, v1);

        // Different IDs
        let v3 = Value::NodeRef(NodeId::new(6));
        assert_ne!(v1, v3);

        // Edge == EdgeRef with same ID
        let edge = crate::graph::Edge::new(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        let ev1 = Value::Edge(EdgeId::new(1), edge);
        let ev2 = Value::EdgeRef(
            EdgeId::new(1), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(ev1, ev2);
        assert_eq!(ev2, ev1);

        // Different types don't equal
        assert_ne!(v1, ev1);
        assert_ne!(Value::Null, v1);

        // Path equality
        let p1 = Value::Path { nodes: vec![NodeId::new(1)], edges: vec![EdgeId::new(1)] };
        let p2 = Value::Path { nodes: vec![NodeId::new(1)], edges: vec![EdgeId::new(1)] };
        let p3 = Value::Path { nodes: vec![NodeId::new(2)], edges: vec![EdgeId::new(1)] };
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_value_hash_cross_variant() {
        use std::collections::hash_map::DefaultHasher;

        fn hash_value(v: &Value) -> u64 {
            let mut hasher = DefaultHasher::new();
            v.hash(&mut hasher);
            hasher.finish()
        }

        // Node and NodeRef with same ID should hash the same
        let node = Node::new(NodeId::new(5), Label::new("A"));
        let v1 = Value::Node(NodeId::new(5), node);
        let v2 = Value::NodeRef(NodeId::new(5));
        assert_eq!(hash_value(&v1), hash_value(&v2));

        // Edge and EdgeRef with same ID should hash the same
        let edge = crate::graph::Edge::new(
            EdgeId::new(3), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        let ev1 = Value::Edge(EdgeId::new(3), edge);
        let ev2 = Value::EdgeRef(
            EdgeId::new(3), NodeId::new(1), NodeId::new(2),
            crate::graph::EdgeType::new("E"),
        );
        assert_eq!(hash_value(&ev1), hash_value(&ev2));

        // Different variant types should have different hashes
        assert_ne!(hash_value(&v1), hash_value(&ev1));
        assert_ne!(hash_value(&Value::Null), hash_value(&v1));
    }
}
