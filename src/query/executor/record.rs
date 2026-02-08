//! Record structures for query execution
//!
//! Records flow through the Volcano iterator pipeline

use crate::graph::{Edge, Node, NodeId, EdgeId, EdgeType, PropertyValue, GraphStore};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// A single record flowing through the query pipeline
#[derive(Debug, Clone)]
pub struct Record {
    /// Variable bindings (variable name -> value)
    bindings: HashMap<String, Value>,
}

/// Value types that can be bound to variables
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
            Value::Null => { 3u8.hash(state); }
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
}
