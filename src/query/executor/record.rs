//! Record structures for query execution
//!
//! Records flow through the Volcano iterator pipeline

use crate::graph::{Edge, Node, NodeId, EdgeId, PropertyValue};
use std::collections::HashMap;

/// A single record flowing through the query pipeline
#[derive(Debug, Clone)]
pub struct Record {
    /// Variable bindings (variable name -> value)
    bindings: HashMap<String, Value>,
}

/// Value types that can be bound to variables
#[derive(Debug, Clone)]
pub enum Value {
    /// A node
    Node(NodeId, Node),
    /// An edge
    Edge(EdgeId, Edge),
    /// A property value
    Property(PropertyValue),
    /// Null
    Null,
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
    /// Get as node if this is a node value
    pub fn as_node(&self) -> Option<(NodeId, &Node)> {
        match self {
            Value::Node(id, node) => Some((*id, node)),
            _ => None,
        }
    }

    /// Get as edge if this is an edge value
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
    use crate::graph::{Label, EdgeType};

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
