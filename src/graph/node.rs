//! Node implementation for property graph
//!
//! Implements:
//! - REQ-GRAPH-002: Nodes with labels
//! - REQ-GRAPH-004: Properties on nodes
//! - REQ-GRAPH-006: Multiple labels per node

use super::property::{PropertyMap, PropertyValue};
use super::types::{Label, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A node in the property graph
///
/// Nodes can have:
/// - A unique ID
/// - Multiple labels (REQ-GRAPH-006)
/// - Properties (key-value pairs)
/// - Creation and update timestamps
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    /// Unique identifier for this node
    pub id: NodeId,

    /// Version for MVCC
    pub version: u64,

    /// Set of labels for this node (supports multiple labels)
    pub labels: HashSet<Label>,

    /// Properties associated with this node
    pub properties: PropertyMap,

    /// Creation timestamp (Unix milliseconds)
    pub created_at: i64,

    /// Last update timestamp (Unix milliseconds)
    pub updated_at: i64,
}

impl Node {
    /// Create a new node with a single label
    pub fn new(id: NodeId, label: impl Into<Label>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let mut labels = HashSet::new();
        labels.insert(label.into());

        Node {
            id,
            version: 1,
            labels,
            properties: PropertyMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new node with multiple labels (REQ-GRAPH-006)
    pub fn new_with_labels(id: NodeId, labels: Vec<Label>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let label_set: HashSet<Label> = labels.into_iter().collect();

        Node {
            id,
            version: 1,
            labels: label_set,
            properties: PropertyMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a new node with labels and properties
    pub fn new_with_properties(
        id: NodeId,
        labels: Vec<Label>,
        properties: PropertyMap,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        let label_set: HashSet<Label> = labels.into_iter().collect();

        Node {
            id,
            version: 1,
            labels: label_set,
            properties,
            created_at: now,
            updated_at: now,
        }
    }

    /// Add a label to this node
    pub fn add_label(&mut self, label: impl Into<Label>) {
        self.labels.insert(label.into());
        self.update_timestamp();
    }

    /// Remove a label from this node
    pub fn remove_label(&mut self, label: &Label) -> bool {
        let removed = self.labels.remove(label);
        if removed {
            self.update_timestamp();
        }
        removed
    }

    /// Check if node has a specific label
    pub fn has_label(&self, label: &Label) -> bool {
        self.labels.contains(label)
    }

    /// Get all labels
    pub fn get_labels(&self) -> Vec<&Label> {
        self.labels.iter().collect()
    }

    /// Set a property value
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<PropertyValue>) -> Option<PropertyValue> {
        let old = self.properties.insert(key.into(), value.into());
        self.update_timestamp();
        old
    }

    /// Get a property value
    pub fn get_property(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }

    /// Remove a property
    pub fn remove_property(&mut self, key: &str) -> Option<PropertyValue> {
        let removed = self.properties.remove(key);
        if removed.is_some() {
            self.update_timestamp();
        }
        removed
    }

    /// Check if property exists
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Update the modification timestamp
    fn update_timestamp(&mut self) {
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Get number of properties
    pub fn property_count(&self) -> usize {
        self.properties.len()
    }

    /// Get number of labels
    pub fn label_count(&self) -> usize {
        self.labels.len()
    }
}

// Add chrono dependency
mod chrono {
    pub struct Utc;
    impl Utc {
        pub fn now() -> DateTime {
            DateTime
        }
    }
    pub struct DateTime;
    impl DateTime {
        pub fn timestamp_millis(&self) -> i64 {
            // Use system time for now
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Node {}

impl std::hash::Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_node_single_label() {
        // REQ-GRAPH-002: Nodes with labels
        let node = Node::new(NodeId::new(1), "Person");
        assert_eq!(node.id, NodeId::new(1));
        assert_eq!(node.labels.len(), 1);
        assert!(node.has_label(&Label::new("Person")));
    }

    #[test]
    fn test_create_node_multiple_labels() {
        // REQ-GRAPH-006: Multiple labels per node
        let labels = vec![Label::new("Person"), Label::new("Employee")];
        let node = Node::new_with_labels(NodeId::new(2), labels);

        assert_eq!(node.label_count(), 2);
        assert!(node.has_label(&Label::new("Person")));
        assert!(node.has_label(&Label::new("Employee")));
    }

    #[test]
    fn test_add_remove_labels() {
        let mut node = Node::new(NodeId::new(3), "Person");

        // Add label
        node.add_label("Employee");
        assert_eq!(node.label_count(), 2);
        assert!(node.has_label(&Label::new("Employee")));

        // Remove label
        let removed = node.remove_label(&Label::new("Person"));
        assert!(removed);
        assert_eq!(node.label_count(), 1);
        assert!(!node.has_label(&Label::new("Person")));
    }

    #[test]
    fn test_node_properties() {
        // REQ-GRAPH-004: Properties on nodes
        let mut node = Node::new(NodeId::new(4), "Person");

        // Set properties
        node.set_property("name", "Alice");
        node.set_property("age", 30i64);
        node.set_property("active", true);

        // Get properties
        assert_eq!(node.get_property("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(node.get_property("age").unwrap().as_integer(), Some(30));
        assert_eq!(node.get_property("active").unwrap().as_boolean(), Some(true));
        assert_eq!(node.property_count(), 3);

        // Remove property
        let removed = node.remove_property("age");
        assert!(removed.is_some());
        assert_eq!(node.property_count(), 2);
        assert!(!node.has_property("age"));
    }

    #[test]
    fn test_node_with_properties() {
        // REQ-GRAPH-005: Multiple data types
        let mut props = PropertyMap::new();
        props.insert("name".to_string(), "Bob".into());
        props.insert("age".to_string(), 25i64.into());
        props.insert("score".to_string(), 95.5.into());

        let node = Node::new_with_properties(
            NodeId::new(5),
            vec![Label::new("Student")],
            props,
        );

        assert_eq!(node.property_count(), 3);
        assert_eq!(node.get_property("name").unwrap().as_string(), Some("Bob"));
        assert_eq!(node.get_property("age").unwrap().as_integer(), Some(25));
        assert_eq!(node.get_property("score").unwrap().as_float(), Some(95.5));
    }

    #[test]
    fn test_node_timestamps() {
        let node = Node::new(NodeId::new(6), "Test");
        assert!(node.created_at > 0);
        assert_eq!(node.created_at, node.updated_at);

        std::thread::sleep(std::time::Duration::from_millis(10));
        let mut node2 = node.clone();
        node2.set_property("key", "value");

        assert!(node2.updated_at > node.updated_at);
    }

    #[test]
    fn test_node_equality() {
        let node1 = Node::new(NodeId::new(7), "Person");
        let node2 = Node::new(NodeId::new(7), "Person");
        let node3 = Node::new(NodeId::new(8), "Person");

        assert_eq!(node1, node2); // Same ID
        assert_ne!(node1, node3); // Different ID
    }
}
