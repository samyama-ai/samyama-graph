//! Edge implementation for property graph
//!
//! Implements:
//! - REQ-GRAPH-003: Edges with types
//! - REQ-GRAPH-004: Properties on edges
//! - REQ-GRAPH-007: Directed edges
//! - REQ-GRAPH-008: Multiple edges between same nodes

use super::property::{PropertyMap, PropertyValue};
use super::types::{EdgeId, EdgeType, NodeId};
use serde::{Deserialize, Serialize};

/// A directed edge in the property graph
///
/// Edges have:
/// - A unique ID
/// - A source node (REQ-GRAPH-007: directed)
/// - A target node
/// - An edge type (relationship type)
/// - Properties (key-value pairs)
/// - Creation timestamp
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Edge {
    /// Unique identifier for this edge
    pub id: EdgeId,

    /// Source node (edge goes FROM this node)
    pub source: NodeId,

    /// Target node (edge goes TO this node)
    pub target: NodeId,

    /// Type of relationship (e.g., "KNOWS", "WORKS_AT")
    pub edge_type: EdgeType,

    /// Properties associated with this edge
    pub properties: PropertyMap,

    /// Creation timestamp (Unix milliseconds)
    pub created_at: i64,
}

impl Edge {
    /// Create a new directed edge
    pub fn new(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        edge_type: impl Into<EdgeType>,
    ) -> Self {
        let now = Self::current_timestamp();

        Edge {
            id,
            source,
            target,
            edge_type: edge_type.into(),
            properties: PropertyMap::new(),
            created_at: now,
        }
    }

    /// Create a new edge with properties
    pub fn new_with_properties(
        id: EdgeId,
        source: NodeId,
        target: NodeId,
        edge_type: impl Into<EdgeType>,
        properties: PropertyMap,
    ) -> Self {
        let now = Self::current_timestamp();

        Edge {
            id,
            source,
            target,
            edge_type: edge_type.into(),
            properties,
            created_at: now,
        }
    }

    /// Set a property value
    pub fn set_property(&mut self, key: impl Into<String>, value: impl Into<PropertyValue>) {
        self.properties.insert(key.into(), value.into());
    }

    /// Get a property value
    pub fn get_property(&self, key: &str) -> Option<&PropertyValue> {
        self.properties.get(key)
    }

    /// Remove a property
    pub fn remove_property(&mut self, key: &str) -> Option<PropertyValue> {
        self.properties.remove(key)
    }

    /// Check if property exists
    pub fn has_property(&self, key: &str) -> bool {
        self.properties.contains_key(key)
    }

    /// Get number of properties
    pub fn property_count(&self) -> usize {
        self.properties.len()
    }

    /// Check if this edge connects two specific nodes (in either direction)
    pub fn connects(&self, node1: NodeId, node2: NodeId) -> bool {
        (self.source == node1 && self.target == node2)
            || (self.source == node2 && self.target == node1)
    }

    /// Check if this edge goes FROM a specific node
    pub fn starts_from(&self, node: NodeId) -> bool {
        self.source == node
    }

    /// Check if this edge goes TO a specific node
    pub fn ends_at(&self, node: NodeId) -> bool {
        self.target == node
    }

    /// Get current timestamp
    fn current_timestamp() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

impl PartialEq for Edge {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Edge {}

impl std::hash::Hash for Edge {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_edge() {
        // REQ-GRAPH-003: Edges with types
        // REQ-GRAPH-007: Directed edges
        let edge = Edge::new(
            EdgeId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            "KNOWS",
        );

        assert_eq!(edge.id, EdgeId::new(1));
        assert_eq!(edge.source, NodeId::new(1));
        assert_eq!(edge.target, NodeId::new(2));
        assert_eq!(edge.edge_type.as_str(), "KNOWS");
    }

    #[test]
    fn test_edge_direction() {
        // REQ-GRAPH-007: Directed edges
        let edge = Edge::new(
            EdgeId::new(2),
            NodeId::new(10),
            NodeId::new(20),
            "FOLLOWS",
        );

        assert!(edge.starts_from(NodeId::new(10)));
        assert!(edge.ends_at(NodeId::new(20)));
        assert!(!edge.starts_from(NodeId::new(20)));
        assert!(!edge.ends_at(NodeId::new(10)));
    }

    #[test]
    fn test_edge_properties() {
        // REQ-GRAPH-004: Properties on edges
        let mut edge = Edge::new(
            EdgeId::new(3),
            NodeId::new(1),
            NodeId::new(2),
            "KNOWS",
        );

        // Set properties
        edge.set_property("since", 2020i64);
        edge.set_property("strength", 0.95);
        edge.set_property("verified", true);

        // Get properties
        assert_eq!(edge.get_property("since").unwrap().as_integer(), Some(2020));
        assert_eq!(edge.get_property("strength").unwrap().as_float(), Some(0.95));
        assert_eq!(edge.get_property("verified").unwrap().as_boolean(), Some(true));
        assert_eq!(edge.property_count(), 3);
    }

    #[test]
    fn test_edge_with_properties() {
        // REQ-GRAPH-005: Multiple data types in properties
        let mut props = PropertyMap::new();
        props.insert("weight".to_string(), 10i64.into());
        props.insert("label".to_string(), "important".into());

        let edge = Edge::new_with_properties(
            EdgeId::new(4),
            NodeId::new(5),
            NodeId::new(6),
            "RELATED_TO",
            props,
        );

        assert_eq!(edge.property_count(), 2);
        assert_eq!(edge.get_property("weight").unwrap().as_integer(), Some(10));
        assert_eq!(
            edge.get_property("label").unwrap().as_string(),
            Some("important")
        );
    }

    #[test]
    fn test_multiple_edges_between_nodes() {
        // REQ-GRAPH-008: Multiple edges between same pair of nodes
        let node1 = NodeId::new(100);
        let node2 = NodeId::new(200);

        let edge1 = Edge::new(EdgeId::new(1), node1, node2, "KNOWS");
        let edge2 = Edge::new(EdgeId::new(2), node1, node2, "WORKS_WITH");
        let edge3 = Edge::new(EdgeId::new(3), node1, node2, "KNOWS");

        // All three edges connect same nodes but are distinct
        assert_ne!(edge1, edge2);
        assert_ne!(edge1, edge3);
        assert_ne!(edge2, edge3);

        // All connect the same nodes
        assert!(edge1.connects(node1, node2));
        assert!(edge2.connects(node1, node2));
        assert!(edge3.connects(node1, node2));

        // Different types
        assert_eq!(edge1.edge_type, EdgeType::new("KNOWS"));
        assert_eq!(edge2.edge_type, EdgeType::new("WORKS_WITH"));
    }

    #[test]
    fn test_edge_connects() {
        let edge = Edge::new(
            EdgeId::new(5),
            NodeId::new(10),
            NodeId::new(20),
            "LINKS",
        );

        assert!(edge.connects(NodeId::new(10), NodeId::new(20)));
        assert!(edge.connects(NodeId::new(20), NodeId::new(10))); // Order doesn't matter for connects()
        assert!(!edge.connects(NodeId::new(10), NodeId::new(30)));
    }

    #[test]
    fn test_remove_property() {
        let mut edge = Edge::new(
            EdgeId::new(6),
            NodeId::new(1),
            NodeId::new(2),
            "TEST",
        );

        edge.set_property("temp", "value");
        assert!(edge.has_property("temp"));

        let removed = edge.remove_property("temp");
        assert!(removed.is_some());
        assert!(!edge.has_property("temp"));
        assert_eq!(edge.property_count(), 0);
    }
}
