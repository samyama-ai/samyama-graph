//! Core type definitions for the graph database

use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for a node
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        NodeId(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeId({})", self.0)
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        NodeId(id)
    }
}

/// Unique identifier for an edge
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct EdgeId(pub u64);

impl EdgeId {
    pub fn new(id: u64) -> Self {
        EdgeId(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl fmt::Display for EdgeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EdgeId({})", self.0)
    }
}

impl From<u64> for EdgeId {
    fn from(id: u64) -> Self {
        EdgeId(id)
    }
}

/// Node label (e.g., "Person", "Employee")
/// Implements REQ-GRAPH-002: Nodes with labels
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Label(String);

impl Label {
    pub fn new(label: impl Into<String>) -> Self {
        Label(label.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Label {
    fn from(s: String) -> Self {
        Label(s)
    }
}

impl From<&str> for Label {
    fn from(s: &str) -> Self {
        Label(s.to_string())
    }
}

/// Edge type (relationship type, e.g., "KNOWS", "WORKS_AT")
/// Implements REQ-GRAPH-003: Edges with types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct EdgeType(String);

impl EdgeType {
    pub fn new(edge_type: impl Into<String>) -> Self {
        EdgeType(edge_type.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EdgeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for EdgeType {
    fn from(s: String) -> Self {
        EdgeType(s)
    }
}

impl From<&str> for EdgeType {
    fn from(s: &str) -> Self {
        EdgeType(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id() {
        let id = NodeId::new(42);
        assert_eq!(id.as_u64(), 42);
        assert_eq!(format!("{}", id), "NodeId(42)");

        let id2: NodeId = 100.into();
        assert_eq!(id2.as_u64(), 100);
    }

    #[test]
    fn test_edge_id() {
        let id = EdgeId::new(99);
        assert_eq!(id.as_u64(), 99);
        assert_eq!(format!("{}", id), "EdgeId(99)");
    }

    #[test]
    fn test_label() {
        let label = Label::new("Person");
        assert_eq!(label.as_str(), "Person");
        assert_eq!(format!("{}", label), "Person");

        let label2: Label = "Employee".into();
        assert_eq!(label2.as_str(), "Employee");
    }

    #[test]
    fn test_edge_type() {
        let edge_type = EdgeType::new("KNOWS");
        assert_eq!(edge_type.as_str(), "KNOWS");
        assert_eq!(format!("{}", edge_type), "KNOWS");
    }

    #[test]
    fn test_id_ordering() {
        let id1 = NodeId::new(1);
        let id2 = NodeId::new(2);
        assert!(id1 < id2);
    }
}
