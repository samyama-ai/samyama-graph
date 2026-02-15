//! Core graph database implementation
//!
//! This module implements the property graph data model with:
//! - Nodes with multiple labels and properties (REQ-GRAPH-001 through REQ-GRAPH-006)
//! - Directed edges with types and properties (REQ-GRAPH-003, REQ-GRAPH-007)
//! - Multiple edges between same nodes (REQ-GRAPH-008)
//! - In-memory storage with hash-based indices (REQ-MEM-001, REQ-MEM-003)

pub mod edge;
pub mod node;
pub mod property;
pub mod store;
pub mod types;
pub mod event;
pub mod storage;

// Re-export main types
pub use edge::Edge;
pub use node::Node;
pub use property::{PropertyMap, PropertyValue};
pub use store::{GraphError, GraphResult, GraphStore, GraphStatistics, PropertyStats};
pub use types::{EdgeId, EdgeType, Label, NodeId};
pub use event::IndexEvent;
pub use storage::{Column, ColumnStore};
