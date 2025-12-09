//! Samyama Graph Database
//!
//! A high-performance, distributed graph database with OpenCypher query support,
//! Redis protocol compatibility, and multi-tenancy.
//!
//! # Architecture
//!
//! This implementation follows the Architecture Decision Records (ADRs):
//! - ADR-001: Rust for memory safety and performance
//! - ADR-002: RocksDB for persistence (future)
//! - ADR-003: RESP protocol for Redis compatibility (future)
//! - ADR-005: Cap'n Proto for serialization (future)
//! - ADR-006: Tokio for async runtime (future)
//!
//! # Requirements Implemented
//!
//! ## Phase 1 - Core Features (Current)
//!
//! - ✅ REQ-GRAPH-001: Property graph data model
//! - ✅ REQ-GRAPH-002: Nodes with labels
//! - ✅ REQ-GRAPH-003: Edges with types
//! - ✅ REQ-GRAPH-004: Properties on nodes and edges
//! - ✅ REQ-GRAPH-005: Multiple property data types
//! - ✅ REQ-GRAPH-006: Multiple labels per node
//! - ✅ REQ-GRAPH-007: Directed edges
//! - ✅ REQ-GRAPH-008: Multiple edges between nodes
//! - ✅ REQ-MEM-001: In-memory storage
//! - ✅ REQ-MEM-003: Memory-optimized data structures
//!
//! ## Phase 2 - Query Engine & RESP Protocol (Current)
//!
//! - ✅ REQ-CYPHER-001: OpenCypher query language
//! - ✅ REQ-CYPHER-002: Pattern matching
//! - ✅ REQ-CYPHER-007: WHERE clauses
//! - ✅ REQ-CYPHER-008: ORDER BY and LIMIT
//! - ✅ REQ-CYPHER-009: Query optimization
//! - ✅ REQ-REDIS-001: RESP protocol implementation
//! - ✅ REQ-REDIS-002: Redis client connections
//! - ✅ REQ-REDIS-004: Redis-compatible graph commands
//! - ✅ REQ-REDIS-006: Redis client library compatibility
//!
//! ## Phase 3 - Persistence & Multi-Tenancy (Complete)
//!
//! - ✅ REQ-PERSIST-001: RocksDB persistence
//! - ✅ REQ-PERSIST-002: Write-Ahead Logging
//! - ✅ REQ-TENANT-001 through REQ-TENANT-008: Multi-tenancy with resource quotas
//!
//! ## Phase 4 - High Availability (In Progress)
//!
//! - 🚧 REQ-HA-001: Raft consensus protocol
//! - 🚧 REQ-HA-002: Leader election and automatic failover
//! - 🚧 REQ-HA-003: Log replication across cluster nodes
//! - 🚧 REQ-HA-004: Cluster membership management
//!
//! ## Example Usage
//!
//! ```rust
//! use samyama::graph::{GraphStore, Label, PropertyValue};
//! use std::collections::HashMap;
//!
//! // Create a new graph store
//! let mut store = GraphStore::new();
//!
//! // Create nodes
//! let alice = store.create_node("Person");
//! let bob = store.create_node("Person");
//!
//! // Set properties
//! if let Some(node) = store.get_node_mut(alice) {
//!     node.set_property("name", "Alice");
//!     node.set_property("age", 30i64);
//! }
//!
//! // Create edge
//! let knows_edge = store.create_edge(alice, bob, "KNOWS").unwrap();
//!
//! // Query by label
//! let persons = store.get_nodes_by_label(&Label::new("Person"));
//! assert_eq!(persons.len(), 2);
//! ```

#![allow(missing_docs)]
#![warn(clippy::all)]

pub mod graph;
pub mod query;
pub mod protocol;
pub mod persistence;
pub mod raft;

// Re-export main types for convenience
pub use graph::{
    Edge, EdgeId, EdgeType, GraphError, GraphResult, GraphStore, Label, Node, NodeId,
    PropertyMap, PropertyValue,
};

pub use query::{
    QueryEngine, parse_query, Query, RecordBatch,
};

pub use protocol::{
    RespServer, ServerConfig, RespValue,
};

pub use persistence::{
    PersistenceManager, PersistenceError, PersistenceResult,
    PersistentStorage, StorageError, StorageResult,
    Tenant, TenantManager, ResourceQuotas, ResourceUsage, TenantError, TenantResult,
    Wal, WalEntry, WalError, WalResult,
};

pub use raft::{
    RaftNode, RaftNodeId, RaftError, RaftResult,
    GraphStateMachine, Request as RaftRequest, Response as RaftResponse,
    ClusterConfig, ClusterManager, NodeId as RaftNodeIdWithAddr,
};

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Get version string
pub fn version() -> &'static str {
    VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let ver = version();
        assert!(!ver.is_empty());
        assert_eq!(ver, "0.1.0");
    }
}
