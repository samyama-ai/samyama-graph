//! # Samyama Graph Database
//!
//! A high-performance graph database written in Rust with ~90% OpenCypher query support,
//! RESP (Redis protocol) compatibility, multi-tenancy, HNSW vector search, natural language
//! queries, and graph algorithms. Currently at v0.6.0.
//!
//! ## How a Graph Database Works
//!
//! Unlike relational databases (PostgreSQL, MySQL) that store data in tables with rows and
//! columns, a graph database stores data as **nodes** (entities) and **edges** (relationships).
//! This model is natural for connected data: social networks, knowledge graphs, fraud detection.
//!
//! ```text
//! Relational:                          Graph:
//! ┌──────────┐  ┌──────────────┐       (Alice)──KNOWS──>(Bob)
//! │ persons  │  │ friendships  │          │                │
//! │──────────│  │──────────────│       WORKS_AT        WORKS_AT
//! │ id│ name │  │ from │ to   │          │                │
//! │ 1 │Alice │  │  1   │  2   │       (Acme)           (Globex)
//! │ 2 │ Bob  │  │  2   │  3   │
//! └──────────┘  └──────────────┘
//! ```
//!
//! The key advantage: traversing relationships is O(1) per hop (follow a pointer) instead of
//! O(n log n) per JOIN in SQL. This is called **index-free adjacency**.
//!
//! ## Architecture Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                    Client Layer                         │
//! │   RESP (Redis)         HTTP/REST         SDK (Rust/TS)  │
//! ├─────────────────────────────────────────────────────────┤
//! │                  Query Pipeline                         │
//! │   Cypher Text ──> PEG Parser ──> AST ──> Planner ──>   │
//! │   ──> Physical Operators (Volcano Model) ──> Results   │
//! ├─────────────────────────────────────────────────────────┤
//! │                  Storage Layer                          │
//! │   In-Memory GraphStore    RocksDB Persistence    WAL   │
//! │   Vec<Vec<Node>> arena    Column families        fsync  │
//! │   Adjacency lists         Tenant isolation       CRC32  │
//! ├─────────────────────────────────────────────────────────┤
//! │                  Extensions                             │
//! │   HNSW Vector Search   Graph Algorithms   NLQ (LLM)    │
//! │   Raft Consensus       Multi-Tenancy      Agentic AI   │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Design Decisions (ADRs)
//!
//! - **ADR-007 Volcano Iterator Model**: query operators are lazy, pull-based iterators.
//!   Each operator's `next()` pulls one record at a time from its child — composable and
//!   memory-efficient (no intermediate materialization).
//! - **ADR-012 Late Materialization**: scan operators produce `NodeRef(id)` (8 bytes) instead
//!   of cloning full nodes. Properties are resolved on-demand. This reduces memory bandwidth
//!   during traversals by ~60%.
//! - **ADR-013 Atomic Keyword Rules**: PEG grammar uses atomic rules (`@{}`) for keywords
//!   to prevent implicit whitespace consumption before word-boundary checks.
//! - **ADR-015 Graph-Native Planning**: cost-based query optimizer using triple-level statistics
//!   for cardinality estimation and join ordering.
//!
//! ## Modules
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`graph`] | Property graph data model (nodes, edges, properties, adjacency lists) |
//! | [`query`] | OpenCypher parser, AST, planner, and Volcano execution engine |
//! | [`protocol`] | RESP (Redis) wire protocol for client compatibility |
//! | [`persistence`] | RocksDB storage, WAL, and multi-tenant isolation |
//! | [`raft`] | Raft consensus for high availability (leader election, log replication) |
//! | [`vector`] | HNSW approximate nearest neighbor search for vector embeddings |
//! | [`algo`] | Graph algorithms (PageRank, WCC, SCC, BFS, Dijkstra, etc.) |
//! | [`nlq`] | Natural language to Cypher translation via LLM providers |
//! | [`agent`] | Agentic AI enrichment (GAK: Generation-Augmented Knowledge) |
//! | [`rdf`] | RDF triple store (foundation for SPARQL support) |
//!
//! ## Rust Concepts Used Throughout
//!
//! - **Ownership & Borrowing**: `QueryExecutor<'a>` borrows `&'a GraphStore` — the compiler
//!   guarantees no data races without runtime locks for read-only queries.
//! - **Algebraic Data Types**: `PropertyValue` enum (tagged union) with exhaustive `match` —
//!   the compiler ensures every variant is handled.
//! - **Trait Objects**: `Box<dyn PhysicalOperator>` for runtime polymorphism in the operator tree.
//! - **Zero-Cost Abstractions**: newtype wrappers (`NodeId(u64)`) provide type safety with no
//!   runtime overhead.
//! - **Error Handling**: `thiserror` for library errors, `Result<T, E>` instead of exceptions.
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
pub mod rdf;
pub mod sparql;
pub mod vector;
pub mod algo;
pub mod index;
pub mod sharding;
pub mod http;
pub mod embed;
pub mod nlq;
pub mod agent;

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
    AutoEmbedConfig, NLQConfig, LLMProvider,
};

pub use embed::{
    EmbedPipeline, EmbedError, EmbedResult, TextChunk,
};

pub use nlq::{
    NLQPipeline, NLQError, NLQResult,
};

pub use raft::{
    RaftNode, RaftNodeId, RaftError, RaftResult,
    GraphStateMachine, Request as RaftRequest, Response as RaftResponse,
    ClusterConfig, ClusterManager, NodeId as RaftNodeIdWithAddr,
};

pub use rdf::{
    RdfStore, RdfStoreError, RdfStoreResult,
    NamedNode, BlankNode, Literal, Triple, Quad,
    RdfTerm, RdfSubject, RdfPredicate, RdfObject,
    TriplePattern, QuadPattern,
    GraphToRdfMapper, RdfToGraphMapper, MappingConfig, MappingError,
    NamespaceManager, Namespace,
    RdfFormat, RdfParser, RdfSerializer,
    RdfsReasoner, InferenceRule,
};

pub use sparql::{
    SparqlEngine, SparqlError, SparqlResult,
    SparqlResults, ResultFormat, QuerySolution,
    SparqlParser, SparqlExecutor,
};

pub use vector::{
    VectorIndex, VectorIndexManager, IndexKey,
    DistanceMetric, VectorError, VectorResult,
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
        assert_eq!(ver, "0.6.0");
    }
}
