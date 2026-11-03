//! Samyama SDK — Client library for the Samyama Graph Database
//!
//! Provides two client implementations:
//!
//! - **`EmbeddedClient`** — In-process, no network. Uses `GraphStore` and `QueryEngine`
//!   directly. Ideal for tests, examples, and embedded applications.
//!
//! - **`RemoteClient`** — Connects to a running Samyama server via HTTP.
//!   For production client applications.
//!
//! Both implement the `SamyamaClient` trait for a unified API.
//!
//! Extension traits (EmbeddedClient only):
//! - **`AlgorithmClient`** — PageRank, WCC, SCC, BFS, Dijkstra, and more
//! - **`VectorClient`** — Vector index creation, insertion, and k-NN search
//!
//! # Quick Start
//!
//! ```rust
//! use samyama_sdk::{EmbeddedClient, SamyamaClient};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = EmbeddedClient::new();
//!
//!     // Create data
//!     client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
//!         .await.unwrap();
//!
//!     // Query data
//!     let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
//!         .await.unwrap();
//!     println!("Found {} records", result.len());
//! }
//! ```

pub mod algo;
pub mod client;
pub mod embedded;
pub mod error;
pub mod models;
pub mod remote;
pub mod vector_ext;

// ============================================================
// Core SDK types
// ============================================================

pub use client::SamyamaClient;
pub use embedded::EmbeddedClient;
pub use error::{SamyamaError, SamyamaResult};
pub use models::{QueryResult, SdkEdge, SdkNode, ServerStatus, StorageStats};
pub use remote::{ConnectionConfig, RemoteClient, RowStream};

// ============================================================
// Extension traits (EmbeddedClient only)
// ============================================================

pub use algo::AlgorithmClient;
pub use vector_ext::VectorClient;

// ============================================================
// Graph types (re-exported from samyama core)
// ============================================================

pub use samyama::graph::{
    Edge, EdgeId, EdgeType, GraphError, GraphResult, GraphStore, Label, Node, NodeId, PropertyMap,
    PropertyValue,
};
pub use samyama::query::{CacheStats, QueryEngine, RecordBatch};

// ============================================================
// Algorithm types (re-exported from samyama-graph-algorithms)
// ============================================================

pub use samyama::algo::{
    bfs, build_view, count_triangles, dijkstra, edmonds_karp, page_rank, pca, prim_mst,
    strongly_connected_components, weakly_connected_components, FlowResult, MSTResult,
    PageRankConfig, PathResult, PcaConfig, PcaResult, PcaSolver, SccResult, WccResult,
};
pub use samyama_graph_algorithms::GraphView;

// ============================================================
// Vector types (re-exported from samyama core)
// ============================================================

pub use samyama::vector::{
    DistanceMetric, IndexKey, VectorError, VectorIndex, VectorIndexManager, VectorResult,
};

// ============================================================
// NLQ types (re-exported from samyama core)
// ============================================================

pub use samyama::{NLQError, NLQPipeline, NLQResult};

// ============================================================
// Agent types (re-exported from samyama core)
// ============================================================

pub use samyama::agent::AgentRuntime;

// ============================================================
// Persistence & Multi-Tenancy types (re-exported from samyama core)
// ============================================================

pub use samyama::persistence::tenant::{AgentConfig, ToolConfig};
pub use samyama::{
    AutoEmbedConfig, LLMProvider, NLQConfig, PersistenceError, PersistenceManager,
    PersistenceResult, PersistentStorage, ResourceQuotas, ResourceUsage, StorageError,
    StorageResult, Tenant, TenantError, TenantManager, TenantResult, Wal, WalEntry, WalError,
    WalResult,
};

// ============================================================
// Optimization types (re-exported from samyama-optimization)
// ============================================================

pub use samyama_optimization::algorithms::{
    ABCSolver, BMRSolver, BatSolver, CuckooSolver, DESolver, FireflySolver, GASolver, GWOSolver,
    JayaSolver, NSGA2Solver, PSOSolver, QOJayaSolver, RaoSolver, SASolver, TLBOSolver,
};
pub use samyama_optimization::{
    Individual, MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult,
    OptimizationResult, Problem, SimpleProblem, SolverConfig,
};

// ============================================================
// ndarray (re-exported for optimization problem definitions)
// ============================================================

pub use ndarray::Array1;

// ============================================================
// Version
// ============================================================

pub use samyama::VERSION;

#[cfg(test)]
#[path = "lib_cov_tests.rs"]
mod cov_tests;
