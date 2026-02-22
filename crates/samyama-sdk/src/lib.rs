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

pub mod client;
pub mod embedded;
pub mod error;
pub mod models;
pub mod remote;
pub mod algo;
pub mod vector_ext;

// ============================================================
// Core SDK types
// ============================================================

pub use client::SamyamaClient;
pub use embedded::EmbeddedClient;
pub use remote::RemoteClient;
pub use error::{SamyamaError, SamyamaResult};
pub use models::{QueryResult, SdkNode, SdkEdge, ServerStatus, StorageStats};

// ============================================================
// Extension traits (EmbeddedClient only)
// ============================================================

pub use algo::AlgorithmClient;
pub use vector_ext::VectorClient;

// ============================================================
// Graph types (re-exported from samyama core)
// ============================================================

pub use samyama::graph::{
    GraphStore, Node, Edge, NodeId, EdgeId, EdgeType, Label,
    PropertyValue, PropertyMap,
    GraphError, GraphResult,
};
pub use samyama::query::{QueryEngine, RecordBatch};

// ============================================================
// Algorithm types (re-exported from samyama-graph-algorithms)
// ============================================================

pub use samyama::algo::{
    build_view, page_rank, weakly_connected_components, strongly_connected_components,
    bfs, dijkstra, edmonds_karp, prim_mst, count_triangles,
    PageRankConfig, PathResult, WccResult, SccResult, FlowResult, MSTResult,
};
pub use samyama_graph_algorithms::GraphView;

// ============================================================
// Vector types (re-exported from samyama core)
// ============================================================

pub use samyama::vector::{
    DistanceMetric, VectorIndex, VectorIndexManager, IndexKey,
    VectorError, VectorResult,
};

// ============================================================
// NLQ types (re-exported from samyama core)
// ============================================================

pub use samyama::{NLQPipeline, NLQError, NLQResult};

// ============================================================
// Agent types (re-exported from samyama core)
// ============================================================

pub use samyama::agent::AgentRuntime;

// ============================================================
// Persistence & Multi-Tenancy types (re-exported from samyama core)
// ============================================================

pub use samyama::{
    PersistenceManager, PersistenceError, PersistenceResult,
    PersistentStorage, StorageError, StorageResult,
    Tenant, TenantManager, ResourceQuotas, ResourceUsage,
    TenantError, TenantResult,
    Wal, WalEntry, WalError, WalResult,
    NLQConfig, LLMProvider, AutoEmbedConfig,
};
pub use samyama::persistence::tenant::{AgentConfig, ToolConfig};

// ============================================================
// Optimization types (re-exported from samyama-optimization)
// ============================================================

pub use samyama_optimization::{
    SolverConfig, OptimizationResult, Individual, SimpleProblem,
    Problem, MultiObjectiveProblem, MultiObjectiveIndividual, MultiObjectiveResult,
};
pub use samyama_optimization::algorithms::{
    JayaSolver, CuckooSolver, NSGA2Solver,
    RaoSolver, TLBOSolver, PSOSolver, DESolver, GASolver,
    FireflySolver, GWOSolver, SASolver, BatSolver, ABCSolver,
};

// ============================================================
// ndarray (re-exported for optimization problem definitions)
// ============================================================

pub use ndarray::Array1;

// ============================================================
// Version
// ============================================================

pub use samyama::VERSION;
