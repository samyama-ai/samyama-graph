//! SamyamaClient trait — the unified interface for embedded and remote modes

use async_trait::async_trait;
use crate::error::SamyamaResult;
use crate::models::{QueryResult, ServerStatus};

/// Unified client interface for the Samyama graph database.
///
/// Implemented by:
/// - `EmbeddedClient` — in-process, no network (for examples, tests, embedded use)
/// - `RemoteClient` — connects to a running Samyama server via HTTP
#[async_trait]
pub trait SamyamaClient: Send + Sync {
    /// Execute a read-write Cypher query
    async fn query(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult>;

    /// Execute a read-only Cypher query
    async fn query_readonly(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult>;

    /// Delete a graph
    async fn delete_graph(&self, graph: &str) -> SamyamaResult<()>;

    /// List all graphs
    async fn list_graphs(&self) -> SamyamaResult<Vec<String>>;

    /// Get server status
    async fn status(&self) -> SamyamaResult<ServerStatus>;

    /// Ping the server
    async fn ping(&self) -> SamyamaResult<String>;
}
