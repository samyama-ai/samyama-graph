//! Data models for the Samyama SDK
//!
//! These types represent the API response structures and are used
//! by both EmbeddedClient and RemoteClient.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A graph node returned from a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkNode {
    /// Node ID
    pub id: String,
    /// Node labels
    pub labels: Vec<String>,
    /// Node properties
    pub properties: HashMap<String, serde_json::Value>,
}

/// A graph edge returned from a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SdkEdge {
    /// Edge ID
    pub id: String,
    /// Source node ID
    pub source: String,
    /// Target node ID
    pub target: String,
    /// Relationship type
    #[serde(rename = "type")]
    pub edge_type: String,
    /// Edge properties
    pub properties: HashMap<String, serde_json::Value>,
}

/// Result of executing a Cypher query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Graph nodes referenced in the result
    pub nodes: Vec<SdkNode>,
    /// Graph edges referenced in the result
    pub edges: Vec<SdkEdge>,
    /// Column names
    pub columns: Vec<String>,
    /// Tabular result rows
    pub records: Vec<Vec<serde_json::Value>>,
}

impl QueryResult {
    /// Number of result records
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the result is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Server status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerStatus {
    /// Health status (e.g., "healthy")
    pub status: String,
    /// Server version
    pub version: String,
    /// Storage statistics
    pub storage: StorageStats,
}

/// Storage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Number of nodes
    pub nodes: u64,
    /// Number of edges
    pub edges: u64,
}
