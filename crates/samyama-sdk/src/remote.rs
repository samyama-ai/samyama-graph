//! RemoteClient — network client for a running Samyama server
//!
//! Connects via HTTP to the Samyama HTTP API.

use async_trait::async_trait;
use reqwest::Client;

use crate::client::SamyamaClient;
use crate::error::{SamyamaError, SamyamaResult};
use crate::models::{QueryResult, ServerStatus};

/// Network client that connects to a running Samyama server.
///
/// Uses HTTP transport for `/api/query` and `/api/status` endpoints.
pub struct RemoteClient {
    http_base_url: String,
    http_client: Client,
}

impl RemoteClient {
    /// Create a new RemoteClient connecting to the given HTTP base URL.
    ///
    /// # Example
    /// ```no_run
    /// # use samyama_sdk::RemoteClient;
    /// let client = RemoteClient::new("http://localhost:8080");
    /// ```
    pub fn new(http_base_url: &str) -> Self {
        Self {
            http_base_url: http_base_url.trim_end_matches('/').to_string(),
            http_client: Client::new(),
        }
    }

    /// Execute a POST request to /api/query
    async fn post_query(&self, cypher: &str) -> SamyamaResult<QueryResult> {
        let url = format!("{}/api/query", self.http_base_url);
        let body = serde_json::json!({ "query": cypher });

        let response = self.http_client.post(&url)
            .json(&body)
            .send()
            .await?;

        if response.status().is_success() {
            let result: QueryResult = response.json().await?;
            Ok(result)
        } else {
            let error_body: serde_json::Value = response.json().await
                .unwrap_or_else(|_| serde_json::json!({"error": "Unknown error"}));
            let msg = error_body.get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error")
                .to_string();
            Err(SamyamaError::QueryError(msg))
        }
    }
}

#[async_trait]
impl SamyamaClient for RemoteClient {
    async fn query(&self, _graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        self.post_query(cypher).await
    }

    async fn query_readonly(&self, _graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        self.post_query(cypher).await
    }

    async fn delete_graph(&self, _graph: &str) -> SamyamaResult<()> {
        // The HTTP API doesn't expose GRAPH.DELETE directly.
        // We can execute a Cypher that deletes all nodes/edges.
        self.post_query("MATCH (n) DELETE n").await?;
        Ok(())
    }

    async fn list_graphs(&self) -> SamyamaResult<Vec<String>> {
        // Single-graph mode in OSS
        Ok(vec!["default".to_string()])
    }

    async fn status(&self) -> SamyamaResult<ServerStatus> {
        let url = format!("{}/api/status", self.http_base_url);
        let response = self.http_client.get(&url)
            .send()
            .await?;

        if response.status().is_success() {
            let status: ServerStatus = response.json().await?;
            Ok(status)
        } else {
            Err(SamyamaError::ConnectionError(
                format!("Status endpoint returned {}", response.status())
            ))
        }
    }

    async fn ping(&self) -> SamyamaResult<String> {
        let status = self.status().await?;
        if status.status == "healthy" {
            Ok("PONG".to_string())
        } else {
            Err(SamyamaError::ConnectionError(
                format!("Server unhealthy: {}", status.status)
            ))
        }
    }
}
