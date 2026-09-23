//! RemoteClient — network client for a running Samyama server
//!
//! Connects via HTTP to the Samyama HTTP API.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;

use crate::client::SamyamaClient;
use crate::error::{SamyamaError, SamyamaResult};
use crate::models::{QueryResult, ServerStatus};

/// How a `RemoteClient` treats the network (API-06).
///
/// Every value here has a default, and the defaults are the point: a client
/// built with `RemoteClient::new` used `Client::new()`, which sets **no request
/// timeout at all**. A server that accepts a connection and then stops talking
/// hangs the caller until the process is killed, and an agent loop or a web
/// request has no way to recover from that (samyama-graph#1326).
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Deadline for a whole request, including the body. `None` waits forever,
    /// which is what this type exists to stop being the default.
    pub timeout: Option<Duration>,
    /// Deadline for establishing the TCP connection. Separate from `timeout`
    /// because an unreachable host and a slow query are different failures and
    /// a caller usually wants a much shorter bound on the first.
    pub connect_timeout: Option<Duration>,
    /// How long an idle pooled connection is kept.
    pub pool_idle_timeout: Option<Duration>,
    /// Idle connections kept per host.
    pub pool_max_idle_per_host: usize,
    /// How many times a *retryable* failure is retried. Zero disables retry.
    pub max_retries: u32,
    /// Delay before the first retry; doubled each attempt.
    ///
    /// Constant-delay retry is deliberately not what this does: it is what
    /// turns one slow server into a thundering herd of clients all waking at
    /// the same moment.
    pub retry_base_delay: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            timeout: Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(5)),
            pool_idle_timeout: Some(Duration::from_secs(90)),
            pool_max_idle_per_host: 8,
            max_retries: 2,
            retry_base_delay: Duration::from_millis(100),
        }
    }
}

/// Network client that connects to a running Samyama server.
///
/// Uses HTTP transport for `/api/query` and `/api/status` endpoints.
pub struct RemoteClient {
    http_base_url: String,
    http_client: Client,
    config: ConnectionConfig,
}

impl RemoteClient {
    /// Create a new RemoteClient connecting to the given HTTP base URL.
    ///
    /// Uses [`ConnectionConfig::default`], which sets a 30 s request timeout.
    ///
    /// # Example
    /// ```no_run
    /// # use samyama_sdk::RemoteClient;
    /// let client = RemoteClient::new("http://localhost:8080");
    /// ```
    pub fn new(http_base_url: &str) -> Self {
        Self::with_config(http_base_url, ConnectionConfig::default())
    }

    /// Create a client with explicit connection settings.
    ///
    /// # Example
    /// ```no_run
    /// # use samyama_sdk::{RemoteClient, ConnectionConfig};
    /// # use std::time::Duration;
    /// let client = RemoteClient::with_config(
    ///     "http://localhost:8080",
    ///     ConnectionConfig { timeout: Some(Duration::from_secs(5)), ..Default::default() },
    /// );
    /// ```
    pub fn with_config(http_base_url: &str, config: ConnectionConfig) -> Self {
        let mut builder = Client::builder()
            .pool_max_idle_per_host(config.pool_max_idle_per_host);
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        if let Some(t) = config.connect_timeout {
            builder = builder.connect_timeout(t);
        }
        if let Some(t) = config.pool_idle_timeout {
            builder = builder.pool_idle_timeout(t);
        }
        // `build` fails only on a bad TLS backend, which is a programming error
        // here rather than a runtime condition; falling back to a default
        // client would silently drop the timeout this type exists to set.
        let http_client = builder.build().unwrap_or_else(|e| {
            panic!("could not build HTTP client: {e}")
        });
        Self {
            http_base_url: http_base_url.trim_end_matches('/').to_string(),
            http_client,
            config,
        }
    }

    /// The settings this client was built with.
    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }

    /// Is this failure worth retrying?
    ///
    /// Only connection and timeout failures. A 4xx is the server answering, and
    /// retrying it repeats a request the server has already refused; a 5xx may
    /// have had an effect, and a blind retry of a non-idempotent write is how a
    /// retry turns one duplicate into several.
    fn is_retryable(err: &reqwest::Error) -> bool {
        err.is_timeout() || err.is_connect()
    }

    /// Send with retry and exponential backoff.
    async fn send_with_retry(
        &self,
        make: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut delay = self.config.retry_base_delay;
        let mut attempt = 0;
        loop {
            match make().send().await {
                Ok(r) => return Ok(r),
                Err(e) if attempt < self.config.max_retries && Self::is_retryable(&e) => {
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                    attempt += 1;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Run a query, giving up if `cancel` resolves first.
    ///
    /// A timeout bounds how long a request may take; cancellation is the
    /// caller deciding it no longer wants the answer — a user closing a tab, an
    /// agent abandoning a branch, a shutdown signal. They are different
    /// questions and a deadline cannot express the second, which is why this
    /// exists alongside [`ConnectionConfig::timeout`].
    ///
    /// Dropping the returned future also cancels the request; this is the
    /// version that says so in the type, and that composes with a token or a
    /// channel the caller already has.
    ///
    /// # Example
    /// ```no_run
    /// # use samyama_sdk::RemoteClient;
    /// # async fn f(client: RemoteClient, mut stop: tokio::sync::oneshot::Receiver<()>) {
    /// let result = client.query_cancellable("default", "MATCH (n) RETURN n", async {
    ///     let _ = (&mut stop).await;
    /// }).await;
    /// # }
    /// ```
    pub async fn query_cancellable(
        &self,
        graph: &str,
        cypher: &str,
        cancel: impl std::future::Future<Output = ()>,
    ) -> SamyamaResult<QueryResult> {
        tokio::select! {
            // Biased towards the request so that an already-resolved
            // cancellation does not beat a response that is also ready, which
            // would report a completed query as cancelled.
            biased;
            r = self.post_query(graph, cypher) => r,
            _ = cancel => Err(SamyamaError::ConnectionError(
                "request cancelled by the caller".to_string()
            )),
        }
    }

    /// Execute a POST request to /api/query
    async fn post_query(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        let url = format!("{}/api/query", self.http_base_url);
        let body = serde_json::json!({ "query": cypher, "graph": graph });

        let response = self
            .send_with_retry(|| self.http_client.post(&url).json(&body))
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
    async fn query(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        self.post_query(graph, cypher).await
    }

    async fn query_readonly(&self, graph: &str, cypher: &str) -> SamyamaResult<QueryResult> {
        self.post_query(graph, cypher).await
    }

    async fn delete_graph(&self, graph: &str) -> SamyamaResult<()> {
        // The HTTP API doesn't expose GRAPH.DELETE directly.
        // We can execute a Cypher that deletes all nodes/edges.
        self.post_query(graph, "MATCH (n) DELETE n").await?;
        Ok(())
    }

    async fn list_graphs(&self) -> SamyamaResult<Vec<String>> {
        // Single-graph mode in OSS
        Ok(vec!["default".to_string()])
    }

    async fn status(&self) -> SamyamaResult<ServerStatus> {
        let url = format!("{}/api/status", self.http_base_url);
        let response = self.send_with_retry(|| self.http_client.get(&url)).await?;

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
