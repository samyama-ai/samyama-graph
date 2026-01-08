//! RESP protocol server implementation
//!
//! Implements REQ-REDIS-001, REQ-REDIS-002 (RESP server, Redis clients)

use crate::graph::GraphStore;
use crate::persistence::PersistenceManager;
use crate::protocol::resp::{RespValue, RespError};
use crate::protocol::command::CommandHandler;
use crate::sharding::{Router, Proxy, RouteResult};
use crate::raft::ClusterManager;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{info, error, debug};

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Bind address
    pub address: String,
    /// Port
    pub port: u16,
    /// Maximum connections
    pub max_connections: usize,
    /// Data directory for persistence (None = in-memory only)
    pub data_path: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 10000,
            data_path: Some("./samyama_data".to_string()),
        }
    }
}

/// RESP protocol server
pub struct RespServer {
    /// Server configuration
    config: ServerConfig,
    /// Shared graph store
    store: Arc<RwLock<GraphStore>>,
    /// Command handler
    handler: Arc<CommandHandler>,
    /// Optional persistence manager for durability
    /// When Some, writes are persisted to disk via WAL + RocksDB
    persistence: Option<Arc<PersistenceManager>>,
    /// Optional sharding router
    router: Option<Arc<Router>>,
    /// Optional proxy client
    proxy: Option<Arc<Proxy>>,
    /// Optional cluster manager for resolving node addresses
    cluster_manager: Option<Arc<ClusterManager>>,
}

impl RespServer {
    /// Create a new RESP server (in-memory only, no persistence)
    pub fn new(config: ServerConfig, store: Arc<RwLock<GraphStore>>) -> Self {
        let handler = Arc::new(CommandHandler::new(None));
        Self {
            config,
            store,
            handler,
            persistence: None,
            router: None,
            proxy: None,
            cluster_manager: None,
        }
    }

    /// Create a new RESP server with persistence enabled
    /// Data will be persisted to disk via WAL + RocksDB
    pub fn new_with_persistence(
        config: ServerConfig,
        store: Arc<RwLock<GraphStore>>,
        persistence: Arc<PersistenceManager>,
    ) -> Self {
        let handler = Arc::new(CommandHandler::new(Some(Arc::clone(&persistence))));
        Self {
            config,
            store,
            handler,
            persistence: Some(persistence),
            router: None,
            proxy: None,
            cluster_manager: None,
        }
    }

    /// Enable sharding for this server
    pub fn with_sharding(
        mut self,
        router: Arc<Router>,
        proxy: Arc<Proxy>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        self.router = Some(router);
        self.proxy = Some(proxy);
        self.cluster_manager = Some(cluster_manager);
        self
    }

    /// Start the server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.address, self.config.port);
        let listener = TcpListener::bind(&addr).await?;

        info!("RESP server listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            debug!("New connection from {}", peer_addr);

            let store = Arc::clone(&self.store);
            let handler = Arc::clone(&self.handler);
            let router = self.router.clone();
            let proxy = self.proxy.clone();
            let cluster = self.cluster_manager.clone();

            // Spawn a new task for each connection
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, store, handler, router, proxy, cluster).await {
                    error!("Error handling connection from {}: {}", peer_addr, e);
                }
            });
        }
    }
}

/// Handle a single client connection
async fn handle_connection(
    mut socket: TcpStream,
    store: Arc<RwLock<GraphStore>>,
    handler: Arc<CommandHandler>,
    router: Option<Arc<Router>>,
    proxy: Option<Arc<Proxy>>,
    cluster: Option<Arc<ClusterManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read data from socket
        let n = socket.read_buf(&mut buffer).await?;

        if n == 0 {
            // Connection closed
            debug!("Connection closed by client");
            return Ok(());
        }

        // Try to parse RESP commands
        loop {
            match RespValue::decode(&mut buffer) {
                Ok(Some(value)) => {
                    let mut forwarded = false;

                    // Attempt routing if configured
                    if let (Some(router), Some(proxy), Some(cluster)) = (&router, &proxy, &cluster) {
                        if let Ok(args) = value.as_array() {
                            if args.len() >= 2 {
                                if let Ok(Some(cmd)) = args[0].as_string() {
                                    if cmd.to_uppercase().starts_with("GRAPH.") {
                                        if let Ok(Some(key)) = args[1].as_string() {
                                            if let Some(RouteResult::Remote(node_id)) = router.route(&key) {
                                                // Resolve address from ClusterConfig
                                                let config = cluster.get_config().await;
                                                if let Some(node_config) = config.nodes.iter().find(|n| n.id == node_id) {
                                                    debug!("Routing command for tenant '{}' to node {} ({})", key, node_id, node_config.address);
                                                    
                                                    // Re-encode command
                                                    let mut cmd_bytes = Vec::new();
                                                    value.encode(&mut cmd_bytes)?;

                                                    // Forward
                                                    match proxy.forward(&node_config.address, &cmd_bytes).await {
                                                        Ok(response_bytes) => {
                                                            socket.write_all(&response_bytes).await?;
                                                            forwarded = true;
                                                        }
                                                        Err(e) => {
                                                            error!("Failed to forward request: {}", e);
                                                            let err = RespValue::Error(format!("ERR routing failed: {}", e));
                                                            let mut buf = Vec::new();
                                                            err.encode(&mut buf)?;
                                                            socket.write_all(&buf).await?;
                                                            forwarded = true; // Handled as error
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if !forwarded {
                        // Process command locally
                        let response = handler.handle_command(&value, &store).await;

                        // Encode and send response
                        let mut response_buf = Vec::new();
                        response.encode(&mut response_buf)?;
                        socket.write_all(&response_buf).await?;
                    }
                }
                Ok(None) => {
                    // Need more data
                    break;
                }
                Err(RespError::Incomplete) => {
                    // Need more data
                    break;
                }
                Err(e) => {
                    // Protocol error
                    error!("Protocol error: {}", e);
                    let error_response = RespValue::Error(format!("ERR {}", e));
                    let mut response_buf = Vec::new();
                    error_response.encode(&mut response_buf)?;
                    socket.write_all(&response_buf).await?;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.address, "127.0.0.1");
        assert_eq!(config.port, 6379);
        assert_eq!(config.max_connections, 10000);
    }

    #[test]
    fn test_server_creation() {
        let config = ServerConfig::default();
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);
        // In-memory server should have no persistence
        assert!(server.persistence.is_none());
    }

    #[tokio::test]
    async fn test_connection_handling() {
        // This is a basic test structure
        // Real integration tests would require a running server
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));  // No persistence for tests

        // Would need to set up mock socket for full test
        drop(store);
        drop(handler);
    }
}
