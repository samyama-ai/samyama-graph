//! RESP protocol server implementation
//!
//! Implements REQ-REDIS-001, REQ-REDIS-002 (RESP server, Redis clients)

use crate::graph::GraphStore;
use crate::protocol::resp::{RespValue, RespError};
use crate::protocol::command::CommandHandler;
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 10000,
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
}

impl RespServer {
    /// Create a new RESP server
    pub fn new(config: ServerConfig, store: Arc<RwLock<GraphStore>>) -> Self {
        let handler = Arc::new(CommandHandler::new());
        Self {
            config,
            store,
            handler,
        }
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

            // Spawn a new task for each connection
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, store, handler).await {
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
                    // Process command
                    let response = handler.handle_command(&value, &store).await;

                    // Encode and send response
                    let mut response_buf = Vec::new();
                    response.encode(&mut response_buf)?;
                    socket.write_all(&response_buf).await?;
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
        let _server = RespServer::new(config, store);
    }

    #[tokio::test]
    async fn test_connection_handling() {
        // This is a basic test structure
        // Real integration tests would require a running server
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new());

        // Would need to set up mock socket for full test
        drop(store);
        drop(handler);
    }
}
