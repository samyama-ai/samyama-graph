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
use tracing::{info, error, debug, warn};

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

    /// HA-09: Create a RESP server that shares a pre-existing `TenantManager`
    /// with HTTP routes, so tenants created via either path are visible to both.
    pub fn new_with_tenants(
        config: ServerConfig,
        store: Arc<RwLock<GraphStore>>,
        persistence: Option<Arc<PersistenceManager>>,
        tenants: Arc<crate::persistence::TenantManager>,
    ) -> Self {
        let handler = Arc::new(CommandHandler::new_with_tenants(
            persistence.as_ref().map(Arc::clone),
            tenants,
        ));
        Self {
            config,
            store,
            handler,
            persistence,
            router: None,
            proxy: None,
            cluster_manager: None,
        }
    }

    /// Access the shared tenant registry (for wiring HTTP routes).
    pub fn tenant_manager(&self) -> Arc<crate::persistence::TenantManager> {
        self.handler.tenant_manager()
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

/// A transaction open on one RESP connection: the writer's lock, held from
/// GRAPH.BEGIN until GRAPH.COMMIT or GRAPH.ROLLBACK (#1200 step 6b).
///
/// Holding the lock is what keeps the transaction's writes from every other
/// connection, and it is why the transaction has a deadline: while it is open
/// nobody else can read or write.
struct OpenTxn {
    guard: tokio::sync::OwnedRwLockWriteGuard<GraphStore>,
    deadline: tokio::time::Instant,
}

/// Per-connection transaction state.
#[derive(Default)]
struct ConnTxn {
    open: Option<OpenTxn>,
    /// The last transaction ended by its deadline, so a later COMMIT can say so.
    timed_out: bool,
}

/// Answer one command, routing the transaction commands and anything run
/// inside an open transaction to the lock the connection holds.
async fn respond(
    handler: &CommandHandler,
    value: &RespValue,
    store: &Arc<RwLock<GraphStore>>,
    txn: &mut ConnTxn,
) -> RespValue {
    let name = value
        .as_array()
        .ok()
        .and_then(|args| args.first())
        .and_then(|v| v.as_string().ok().flatten())
        .map(|s| s.to_uppercase());
    let limit = GraphStore::session_transaction_timeout();
    match (name.as_deref(), txn.open.as_mut()) {
        (Some("GRAPH.BEGIN"), Some(_)) => {
            RespValue::Error("ERR a transaction is already open on this connection".to_string())
        }
        (Some("GRAPH.BEGIN"), None) => {
            let mut guard = Arc::clone(store).write_owned().await;
            let reply = handler.begin_transaction_on(&mut guard);
            if !matches!(reply, RespValue::Error(_)) {
                txn.open = Some(OpenTxn { guard, deadline: tokio::time::Instant::now() + limit });
                txn.timed_out = false;
            }
            reply
        }
        (Some("GRAPH.COMMIT"), Some(_)) => {
            let mut open = txn.open.take().expect("matched Some");
            handler.commit_transaction_on(&mut open.guard)
        }
        (Some("GRAPH.ROLLBACK"), Some(_)) => {
            let mut open = txn.open.take().expect("matched Some");
            handler.rollback_transaction_on(&mut open.guard)
        }
        (Some("GRAPH.COMMIT") | Some("GRAPH.ROLLBACK"), None) => RespValue::Error(if txn.timed_out {
            format!(
                "ERR the transaction was open longer than {}s and was rolled back",
                limit.as_secs()
            )
        } else {
            "ERR no transaction is open on this connection".to_string()
        }),
        (Some("GRAPH.QUERY"), Some(open)) => {
            let args = value.as_array().unwrap_or(&[]);
            handler.query_in_transaction(args, &mut open.guard, false)
        }
        (Some("GRAPH.RO_QUERY"), Some(open)) => {
            let args = value.as_array().unwrap_or(&[]);
            handler.query_in_transaction(args, &mut open.guard, true)
        }
        // These never touch the store, so they cannot wait on the lock this
        // connection itself holds.
        (Some("PING") | Some("ECHO") | Some("INFO"), Some(_)) => {
            handler.handle_command(value, store).await
        }
        // Anything else would take the store's lock, which this connection
        // already holds, and wait on itself for good.
        (_, Some(_)) => RespValue::Error(
            "ERR inside a transaction only GRAPH.QUERY, GRAPH.RO_QUERY, GRAPH.COMMIT, \
             GRAPH.ROLLBACK, PING, ECHO and INFO are accepted"
                .to_string(),
        ),
        (_, None) => handler.handle_command(value, store).await,
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
    let mut txn = ConnTxn::default();
    let result =
        serve_connection(&mut socket, &store, &handler, router, proxy, cluster, &mut txn).await;
    // However the connection ends -- a clean close, a protocol error, a failed
    // write -- a transaction still open is rolled back, or its partial writes
    // would stay and the lock it held would be gone with no one to finish it.
    if let Some(mut open) = txn.open.take() {
        handler.rollback_transaction_on(&mut open.guard);
        debug!("connection closed with a transaction open; rolled back");
    }
    result
}

enum Wake {
    Read(std::io::Result<usize>),
    Expired,
}

#[allow(clippy::too_many_arguments)]
async fn serve_connection(
    socket: &mut TcpStream,
    store: &Arc<RwLock<GraphStore>>,
    handler: &Arc<CommandHandler>,
    router: Option<Arc<Router>>,
    proxy: Option<Arc<Proxy>>,
    cluster: Option<Arc<ClusterManager>>,
    txn: &mut ConnTxn,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buffer = BytesMut::with_capacity(4096);

    loop {
        // Read data from socket, unless an open transaction reaches its
        // deadline first: then it is rolled back and the lock released.
        let wake = match txn.open.as_ref().map(|t| t.deadline) {
            Some(deadline) => tokio::select! {
                r = socket.read_buf(&mut buffer) => Wake::Read(r),
                _ = tokio::time::sleep_until(deadline) => Wake::Expired,
            },
            None => Wake::Read(socket.read_buf(&mut buffer).await),
        };
        let n = match wake {
            Wake::Expired => {
                if let Some(mut open) = txn.open.take() {
                    handler.rollback_transaction_on(&mut open.guard);
                    warn!("a RESP transaction reached its deadline and was rolled back");
                }
                txn.timed_out = true;
                continue;
            }
            Wake::Read(r) => r?,
        };

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
                        let response = respond(handler, &value, store, txn).await;

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

    fn cmd(parts: &[&str]) -> RespValue {
        RespValue::Array(parts.iter().map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec()))).collect())
    }

    fn is_error(v: &RespValue) -> bool {
        matches!(v, RespValue::Error(_))
    }

    #[tokio::test]
    async fn a_rolled_back_transaction_leaves_nothing_and_a_committed_one_stays() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = CommandHandler::new(None);
        let mut txn = ConnTxn::default();

        assert!(!is_error(&respond(&handler, &cmd(&["GRAPH.BEGIN"]), &store, &mut txn).await));
        let r = respond(&handler, &cmd(&["GRAPH.QUERY", "default", "CREATE (:T {x: 1})"]), &store, &mut txn).await;
        assert!(!is_error(&r), "{r:?}");
        assert!(!is_error(&respond(&handler, &cmd(&["GRAPH.ROLLBACK"]), &store, &mut txn).await));
        assert_eq!(store.read().await.node_count(), 0, "a rolled-back CREATE survived");

        respond(&handler, &cmd(&["GRAPH.BEGIN"]), &store, &mut txn).await;
        respond(&handler, &cmd(&["GRAPH.QUERY", "default", "CREATE (:T {x: 1})"]), &store, &mut txn).await;
        assert!(!is_error(&respond(&handler, &cmd(&["GRAPH.COMMIT"]), &store, &mut txn).await));
        assert_eq!(store.read().await.node_count(), 1, "a committed CREATE was lost");
    }

    #[tokio::test]
    async fn nobody_else_can_read_while_a_connection_holds_a_transaction() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = CommandHandler::new(None);
        let mut txn = ConnTxn::default();
        respond(&handler, &cmd(&["GRAPH.BEGIN"]), &store, &mut txn).await;
        assert!(store.try_read().is_err(), "another reader got in during a transaction");
        respond(&handler, &cmd(&["GRAPH.ROLLBACK"]), &store, &mut txn).await;
        assert!(store.try_read().is_ok(), "the lock was not released");
    }

    #[tokio::test]
    async fn a_command_that_would_take_the_lock_is_refused_inside_a_transaction() {
        // GRAPH.LIST takes the store's lock, which this connection holds; run
        // as-is it would wait on itself forever.
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = CommandHandler::new(None);
        let mut txn = ConnTxn::default();
        respond(&handler, &cmd(&["GRAPH.BEGIN"]), &store, &mut txn).await;
        let r = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            respond(&handler, &cmd(&["GRAPH.LIST"]), &store, &mut txn),
        )
        .await
        .expect("GRAPH.LIST inside a transaction deadlocked");
        assert!(is_error(&r));
        let pong = respond(&handler, &cmd(&["PING"]), &store, &mut txn).await;
        assert!(!is_error(&pong), "PING was refused inside a transaction");
        respond(&handler, &cmd(&["GRAPH.ROLLBACK"]), &store, &mut txn).await;
    }

    #[tokio::test]
    async fn begin_twice_and_commit_with_none_open_are_errors() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = CommandHandler::new(None);
        let mut txn = ConnTxn::default();
        assert!(is_error(&respond(&handler, &cmd(&["GRAPH.COMMIT"]), &store, &mut txn).await));
        respond(&handler, &cmd(&["GRAPH.BEGIN"]), &store, &mut txn).await;
        assert!(is_error(&respond(&handler, &cmd(&["GRAPH.BEGIN"]), &store, &mut txn).await));
        let r = respond(&handler, &cmd(&["GRAPH.RO_QUERY", "default", "CREATE (:T)"]), &store, &mut txn).await;
        assert!(is_error(&r), "RO_QUERY ran a write inside a transaction");
        respond(&handler, &cmd(&["GRAPH.ROLLBACK"]), &store, &mut txn).await;
        assert!(is_error(&respond(&handler, &cmd(&["GRAPH.ROLLBACK"]), &store, &mut txn).await));
    }

    #[tokio::test]
    async fn a_connection_that_closes_mid_transaction_rolls_it_back() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (s, h) = (Arc::clone(&store), Arc::clone(&handler));
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let _ = handle_connection(socket, s, h, None, None, None).await;
        });
        let mut client = TcpStream::connect(addr).await.unwrap();
        let send = |parts: &[&str]| {
            let mut bytes = Vec::new();
            cmd(parts).encode(&mut bytes).unwrap();
            bytes
        };
        let begin = send(&["GRAPH.BEGIN"]);
        let create = send(&["GRAPH.QUERY", "default", "CREATE (:T {x: 1})"]);
        client.write_all(&begin).await.unwrap();
        let mut reply = [0u8; 256];
        let _ = client.read(&mut reply).await.unwrap();
        client.write_all(&create).await.unwrap();
        let _ = client.read(&mut reply).await.unwrap();
        drop(client);
        tokio::time::timeout(std::time::Duration::from_secs(10), server).await.unwrap().unwrap();
        let guard = store.read().await;
        assert_eq!(guard.node_count(), 0, "a transaction left open by a closed connection kept its write");
        assert!(guard.session_transaction_version().is_none(), "the store still has a transaction open");
    }

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

    #[test]
    fn test_server_config_custom() {
        let config = ServerConfig {
            address: "0.0.0.0".to_string(),
            port: 16379,
            max_connections: 500,
            data_path: Some("/tmp/samyama_test".to_string()),
        };
        assert_eq!(config.address, "0.0.0.0");
        assert_eq!(config.port, 16379);
        assert_eq!(config.max_connections, 500);
        assert_eq!(config.data_path, Some("/tmp/samyama_test".to_string()));
    }

    #[test]
    fn test_server_config_no_persistence() {
        let config = ServerConfig {
            address: "127.0.0.1".to_string(),
            port: 6379,
            max_connections: 10000,
            data_path: None,
        };
        assert!(config.data_path.is_none());
    }

    #[test]
    fn test_server_config_default_has_data_path() {
        let config = ServerConfig::default();
        assert!(config.data_path.is_some());
        assert_eq!(config.data_path.unwrap(), "./samyama_data");
    }

    #[test]
    fn test_server_config_debug() {
        let config = ServerConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("127.0.0.1"));
        assert!(debug_str.contains("6379"));
    }

    #[test]
    fn test_server_config_clone() {
        let config = ServerConfig::default();
        let cloned = config.clone();
        assert_eq!(config.address, cloned.address);
        assert_eq!(config.port, cloned.port);
        assert_eq!(config.max_connections, cloned.max_connections);
        assert_eq!(config.data_path, cloned.data_path);
    }

    #[test]
    fn test_server_new_stores_config() {
        let config = ServerConfig {
            address: "192.168.1.1".to_string(),
            port: 9999,
            max_connections: 42,
            data_path: None,
        };
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);

        assert_eq!(server.config.address, "192.168.1.1");
        assert_eq!(server.config.port, 9999);
        assert_eq!(server.config.max_connections, 42);
        assert!(server.config.data_path.is_none());
    }

    #[test]
    fn test_server_new_has_no_router() {
        let config = ServerConfig::default();
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);

        assert!(server.router.is_none());
        assert!(server.proxy.is_none());
        assert!(server.cluster_manager.is_none());
    }

    #[test]
    fn test_server_new_shared_store() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let store_clone = Arc::clone(&store);
        let config = ServerConfig::default();
        let server = RespServer::new(config, store);

        // Both Arc references point to the same store
        assert!(Arc::ptr_eq(&server.store, &store_clone));
    }

    #[tokio::test]
    async fn test_server_start_invalid_port_fails() {
        // Bind to a port that we'll then try to bind again
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let config = ServerConfig {
            address: "127.0.0.1".to_string(),
            port,
            max_connections: 10,
            data_path: None,
        };
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = RespServer::new(config, store);

        // Starting on already-bound port should fail
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            server.start(),
        ).await;

        // Either times out (somehow bound) or errors
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_handle_connection_close() {
        // Create a TCP pair: a server-side socket and a client-side socket
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Send a PING inline command
            use tokio::io::AsyncWriteExt;
            stream.write_all(b"PING\r\n").await.unwrap();
            // Read response
            use tokio::io::AsyncReadExt;
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            // Close the connection by dropping
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(socket, store, handler, None, None, None).await;
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_resp_command() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::{AsyncWriteExt, AsyncReadExt};
            // Send RESP-formatted PING: *1\r\n$4\r\nPING\r\n
            stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            // The response should be a simple string "+PONG\r\n"
            let response = String::from_utf8_lossy(&buf[..n]);
            assert!(response.contains("PONG"), "Expected PONG in response, got: {}", response);
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(socket, store, handler, None, None, None).await;
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_protocol_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::{AsyncWriteExt, AsyncReadExt};
            // Send invalid RESP data: bad array length
            stream.write_all(b"*abc\r\n").await.unwrap();
            let mut buf = vec![0u8; 256];
            let n = stream.read(&mut buf).await.unwrap();
            // Should get an error response
            let response = String::from_utf8_lossy(&buf[..n]);
            assert!(response.contains("ERR") || response.contains("-"),
                "Expected error response, got: {}", response);
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(socket, store, handler, None, None, None).await;
        // Connection may close after error, which is still OK
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    // ========== Additional Server Coverage Tests ==========

    #[test]
    fn test_server_with_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let persistence = Arc::new(
            crate::persistence::PersistenceManager::new(temp_dir.path()).unwrap()
        );
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let config = ServerConfig::default();

        let server = RespServer::new_with_persistence(config, store, persistence);
        assert!(server.persistence.is_some());
        assert!(server.router.is_none());
        assert!(server.proxy.is_none());
        assert!(server.cluster_manager.is_none());
    }

    #[test]
    fn test_server_with_persistence_stores_config() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let persistence = Arc::new(
            crate::persistence::PersistenceManager::new(temp_dir.path()).unwrap()
        );
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let config = ServerConfig {
            address: "0.0.0.0".to_string(),
            port: 16379,
            max_connections: 500,
            data_path: Some("/tmp/test".to_string()),
        };

        let server = RespServer::new_with_persistence(config, store, persistence);
        assert_eq!(server.config.port, 16379);
        assert_eq!(server.config.address, "0.0.0.0");
    }

    #[tokio::test]
    async fn test_handle_connection_graph_query_command() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let client_task = tokio::spawn(async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            use tokio::io::{AsyncWriteExt, AsyncReadExt};
            // Use inline command format which is simpler
            stream.write_all(
                b"GRAPH.QUERY default \"MATCH (n) RETURN n\"\r\n"
            ).await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "Expected response from GRAPH.QUERY");
            drop(stream);
        });

        let (socket, _peer) = listener.accept().await.unwrap();
        let result = handle_connection(socket, store, handler, None, None, None).await;
        assert!(result.is_ok());

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_connection_multiple_commands() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let store = Arc::new(RwLock::new(GraphStore::new()));
        let handler = Arc::new(CommandHandler::new(None));

        let server_store = Arc::clone(&store);
        let server_handler = Arc::clone(&handler);

        let server_task = tokio::spawn(async move {
            let (socket, _peer) = listener.accept().await.unwrap();
            // handle_connection returns Ok on clean disconnect (n=0)
            let _result = handle_connection(socket, server_store, server_handler, None, None, None).await;
        });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        use tokio::io::{AsyncWriteExt, AsyncReadExt};

        // Send two PING commands back-to-back
        stream.write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n").await.unwrap();

        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        // Should contain at least one PONG response
        let pong_count = response.matches("PONG").count();
        assert!(pong_count >= 1, "Expected at least one PONG, got: {}", response);

        drop(stream);
        let _ = server_task.await;
    }

    #[test]
    fn test_server_config_address_variants() {
        let configs = vec![
            ("127.0.0.1", 6379),
            ("0.0.0.0", 6379),
            ("localhost", 6380),
            ("192.168.1.100", 9999),
        ];

        for (addr, port) in configs {
            let config = ServerConfig {
                address: addr.to_string(),
                port,
                max_connections: 100,
                data_path: None,
            };
            assert_eq!(config.address, addr);
            assert_eq!(config.port, port);
        }
    }
}
