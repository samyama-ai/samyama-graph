//! Integration test for Tenant Sharding (Phase 10)
//!
//! Spawns two server instances and tests request forwarding.
//! Requires free ports — skips gracefully if ports are unavailable.

use samyama::{RespServer, ServerConfig, GraphStore};
use samyama::sharding::{Router, Proxy};
use samyama::raft::{ClusterManager, ClusterConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::net::{TcpStream, TcpListener};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;

const PORT1: u16 = 36379;
const PORT2: u16 = 36380;

/// Check if a port is available by trying to bind it briefly.
async fn port_available(port: u16) -> bool {
    TcpListener::bind(format!("127.0.0.1:{}", port)).await.is_ok()
}

async fn start_node(id: u64, port: u16) -> (Arc<Router>, Arc<ClusterManager>) {
    let mut config = ServerConfig::default();
    config.port = port;
    config.address = "127.0.0.1".to_string();
    config.data_path = None; // In-memory

    let store = Arc::new(RwLock::new(GraphStore::new()));

    // Setup sharding components
    let router = Arc::new(Router::new(id));
    let proxy = Arc::new(Proxy::new());

    // Setup cluster manager (mock config)
    let mut cluster_config = ClusterConfig::new("test-cluster".to_string(), 1);
    cluster_config.add_node(1, format!("127.0.0.1:{}", PORT1), true);
    cluster_config.add_node(2, format!("127.0.0.1:{}", PORT2), true);
    let cluster_manager = Arc::new(ClusterManager::new(cluster_config).unwrap());

    let server = RespServer::new(config, store)
        .with_sharding(router.clone(), proxy, cluster_manager.clone());

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server on port {} failed: {}", port, e);
        }
    });

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    (router, cluster_manager)
}

#[tokio::test]
#[ignore = "Phase 10 sharding proxy not fully wired — run with --ignored"]
async fn test_request_forwarding() {
    // Skip if ports are unavailable (e.g., another test run or server occupying them)
    if !port_available(PORT1).await || !port_available(PORT2).await {
        eprintln!("Skipping sharding test: ports {}/{} not available", PORT1, PORT2);
        return;
    }

    // Wrap in timeout so the test never hangs
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        // Start Node 1
        let (router1, _) = start_node(1, PORT1).await;

        // Start Node 2
        let (_router2, _) = start_node(2, PORT2).await;

        // Configure Router on Node 1: "tenant_local" -> Node 1, "tenant_remote" -> Node 2
        router1.update_route("tenant_local".to_string(), 1);
        router1.update_route("tenant_remote".to_string(), 2);

        // 1. Test Local Request (Direct to Node 1)
        let mut client = TcpStream::connect(format!("127.0.0.1:{}", PORT1)).await.unwrap();
        let cmd_local = "*3\r\n$11\r\nGRAPH.QUERY\r\n$12\r\ntenant_local\r\n$18\r\nCREATE (n:Local)\r\n";
        client.write_all(cmd_local.as_bytes()).await.unwrap();

        let mut buf = [0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
            .await.expect("Local read timed out").unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("Node(")); // Success

        // 2. Test Remote Request (Node 1 -> Proxy -> Node 2)
        let cmd_remote = "*3\r\n$11\r\nGRAPH.QUERY\r\n$13\r\ntenant_remote\r\n$19\r\nCREATE (n:Remote)\r\n";
        client.write_all(cmd_remote.as_bytes()).await.unwrap();

        let n = tokio::time::timeout(Duration::from_secs(5), client.read(&mut buf))
            .await.expect("Remote read timed out").unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        assert!(response.contains("Node("));

        println!("Sharding test passed: Local and Remote requests handled.");
    }).await;

    assert!(result.is_ok(), "Sharding test timed out after 10s");
}
