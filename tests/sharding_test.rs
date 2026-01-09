//! Integration test for Tenant Sharding (Phase 10)
//!
//! Spawns two server instances and tests request forwarding.

use samyama::{RespServer, ServerConfig, GraphStore};
use samyama::sharding::{Router, Proxy, RouteResult};
use samyama::raft::{ClusterManager, ClusterConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;

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
    cluster_config.add_node(1, "127.0.0.1:16379".to_string(), true);
    cluster_config.add_node(2, "127.0.0.1:16380".to_string(), true);
    let cluster_manager = Arc::new(ClusterManager::new(cluster_config).unwrap());

    let server = RespServer::new(config, store)
        .with_sharding(router.clone(), proxy, cluster_manager.clone());

    tokio::spawn(async move {
        server.start().await.unwrap();
    });

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    (router, cluster_manager)
}

#[tokio::test]
async fn test_request_forwarding() {
    // Start Node 1 (Port 16379)
    let (router1, _) = start_node(1, 16379).await;
    
    // Start Node 2 (Port 16380)
    let (_router2, _) = start_node(2, 16380).await;

    // Configure Router on Node 1: "tenant_local" -> Node 1, "tenant_remote" -> Node 2
    router1.update_route("tenant_local".to_string(), 1);
    router1.update_route("tenant_remote".to_string(), 2);

    // 1. Test Local Request (Direct to Node 1)
    let mut client = TcpStream::connect("127.0.0.1:16379").await.unwrap();
    // RESP: ARRAY(3) ["GRAPH.QUERY", "tenant_local", "CREATE (n:Local)"]
    let cmd_local = "*3\r\n$11\r\nGRAPH.QUERY\r\n$12\r\ntenant_local\r\n$18\r\nCREATE (n:Local)\r\n";
    client.write_all(cmd_local.as_bytes()).await.unwrap();
    
    let mut buf = [0u8; 1024];
    let n = client.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(response.contains("Node(")); // Success

    // 2. Test Remote Request (Node 1 -> Proxy -> Node 2)
    let cmd_remote = "*3\r\n$11\r\nGRAPH.QUERY\r\n$13\r\ntenant_remote\r\n$19\r\nCREATE (n:Remote)\r\n";
    client.write_all(cmd_remote.as_bytes()).await.unwrap();
    
    let n = client.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    // The response should come back from Node 2 (success)
    // If routing failed, we'd see an error or timeout
    assert!(response.contains("Node("));
    
    println!("✅ Sharding test passed: Local and Remote requests handled.");
}
