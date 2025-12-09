//! Demonstration of Phase 4 High Availability features
//!
//! This example shows:
//! - Raft cluster setup
//! - Leader election simulation
//! - Cluster membership management
//! - Health monitoring

use samyama::{
    ClusterConfig, ClusterManager,
    GraphStateMachine, PersistenceManager,
    RaftNode, RaftRequest, graph::{NodeId, Label},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Samyama Cluster Demo ===\n");

    // 1. Create cluster configuration
    println!("1. Creating 3-node cluster configuration");
    let mut config = ClusterConfig::new("samyama-cluster".to_string(), 3);
    config.add_node(1, "127.0.0.1:5000".to_string(), true); // Voter
    config.add_node(2, "127.0.0.1:5001".to_string(), true); // Voter
    config.add_node(3, "127.0.0.1:5002".to_string(), true); // Voter
    println!("   ✓ Cluster 'samyama-cluster' configured with 3 voter nodes\n");

    // 2. Create cluster manager
    println!("2. Initializing cluster manager");
    let manager = ClusterManager::new(config)?;
    println!("   ✓ Cluster manager initialized\n");

    // 3. Create Raft nodes
    println!("3. Creating Raft nodes");
    let temp_dir = tempfile::TempDir::new()?;
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path())?);

    let sm1 = GraphStateMachine::new(Arc::clone(&persistence));
    let sm2 = GraphStateMachine::new(Arc::clone(&persistence));
    let sm3 = GraphStateMachine::new(Arc::clone(&persistence));

    let mut node1 = RaftNode::new(1, sm1);
    let mut node2 = RaftNode::new(2, sm2);
    let mut node3 = RaftNode::new(3, sm3);

    println!("   ✓ Created Raft nodes 1, 2, 3\n");

    // 4. Initialize nodes
    println!("4. Initializing Raft nodes");
    let peers = vec![
        samyama::RaftNodeIdWithAddr::new(1, "127.0.0.1:5000".to_string()),
        samyama::RaftNodeIdWithAddr::new(2, "127.0.0.1:5001".to_string()),
        samyama::RaftNodeIdWithAddr::new(3, "127.0.0.1:5002".to_string()),
    ];

    node1.initialize(peers.clone()).await?;
    node2.initialize(peers.clone()).await?;
    node3.initialize(peers).await?;

    println!("   ✓ All nodes initialized\n");

    // 5. Simulate leader election
    println!("5. Simulating leader election");
    manager.mark_active(1).await;
    manager.mark_active(2).await;
    manager.mark_active(3).await;

    // Node 1 becomes leader
    manager.update_node_role(1, samyama::raft::cluster::NodeRole::Leader).await;
    manager.update_node_role(2, samyama::raft::cluster::NodeRole::Follower).await;
    manager.update_node_role(3, samyama::raft::cluster::NodeRole::Follower).await;

    println!("   ✓ Node 1 elected as leader");
    println!("   ✓ Nodes 2 and 3 are followers\n");

    // 6. Check cluster health
    println!("6. Checking cluster health");
    let health = manager.health_status().await;
    println!("   Cluster: {}", if health.healthy { "HEALTHY ✓" } else { "UNHEALTHY ✗" });
    println!("   Total nodes: {}", health.total_nodes);
    println!("   Active nodes: {}", health.active_nodes);
    println!("   Voters: {}/{}", health.active_voters, health.total_voters);
    println!("   Has leader: {}\n", if health.has_leader { "Yes" } else { "No" });

    // 7. Write data through leader
    println!("7. Writing data through Raft consensus");
    let request = RaftRequest::CreateNode {
        tenant: "default".to_string(),
        node_id: 1,
        labels: vec!["Person".to_string()],
        properties: {
            let mut props = samyama::PropertyMap::new();
            props.insert("name".to_string(), samyama::PropertyValue::String("Alice".to_string()));
            props.insert("age".to_string(), samyama::PropertyValue::Integer(30));
            props
        },
    };

    let response = node1.write(request).await?;
    println!("   ✓ Node created: {:?}", response);

    let metrics = node1.metrics().await;
    println!("   ✓ Log index: {}, Applied: {}\n", metrics.last_log_index, metrics.last_applied);

    // 8. Simulate node failure
    println!("8. Simulating node failure");
    manager.mark_inactive(3).await;
    println!("   ✓ Node 3 marked as inactive");

    let health = manager.health_status().await;
    println!("   Cluster: {}", if health.healthy { "HEALTHY ✓" } else { "UNHEALTHY ✗" });
    println!("   Active nodes: {}/{}",health.active_nodes, health.total_nodes);
    println!("   Note: Cluster remains healthy with quorum (2 out of 3)\n");

    // 9. Add a learner node
    println!("9. Adding a learner (non-voting) node");
    manager.add_node(4, "127.0.0.1:5003".to_string(), false).await?;
    println!("   ✓ Node 4 added as learner");

    let config = manager.get_config().await;
    println!("   Total nodes in cluster: {}", config.nodes.len());
    println!("   Voters: {}", config.voters().len());
    println!("   Learners: {}\n", config.learners().len());

    // 10. Check leadership
    println!("10. Verifying leadership");
    println!("    Node 1 is leader: {}", node1.is_leader().await);
    println!("    Node 2 is leader: {}", node2.is_leader().await);
    println!("    Node 3 is leader: {}", node3.is_leader().await);

    let leader_id = node1.get_leader().await;
    println!("    Current leader: {:?}\n", leader_id);

    // 11. Shutdown
    println!("11. Shutting down cluster");
    node1.shutdown().await?;
    node2.shutdown().await?;
    node3.shutdown().await?;
    println!("    ✓ All nodes shut down cleanly\n");

    println!("=== Demo Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("  ✓ 3-node Raft cluster");
    println!("  ✓ Leader election and role management");
    println!("  ✓ Cluster health monitoring");
    println!("  ✓ Quorum-based availability (2/3 nodes)");
    println!("  ✓ Learner node support");
    println!("  ✓ Write operations through consensus");
    println!("  ✓ Graceful shutdown");

    Ok(())
}
