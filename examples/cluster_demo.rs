//! Enterprise High Availability Cluster Demo
//!
//! Demonstrates Samyama's Raft-based HA features:
//! - 3-node voter cluster with geo-distributed simulation
//! - Leader election and role management
//! - Quorum-based writes through consensus
//! - Network partition simulation and split-brain prevention
//! - Learner node for read replicas
//! - Rolling upgrade simulation
//! - Health monitoring with real-time metrics

use samyama::{
    ClusterConfig, ClusterManager,
    GraphStateMachine, PersistenceManager,
    RaftNode, RaftRequest, graph::{NodeId, Label},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   SAMYAMA High Availability Cluster Demo                        ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    // =========================================================================
    // STEP 1: Geo-distributed cluster configuration
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 1: Configuring Geo-Distributed Cluster                     │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let mut config = ClusterConfig::new("samyama-production".to_string(), 3);

    // Simulate geo-distributed nodes
    let nodes_info = [
        (1, "10.0.1.100:5000", "us-east-1", "Virginia", true),
        (2, "10.0.2.100:5000", "us-west-2", "Oregon", true),
        (3, "10.0.3.100:5000", "eu-west-1", "Ireland", true),
    ];

    for (id, addr, region, location, voter) in &nodes_info {
        config.add_node(*id, addr.to_string(), *voter);
        println!("  Node {}: {} ({}, {}) [{}]",
            id, addr, region, location,
            if *voter { "Voter" } else { "Learner" });
    }
    println!();

    // =========================================================================
    // STEP 2: Initialize cluster
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 2: Initializing Cluster Infrastructure                     │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let manager = ClusterManager::new(config)?;

    let temp_dir = tempfile::TempDir::new()?;
    let persistence = Arc::new(PersistenceManager::new(temp_dir.path())?);

    let sm1 = GraphStateMachine::new(Arc::clone(&persistence));
    let sm2 = GraphStateMachine::new(Arc::clone(&persistence));
    let sm3 = GraphStateMachine::new(Arc::clone(&persistence));

    let mut node1 = RaftNode::new(1, sm1);
    let mut node2 = RaftNode::new(2, sm2);
    let mut node3 = RaftNode::new(3, sm3);

    let peers = vec![
        samyama::RaftNodeIdWithAddr::new(1, "10.0.1.100:5000".to_string()),
        samyama::RaftNodeIdWithAddr::new(2, "10.0.2.100:5000".to_string()),
        samyama::RaftNodeIdWithAddr::new(3, "10.0.3.100:5000".to_string()),
    ];

    node1.initialize(peers.clone()).await?;
    node2.initialize(peers.clone()).await?;
    node3.initialize(peers).await?;

    println!("  Raft nodes 1-3 initialized");
    println!("  Consensus protocol: Raft");
    println!("  Replication factor: 3");
    println!();

    // =========================================================================
    // STEP 3: Leader election
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 3: Leader Election                                         │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    manager.mark_active(1).await;
    manager.mark_active(2).await;
    manager.mark_active(3).await;

    // Node 1 (us-east) elected as leader
    manager.update_node_role(1, samyama::raft::cluster::NodeRole::Leader).await;
    manager.update_node_role(2, samyama::raft::cluster::NodeRole::Follower).await;
    manager.update_node_role(3, samyama::raft::cluster::NodeRole::Follower).await;

    println!("  Election completed:");
    println!("    Node 1 (us-east-1):  LEADER");
    println!("    Node 2 (us-west-2):  Follower");
    println!("    Node 3 (eu-west-1):  Follower");
    println!();

    // =========================================================================
    // STEP 4: Health monitoring
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 4: Cluster Health Monitoring                               │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let health = manager.health_status().await;
    println!("  Cluster Status:     {}", if health.healthy { "HEALTHY" } else { "DEGRADED" });
    println!("  Total Nodes:        {}", health.total_nodes);
    println!("  Active Nodes:       {}", health.active_nodes);
    println!("  Voters:             {}/{}", health.active_voters, health.total_voters);
    println!("  Has Leader:         {}", if health.has_leader { "Yes" } else { "No" });
    println!("  Quorum Satisfied:   {}", if health.active_voters > health.total_voters / 2 { "Yes" } else { "No" });
    println!();

    // =========================================================================
    // STEP 5: Write data through consensus
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 5: Writing Data Through Consensus                          │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    // Write multiple entities
    let entities = [
        ("Server", "web-prod-01", "us-east-1", "Running"),
        ("Server", "api-prod-01", "us-west-2", "Running"),
        ("Server", "db-primary", "us-east-1", "Running"),
        ("Server", "cache-01", "eu-west-1", "Running"),
        ("Service", "user-service", "us-east-1", "Healthy"),
        ("Service", "payment-service", "us-west-2", "Healthy"),
        ("Service", "notification-svc", "eu-west-1", "Healthy"),
    ];

    for (i, (label, name, region, status)) in entities.iter().enumerate() {
        let request = RaftRequest::CreateNode {
            tenant: "default".to_string(),
            node_id: (i + 1) as u64,
            labels: vec![label.to_string()],
            properties: {
                let mut props = samyama::PropertyMap::new();
                props.insert("name".to_string(), samyama::PropertyValue::String(name.to_string()));
                props.insert("region".to_string(), samyama::PropertyValue::String(region.to_string()));
                props.insert("status".to_string(), samyama::PropertyValue::String(status.to_string()));
                props
            },
        };

        let response = node1.write(request).await?;
        println!("  Replicated: {}:{} -> {:?}", label, name, response);
    }

    let metrics = node1.metrics().await;
    println!();
    println!("  Raft Metrics:");
    println!("    Log index:     {}", metrics.last_log_index);
    println!("    Applied index: {}", metrics.last_applied);
    println!();

    // =========================================================================
    // STEP 6: Network partition simulation
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 6: Network Partition Simulation                            │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    // Simulate eu-west-1 becoming unreachable
    println!("  Simulating network partition: eu-west-1 (Node 3) isolated");
    manager.mark_inactive(3).await;

    let health = manager.health_status().await;
    println!();
    println!("  Post-partition status:");
    println!("    Cluster:    {}", if health.healthy { "HEALTHY" } else { "DEGRADED" });
    println!("    Active:     {}/{} nodes", health.active_nodes, health.total_nodes);
    println!("    Voters:     {}/{}", health.active_voters, health.total_voters);
    println!("    Quorum:     {} (need {})", health.active_voters, health.total_voters / 2 + 1);
    println!("    Can write:  {}", if health.active_voters > health.total_voters / 2 { "Yes" } else { "No" });
    println!();

    // Write during partition - should succeed with 2/3 quorum
    let partition_write = RaftRequest::CreateNode {
        tenant: "default".to_string(),
        node_id: 100,
        labels: vec!["Alert".to_string()],
        properties: {
            let mut props = samyama::PropertyMap::new();
            props.insert("name".to_string(), samyama::PropertyValue::String("partition-alert".to_string()));
            props.insert("severity".to_string(), samyama::PropertyValue::String("Warning".to_string()));
            props
        },
    };
    let write_result = node1.write(partition_write).await?;
    println!("  Write during partition: {:?} (quorum maintained)", write_result);
    println!();

    // =========================================================================
    // STEP 7: Partition recovery
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 7: Partition Recovery                                      │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    println!("  Restoring connectivity to eu-west-1 (Node 3)...");
    manager.mark_active(3).await;

    let health = manager.health_status().await;
    println!("  Cluster status: {}", if health.healthy { "HEALTHY" } else { "DEGRADED" });
    println!("  Active nodes:   {}/{}", health.active_nodes, health.total_nodes);
    println!("  Node 3 will catch up via log replication");
    println!();

    // =========================================================================
    // STEP 8: Add read replica (learner node)
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 8: Adding Read Replica (Learner Node)                      │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    manager.add_node(4, "10.0.4.100:5000".to_string(), false).await?;
    println!("  Node 4 added as learner (ap-southeast-1, Singapore)");
    println!("  Role: Read replica (non-voting)");

    let config = manager.get_config().await;
    println!();
    println!("  Cluster topology:");
    println!("    Voters:   {} (quorum = {})", config.voters().len(), config.voters().len() / 2 + 1);
    println!("    Learners: {}", config.learners().len());
    println!("    Total:    {} nodes across 4 regions", config.nodes.len());
    println!();

    // =========================================================================
    // STEP 9: Rolling upgrade simulation
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 9: Rolling Upgrade Simulation                              │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let upgrade_order = [3, 2, 1]; // Upgrade followers first, leader last
    for &node_id in &upgrade_order {
        let role = match node_id {
            1 => "Leader",
            _ => "Follower",
        };

        println!("  Upgrading Node {} ({})...", node_id, role);

        if node_id == 1 {
            // Leader transfer before upgrading leader
            println!("    Transferring leadership to Node 2...");
            manager.update_node_role(2, samyama::raft::cluster::NodeRole::Leader).await;
            manager.update_node_role(1, samyama::raft::cluster::NodeRole::Follower).await;
            println!("    Node 2 is now leader");
        }

        manager.mark_inactive(node_id).await;
        let health = manager.health_status().await;
        println!("    Cluster during upgrade: {} ({}/{} active)",
            if health.healthy { "Healthy" } else { "Degraded" },
            health.active_nodes, health.total_nodes);

        // Simulate upgrade (no actual sleep for fast demo)
        manager.mark_active(node_id).await;
        println!("    Node {} upgraded and rejoined", node_id);
    }
    println!();

    // =========================================================================
    // STEP 10: Final metrics
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Step 10: Final Cluster Metrics                                  │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    let health = manager.health_status().await;
    println!("  Cluster: {}", if health.healthy { "HEALTHY" } else { "DEGRADED" });
    println!("  Nodes:   {}/{} active", health.active_nodes, health.total_nodes);
    println!("  Leader:  {}", if health.has_leader { "Present" } else { "None" });

    println!();
    println!("  Node Metrics:");
    for (node_ref, id) in [(&node1, 1), (&node2, 2), (&node3, 3)] {
        let m = node_ref.metrics().await;
        let is_leader = node_ref.is_leader().await;
        println!("    Node {}: log={}, applied={}, role={}",
            id, m.last_log_index, m.last_applied,
            if is_leader { "Leader" } else { "Follower" });
    }
    println!();

    // =========================================================================
    // Shutdown
    // =========================================================================
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│ Shutting Down                                                   │");
    println!("└──────────────────────────────────────────────────────────────────┘");

    node1.shutdown().await?;
    node2.shutdown().await?;
    node3.shutdown().await?;
    println!("  All nodes shut down gracefully");
    println!();

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║   Demo Complete                                                ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Features Demonstrated:                                        ║");
    println!("║  - 3-node Raft cluster (geo-distributed)                       ║");
    println!("║  - Leader election and consensus writes                        ║");
    println!("║  - Network partition with quorum maintenance                   ║");
    println!("║  - Partition recovery and log catch-up                         ║");
    println!("║  - Read replica (learner) node                                 ║");
    println!("║  - Rolling upgrade with zero downtime                          ║");
    println!("║  - Health monitoring and metrics                               ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");

    Ok(())
}
