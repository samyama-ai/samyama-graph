//! Cluster membership management

// NodeId removed - was unused import causing compiler warning
use crate::raft::{RaftError, RaftNodeId, RaftResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
// warn removed - was unused import causing compiler warning
use tracing::info;

/// Cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name
    pub name: String,
    /// Nodes in the cluster
    pub nodes: Vec<NodeConfig>,
    /// Replication factor
    pub replication_factor: usize,
}

/// Node configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Node ID
    pub id: RaftNodeId,
    /// Node address
    pub address: String,
    /// Is this node a voter?
    pub voter: bool,
}

impl ClusterConfig {
    /// Create a new cluster configuration
    pub fn new(name: String, replication_factor: usize) -> Self {
        Self {
            name,
            nodes: Vec::new(),
            replication_factor,
        }
    }

    /// Add a node to the configuration
    pub fn add_node(&mut self, id: RaftNodeId, address: String, voter: bool) {
        self.nodes.push(NodeConfig { id, address, voter });
    }

    /// Get voter nodes
    pub fn voters(&self) -> Vec<&NodeConfig> {
        self.nodes.iter().filter(|n| n.voter).collect()
    }

    /// Get learner nodes (non-voters)
    pub fn learners(&self) -> Vec<&NodeConfig> {
        self.nodes.iter().filter(|n| !n.voter).collect()
    }

    /// Validate configuration
    pub fn validate(&self) -> RaftResult<()> {
        if self.nodes.is_empty() {
            return Err(RaftError::Cluster("No nodes in cluster".to_string()));
        }

        let voters = self.voters();
        if voters.is_empty() {
            return Err(RaftError::Cluster("No voters in cluster".to_string()));
        }

        if voters.len() < self.replication_factor {
            return Err(RaftError::Cluster(format!(
                "Not enough voters ({}) for replication factor ({})",
                voters.len(),
                self.replication_factor
            )));
        }

        Ok(())
    }
}

/// Cluster manager
pub struct ClusterManager {
    /// Current configuration
    config: Arc<RwLock<ClusterConfig>>,
    /// Active nodes (heartbeat tracking)
    active_nodes: Arc<RwLock<HashSet<RaftNodeId>>>,
    /// Node metadata
    node_metadata: Arc<RwLock<HashMap<RaftNodeId, NodeMetadata>>>,
}

/// Node metadata
#[derive(Debug, Clone)]
pub struct NodeMetadata {
    /// Last heartbeat timestamp
    pub last_heartbeat: i64,
    /// Is node reachable
    pub reachable: bool,
    /// Current role (leader, follower, candidate)
    pub role: NodeRole,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeRole {
    Leader,
    Follower,
    Candidate,
    Learner,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub fn new(config: ClusterConfig) -> RaftResult<Self> {
        config.validate()?;

        info!("Creating cluster manager for cluster: {}", config.name);

        // Initialize metadata for all nodes in the config
        let mut node_metadata = HashMap::new();
        for node in &config.nodes {
            node_metadata.insert(
                node.id,
                NodeMetadata {
                    last_heartbeat: chrono::Utc::now().timestamp(),
                    reachable: false,
                    role: if node.voter {
                        NodeRole::Follower
                    } else {
                        NodeRole::Learner
                    },
                },
            );
        }

        Ok(Self {
            config: Arc::new(RwLock::new(config)),
            active_nodes: Arc::new(RwLock::new(HashSet::new())),
            node_metadata: Arc::new(RwLock::new(node_metadata)),
        })
    }

    /// Get cluster configuration
    pub async fn get_config(&self) -> ClusterConfig {
        self.config.read().await.clone()
    }

    /// Update cluster configuration
    pub async fn update_config(&self, config: ClusterConfig) -> RaftResult<()> {
        config.validate()?;

        let mut current = self.config.write().await;
        *current = config;

        info!("Updated cluster configuration");
        Ok(())
    }

    /// Add a node to the cluster
    pub async fn add_node(
        &self,
        id: RaftNodeId,
        address: String,
        voter: bool,
    ) -> RaftResult<()> {
        info!("Adding node {} to cluster at {}", id, address);

        let mut config = self.config.write().await;
        config.add_node(id, address, voter);

        // Initialize metadata
        let mut metadata = self.node_metadata.write().await;
        metadata.insert(
            id,
            NodeMetadata {
                last_heartbeat: chrono::Utc::now().timestamp(),
                reachable: false,
                role: if voter {
                    NodeRole::Follower
                } else {
                    NodeRole::Learner
                },
            },
        );

        Ok(())
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, id: RaftNodeId) -> RaftResult<()> {
        info!("Removing node {} from cluster", id);

        let mut config = self.config.write().await;
        config.nodes.retain(|n| n.id != id);

        let mut active = self.active_nodes.write().await;
        active.remove(&id);

        let mut metadata = self.node_metadata.write().await;
        metadata.remove(&id);

        Ok(())
    }

    /// Mark node as active (received heartbeat)
    pub async fn mark_active(&self, id: RaftNodeId) {
        let mut active = self.active_nodes.write().await;
        active.insert(id);

        let mut metadata = self.node_metadata.write().await;
        if let Some(meta) = metadata.get_mut(&id) {
            meta.last_heartbeat = chrono::Utc::now().timestamp();
            meta.reachable = true;
        }
    }

    /// Mark node as inactive
    pub async fn mark_inactive(&self, id: RaftNodeId) {
        let mut active = self.active_nodes.write().await;
        active.remove(&id);

        let mut metadata = self.node_metadata.write().await;
        if let Some(meta) = metadata.get_mut(&id) {
            meta.reachable = false;
        }
    }

    /// Get active nodes
    pub async fn get_active_nodes(&self) -> Vec<RaftNodeId> {
        self.active_nodes.read().await.iter().copied().collect()
    }

    /// Update node role
    pub async fn update_node_role(&self, id: RaftNodeId, role: NodeRole) {
        let mut metadata = self.node_metadata.write().await;
        if let Some(meta) = metadata.get_mut(&id) {
            meta.role = role;
        }
    }

    /// Get node metadata
    pub async fn get_node_metadata(&self, id: RaftNodeId) -> Option<NodeMetadata> {
        self.node_metadata.read().await.get(&id).cloned()
    }

    /// Get cluster health status
    pub async fn health_status(&self) -> ClusterHealth {
        let config = self.config.read().await;
        let active = self.active_nodes.read().await;
        let metadata = self.node_metadata.read().await;

        let total_nodes = config.nodes.len();
        let active_nodes = active.len();
        let voters = config.voters().len();
        let active_voters = config
            .voters()
            .iter()
            .filter(|n| active.contains(&n.id))
            .count();

        let has_leader = metadata.values().any(|m| m.role == NodeRole::Leader);

        let healthy = active_voters >= (voters / 2 + 1) && has_leader;

        ClusterHealth {
            healthy,
            total_nodes,
            active_nodes,
            total_voters: voters,
            active_voters,
            has_leader,
        }
    }
}

/// Cluster health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealth {
    /// Is cluster healthy?
    pub healthy: bool,
    /// Total number of nodes
    pub total_nodes: usize,
    /// Number of active nodes
    pub active_nodes: usize,
    /// Total number of voters
    pub total_voters: usize,
    /// Number of active voters
    pub active_voters: usize,
    /// Has a leader?
    pub has_leader: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config() {
        let mut config = ClusterConfig::new("test-cluster".to_string(), 3);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        config.add_node(3, "127.0.0.1:5002".to_string(), true);

        assert_eq!(config.voters().len(), 3);
        assert_eq!(config.learners().len(), 0);
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_cluster_manager() {
        let mut config = ClusterConfig::new("test-cluster".to_string(), 3);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        config.add_node(3, "127.0.0.1:5002".to_string(), true);

        let manager = ClusterManager::new(config).unwrap();

        manager.mark_active(1).await;
        manager.mark_active(2).await;

        let active = manager.get_active_nodes().await;
        assert_eq!(active.len(), 2);
    }

    #[tokio::test]
    async fn test_cluster_health() {
        let mut config = ClusterConfig::new("test-cluster".to_string(), 3);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        config.add_node(3, "127.0.0.1:5002".to_string(), true);

        let manager = ClusterManager::new(config).unwrap();

        manager.mark_active(1).await;
        manager.mark_active(2).await;
        manager.update_node_role(1, NodeRole::Leader).await;

        let health = manager.health_status().await;
        assert!(health.healthy);
        assert_eq!(health.active_voters, 2);
        assert!(health.has_leader);
    }
}
