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
    pub async fn add_node(&self, id: RaftNodeId, address: String, voter: bool) -> RaftResult<()> {
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

    // ========== Additional Cluster Coverage Tests ==========

    #[test]
    fn test_cluster_config_new_defaults() {
        let config = ClusterConfig::new("my-cluster".to_string(), 5);
        assert_eq!(config.name, "my-cluster");
        assert_eq!(config.replication_factor, 5);
        assert!(config.nodes.is_empty());
        assert_eq!(config.voters().len(), 0);
        assert_eq!(config.learners().len(), 0);
    }

    #[test]
    fn test_cluster_config_with_learners() {
        let mut config = ClusterConfig::new("test".to_string(), 2);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        config.add_node(3, "127.0.0.1:5002".to_string(), false); // learner
        config.add_node(4, "127.0.0.1:5003".to_string(), false); // learner

        assert_eq!(config.voters().len(), 2);
        assert_eq!(config.learners().len(), 2);
        assert_eq!(config.nodes.len(), 4);
    }

    #[test]
    fn test_cluster_config_validate_empty_nodes() {
        let config = ClusterConfig::new("test".to_string(), 1);
        let result = config.validate();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, RaftError::Cluster(_)));
        let msg = format!("{}", err);
        assert!(msg.contains("No nodes"));
    }

    #[test]
    fn test_cluster_config_validate_no_voters() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), false); // learner only
        config.add_node(2, "127.0.0.1:5001".to_string(), false);

        let result = config.validate();
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("No voters"));
    }

    #[test]
    fn test_cluster_config_validate_insufficient_voters() {
        let mut config = ClusterConfig::new("test".to_string(), 5);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        // Only 2 voters but replication_factor is 5

        let result = config.validate();
        assert!(result.is_err());
        let msg = format!("{}", result.unwrap_err());
        assert!(msg.contains("Not enough voters"));
    }

    #[test]
    fn test_cluster_config_serialization() {
        let mut config = ClusterConfig::new("test-cluster".to_string(), 3);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), false);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: ClusterConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test-cluster");
        assert_eq!(deserialized.replication_factor, 3);
        assert_eq!(deserialized.nodes.len(), 2);
        assert_eq!(deserialized.nodes[0].id, 1);
        assert!(deserialized.nodes[0].voter);
        assert_eq!(deserialized.nodes[1].id, 2);
        assert!(!deserialized.nodes[1].voter);
    }

    #[test]
    fn test_node_config_serialization() {
        let node_config = NodeConfig {
            id: 42,
            address: "10.0.0.1:6379".to_string(),
            voter: true,
        };
        let json = serde_json::to_string(&node_config).unwrap();
        let deserialized: NodeConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, 42);
        assert_eq!(deserialized.address, "10.0.0.1:6379");
        assert!(deserialized.voter);
    }

    #[test]
    fn test_cluster_manager_new_fails_without_valid_config() {
        let config = ClusterConfig::new("empty".to_string(), 1);
        let result = ClusterManager::new(config);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cluster_manager_get_config() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        let cfg = manager.get_config().await;
        assert_eq!(cfg.name, "test");
        assert_eq!(cfg.nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_cluster_manager_update_config() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        let mut new_config = ClusterConfig::new("updated".to_string(), 2);
        new_config.add_node(1, "127.0.0.1:5000".to_string(), true);
        new_config.add_node(2, "127.0.0.1:5001".to_string(), true);

        manager.update_config(new_config).await.unwrap();

        let cfg = manager.get_config().await;
        assert_eq!(cfg.name, "updated");
        assert_eq!(cfg.nodes.len(), 2);
    }

    #[tokio::test]
    async fn test_cluster_manager_update_config_invalid() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Empty config should fail validation
        let empty_config = ClusterConfig::new("empty".to_string(), 1);
        let result = manager.update_config(empty_config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cluster_manager_add_node() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        manager
            .add_node(2, "127.0.0.1:5001".to_string(), true)
            .await
            .unwrap();

        let cfg = manager.get_config().await;
        assert_eq!(cfg.nodes.len(), 2);

        // Check metadata was created for the new node
        let meta = manager.get_node_metadata(2).await;
        assert!(meta.is_some());
        let meta = meta.unwrap();
        assert_eq!(meta.role, NodeRole::Follower);
        assert!(!meta.reachable);
    }

    #[tokio::test]
    async fn test_cluster_manager_add_learner_node() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        manager
            .add_node(2, "127.0.0.1:5001".to_string(), false)
            .await
            .unwrap();

        let meta = manager.get_node_metadata(2).await.unwrap();
        assert_eq!(meta.role, NodeRole::Learner);
    }

    #[tokio::test]
    async fn test_cluster_manager_remove_node() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        manager.mark_active(2).await;

        manager.remove_node(2).await.unwrap();

        let cfg = manager.get_config().await;
        assert_eq!(cfg.nodes.len(), 1);
        assert_eq!(cfg.nodes[0].id, 1);

        // Metadata should also be removed
        let meta = manager.get_node_metadata(2).await;
        assert!(meta.is_none());

        // Active set should also be cleaned
        let active = manager.get_active_nodes().await;
        assert!(!active.contains(&2));
    }

    #[tokio::test]
    async fn test_cluster_manager_mark_active_inactive() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Initially not active
        let active = manager.get_active_nodes().await;
        assert!(active.is_empty());

        // Mark active
        manager.mark_active(1).await;
        let active = manager.get_active_nodes().await;
        assert_eq!(active.len(), 1);
        assert!(active.contains(&1));

        // Check metadata updated
        let meta = manager.get_node_metadata(1).await.unwrap();
        assert!(meta.reachable);
        assert!(meta.last_heartbeat > 0);

        // Mark inactive
        manager.mark_inactive(1).await;
        let active = manager.get_active_nodes().await;
        assert!(active.is_empty());

        let meta = manager.get_node_metadata(1).await.unwrap();
        assert!(!meta.reachable);
    }

    #[tokio::test]
    async fn test_cluster_manager_update_node_role() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Initially follower
        let meta = manager.get_node_metadata(1).await.unwrap();
        assert_eq!(meta.role, NodeRole::Follower);

        // Promote to leader
        manager.update_node_role(1, NodeRole::Leader).await;
        let meta = manager.get_node_metadata(1).await.unwrap();
        assert_eq!(meta.role, NodeRole::Leader);

        // Change to candidate
        manager.update_node_role(1, NodeRole::Candidate).await;
        let meta = manager.get_node_metadata(1).await.unwrap();
        assert_eq!(meta.role, NodeRole::Candidate);
    }

    #[tokio::test]
    async fn test_cluster_manager_get_node_metadata_nonexistent() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        let meta = manager.get_node_metadata(999).await;
        assert!(meta.is_none());
    }

    #[tokio::test]
    async fn test_cluster_health_unhealthy_no_leader() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        config.add_node(3, "127.0.0.1:5002".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Mark majority active but no leader
        manager.mark_active(1).await;
        manager.mark_active(2).await;

        let health = manager.health_status().await;
        assert!(!health.healthy); // no leader
        assert_eq!(health.active_voters, 2);
        assert!(!health.has_leader);
        assert_eq!(health.total_nodes, 3);
        assert_eq!(health.total_voters, 3);
    }

    #[tokio::test]
    async fn test_cluster_health_unhealthy_no_quorum() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), true);
        config.add_node(3, "127.0.0.1:5002".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Only 1 active voter with leader (not quorum of 3)
        manager.mark_active(1).await;
        manager.update_node_role(1, NodeRole::Leader).await;

        let health = manager.health_status().await;
        assert!(!health.healthy); // no quorum (1 < 3/2+1=2)
        assert_eq!(health.active_voters, 1);
    }

    #[tokio::test]
    async fn test_cluster_health_serialization() {
        let health = ClusterHealth {
            healthy: true,
            total_nodes: 5,
            active_nodes: 3,
            total_voters: 3,
            active_voters: 2,
            has_leader: true,
        };
        let json = serde_json::to_string(&health).unwrap();
        let deserialized: ClusterHealth = serde_json::from_str(&json).unwrap();
        assert!(deserialized.healthy);
        assert_eq!(deserialized.total_nodes, 5);
        assert_eq!(deserialized.active_nodes, 3);
        assert!(deserialized.has_leader);
    }

    #[test]
    fn test_node_role_serialization() {
        let roles = vec![
            NodeRole::Leader,
            NodeRole::Follower,
            NodeRole::Candidate,
            NodeRole::Learner,
        ];
        for role in roles {
            let json = serde_json::to_string(&role).unwrap();
            let deserialized: NodeRole = serde_json::from_str(&json).unwrap();
            assert_eq!(role, deserialized);
        }
    }

    #[tokio::test]
    async fn test_mark_active_nonexistent_node() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Mark a node active that doesn't exist in metadata -- should not panic
        manager.mark_active(999).await;
        let active = manager.get_active_nodes().await;
        assert!(active.contains(&999));
    }

    #[tokio::test]
    async fn test_mark_inactive_nonexistent_node() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Should not panic
        manager.mark_inactive(999).await;
    }

    #[tokio::test]
    async fn test_update_role_nonexistent_node() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        let manager = ClusterManager::new(config).unwrap();

        // Should not panic
        manager.update_node_role(999, NodeRole::Leader).await;
    }

    #[tokio::test]
    async fn test_cluster_manager_node_metadata_initialization() {
        let mut config = ClusterConfig::new("test".to_string(), 1);
        config.add_node(1, "127.0.0.1:5000".to_string(), true);
        config.add_node(2, "127.0.0.1:5001".to_string(), false);
        let manager = ClusterManager::new(config).unwrap();

        // Voter should be Follower
        let meta1 = manager.get_node_metadata(1).await.unwrap();
        assert_eq!(meta1.role, NodeRole::Follower);
        assert!(!meta1.reachable);

        // Learner should be Learner
        let meta2 = manager.get_node_metadata(2).await.unwrap();
        assert_eq!(meta2.role, NodeRole::Learner);
        assert!(!meta2.reachable);
    }
}
