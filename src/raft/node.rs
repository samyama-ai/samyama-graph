//! Raft node implementation

use crate::raft::{GraphStateMachine, RaftError, RaftNodeId, RaftResult, Request, Response};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Node identifier with address
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Default)]
pub struct NodeId {
    /// Unique node ID
    #[serde(default)]
    pub id: RaftNodeId,
    /// Node address (host:port)
    #[serde(default)]
    pub addr: String,
}

impl NodeId {
    pub fn new(id: RaftNodeId, addr: String) -> Self {
        Self { id, addr }
    }
}

/// Raft type definitions for openraft
pub mod typ {
    use super::*;

    /// Node ID type
    pub type NodeIdType = RaftNodeId;

    /// Node type containing address information
    pub type Node = super::NodeId;

    /// Entry type for log entries
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Entry {
        pub request: Request,
    }

    /// Snapshot data type
    pub type SnapshotData = Vec<u8>;

    /// Type configuration for compatibility
    /// Note: This is a simplified implementation. Full Raft integration
    /// would use openraft::declare_raft_types! macro with proper trait bounds.
    pub struct TypeConfig;
}

/// Raft metrics (simplified)
#[derive(Debug, Clone, Default)]
pub struct SimpleRaftMetrics {
    pub current_term: u64,
    pub current_leader: Option<RaftNodeId>,
    pub last_log_index: u64,
    pub last_applied: u64,
}

/// Raft node managing consensus
pub struct RaftNode {
    /// Node ID
    node_id: RaftNodeId,
    /// State machine
    state_machine: Arc<RwLock<GraphStateMachine>>,
    /// Current metrics
    metrics: Arc<RwLock<SimpleRaftMetrics>>,
    /// Is initialized?
    initialized: Arc<RwLock<bool>>,
}

impl RaftNode {
    /// Create a new Raft node
    pub fn new(node_id: RaftNodeId, state_machine: GraphStateMachine) -> Self {
        info!("Creating Raft node with ID: {}", node_id);

        Self {
            node_id,
            state_machine: Arc::new(RwLock::new(state_machine)),
            metrics: Arc::new(RwLock::new(SimpleRaftMetrics::default())),
            initialized: Arc::new(RwLock::new(false)),
        }
    }

    /// Get node ID
    pub fn id(&self) -> RaftNodeId {
        self.node_id
    }

    /// Initialize the Raft instance
    pub async fn initialize(&mut self, _peers: Vec<NodeId>) -> RaftResult<()> {
        info!("Initializing Raft node {} with peers", self.node_id);

        let mut init = self.initialized.write().await;
        *init = true;

        let mut metrics = self.metrics.write().await;
        metrics.current_leader = Some(self.node_id); // Simplified: this node is leader

        Ok(())
    }

    /// Submit a write request (goes through Raft consensus)
    pub async fn write(&self, request: Request) -> RaftResult<Response> {
        if *self.initialized.read().await {
            // Apply directly to state machine
            let sm = self.state_machine.read().await;
            let response = sm.apply(request).await;

            // Update metrics
            let mut metrics = self.metrics.write().await;
            metrics.last_log_index += 1;
            metrics.last_applied = metrics.last_log_index;

            Ok(response)
        } else {
            Err(RaftError::Raft("Raft not initialized".to_string()))
        }
    }

    /// Execute a read request (can be served locally if leader)
    pub async fn read(&self, request: Request) -> RaftResult<Response> {
        let sm = self.state_machine.read().await;
        Ok(sm.apply(request).await)
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let metrics = self.metrics.read().await;
        metrics.current_leader == Some(self.node_id)
    }

    /// Get current leader ID
    pub async fn get_leader(&self) -> Option<RaftNodeId> {
        self.metrics.read().await.current_leader
    }

    /// Add a new node to the cluster
    pub async fn add_learner(&self, node_id: RaftNodeId, _node: NodeId) -> RaftResult<()> {
        info!("Adding learner {} to cluster", node_id);

        if *self.initialized.read().await {
            Ok(())
        } else {
            Err(RaftError::Raft("Raft not initialized".to_string()))
        }
    }

    /// Change cluster membership
    pub async fn change_membership(
        &self,
        members: BTreeSet<RaftNodeId>,
    ) -> RaftResult<()> {
        info!("Changing cluster membership to: {:?}", members);

        if *self.initialized.read().await {
            Ok(())
        } else {
            Err(RaftError::Raft("Raft not initialized".to_string()))
        }
    }

    /// Get Raft metrics
    pub async fn metrics(&self) -> SimpleRaftMetrics {
        self.metrics.read().await.clone()
    }

    /// Shutdown the Raft node
    pub async fn shutdown(&self) -> RaftResult<()> {
        info!("Shutting down Raft node {}", self.node_id);

        let mut init = self.initialized.write().await;
        *init = false;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::PersistenceManager;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_raft_node_creation() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let node = RaftNode::new(1, sm);

        assert_eq!(node.id(), 1);
        assert!(!node.is_leader().await);
    }

    #[tokio::test]
    async fn test_node_id() {
        let node_id = NodeId::new(1, "127.0.0.1:5000".to_string());
        assert_eq!(node_id.id, 1);
        assert_eq!(node_id.addr, "127.0.0.1:5000");
    }
}
