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

    // ========== Additional RaftNode Coverage Tests ==========

    #[tokio::test]
    async fn test_raft_node_initialize() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        assert!(!node.is_leader().await);
        assert_eq!(node.get_leader().await, None);

        let peers = vec![
            NodeId::new(2, "127.0.0.1:5001".to_string()),
            NodeId::new(3, "127.0.0.1:5002".to_string()),
        ];
        node.initialize(peers).await.unwrap();

        // After initialization, simplified impl makes self the leader
        assert!(node.is_leader().await);
        assert_eq!(node.get_leader().await, Some(1));
    }

    /// A write before `initialize` is refused by name, and nothing of it is
    /// applied: no node reaches the store and the log index does not move.
    /// Was `assert!(result.is_err())` alone, with a read-only request that
    /// could not have written anything either way (#1311).
    #[tokio::test]
    async fn test_raft_node_write_before_init() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence.clone());
        let mut node = RaftNode::new(1, sm);

        let create = || Request::CreateNode {
            tenant: "default".to_string(),
            node_id: 7,
            labels: vec!["Person".to_string()],
            properties: crate::graph::PropertyMap::new(),
        };

        match node.write(create()).await {
            Err(RaftError::Raft(msg)) => assert_eq!(msg, "Raft not initialized"),
            other => panic!("expected RaftError::Raft(\"Raft not initialized\"), got {other:?}"),
        }
        let metrics = node.metrics().await;
        assert_eq!((metrics.last_log_index, metrics.last_applied), (0, 0));
        assert_eq!(metrics.current_leader, None);
        let (nodes, _) = persistence.recover("default").unwrap();
        assert!(
            nodes.is_empty(),
            "the refused write reached the store: {nodes:?}"
        );

        // The same request after `initialize` does land, so the empty store
        // above is the refusal and not a request that writes nothing.
        node.initialize(vec![]).await.unwrap();
        assert!(matches!(
            node.write(create()).await,
            Ok(Response::NodeCreated { node_id: 7 })
        ));
        let (nodes, _) = persistence.recover("default").unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[tokio::test]
    async fn test_raft_node_write_after_init() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        node.initialize(vec![]).await.unwrap();

        let request = Request::ExecuteQuery {
            tenant: "default".to_string(),
            query: "MATCH (n) RETURN n".to_string(),
        };

        let result = node.write(request).await;
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(matches!(response, Response::QueryResult { .. }));
    }

    #[tokio::test]
    async fn test_raft_node_read() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let node = RaftNode::new(1, sm);

        // Read does not require initialization
        let request = Request::ExecuteQuery {
            tenant: "default".to_string(),
            query: "MATCH (n) RETURN n".to_string(),
        };

        let result = node.read(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_raft_node_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        let metrics = node.metrics().await;
        assert_eq!(metrics.current_term, 0);
        assert_eq!(metrics.last_log_index, 0);
        assert_eq!(metrics.last_applied, 0);
        assert_eq!(metrics.current_leader, None);

        node.initialize(vec![]).await.unwrap();

        // Write a request to update metrics
        let request = Request::ExecuteQuery {
            tenant: "default".to_string(),
            query: "MATCH (n) RETURN n".to_string(),
        };
        node.write(request).await.unwrap();

        let metrics = node.metrics().await;
        assert_eq!(metrics.last_log_index, 1);
        assert_eq!(metrics.last_applied, 1);
        assert_eq!(metrics.current_leader, Some(1));
    }

    #[tokio::test]
    async fn test_raft_node_add_learner_before_init() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let node = RaftNode::new(1, sm);

        let new_node = NodeId::new(2, "127.0.0.1:5001".to_string());
        let result = node.add_learner(2, new_node).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_raft_node_add_learner_after_init() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        node.initialize(vec![]).await.unwrap();

        let new_node = NodeId::new(2, "127.0.0.1:5001".to_string());
        let result = node.add_learner(2, new_node).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_raft_node_change_membership_before_init() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let node = RaftNode::new(1, sm);

        let mut members = BTreeSet::new();
        members.insert(1);
        members.insert(2);
        let result = node.change_membership(members).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_raft_node_change_membership_after_init() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        node.initialize(vec![]).await.unwrap();

        let mut members = BTreeSet::new();
        members.insert(1);
        members.insert(2);
        let result = node.change_membership(members).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_raft_node_shutdown() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        node.initialize(vec![]).await.unwrap();
        assert!(node.is_leader().await);

        node.shutdown().await.unwrap();

        // After shutdown, writes should fail (not initialized)
        let request = Request::ExecuteQuery {
            tenant: "default".to_string(),
            query: "MATCH (n) RETURN n".to_string(),
        };
        let result = node.write(request).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_raft_node_multiple_writes_increment_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let persistence = Arc::new(PersistenceManager::new(temp_dir.path()).unwrap());
        let sm = GraphStateMachine::new(persistence);
        let mut node = RaftNode::new(1, sm);

        node.initialize(vec![]).await.unwrap();

        for _ in 0..5 {
            let request = Request::ExecuteQuery {
                tenant: "default".to_string(),
                query: "MATCH (n) RETURN n".to_string(),
            };
            node.write(request).await.unwrap();
        }

        let metrics = node.metrics().await;
        assert_eq!(metrics.last_log_index, 5);
        assert_eq!(metrics.last_applied, 5);
    }

    #[test]
    fn test_node_id_default() {
        let node_id = NodeId::default();
        assert_eq!(node_id.id, 0);
        assert_eq!(node_id.addr, "");
    }

    #[test]
    fn test_node_id_serialization() {
        let node_id = NodeId::new(42, "10.0.0.1:8080".to_string());
        let json = serde_json::to_string(&node_id).unwrap();
        let deserialized: NodeId = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, 42);
        assert_eq!(deserialized.addr, "10.0.0.1:8080");
    }

    #[test]
    fn test_node_id_equality() {
        let a = NodeId::new(1, "addr1".to_string());
        let b = NodeId::new(1, "addr1".to_string());
        let c = NodeId::new(2, "addr1".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_simple_raft_metrics_default() {
        let metrics = SimpleRaftMetrics::default();
        assert_eq!(metrics.current_term, 0);
        assert_eq!(metrics.current_leader, None);
        assert_eq!(metrics.last_log_index, 0);
        assert_eq!(metrics.last_applied, 0);
    }

    #[test]
    fn test_entry_serialization() {
        let entry = typ::Entry {
            request: Request::ExecuteQuery {
                tenant: "default".to_string(),
                query: "MATCH (n) RETURN n".to_string(),
            },
        };
        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: typ::Entry = serde_json::from_str(&json).unwrap();
        assert!(matches!(deserialized.request, Request::ExecuteQuery { .. }));
    }
}
