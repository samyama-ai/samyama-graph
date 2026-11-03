//! Raft network layer for inter-node communication

use crate::raft::{RaftError, RaftNodeId, RaftResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Network message types between Raft nodes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Append entries (heartbeat or log replication)
    AppendEntries {
        /// Leader's term
        term: u64,
        /// Leader's node ID
        leader_id: RaftNodeId,
        /// Index of log entry preceding new ones
        prev_log_index: u64,
        /// Term of prev_log_index entry
        prev_log_term: u64,
        /// Log entries to replicate
        entries: Vec<Vec<u8>>,
        /// Leader's commit index
        leader_commit: u64,
    },
    /// Append entries response
    AppendEntriesResponse {
        /// Current term for leader to update
        term: u64,
        /// True if follower contained matching entry
        success: bool,
        /// Last matched log index
        match_index: Option<u64>,
    },
    /// Request vote for leader election
    RequestVote {
        /// Candidate's term
        term: u64,
        /// Candidate requesting vote
        candidate_id: RaftNodeId,
        /// Index of candidate's last log entry
        last_log_index: u64,
        /// Term of candidate's last log entry
        last_log_term: u64,
    },
    /// Vote response
    VoteResponse {
        /// Current term for candidate to update
        term: u64,
        /// True if vote granted
        vote_granted: bool,
    },
    /// Install snapshot
    InstallSnapshot {
        /// Leader's term
        term: u64,
        /// Leader's node ID
        leader_id: RaftNodeId,
        /// Last log index included in snapshot
        last_included_index: u64,
        /// Last log term included in snapshot
        last_included_term: u64,
        /// Snapshot data
        data: Vec<u8>,
    },
    /// Snapshot response
    SnapshotResponse {
        /// Current term for leader to update
        term: u64,
    },
}

/// Network address for a node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddress {
    /// Hostname or IP address
    pub host: String,
    /// Port number
    pub port: u16,
}

impl NodeAddress {
    /// Create a new node address
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }

    /// Convert to host:port string format
    pub fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Raft network manager
pub struct RaftNetwork {
    /// Node ID (retained for future node identification in network operations)
    #[allow(dead_code)]
    node_id: RaftNodeId,
    /// Known peer addresses
    peers: Arc<RwLock<HashMap<RaftNodeId, NodeAddress>>>,
}

impl RaftNetwork {
    /// Create a new Raft network
    pub fn new(node_id: RaftNodeId) -> Self {
        info!("Creating Raft network for node {}", node_id);

        Self {
            node_id,
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a peer to the network
    pub async fn add_peer(&self, peer_id: RaftNodeId, address: NodeAddress) {
        info!("Adding peer {} at {}", peer_id, address.to_string());

        let mut peers = self.peers.write().await;
        peers.insert(peer_id, address);
    }

    /// Remove a peer from the network
    pub async fn remove_peer(&self, peer_id: RaftNodeId) {
        info!("Removing peer {}", peer_id);

        let mut peers = self.peers.write().await;
        peers.remove(&peer_id);
    }

    /// Send a message to a peer
    pub async fn send(&self, target: RaftNodeId, message: RaftMessage) -> RaftResult<RaftMessage> {
        let peers = self.peers.read().await;

        if let Some(address) = peers.get(&target) {
            debug!(
                "Sending message to node {} at {}",
                target,
                address.to_string()
            );

            // In production, this would:
            // 1. Serialize the message
            // 2. Send it via TCP/HTTP to the peer
            // 3. Wait for response
            // 4. Deserialize and return

            // For now, simulate successful communication
            match message {
                RaftMessage::AppendEntries { term, .. } => Ok(RaftMessage::AppendEntriesResponse {
                    term,
                    success: true,
                    match_index: Some(0),
                }),
                RaftMessage::RequestVote { term, .. } => Ok(RaftMessage::VoteResponse {
                    term,
                    vote_granted: true,
                }),
                RaftMessage::InstallSnapshot { term, .. } => {
                    Ok(RaftMessage::SnapshotResponse { term })
                }
                _ => Err(RaftError::Network("Unexpected message type".to_string())),
            }
        } else {
            error!("Peer {} not found in network", target);
            Err(RaftError::Network(format!("Peer {} not found", target)))
        }
    }

    /// Broadcast a message to all peers
    pub async fn broadcast(&self, message: RaftMessage) -> Vec<RaftResult<RaftMessage>> {
        let peers = self.peers.read().await;
        let peer_ids: Vec<RaftNodeId> = peers.keys().copied().collect();
        drop(peers);

        let mut responses = Vec::new();

        for peer_id in peer_ids {
            let response = self.send(peer_id, message.clone()).await;
            responses.push(response);
        }

        responses
    }

    /// Get list of known peers
    pub async fn get_peers(&self) -> Vec<RaftNodeId> {
        self.peers.read().await.keys().copied().collect()
    }

    /// Check if a peer is reachable
    pub async fn is_reachable(&self, peer_id: RaftNodeId) -> bool {
        let peers = self.peers.read().await;
        peers.contains_key(&peer_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_network_creation() {
        let network = RaftNetwork::new(1);
        assert_eq!(network.node_id, 1);
    }

    #[tokio::test]
    async fn test_add_remove_peer() {
        let network = RaftNetwork::new(1);

        let addr = NodeAddress::new("127.0.0.1".to_string(), 5000);
        network.add_peer(2, addr).await;

        assert!(network.is_reachable(2).await);

        network.remove_peer(2).await;
        assert!(!network.is_reachable(2).await);
    }

    #[tokio::test]
    async fn test_send_message() {
        let network = RaftNetwork::new(1);

        let addr = NodeAddress::new("127.0.0.1".to_string(), 5000);
        network.add_peer(2, addr).await;

        let message = RaftMessage::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = network.send(2, message).await;
        assert!(response.is_ok());
    }

    // ========== Additional Network Coverage Tests ==========

    #[test]
    fn test_node_address_new() {
        let addr = NodeAddress::new("10.0.0.1".to_string(), 8080);
        assert_eq!(addr.host, "10.0.0.1");
        assert_eq!(addr.port, 8080);
    }

    #[test]
    fn test_node_address_to_string() {
        let addr = NodeAddress::new("localhost".to_string(), 6379);
        assert_eq!(addr.to_string(), "localhost:6379");
    }

    #[test]
    fn test_node_address_serialization() {
        let addr = NodeAddress::new("192.168.1.1".to_string(), 9000);
        let json = serde_json::to_string(&addr).unwrap();
        let deserialized: NodeAddress = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.host, "192.168.1.1");
        assert_eq!(deserialized.port, 9000);
    }

    #[test]
    fn test_raft_message_append_entries_serialization() {
        let msg = RaftMessage::AppendEntries {
            term: 5,
            leader_id: 1,
            prev_log_index: 10,
            prev_log_term: 4,
            entries: vec![vec![1, 2, 3], vec![4, 5, 6]],
            leader_commit: 9,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_str(&json).unwrap();
        if let RaftMessage::AppendEntries {
            term,
            leader_id,
            entries,
            ..
        } = deserialized
        {
            assert_eq!(term, 5);
            assert_eq!(leader_id, 1);
            assert_eq!(entries.len(), 2);
        } else {
            panic!("Expected AppendEntries");
        }
    }

    #[test]
    fn test_raft_message_append_entries_response_serialization() {
        let msg = RaftMessage::AppendEntriesResponse {
            term: 5,
            success: true,
            match_index: Some(10),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_str(&json).unwrap();
        if let RaftMessage::AppendEntriesResponse {
            term,
            success,
            match_index,
        } = deserialized
        {
            assert_eq!(term, 5);
            assert!(success);
            assert_eq!(match_index, Some(10));
        } else {
            panic!("Expected AppendEntriesResponse");
        }
    }

    #[test]
    fn test_raft_message_request_vote_serialization() {
        let msg = RaftMessage::RequestVote {
            term: 3,
            candidate_id: 2,
            last_log_index: 5,
            last_log_term: 2,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_str(&json).unwrap();
        if let RaftMessage::RequestVote {
            term, candidate_id, ..
        } = deserialized
        {
            assert_eq!(term, 3);
            assert_eq!(candidate_id, 2);
        } else {
            panic!("Expected RequestVote");
        }
    }

    #[test]
    fn test_raft_message_vote_response_serialization() {
        let msg = RaftMessage::VoteResponse {
            term: 3,
            vote_granted: false,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_str(&json).unwrap();
        if let RaftMessage::VoteResponse { term, vote_granted } = deserialized {
            assert_eq!(term, 3);
            assert!(!vote_granted);
        } else {
            panic!("Expected VoteResponse");
        }
    }

    #[test]
    fn test_raft_message_install_snapshot_serialization() {
        let msg = RaftMessage::InstallSnapshot {
            term: 7,
            leader_id: 1,
            last_included_index: 100,
            last_included_term: 6,
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_str(&json).unwrap();
        if let RaftMessage::InstallSnapshot {
            term,
            leader_id,
            data,
            ..
        } = deserialized
        {
            assert_eq!(term, 7);
            assert_eq!(leader_id, 1);
            assert_eq!(data, vec![0xDE, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("Expected InstallSnapshot");
        }
    }

    #[test]
    fn test_raft_message_snapshot_response_serialization() {
        let msg = RaftMessage::SnapshotResponse { term: 7 };
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_str(&json).unwrap();
        if let RaftMessage::SnapshotResponse { term } = deserialized {
            assert_eq!(term, 7);
        } else {
            panic!("Expected SnapshotResponse");
        }
    }

    #[tokio::test]
    async fn test_send_request_vote() {
        let network = RaftNetwork::new(1);
        let addr = NodeAddress::new("127.0.0.1".to_string(), 5000);
        network.add_peer(2, addr).await;

        let message = RaftMessage::RequestVote {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        let response = network.send(2, message).await.unwrap();
        if let RaftMessage::VoteResponse { term, vote_granted } = response {
            assert_eq!(term, 1);
            assert!(vote_granted);
        } else {
            panic!("Expected VoteResponse");
        }
    }

    #[tokio::test]
    async fn test_send_install_snapshot() {
        let network = RaftNetwork::new(1);
        let addr = NodeAddress::new("127.0.0.1".to_string(), 5000);
        network.add_peer(2, addr).await;

        let message = RaftMessage::InstallSnapshot {
            term: 3,
            leader_id: 1,
            last_included_index: 50,
            last_included_term: 2,
            data: vec![1, 2, 3],
        };

        let response = network.send(2, message).await.unwrap();
        if let RaftMessage::SnapshotResponse { term } = response {
            assert_eq!(term, 3);
        } else {
            panic!("Expected SnapshotResponse");
        }
    }

    #[tokio::test]
    async fn test_send_unexpected_message_type() {
        let network = RaftNetwork::new(1);
        let addr = NodeAddress::new("127.0.0.1".to_string(), 5000);
        network.add_peer(2, addr).await;

        // Sending a response type (not a request type) should return error
        let message = RaftMessage::AppendEntriesResponse {
            term: 1,
            success: true,
            match_index: Some(0),
        };

        let result = network.send(2, message).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_to_unknown_peer() {
        let network = RaftNetwork::new(1);

        let message = RaftMessage::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let result = network.send(999, message).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_broadcast() {
        let network = RaftNetwork::new(1);
        network
            .add_peer(2, NodeAddress::new("127.0.0.1".to_string(), 5000))
            .await;
        network
            .add_peer(3, NodeAddress::new("127.0.0.1".to_string(), 5001))
            .await;
        network
            .add_peer(4, NodeAddress::new("127.0.0.1".to_string(), 5002))
            .await;

        let message = RaftMessage::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let responses = network.broadcast(message).await;
        assert_eq!(responses.len(), 3);
        for response in &responses {
            assert!(response.is_ok());
        }
    }

    #[tokio::test]
    async fn test_broadcast_empty_peers() {
        let network = RaftNetwork::new(1);

        let message = RaftMessage::AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let responses = network.broadcast(message).await;
        assert!(responses.is_empty());
    }

    #[tokio::test]
    async fn test_get_peers() {
        let network = RaftNetwork::new(1);
        assert!(network.get_peers().await.is_empty());

        network
            .add_peer(2, NodeAddress::new("127.0.0.1".to_string(), 5000))
            .await;
        network
            .add_peer(3, NodeAddress::new("127.0.0.1".to_string(), 5001))
            .await;

        let peers = network.get_peers().await;
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&2));
        assert!(peers.contains(&3));
    }

    #[tokio::test]
    async fn test_is_reachable() {
        let network = RaftNetwork::new(1);

        assert!(!network.is_reachable(2).await);

        network
            .add_peer(2, NodeAddress::new("127.0.0.1".to_string(), 5000))
            .await;
        assert!(network.is_reachable(2).await);

        network.remove_peer(2).await;
        assert!(!network.is_reachable(2).await);
    }
}
