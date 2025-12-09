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
        info!(
            "Adding peer {} at {}",
            peer_id,
            address.to_string()
        );

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
    pub async fn send(
        &self,
        target: RaftNodeId,
        message: RaftMessage,
    ) -> RaftResult<RaftMessage> {
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
                RaftMessage::AppendEntries { term, .. } => {
                    Ok(RaftMessage::AppendEntriesResponse {
                        term,
                        success: true,
                        match_index: Some(0),
                    })
                }
                RaftMessage::RequestVote { term, .. } => {
                    Ok(RaftMessage::VoteResponse {
                        term,
                        vote_granted: true,
                    })
                }
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
}
