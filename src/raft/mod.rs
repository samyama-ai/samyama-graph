//! Raft consensus implementation for high availability
//!
//! Implements Phase 4 requirements:
//! - REQ-HA-001: Raft consensus protocol
//! - REQ-HA-002: Leader election and failover
//! - REQ-HA-003: Log replication across nodes
//! - REQ-HA-004: Cluster membership management

pub mod node;
pub mod network;
pub mod state_machine;
pub mod storage;
pub mod cluster;

pub use node::{NodeId, RaftNode};
pub use network::RaftNetwork;
pub use state_machine::{GraphStateMachine, Request, Response};
pub use storage::RaftStorage;
pub use cluster::{ClusterConfig, ClusterManager};

use openraft::Config;
// Arc removed - was unused import causing compiler warning
use thiserror::Error;

/// Raft node identifier type
pub type RaftNodeId = u64;

/// Raft errors
#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Raft error: {0}")]
    Raft(String),

    #[error("Not leader: current leader is {leader:?}")]
    NotLeader { leader: Option<RaftNodeId> },

    #[error("Network error: {0}")]
    Network(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Cluster error: {0}")]
    Cluster(String),
}

pub type RaftResult<T> = Result<T, RaftError>;

/// Create default Raft configuration
pub fn default_raft_config() -> Config {
    Config {
        heartbeat_interval: 500,
        election_timeout_min: 1500,
        election_timeout_max: 3000,
        max_payload_entries: 300,
        replication_lag_threshold: 1000,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_config() {
        let config = default_raft_config();
        assert_eq!(config.heartbeat_interval, 500);
        assert_eq!(config.election_timeout_min, 1500);
    }
}
