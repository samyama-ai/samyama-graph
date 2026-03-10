//! # Raft Consensus for High Availability
//!
//! ## What is Raft?
//!
//! Raft (Ongaro & Ousterhout, 2014) is a distributed consensus algorithm that ensures
//! all nodes in a cluster agree on the same sequence of operations, even when some nodes
//! fail or network partitions occur. It was designed to be **understandable** — in contrast
//! to Paxos, which is mathematically equivalent but notoriously difficult to implement
//! correctly.
//!
//! ## Key concepts
//!
//! - **Leader Election**: at any time, one node is the leader and all others are followers.
//!   If the leader fails, followers detect the timeout and hold an election. A candidate
//!   needs votes from a majority (quorum) to become the new leader.
//! - **Log Replication**: the leader receives all write requests, appends them to its log,
//!   and replicates log entries to followers. An entry is "committed" once a majority of
//!   nodes have acknowledged it — committed entries are never lost.
//! - **Safety**: Raft guarantees that if a log entry is committed, it will be present in
//!   the logs of all future leaders. This is enforced by election restrictions (candidates
//!   must have all committed entries to win).
//!
//! ## Terms
//!
//! Terms are Raft's logical clock — monotonically increasing integers that represent
//! leader epochs. Each term has at most one leader. When a node sees a higher term, it
//! knows its information is stale and updates. Terms prevent "split brain" scenarios
//! where two nodes both think they are leader.
//!
//! ## Why Raft over Paxos?
//!
//! Paxos solves the same problem but is presented as a monolithic protocol. Raft
//! decomposes consensus into three sub-problems (leader election, log replication, safety)
//! that can be understood and implemented independently. This decomposition has made Raft
//! the dominant choice for new distributed systems (etcd, CockroachDB, TiKV).
//!
//! ## In Samyama
//!
//! All write operations (CREATE, SET, DELETE, MERGE) go through the Raft leader, which
//! replicates them to followers before committing. Reads can go to any node with relaxed
//! consistency (may read slightly stale data) or only to the leader for strong consistency.
//! This module uses the `openraft` crate, a Rust implementation of the Raft protocol.

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
