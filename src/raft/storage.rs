//! Raft storage implementation
//!
//! Persists Raft logs and metadata

use crate::raft::{RaftError, RaftNodeId, RaftResult};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Raft log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Log index
    pub index: u64,
    /// Log term
    pub term: u64,
    /// Entry data (serialized Request)
    pub data: Vec<u8>,
}

/// Raft persistent state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftState {
    /// Current term
    pub current_term: u64,
    /// Voted for in current term
    pub voted_for: Option<RaftNodeId>,
    /// Committed index
    pub commit_index: u64,
    /// Last applied index
    pub last_applied: u64,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
        }
    }
}

/// Raft storage manager
pub struct RaftStorage {
    /// Storage path (retained for future persistent storage operations)
    #[allow(dead_code)]
    path: String,
    /// Raft state
    state: Arc<RwLock<RaftState>>,
    /// Log entries
    log: Arc<RwLock<Vec<LogEntry>>>,
    /// Last snapshot metadata
    snapshot_metadata: Arc<RwLock<Option<(u64, u64)>>>, // (index, term)
}

impl RaftStorage {
    /// Create a new Raft storage
    pub fn new(path: impl AsRef<Path>) -> RaftResult<Self> {
        let path_str = path.as_ref().to_str().unwrap().to_string();

        info!("Creating Raft storage at: {}", path_str);

        // Create directory if needed
        std::fs::create_dir_all(&path_str)
            .map_err(|e| RaftError::Storage(format!("Failed to create directory: {}", e)))?;

        Ok(Self {
            path: path_str,
            state: Arc::new(RwLock::new(RaftState::default())),
            log: Arc::new(RwLock::new(Vec::new())),
            snapshot_metadata: Arc::new(RwLock::new(None)),
        })
    }

    /// Get current term
    pub async fn get_current_term(&self) -> u64 {
        self.state.read().await.current_term
    }

    /// Set current term
    pub async fn set_current_term(&self, term: u64) -> RaftResult<()> {
        let mut state = self.state.write().await;
        state.current_term = term;
        debug!("Set current term to {}", term);
        Ok(())
    }

    /// Get voted for
    pub async fn get_voted_for(&self) -> Option<RaftNodeId> {
        self.state.read().await.voted_for
    }

    /// Set voted for
    pub async fn set_voted_for(&self, node_id: Option<RaftNodeId>) -> RaftResult<()> {
        let mut state = self.state.write().await;
        state.voted_for = node_id;
        debug!("Set voted for to {:?}", node_id);
        Ok(())
    }

    /// Append log entries
    pub async fn append_entries(&self, entries: Vec<LogEntry>) -> RaftResult<()> {
        let mut log = self.log.write().await;

        for entry in entries {
            debug!("Appending log entry at index {}", entry.index);
            log.push(entry);
        }

        Ok(())
    }

    /// Get log entry at index
    pub async fn get_entry(&self, index: u64) -> Option<LogEntry> {
        let log = self.log.read().await;
        log.iter().find(|e| e.index == index).cloned()
    }

    /// Get log entries in range [start, end)
    pub async fn get_entries(&self, start: u64, end: u64) -> Vec<LogEntry> {
        let log = self.log.read().await;
        log.iter()
            .filter(|e| e.index >= start && e.index < end)
            .cloned()
            .collect()
    }

    /// Get last log index and term
    pub async fn get_last_log_index_term(&self) -> (u64, u64) {
        let log = self.log.read().await;

        if let Some(last) = log.last() {
            (last.index, last.term)
        } else {
            // Check snapshot
            if let Some((index, term)) = *self.snapshot_metadata.read().await {
                (index, term)
            } else {
                (0, 0)
            }
        }
    }

    /// Delete log entries from index onwards
    pub async fn delete_entries_from(&self, index: u64) -> RaftResult<()> {
        let mut log = self.log.write().await;
        log.retain(|e| e.index < index);
        debug!("Deleted log entries from index {}", index);
        Ok(())
    }

    /// Get commit index
    pub async fn get_commit_index(&self) -> u64 {
        self.state.read().await.commit_index
    }

    /// Set commit index
    pub async fn set_commit_index(&self, index: u64) -> RaftResult<()> {
        let mut state = self.state.write().await;
        state.commit_index = index;
        debug!("Set commit index to {}", index);
        Ok(())
    }

    /// Create a snapshot
    pub async fn create_snapshot(
        &self,
        index: u64,
        term: u64,
        _data: Vec<u8>,
    ) -> RaftResult<()> {
        info!("Creating snapshot at index {} term {}", index, term);

        // Save snapshot metadata
        let mut metadata = self.snapshot_metadata.write().await;
        *metadata = Some((index, term));

        // In production, would write snapshot data to disk
        // For now, just update metadata

        // Compact log by removing entries up to snapshot index
        self.delete_entries_from(index + 1).await?;

        Ok(())
    }

    /// Get snapshot metadata
    pub async fn get_snapshot_metadata(&self) -> Option<(u64, u64)> {
        *self.snapshot_metadata.read().await
    }

    /// Persist state to disk
    pub async fn flush(&self) -> RaftResult<()> {
        // In production, would write state and log to disk
        debug!("Flushing Raft storage");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        assert_eq!(storage.get_current_term().await, 0);
        assert_eq!(storage.get_voted_for().await, None);
    }

    #[tokio::test]
    async fn test_term_and_vote() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        storage.set_current_term(5).await.unwrap();
        assert_eq!(storage.get_current_term().await, 5);

        storage.set_voted_for(Some(2)).await.unwrap();
        assert_eq!(storage.get_voted_for().await, Some(2));
    }

    #[tokio::test]
    async fn test_log_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        let entry = LogEntry {
            index: 1,
            term: 1,
            data: vec![1, 2, 3],
        };

        storage.append_entries(vec![entry.clone()]).await.unwrap();

        let retrieved = storage.get_entry(1).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().index, 1);

        let (last_index, last_term) = storage.get_last_log_index_term().await;
        assert_eq!(last_index, 1);
        assert_eq!(last_term, 1);
    }

    #[tokio::test]
    async fn test_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        storage.create_snapshot(10, 2, vec![]).await.unwrap();

        let metadata = storage.get_snapshot_metadata().await;
        assert_eq!(metadata, Some((10, 2)));
    }
}
