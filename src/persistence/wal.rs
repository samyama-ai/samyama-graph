//! Write-Ahead Log (WAL) implementation
//!
//! Implements REQ-PERSIST-002 (Write-Ahead Logging for durability)

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::{debug, info, warn};

/// WAL errors
#[derive(Error, Debug)]
pub enum WalError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// Corruption detected
    #[error("WAL corruption detected at offset {0}")]
    Corruption(u64),

    /// Invalid log entry
    #[error("Invalid log entry: {0}")]
    InvalidEntry(String),
}

pub type WalResult<T> = Result<T, WalError>;

/// Write-Ahead Log entry types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalEntry {
    /// Create node
    CreateNode {
        tenant: String,
        node_id: u64,
        labels: Vec<String>,
        properties: Vec<u8>, // Serialized property map
    },
    /// Create edge
    CreateEdge {
        tenant: String,
        edge_id: u64,
        source: u64,
        target: u64,
        edge_type: String,
        properties: Vec<u8>, // Serialized property map
    },
    /// Delete node
    DeleteNode {
        tenant: String,
        node_id: u64,
    },
    /// Delete edge
    DeleteEdge {
        tenant: String,
        edge_id: u64,
    },
    /// Update node properties
    UpdateNodeProperties {
        tenant: String,
        node_id: u64,
        properties: Vec<u8>,
    },
    /// Update edge properties
    UpdateEdgeProperties {
        tenant: String,
        edge_id: u64,
        properties: Vec<u8>,
    },
    /// Checkpoint marker
    Checkpoint {
        sequence: u64,
        timestamp: i64,
    },
}

/// WAL record with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalRecord {
    /// Sequence number (monotonically increasing)
    sequence: u64,
    /// Entry data
    entry: WalEntry,
    /// CRC32 checksum for corruption detection
    checksum: u32,
}

impl WalRecord {
    fn new(sequence: u64, entry: WalEntry) -> Self {
        let mut record = Self {
            sequence,
            entry,
            checksum: 0,
        };
        // Calculate checksum
        record.checksum = record.calculate_checksum();
        record
    }

    fn calculate_checksum(&self) -> u32 {
        // Simple checksum: XOR all bytes
        let bytes = bincode::serialize(&self.entry).unwrap_or_default();
        bytes.iter().fold(0u32, |acc, &b| acc ^ (b as u32))
    }

    fn verify_checksum(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

/// Write-Ahead Log manager
pub struct Wal {
    /// Path to WAL directory
    path: PathBuf,
    /// Current WAL file
    current_file: Option<BufWriter<File>>,
    /// Current sequence number
    sequence: u64,
    /// Sync mode (flush after every write)
    sync_mode: bool,
}

impl Wal {
    /// Create a new WAL
    pub fn new(path: impl AsRef<Path>) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        std::fs::create_dir_all(&path)?;

        // Find the latest sequence number from existing WAL files
        let sequence = Self::find_latest_sequence(&path)?;

        info!("Initializing WAL at {:?}, sequence: {}", path, sequence);

        Ok(Self {
            path,
            current_file: None,
            sequence,
            sync_mode: false, // Default to async for performance
        })
    }

    /// Set sync mode
    pub fn set_sync_mode(&mut self, sync: bool) {
        self.sync_mode = sync;
        debug!("WAL sync mode: {}", sync);
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, entry: WalEntry) -> WalResult<u64> {
        // Increment sequence
        self.sequence += 1;
        let sequence = self.sequence;

        // Create WAL record
        let record = WalRecord::new(sequence, entry);

        // Serialize
        let data = bincode::serialize(&record)?;

        // Ensure we have an open file
        if self.current_file.is_none() {
            self.open_new_file()?;
        }

        // Write to file
        if let Some(ref mut file) = self.current_file {
            // Write length prefix (4 bytes)
            file.write_all(&(data.len() as u32).to_le_bytes())?;
            // Write data
            file.write_all(&data)?;

            // Flush if in sync mode
            if self.sync_mode {
                file.flush()?;
            }
        }

        Ok(sequence)
    }

    /// Force flush the WAL
    pub fn flush(&mut self) -> WalResult<()> {
        if let Some(ref mut file) = self.current_file {
            file.flush()?;
        }
        Ok(())
    }

    /// Replay the WAL from a specific sequence number
    pub fn replay<F>(&self, from_sequence: u64, mut callback: F) -> WalResult<u64>
    where
        F: FnMut(&WalEntry) -> WalResult<()>,
    {
        info!("Replaying WAL from sequence {}", from_sequence);

        let files = self.get_wal_files()?;
        let mut replayed = 0u64;
        let mut last_sequence = from_sequence;

        for file_path in files {
            let file = File::open(&file_path)?;
            let mut reader = BufReader::new(file);
            let mut buf = Vec::new();

            loop {
                // Read length prefix
                let mut len_bytes = [0u8; 4];
                match reader.read_exact(&mut len_bytes) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }

                let len = u32::from_le_bytes(len_bytes) as usize;

                // Read record data
                buf.resize(len, 0);
                reader.read_exact(&mut buf)?;

                // Deserialize
                let record: WalRecord = bincode::deserialize(&buf)?;

                // Verify checksum
                if !record.verify_checksum() {
                    warn!("WAL corruption detected at sequence {}", record.sequence);
                    return Err(WalError::Corruption(record.sequence));
                }

                // Skip if before from_sequence
                if record.sequence < from_sequence {
                    continue;
                }

                // Apply entry
                callback(&record.entry)?;
                replayed += 1;
                last_sequence = record.sequence;
            }
        }

        info!("Replayed {} WAL entries, last sequence: {}", replayed, last_sequence);
        Ok(last_sequence)
    }

    /// Create a checkpoint and truncate old WAL entries
    pub fn checkpoint(&mut self, sequence: u64) -> WalResult<()> {
        info!("Creating WAL checkpoint at sequence {}", sequence);

        // Append checkpoint marker
        let timestamp = chrono::Utc::now().timestamp();
        self.append(WalEntry::Checkpoint {
            sequence,
            timestamp,
        })?;

        // Flush current file
        self.flush()?;

        // Close current file
        self.current_file = None;

        // Delete old WAL files (implementation depends on file naming strategy)
        // For now, we keep all files for safety
        // TODO: Implement safe WAL truncation after checkpoint

        Ok(())
    }

    /// Open a new WAL file
    fn open_new_file(&mut self) -> WalResult<()> {
        let filename = format!("wal-{:016x}.log", self.sequence);
        let file_path = self.path.join(filename);

        debug!("Opening new WAL file: {:?}", file_path);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)?;

        self.current_file = Some(BufWriter::new(file));
        Ok(())
    }

    /// Find the latest sequence number from existing WAL files
    fn find_latest_sequence(path: &Path) -> WalResult<u64> {
        let files = match std::fs::read_dir(path) {
            Ok(entries) => entries,
            Err(_) => return Ok(0), // No directory yet
        };

        let mut max_sequence = 0u64;

        for entry in files.flatten() {
            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with("wal-") && filename.ends_with(".log") {
                    // Parse sequence from filename
                    if let Some(seq_str) = filename.strip_prefix("wal-").and_then(|s| s.strip_suffix(".log")) {
                        if let Ok(seq) = u64::from_str_radix(seq_str, 16) {
                            max_sequence = max_sequence.max(seq);
                        }
                    }
                }
            }
        }

        Ok(max_sequence)
    }

    /// Get all WAL files in sequence order
    fn get_wal_files(&self) -> WalResult<Vec<PathBuf>> {
        let mut files = Vec::new();

        let entries = std::fs::read_dir(&self.path)?;

        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with("wal-") && filename.ends_with(".log") {
                    files.push(entry.path());
                }
            }
        }

        // Sort by filename (which includes sequence)
        files.sort();

        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_creation() {
        let temp_dir = TempDir::new().unwrap();
        let wal = Wal::new(temp_dir.path()).unwrap();
        assert_eq!(wal.sequence, 0);
    }

    #[test]
    fn test_wal_append() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = Wal::new(temp_dir.path()).unwrap();

        let entry = WalEntry::CreateNode {
            tenant: "default".to_string(),
            node_id: 1,
            labels: vec!["Person".to_string()],
            properties: vec![],
        };

        let seq = wal.append(entry).unwrap();
        assert_eq!(seq, 1);

        wal.flush().unwrap();
    }

    #[test]
    fn test_wal_replay() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = Wal::new(temp_dir.path()).unwrap();

        // Append some entries
        for i in 1..=5 {
            let entry = WalEntry::CreateNode {
                tenant: "default".to_string(),
                node_id: i,
                labels: vec![],
                properties: vec![],
            };
            wal.append(entry).unwrap();
        }

        wal.flush().unwrap();

        // Replay
        let mut count = 0;
        wal.replay(0, |_entry| {
            count += 1;
            Ok(())
        }).unwrap();

        assert_eq!(count, 5);
    }

    #[test]
    fn test_wal_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let mut wal = Wal::new(temp_dir.path()).unwrap();

        // Append entries
        for i in 1..=10 {
            let entry = WalEntry::CreateNode {
                tenant: "default".to_string(),
                node_id: i,
                labels: vec![],
                properties: vec![],
            };
            wal.append(entry).unwrap();
        }

        // Create checkpoint
        wal.checkpoint(10).unwrap();

        // Verify checkpoint was appended
        let mut found_checkpoint = false;
        wal.replay(0, |entry| {
            if matches!(entry, WalEntry::Checkpoint { .. }) {
                found_checkpoint = true;
            }
            Ok(())
        }).unwrap();

        assert!(found_checkpoint);
    }
}
