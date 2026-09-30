//! # Write-Ahead Log (WAL)
//!
//! ## Core theory
//!
//! The fundamental insight behind WAL is that **sequential writes are fast** (especially
//! on SSDs and spinning disks), while random writes are slow. By appending each operation
//! to a log file before modifying the in-memory data structures, we get durability without
//! expensive random I/O. If the process crashes after writing to the log but before
//! updating the main data, we can reconstruct the correct state from the log.
//!
//! ## Recovery
//!
//! On startup, the WAL is scanned from the beginning (or from the last checkpoint).
//! Each entry is replayed against the in-memory graph to reconstruct state. Checkpoints
//! record a "safe point" — all data before the checkpoint is known to be persisted to
//! RocksDB, so the WAL can be truncated to prevent unbounded growth.
//!
//! ## Record format and checksums
//!
//! Every record on disk is a 4-byte little-endian length word followed by the
//! record body. There are two body formats, told apart by the top bit of the
//! length word (#1311):
//!
//! - **Format 1** (top bit set; everything this code writes). The low 31 bits
//!   are the body length. The body is
//!   `[format: u8 = 1][sequence: u64 LE][bincode(entry)][crc: u32 LE]`, and
//!   `crc` is CRC-32 (IEEE, via `crc32fast`) over every body byte before it —
//!   the format byte, the sequence number and the entry. The CRC is checked
//!   **before** the entry is decoded.
//! - **Legacy** (top bit clear; written before #1311). The body is
//!   `bincode(WalRecord { sequence, entry, checksum })`, where `checksum` is
//!   an XOR of the entry's bytes: it keeps only 8 bits, does not cover the
//!   sequence number, and cannot see the same bit flipped in two bytes. It is
//!   still verified, so old WALs replay exactly as they did.
//!
//! A single file may hold both: a process that restarts on an old WAL appends
//! format-1 records after the legacy ones.
//!
//! During replay a checksum mismatch **stops** the replay with
//! [`WalError::Corruption`] carrying the byte offset of the damaged record in
//! its file. Records before it have already been handed to the callback; the
//! damaged record and everything after it are not. It is not skipped: a record
//! written in full and then damaged means a damaged disk, and applying the
//! records after a hole could apply a write whose predecessor was lost. A
//! record that is *short* (the file ends inside it) is a torn tail from a
//! write that never finished, and replay stops there cleanly instead.
//!
//! ## Sequence numbers
//!
//! Every WAL entry gets a monotonically increasing sequence number. These provide a
//! total ordering of operations, which is essential for exactly-once replay during
//! recovery (skip entries already applied) and for coordinating with checkpoints.
//!
//! ## Sync modes
//!
//! There is a fundamental trade-off between durability and performance:
//! - **`fsync` after every write**: guarantees the data is on stable storage, but each
//!   fsync can take 1-10ms (limits throughput to ~100-1000 writes/sec)
//! - **Buffered writes**: the OS may cache writes in its page cache, risking loss of
//!   the most recent writes on crash, but achieving much higher throughput
//!
//! Most databases offer both modes and let users choose based on their requirements.

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::{debug, info, warn};

#[cfg(test)]
thread_local! {
    /// WAL mutex acquisitions through [`lock`] and records written by
    /// [`Wal::append`], for tests that pin how often a batch takes the lock (#1109).
    pub(crate) static WAL_LOCKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub(crate) static WAL_APPENDS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Take the WAL mutex. One place to acquire it, so a test can count how often
/// the write path does (#1109).
pub(crate) fn lock(wal: &std::sync::Mutex<Wal>) -> std::sync::MutexGuard<'_, Wal> {
    #[cfg(test)]
    WAL_LOCKS.with(|c| c.set(c.get() + 1));
    wal.lock().unwrap()
}

/// WAL errors
#[derive(Error, Debug)]
pub enum WalError {
    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// A record written in full failed its checksum. The value is the byte
    /// offset of the record's length word within its WAL file.
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
        /// MVCC version at which this update was made (0 = legacy entry without version)
        #[serde(default)]
        version: u64,
    },
    /// Update edge properties
    UpdateEdgeProperties {
        tenant: String,
        edge_id: u64,
        properties: Vec<u8>,
        /// MVCC version at which this update was made (0 = legacy entry without version)
        #[serde(default)]
        version: u64,
    },
    /// Checkpoint marker
    Checkpoint {
        sequence: u64,
        timestamp: i64,
    },
}

/// Top bit of the length word: set on format-1 records, clear on legacy ones.
const FORMAT_FLAG: u32 = 0x8000_0000;
/// The body-format byte of records written by this code.
const FORMAT_V1: u8 = 1;
/// Format byte + sequence number.
const V1_HEADER_LEN: usize = 1 + 8;
/// Trailing CRC-32.
const V1_CRC_LEN: usize = 4;

/// A record as written before #1311: read-only, kept so old WALs replay.
#[derive(Debug, Deserialize)]
struct LegacyWalRecord {
    sequence: u64,
    entry: WalEntry,
    /// XOR of the entry's bincode bytes -- not a CRC, despite what the docs
    /// used to say. Only the low 8 bits can ever be set.
    checksum: u32,
}

impl LegacyWalRecord {
    fn verify_checksum(&self) -> bool {
        let bytes = bincode::serialize(&self.entry).unwrap_or_default();
        self.checksum == bytes.iter().fold(0u32, |acc, &b| acc ^ (b as u32))
    }
}

/// Encode a format-1 record body (without the length word).
fn encode_v1(sequence: u64, entry: &WalEntry) -> WalResult<Vec<u8>> {
    let payload = bincode::serialize(entry)?;
    let mut body = Vec::with_capacity(V1_HEADER_LEN + payload.len() + V1_CRC_LEN);
    body.push(FORMAT_V1);
    body.extend_from_slice(&sequence.to_le_bytes());
    body.extend_from_slice(&payload);
    let crc = crc32fast::hash(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    Ok(body)
}

/// Why a record body could not be turned into an entry.
enum DecodeError {
    /// The checksum does not match: the bytes were damaged after being written.
    Checksum,
    /// Anything else (unknown format byte, undecodable entry).
    Other(WalError),
}

/// Decode a record body. `versioned` is the length word's top bit.
fn decode_body(versioned: bool, body: &[u8]) -> Result<(u64, WalEntry), DecodeError> {
    if !versioned {
        let record: LegacyWalRecord =
            bincode::deserialize(body).map_err(|e| DecodeError::Other(e.into()))?;
        if !record.verify_checksum() {
            return Err(DecodeError::Checksum);
        }
        return Ok((record.sequence, record.entry));
    }

    if body.len() < V1_HEADER_LEN + V1_CRC_LEN {
        // Too short to carry its own checksum: treat as damage, not as an
        // unknown format -- a well-formed writer never produces this.
        return Err(DecodeError::Checksum);
    }
    let (covered, crc_bytes) = body.split_at(body.len() - V1_CRC_LEN);
    let stored = u32::from_le_bytes(crc_bytes.try_into().expect("4 bytes"));
    if crc32fast::hash(covered) != stored {
        return Err(DecodeError::Checksum);
    }
    if covered[0] != FORMAT_V1 {
        return Err(DecodeError::Other(WalError::InvalidEntry(format!(
            "unknown WAL record format {}",
            covered[0]
        ))));
    }
    let sequence = u64::from_le_bytes(covered[1..V1_HEADER_LEN].try_into().expect("8 bytes"));
    let entry: WalEntry = bincode::deserialize(&covered[V1_HEADER_LEN..])
        .map_err(|e| DecodeError::Other(e.into()))?;
    Ok((sequence, entry))
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
            sync_mode: Self::sync_mode_from_env(),
        })
    }

    /// Whether an acknowledged write has been forced to the platter.
    ///
    /// `SAMYAMA_FSYNC=1` turns it on. **Off by default**, which is what the
    /// engine has always done, and the default is the honest one to keep: this
    /// is a change of what users can choose, not a change of what they get
    /// without asking. `docs/ACID_GUARANTEES.md` §4 states both costs.
    ///
    /// Read once, at construction. A durability level that could change under
    /// a running process would make "was this write durable?" unanswerable for
    /// any particular write.
    fn sync_mode_from_env() -> bool {
        matches!(
            std::env::var("SAMYAMA_FSYNC").unwrap_or_default().to_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }

    /// Set sync mode.
    ///
    /// Exists so a caller can override the environment — the benchmark that
    /// measures what fsync costs needs both modes in one process.
    pub fn set_sync_mode(&mut self, sync: bool) {
        self.sync_mode = sync;
        debug!("WAL sync mode: {}", sync);
    }

    /// Is an acknowledged write forced to the platter?
    pub fn sync_mode(&self) -> bool {
        self.sync_mode
    }

    /// Get current sequence number
    ///
    /// Returns the current WAL sequence number which increments with each write.
    /// This is needed by PersistenceManager::checkpoint() to record the actual
    /// checkpoint sequence instead of always using 0, which was causing misleading
    /// output in the banking demo (WAL checkpoint always showed sequence 0).
    pub fn current_sequence(&self) -> u64 {
        self.sequence
    }

    /// Append an entry to the WAL
    pub fn append(&mut self, entry: WalEntry) -> WalResult<u64> {
        #[cfg(test)]
        WAL_APPENDS.with(|c| c.set(c.get() + 1));
        // Increment sequence
        self.sequence += 1;
        let sequence = self.sequence;

        // Encode as a format-1 record (see the module docs).
        let data = encode_v1(sequence, &entry)?;
        if data.len() as u64 >= FORMAT_FLAG as u64 {
            self.sequence -= 1;
            return Err(WalError::InvalidEntry(format!(
                "WAL record of {} bytes exceeds the 2 GiB record limit",
                data.len()
            )));
        }

        // Ensure we have an open file
        if self.current_file.is_none() {
            self.open_new_file()?;
        }

        // Write to file
        if let Some(ref mut file) = self.current_file {
            // Write length prefix (4 bytes)
            file.write_all(&(data.len() as u32 | FORMAT_FLAG).to_le_bytes())?;
            // Write data
            file.write_all(&data)?;

            // Sync if asked. `flush()` alone moves bytes out of the `BufWriter`
            // into the OS page cache and is **not** a durability barrier — a
            // host crash or power loss still loses them. `sync_data()` is the
            // barrier, and it is what "durable" has to mean for a write that
            // has been acknowledged (#1309).
            //
            // `sync_data` rather than `sync_all`: the file's length and
            // contents are what a replay needs, and skipping the metadata
            // flush is the cheaper of the two barriers.
            if self.sync_mode {
                file.flush()?;
                file.get_ref().sync_data()?;
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
            // Byte offset of the current record's length word in this file.
            let mut offset = 0u64;

            loop {
                // Read length prefix
                let mut len_bytes = [0u8; 4];
                match reader.read_exact(&mut len_bytes) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                }

                let word = u32::from_le_bytes(len_bytes);
                let versioned = word & FORMAT_FLAG != 0;
                let len = (word & !FORMAT_FLAG) as usize;

                // Read record data.
                //
                // A record whose body is short is a **torn tail**: the process
                // died between writing the length prefix and writing the bytes
                // it promised. That is the ordinary shape of a crash, and the
                // record was never acknowledged to anyone, so dropping it is
                // correct.
                //
                // Propagating the error here was not. It abandoned the whole
                // replay and took every complete record before it down with the
                // torn one -- so a clean crash made the WAL unreplayable rather
                // than replayable up to the last good record. The length-prefix
                // read above has always stopped cleanly on a short read; this
                // is the same stop, for the same reason, one field later
                // (samyama-graph#1311).
                //
                // A failed **checksum** still errors. That is corruption of a
                // record that was written in full, which is a different fact
                // from a write that did not finish, and quietly discarding it
                // would hide a damaged disk.
                buf.resize(len, 0);
                match reader.read_exact(&mut buf) {
                    Ok(_) => {}
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        warn!(
                            "WAL {}: a record promising {} bytes is short; the write \
                             did not finish. Replaying up to the previous record.",
                            file_path.display(),
                            len
                        );
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }

                let record_offset = offset;
                offset += 4 + len as u64;

                // Verify the checksum and decode. Format 1 checks the CRC
                // before decoding, so damage anywhere in the body -- sequence
                // included -- is reported as corruption.
                let (sequence, entry) = match decode_body(versioned, &buf) {
                    Ok(decoded) => decoded,
                    Err(DecodeError::Checksum) => {
                        warn!(
                            "WAL {}: checksum mismatch in the record at byte {}; \
                             stopping replay",
                            file_path.display(),
                            record_offset
                        );
                        return Err(WalError::Corruption(record_offset));
                    }
                    Err(DecodeError::Other(e)) => return Err(e),
                };

                // Skip if before from_sequence
                if sequence < from_sequence {
                    continue;
                }

                // Apply entry
                callback(&entry)?;
                replayed += 1;
                last_sequence = sequence;
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

    #[test]
    fn test_wal_versioned_node_update() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();

        let entry = WalEntry::UpdateNodeProperties {
            tenant: "default".to_string(),
            node_id: 42,
            properties: vec![1, 2, 3],
            version: 5,
        };
        let seq = wal.append(entry).unwrap();
        assert_eq!(seq, 1);
        wal.flush().unwrap();

        let mut found = false;
        wal.replay(0, |entry| {
            if let WalEntry::UpdateNodeProperties { node_id, version, .. } = entry {
                assert_eq!(*node_id, 42);
                assert_eq!(*version, 5);
                found = true;
            }
            Ok(())
        }).unwrap();
        assert!(found);
    }

    #[test]
    fn test_wal_versioned_edge_update() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();

        let entry = WalEntry::UpdateEdgeProperties {
            tenant: "default".to_string(),
            edge_id: 99,
            properties: vec![4, 5, 6],
            version: 3,
        };
        wal.append(entry).unwrap();
        wal.flush().unwrap();

        let mut found = false;
        wal.replay(0, |entry| {
            if let WalEntry::UpdateEdgeProperties { edge_id, version, .. } = entry {
                assert_eq!(*edge_id, 99);
                assert_eq!(*version, 3);
                found = true;
            }
            Ok(())
        }).unwrap();
        assert!(found);
    }

    #[test]
    fn test_wal_legacy_entry_defaults_version_zero() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();

        let entry = WalEntry::UpdateNodeProperties {
            tenant: "t".to_string(),
            node_id: 1,
            properties: vec![],
            version: 0,
        };
        wal.append(entry).unwrap();
        wal.flush().unwrap();

        let mut found_version = None;
        wal.replay(0, |entry| {
            if let WalEntry::UpdateNodeProperties { version, .. } = entry {
                found_version = Some(*version);
            }
            Ok(())
        }).unwrap();
        assert_eq!(found_version, Some(0));
    }

    fn node_entry(id: u64) -> WalEntry {
        WalEntry::CreateNode {
            tenant: "default".to_string(),
            node_id: id,
            labels: vec!["L".to_string()],
            properties: vec![],
        }
    }

    /// Write raw records (length word, body) into a WAL file of the directory.
    fn write_records(dir: &Path, name: &str, records: &[(u32, Vec<u8>)]) {
        let mut f = File::create(dir.join(name)).unwrap();
        for (word, body) in records {
            f.write_all(&word.to_le_bytes()).unwrap();
            f.write_all(body).unwrap();
        }
    }

    fn legacy_body(sequence: u64, entry: &WalEntry, corrupt: bool) -> Vec<u8> {
        let bytes = bincode::serialize(entry).unwrap();
        let mut checksum = bytes.iter().fold(0u32, |acc, &b| acc ^ (b as u32));
        if corrupt {
            checksum ^= 0xff;
        }
        bincode::serialize(&(sequence, entry, checksum)).unwrap()
    }

    fn replay_all(wal: &Wal, from: u64) -> WalResult<Vec<u64>> {
        let mut ids = Vec::new();
        wal.replay(from, |e| {
            if let WalEntry::CreateNode { node_id, .. } = e {
                ids.push(*node_id);
            }
            Ok(())
        })?;
        Ok(ids)
    }

    #[test]
    fn a_legacy_record_still_replays() {
        let dir = TempDir::new().unwrap();
        let body = legacy_body(1, &node_entry(7), false);
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[(body.len() as u32, body)],
        );
        let wal = Wal::new(dir.path()).unwrap();
        assert_eq!(replay_all(&wal, 0).unwrap(), vec![7]);
    }

    #[test]
    fn a_legacy_record_with_a_bad_checksum_is_corruption() {
        let dir = TempDir::new().unwrap();
        let body = legacy_body(1, &node_entry(7), true);
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[(body.len() as u32, body)],
        );
        let wal = Wal::new(dir.path()).unwrap();
        assert!(matches!(replay_all(&wal, 0), Err(WalError::Corruption(0))));
    }

    #[test]
    fn a_legacy_record_that_does_not_decode_is_an_error() {
        let dir = TempDir::new().unwrap();
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[(3, vec![1, 2, 3])],
        );
        let wal = Wal::new(dir.path()).unwrap();
        assert!(matches!(
            replay_all(&wal, 0),
            Err(WalError::Serialization(_))
        ));
    }

    #[test]
    fn a_versioned_record_too_short_for_its_checksum_is_corruption() {
        let dir = TempDir::new().unwrap();
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[(3 | FORMAT_FLAG, vec![1, 2, 3])],
        );
        let wal = Wal::new(dir.path()).unwrap();
        assert!(matches!(replay_all(&wal, 0), Err(WalError::Corruption(0))));
    }

    #[test]
    fn an_unknown_record_format_is_refused_by_name() {
        let dir = TempDir::new().unwrap();
        let mut body = encode_v1(1, &node_entry(1)).unwrap();
        // Change the format byte and re-seal it so the checksum still holds.
        body.truncate(body.len() - V1_CRC_LEN);
        body[0] = 9;
        let crc = crc32fast::hash(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[(body.len() as u32 | FORMAT_FLAG, body)],
        );
        let wal = Wal::new(dir.path()).unwrap();
        match replay_all(&wal, 0) {
            Err(WalError::InvalidEntry(msg)) => assert!(msg.contains("format 9"), "{msg}"),
            other => panic!("expected an invalid-entry error, got {other:?}"),
        }
    }

    #[test]
    fn a_body_that_does_not_decode_under_a_valid_checksum_is_an_error() {
        let dir = TempDir::new().unwrap();
        let mut body = vec![FORMAT_V1];
        body.extend_from_slice(&1u64.to_le_bytes());
        body.extend_from_slice(&[0xff, 0xff, 0xff, 0xff]);
        let crc = crc32fast::hash(&body);
        body.extend_from_slice(&crc.to_le_bytes());
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[(body.len() as u32 | FORMAT_FLAG, body)],
        );
        let wal = Wal::new(dir.path()).unwrap();
        assert!(matches!(
            replay_all(&wal, 0),
            Err(WalError::Serialization(_))
        ));
    }

    #[test]
    fn replay_skips_records_before_the_requested_sequence() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();
        for id in 1..=4 {
            wal.append(node_entry(id)).unwrap();
        }
        wal.flush().unwrap();
        assert_eq!(replay_all(&wal, 3).unwrap(), vec![3, 4]);
    }

    #[test]
    fn sync_mode_can_be_forced_and_still_appends() {
        let dir = TempDir::new().unwrap();
        let mut wal = Wal::new(dir.path()).unwrap();
        wal.set_sync_mode(true);
        assert!(wal.sync_mode());
        assert_eq!(wal.append(node_entry(1)).unwrap(), 1);
        wal.set_sync_mode(false);
        assert!(!wal.sync_mode());
        assert_eq!(wal.current_sequence(), 1);
        assert_eq!(replay_all(&wal, 0).unwrap(), vec![1]);
    }

    #[test]
    fn unrelated_and_unparseable_files_are_ignored() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"hello").unwrap();
        std::fs::write(dir.path().join("wal-zzzz.log"), b"").unwrap();
        let wal = Wal::new(dir.path()).unwrap();
        assert_eq!(
            wal.current_sequence(),
            0,
            "a non-hex name does not set the sequence"
        );
        assert!(replay_all(&wal, 0).unwrap().is_empty());
    }

    #[test]
    fn a_torn_tail_is_dropped_and_earlier_records_replay() {
        let dir = TempDir::new().unwrap();
        let good = encode_v1(1, &node_entry(5)).unwrap();
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[
                (good.len() as u32 | FORMAT_FLAG, good),
                // Promises 100 bytes, delivers 3: the write did not finish.
                (100 | FORMAT_FLAG, vec![1, 2, 3]),
            ],
        );
        let wal = Wal::new(dir.path()).unwrap();
        assert_eq!(replay_all(&wal, 0).unwrap(), vec![5]);
    }

    #[test]
    fn a_versioned_record_with_a_bad_crc_is_corruption_at_its_offset() {
        let dir = TempDir::new().unwrap();
        let good = encode_v1(1, &node_entry(5)).unwrap();
        let mut bad = encode_v1(2, &node_entry(6)).unwrap();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        let first_len = good.len() as u64;
        write_records(
            dir.path(),
            "wal-0000000000000000.log",
            &[
                (good.len() as u32 | FORMAT_FLAG, good),
                (bad.len() as u32 | FORMAT_FLAG, bad),
            ],
        );
        let wal = Wal::new(dir.path()).unwrap();
        match replay_all(&wal, 0) {
            Err(WalError::Corruption(offset)) => assert_eq!(offset, 4 + first_len),
            other => panic!("expected corruption, got {other:?}"),
        }
    }
}
