//! # RocksDB Storage Layer
//!
//! ## Column families
//!
//! RocksDB column families are like separate "namespaces" within one database instance.
//! Each column family has its own memtable, SSTable files, and compaction settings. In
//! Samyama, each tenant gets a dedicated column family, providing data isolation without
//! the overhead of separate database instances.
//!
//! ## Key encoding
//!
//! Keys are tenant-prefixed -- `{tenant}:n:{id:016x}` and `{tenant}:e:{id:016x}` -- so
//! that even within a column family, data is logically partitioned.
//!
//! The prefix alone does not establish that partition, and reading it as though it did
//! is what #2108 was. Two things are needed on every tenant-scoped read and delete:
//!
//!   * an explicit upper bound on the iterator. `prefix_iterator_cf` confines an
//!     iterator only when the column family has a prefix extractor configured, and
//!     none is, so `prefix_same_as_start` is inert and the iterator runs to the end
//!     of the column family.
//!   * a check that the key has a row key's exact shape. A tenant id is not
//!     validated anywhere, so a tenant may be named to sit inside another's range:
//!     `acme:n` writes `acme:n:n:{id}`, which is inside `acme`'s `acme:n:` range.
//!
//! `row_range` and `owns_row` are the two halves, and every tenant-scoped operation
//! here uses both.
//!
//! ## Serialization
//!
//! Rust structs (nodes, edges, properties) are serialized to bytes using `bincode` — a
//! compact binary format that is significantly faster and smaller than JSON. The trade-off
//! is that bincode is not human-readable, but for internal storage this is the right choice.
//!
//! ## Write buffer tuning
//!
//! RocksDB's write buffer (memtable) accumulates writes in memory before flushing to disk
//! as sorted SSTable files. Larger write buffers batch more writes per flush, improving
//! throughput but using more memory. The default is typically 64MB per column family.
//!
//! ## Rust concept: `Arc<T>`
//!
//! `Arc` (Atomic Reference Counting) enables shared ownership across threads. Multiple
//! parts of the system (server, query executor, persistence manager) hold `Arc` references
//! to the same storage instance. The reference count is updated atomically, and the storage
//! is dropped only when the last reference goes away. This is Rust's safe alternative to
//! shared pointers in C++.

use crate::graph::{Edge, EdgeId, Node, NodeId, PropertyMap};
use rocksdb::{ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, info};

/// Storage errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// RocksDB error
    #[error("RocksDB error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    /// Not found
    #[error("Key not found: {0}")]
    NotFound(String),

    /// Column family error
    #[error("Column family error: {0}")]
    ColumnFamily(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Serialized node for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredNode {
    id: u64,
    labels: Vec<String>,
    properties: Vec<u8>, // Serialized PropertyMap
    created_at: i64,
    updated_at: i64,
}

/// Serialized edge for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredEdge {
    id: u64,
    source: u64,
    target: u64,
    edge_type: String,
    properties: Vec<u8>, // Serialized PropertyMap
    created_at: i64,
}

#[cfg(test)]
thread_local! {
    /// Point reads of a node or edge row, for tests that pin how many
    /// existence probes a batch of writes costs (#1109).
    pub(crate) static ROW_READS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Row writes staged for one RocksDB write. See
/// [`PersistentStorage::commit_rows`].
#[derive(Default)]
pub struct RowBatch {
    inner: WriteBatch,
    rows: usize,
}

impl RowBatch {
    /// Rows staged since the last commit.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Whether nothing is staged.
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
}

/// RocksDB-based persistent storage
pub struct PersistentStorage {
    /// RocksDB instance
    db: Arc<DB>,
    /// Storage path (retained for debugging and future path-based operations)
    #[allow(dead_code)]
    path: String,
    /// Write options, carrying whether each write is synced to the platter.
    ///
    /// RocksDB was opened with no `WriteOptions` at all, so every write used
    /// the default — unsynced. With the WAL also unsynced that made the whole
    /// write path unsynced, which is what `docs/ACID-GUARANTEES.md` §4 had to
    /// say and what #1309 is about. `SAMYAMA_FSYNC=1` turns both on together;
    /// turning on one and not the other would be a durability level nobody
    /// asked for.
    write_opts: WriteOptions,
}

/// Is an acknowledged write forced to the platter?
///
/// Off by default, which is what the engine has always done. This is a change
/// of what a user can *choose*, not of what they get without asking.
pub fn fsync_enabled() -> bool {
    matches!(
        std::env::var("SAMYAMA_FSYNC").unwrap_or_default().to_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// One row as a RocksDB iterator yields it: the key and value, or the read error.
type RowItem = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>;

impl PersistentStorage {
    /// Open or create a new persistent storage
    pub fn open(path: impl AsRef<Path>) -> StorageResult<Self> {
        let path_str = path.as_ref().to_str().unwrap().to_string();

        info!("Opening persistent storage at: {}", path_str);

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Performance tuning (from ADR-002)
        opts.set_write_buffer_size(64 * 1024 * 1024); // 64 MB
        opts.set_max_write_buffer_number(3);
        opts.set_min_write_buffer_number_to_merge(1);

        // Compression (LZ4 for lower levels, Zstd for higher)
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);

        // WAL configuration
        opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);

        // Define column families
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new("nodes", Self::node_cf_options()),
            ColumnFamilyDescriptor::new("edges", Self::edge_cf_options()),
            ColumnFamilyDescriptor::new("indices", Self::index_cf_options()),
        ];

        // Open database
        let db = DB::open_cf_descriptors(&opts, &path_str, cf_descriptors)?;

        info!("Persistent storage opened successfully");

        let mut write_opts = WriteOptions::default();
        write_opts.set_sync(fsync_enabled());
        Ok(Self {
            db: Arc::new(db),
            path: path_str,
            write_opts,
        })
    }

    /// Column family options for nodes
    fn node_cf_options() -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }

    /// Column family options for edges
    fn edge_cf_options() -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }

    /// Column family options for indices
    fn index_cf_options() -> Options {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts
    }

    /// Store a node
    /// Write a node to storage exactly as given.
    ///
    /// **The caller owns the property merge**, for the same reason as
    /// `PersistenceManager::persist_create_node`: this serialises the row copy, and
    /// a node from a snapshot import has an empty one while its values sit in the
    /// column store (#545, #1129). A `Node` on its own cannot reach the columns, so
    /// this function cannot fix it — the caller has to pass a merged node.
    pub fn put_node(&self, tenant: &str, node: &Node) -> StorageResult<()> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;
        let (key, value) = Self::encode_node(tenant, node)?;

        // Write to RocksDB
        self.db.put_cf_opt(&cf, key, value, &self.write_opts)?;

        debug!("Stored node {} for tenant {}", node.id, tenant);

        Ok(())
    }

    /// A node's key and stored bytes. One encoding for the single-row write and
    /// the batched one, so a row reads back the same whichever wrote it.
    fn encode_node(tenant: &str, node: &Node) -> StorageResult<(Vec<u8>, Vec<u8>)> {
        // Serialize properties
        let properties = bincode::serialize(&node.properties)?;

        // Create stored node
        let stored = StoredNode {
            id: node.id.as_u64(),
            labels: node.labels.iter().map(|l| l.as_str().to_string()).collect(),
            properties,
            created_at: node.created_at,
            updated_at: node.updated_at,
        };

        // Serialize node
        let value = bincode::serialize(&stored)?;

        // Create key with tenant prefix
        Ok((Self::node_key(tenant, node.id.as_u64()), value))
    }

    /// Get a node
    pub fn get_node(&self, tenant: &str, node_id: u64) -> StorageResult<Option<Node>> {
        #[cfg(test)]
        ROW_READS.with(|c| c.set(c.get() + 1));
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;

        let key = Self::node_key(tenant, node_id);

        match self.db.get_cf(&cf, key)? {
            Some(value) => {
                let stored: StoredNode = bincode::deserialize(&value)?;
                let properties: PropertyMap = bincode::deserialize(&stored.properties)?;

                let node = Node {
                    id: NodeId::new(stored.id),
                    version: 1,
                    labels: stored.labels.into_iter()
                        .map(|s| crate::graph::Label::new(s))
                        .collect(),
                    properties,
                    created_at: stored.created_at,
                    updated_at: stored.updated_at,
                };

                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    /// Which of `ids` already have a stored row, in one batched read (#1109).
    ///
    /// A writer that needs only to know whether an id is new used to call
    /// `get_node` per id: a point read each, and a decode of a row it then
    /// threw away. This is one `batched_multi_get_cf` for the whole list, and
    /// the values are pinned rather than copied or decoded. `edges` picks the
    /// column family; the answer is in the order of `ids`.
    pub fn rows_exist(&self, tenant: &str, edges: bool, ids: &[u64]) -> StorageResult<Vec<bool>> {
        #[cfg(test)]
        ROW_READS.with(|c| c.set(c.get() + 1));
        let name = if edges { "edges" } else { "nodes" };
        let cf = self
            .db
            .cf_handle(name)
            .ok_or_else(|| StorageError::ColumnFamily(name.to_string()))?;
        let keys: Vec<Vec<u8>> = ids
            .iter()
            .map(|&id| {
                if edges {
                    Self::edge_key(tenant, id)
                } else {
                    Self::node_key(tenant, id)
                }
            })
            .collect();
        self.db
            .batched_multi_get_cf(&cf, &keys, false)
            .into_iter()
            .map(|r| Ok(r?.is_some()))
            .collect()
    }

    /// Store an edge
    pub fn put_edge(&self, tenant: &str, edge: &Edge) -> StorageResult<()> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;
        let (key, value) = Self::encode_edge(tenant, edge)?;

        // Write to RocksDB
        self.db.put_cf_opt(&cf, key, value, &self.write_opts)?;

        debug!("Stored edge {} for tenant {}", edge.id, tenant);

        Ok(())
    }

    /// An edge's key and stored bytes; see `encode_node`.
    fn encode_edge(tenant: &str, edge: &Edge) -> StorageResult<(Vec<u8>, Vec<u8>)> {
        // Serialize properties
        let properties = bincode::serialize(&edge.properties)?;

        // Create stored edge
        let stored = StoredEdge {
            id: edge.id.as_u64(),
            source: edge.source.as_u64(),
            target: edge.target.as_u64(),
            edge_type: edge.edge_type.as_str().to_string(),
            properties,
            created_at: edge.created_at,
        };

        // Serialize edge
        let value = bincode::serialize(&stored)?;

        // Create key with tenant prefix
        Ok((Self::edge_key(tenant, edge.id.as_u64()), value))
    }

    /// Get an edge
    pub fn get_edge(&self, tenant: &str, edge_id: u64) -> StorageResult<Option<Edge>> {
        #[cfg(test)]
        ROW_READS.with(|c| c.set(c.get() + 1));
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;

        let key = Self::edge_key(tenant, edge_id);

        match self.db.get_cf(&cf, key)? {
            Some(value) => {
                let stored: StoredEdge = bincode::deserialize(&value)?;
                let properties: PropertyMap = bincode::deserialize(&stored.properties)?;

                let edge = Edge {
                    id: EdgeId::new(stored.id),
                    version: 1,
                    source: NodeId::new(stored.source),
                    target: NodeId::new(stored.target),
                    edge_type: crate::graph::EdgeType::new(stored.edge_type),
                    properties,
                    created_at: stored.created_at,
                };

                Ok(Some(edge))
            }
            None => Ok(None),
        }
    }

    /// Delete a node
    pub fn delete_node(&self, tenant: &str, node_id: u64) -> StorageResult<()> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;

        let key = Self::node_key(tenant, node_id);
        self.db.delete_cf_opt(&cf, key, &self.write_opts)?;

        debug!("Deleted node {} for tenant {}", node_id, tenant);

        Ok(())
    }

    /// Delete an edge
    pub fn delete_edge(&self, tenant: &str, edge_id: u64) -> StorageResult<()> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;

        let key = Self::edge_key(tenant, edge_id);
        self.db.delete_cf_opt(&cf, key, &self.write_opts)?;

        debug!("Deleted edge {} for tenant {}", edge_id, tenant);

        Ok(())
    }

    /// Stage a node write into `batch`; nothing reaches the database until
    /// [`Self::commit_rows`]. Same key and bytes as [`Self::put_node`].
    pub fn stage_put_node(&self, batch: &mut RowBatch, tenant: &str, node: &Node) -> StorageResult<()> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;
        let (key, value) = Self::encode_node(tenant, node)?;
        batch.inner.put_cf(&cf, key, value);
        batch.rows += 1;
        Ok(())
    }

    /// Stage an edge write; see [`Self::stage_put_node`].
    pub fn stage_put_edge(&self, batch: &mut RowBatch, tenant: &str, edge: &Edge) -> StorageResult<()> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;
        let (key, value) = Self::encode_edge(tenant, edge)?;
        batch.inner.put_cf(&cf, key, value);
        batch.rows += 1;
        Ok(())
    }

    /// Stage a node delete; see [`Self::stage_put_node`].
    pub fn stage_delete_node(&self, batch: &mut RowBatch, tenant: &str, node_id: u64) -> StorageResult<()> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;
        batch.inner.delete_cf(&cf, Self::node_key(tenant, node_id));
        batch.rows += 1;
        Ok(())
    }

    /// Stage an edge delete; see [`Self::stage_put_node`].
    pub fn stage_delete_edge(&self, batch: &mut RowBatch, tenant: &str, edge_id: u64) -> StorageResult<()> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;
        batch.inner.delete_cf(&cf, Self::edge_key(tenant, edge_id));
        batch.rows += 1;
        Ok(())
    }

    /// Write every staged row in one RocksDB write: one log record, one
    /// `write(2)`, and with `SAMYAMA_FSYNC=1` one fsync, instead of one of each
    /// per row (#1666). Atomic: all of the batch's rows land or none do. The
    /// batch is empty afterwards and can be staged into again.
    pub fn commit_rows(&self, batch: &mut RowBatch) -> StorageResult<()> {
        if batch.rows == 0 {
            return Ok(());
        }
        let staged = std::mem::take(&mut batch.inner);
        batch.rows = 0;
        self.db.write_opt(staged, &self.write_opts)?;
        Ok(())
    }

    /// Create a snapshot
    pub fn create_snapshot(&self) -> rocksdb::Snapshot<'_> {
        self.db.snapshot()
    }

    /// Flush all data to disk
    pub fn flush(&self) -> StorageResult<()> {
        self.db.flush()?;
        debug!("Flushed storage to disk");
        Ok(())
    }

    /// Write a tenant's index declarations (#1477).
    ///
    /// Into the `indices` column family, which has existed since the database
    /// was first laid out and held nothing: the three index registries were
    /// in-memory only, so a restart returned every row and no index. One key per
    /// tenant holding the whole catalog, because the catalog is written as a
    /// snapshot — see `crate::index::catalog::IndexCatalog` for why it is not a
    /// log of creates and drops.
    ///
    /// Same database and same `write_opts` as the rows, so a definition is
    /// exactly as durable as the nodes it indexes and honours `SAMYAMA_FSYNC`
    /// the same way.
    pub fn put_index_catalog(
        &self,
        tenant: &str,
        catalog: &crate::index::catalog::IndexCatalog,
    ) -> StorageResult<()> {
        let cf = self
            .db
            .cf_handle("indices")
            .ok_or_else(|| StorageError::ColumnFamily("indices".to_string()))?;
        let key = format!("catalog:{}", tenant);
        let value = bincode::serialize(catalog)?;
        self.db.put_cf_opt(&cf, key.as_bytes(), value, &self.write_opts)?;
        debug!("Wrote {} index definitions for {}", catalog.len(), tenant);
        Ok(())
    }

    /// Read a tenant's index declarations. An absent key is an empty catalog,
    /// not an error: every database written before #1477 has no key at all.
    pub fn get_index_catalog(
        &self,
        tenant: &str,
    ) -> StorageResult<crate::index::catalog::IndexCatalog> {
        let cf = self
            .db
            .cf_handle("indices")
            .ok_or_else(|| StorageError::ColumnFamily("indices".to_string()))?;
        let key = format!("catalog:{}", tenant);
        match self.db.get_cf(&cf, key.as_bytes())? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(Default::default()),
        }
    }

    /// Get all nodes for a tenant (for recovery)
    pub fn scan_nodes(&self, tenant: &str) -> StorageResult<Vec<Node>> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;

        let mut nodes = Vec::new();

        for item in self.row_iter(&cf, tenant, 'n') {
            let (key, value) = item?;
            // The iterator is bounded, so this only rejects a differently
            // shaped key inside the range -- another tenant named like this
            // one's prefix.
            if !Self::owns_row(tenant, 'n', &key) {
                continue;
            }
            let stored: StoredNode = bincode::deserialize(&value)?;
            let properties: PropertyMap = bincode::deserialize(&stored.properties)?;

            let node = Node {
                id: NodeId::new(stored.id),
                version: 1,
                labels: stored.labels.into_iter()
                    .map(crate::graph::Label::new)
                    .collect(),
                properties,
                created_at: stored.created_at,
                updated_at: stored.updated_at,
            };

            nodes.push(node);
        }

        Ok(nodes)
    }

    /// Get all edges for a tenant (for recovery)
    pub fn scan_edges(&self, tenant: &str) -> StorageResult<Vec<Edge>> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;

        let mut edges = Vec::new();

        for item in self.row_iter(&cf, tenant, 'e') {
            let (key, value) = item?;
            if !Self::owns_row(tenant, 'e', &key) {
                continue;
            }
            let stored: StoredEdge = bincode::deserialize(&value)?;
            let properties: PropertyMap = bincode::deserialize(&stored.properties)?;

            let edge = Edge {
                id: EdgeId::new(stored.id),
                version: 1,
                source: NodeId::new(stored.source),
                target: NodeId::new(stored.target),
                edge_type: crate::graph::EdgeType::new(stored.edge_type),
                properties,
                created_at: stored.created_at,
            };

            edges.push(edge);
        }

        Ok(edges)
    }

    /// List all tenants that have persisted data
    pub fn list_persisted_tenants(&self) -> StorageResult<Vec<String>> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;

        let mut tenants = HashSet::new();
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        // `{tenant}:n:{id:016x}`. The tenant is read by removing the suffix the
        // writer added, not by splitting on the first ':': a tenant id may
        // contain one and nothing rejects that, so `a:b` was listed as `a` --
        // and a caller that then scanned what the list named scanned the wrong
        // range. Split on bytes, because the cut may land inside a multi-byte
        // character of a tenant id and `str::split_at` would panic on it.
        const SUFFIX: usize = ":n:".len() + 16;
        for item in iter {
            let (key, _) = item?;
            let Some(cut) = key.len().checked_sub(SUFFIX) else {
                continue;
            };
            let (owner, tail) = key.split_at(cut);
            if &tail[..3] != b":n:" || !tail[3..].iter().all(u8::is_ascii_hexdigit) {
                continue;
            }
            if let Ok(tenant) = std::str::from_utf8(owner) {
                tenants.insert(tenant.to_string());
            }
        }

        Ok(tenants.into_iter().collect())
    }

    /// Remove every node and edge belonging to a tenant.
    ///
    /// `GRAPH.DELETE` cleared the in-memory store and left all of this behind
    /// (#1110), so a restart brought the graph back — and because `clear()` also
    /// rewinds the id counters, the writes that followed were persisted over ids
    /// that still belonged to the deleted graph.
    ///
    /// Bounded iteration and a shape check per key, not a range delete.
    ///
    /// A range delete over `["{tenant}:{kind}:", "{tenant}:{kind};")` is one
    /// round trip for the whole drop, and the comment here used to call that
    /// upper bound "the first key that cannot belong to it". It is not: a tenant
    /// id is never validated, so a tenant named `acme:n` writes
    /// `acme:n:n:{id}`, which is inside `acme`'s range -- and dropping `acme`
    /// deleted that tenant's rows with no way to tell afterwards. A range delete
    /// cannot express "and of this exact shape", so the drop reads the keys and
    /// deletes the ones this tenant owns.
    ///
    /// That costs a scan of the tenant's own range, which is the price of the
    /// guarantee. It is not one round trip per row: the deletes go in batches of
    /// `DELETE_BATCH`, bounded so dropping a large tenant does not hold the
    /// whole key set in memory at once.
    ///
    /// The index catalog goes too. `put_index_catalog` writes `catalog:{tenant}`
    /// into the `indices` column family, which this walked right past, so a
    /// dropped graph's index declarations outlived it and came back on a tenant
    /// later created under the same name -- the shape of #1110, in the column
    /// family added after it.
    pub fn delete_graph(&self, tenant: &str) -> StorageResult<()> {
        /// Keys per write. Large enough that the drop is not a round trip per
        /// row, small enough to bound what is held while dropping a big tenant.
        const DELETE_BATCH: usize = 4096;

        for (cf_name, kind) in [("nodes", 'n'), ("edges", 'e')] {
            let cf = self
                .db
                .cf_handle(cf_name)
                .ok_or_else(|| StorageError::ColumnFamily(cf_name.to_string()))?;
            let mut batch = WriteBatch::default();
            let mut staged = 0usize;
            for item in self.row_iter(&cf, tenant, kind) {
                let (key, _) = item?;
                if !Self::owns_row(tenant, kind, &key) {
                    continue;
                }
                batch.delete_cf(&cf, &key);
                staged += 1;
                if staged == DELETE_BATCH {
                    self.db
                        .write_opt(std::mem::take(&mut batch), &self.write_opts)?;
                    staged = 0;
                }
            }
            if staged > 0 {
                self.db.write_opt(batch, &self.write_opts)?;
            }
        }

        // Absent on every database written before #1477, so a missing column
        // family is not an error here.
        if let Some(cf) = self.db.cf_handle("indices") {
            let key = format!("catalog:{}", tenant);
            self.db.delete_cf_opt(&cf, key.as_bytes(), &self.write_opts)?;
        }

        debug!("Dropped every node, edge and index declaration for tenant {}", tenant);
        Ok(())
    }

    /// The half-open key range that can hold `tenant`'s rows in a `kind`
    /// column family.
    ///
    /// The upper bound is the prefix with its last byte incremented. Every key
    /// carrying the prefix is below it, so it bounds the iterator -- but it does
    /// not by itself decide ownership: see `owns_row`.
    fn row_range(tenant: &str, kind: char) -> (Vec<u8>, Vec<u8>) {
        let from = format!("{}:{}:", tenant, kind).into_bytes();
        let mut to = from.clone();
        *to.last_mut().expect("the prefix always ends in ':'") += 1;
        (from, to)
    }

    /// Whether `key` is one of `tenant`'s own rows.
    ///
    /// Inside the range *and* shaped like a row key: the prefix followed by
    /// exactly the sixteen hex digits `node_key`/`edge_key` write. Being inside
    /// the range is not sufficient, because a tenant id is never validated, so
    /// a tenant can be named to put its own keys inside another's range --
    /// `acme:n` writes `acme:n:n:{id}`, inside `acme`'s `acme:n:` range. The
    /// length check is what separates them.
    fn owns_row(tenant: &str, kind: char, key: &[u8]) -> bool {
        let prefix = format!("{}:{}:", tenant, kind).into_bytes();
        key.len() == prefix.len() + 16
            && key.starts_with(&prefix)
            && key[prefix.len()..].iter().all(u8::is_ascii_hexdigit)
    }

    /// A forward iterator over just `tenant`'s range in `cf`.
    fn row_iter<'a>(
        &'a self,
        cf: &impl rocksdb::AsColumnFamilyRef,
        tenant: &str,
        kind: char,
    ) -> impl Iterator<Item = RowItem> + 'a {
        let (from, to) = Self::row_range(tenant, kind);
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_upper_bound(to);
        self.db.iterator_cf_opt(
            cf,
            opts,
            rocksdb::IteratorMode::From(&from, rocksdb::Direction::Forward),
        )
    }

    /// Create node key with tenant prefix
    fn node_key(tenant: &str, node_id: u64) -> Vec<u8> {
        format!("{}:n:{:016x}", tenant, node_id).into_bytes()
    }

    /// Create edge key with tenant prefix
    fn edge_key(tenant: &str, edge_id: u64) -> Vec<u8> {
        format!("{}:e:{:016x}", tenant, edge_id).into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::graph::Label;

    /// A row for `tenant`, so a test can say who wrote what.
    fn node_for(storage: &PersistentStorage, tenant: &str, id: u64) {
        let mut node = Node::new(NodeId::new(id), Label::new("Row"));
        node.set_property("owner", tenant);
        storage.put_node(tenant, &node).unwrap();
    }

    fn owners(nodes: &[Node]) -> Vec<String> {
        let mut out: Vec<String> = nodes
            .iter()
            .map(|n| n.get_property("owner").unwrap().as_string().unwrap().to_string())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// A tenant's scan must stop at that tenant's rows.
    ///
    /// The keys are `{tenant}:n:{id:016x}`, one contiguous range per tenant, but
    /// `prefix_iterator_cf` only confines an iterator when the column family has
    /// a prefix extractor configured. None is, so `prefix_same_as_start` is
    /// inert: the iterator seeks to `{tenant}:` and then runs to the end of the
    /// column family, and the scan loop discards the key, so nothing downstream
    /// notices. Recovery for one tenant loaded every tenant sorting after it.
    #[test]
    fn a_tenant_scan_stops_at_its_own_rows() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        // `b` sorts after `a:`, so it is what an unconfined iterator walks into.
        node_for(&storage, "a", 1);
        node_for(&storage, "b", 2);
        node_for(&storage, "c", 3);

        assert_eq!(owners(&storage.scan_nodes("a").unwrap()), vec!["a"]);
        assert_eq!(owners(&storage.scan_nodes("b").unwrap()), vec!["b"]);
        assert_eq!(owners(&storage.scan_nodes("c").unwrap()), vec!["c"]);
    }

    /// Nothing validates a tenant id, so one can be named to look like another
    /// tenant's key prefix. `acme:n` writes `acme:n:n:{id}`, which lies inside
    /// `acme`'s own `acme:n:` range -- so the two tenants must not read or
    /// delete each other's rows.
    #[test]
    fn a_tenant_named_like_another_s_key_prefix_stays_separate() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        node_for(&storage, "acme", 1);
        node_for(&storage, "acme:n", 2);

        assert_eq!(owners(&storage.scan_nodes("acme").unwrap()), vec!["acme"]);
        assert_eq!(owners(&storage.scan_nodes("acme:n").unwrap()), vec!["acme:n"]);

        // And dropping one must leave the other whole. A range delete over
        // ["acme:n:", "acme:n;") covers every key `acme:n` owns.
        storage.delete_graph("acme").unwrap();
        assert!(storage.scan_nodes("acme").unwrap().is_empty(), "acme is gone");
        assert_eq!(
            owners(&storage.scan_nodes("acme:n").unwrap()),
            vec!["acme:n"],
            "dropping `acme` must not touch the tenant named `acme:n`"
        );
    }

    /// Edges too: same key shape, same column family layout, separate code path.
    #[test]
    fn a_tenant_edge_scan_stops_at_its_own_rows() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        for (tenant, id) in [("a", 1u64), ("b", 2), ("acme:e", 3)] {
            let edge = Edge::new(EdgeId::new(id), NodeId::new(1), NodeId::new(2), "LINKS");
            storage.put_edge(tenant, &edge).unwrap();
        }

        assert_eq!(storage.scan_edges("a").unwrap().len(), 1);
        assert_eq!(storage.scan_edges("b").unwrap().len(), 1);
        assert_eq!(storage.scan_edges("acme:e").unwrap().len(), 1);
    }

    /// The tenant of `a:b:n:{id}` is `a:b`, not `a`.
    ///
    /// Splitting on the first `:` reads the key as if a tenant id could not
    /// contain one. Nothing enforces that, so a tenant with a `:` in its name
    /// was listed under a prefix of itself -- and a caller recovering what the
    /// list named then scanned the wrong range.
    #[test]
    fn listing_tenants_reads_the_whole_tenant_id() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        node_for(&storage, "plain", 1);
        node_for(&storage, "a:b", 2);

        let mut listed = storage.list_persisted_tenants().unwrap();
        listed.sort();
        assert_eq!(listed, vec!["a:b".to_string(), "plain".to_string()]);
    }

    /// A drop must take the tenant's index declarations with it.
    ///
    /// `put_index_catalog` writes `catalog:{tenant}` into the `indices` column
    /// family, and the drop walked only `nodes` and `edges`. So the catalog
    /// outlived the graph, and a tenant later created under the same name came
    /// back with index definitions for rows that no longer existed -- the shape
    /// of #1110, in the column family that was added after it.
    #[test]
    fn dropping_a_graph_drops_its_index_catalog() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        node_for(&storage, "acme", 1);
        let catalog = crate::index::catalog::IndexCatalog {
            definitions: vec![crate::index::catalog::IndexDefinition::Property {
                label: "Person".to_string(),
                property: "email".to_string(),
            }],
        };
        storage.put_index_catalog("acme", &catalog).unwrap();
        assert!(!storage.get_index_catalog("acme").unwrap().is_empty());

        storage.delete_graph("acme").unwrap();
        assert_eq!(
            storage.get_index_catalog("acme").unwrap().len(),
            0,
            "the dropped graph's index declarations outlived it"
        );
    }

    #[test]
    fn test_storage_open() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();
        drop(storage);
    }

    #[test]
    fn test_put_get_node() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let mut node = Node::new(NodeId::new(1), Label::new("Person"));
        node.set_property("name", "Alice");

        storage.put_node("default", &node).unwrap();

        let retrieved = storage.get_node("default", 1).unwrap();
        assert!(retrieved.is_some());

        let retrieved_node = retrieved.unwrap();
        assert_eq!(retrieved_node.id, NodeId::new(1));
        assert_eq!(retrieved_node.get_property("name").unwrap().as_string().unwrap(), "Alice");
    }

    #[test]
    fn test_tenant_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let node = Node::new(NodeId::new(1), Label::new("Person"));

        storage.put_node("tenant1", &node).unwrap();
        storage.put_node("tenant2", &node).unwrap();

        // Both tenants should have their own copy
        assert!(storage.get_node("tenant1", 1).unwrap().is_some());
        assert!(storage.get_node("tenant2", 1).unwrap().is_some());

        // Delete from tenant1 shouldn't affect tenant2
        storage.delete_node("tenant1", 1).unwrap();
        assert!(storage.get_node("tenant1", 1).unwrap().is_none());
        assert!(storage.get_node("tenant2", 1).unwrap().is_some());
    }

    #[test]
    fn test_scan_nodes() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        // Create multiple nodes
        for i in 1..=5 {
            let node = Node::new(NodeId::new(i), Label::new("Person"));
            storage.put_node("default", &node).unwrap();
        }

        let nodes = storage.scan_nodes("default").unwrap();
        assert_eq!(nodes.len(), 5);
    }

    // ========== Additional Storage Coverage Tests ==========

    #[test]
    fn test_get_node_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let result = storage.get_node("default", 999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_put_get_edge() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let mut edge = Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("KNOWS"),
        );
        edge.set_property("since", "2020");

        storage.put_edge("default", &edge).unwrap();

        let retrieved = storage.get_edge("default", 1).unwrap();
        assert!(retrieved.is_some());
        let retrieved_edge = retrieved.unwrap();
        assert_eq!(retrieved_edge.id, EdgeId::new(1));
        assert_eq!(retrieved_edge.source, NodeId::new(10));
        assert_eq!(retrieved_edge.target, NodeId::new(20));
        assert_eq!(retrieved_edge.edge_type.as_str(), "KNOWS");
    }

    #[test]
    fn test_get_edge_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let result = storage.get_edge("default", 999).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_node() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let node = Node::new(NodeId::new(1), Label::new("Person"));
        storage.put_node("default", &node).unwrap();
        assert!(storage.get_node("default", 1).unwrap().is_some());

        storage.delete_node("default", 1).unwrap();
        assert!(storage.get_node("default", 1).unwrap().is_none());
    }

    #[test]
    fn test_delete_edge() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let edge = Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("KNOWS"),
        );
        storage.put_edge("default", &edge).unwrap();
        assert!(storage.get_edge("default", 1).unwrap().is_some());

        storage.delete_edge("default", 1).unwrap();
        assert!(storage.get_edge("default", 1).unwrap().is_none());
    }

    #[test]
    fn test_delete_nonexistent_node() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        // Deleting a non-existent node should not error (RocksDB delete is idempotent)
        let result = storage.delete_node("default", 999);
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_nonexistent_edge() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let result = storage.delete_edge("default", 999);
        assert!(result.is_ok());
    }

    #[test]
    fn test_scan_edges() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        for i in 1..=3 {
            let edge = Edge::new(
                EdgeId::new(i),
                NodeId::new(i * 10),
                NodeId::new(i * 10 + 1),
                crate::graph::EdgeType::new("REL"),
            );
            storage.put_edge("default", &edge).unwrap();
        }

        let edges = storage.scan_edges("default").unwrap();
        assert_eq!(edges.len(), 3);
    }

    #[test]
    fn test_scan_nodes_empty_tenant() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let nodes = storage.scan_nodes("empty_tenant").unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_scan_edges_empty_tenant() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let edges = storage.scan_edges("empty_tenant").unwrap();
        assert!(edges.is_empty());
    }

    #[test]
    fn test_flush() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let node = Node::new(NodeId::new(1), Label::new("Person"));
        storage.put_node("default", &node).unwrap();

        let result = storage.flush();
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let node = Node::new(NodeId::new(1), Label::new("Person"));
        storage.put_node("default", &node).unwrap();

        // Just ensure it doesn't panic
        let _snapshot = storage.create_snapshot();
    }

    #[test]
    fn test_list_persisted_tenants() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        // Initially empty
        let tenants = storage.list_persisted_tenants().unwrap();
        assert!(tenants.is_empty());

        // Add data for two tenants
        let node1 = Node::new(NodeId::new(1), Label::new("A"));
        storage.put_node("tenant_a", &node1).unwrap();

        let node2 = Node::new(NodeId::new(2), Label::new("B"));
        storage.put_node("tenant_b", &node2).unwrap();

        let tenants = storage.list_persisted_tenants().unwrap();
        assert!(tenants.len() >= 2);
        assert!(tenants.contains(&"tenant_a".to_string()));
        assert!(tenants.contains(&"tenant_b".to_string()));
    }

    #[test]
    fn test_node_with_multiple_labels() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let mut node = Node::new(NodeId::new(1), Label::new("Person"));
        node.add_label(Label::new("Employee"));
        node.add_label(Label::new("Manager"));

        storage.put_node("default", &node).unwrap();

        let retrieved = storage.get_node("default", 1).unwrap().unwrap();
        assert_eq!(retrieved.labels.len(), 3);
    }

    #[test]
    fn test_overwrite_node() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let mut node = Node::new(NodeId::new(1), Label::new("Person"));
        node.set_property("name", "Alice");
        storage.put_node("default", &node).unwrap();

        // Overwrite with different property
        let mut node2 = Node::new(NodeId::new(1), Label::new("Person"));
        node2.set_property("name", "Bob");
        storage.put_node("default", &node2).unwrap();

        let retrieved = storage.get_node("default", 1).unwrap().unwrap();
        assert_eq!(
            retrieved.get_property("name").unwrap().as_string().unwrap(),
            "Bob"
        );
    }

    #[test]
    fn test_tenant_isolation_edges() {
        let temp_dir = TempDir::new().unwrap();
        let storage = PersistentStorage::open(temp_dir.path()).unwrap();

        let edge = Edge::new(
            EdgeId::new(1),
            NodeId::new(10),
            NodeId::new(20),
            crate::graph::EdgeType::new("KNOWS"),
        );

        storage.put_edge("tenant1", &edge).unwrap();
        storage.put_edge("tenant2", &edge).unwrap();

        assert!(storage.get_edge("tenant1", 1).unwrap().is_some());
        assert!(storage.get_edge("tenant2", 1).unwrap().is_some());

        storage.delete_edge("tenant1", 1).unwrap();
        assert!(storage.get_edge("tenant1", 1).unwrap().is_none());
        assert!(storage.get_edge("tenant2", 1).unwrap().is_some());
    }

    #[test]
    fn test_storage_error_display() {
        let err = StorageError::NotFound("test_key".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Key not found"));
        assert!(msg.contains("test_key"));

        let err2 = StorageError::ColumnFamily("nodes".to_string());
        let msg2 = format!("{}", err2);
        assert!(msg2.contains("Column family error"));
    }
}
