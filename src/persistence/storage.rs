//! RocksDB storage layer implementation
//!
//! Implements REQ-PERSIST-001 (Persistence to disk) using RocksDB
//! Implements REQ-TENANT-002 (Data isolation) using column families

use crate::graph::{Edge, EdgeId, Node, NodeId, PropertyMap};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
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

/// RocksDB-based persistent storage
pub struct PersistentStorage {
    /// RocksDB instance
    db: Arc<DB>,
    /// Storage path (retained for debugging and future path-based operations)
    #[allow(dead_code)]
    path: String,
}

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

        Ok(Self {
            db: Arc::new(db),
            path: path_str,
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
    pub fn put_node(&self, tenant: &str, node: &Node) -> StorageResult<()> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;

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
        let key = Self::node_key(tenant, node.id.as_u64());

        // Write to RocksDB
        self.db.put_cf(&cf, key, value)?;

        debug!("Stored node {} for tenant {}", node.id, tenant);

        Ok(())
    }

    /// Get a node
    pub fn get_node(&self, tenant: &str, node_id: u64) -> StorageResult<Option<Node>> {
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

    /// Store an edge
    pub fn put_edge(&self, tenant: &str, edge: &Edge) -> StorageResult<()> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;

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
        let key = Self::edge_key(tenant, edge.id.as_u64());

        // Write to RocksDB
        self.db.put_cf(&cf, key, value)?;

        debug!("Stored edge {} for tenant {}", edge.id, tenant);

        Ok(())
    }

    /// Get an edge
    pub fn get_edge(&self, tenant: &str, edge_id: u64) -> StorageResult<Option<Edge>> {
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
        self.db.delete_cf(&cf, key)?;

        debug!("Deleted node {} for tenant {}", node_id, tenant);

        Ok(())
    }

    /// Delete an edge
    pub fn delete_edge(&self, tenant: &str, edge_id: u64) -> StorageResult<()> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;

        let key = Self::edge_key(tenant, edge_id);
        self.db.delete_cf(&cf, key)?;

        debug!("Deleted edge {} for tenant {}", edge_id, tenant);

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

    /// Get all nodes for a tenant (for recovery)
    pub fn scan_nodes(&self, tenant: &str) -> StorageResult<Vec<Node>> {
        let cf = self.db.cf_handle("nodes")
            .ok_or_else(|| StorageError::ColumnFamily("nodes".to_string()))?;

        let prefix = format!("{}:", tenant);
        let mut nodes = Vec::new();

        let iter = self.db.prefix_iterator_cf(&cf, prefix.as_bytes());

        for item in iter {
            let (_key, value) = item?;
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

            nodes.push(node);
        }

        Ok(nodes)
    }

    /// Get all edges for a tenant (for recovery)
    pub fn scan_edges(&self, tenant: &str) -> StorageResult<Vec<Edge>> {
        let cf = self.db.cf_handle("edges")
            .ok_or_else(|| StorageError::ColumnFamily("edges".to_string()))?;

        let prefix = format!("{}:", tenant);
        let mut edges = Vec::new();

        let iter = self.db.prefix_iterator_cf(&cf, prefix.as_bytes());

        for item in iter {
            let (_key, value) = item?;
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

        for item in iter {
            let (key, _) = item?;
            if let Ok(key_str) = std::str::from_utf8(&key) {
                if let Some(tenant) = key_str.split(':').next() {
                    tenants.insert(tenant.to_string());
                }
            }
        }

        Ok(tenants.into_iter().collect())
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
}
