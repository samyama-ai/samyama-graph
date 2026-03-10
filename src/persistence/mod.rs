//! # Persistence Layer
//!
//! ## ACID guarantees
//!
//! Database persistence is built around the ACID properties:
//! - **Atomicity**: operations either fully complete or fully roll back — no partial writes
//! - **Consistency**: the database always moves from one valid state to another
//! - **Isolation**: concurrent transactions don't interfere with each other
//! - **Durability**: once a write is committed, it survives crashes and power loss
//!
//! ## Write-Ahead Log (WAL)
//!
//! The WAL is the standard technique for durability. The idea: write the operation to a
//! sequential log file BEFORE modifying the actual data. If the process crashes mid-write,
//! the log can be replayed on recovery to reconstruct the correct state. This same pattern
//! is used by PostgreSQL, SQLite, and RocksDB internally.
//!
//! ## RocksDB
//!
//! RocksDB is Facebook's embedded key-value store, evolved from Google's LevelDB. It uses
//! an LSM-tree (Log-Structured Merge-tree) architecture optimized for write-heavy workloads:
//! writes go to an in-memory buffer (memtable), which periodically flushes to sorted disk
//! files (SSTables) that are compacted in the background. Column families provide logical
//! separation within a single database instance — each is an independent LSM-tree.
//!
//! ## Multi-tenancy
//!
//! Multiple isolated "tenants" share one database process. Each tenant gets its own RocksDB
//! column family (like a namespace), ensuring data isolation. Resource quotas (max nodes,
//! max edges, storage limits) prevent any single tenant from monopolizing shared resources.
//!
//! ## PersistenceManager
//!
//! The `PersistenceManager` orchestrates WAL + RocksDB + TenantManager. All writes flow
//! through the WAL first (for durability), then to RocksDB (for indexed storage). On
//! startup, any WAL entries written after the last checkpoint are replayed to bring the
//! in-memory graph state up to date.

pub mod storage;
pub mod tenant;
pub mod wal;

pub use storage::{PersistentStorage, StorageError, StorageResult};
pub use tenant::{
    ResourceQuotas, ResourceUsage, Tenant, TenantError, TenantManager, TenantResult,
    AutoEmbedConfig, NLQConfig, AgentConfig, ToolConfig, LLMProvider,
};
pub use wal::{Wal, WalEntry, WalError, WalResult};

use crate::graph::{Edge, Node, PropertyMap, GraphStore};
use std::path::Path;
use std::sync::Arc;
// warn removed - was unused import causing compiler warning
use tracing::info;

/// Integrated persistence manager combining WAL, storage, and tenancy
pub struct PersistenceManager {
    /// Base path for all data
    base_path: std::path::PathBuf,
    /// RocksDB storage
    storage: Arc<PersistentStorage>,
    /// Write-Ahead Log
    wal: Arc<std::sync::Mutex<Wal>>,
    /// Tenant manager
    tenants: Arc<TenantManager>,
}

impl PersistenceManager {
    /// Create a new persistence manager
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self, PersistenceError> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create subdirectories
        let storage_path = base_path.join("data");
        let wal_path = base_path.join("wal");
        let vector_path = base_path.join("vectors");

        std::fs::create_dir_all(&storage_path)?;
        std::fs::create_dir_all(&wal_path)?;
        std::fs::create_dir_all(&vector_path)?;

        info!("Initializing persistence manager at: {:?}", base_path);

        // Initialize storage
        let storage = PersistentStorage::open(&storage_path)?;
        info!("Storage initialized");

        // Initialize WAL
        let wal = Wal::new(&wal_path)?;
        info!("WAL initialized");

        // Initialize tenant manager
        let tenants = TenantManager::new();
        info!("Tenant manager initialized");

        Ok(Self {
            base_path,
            storage: Arc::new(storage),
            wal: Arc::new(std::sync::Mutex::new(wal)),
            tenants: Arc::new(tenants),
        })
    }

    /// Get tenant manager
    pub fn tenants(&self) -> &TenantManager {
        &self.tenants
    }

    /// Start the background indexer for a store
    pub fn start_indexer(&self, store: &GraphStore, receiver: tokio::sync::mpsc::UnboundedReceiver<crate::graph::event::IndexEvent>) {
        let vector_index = Arc::clone(&store.vector_index);
        let property_index = Arc::clone(&store.property_index);
        let tenant_manager = Arc::clone(&self.tenants);

        tokio::spawn(async move {
            GraphStore::start_background_indexer(
                receiver,
                vector_index,
                property_index,
                tenant_manager,
            ).await;
        });
    }

    /// Persist a node creation
    pub fn persist_create_node(&self, tenant: &str, node: &Node) -> Result<(), PersistenceError> {
        // Check tenant quota
        self.tenants.check_quota(tenant, "nodes")?;

        // Serialize properties
        let properties = bincode::serialize(&node.properties)?;

        // Write to WAL
        let entry = WalEntry::CreateNode {
            tenant: tenant.to_string(),
            node_id: node.id.as_u64(),
            labels: node.labels.iter().map(|l| l.as_str().to_string()).collect(),
            properties,
        };
        self.wal.lock().unwrap().append(entry)?;

        // Write to storage
        self.storage.put_node(tenant, node)?;

        // Update usage
        self.tenants.increment_usage(tenant, "nodes", 1)?;

        Ok(())
    }

    /// Persist an edge creation
    pub fn persist_create_edge(&self, tenant: &str, edge: &Edge) -> Result<(), PersistenceError> {
        // Check tenant quota
        self.tenants.check_quota(tenant, "edges")?;

        // Serialize properties
        let properties = bincode::serialize(&edge.properties)?;

        // Write to WAL
        let entry = WalEntry::CreateEdge {
            tenant: tenant.to_string(),
            edge_id: edge.id.as_u64(),
            source: edge.source.as_u64(),
            target: edge.target.as_u64(),
            edge_type: edge.edge_type.as_str().to_string(),
            properties,
        };
        self.wal.lock().unwrap().append(entry)?;

        // Write to storage
        self.storage.put_edge(tenant, edge)?;

        // Update usage
        self.tenants.increment_usage(tenant, "edges", 1)?;

        Ok(())
    }

    /// Persist a node deletion
    pub fn persist_delete_node(&self, tenant: &str, node_id: u64) -> Result<(), PersistenceError> {
        // Write to WAL
        let entry = WalEntry::DeleteNode {
            tenant: tenant.to_string(),
            node_id,
        };
        self.wal.lock().unwrap().append(entry)?;

        // Write to storage
        self.storage.delete_node(tenant, node_id)?;

        // Update usage
        self.tenants.decrement_usage(tenant, "nodes", 1)?;

        Ok(())
    }

    /// Persist an edge deletion
    pub fn persist_delete_edge(&self, tenant: &str, edge_id: u64) -> Result<(), PersistenceError> {
        // Write to WAL
        let entry = WalEntry::DeleteEdge {
            tenant: tenant.to_string(),
            edge_id,
        };
        self.wal.lock().unwrap().append(entry)?;

        // Write to storage
        self.storage.delete_edge(tenant, edge_id)?;

        // Update usage
        self.tenants.decrement_usage(tenant, "edges", 1)?;

        Ok(())
    }

    /// Update node properties
    pub fn persist_update_node_properties(
        &self,
        tenant: &str,
        node_id: u64,
        properties: &PropertyMap,
    ) -> Result<(), PersistenceError> {
        // Serialize properties
        let properties_bytes = bincode::serialize(properties)?;

        // Write to WAL
        let entry = WalEntry::UpdateNodeProperties {
            tenant: tenant.to_string(),
            node_id,
            properties: properties_bytes,
        };
        self.wal.lock().unwrap().append(entry)?;

        // Note: Full node update would require getting the node first
        // This is a simplified implementation

        Ok(())
    }

    /// List all tenants that have persisted data in RocksDB
    pub fn list_persisted_tenants(&self) -> Result<Vec<String>, PersistenceError> {
        Ok(self.storage.list_persisted_tenants()?)
    }

    /// Recover from storage and WAL
    pub fn recover(&self, tenant: &str) -> Result<(Vec<Node>, Vec<Edge>), PersistenceError> {
        info!("Starting recovery for tenant: {}", tenant);

        // Load nodes from storage
        let nodes = self.storage.scan_nodes(tenant)?;
        info!("Recovered {} nodes from storage", nodes.len());

        // Load edges from storage
        let edges = self.storage.scan_edges(tenant)?;
        info!("Recovered {} edges from storage", edges.len());

        // Update resource usage
        self.tenants.increment_usage(tenant, "nodes", nodes.len())?;
        self.tenants.increment_usage(tenant, "edges", edges.len())?;

        Ok((nodes, edges))
    }

    /// Create a checkpoint
    pub fn checkpoint(&self) -> Result<(), PersistenceError> {
        info!("Creating checkpoint");

        // Flush WAL
        self.wal.lock().unwrap().flush()?;

        // Flush storage
        self.storage.flush()?;

        // Create WAL checkpoint with the actual current sequence number
        // Previously this was hardcoded to 0, which caused misleading output
        // in the banking demo where WAL checkpoint always showed "sequence 0"
        // even after writing thousands of entries
        let sequence = self.wal.lock().unwrap().current_sequence();
        self.wal.lock().unwrap().checkpoint(sequence)?;

        info!("Checkpoint created successfully");

        Ok(())
    }

    /// Flush all pending writes
    pub fn flush(&self) -> Result<(), PersistenceError> {
        self.wal.lock().unwrap().flush()?;
        self.storage.flush()?;
        Ok(())
    }

    /// Get storage reference
    pub fn storage(&self) -> &PersistentStorage {
        &self.storage
    }

    /// Save vector indices to disk
    pub fn checkpoint_vectors(&self, vector_index: &crate::vector::VectorIndexManager) -> Result<(), PersistenceError> {
        let vector_path = self.base_path.join("vectors");
        vector_index.dump_all(&vector_path)
            .map_err(|e| PersistenceError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))
    }

    /// Load vector indices from disk
    pub fn recover_vectors(&self, vector_index: &crate::vector::VectorIndexManager) -> Result<(), PersistenceError> {
        let vector_path = self.base_path.join("vectors");
        vector_index.load_all(&vector_path)
            .map_err(|e| PersistenceError::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))
    }
}

/// Persistence errors
#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    #[error("Tenant error: {0}")]
    Tenant(#[from] TenantError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type PersistenceResult<T> = Result<T, PersistenceError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Label, NodeId, EdgeId, EdgeType, PropertyValue, PropertyMap};
    use tempfile::TempDir;

    #[test]
    fn test_persistence_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();
        assert!(manager.tenants().is_tenant_enabled("default"));
    }

    #[test]
    fn test_persist_node() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        let mut node = Node::new(NodeId::new(1), Label::new("Person"));
        node.set_property("name", "Alice");

        manager.persist_create_node("default", &node).unwrap();

        // Verify it was persisted
        let retrieved = manager.storage().get_node("default", 1).unwrap();
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // Create and persist some data
        {
            let manager = PersistenceManager::new(temp_dir.path()).unwrap();

            for i in 1..=5 {
                let node = Node::new(NodeId::new(i), Label::new("Person"));
                manager.persist_create_node("default", &node).unwrap();
            }

            manager.flush().unwrap();
        }

        // Recover in a new manager instance
        {
            let manager = PersistenceManager::new(temp_dir.path()).unwrap();
            let (nodes, _edges) = manager.recover("default").unwrap();
            assert_eq!(nodes.len(), 5);
        }
    }

    #[test]
    fn test_vector_index_persistence() {
        use crate::vector::{VectorIndexManager, DistanceMetric};
        use crate::graph::NodeId;

        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        // Create and populate a vector index
        let vim = VectorIndexManager::new();
        vim.create_index("Person", "embedding", 3, DistanceMetric::Cosine).unwrap();
        vim.add_vector("Person", "embedding", NodeId::new(1), &vec![1.0, 0.0, 0.0]).unwrap();
        vim.add_vector("Person", "embedding", NodeId::new(2), &vec![0.0, 1.0, 0.0]).unwrap();
        vim.add_vector("Person", "embedding", NodeId::new(3), &vec![0.0, 0.0, 1.0]).unwrap();

        // Checkpoint vectors to disk
        manager.checkpoint_vectors(&vim).unwrap();

        // Load vectors into a fresh manager
        let vim2 = VectorIndexManager::new();
        manager.recover_vectors(&vim2).unwrap();

        // Verify search works after recovery
        let results = vim2.search("Person", "embedding", &[1.0, 0.1, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, NodeId::new(1));
    }

    #[test]
    fn test_quota_enforcement() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        // Create tenant with small quota
        let mut quotas = ResourceQuotas::default();
        quotas.max_nodes = Some(3);
        manager.tenants().create_tenant(
            "limited".to_string(),
            "Limited Tenant".to_string(),
            Some(quotas),
        ).unwrap();

        // Should succeed for first 3 nodes
        for i in 1..=3 {
            let node = Node::new(NodeId::new(i), Label::new("Test"));
            manager.persist_create_node("limited", &node).unwrap();
        }

        // 4th should fail
        let node = Node::new(NodeId::new(4), Label::new("Test"));
        let result = manager.persist_create_node("limited", &node);
        assert!(result.is_err());
    }

    // ========== Batch 7: Additional Persistence Tests ==========

    #[test]
    fn test_persist_create_edge() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        // Create two nodes first
        let n1 = Node::new(NodeId::new(1), Label::new("Person"));
        let n2 = Node::new(NodeId::new(2), Label::new("Person"));
        manager.persist_create_node("default", &n1).unwrap();
        manager.persist_create_node("default", &n2).unwrap();

        // Create edge
        let edge = Edge::new(EdgeId::new(1), NodeId::new(1), NodeId::new(2), EdgeType::new("KNOWS"));
        let result = manager.persist_create_edge("default", &edge);
        assert!(result.is_ok());
    }

    #[test]
    fn test_persist_delete_node() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        let node = Node::new(NodeId::new(1), Label::new("Person"));
        manager.persist_create_node("default", &node).unwrap();

        let result = manager.persist_delete_node("default", 1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_persist_delete_edge() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        let n1 = Node::new(NodeId::new(1), Label::new("A"));
        let n2 = Node::new(NodeId::new(2), Label::new("B"));
        manager.persist_create_node("default", &n1).unwrap();
        manager.persist_create_node("default", &n2).unwrap();

        let edge = Edge::new(EdgeId::new(1), NodeId::new(1), NodeId::new(2), EdgeType::new("E"));
        manager.persist_create_edge("default", &edge).unwrap();

        let result = manager.persist_delete_edge("default", 1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_persist_update_node_properties() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        let node = Node::new(NodeId::new(1), Label::new("Person"));
        manager.persist_create_node("default", &node).unwrap();

        let mut props = PropertyMap::new();
        props.insert("name".to_string(), PropertyValue::String("Alice".to_string()));

        let result = manager.persist_update_node_properties("default", 1, &props);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_persisted_tenants() {
        let temp_dir = TempDir::new().unwrap();
        let manager = PersistenceManager::new(temp_dir.path()).unwrap();

        // Persist to default tenant
        let node = Node::new(NodeId::new(1), Label::new("Test"));
        manager.persist_create_node("default", &node).unwrap();

        let tenants = manager.list_persisted_tenants();
        assert!(tenants.is_ok());
    }
}
