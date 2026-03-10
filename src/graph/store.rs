//! # In-Memory Graph Storage -- Adjacency Lists, Indices, and Statistics
//!
//! [`GraphStore`] is the central data structure of the Samyama graph engine. It
//! holds all nodes, edges, indices, and metadata in memory, optimized for the
//! access patterns of a graph query engine.
//!
//! ## Why adjacency lists, not adjacency matrices?
//!
//! An adjacency matrix for V vertices requires O(V^2) space and O(V) time to
//! find a node's neighbors. Real-world graphs are overwhelmingly **sparse** --
//! a social network with 1 million users might have 100 million edges, but a
//! matrix would need 10^12 cells. Adjacency lists use O(V + E) space and give
//! O(degree) neighbor iteration -- a far better fit.
//!
//! ## Arena allocation with MVCC versioning
//!
//! Nodes and edges are stored in `Vec<Vec<T>>` structures (arena allocation).
//! The outer `Vec` is indexed by entity ID (acting as a dense array), and the
//! inner `Vec` holds successive **MVCC versions** of that entity. This layout
//! gives O(1) lookup by ID (direct indexing, no hash computation), excellent
//! cache locality for sequential scans, and natural support for snapshot reads
//! at any historical version. This is similar to how PostgreSQL stores tuple
//! versions in heap pages, but without the overhead of disk I/O.
//!
//! ## Sorted adjacency lists and `edge_between()`
//!
//! The `outgoing` and `incoming` adjacency lists store `Vec<(NodeId, EdgeId)>`
//! tuples **sorted by NodeId**. This enables `edge_between(a, b)` to use
//! **binary search** with O(log d) complexity (where d = degree of the node),
//! which is critical for the `ExpandIntoOperator` that checks edge existence
//! between two already-bound nodes in triangle-pattern queries.
//!
//! ## Secondary indices: label and edge-type
//!
//! `label_index: HashMap<Label, HashSet<NodeId>>` and
//! `edge_type_index: HashMap<EdgeType, HashSet<EdgeId>>` act as secondary
//! indices, analogous to B-tree indices in an RDBMS. When a Cypher query
//! specifies `MATCH (n:Person)`, the engine looks up the `Person` entry in
//! `label_index` and scans only matching nodes, avoiding a full table scan.
//! We use `HashMap` (not `BTreeMap`) because we only need equality lookups
//! on labels, not range scans.
//!
//! ## ColumnStore integration (late materialization)
//!
//! `node_columns` and `edge_columns` provide **columnar storage** for
//! frequently accessed properties. In the traditional row store (`PropertyMap`
//! on each node), reading one property from 1000 nodes touches 1000 scattered
//! `HashMap`s. The columnar store groups all values of the same property
//! contiguously, enabling vectorized reads and better CPU cache utilization.
//! The query engine uses **late materialization**: scan operators produce
//! lightweight `Value::NodeRef(id)` references instead of cloning full nodes,
//! and properties are resolved on demand from the column store.
//!
//! ## GraphStatistics for cost-based optimization
//!
//! The query planner uses [`GraphStatistics`] to estimate the cardinality
//! (number of rows) at each stage of a query plan, choosing the plan with
//! the lowest estimated cost. See the struct documentation for details.
//!
//! ## Key Rust patterns
//!
//! - **`Vec<Vec<T>>` arena**: dense ID-indexed storage avoids HashMap overhead;
//!   inner Vec holds MVCC versions.
//! - **`HashMap` vs `BTreeMap`**: `HashMap` is used for indices because we need
//!   O(1) point lookups (label equality), not ordered iteration. `BTreeMap`
//!   would add an unnecessary O(log n) factor.
//! - **`Arc`**: `vector_index` and `property_index` are wrapped in `Arc`
//!   (atomic reference counting) for shared ownership across the query engine
//!   and background index-maintenance tasks.
//! - **`thiserror`**: the [`GraphError`] enum uses the `thiserror` crate's
//!   derive macro to auto-generate `Display` and `Error` implementations,
//!   reducing boilerplate for error types.
//!
//! ## Requirements coverage
//!
//! - REQ-GRAPH-001: Property graph data model
//! - REQ-MEM-001: In-memory storage
//! - REQ-MEM-003: Memory-optimized data structures

use super::catalog::GraphCatalog;
use super::edge::Edge;
use super::node::Node;
use super::property::{PropertyMap, PropertyValue};
use super::types::{EdgeId, EdgeType, Label, NodeId};
use crate::vector::{VectorIndexManager, DistanceMetric, VectorResult};
use crate::index::IndexManager;
use crate::graph::storage::ColumnStore;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use crate::agent::{AgentRuntime, tools::WebSearchTool};

// Add chrono dependency (local hack like in node.rs)
mod chrono {
    pub struct Utc;
    impl Utc {
        pub fn now() -> DateTime {
            DateTime
        }
    }
    pub struct DateTime;
    impl DateTime {
        pub fn timestamp_millis(&self) -> i64 {
            // Use system time for now
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        }
    }
}

/// Errors that can occur during graph operations
#[derive(Error, Debug, PartialEq)]
pub enum GraphError {
    #[error("Node {0} not found")]
    NodeNotFound(NodeId),

    #[error("Edge {0} not found")]
    EdgeNotFound(EdgeId),

    #[error("Node {0} already exists")]
    NodeAlreadyExists(NodeId),

    #[error("Edge {0} already exists")]
    EdgeAlreadyExists(EdgeId),

    #[error("Invalid edge: source node {0} does not exist")]
    InvalidEdgeSource(NodeId),

    #[error("Invalid edge: target node {0} does not exist")]
    InvalidEdgeTarget(NodeId),
}

pub type GraphResult<T> = Result<T, GraphError>;

/// Statistics about graph contents for **cost-based query optimization**.
///
/// # What is cardinality estimation?
///
/// Every query plan is a tree of operators (scan, filter, join, sort, ...).
/// The query planner must choose among many possible orderings of these
/// operators. To compare plans, it estimates the **cardinality** (number of
/// rows) flowing through each operator. For example, if `:Person` has 10,000
/// nodes and `:Company` has 500, the planner should scan `:Company` first in
/// a join -- fewer rows means less work at every subsequent stage.
///
/// # How selectivity works
///
/// **Selectivity** is the probability that a predicate evaluates to `true`
/// for a randomly chosen row. A selectivity of 0.01 on a 10,000-row scan
/// means the filter will pass approximately 100 rows. Selectivity is
/// estimated as `1 / distinct_count` for equality predicates (assuming a
/// uniform distribution). The `null_fraction` is subtracted before
/// estimation because NULL values never match equality predicates.
///
/// # Sampling approach for property stats
///
/// Computing exact statistics for every property on every label would be
/// expensive in a large graph. Instead, `compute_statistics()` samples the
/// first 1,000 nodes per label and extrapolates `distinct_count`,
/// `null_fraction`, and `selectivity`. This is similar to PostgreSQL's
/// `ANALYZE` command, which samples a configurable fraction of each table.
/// The trade-off is speed vs. accuracy -- sampling may miss rare values,
/// but is sufficient for plan selection in practice.
#[derive(Debug, Clone)]
pub struct GraphStatistics {
    /// Total number of nodes
    pub total_nodes: usize,
    /// Total number of edges
    pub total_edges: usize,
    /// Node count per label
    pub label_counts: HashMap<Label, usize>,
    /// Edge count per type
    pub edge_type_counts: HashMap<EdgeType, usize>,
    /// Average outgoing degree
    pub avg_out_degree: f64,
    /// Property statistics per (label, property_name)
    pub property_stats: HashMap<(Label, String), PropertyStats>,
}

/// Statistics about a specific property
#[derive(Debug, Clone)]
pub struct PropertyStats {
    /// Fraction of nodes with this label that have NULL for this property
    pub null_fraction: f64,
    /// Estimated number of distinct values
    pub distinct_count: usize,
    /// Selectivity: probability of matching a random value (1/distinct_count)
    pub selectivity: f64,
}

impl GraphStatistics {
    /// Estimate the number of rows from a label scan
    pub fn estimate_label_scan(&self, label: &Label) -> usize {
        self.label_counts.get(label).copied().unwrap_or(self.total_nodes)
    }

    /// Estimate the number of rows after an expand (edge traversal)
    pub fn estimate_expand(&self, edge_type: Option<&EdgeType>) -> f64 {
        match edge_type {
            Some(et) => self.edge_type_counts.get(et).copied().unwrap_or(0) as f64,
            None => self.total_edges as f64,
        }
    }

    /// Estimate selectivity of an equality filter on a property
    pub fn estimate_equality_selectivity(&self, label: &Label, property: &str) -> f64 {
        self.property_stats
            .get(&(label.clone(), property.to_string()))
            .map(|ps| ps.selectivity)
            .unwrap_or(0.1) // Default 10% selectivity
    }

    /// Format statistics as human-readable text
    pub fn format(&self) -> String {
        let mut result = String::new();
        result.push_str(&format!("Graph Statistics:\n"));
        result.push_str(&format!("  Total nodes: {}\n", self.total_nodes));
        result.push_str(&format!("  Total edges: {}\n", self.total_edges));
        result.push_str(&format!("  Avg out-degree: {:.2}\n", self.avg_out_degree));
        result.push_str(&format!("  Labels:\n"));
        let mut labels: Vec<_> = self.label_counts.iter().collect();
        labels.sort_by(|a, b| b.1.cmp(a.1));
        for (label, count) in labels {
            result.push_str(&format!("    :{} = {} nodes\n", label.as_str(), count));
        }
        result.push_str(&format!("  Edge types:\n"));
        let mut types: Vec<_> = self.edge_type_counts.iter().collect();
        types.sort_by(|a, b| b.1.cmp(a.1));
        for (etype, count) in types {
            result.push_str(&format!("    :{} = {} edges\n", etype.as_str(), count));
        }
        result
    }
}

/// In-memory graph storage engine.
///
/// `GraphStore` is the authoritative source of truth for all graph data.
/// Its data structure layout is designed for the access patterns of a
/// Cypher query engine:
///
/// | Field | Type | Purpose | Lookup cost |
/// |---|---|---|---|
/// | `nodes` | `Vec<Vec<Node>>` | Arena-allocated node versions, indexed by `NodeId` | O(1) |
/// | `edges` | `Vec<Vec<Edge>>` | Arena-allocated edge versions, indexed by `EdgeId` | O(1) |
/// | `outgoing` | `Vec<Vec<(NodeId, EdgeId)>>` | Sorted adjacency list per node (outgoing) | O(log d) binary search |
/// | `incoming` | `Vec<Vec<(NodeId, EdgeId)>>` | Sorted adjacency list per node (incoming) | O(log d) binary search |
/// | `label_index` | `HashMap<Label, HashSet<NodeId>>` | Secondary index: label -> node set | O(1) lookup, O(n) scan |
/// | `edge_type_index` | `HashMap<EdgeType, HashSet<EdgeId>>` | Secondary index: type -> edge set | O(1) lookup, O(n) scan |
/// | `node_columns` | `ColumnStore` | Columnar property storage for late materialization | O(1) per cell |
/// | `catalog` | `GraphCatalog` | Triple-level statistics for graph-native planning (ADR-015) | O(1) |
///
/// The `free_node_ids` / `free_edge_ids` vectors enable **ID reuse** after
/// deletions, avoiding unbounded growth of the arena vectors.
///
/// Thread safety: `GraphStore` is not `Sync` by itself. Concurrent access
/// is managed by the server layer, which wraps it in `Arc<RwLock<GraphStore>>`
/// for shared-nothing read parallelism with exclusive write access.
#[derive(Debug)]
pub struct GraphStore {
    /// Node storage (Arena with versioning: NodeId -> [Versions])
    nodes: Vec<Vec<Node>>,

    /// Edge storage (Arena with versioning: EdgeId -> [Versions])
    edges: Vec<Vec<Edge>>,

    /// Outgoing edges for each node, sorted by target NodeId: (target_NodeId, EdgeId)
    outgoing: Vec<Vec<(NodeId, EdgeId)>>,

    /// Incoming edges for each node, sorted by source NodeId: (source_NodeId, EdgeId)
    incoming: Vec<Vec<(NodeId, EdgeId)>>,

    /// Current global version for MVCC
    pub current_version: u64,

    /// Free node IDs for reuse
    free_node_ids: Vec<u64>,

    /// Free edge IDs for reuse
    free_edge_ids: Vec<u64>,

    /// Label index for fast lookups
    label_index: HashMap<Label, HashSet<NodeId>>,

    /// Edge type index for fast lookups
    edge_type_index: HashMap<EdgeType, HashSet<EdgeId>>,

    /// Vector indices manager
    pub vector_index: Arc<VectorIndexManager>,

    /// Property indices manager
    pub property_index: Arc<IndexManager>,

    /// Columnar storage for node properties
    pub node_columns: ColumnStore,

    /// Columnar storage for edge properties
    pub edge_columns: ColumnStore,

    /// Async index event sender
    pub index_sender: Option<UnboundedSender<crate::graph::event::IndexEvent>>,

    /// Next node ID
    next_node_id: u64,

    /// Next edge ID
    next_edge_id: u64,

    /// Triple-level statistics catalog for graph-native query planning (ADR-015)
    catalog: GraphCatalog,
}

impl GraphStore {
    /// Create a new empty graph store
    pub fn new() -> Self {
        GraphStore {
            nodes: Vec::with_capacity(1024),
            edges: Vec::with_capacity(4096),
            outgoing: Vec::with_capacity(1024),
            incoming: Vec::with_capacity(1024),
            current_version: 1,
            free_node_ids: Vec::new(),
            free_edge_ids: Vec::new(),
            label_index: HashMap::new(),
            edge_type_index: HashMap::new(),
            vector_index: Arc::new(VectorIndexManager::new()),
            property_index: Arc::new(IndexManager::new()),
            node_columns: ColumnStore::new(),
            edge_columns: ColumnStore::new(),
            index_sender: None,
            next_node_id: 1,
            next_edge_id: 1,
            catalog: GraphCatalog::new(),
        }
    }

    /// Create a new GraphStore with async indexing enabled
    pub fn with_async_indexing() -> (Self, tokio::sync::mpsc::UnboundedReceiver<crate::graph::event::IndexEvent>) {
        let (tx, rx) = unbounded_channel();
        let mut store = Self::new();
        store.index_sender = Some(tx);
        (store, rx)
    }

    /// Start the background indexer loop
    pub async fn start_background_indexer(
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<crate::graph::event::IndexEvent>,
        vector_index: Arc<VectorIndexManager>,
        property_index: Arc<IndexManager>,
        tenant_manager: Arc<crate::persistence::TenantManager>,
    ) {
        use crate::graph::event::IndexEvent::*;
        
        while let Some(event) = receiver.recv().await {
            match event {
                NodeCreated { tenant_id, id, labels, properties } => {
                    for (key, value) in &properties {
                        if let PropertyValue::Vector(vec) = value {
                            for label in &labels {
                                let _ = vector_index.add_vector(label.as_str(), key, id, vec);
                            }
                        }
                        for label in &labels {
                            property_index.index_insert(label, key, value.clone(), id);
                        }
                        
                        // Auto-Embed check
                        if let PropertyValue::String(text) = value {
                            if let Ok(tenant) = tenant_manager.get_tenant(&tenant_id) {
                                if let Some(config) = tenant.embed_config {
                                    for label in &labels {
                                        if let Some(keys) = config.embedding_policies.get(label.as_str()) {
                                            if keys.contains(key) {
                                                // Trigger Auto-Embed
                                                let vector_index_clone = Arc::clone(&vector_index);
                                                let label_str = label.as_str().to_string();
                                                let key_clone = key.clone();
                                                let text_clone = text.clone();
                                                let config_clone = config.clone();
                                                
                                                tokio::spawn(async move {
                                                    if let Ok(pipeline) = crate::embed::EmbedPipeline::new(config_clone) {
                                                        if let Ok(chunks) = pipeline.process_text(&text_clone).await {
                                                            for chunk in chunks {
                                                                let _ = vector_index_clone.add_vector(&label_str, &key_clone, id, &chunk.embedding);
                                                            }
                                                        }
                                                    }
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Agentic Enrichment Trigger
                    if let Ok(tenant) = tenant_manager.get_tenant(&tenant_id) {
                        if let Some(agent_config) = tenant.agent_config {
                            if agent_config.enabled {
                                for label in &labels {
                                    if let Some(trigger_prompt) = agent_config.policies.get(label.as_str()) {
                                        // Construct context from node properties
                                        let context = format!("Node: {} {:?}", label.as_str(), properties);
                                        let task = trigger_prompt.clone();
                                        let mut runtime = AgentRuntime::new(agent_config.clone());
                                        let label_str = label.as_str().to_string();
                                        
                                        // Register tools based on config/availability
                                        if let Some(api_key) = &agent_config.api_key {
                                            runtime.register_tool(Arc::new(WebSearchTool::new(api_key.clone())));
                                        } else {
                                            // Mock/Prototype mode
                                            runtime.register_tool(Arc::new(WebSearchTool::new("mock".to_string())));
                                        }

                                        tokio::spawn(async move {
                                            if let Ok(result) = runtime.process_trigger(&task, &context).await {
                                                println!("Agent Action [{}] -> {}", label_str, result);
                                                // Future: Write result back to graph properties using a GraphStore client
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
NodeDeleted { tenant_id: _, id, labels, properties } => {
                    for (key, value) in properties {
                        for label in &labels {
                            property_index.index_remove(label, &key, &value, id);
                        }
                    }
                }
                PropertySet { tenant_id, id, labels, key, old_value, new_value } => {
                    if let Some(old) = old_value {
                        for label in &labels {
                            property_index.index_remove(label, &key, &old, id);
                        }
                    }
                    for label in &labels {
                        property_index.index_insert(label, &key, new_value.clone(), id);
                    }
                    if let PropertyValue::Vector(vec) = &new_value {
                        for label in &labels {
                            let _ = vector_index.add_vector(label.as_str(), &key, id, vec);
                        }
                    }
                    
                    // Auto-Embed check
                    if let PropertyValue::String(text) = &new_value {
                        if let Ok(tenant) = tenant_manager.get_tenant(&tenant_id) {
                            if let Some(config) = tenant.embed_config {
                                for label in &labels {
                                    if let Some(keys) = config.embedding_policies.get(label.as_str()) {
                                        if keys.contains(&key) {
                                            let vector_index_clone = Arc::clone(&vector_index);
                                            let label_str = label.as_str().to_string();
                                            let key_clone = key.clone();
                                            let text_clone = text.clone();
                                            let config_clone = config.clone();
                                            
                                            tokio::spawn(async move {
                                                if let Ok(pipeline) = crate::embed::EmbedPipeline::new(config_clone) {
                                                    if let Ok(chunks) = pipeline.process_text(&text_clone).await {
                                                        if let Some(first) = chunks.first() {
                                                            let _ = vector_index_clone.add_vector(&label_str, &key_clone, id, &first.embedding);
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Agentic Enrichment Trigger (PropertySet)
                    if let Ok(tenant) = tenant_manager.get_tenant(&tenant_id) {
                        if let Some(agent_config) = tenant.agent_config {
                            if agent_config.enabled {
                                for label in &labels {
                                    if let Some(trigger_prompt) = agent_config.policies.get(label.as_str()) {
                                        let context = format!("Node: {} (Property Updated: {})", label.as_str(), key);
                                        let task = trigger_prompt.clone();
                                        let mut runtime = AgentRuntime::new(agent_config.clone());
                                        let label_str = label.as_str().to_string();
                                        
                                        if let Some(api_key) = &agent_config.api_key {
                                            runtime.register_tool(Arc::new(WebSearchTool::new(api_key.clone())));
                                        } else {
                                            runtime.register_tool(Arc::new(WebSearchTool::new("mock".to_string())));
                                        }

                                        tokio::spawn(async move {
                                            if let Ok(result) = runtime.process_trigger(&task, &context).await {
                                                println!("Agent Action [{}] -> {}", label_str, result);
                                            }
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
                LabelAdded { tenant_id, id, label, properties } => {
                    for (key, value) in properties {
                        if let PropertyValue::Vector(vec) = &value {
                            let _ = vector_index.add_vector(label.as_str(), &key, id, vec);
                        }
                        property_index.index_insert(&label, &key, value.clone(), id);
                        
                        // Auto-Embed check
                        if let PropertyValue::String(text) = &value {
                            if let Ok(tenant) = tenant_manager.get_tenant(&tenant_id) {
                                if let Some(config) = tenant.embed_config {
                                    if let Some(keys) = config.embedding_policies.get(label.as_str()) {
                                        if keys.contains(&key) {
                                            let vector_index_clone = Arc::clone(&vector_index);
                                            let label_str = label.as_str().to_string();
                                            let key_clone = key.clone();
                                            let text_clone = text.clone();
                                            let config_clone = config.clone();
                                            
                                            tokio::spawn(async move {
                                                if let Ok(pipeline) = crate::embed::EmbedPipeline::new(config_clone) {
                                                    if let Ok(chunks) = pipeline.process_text(&text_clone).await {
                                                        if let Some(first) = chunks.first() {
                                                            let _ = vector_index_clone.add_vector(&label_str, &key_clone, id, &first.embedding);
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Create a node with auto-generated ID and single label
    pub fn create_node(&mut self, label: impl Into<Label>) -> NodeId {
        let node_id_u64 = if let Some(id) = self.free_node_ids.pop() {
            id
        } else {
            let id = self.next_node_id;
            self.next_node_id += 1;
            id
        };
        let node_id = NodeId::new(node_id_u64);
        let idx = node_id_u64 as usize;

        let label = label.into();
        let mut node = Node::new(node_id, label.clone());
        node.version = self.current_version;

        // Add to label index
        self.label_index
            .entry(label.clone())
            .or_insert_with(HashSet::new)
            .insert(node_id);

        // Update catalog label count
        self.catalog.on_label_added(&label);

        // Ensure storage capacity
        if idx >= self.nodes.len() {
            self.nodes.resize(idx + 1, Vec::new());
            self.outgoing.resize(idx + 1, Vec::new());
            self.incoming.resize(idx + 1, Vec::new());
        }

        let event = crate::graph::event::IndexEvent::NodeCreated {
            tenant_id: "default".to_string(),
            id: node_id,
            labels: node.labels.iter().cloned().collect(),
            properties: node.properties.clone(),
        };

        if let Some(sender) = &self.index_sender {
            let _ = sender.send(event);
        } else {
            self.handle_index_event(event, None);
        }

        self.nodes[idx].push(node);
        node_id
    }

    /// Create a node with multiple labels and properties
    pub fn create_node_with_properties(
        &mut self,
        tenant_id: &str,
        labels: Vec<Label>,
        properties: PropertyMap,
    ) -> NodeId {
        let node_id_u64 = if let Some(id) = self.free_node_ids.pop() {
            id
        } else {
            let id = self.next_node_id;
            self.next_node_id += 1;
            id
        };
        let node_id = NodeId::new(node_id_u64);
        let idx = node_id_u64 as usize;

        // Populate columnar storage
        for (key, value) in &properties {
            self.node_columns.set_property(idx, key, value.clone());
        }

        let mut node = Node::new_with_properties(node_id, labels.clone(), properties);
        node.version = self.current_version;

        // Add to label indices
        for label in &labels {
            self.label_index
                .entry(label.clone())
                .or_insert_with(HashSet::new)
                .insert(node_id);
            // Update catalog label count
            self.catalog.on_label_added(label);
        }

        // Ensure storage capacity
        if idx >= self.nodes.len() {
            self.nodes.resize(idx + 1, Vec::new());
            self.outgoing.resize(idx + 1, Vec::new());
            self.incoming.resize(idx + 1, Vec::new());
        }

        let event = crate::graph::event::IndexEvent::NodeCreated {
            tenant_id: tenant_id.to_string(),
            id: node_id,
            labels: node.labels.iter().cloned().collect(),
            properties: node.properties.clone(),
        };

        if let Some(sender) = &self.index_sender {
            let _ = sender.send(event);
        } else {
            self.handle_index_event(event, None);
        }

        self.nodes[idx].push(node);
        node_id
    }

    /// Get a node by ID at a specific version (MVCC)
    pub fn get_node_at_version(&self, id: NodeId, version: u64) -> Option<&Node> {
        let idx = id.as_u64() as usize;
        let versions = self.nodes.get(idx)?;
        
        // Find the latest version <= requested version
        // Versions are sorted by creation time
        versions.iter()
            .rev()
            .find(|n| n.version <= version)
    }

    /// Get a node by ID (uses current version)
    pub fn get_node(&self, id: NodeId) -> Option<&Node> {
        self.get_node_at_version(id, self.current_version)
    }

    /// Get a mutable node by ID (always latest version)
    pub fn get_node_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(id.as_u64() as usize).and_then(|v| v.last_mut())
    }

    /// Check if a node exists
    pub fn has_node(&self, id: NodeId) -> bool {
        self.get_node(id).is_some()
    }

    /// Set a property on a node and update vector indices if necessary
    pub fn set_node_property(
        &mut self,
        tenant_id: &str,
        node_id: NodeId,
        key: impl Into<String>,
        value: impl Into<PropertyValue>,
    ) -> GraphResult<()> {
        let key_str = key.into();
        let val = value.into();
        let idx = node_id.as_u64() as usize;

        // Update columnar storage (always latest)
        self.node_columns.set_property(idx, &key_str, val.clone());

        // Get access to versions
        let versions = self.nodes.get_mut(idx).ok_or(GraphError::NodeNotFound(node_id))?;
        let latest_node = versions.last().ok_or(GraphError::NodeNotFound(node_id))?;

        let old_val;
        
        if latest_node.version < self.current_version {
            // COW: Create new version
            let mut new_node = latest_node.clone();
            new_node.version = self.current_version;
            new_node.updated_at = chrono::Utc::now().timestamp_millis();
            old_val = new_node.set_property(key_str.clone(), val.clone());
            versions.push(new_node);
        } else {
            // Update in place (same transaction/version)
            let node = versions.last_mut().unwrap();
            old_val = node.set_property(key_str.clone(), val.clone());
        }

        let event = crate::graph::event::IndexEvent::PropertySet {
            tenant_id: tenant_id.to_string(),
            id: node_id,
            labels: versions.last().unwrap().labels.iter().cloned().collect(),
            key: key_str,
            old_value: old_val,
            new_value: val,
        };

        if let Some(sender) = &self.index_sender {
            let _ = sender.send(event);
        } else {
            self.handle_index_event(event, None);
        }

        Ok(())
    }

    /// Delete a node and all its connected edges
    pub fn delete_node(
        &mut self,
        tenant_id: &str,
        id: NodeId
    ) -> GraphResult<Node> {
        let idx = id.as_u64() as usize;
        let latest_node = self.get_node(id).ok_or(GraphError::NodeNotFound(id))?.clone();

        // Create a tombstone version (we use a special property or metadata in a real system)
        // For now, we'll just not return it in get_node_at_version if we had a flag.
        // Let's add a `deleted` flag to Node/Edge for true MVCC.
        
        // Add to free list for reuse (In true MVCC, we only reuse after compaction/vacuum)
        self.free_node_ids.push(id.as_u64());

        // For this prototype, we'll keep the removal logic but wrap it in MVCC metadata if needed.
        // Actually, let's keep it simple: removal from the latest version effectively deletes it.
        // But to keep history, we should NOT remove from the Vec.
        
        // Remove from label indices and update catalog
        for label in &latest_node.labels {
            if let Some(node_set) = self.label_index.get_mut(label) {
                node_set.remove(&id);
            }
            self.catalog.on_label_removed(label);
        }

        let event = crate::graph::event::IndexEvent::NodeDeleted {
            tenant_id: tenant_id.to_string(),
            id,
            labels: latest_node.labels.iter().cloned().collect(),
            properties: latest_node.properties.clone(),
        };

        if let Some(sender) = &self.index_sender {
            let _ = sender.send(event);
        } else {
            self.handle_index_event(event, None);
        }

        // Remove from the versions (breaking historical reads for now, full MVCC is complex)
        // TODO: Implement proper tombstone versions
        let node = self.nodes[idx].pop().unwrap();

        // Remove all connected edges
        let outgoing_edges: Vec<EdgeId> = std::mem::take(&mut self.outgoing[idx])
            .into_iter().map(|(_, eid)| eid).collect();
        let incoming_edges: Vec<EdgeId> = std::mem::take(&mut self.incoming[idx])
            .into_iter().map(|(_, eid)| eid).collect();

        for edge_id in outgoing_edges.iter().chain(incoming_edges.iter()) {
            let _ = self.delete_edge(*edge_id);
        }

        Ok(node)
    }

    /// Add a label to an existing node AND update the label index
    ///
    /// This is the correct way to add labels to nodes after creation.
    /// Using `node.add_label()` directly will NOT update the label_index,
    /// making the node invisible to `get_nodes_by_label()` queries.
    pub fn add_label_to_node(
        &mut self,
        tenant_id: &str,
        node_id: NodeId,
        label: impl Into<Label>
    ) -> GraphResult<()> {
        let label = label.into();
        let idx = node_id.as_u64() as usize;

        // Get the node and add the label
        let node = self.nodes.get_mut(idx).and_then(|v| v.last_mut()).ok_or(GraphError::NodeNotFound(node_id))?;
        node.add_label(label.clone());

        // Update the label index so queries can find this node by the new label
        self.label_index
            .entry(label.clone())
            .or_insert_with(HashSet::new)
            .insert(node_id);

        // Update catalog label count
        self.catalog.on_label_added(&label);

        let event = crate::graph::event::IndexEvent::LabelAdded {
            tenant_id: tenant_id.to_string(),
            id: node_id,
            label: label.clone(),
            properties: node.properties.clone(),
        };

        if let Some(sender) = &self.index_sender {
            let _ = sender.send(event);
        } else {
            self.handle_index_event(event, None);
        }

        Ok(())
    }

    /// Create an edge between two nodes
    pub fn create_edge(
        &mut self,
        source: NodeId,
        target: NodeId,
        edge_type: impl Into<EdgeType>,
    ) -> GraphResult<EdgeId> {
        // Validate nodes exist
        if !self.has_node(source) {
            return Err(GraphError::InvalidEdgeSource(source));
        }
        if !self.has_node(target) {
            return Err(GraphError::InvalidEdgeTarget(target));
        }

        let edge_id_u64 = if let Some(id) = self.free_edge_ids.pop() {
            id
        } else {
            let id = self.next_edge_id;
            self.next_edge_id += 1;
            id
        };
        let edge_id = EdgeId::new(edge_id_u64);
        let idx = edge_id_u64 as usize;

        let edge_type = edge_type.into();
        let mut edge = Edge::new(edge_id, source, target, edge_type.clone());
        edge.version = self.current_version;

        // Update adjacency lists (sorted insert by target/source NodeId)
        {
            let out_list = &mut self.outgoing[source.as_u64() as usize];
            let pos = out_list.binary_search_by_key(&target, |(nid, _)| *nid)
                .unwrap_or_else(|p| p);
            out_list.insert(pos, (target, edge_id));
        }
        {
            let in_list = &mut self.incoming[target.as_u64() as usize];
            let pos = in_list.binary_search_by_key(&source, |(nid, _)| *nid)
                .unwrap_or_else(|p| p);
            in_list.insert(pos, (source, edge_id));
        }

        // Ensure storage capacity
        if idx >= self.edges.len() {
            self.edges.resize(idx + 1, Vec::new());
        }

        // Update edge type index
        self.edge_type_index
            .entry(edge_type.clone())
            .or_insert_with(HashSet::new)
            .insert(edge_id);

        // Update catalog triple stats
        let src_labels: Vec<Label> = self.get_node(source).map(|n| n.labels.iter().cloned().collect()).unwrap_or_default();
        let tgt_labels: Vec<Label> = self.get_node(target).map(|n| n.labels.iter().cloned().collect()).unwrap_or_default();
        self.catalog.on_edge_created(source, &src_labels, &edge_type, target, &tgt_labels);

        self.edges[idx].push(edge);
        Ok(edge_id)
    }

    /// Create an edge with properties
    pub fn create_edge_with_properties(
        &mut self,
        source: NodeId,
        target: NodeId,
        edge_type: impl Into<EdgeType>,
        properties: PropertyMap,
    ) -> GraphResult<EdgeId> {
        // Validate nodes exist
        if !self.has_node(source) {
            return Err(GraphError::InvalidEdgeSource(source));
        }
        if !self.has_node(target) {
            return Err(GraphError::InvalidEdgeTarget(target));
        }

        let edge_id_u64 = if let Some(id) = self.free_edge_ids.pop() {
            id
        } else {
            let id = self.next_edge_id;
            self.next_edge_id += 1;
            id
        };
        let edge_id = EdgeId::new(edge_id_u64);
        let idx = edge_id_u64 as usize;

        // Populate columnar storage
        for (key, value) in &properties {
            self.edge_columns.set_property(idx, key, value.clone());
        }

        let edge_type = edge_type.into();
        let mut edge = Edge::new_with_properties(edge_id, source, target, edge_type.clone(), properties);
        edge.version = self.current_version;

        // Update adjacency lists (sorted insert by target/source NodeId)
        {
            let out_list = &mut self.outgoing[source.as_u64() as usize];
            let pos = out_list.binary_search_by_key(&target, |(nid, _)| *nid)
                .unwrap_or_else(|p| p);
            out_list.insert(pos, (target, edge_id));
        }
        {
            let in_list = &mut self.incoming[target.as_u64() as usize];
            let pos = in_list.binary_search_by_key(&source, |(nid, _)| *nid)
                .unwrap_or_else(|p| p);
            in_list.insert(pos, (source, edge_id));
        }

        // Ensure storage capacity
        if idx >= self.edges.len() {
            self.edges.resize(idx + 1, Vec::new());
        }

        // Update edge type index
        self.edge_type_index
            .entry(edge_type.clone())
            .or_insert_with(HashSet::new)
            .insert(edge_id);

        // Update catalog triple stats
        let src_labels: Vec<Label> = self.get_node(source).map(|n| n.labels.iter().cloned().collect()).unwrap_or_default();
        let tgt_labels: Vec<Label> = self.get_node(target).map(|n| n.labels.iter().cloned().collect()).unwrap_or_default();
        self.catalog.on_edge_created(source, &src_labels, &edge_type, target, &tgt_labels);

        self.edges[idx].push(edge);
        Ok(edge_id)
    }

    /// Get an edge by ID at a specific version (MVCC)
    pub fn get_edge_at_version(&self, id: EdgeId, version: u64) -> Option<&Edge> {
        let idx = id.as_u64() as usize;
        let versions = self.edges.get(idx)?;
        
        // Find the latest version <= requested version
        versions.iter()
            .rev()
            .find(|e| e.version <= version)
    }

    /// Get an edge by ID (uses current version)
    pub fn get_edge(&self, id: EdgeId) -> Option<&Edge> {
        self.get_edge_at_version(id, self.current_version)
    }

    /// Get a mutable edge by ID (always latest version)
    pub fn get_edge_mut(&mut self, id: EdgeId) -> Option<&mut Edge> {
        self.edges.get_mut(id.as_u64() as usize).and_then(|v| v.last_mut())
    }

    /// Check if an edge exists
    pub fn has_edge(&self, id: EdgeId) -> bool {
        self.get_edge(id).is_some()
    }

    /// Delete an edge
    pub fn delete_edge(&mut self, id: EdgeId) -> GraphResult<Edge> {
        let idx = id.as_u64() as usize;

        // Collect catalog info before removal
        let (src_labels, tgt_labels, source_id, target_id, edge_type_clone) = {
            let edge = self.edges.get(idx).and_then(|v| v.last()).ok_or(GraphError::EdgeNotFound(id))?;
            let src_labels: Vec<Label> = self.get_node(edge.source).map(|n| n.labels.iter().cloned().collect()).unwrap_or_default();
            let tgt_labels: Vec<Label> = self.get_node(edge.target).map(|n| n.labels.iter().cloned().collect()).unwrap_or_default();
            (src_labels, tgt_labels, edge.source, edge.target, edge.edge_type.clone())
        };

        let edge = self.edges.get_mut(idx).and_then(|v| v.pop()).ok_or(GraphError::EdgeNotFound(id))?;

        // Add to free list
        self.free_edge_ids.push(id.as_u64());

        // Remove from edge type index
        if let Some(edge_set) = self.edge_type_index.get_mut(&edge.edge_type) {
            edge_set.remove(&id);
        }

        // Remove from adjacency lists
        if let Some(adj) = self.outgoing.get_mut(edge.source.as_u64() as usize) {
            adj.retain(|&(_, eid)| eid != id);
        }
        if let Some(adj) = self.incoming.get_mut(edge.target.as_u64() as usize) {
            adj.retain(|&(_, eid)| eid != id);
        }

        // Update catalog triple stats
        self.catalog.on_edge_deleted(source_id, &src_labels, &edge_type_clone, target_id, &tgt_labels);

        Ok(edge)
    }

    /// Get all outgoing edges from a node
    pub fn get_outgoing_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.outgoing
            .get(node_id.as_u64() as usize)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|&(_, eid)| self.get_edge(eid))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all incoming edges to a node
    pub fn get_incoming_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.incoming
            .get(node_id.as_u64() as usize)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|&(_, eid)| self.get_edge(eid))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get outgoing edge targets as lightweight tuples (no Edge clone)
    /// Returns (EdgeId, source NodeId, target NodeId, &EdgeType) for each outgoing edge
    pub fn get_outgoing_edge_targets(&self, node_id: NodeId) -> Vec<(EdgeId, NodeId, NodeId, &EdgeType)> {
        self.outgoing
            .get(node_id.as_u64() as usize)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|&(_, eid)| {
                        self.get_edge(eid).map(|e| (eid, e.source, e.target, &e.edge_type))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get incoming edge sources as lightweight tuples (no Edge clone)
    /// Returns (EdgeId, source NodeId, target NodeId, &EdgeType) for each incoming edge
    pub fn get_incoming_edge_sources(&self, node_id: NodeId) -> Vec<(EdgeId, NodeId, NodeId, &EdgeType)> {
        self.incoming
            .get(node_id.as_u64() as usize)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|&(_, eid)| {
                        self.get_edge(eid).map(|e| (eid, e.source, e.target, &e.edge_type))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Check if an edge exists between source and target, optionally filtered by edge type.
    /// Uses binary search on sorted adjacency lists for O(log d) lookup.
    /// Returns the first matching EdgeId, or None.
    pub fn edge_between(&self, source: NodeId, target: NodeId, edge_type: Option<&EdgeType>) -> Option<EdgeId> {
        let out_len = self.outgoing.get(source.as_u64() as usize).map(|v| v.len()).unwrap_or(0);
        let in_len = self.incoming.get(target.as_u64() as usize).map(|v| v.len()).unwrap_or(0);

        // Use outgoing from source (search for target) or incoming to target (search for source)
        let (entries, search_key) = if out_len <= in_len {
            (self.outgoing.get(source.as_u64() as usize)?, target)
        } else {
            (self.incoming.get(target.as_u64() as usize)?, source)
        };

        // Binary search to find the neighborhood of matching entries
        let start = match entries.binary_search_by_key(&search_key, |(nid, _)| *nid) {
            Ok(pos) => {
                // Walk back to find the first entry with this key
                let mut p = pos;
                while p > 0 && entries[p - 1].0 == search_key { p -= 1; }
                p
            }
            Err(_) => return None,
        };

        // Scan forward through entries with the matching key
        for i in start..entries.len() {
            let (nid, eid) = entries[i];
            if nid != search_key { break; }
            if let Some(e) = self.get_edge(eid) {
                if e.source == source && e.target == target {
                    match edge_type {
                        Some(et) if &e.edge_type != et => continue,
                        _ => return Some(eid),
                    }
                }
            }
        }
        None
    }

    /// Get all edges between source and target, optionally filtered by edge type.
    /// Uses binary search on sorted adjacency lists.
    pub fn edges_between(&self, source: NodeId, target: NodeId, edge_type: Option<&EdgeType>) -> Vec<EdgeId> {
        let out_len = self.outgoing.get(source.as_u64() as usize).map(|v| v.len()).unwrap_or(0);
        let in_len = self.incoming.get(target.as_u64() as usize).map(|v| v.len()).unwrap_or(0);

        let (entries, search_key) = if out_len <= in_len {
            match self.outgoing.get(source.as_u64() as usize) {
                Some(e) => (e, target),
                None => return Vec::new(),
            }
        } else {
            match self.incoming.get(target.as_u64() as usize) {
                Some(e) => (e, source),
                None => return Vec::new(),
            }
        };

        let start = match entries.binary_search_by_key(&search_key, |(nid, _)| *nid) {
            Ok(pos) => {
                let mut p = pos;
                while p > 0 && entries[p - 1].0 == search_key { p -= 1; }
                p
            }
            Err(_) => return Vec::new(),
        };

        let mut result = Vec::new();
        for i in start..entries.len() {
            let (nid, eid) = entries[i];
            if nid != search_key { break; }
            if let Some(e) = self.get_edge(eid) {
                if e.source == source && e.target == target {
                    match edge_type {
                        Some(et) if &e.edge_type != et => {}
                        _ => result.push(eid),
                    }
                }
            }
        }
        result
    }

    /// Get all nodes with a specific label
    pub fn get_nodes_by_label(&self, label: &Label) -> Vec<&Node> {
        self.label_index
            .get(label)
            .map(|node_ids| {
                node_ids
                    .iter()
                    .filter_map(|&id| self.get_node(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all edges of a specific type
    pub fn get_edges_by_type(&self, edge_type: &EdgeType) -> Vec<&Edge> {
        self.edge_type_index
            .get(edge_type)
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .filter_map(|&id| self.get_edge(id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get total number of nodes
    pub fn node_count(&self) -> usize {
        self.nodes.iter().flatten().count()
    }

    /// Get total number of edges
    pub fn edge_count(&self) -> usize {
        self.edges.iter().flatten().count()
    }

    /// Get all nodes in the graph
    pub fn all_nodes(&self) -> Vec<&Node> {
        self.nodes.iter().flatten().collect()
    }

    // ============================================================
    // Graph Statistics (for cost-based query optimization)
    // ============================================================

    /// Compute statistics for the current graph state
    pub fn compute_statistics(&self) -> GraphStatistics {
        let total_nodes = self.node_count();
        let total_edges = self.edge_count();

        let mut label_counts = HashMap::new();
        for (label, node_ids) in &self.label_index {
            label_counts.insert(label.clone(), node_ids.len());
        }

        let mut edge_type_counts = HashMap::new();
        for (edge_type, edge_ids) in &self.edge_type_index {
            edge_type_counts.insert(edge_type.clone(), edge_ids.len());
        }

        // Compute average degree
        let avg_out_degree = if total_nodes > 0 {
            total_edges as f64 / total_nodes as f64
        } else {
            0.0
        };

        // Sample property selectivity for common properties
        let mut property_stats: HashMap<(Label, String), PropertyStats> = HashMap::new();
        for (label, node_ids) in &self.label_index {
            let sample_size = node_ids.len().min(1000);
            let mut property_presence: HashMap<String, usize> = HashMap::new();
            let mut property_distinct: HashMap<String, HashSet<u64>> = HashMap::new();

            for (i, &node_id) in node_ids.iter().enumerate() {
                if i >= sample_size { break; }
                if let Some(node) = self.get_node(node_id) {
                    for (key, val) in &node.properties {
                        *property_presence.entry(key.clone()).or_insert(0) += 1;

                        let hash = {
                            use std::hash::{Hash, Hasher};
                            let mut hasher = std::collections::hash_map::DefaultHasher::new();
                            val.hash(&mut hasher);
                            hasher.finish()
                        };
                        property_distinct.entry(key.clone()).or_default().insert(hash);
                    }
                }
            }

            for (prop, count) in &property_presence {
                let distinct = property_distinct.get(prop).map(|s| s.len()).unwrap_or(0);
                let selectivity = if distinct > 0 { 1.0 / distinct as f64 } else { 1.0 };
                property_stats.insert((label.clone(), prop.clone()), PropertyStats {
                    null_fraction: 1.0 - (*count as f64 / sample_size as f64),
                    distinct_count: distinct,
                    selectivity,
                });
            }
        }

        GraphStatistics {
            total_nodes,
            total_edges,
            label_counts,
            edge_type_counts,
            avg_out_degree,
            property_stats,
        }
    }

    /// Get the triple-level statistics catalog (for graph-native query planning)
    pub fn catalog(&self) -> &GraphCatalog {
        &self.catalog
    }

    /// Get node count for a specific label (fast, O(1))
    pub fn label_node_count(&self, label: &Label) -> usize {
        self.label_index.get(label).map(|s| s.len()).unwrap_or(0)
    }

    /// Get edge count for a specific type (fast, O(1))
    pub fn edge_type_count(&self, edge_type: &EdgeType) -> usize {
        self.edge_type_index.get(edge_type).map(|s| s.len()).unwrap_or(0)
    }

    /// Get all label names in the graph
    pub fn all_labels(&self) -> Vec<&Label> {
        self.label_index.keys().collect()
    }

    /// Get all edge type names in the graph
    pub fn all_edge_types(&self) -> Vec<&EdgeType> {
        self.edge_type_index.keys().collect()
    }

    /// Clear all data from the graph
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();
        self.outgoing.clear();
        self.incoming.clear();
        self.free_node_ids.clear();
        self.free_edge_ids.clear();
        self.label_index.clear();
        self.edge_type_index.clear();
        self.vector_index = Arc::new(VectorIndexManager::new());
        self.property_index = Arc::new(IndexManager::new());
        self.node_columns = ColumnStore::new();
        self.edge_columns = ColumnStore::new();
        self.next_node_id = 1;
        self.next_edge_id = 1;
        self.catalog.clear();
    }

    // ============================================================
    // Event Handling
    // ============================================================

    pub fn handle_index_event(&self, event: crate::graph::event::IndexEvent, _tenant_manager: Option<Arc<crate::persistence::TenantManager>>) {
        use crate::graph::event::IndexEvent::*;
        match event {
            NodeCreated { tenant_id: _, id, labels, properties } => {
                for (key, value) in properties {
                    if let PropertyValue::Vector(vec) = &value {
                        for label in &labels {
                            let _ = self.vector_index.add_vector(label.as_str(), &key, id, vec);
                        }
                    }
                    for label in &labels {
                        self.property_index.index_insert(label, &key, value.clone(), id);
                    }
                }
            }
            NodeDeleted { tenant_id: _, id, labels, properties } => {
                for (key, value) in properties {
                    for label in &labels {
                        self.property_index.index_remove(label, &key, &value, id);
                    }
                }
            }
            PropertySet { tenant_id: _, id, labels, key, old_value, new_value } => {
                if let Some(old) = old_value {
                    for label in &labels {
                        self.property_index.index_remove(label, &key, &old, id);
                    }
                }
                for label in &labels {
                    self.property_index.index_insert(label, &key, new_value.clone(), id);
                }
                if let PropertyValue::Vector(vec) = &new_value {
                    for label in &labels {
                        let _ = self.vector_index.add_vector(label.as_str(), &key, id, vec);
                    }
                }
            }
            LabelAdded { tenant_id: _, id, label, properties } => {
                for (key, value) in properties {
                    if let PropertyValue::Vector(vec) = &value {
                        let _ = self.vector_index.add_vector(label.as_str(), &key, id, vec);
                    }
                    self.property_index.index_insert(&label, &key, value.clone(), id);
                }
            }
        }
    }

    // ============================================================
    // Vector Index methods
    // ============================================================

    /// Create a vector index for a specific label and property
    pub fn create_vector_index(
        &self,
        label: &str,
        property_key: &str,
        dimensions: usize,
        metric: DistanceMetric,
    ) -> VectorResult<()> {
        self.vector_index.create_index(label, property_key, dimensions, metric)
    }

    /// Search for nearest neighbors using a vector index
    pub fn vector_search(
        &self,
        label: &str,
        property_key: &str,
        query: &[f32],
        k: usize,
    ) -> VectorResult<Vec<(NodeId, f32)>> {
        self.vector_index.search(label, property_key, query, k)
    }

    // ============================================================
    // Recovery methods - used to rebuild graph from persisted data
    // ============================================================

    /// Insert a recovered node (used during recovery from persistence)
    /// Unlike create_node(), this preserves the node's existing ID
    pub fn insert_recovered_node(&mut self, node: Node) {
        let node_id = node.id;
        let idx = node_id.as_u64() as usize;

        // Ensure storage capacity
        if idx >= self.nodes.len() {
            self.nodes.resize(idx + 1, Vec::new());
            self.outgoing.resize(idx + 1, Vec::new());
            self.incoming.resize(idx + 1, Vec::new());
        }

        // Update label indices for all labels
        for label in &node.labels {
            self.label_index
                .entry(label.clone())
                .or_insert_with(HashSet::new)
                .insert(node_id);
        }

        // Insert the node
        self.nodes[idx].push(node);

        // Update next_node_id to be higher than any recovered node
        if node_id.as_u64() >= self.next_node_id {
            self.next_node_id = node_id.as_u64() + 1;
        }
    }

    /// Insert a recovered edge (used during recovery from persistence)
    /// Unlike create_edge(), this preserves the edge's existing ID
    /// Note: Source and target nodes must already exist
    pub fn insert_recovered_edge(&mut self, edge: Edge) -> GraphResult<()> {
        let edge_id = edge.id;
        let idx = edge_id.as_u64() as usize;
        let source = edge.source;
        let target = edge.target;

        // Ensure capacity
        if idx >= self.edges.len() {
            self.edges.resize(idx + 1, Vec::new());
        }

        // Validate nodes exist
        if !self.has_node(source) {
            return Err(GraphError::InvalidEdgeSource(source));
        }
        if !self.has_node(target) {
            return Err(GraphError::InvalidEdgeTarget(target));
        }

        // Update adjacency lists (sorted insert)
        {
            let out_list = &mut self.outgoing[source.as_u64() as usize];
            let pos = out_list.binary_search_by_key(&target, |(nid, _)| *nid)
                .unwrap_or_else(|p| p);
            out_list.insert(pos, (target, edge_id));
        }
        {
            let in_list = &mut self.incoming[target.as_u64() as usize];
            let pos = in_list.binary_search_by_key(&source, |(nid, _)| *nid)
                .unwrap_or_else(|p| p);
            in_list.insert(pos, (source, edge_id));
        }

        // Update edge type index
        self.edge_type_index
            .entry(edge.edge_type.clone())
            .or_insert_with(HashSet::new)
            .insert(edge_id);

        // Insert the edge
        self.edges[idx].push(edge);

        // Update next_edge_id to be higher than any recovered edge
        if edge_id.as_u64() >= self.next_edge_id {
            self.next_edge_id = edge_id.as_u64() + 1;
        }

        Ok(())
    }
}

impl Default for GraphStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_node() {
        let mut store = GraphStore::new();
        let node_id = store.create_node("Person");

        assert_eq!(store.node_count(), 1);
        let node = store.get_node(node_id).unwrap();
        assert_eq!(node.id, node_id);
        assert!(node.has_label(&Label::new("Person")));
    }

    #[test]
    fn test_create_node_with_properties() {
        let mut store = GraphStore::new();
        let mut props = PropertyMap::new();
        props.insert("name".to_string(), "Alice".into());
        props.insert("age".to_string(), 30i64.into());

        let node_id = store.create_node_with_properties(
            "default",
            vec![Label::new("Person"), Label::new("Employee")],
            props,
        );

        let node = store.get_node(node_id).unwrap();
        assert_eq!(node.label_count(), 2);
        assert_eq!(node.get_property("name").unwrap().as_string(), Some("Alice"));
        assert_eq!(node.get_property("age").unwrap().as_integer(), Some(30));
    }

    #[test]
    fn test_create_and_get_edge() {
        let mut store = GraphStore::new();
        let node1 = store.create_node("Person");
        let node2 = store.create_node("Person");

        let edge_id = store.create_edge(node1, node2, "KNOWS").unwrap();

        assert_eq!(store.edge_count(), 1);
        let edge = store.get_edge(edge_id).unwrap();
        assert_eq!(edge.source, node1);
        assert_eq!(edge.target, node2);
        assert_eq!(edge.edge_type, EdgeType::new("KNOWS"));
    }

    #[test]
    fn test_edge_validation() {
        let mut store = GraphStore::new();
        let node1 = store.create_node("Person");
        let invalid_node = NodeId::new(999);

        // Invalid source node
        let result = store.create_edge(invalid_node, node1, "KNOWS");
        assert_eq!(result, Err(GraphError::InvalidEdgeSource(invalid_node)));

        // Invalid target node
        let result = store.create_edge(node1, invalid_node, "KNOWS");
        assert_eq!(result, Err(GraphError::InvalidEdgeTarget(invalid_node)));
    }

    #[test]
    fn test_adjacency_lists() {
        let mut store = GraphStore::new();
        let node1 = store.create_node("Person");
        let node2 = store.create_node("Person");
        let node3 = store.create_node("Person");

        store.create_edge(node1, node2, "KNOWS").unwrap();
        store.create_edge(node1, node3, "KNOWS").unwrap();
        store.create_edge(node2, node3, "FOLLOWS").unwrap();

        // Node1 has 2 outgoing edges
        let outgoing = store.get_outgoing_edges(node1);
        assert_eq!(outgoing.len(), 2);

        // Node2 has 1 outgoing, 1 incoming
        let outgoing = store.get_outgoing_edges(node2);
        assert_eq!(outgoing.len(), 1);
        let incoming = store.get_incoming_edges(node2);
        assert_eq!(incoming.len(), 1);

        // Node3 has 0 outgoing, 2 incoming
        let outgoing = store.get_outgoing_edges(node3);
        assert_eq!(outgoing.len(), 0);
        let incoming = store.get_incoming_edges(node3);
        assert_eq!(incoming.len(), 2);
    }

    #[test]
    fn test_label_index() {
        let mut store = GraphStore::new();
        store.create_node("Person");
        store.create_node("Person");
        store.create_node("Company");

        let persons = store.get_nodes_by_label(&Label::new("Person"));
        assert_eq!(persons.len(), 2);

        let companies = store.get_nodes_by_label(&Label::new("Company"));
        assert_eq!(companies.len(), 1);
    }

    #[test]
    fn test_edge_type_index() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let n3 = store.create_node("Person");

        store.create_edge(n1, n2, "KNOWS").unwrap();
        store.create_edge(n2, n3, "KNOWS").unwrap();
        store.create_edge(n1, n3, "FOLLOWS").unwrap();

        let knows_edges = store.get_edges_by_type(&EdgeType::new("KNOWS"));
        assert_eq!(knows_edges.len(), 2);

        let follows_edges = store.get_edges_by_type(&EdgeType::new("FOLLOWS"));
        assert_eq!(follows_edges.len(), 1);
    }

    #[test]
    fn test_delete_node() {
        let mut store = GraphStore::new();
        let node1 = store.create_node("Person");
        let node2 = store.create_node("Person");
        store.create_edge(node1, node2, "KNOWS").unwrap();

        assert_eq!(store.node_count(), 2);
        assert_eq!(store.edge_count(), 1);

        // Delete node1 (should also delete connected edge)
        let deleted = store.delete_node("default", node1);
        assert!(deleted.is_ok());
        assert_eq!(store.node_count(), 1);
        assert_eq!(store.edge_count(), 0);
    }

    #[test]
    fn test_delete_edge() {
        let mut store = GraphStore::new();
        let node1 = store.create_node("Person");
        let node2 = store.create_node("Person");
        let edge_id = store.create_edge(node1, node2, "KNOWS").unwrap();

        assert_eq!(store.edge_count(), 1);

        let deleted = store.delete_edge(edge_id);
        assert!(deleted.is_ok());
        assert_eq!(store.edge_count(), 0);

        // Edge removed from adjacency lists
        assert_eq!(store.get_outgoing_edges(node1).len(), 0);
        assert_eq!(store.get_incoming_edges(node2).len(), 0);
    }

    #[test]
    fn test_multiple_edges_between_nodes() {
        // REQ-GRAPH-008: Multiple edges between same nodes
        let mut store = GraphStore::new();
        let node1 = store.create_node("Person");
        let node2 = store.create_node("Person");

        let edge1 = store.create_edge(node1, node2, "KNOWS").unwrap();
        let edge2 = store.create_edge(node1, node2, "WORKS_WITH").unwrap();
        let edge3 = store.create_edge(node1, node2, "KNOWS").unwrap();

        assert_eq!(store.edge_count(), 3);
        assert_ne!(edge1, edge2);
        assert_ne!(edge1, edge3);

        let outgoing = store.get_outgoing_edges(node1);
        assert_eq!(outgoing.len(), 3);
    }

    #[test]
    fn test_clear() {
        let mut store = GraphStore::new();
        store.create_node("Person");
        store.create_node("Person");

        assert_eq!(store.node_count(), 2);

        store.clear();
        assert_eq!(store.node_count(), 0);
        assert_eq!(store.edge_count(), 0);
    }

    #[test]
    fn test_add_label_to_node() {
        let mut store = GraphStore::new();
        let node_id = store.create_node("Person");

        // Initially only "Person" label is indexed
        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 1);
        assert_eq!(store.get_nodes_by_label(&Label::new("Employee")).len(), 0);

        // Add "Employee" label using the proper method
        store.add_label_to_node("default", node_id, "Employee").unwrap();

        // Now both labels should be indexed and queryable
        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 1);
        assert_eq!(store.get_nodes_by_label(&Label::new("Employee")).len(), 1);

        // Verify the node actually has both labels
        let node = store.get_node(node_id).unwrap();
        assert!(node.has_label(&Label::new("Person")));
        assert!(node.has_label(&Label::new("Employee")));
    }

    #[test]
    fn test_add_label_to_nonexistent_node() {
        let mut store = GraphStore::new();
        let invalid_id = NodeId::new(999);

        let result = store.add_label_to_node("default", invalid_id, "Employee");
        assert_eq!(result, Err(GraphError::NodeNotFound(invalid_id)));
    }

    #[test]
    fn test_mvcc_node_versioning() {
        let mut store = GraphStore::new();
        
        // Version 1: Create node
        let node_id = store.create_node("Person");
        store.set_node_property("default", node_id, "name", "Alice").unwrap();
        
        // Check version 1
        let v1_node = store.get_node_at_version(node_id, 1).unwrap();
        assert_eq!(v1_node.version, 1);
        assert_eq!(v1_node.get_property("name").unwrap().as_string(), Some("Alice"));

        // Version 2: Update property (creates new version)
        store.current_version = 2;
        store.set_node_property("default", node_id, "name", "Alice Cooper").unwrap();

        // Check version 2
        let v2_node = store.get_node_at_version(node_id, 2).unwrap();
        assert_eq!(v2_node.version, 2);
        assert_eq!(v2_node.get_property("name").unwrap().as_string(), Some("Alice Cooper"));

        // Historical read (Version 1 should still be Alice)
        let v1_lookup = store.get_node_at_version(node_id, 1).unwrap();
        assert_eq!(v1_lookup.version, 1);
        assert_eq!(v1_lookup.get_property("name").unwrap().as_string(), Some("Alice"));
        
        let node = store.get_node(node_id).unwrap();
        assert_eq!(node.version, 2); // Should be latest version
    }

    #[test]
    fn test_arena_resize() {
        let mut store = GraphStore::new();
        // Default capacity is 1024. Let's force a resize.
        // We can't easily peek capacity, but we can add > 1024 nodes.
        
        for _ in 0..1100 {
            store.create_node("Item");
        }
        
        assert_eq!(store.node_count(), 1100);
        let last_id = NodeId::new(1100);
        assert!(store.has_node(last_id));
    }

    #[test]
    fn test_id_reuse() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("A");
        let _n2 = store.create_node("B");

        store.delete_node("default", n1).unwrap();

        // Next creation should reuse n1's ID (which is 1)
        // n2 is 2.
        let n3 = store.create_node("C");

        assert_eq!(n3, n1); // ID reuse
        assert_eq!(store.node_count(), 2); // B and C
    }

    // ========== Batch 5: Additional Store Tests ==========

    #[test]
    fn test_get_node() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        let node = store.get_node(id);
        assert!(node.is_some());
        assert!(node.unwrap().labels.contains(&Label::new("Person")));

        // Non-existent node
        assert!(store.get_node(NodeId::new(9999)).is_none());
    }

    #[test]
    fn test_get_node_mut() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        {
            let node = store.get_node_mut(id).unwrap();
            node.set_property("name".to_string(), PropertyValue::String("Alice".to_string()));
        }
        let node = store.get_node(id).unwrap();
        assert_eq!(
            node.get_property("name"),
            Some(&PropertyValue::String("Alice".to_string()))
        );

        // Non-existent node
        assert!(store.get_node_mut(NodeId::new(9999)).is_none());
    }

    #[test]
    fn test_has_node() {
        let mut store = GraphStore::new();
        let id = store.create_node("A");
        assert!(store.has_node(id));
        store.delete_node("default", id).unwrap();
        assert!(!store.has_node(id));
    }

    #[test]
    fn test_set_node_property() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.set_node_property("default", id, "age", PropertyValue::Integer(30)).unwrap();
        let node = store.get_node(id).unwrap();
        assert_eq!(
            node.get_property("age"),
            Some(&PropertyValue::Integer(30))
        );

        // Update existing property
        store.set_node_property("default", id, "age", PropertyValue::Integer(31)).unwrap();
        let node = store.get_node(id).unwrap();
        assert_eq!(
            node.get_property("age"),
            Some(&PropertyValue::Integer(31))
        );

        // Non-existent node
        let result = store.set_node_property("default", NodeId::new(9999), "x", PropertyValue::Null);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_edge_with_properties() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Person");

        let mut props = std::collections::HashMap::new();
        props.insert("since".to_string(), PropertyValue::Integer(2020));
        props.insert("weight".to_string(), PropertyValue::Float(0.8));

        let eid = store.create_edge_with_properties(a, b, "KNOWS", props).unwrap();
        let edge = store.get_edge(eid).unwrap();
        assert_eq!(edge.source, a);
        assert_eq!(edge.target, b);
        assert_eq!(edge.get_property("since"), Some(&PropertyValue::Integer(2020)));
        assert_eq!(edge.get_property("weight"), Some(&PropertyValue::Float(0.8)));
    }

    #[test]
    fn test_create_edge_with_properties_invalid_nodes() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let props = std::collections::HashMap::new();

        // Invalid target
        let result = store.create_edge_with_properties(a, NodeId::new(9999), "E", props.clone());
        assert!(result.is_err());

        // Invalid source
        let result = store.create_edge_with_properties(NodeId::new(9999), a, "E", props);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_edge_and_has_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let eid = store.create_edge(a, b, "LINKS").unwrap();

        assert!(store.has_edge(eid));
        let edge = store.get_edge(eid).unwrap();
        assert_eq!(edge.source, a);
        assert_eq!(edge.target, b);

        // Non-existent
        assert!(!store.has_edge(EdgeId::new(9999)));
        assert!(store.get_edge(EdgeId::new(9999)).is_none());
    }

    #[test]
    fn test_get_edge_mut() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let eid = store.create_edge(a, b, "LINKS").unwrap();

        {
            let edge = store.get_edge_mut(eid).unwrap();
            edge.set_property("weight".to_string(), PropertyValue::Float(1.5));
        }
        let edge = store.get_edge(eid).unwrap();
        assert_eq!(edge.get_property("weight"), Some(&PropertyValue::Float(1.5)));

        assert!(store.get_edge_mut(EdgeId::new(9999)).is_none());
    }

    #[test]
    fn test_get_outgoing_edge_targets() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        let e1 = store.create_edge(a, b, "KNOWS").unwrap();
        let e2 = store.create_edge(a, c, "LIKES").unwrap();

        let targets = store.get_outgoing_edge_targets(a);
        assert_eq!(targets.len(), 2);
        // Each tuple is (EdgeId, source, target, &EdgeType)
        let edge_ids: Vec<EdgeId> = targets.iter().map(|t| t.0).collect();
        assert!(edge_ids.contains(&e1));
        assert!(edge_ids.contains(&e2));

        // Node with no outgoing edges
        let targets = store.get_outgoing_edge_targets(b);
        assert!(targets.is_empty());
    }

    #[test]
    fn test_get_incoming_edge_sources() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        store.create_edge(a, c, "KNOWS").unwrap();
        store.create_edge(b, c, "LIKES").unwrap();

        let sources = store.get_incoming_edge_sources(c);
        assert_eq!(sources.len(), 2);
        let src_nodes: Vec<NodeId> = sources.iter().map(|t| t.1).collect();
        assert!(src_nodes.contains(&a));
        assert!(src_nodes.contains(&b));

        // Node with no incoming edges
        let sources = store.get_incoming_edge_sources(a);
        assert!(sources.is_empty());
    }

    #[test]
    fn test_all_nodes() {
        let mut store = GraphStore::new();
        assert!(store.all_nodes().is_empty());

        store.create_node("A");
        store.create_node("B");
        store.create_node("C");
        assert_eq!(store.all_nodes().len(), 3);
    }

    #[test]
    fn test_compute_statistics() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Person");
        let c = store.create_node("Company");
        store.get_node_mut(a).unwrap().set_property("name".to_string(), PropertyValue::String("Alice".to_string()));
        store.get_node_mut(b).unwrap().set_property("name".to_string(), PropertyValue::String("Bob".to_string()));
        store.get_node_mut(c).unwrap().set_property("name".to_string(), PropertyValue::String("Acme".to_string()));
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(a, c, "WORKS_AT").unwrap();

        let stats = store.compute_statistics();
        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.total_edges, 2);
        assert_eq!(*stats.label_counts.get(&Label::new("Person")).unwrap(), 2);
        assert_eq!(*stats.label_counts.get(&Label::new("Company")).unwrap(), 1);
        assert_eq!(*stats.edge_type_counts.get(&EdgeType::new("KNOWS")).unwrap(), 1);
        assert_eq!(*stats.edge_type_counts.get(&EdgeType::new("WORKS_AT")).unwrap(), 1);
        assert!(stats.avg_out_degree > 0.0);
        // Property stats should exist for Person.name
        let person_name_stats = stats.property_stats.get(&(Label::new("Person"), "name".to_string()));
        assert!(person_name_stats.is_some());
        let ps = person_name_stats.unwrap();
        assert_eq!(ps.null_fraction, 0.0); // All Person nodes have "name"
        assert_eq!(ps.distinct_count, 2); // Alice, Bob
    }

    #[test]
    fn test_compute_statistics_empty_graph() {
        let store = GraphStore::new();
        let stats = store.compute_statistics();
        assert_eq!(stats.total_nodes, 0);
        assert_eq!(stats.total_edges, 0);
        assert_eq!(stats.avg_out_degree, 0.0);
        assert!(stats.label_counts.is_empty());
        assert!(stats.edge_type_counts.is_empty());
    }

    #[test]
    fn test_label_node_count() {
        let mut store = GraphStore::new();
        store.create_node("Person");
        store.create_node("Person");
        store.create_node("Company");

        assert_eq!(store.label_node_count(&Label::new("Person")), 2);
        assert_eq!(store.label_node_count(&Label::new("Company")), 1);
        assert_eq!(store.label_node_count(&Label::new("NotExist")), 0);
    }

    #[test]
    fn test_edge_type_count() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(a, c, "KNOWS").unwrap();
        store.create_edge(b, c, "LIKES").unwrap();

        assert_eq!(store.edge_type_count(&EdgeType::new("KNOWS")), 2);
        assert_eq!(store.edge_type_count(&EdgeType::new("LIKES")), 1);
        assert_eq!(store.edge_type_count(&EdgeType::new("NOPE")), 0);
    }

    #[test]
    fn test_all_labels() {
        let mut store = GraphStore::new();
        store.create_node("Person");
        store.create_node("Company");
        store.create_node("Person"); // duplicate label

        let labels = store.all_labels();
        assert_eq!(labels.len(), 2);
        let label_strs: Vec<&str> = labels.iter().map(|l| l.as_str()).collect();
        assert!(label_strs.contains(&"Person"));
        assert!(label_strs.contains(&"Company"));
    }

    #[test]
    fn test_all_edge_types() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(b, c, "LIKES").unwrap();

        let types = store.all_edge_types();
        assert_eq!(types.len(), 2);
        let type_strs: Vec<&str> = types.iter().map(|t| t.as_str()).collect();
        assert!(type_strs.contains(&"KNOWS"));
        assert!(type_strs.contains(&"LIKES"));
    }

    #[test]
    fn test_get_node_at_version() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        let v0 = store.get_node(id).unwrap().version;

        // Node at its creation version should exist
        let node = store.get_node_at_version(id, v0);
        assert!(node.is_some());
        assert!(node.unwrap().labels.contains(&Label::new("Person")));

        // Node at a version before creation should not exist
        // (only if v0 > 0, otherwise any version >= 0 finds it)
        if v0 > 0 {
            assert!(store.get_node_at_version(id, v0 - 1).is_none());
        }

        // Non-existent node
        assert!(store.get_node_at_version(NodeId::new(9999), 0).is_none());
    }

    #[test]
    fn test_get_edge_at_version() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let eid = store.create_edge(a, b, "KNOWS").unwrap();
        let v0 = store.get_edge(eid).unwrap().version;

        // Edge at version 0 should exist
        assert!(store.get_edge_at_version(eid, v0).is_some());

        // Non-existent edge
        assert!(store.get_edge_at_version(EdgeId::new(9999), 0).is_none());
    }

    #[test]
    fn test_create_vector_index() {
        let store = GraphStore::new();
        let result = store.create_vector_index("Person", "embedding", 128, crate::vector::DistanceMetric::Cosine);
        assert!(result.is_ok());

        // Creating a second index with different label should also succeed
        let result2 = store.create_vector_index("Document", "vec", 256, crate::vector::DistanceMetric::L2);
        assert!(result2.is_ok());
    }

    #[test]
    fn test_set_node_property_updates_in_place() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.set_node_property("default", id, "name", PropertyValue::String("Alice".to_string())).unwrap();

        // Same version update — in-place
        store.set_node_property("default", id, "name", PropertyValue::String("Bob".to_string())).unwrap();
        let node = store.get_node(id).unwrap();
        assert_eq!(node.get_property("name"), Some(&PropertyValue::String("Bob".to_string())));

        // Add another property
        store.set_node_property("default", id, "age", PropertyValue::Integer(30)).unwrap();
        let node = store.get_node(id).unwrap();
        assert_eq!(node.get_property("age"), Some(&PropertyValue::Integer(30)));
        assert_eq!(node.get_property("name"), Some(&PropertyValue::String("Bob".to_string())));
    }

    // ========== Coverage Enhancement Tests ==========

    #[test]
    fn test_graph_error_display_node_not_found() {
        let err = GraphError::NodeNotFound(NodeId::new(42));
        assert_eq!(format!("{}", err), "Node NodeId(42) not found");
    }

    #[test]
    fn test_graph_error_display_edge_not_found() {
        let err = GraphError::EdgeNotFound(EdgeId::new(7));
        assert_eq!(format!("{}", err), "Edge EdgeId(7) not found");
    }

    #[test]
    fn test_graph_error_display_node_already_exists() {
        let err = GraphError::NodeAlreadyExists(NodeId::new(1));
        assert_eq!(format!("{}", err), "Node NodeId(1) already exists");
    }

    #[test]
    fn test_graph_error_display_edge_already_exists() {
        let err = GraphError::EdgeAlreadyExists(EdgeId::new(3));
        assert_eq!(format!("{}", err), "Edge EdgeId(3) already exists");
    }

    #[test]
    fn test_graph_error_display_invalid_edge_source() {
        let err = GraphError::InvalidEdgeSource(NodeId::new(99));
        assert_eq!(format!("{}", err), "Invalid edge: source node NodeId(99) does not exist");
    }

    #[test]
    fn test_graph_error_display_invalid_edge_target() {
        let err = GraphError::InvalidEdgeTarget(NodeId::new(88));
        assert_eq!(format!("{}", err), "Invalid edge: target node NodeId(88) does not exist");
    }

    #[test]
    fn test_graph_error_equality() {
        assert_eq!(GraphError::NodeNotFound(NodeId::new(1)), GraphError::NodeNotFound(NodeId::new(1)));
        assert_ne!(GraphError::NodeNotFound(NodeId::new(1)), GraphError::NodeNotFound(NodeId::new(2)));
        assert_ne!(GraphError::NodeNotFound(NodeId::new(1)), GraphError::EdgeNotFound(EdgeId::new(1)));
    }

    #[test]
    fn test_graph_statistics_estimate_label_scan() {
        let mut store = GraphStore::new();
        for _ in 0..50 {
            store.create_node("Person");
        }
        for _ in 0..20 {
            store.create_node("Company");
        }
        let stats = store.compute_statistics();
        assert_eq!(stats.estimate_label_scan(&Label::new("Person")), 50);
        assert_eq!(stats.estimate_label_scan(&Label::new("Company")), 20);
        // Unknown label falls back to total_nodes (all-node scan estimate)
        assert_eq!(stats.estimate_label_scan(&Label::new("Unknown")), 70);
    }

    #[test]
    fn test_graph_statistics_estimate_expand() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(a, c, "KNOWS").unwrap();
        store.create_edge(b, c, "LIKES").unwrap();

        let stats = store.compute_statistics();
        assert_eq!(stats.estimate_expand(Some(&EdgeType::new("KNOWS"))) as usize, 2);
        assert_eq!(stats.estimate_expand(Some(&EdgeType::new("LIKES"))) as usize, 1);
        assert_eq!(stats.estimate_expand(Some(&EdgeType::new("NOPE"))) as usize, 0);
        // None means all edges
        assert_eq!(stats.estimate_expand(None) as usize, 3);
    }

    #[test]
    fn test_graph_statistics_estimate_equality_selectivity() {
        let mut store = GraphStore::new();
        for i in 0..10 {
            let id = store.create_node("Person");
            store.get_node_mut(id).unwrap().set_property(
                "city".to_string(),
                PropertyValue::String(format!("City{}", i % 5)),
            );
        }
        let stats = store.compute_statistics();
        // 5 distinct cities among 10 Person nodes => selectivity = 1/5 = 0.2
        let sel = stats.estimate_equality_selectivity(&Label::new("Person"), "city");
        assert!((sel - 0.2).abs() < 0.01);
        // Unknown property should return default 0.1
        let default_sel = stats.estimate_equality_selectivity(&Label::new("Person"), "unknown_prop");
        assert!((default_sel - 0.1).abs() < 0.01);
        // Unknown label should return default 0.1
        let default_sel2 = stats.estimate_equality_selectivity(&Label::new("NotExist"), "city");
        assert!((default_sel2 - 0.1).abs() < 0.01);
    }

    #[test]
    fn test_graph_statistics_format() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Company");
        store.create_edge(a, b, "WORKS_AT").unwrap();

        let stats = store.compute_statistics();
        let formatted = stats.format();
        assert!(formatted.contains("Graph Statistics:"));
        assert!(formatted.contains("Total nodes: 2"));
        assert!(formatted.contains("Total edges: 1"));
        assert!(formatted.contains("Avg out-degree:"));
        assert!(formatted.contains(":Person"));
        assert!(formatted.contains(":Company"));
        assert!(formatted.contains(":WORKS_AT"));
    }

    #[test]
    fn test_graph_statistics_property_null_fraction() {
        let mut store = GraphStore::new();
        // Create 4 Person nodes, only 2 have the "email" property
        for i in 0..4 {
            let id = store.create_node("Person");
            store.get_node_mut(id).unwrap().set_property(
                "name".to_string(),
                PropertyValue::String(format!("Person{}", i)),
            );
            if i < 2 {
                store.get_node_mut(id).unwrap().set_property(
                    "email".to_string(),
                    PropertyValue::String(format!("person{}@example.com", i)),
                );
            }
        }
        let stats = store.compute_statistics();
        // name should have null_fraction = 0.0 (all 4 have it)
        let name_stats = stats.property_stats.get(&(Label::new("Person"), "name".to_string())).unwrap();
        assert_eq!(name_stats.null_fraction, 0.0);
        // email should have null_fraction = 0.5 (2 out of 4 have it)
        let email_stats = stats.property_stats.get(&(Label::new("Person"), "email".to_string())).unwrap();
        assert!((email_stats.null_fraction - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_vector_search_with_data() {
        let store = GraphStore::new();
        // Create a 4-dimensional index
        store.create_vector_index("Document", "embedding", 4, crate::vector::DistanceMetric::Cosine).unwrap();

        // Add some vectors
        let n1 = NodeId::new(1);
        let n2 = NodeId::new(2);
        let n3 = NodeId::new(3);
        let v1 = vec![1.0, 0.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0, 0.0];
        let v3 = vec![0.9, 0.1, 0.0, 0.0]; // close to v1

        store.vector_index.add_vector("Document", "embedding", n1, &v1).unwrap();
        store.vector_index.add_vector("Document", "embedding", n2, &v2).unwrap();
        store.vector_index.add_vector("Document", "embedding", n3, &v3).unwrap();

        // Search for vectors similar to v1
        let results = store.vector_search("Document", "embedding", &[1.0, 0.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(results.len(), 2);
        // n1 should be the closest (exact match)
        assert_eq!(results[0].0, n1);
    }

    #[test]
    fn test_vector_search_nonexistent_index() {
        let store = GraphStore::new();
        // Search on a non-existent index should return empty results
        let results = store.vector_search("NoLabel", "noprop", &[1.0, 2.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_clear_thorough() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("Company");
        store.set_node_property("default", a, "name", PropertyValue::String("Alice".to_string())).unwrap();
        let eid = store.create_edge(a, b, "WORKS_AT").unwrap();

        // Verify state before clear
        assert_eq!(store.node_count(), 2);
        assert_eq!(store.edge_count(), 1);
        assert_eq!(store.all_labels().len(), 2);
        assert_eq!(store.all_edge_types().len(), 1);
        assert!(store.has_node(a));
        assert!(store.has_edge(eid));

        store.clear();

        // Verify everything is cleaned up
        assert_eq!(store.node_count(), 0);
        assert_eq!(store.edge_count(), 0);
        assert!(store.all_labels().is_empty());
        assert!(store.all_edge_types().is_empty());
        assert!(!store.has_node(a));
        assert!(!store.has_edge(eid));
        assert!(store.get_nodes_by_label(&Label::new("Person")).is_empty());
        assert!(store.get_edges_by_type(&EdgeType::new("WORKS_AT")).is_empty());

        // After clear, creating new nodes should start from ID 1 again
        let new_node = store.create_node("NewLabel");
        assert_eq!(new_node, NodeId::new(1));
    }

    #[test]
    fn test_delete_edge_verifies_edge_type_index_cleanup() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");

        let e1 = store.create_edge(a, b, "KNOWS").unwrap();
        let e2 = store.create_edge(a, c, "KNOWS").unwrap();
        assert_eq!(store.get_edges_by_type(&EdgeType::new("KNOWS")).len(), 2);

        // Delete one edge
        store.delete_edge(e1).unwrap();
        assert_eq!(store.get_edges_by_type(&EdgeType::new("KNOWS")).len(), 1);

        // Delete the other
        store.delete_edge(e2).unwrap();
        assert_eq!(store.get_edges_by_type(&EdgeType::new("KNOWS")).len(), 0);
    }

    #[test]
    fn test_delete_edge_nonexistent() {
        let mut store = GraphStore::new();
        let result = store.delete_edge(EdgeId::new(999));
        assert_eq!(result, Err(GraphError::EdgeNotFound(EdgeId::new(999))));
    }

    #[test]
    fn test_delete_node_nonexistent() {
        let mut store = GraphStore::new();
        let result = store.delete_node("default", NodeId::new(999));
        assert_eq!(result, Err(GraphError::NodeNotFound(NodeId::new(999))));
    }

    #[test]
    fn test_delete_node_removes_from_label_index() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let _b = store.create_node("Person");
        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 2);

        store.delete_node("default", a).unwrap();
        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 1);
    }

    #[test]
    fn test_delete_node_cascades_edges() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        store.create_edge(a, b, "E1").unwrap();
        store.create_edge(c, a, "E2").unwrap();

        assert_eq!(store.edge_count(), 2);
        store.delete_node("default", a).unwrap();
        assert_eq!(store.edge_count(), 0);
        // b and c should still exist
        assert!(store.has_node(b));
        assert!(store.has_node(c));
    }

    #[test]
    fn test_edge_id_reuse() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");

        let e1 = store.create_edge(a, b, "X").unwrap();
        store.delete_edge(e1).unwrap();

        // Next edge should reuse e1's ID
        let e2 = store.create_edge(a, c, "Y").unwrap();
        assert_eq!(e2, e1);
    }

    #[test]
    fn test_insert_recovered_node() {
        let mut store = GraphStore::new();
        let node = Node::new(NodeId::new(10), Label::new("Recovered"));

        store.insert_recovered_node(node);
        assert!(store.has_node(NodeId::new(10)));
        assert_eq!(store.get_nodes_by_label(&Label::new("Recovered")).len(), 1);

        // Next node created should have ID > 10
        let new_id = store.create_node("New");
        assert!(new_id.as_u64() > 10);
    }

    #[test]
    fn test_insert_recovered_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");

        let edge = Edge::new(EdgeId::new(50), a, b, EdgeType::new("RECOVERED"));
        store.insert_recovered_edge(edge).unwrap();

        assert!(store.has_edge(EdgeId::new(50)));
        assert_eq!(store.get_outgoing_edges(a).len(), 1);
        assert_eq!(store.get_incoming_edges(b).len(), 1);
        assert_eq!(store.get_edges_by_type(&EdgeType::new("RECOVERED")).len(), 1);

        // Next edge should have ID > 50
        let new_eid = store.create_edge(a, b, "NEW").unwrap();
        assert!(new_eid.as_u64() > 50);
    }

    #[test]
    fn test_insert_recovered_edge_invalid_source() {
        let mut store = GraphStore::new();
        let b = store.create_node("B");
        let edge = Edge::new(EdgeId::new(1), NodeId::new(999), b, EdgeType::new("E"));
        let result = store.insert_recovered_edge(edge);
        assert_eq!(result, Err(GraphError::InvalidEdgeSource(NodeId::new(999))));
    }

    #[test]
    fn test_insert_recovered_edge_invalid_target() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let edge = Edge::new(EdgeId::new(1), a, NodeId::new(999), EdgeType::new("E"));
        let result = store.insert_recovered_edge(edge);
        assert_eq!(result, Err(GraphError::InvalidEdgeTarget(NodeId::new(999))));
    }

    #[test]
    fn test_default_impl() {
        let store = GraphStore::default();
        assert_eq!(store.node_count(), 0);
        assert_eq!(store.edge_count(), 0);
    }

    #[test]
    fn test_mvcc_cow_versioning() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.set_node_property("default", id, "name", PropertyValue::String("Alice".to_string())).unwrap();

        // Bump version to trigger COW
        store.current_version = 2;
        store.set_node_property("default", id, "name", PropertyValue::String("Bob".to_string())).unwrap();

        // Latest version should be Bob
        let latest = store.get_node(id).unwrap();
        assert_eq!(latest.get_property("name"), Some(&PropertyValue::String("Bob".to_string())));
        assert_eq!(latest.version, 2);

        // Version 1 should still be Alice
        let v1 = store.get_node_at_version(id, 1).unwrap();
        assert_eq!(v1.get_property("name"), Some(&PropertyValue::String("Alice".to_string())));
        assert_eq!(v1.version, 1);
    }

    #[test]
    fn test_get_outgoing_edge_targets_detail() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        let e1 = store.create_edge(a, b, "KNOWS").unwrap();
        let e2 = store.create_edge(a, c, "LIKES").unwrap();

        let targets = store.get_outgoing_edge_targets(a);
        assert_eq!(targets.len(), 2);

        // Check that tuples contain correct (edge_id, source, target, edge_type)
        for (eid, src, tgt, etype) in &targets {
            assert_eq!(*src, a);
            if *eid == e1 {
                assert_eq!(*tgt, b);
                assert_eq!(etype.as_str(), "KNOWS");
            } else if *eid == e2 {
                assert_eq!(*tgt, c);
                assert_eq!(etype.as_str(), "LIKES");
            } else {
                panic!("Unexpected edge ID");
            }
        }

        // Non-existent node returns empty
        let empty = store.get_outgoing_edge_targets(NodeId::new(9999));
        assert!(empty.is_empty());
    }

    #[test]
    fn test_get_incoming_edge_sources_detail() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        let e1 = store.create_edge(a, c, "KNOWS").unwrap();
        let e2 = store.create_edge(b, c, "LIKES").unwrap();

        let sources = store.get_incoming_edge_sources(c);
        assert_eq!(sources.len(), 2);

        for (eid, src, tgt, etype) in &sources {
            assert_eq!(*tgt, c);
            if *eid == e1 {
                assert_eq!(*src, a);
                assert_eq!(etype.as_str(), "KNOWS");
            } else if *eid == e2 {
                assert_eq!(*src, b);
                assert_eq!(etype.as_str(), "LIKES");
            } else {
                panic!("Unexpected edge ID");
            }
        }

        // Non-existent node returns empty
        let empty = store.get_incoming_edge_sources(NodeId::new(9999));
        assert!(empty.is_empty());
    }

    #[test]
    fn test_all_labels_empty() {
        let store = GraphStore::new();
        assert!(store.all_labels().is_empty());
    }

    #[test]
    fn test_all_edge_types_empty() {
        let store = GraphStore::new();
        assert!(store.all_edge_types().is_empty());
    }

    #[test]
    fn test_columnar_storage_integration() {
        let mut store = GraphStore::new();
        let id = store.create_node_with_properties(
            "default",
            vec![Label::new("Person")],
            {
                let mut props = PropertyMap::new();
                props.insert("name".to_string(), PropertyValue::String("Alice".to_string()));
                props.insert("age".to_string(), PropertyValue::Integer(30));
                props
            },
        );

        // Verify columnar storage has the values
        let idx = id.as_u64() as usize;
        let name_col = store.node_columns.get_property(idx, "name");
        assert_eq!(name_col, PropertyValue::String("Alice".to_string()));
        let age_col = store.node_columns.get_property(idx, "age");
        assert_eq!(age_col, PropertyValue::Integer(30));
    }

    #[test]
    fn test_edge_columnar_storage_integration() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let mut props = std::collections::HashMap::new();
        props.insert("weight".to_string(), PropertyValue::Float(0.75));
        let eid = store.create_edge_with_properties(a, b, "WEIGHTED", props).unwrap();

        let idx = eid.as_u64() as usize;
        let weight_col = store.edge_columns.get_property(idx, "weight");
        assert_eq!(weight_col, PropertyValue::Float(0.75));
    }

    #[test]
    fn test_set_node_property_updates_columnar_storage() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.set_node_property("default", id, "score", PropertyValue::Float(1.0)).unwrap();

        let idx = id.as_u64() as usize;
        assert_eq!(store.node_columns.get_property(idx, "score"), PropertyValue::Float(1.0));

        // Update
        store.set_node_property("default", id, "score", PropertyValue::Float(2.5)).unwrap();
        assert_eq!(store.node_columns.get_property(idx, "score"), PropertyValue::Float(2.5));
    }

    #[test]
    fn test_get_nodes_by_label_after_deletions() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b_id = store.create_node("Person");
        let c = store.create_node("Person");

        store.delete_node("default", b_id).unwrap();
        let persons = store.get_nodes_by_label(&Label::new("Person"));
        assert_eq!(persons.len(), 2);
        let ids: Vec<NodeId> = persons.iter().map(|n| n.id).collect();
        assert!(ids.contains(&a));
        assert!(ids.contains(&c));
        assert!(!ids.contains(&b_id));
    }

    #[test]
    fn test_get_edges_by_type_after_deletion() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        let e1 = store.create_edge(a, b, "KNOWS").unwrap();
        let e2 = store.create_edge(b, c, "KNOWS").unwrap();

        store.delete_edge(e1).unwrap();
        let knows_edges = store.get_edges_by_type(&EdgeType::new("KNOWS"));
        assert_eq!(knows_edges.len(), 1);
        assert_eq!(knows_edges[0].id, e2);
    }

    #[test]
    fn test_all_nodes_after_operations() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        store.create_node("B");
        store.create_node("C");
        store.delete_node("default", a).unwrap();

        let all = store.all_nodes();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_compute_statistics_with_large_sample() {
        let mut store = GraphStore::new();
        // Create more than 1000 nodes to test sample limiting
        for i in 0..1100 {
            let id = store.create_node("BigLabel");
            store.get_node_mut(id).unwrap().set_property(
                "idx".to_string(),
                PropertyValue::Integer(i),
            );
        }
        let stats = store.compute_statistics();
        assert_eq!(stats.total_nodes, 1100);
        // Property stats should exist (sampled from first 1000)
        let idx_stats = stats.property_stats.get(&(Label::new("BigLabel"), "idx".to_string()));
        assert!(idx_stats.is_some());
        let ps = idx_stats.unwrap();
        // All sampled nodes have the property => null_fraction = 0
        assert_eq!(ps.null_fraction, 0.0);
        // distinct_count is from the sample (first 1000 nodes), each has unique value
        assert_eq!(ps.distinct_count, 1000);
    }

    #[test]
    fn test_add_label_then_label_count() {
        let mut store = GraphStore::new();
        let id = store.create_node("Person");
        store.add_label_to_node("default", id, "Employee").unwrap();

        assert_eq!(store.label_node_count(&Label::new("Person")), 1);
        assert_eq!(store.label_node_count(&Label::new("Employee")), 1);
    }

    #[test]
    fn test_insert_recovered_node_with_multiple_labels() {
        let mut store = GraphStore::new();
        let mut node = Node::new(NodeId::new(5), Label::new("Person"));
        node.add_label(Label::new("Employee"));

        store.insert_recovered_node(node);
        assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 1);
        assert_eq!(store.get_nodes_by_label(&Label::new("Employee")).len(), 1);
    }

    #[test]
    fn test_graph_statistics_avg_out_degree() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let c = store.create_node("C");
        let d = store.create_node("D");
        // a -> b, a -> c, a -> d (3 edges, 4 nodes)
        store.create_edge(a, b, "E").unwrap();
        store.create_edge(a, c, "E").unwrap();
        store.create_edge(a, d, "E").unwrap();

        let stats = store.compute_statistics();
        assert!((stats.avg_out_degree - 0.75).abs() < 0.01); // 3/4 = 0.75
    }

    #[test]
    fn test_self_loop_edge() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let eid = store.create_edge(a, a, "SELF").unwrap();

        assert_eq!(store.get_outgoing_edges(a).len(), 1);
        assert_eq!(store.get_incoming_edges(a).len(), 1);

        store.delete_edge(eid).unwrap();
        assert_eq!(store.get_outgoing_edges(a).len(), 0);
        assert_eq!(store.get_incoming_edges(a).len(), 0);
    }

    #[test]
    fn test_get_outgoing_edges_nonexistent_node() {
        let store = GraphStore::new();
        let edges = store.get_outgoing_edges(NodeId::new(999));
        assert!(edges.is_empty());
    }

    #[test]
    fn test_get_incoming_edges_nonexistent_node() {
        let store = GraphStore::new();
        let edges = store.get_incoming_edges(NodeId::new(999));
        assert!(edges.is_empty());
    }

    #[test]
    fn test_get_nodes_by_label_nonexistent() {
        let store = GraphStore::new();
        let nodes = store.get_nodes_by_label(&Label::new("NoSuch"));
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_get_edges_by_type_nonexistent() {
        let store = GraphStore::new();
        let edges = store.get_edges_by_type(&EdgeType::new("NoSuch"));
        assert!(edges.is_empty());
    }

    // ---- edge_between / edges_between tests (TDD) ----

    #[test]
    fn test_edge_between_exists() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let eid = store.create_edge(n1, n2, "KNOWS").unwrap();

        assert_eq!(store.edge_between(n1, n2, Some(&EdgeType::new("KNOWS"))), Some(eid));
    }

    #[test]
    fn test_edge_between_not_exists() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");

        assert_eq!(store.edge_between(n1, n2, Some(&EdgeType::new("KNOWS"))), None);
    }

    #[test]
    fn test_edge_between_wrong_type() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        store.create_edge(n1, n2, "KNOWS").unwrap();

        assert_eq!(store.edge_between(n1, n2, Some(&EdgeType::new("FOLLOWS"))), None);
    }

    #[test]
    fn test_edge_between_any_type() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let eid = store.create_edge(n1, n2, "KNOWS").unwrap();

        // None edge_type means any type
        assert_eq!(store.edge_between(n1, n2, None), Some(eid));
    }

    #[test]
    fn test_edge_between_reverse_direction() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        store.create_edge(n1, n2, "KNOWS").unwrap();

        // Reverse direction should NOT find the edge
        assert_eq!(store.edge_between(n2, n1, Some(&EdgeType::new("KNOWS"))), None);
    }

    #[test]
    fn test_edges_between_multi() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let eid1 = store.create_edge(n1, n2, "KNOWS").unwrap();
        let eid2 = store.create_edge(n1, n2, "FOLLOWS").unwrap();
        let eid3 = store.create_edge(n1, n2, "KNOWS").unwrap();

        // All edges between n1 and n2 (any type)
        let all = store.edges_between(n1, n2, None);
        assert_eq!(all.len(), 3);

        // Only KNOWS edges
        let knows = store.edges_between(n1, n2, Some(&EdgeType::new("KNOWS")));
        assert_eq!(knows.len(), 2);
        assert!(knows.contains(&eid1));
        assert!(knows.contains(&eid3));

        // Only FOLLOWS edges
        let follows = store.edges_between(n1, n2, Some(&EdgeType::new("FOLLOWS")));
        assert_eq!(follows.len(), 1);
        assert!(follows.contains(&eid2));
    }

    // ---- Sorted adjacency list tests (Phase 1B TDD) ----

    #[test]
    fn test_sorted_adjacency_insert_order() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n3 = store.create_node("Person");
        let n2 = store.create_node("Person");

        // Insert edges to n3 first, then n2 — adjacency should still be sorted by target NodeId
        store.create_edge(n1, n3, "KNOWS").unwrap();
        store.create_edge(n1, n2, "KNOWS").unwrap();

        // Outgoing from n1 should be sorted by target: n2 before n3
        let out = &store.outgoing[n1.as_u64() as usize];
        assert_eq!(out.len(), 2);
        assert!(out[0].0 <= out[1].0, "outgoing adjacency should be sorted by target NodeId");
    }

    #[test]
    fn test_sorted_adjacency_delete() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let n3 = store.create_node("Person");

        let e1 = store.create_edge(n1, n2, "KNOWS").unwrap();
        let _e2 = store.create_edge(n1, n3, "KNOWS").unwrap();

        store.delete_edge(e1).unwrap();

        let out = &store.outgoing[n1.as_u64() as usize];
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, n3); // only n3 remains
    }

    #[test]
    fn test_sorted_adjacency_multiple_edges_same_target() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");

        let e1 = store.create_edge(n1, n2, "KNOWS").unwrap();
        let e2 = store.create_edge(n1, n2, "FOLLOWS").unwrap();

        // Both edges should be to the same target
        let out = &store.outgoing[n1.as_u64() as usize];
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].0, n2);
        assert_eq!(out[1].0, n2);

        // edge_between with binary search should find the right one
        assert!(store.edge_between(n1, n2, Some(&EdgeType::new("KNOWS"))).is_some());
        assert!(store.edge_between(n1, n2, Some(&EdgeType::new("FOLLOWS"))).is_some());
    }

    #[test]
    fn test_edge_between_binary_search_high_degree() {
        let mut store = GraphStore::new();
        let hub = store.create_node("Hub");

        // Create 100 target nodes and edges from hub
        let mut targets = Vec::new();
        for _ in 0..100 {
            let t = store.create_node("Target");
            store.create_edge(hub, t, "LINKS").unwrap();
            targets.push(t);
        }

        // Binary search should find any of them
        for &t in &targets {
            assert!(
                store.edge_between(hub, t, Some(&EdgeType::new("LINKS"))).is_some(),
                "edge_between should find edge to target {:?}", t
            );
        }

        // Non-existent target
        let fake = NodeId::new(9999);
        assert!(store.edge_between(hub, fake, Some(&EdgeType::new("LINKS"))).is_none());
    }
}