//! In-memory graph storage implementation
//!
//! Implements:
//! - REQ-GRAPH-001: Property graph data model
//! - REQ-MEM-001: In-memory storage
//! - REQ-MEM-003: Memory-optimized data structures

use super::edge::Edge;
use super::node::Node;
use super::property::{PropertyMap, PropertyValue};
use super::types::{EdgeId, EdgeType, Label, NodeId};
use crate::vector::{VectorIndexManager, DistanceMetric, VectorResult};
use crate::index::IndexManager;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use crate::agent::{AgentRuntime, tools::WebSearchTool};

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

/// In-memory graph storage
///
/// Uses hash maps for O(1) lookup performance:
/// - nodes: NodeId -> Node
/// - edges: EdgeId -> Edge
/// - outgoing: NodeId -> Vec<EdgeId> (adjacency list for outgoing edges)
/// - incoming: NodeId -> Vec<EdgeId> (adjacency list for incoming edges)
/// - label_index: Label -> Vec<NodeId> (index for fast label lookups)
#[derive(Debug)]
pub struct GraphStore {
    /// Node storage
    nodes: HashMap<NodeId, Node>,

    /// Edge storage
    edges: HashMap<EdgeId, Edge>,

    /// Outgoing edges for each node (adjacency list)
    outgoing: HashMap<NodeId, Vec<EdgeId>>,

    /// Incoming edges for each node (adjacency list)
    incoming: HashMap<NodeId, Vec<EdgeId>>,

    /// Label index for fast lookups
    label_index: HashMap<Label, HashSet<NodeId>>,

    /// Edge type index for fast lookups
    edge_type_index: HashMap<EdgeType, HashSet<EdgeId>>,

    /// Vector indices manager
    pub vector_index: Arc<VectorIndexManager>,

    /// Property indices manager
    pub property_index: Arc<IndexManager>,

    /// Async index event sender
    pub index_sender: Option<UnboundedSender<crate::graph::event::IndexEvent>>,

    /// Next node ID
    next_node_id: u64,

    /// Next edge ID
    next_edge_id: u64,
}

impl GraphStore {
    /// Create a new empty graph store
    pub fn new() -> Self {
        GraphStore {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            label_index: HashMap::new(),
            edge_type_index: HashMap::new(),
            vector_index: Arc::new(VectorIndexManager::new()),
            property_index: Arc::new(IndexManager::new()),
            index_sender: None,
            next_node_id: 1,
            next_edge_id: 1,
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
        let node_id = NodeId::new(self.next_node_id);
        self.next_node_id += 1;

        let label = label.into();
        let node = Node::new(node_id, label.clone());

        // Add to label index
        self.label_index
            .entry(label)
            .or_insert_with(HashSet::new)
            .insert(node_id);

        // Initialize adjacency lists
        self.outgoing.insert(node_id, Vec::new());
        self.incoming.insert(node_id, Vec::new());

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

        self.nodes.insert(node_id, node);
        node_id
    }

    /// Create a node with multiple labels and properties
    pub fn create_node_with_properties(
        &mut self,
        tenant_id: &str,
        labels: Vec<Label>,
        properties: PropertyMap,
    ) -> NodeId {
        let node_id = NodeId::new(self.next_node_id);
        self.next_node_id += 1;

        let node = Node::new_with_properties(node_id, labels.clone(), properties);

        // Add to label indices
        for label in &labels {
            self.label_index
                .entry(label.clone())
                .or_insert_with(HashSet::new)
                .insert(node_id);
        }

        // Initialize adjacency lists
        self.outgoing.insert(node_id, Vec::new());
        self.incoming.insert(node_id, Vec::new());

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

        self.nodes.insert(node_id, node);
        node_id
    }

    /// Get a node by ID
    pub fn get_node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Get a mutable node by ID
    pub fn get_node_mut(&mut self, id: NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(&id)
    }

    /// Check if a node exists
    pub fn has_node(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
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

        let node = self.nodes.get_mut(&node_id).ok_or(GraphError::NodeNotFound(node_id))?;
        
        let old_val = node.set_property(key_str.clone(), val.clone());

        let event = crate::graph::event::IndexEvent::PropertySet {
            tenant_id: tenant_id.to_string(),
            id: node_id,
            labels: node.labels.iter().cloned().collect(),
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
        let node = self.nodes.remove(&id).ok_or(GraphError::NodeNotFound(id))?;

        // Remove from label indices
        for label in &node.labels {
            if let Some(node_set) = self.label_index.get_mut(label) {
                node_set.remove(&id);
            }
        }

        let event = crate::graph::event::IndexEvent::NodeDeleted {
            tenant_id: tenant_id.to_string(),
            id,
            labels: node.labels.iter().cloned().collect(),
            properties: node.properties.clone(),
        };

        if let Some(sender) = &self.index_sender {
            let _ = sender.send(event);
        } else {
            self.handle_index_event(event, None);
        }

        // Remove all connected edges
        let outgoing_edges = self.outgoing.remove(&id).unwrap_or_default();
        let incoming_edges = self.incoming.remove(&id).unwrap_or_default();

        for edge_id in outgoing_edges.iter().chain(incoming_edges.iter()) {
            if let Some(edge) = self.edges.remove(edge_id) {
                // Remove from edge type index
                if let Some(edge_set) = self.edge_type_index.get_mut(&edge.edge_type) {
                    edge_set.remove(edge_id);
                }

                // Clean up adjacency lists
                if let Some(adj) = self.incoming.get_mut(&edge.target) {
                    adj.retain(|&eid| eid != *edge_id);
                }
                if let Some(adj) = self.outgoing.get_mut(&edge.source) {
                    adj.retain(|&eid| eid != *edge_id);
                }
            }
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

        // Get the node and add the label
        let node = self.nodes.get_mut(&node_id).ok_or(GraphError::NodeNotFound(node_id))?;
        node.add_label(label.clone());

        // Update the label index so queries can find this node by the new label
        self.label_index
            .entry(label.clone())
            .or_insert_with(HashSet::new)
            .insert(node_id);

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

        let edge_id = EdgeId::new(self.next_edge_id);
        self.next_edge_id += 1;

        let edge_type = edge_type.into();
        let edge = Edge::new(edge_id, source, target, edge_type.clone());

        // Update adjacency lists
        self.outgoing.get_mut(&source).unwrap().push(edge_id);
        self.incoming.get_mut(&target).unwrap().push(edge_id);

        // Update edge type index
        self.edge_type_index
            .entry(edge_type)
            .or_insert_with(HashSet::new)
            .insert(edge_id);

        self.edges.insert(edge_id, edge);
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

        let edge_id = EdgeId::new(self.next_edge_id);
        self.next_edge_id += 1;

        let edge_type = edge_type.into();
        let edge = Edge::new_with_properties(edge_id, source, target, edge_type.clone(), properties);

        // Update adjacency lists
        self.outgoing.get_mut(&source).unwrap().push(edge_id);
        self.incoming.get_mut(&target).unwrap().push(edge_id);

        // Update edge type index
        self.edge_type_index
            .entry(edge_type)
            .or_insert_with(HashSet::new)
            .insert(edge_id);

        self.edges.insert(edge_id, edge);
        Ok(edge_id)
    }

    /// Get an edge by ID
    pub fn get_edge(&self, id: EdgeId) -> Option<&Edge> {
        self.edges.get(&id)
    }

    /// Get a mutable edge by ID
    pub fn get_edge_mut(&mut self, id: EdgeId) -> Option<&mut Edge> {
        self.edges.get_mut(&id)
    }

    /// Check if an edge exists
    pub fn has_edge(&self, id: EdgeId) -> bool {
        self.edges.contains_key(&id)
    }

    /// Delete an edge
    pub fn delete_edge(&mut self, id: EdgeId) -> GraphResult<Edge> {
        let edge = self.edges.remove(&id).ok_or(GraphError::EdgeNotFound(id))?;

        // Remove from edge type index
        if let Some(edge_set) = self.edge_type_index.get_mut(&edge.edge_type) {
            edge_set.remove(&id);
        }

        // Remove from adjacency lists
        if let Some(adj) = self.outgoing.get_mut(&edge.source) {
            adj.retain(|&eid| eid != id);
        }
        if let Some(adj) = self.incoming.get_mut(&edge.target) {
            adj.retain(|&eid| eid != id);
        }

        Ok(edge)
    }

    /// Get all outgoing edges from a node
    pub fn get_outgoing_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.outgoing
            .get(&node_id)
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .filter_map(|&id| self.edges.get(&id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all incoming edges to a node
    pub fn get_incoming_edges(&self, node_id: NodeId) -> Vec<&Edge> {
        self.incoming
            .get(&node_id)
            .map(|edge_ids| {
                edge_ids
                    .iter()
                    .filter_map(|&id| self.edges.get(&id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get all nodes with a specific label
    pub fn get_nodes_by_label(&self, label: &Label) -> Vec<&Node> {
        self.label_index
            .get(label)
            .map(|node_ids| {
                node_ids
                    .iter()
                    .filter_map(|&id| self.nodes.get(&id))
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
                    .filter_map(|&id| self.edges.get(&id))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get total number of nodes
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get total number of edges
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Get all nodes in the graph
    pub fn all_nodes(&self) -> Vec<&Node> {
        self.nodes.values().collect()
    }

    /// Clear all data from the graph
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();
        self.outgoing.clear();
        self.incoming.clear();
        self.label_index.clear();
        self.edge_type_index.clear();
        self.vector_index = Arc::new(VectorIndexManager::new());
        self.property_index = Arc::new(IndexManager::new());
        // Keep existing index_sender if any, or reset? Resetting usually means clearing data.
        self.next_node_id = 1;
        self.next_edge_id = 1;
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

        // Update label indices for all labels
        for label in &node.labels {
            self.label_index
                .entry(label.clone())
                .or_insert_with(HashSet::new)
                .insert(node_id);
        }

        // Initialize adjacency lists
        self.outgoing.entry(node_id).or_insert_with(Vec::new);
        self.incoming.entry(node_id).or_insert_with(Vec::new);

        // Insert the node
        self.nodes.insert(node_id, node);

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
        let source = edge.source;
        let target = edge.target;

        // Validate nodes exist
        if !self.has_node(source) {
            return Err(GraphError::InvalidEdgeSource(source));
        }
        if !self.has_node(target) {
            return Err(GraphError::InvalidEdgeTarget(target));
        }

        // Update adjacency lists
        self.outgoing.get_mut(&source).unwrap().push(edge_id);
        self.incoming.get_mut(&target).unwrap().push(edge_id);

        // Update edge type index
        self.edge_type_index
            .entry(edge.edge_type.clone())
            .or_insert_with(HashSet::new)
            .insert(edge_id);

        // Insert the edge
        self.edges.insert(edge_id, edge);

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
}