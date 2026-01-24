//! Shared utilities for graph algorithms
//!
//! Provides a read-only, optimized view of the graph topology for algorithm execution.

use std::collections::HashMap;

/// Node Identifier type (u64)
pub type NodeId = u64;

/// A dense, integer-indexed view of the graph topology.
pub struct GraphView {
    /// Number of nodes
    pub node_count: usize,
    /// Mapping from dense index (0..N) back to NodeId
    pub index_to_node: Vec<NodeId>,
    /// Mapping from NodeId to dense index
    pub node_to_index: HashMap<NodeId, usize>,
    /// Outgoing edges: index -> vec![target_index]
    pub outgoing: Vec<Vec<usize>>,
    /// Incoming edges: index -> vec![source_index]
    pub incoming: Vec<Vec<usize>>,
    /// Edge weights: index -> vec![weight] (corresponds to outgoing edges)
    pub weights: Option<Vec<Vec<f64>>>,
}

impl GraphView {
    /// Get the out-degree of a node (by index)
    pub fn out_degree(&self, idx: usize) -> usize {
        self.outgoing[idx].len()
    }

    /// Get the in-degree of a node (by index)
    pub fn in_degree(&self, idx: usize) -> usize {
        self.incoming[idx].len()
    }
}
