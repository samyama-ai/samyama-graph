//! Shared utilities for graph algorithms
//!
//! Provides a read-only, optimized view of the graph topology for algorithm execution.

use std::collections::HashMap;

/// Node Identifier type (u64)
pub type NodeId = u64;

/// A dense, integer-indexed view of the graph topology using Compressed Sparse Row (CSR) format.
pub struct GraphView {
    /// Number of nodes
    pub node_count: usize,
    /// Mapping from dense index (0..N) back to NodeId
    pub index_to_node: Vec<NodeId>,
    /// Mapping from NodeId to dense index
    pub node_to_index: HashMap<NodeId, usize>,
    
    /// Outgoing edges CSR structure
    /// Offsets into `out_targets`. Size = node_count + 1
    pub out_offsets: Vec<usize>,
    /// Contiguous array of target node indices
    pub out_targets: Vec<usize>,

    /// Incoming edges CSR structure (Compressed Sparse Column effectively)
    /// Offsets into `in_sources`. Size = node_count + 1
    pub in_offsets: Vec<usize>,
    /// Contiguous array of source node indices
    pub in_sources: Vec<usize>,

    /// Edge weights: aligned with `out_targets`
    pub weights: Option<Vec<f64>>,
}

impl GraphView {
    /// Get the out-degree of a node (by index)
    pub fn out_degree(&self, idx: usize) -> usize {
        self.out_offsets[idx + 1] - self.out_offsets[idx]
    }

    /// Get the in-degree of a node (by index)
    pub fn in_degree(&self, idx: usize) -> usize {
        self.in_offsets[idx + 1] - self.in_offsets[idx]
    }

    /// Get outgoing neighbors (successors) of a node
    pub fn successors(&self, idx: usize) -> &[usize] {
        let start = self.out_offsets[idx];
        let end = self.out_offsets[idx + 1];
        &self.out_targets[start..end]
    }

    /// Get incoming neighbors (predecessors) of a node
    pub fn predecessors(&self, idx: usize) -> &[usize] {
        let start = self.in_offsets[idx];
        let end = self.in_offsets[idx + 1];
        &self.in_sources[start..end]
    }

    /// Get weights for outgoing edges of a node
    pub fn weights(&self, idx: usize) -> Option<&[f64]> {
        self.weights.as_ref().map(|w| {
            let start = self.out_offsets[idx];
            let end = self.out_offsets[idx + 1];
            &w[start..end]
        })
    }

    /// Helper to create GraphView from adjacency lists (legacy/test support)
    pub fn from_adjacency_list(
        node_count: usize,
        index_to_node: Vec<NodeId>,
        node_to_index: HashMap<NodeId, usize>,
        outgoing: Vec<Vec<usize>>,
        incoming: Vec<Vec<usize>>,
        weights: Option<Vec<Vec<f64>>>,
    ) -> Self {
        let mut out_offsets = Vec::with_capacity(node_count + 1);
        let mut out_targets = Vec::new();
        let mut in_offsets = Vec::with_capacity(node_count + 1);
        let mut in_sources = Vec::new();
        let mut flat_weights = if weights.is_some() { Some(Vec::new()) } else { None };

        out_offsets.push(0);
        for (i, neighbors) in outgoing.into_iter().enumerate() {
            out_targets.extend(neighbors);
            out_offsets.push(out_targets.len());
            
            if let Some(ref mut w_flat) = flat_weights {
                if let Some(w_row) = weights.as_ref().map(|w| &w[i]) {
                    w_flat.extend(w_row.iter());
                }
            }
        }

        in_offsets.push(0);
        for sources in incoming {
            in_sources.extend(sources);
            in_offsets.push(in_sources.len());
        }

        GraphView {
            node_count,
            index_to_node,
            node_to_index,
            out_offsets,
            out_targets,
            in_offsets,
            in_sources,
            weights: flat_weights,
        }
    }
}
