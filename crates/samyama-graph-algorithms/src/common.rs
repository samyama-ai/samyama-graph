//! Shared utilities for graph algorithms
//!
//! Provides a read-only, optimized view of the graph topology for algorithm execution.

use std::collections::HashMap;

/// Node Identifier type (u64)
pub type NodeId = u64;

/// Bytes of metadata a `HashMap` keeps per slot alongside the entry: its
/// SwissTable layout stores one control byte for each.
const HASH_CONTROL_BYTES: usize = 1;

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

    /// The first negative edge weight in the view, if there is one.
    ///
    /// Dijkstra and A* are only correct on non-negative weights, and both of
    /// them *skip* a negative edge rather than refusing it -- so a graph with
    /// one gets a shortest path computed on a different graph, and the caller
    /// cannot tell (#1303). This is the check a call surface runs before
    /// offering an answer it cannot stand behind. `bellman_ford` handles
    /// negative weights and detects a negative cycle, so there is somewhere to
    /// send the caller.
    ///
    /// One pass over the weight array, so it costs a scan of the edges and
    /// nothing per node. `None` when the view is unweighted: an absent weight
    /// is 1.0.
    pub fn first_negative_weight(&self) -> Option<f64> {
        self.weights.as_ref()?.iter().copied().find(|w| *w < 0.0)
    }

    /// Number of relationships in the view (each directed edge once).
    pub fn edge_count(&self) -> usize {
        self.out_targets.len()
    }

    /// Bytes this view holds on the heap, estimated from what it allocated.
    ///
    /// Counts each vector's capacity, not its length, because capacity is what
    /// is resident. `node_to_index` is a hash table: each slot holds a key and
    /// a value plus one control byte, and the table allocates `capacity` slots
    /// rounded up to its group width. That rounding and the allocator's own
    /// bookkeeping are not visible from here, so the result is a floor, the
    /// same contract as the engine's index memory figures.
    ///
    /// Used to account for a named graph projection (ALGO-05), which keeps a
    /// view alive between queries rather than for the length of one.
    pub fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        let usizes = self.out_offsets.capacity()
            + self.out_targets.capacity()
            + self.in_offsets.capacity()
            + self.in_sources.capacity();
        let map_slot = size_of::<NodeId>() + size_of::<usize>() + HASH_CONTROL_BYTES;
        usizes * size_of::<usize>()
            + self.index_to_node.capacity() * size_of::<NodeId>()
            + self.node_to_index.capacity() * map_slot
            + self
                .weights
                .as_ref()
                .map_or(0, |w| w.capacity() * size_of::<f64>())
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

#[cfg(test)]
mod heap_tests {
    use super::*;

    /// A directed path 0 -> 1 -> 2, optionally weighted.
    fn path(weighted: bool) -> GraphView {
        let ids: Vec<NodeId> = vec![10, 20, 30];
        let index = ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        let out = vec![vec![1], vec![2], vec![]];
        let inc = vec![vec![], vec![0], vec![1]];
        let weights = weighted.then(|| vec![vec![1.5], vec![2.5], vec![]]);
        GraphView::from_adjacency_list(ids.len(), ids, index, out, inc, weights)
    }

    #[test]
    fn edge_count_counts_each_directed_edge_once() {
        assert_eq!(path(false).edge_count(), 2);
    }

    #[test]
    fn heap_bytes_covers_every_array_it_holds() {
        use std::mem::size_of;
        let view = path(false);
        // At least the four CSR arrays, the id list and one map slot per node.
        let floor = (view.out_offsets.len()
            + view.out_targets.len()
            + view.in_offsets.len()
            + view.in_sources.len())
            * size_of::<usize>()
            + view.index_to_node.len() * size_of::<NodeId>()
            + view.node_count * (size_of::<NodeId>() + size_of::<usize>());
        assert!(view.heap_bytes() >= floor, "{} < {floor}", view.heap_bytes());
    }

    #[test]
    fn weights_add_to_the_estimate() {
        let unweighted = path(false).heap_bytes();
        let weighted = path(true).heap_bytes();
        assert!(
            weighted >= unweighted + 2 * std::mem::size_of::<f64>(),
            "{weighted} vs {unweighted}"
        );
    }

    #[test]
    fn an_empty_view_holds_almost_nothing() {
        let view = GraphView::from_adjacency_list(0, vec![], HashMap::new(), vec![], vec![], None);
        // Only the one-element offset arrays CSR always carries.
        assert!(view.heap_bytes() <= 4 * std::mem::size_of::<usize>(), "{}", view.heap_bytes());
    }
}

