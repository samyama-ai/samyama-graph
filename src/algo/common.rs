//! Shared utilities for graph algorithms
//!
//! Provides a read-only, optimized view of the graph topology for algorithm execution.

use crate::graph::{GraphStore, NodeId, EdgeType, Label};
use std::collections::HashMap;

/// A dense, integer-indexed view of the graph topology.
///
/// Algorithms like PageRank require iterating over nodes and edges efficiently.
/// The standard GraphStore uses HashMaps with NodeIds (u64), which is good for
/// random access but slower for dense iteration.
///
/// This view maps NodeIds to dense indices (0..N) and stores the graph
/// as an adjacency list.
pub struct GraphView {
    /// Number of nodes
    pub node_count: usize,
    /// Mapping from dense index (0..N) back to NodeId
    pub index_to_node: Vec<NodeId>,
    /// Mapping from NodeId to dense index
    pub node_to_index: HashMap<NodeId, usize>,
    /// Outgoing edges: index -> vec![target_index]
    pub outgoing: Vec<Vec<usize>>,
    /// Incoming edges: index -> vec![target_index]
    pub incoming: Vec<Vec<usize>>,
}

impl GraphView {
    /// Create a new GraphView from the GraphStore, optionally filtering by label and edge type.
    pub fn new(
        store: &GraphStore,
        node_label: Option<&str>,
        edge_type: Option<&str>,
    ) -> Self {
        // 1. Collect relevant nodes
        let nodes: Vec<NodeId> = if let Some(label_str) = node_label {
            let label = Label::new(label_str);
            store.get_nodes_by_label(&label)
                .iter()
                .map(|n| n.id)
                .collect()
        } else {
            store.all_nodes()
                .iter()
                .map(|n| n.id)
                .collect()
        };

        // 2. Build index mappings
        let mut index_to_node = Vec::with_capacity(nodes.len());
        let mut node_to_index = HashMap::with_capacity(nodes.len());

        for (idx, node_id) in nodes.iter().enumerate() {
            index_to_node.push(*node_id);
            node_to_index.insert(*node_id, idx);
        }

        let node_count = index_to_node.len();
        let mut outgoing = vec![Vec::new(); node_count];
        let mut incoming = vec![Vec::new(); node_count];

        // 3. Build adjacency lists
        let filter_edge_type = edge_type.map(EdgeType::new);

        for (u_idx, u_id) in index_to_node.iter().enumerate() {
            let edges = store.get_outgoing_edges(*u_id);
            
            for edge in edges {
                // Apply edge filter if present
                if let Some(ref et) = filter_edge_type {
                    if edge.edge_type != *et {
                        continue;
                    }
                }

                // If target is in our subgraph, add the connection
                if let Some(&v_idx) = node_to_index.get(&edge.target) {
                    outgoing[u_idx].push(v_idx);
                    incoming[v_idx].push(u_idx);
                }
            }
        }

        Self {
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
        }
    }

    /// Get the out-degree of a node (by index)
    pub fn out_degree(&self, idx: usize) -> usize {
        self.outgoing[idx].len()
    }

    /// Get the in-degree of a node (by index)
    pub fn in_degree(&self, idx: usize) -> usize {
        self.incoming[idx].len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;

    #[test]
    fn test_graph_view_projection() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Person");
        let n2 = store.create_node("Person");
        let n3 = store.create_node("Person");

        // n1 -> n2 -> n3
        store.create_edge(n1, n2, "KNOWS").unwrap();
        store.create_edge(n2, n3, "KNOWS").unwrap();

        let view = GraphView::new(&store, None, None);

        assert_eq!(view.node_count, 3);
        
        let n1_idx = *view.node_to_index.get(&n1).unwrap();
        let n2_idx = *view.node_to_index.get(&n2).unwrap();
        let n3_idx = *view.node_to_index.get(&n3).unwrap();

        // Check topology
        assert!(view.outgoing[n1_idx].contains(&n2_idx));
        assert!(view.outgoing[n2_idx].contains(&n3_idx));
        
        assert_eq!(view.out_degree(n1_idx), 1);
        assert_eq!(view.in_degree(n2_idx), 1);
    }
}
