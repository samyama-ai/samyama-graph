//! Graph algorithms module
//!
//! Implements analytics algorithms for Phase 7.
//! Algorithms are implemented in `samyama-graph-algorithms` crate.
//! This module provides the integration/adapter layer.

use crate::graph::{GraphStore, EdgeType, Label, PropertyValue};
use samyama_graph_algorithms::{GraphView, NodeId as AlgoNodeId};
use std::collections::HashMap;

// Re-export algorithms
pub use samyama_graph_algorithms::{
    page_rank, PageRankConfig,
    weakly_connected_components, WccResult,
    bfs, dijkstra, PathResult,
    edmonds_karp, FlowResult,
    prim_mst, MSTResult,
    count_triangles
};

/// Build a GraphView from the store for algorithm execution
pub fn build_view(
    store: &GraphStore,
    node_label: Option<&str>,
    edge_type: Option<&str>,
    weight_property: Option<&str>,
) -> GraphView {
    // 1. Collect relevant nodes
    let nodes: Vec<AlgoNodeId> = if let Some(label_str) = node_label {
        let label = Label::new(label_str);
        store.get_nodes_by_label(&label)
            .iter()
            .map(|n| n.id.as_u64())
            .collect()
    } else {
        store.all_nodes()
            .iter()
            .map(|n| n.id.as_u64())
            .collect()
    };

    // 2. Build index mappings
    let mut index_to_node = Vec::with_capacity(nodes.len());
    let mut node_to_index = HashMap::with_capacity(nodes.len());

    for (idx, &node_id) in nodes.iter().enumerate() {
        index_to_node.push(node_id);
        node_to_index.insert(node_id, idx);
    }

    let node_count = index_to_node.len();
    let mut outgoing = vec![Vec::new(); node_count];
    let mut incoming = vec![Vec::new(); node_count];
    let mut weights = if weight_property.is_some() {
        Some(vec![Vec::new(); node_count])
    } else {
        None
    };

    // 3. Build adjacency lists
    let filter_edge_type = edge_type.map(EdgeType::new);

    for (u_idx, &u_id) in index_to_node.iter().enumerate() {
        let u_node_id = crate::graph::NodeId::new(u_id);
        let edges = store.get_outgoing_edges(u_node_id);
        
        for edge in edges {
            // Apply edge filter if present
            if let Some(ref et) = filter_edge_type {
                if edge.edge_type != *et {
                    continue;
                }
            }

            // If target is in our subgraph, add the connection
            if let Some(&v_idx) = node_to_index.get(&edge.target.as_u64()) {
                outgoing[u_idx].push(v_idx);
                incoming[v_idx].push(u_idx);

                // Handle weights
                if let Some(ref mut w_vec) = weights {
                    let prop_name = weight_property.unwrap();
                    let w = match edge.get_property(prop_name) {
                        Some(PropertyValue::Integer(i)) => *i as f64,
                        Some(PropertyValue::Float(f)) => *f,
                        _ => 1.0, 
                    };
                    w_vec[u_idx].push(w);
                }
            }
        }
    }

    GraphView {
        node_count,
        index_to_node,
        node_to_index,
        outgoing,
        incoming,
        weights,
    }
}