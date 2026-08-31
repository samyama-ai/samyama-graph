//! Graph algorithms module
//!
//! Implements analytics algorithms for Phase 7.
//! Algorithms are implemented in `samyama-graph-algorithms` crate.
//! This module provides the integration/adapter layer.

use crate::graph::{GraphStore, EdgeType, Label, PropertyValue};
pub use samyama_graph_algorithms::GraphView;
use samyama_graph_algorithms::NodeId as AlgoNodeId;
use std::collections::HashMap;

// Re-export algorithms
pub use samyama_graph_algorithms::{
    page_rank, PageRankConfig,
    weakly_connected_components, WccResult,
    strongly_connected_components, SccResult,
    bfs, dijkstra, bfs_all_shortest_paths, PathResult,
    edmonds_karp, FlowResult,
    prim_mst, MSTResult,
    count_triangles,
    cdlp, CdlpResult, CdlpConfig,
    local_clustering_coefficient, local_clustering_coefficient_directed, LccResult,
    pca, PcaConfig, PcaResult, PcaSolver,
    temporal_reachability, temporal_shortest_path, symptom_explanation,
    TemporalEdges, Explanation, TemporalPath,
    betweenness_centrality, closeness_centrality, core_number, degree_centrality,
    eigenvector_centrality, harmonic_centrality, ranked,
    predict_links, score_one, LinkScore, PairScore,
    articulation_points, bridges, find_cycle, topological_sort, TopoResult,
};

/// Build a GraphView from the store for algorithm execution
pub fn build_view(
    store: &GraphStore,
    node_label: Option<&str>,
    edge_type: Option<&str>,
    weight_property: Option<&str>,
) -> GraphView {
    build_view_inner(store, node_label, edge_type, weight_property, None).0
}

/// A view plus edge timestamps aligned with its `out_targets`, for the
/// temporal primitives (ALGO-15).
///
/// `time_property` names an edge property holding an integer or a temporal
/// value; when it is `None`, or an edge does not carry it, the edge's own
/// `created_at` is used. That fallback is what makes the primitives usable on
/// an ordinary graph nobody prepared for them.
///
/// The times **must** be collected in the same pass that builds the CSR.
/// Recovering them afterwards by matching `(source, target)` is wrong wherever
/// two nodes have more than one edge between them -- and a graph of service
/// calls is nothing but parallel edges. The alignment is then checked again by
/// `TemporalEdges::new`, because a silent misalignment answers a different
/// question on every edge and still returns plausible times.
pub fn build_temporal_view(
    store: &GraphStore,
    node_label: Option<&str>,
    edge_type: Option<&str>,
    time_property: Option<&str>,
) -> (GraphView, Vec<i64>) {
    let (view, times) = build_view_inner(store, node_label, edge_type, None, Some(time_property));
    (view, times.unwrap_or_default())
}

/// One implementation behind both. `time_property` is `Some(None)` to collect
/// times using the `created_at` fallback, and `None` to collect none at all --
/// a second copy of this loop would be a second place for the CSR ordering to
/// drift out of step with what is aligned against it.
fn build_view_inner(
    store: &GraphStore,
    node_label: Option<&str>,
    edge_type: Option<&str>,
    weight_property: Option<&str>,
    time_property: Option<Option<&str>>,
) -> (GraphView, Option<Vec<i64>>) {
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

    // 3. Build adjacency lists (Intermediate step)
    let filter_edge_type = edge_type.map(EdgeType::new);

    // Temp storage
    let mut temp_outgoing: Vec<Vec<usize>> = vec![Vec::new(); node_count];
    let mut temp_incoming: Vec<Vec<usize>> = vec![Vec::new(); node_count];
    let mut temp_weights: Option<Vec<Vec<f64>>> = if weight_property.is_some() {
        Some(vec![Vec::new(); node_count])
    } else {
        None
    };
    let mut temp_times: Option<Vec<Vec<i64>>> = time_property.map(|_| vec![Vec::new(); node_count]);

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
                temp_outgoing[u_idx].push(v_idx);
                temp_incoming[v_idx].push(u_idx);

                // Handle weights
                if let Some(ref mut w_vec) = temp_weights {
                    let prop_name = weight_property.unwrap();
                    let w = match edge.get_property(prop_name) {
                        Some(PropertyValue::Integer(i)) => *i as f64,
                        Some(PropertyValue::Float(f)) => *f,
                        _ => 1.0, 
                    };
                    w_vec[u_idx].push(w);
                }

                // Pushed inside the same `if let Some(v_idx)` as the target,
                // so an edge leaving the subgraph contributes to neither and
                // the two arrays stay the same length.
                if let Some(ref mut t_vec) = temp_times {
                    t_vec[u_idx].push(edge_time(&edge, time_property.flatten()));
                }
            }
        }
    }

    // 4. Convert to CSR
    let mut out_offsets = Vec::with_capacity(node_count + 1);
    let mut out_targets = Vec::new();
    let mut in_offsets = Vec::with_capacity(node_count + 1);
    let mut in_sources = Vec::new();
    let mut weights = if temp_weights.is_some() { Some(Vec::new()) } else { None };
    let mut times: Option<Vec<i64>> = if temp_times.is_some() { Some(Vec::new()) } else { None };

    // Flatten Outgoing
    out_offsets.push(0);
    for (i, neighbors) in temp_outgoing.into_iter().enumerate() {
        out_targets.extend(neighbors);
        out_offsets.push(out_targets.len());
        
        if let Some(ref mut w_flat) = weights {
            if let Some(w_row) = temp_weights.as_mut().map(|w| &mut w[i]) {
                w_flat.extend(w_row.iter());
            }
        }
        // Flattened in the same loop and the same node order as the targets,
        // which is what the alignment means.
        if let Some(ref mut t_flat) = times {
            if let Some(t_row) = temp_times.as_mut().map(|t| &mut t[i]) {
                t_flat.extend(t_row.iter());
            }
        }
    }

    // Flatten Incoming
    in_offsets.push(0);
    for sources in temp_incoming {
        in_sources.extend(sources);
        in_offsets.push(in_sources.len());
    }

    (
        GraphView {
            node_count,
            index_to_node,
            node_to_index,
            out_offsets,
            out_targets,
            in_offsets,
            in_sources,
            weights,
        },
        times,
    )
}

/// The time an edge fired: a named property if it has one, else `created_at`.
///
/// A temporal value is read as its own instant rather than coerced through a
/// float, so a `DateTime` and an integer epoch sort together.
fn edge_time(edge: &crate::graph::Edge, property: Option<&str>) -> i64 {
    if let Some(name) = property {
        match edge.get_property(name) {
            Some(PropertyValue::Integer(i)) => return *i,
            Some(PropertyValue::DateTime(ms)) => return *ms,
            Some(PropertyValue::LocalDateTime { secs, .. }) => return *secs,
            Some(PropertyValue::ZonedDateTime { secs, .. }) => return *secs,
            Some(PropertyValue::Date(days)) => return *days as i64 * 86_400,
            Some(PropertyValue::Float(f)) => return *f as i64,
            // A property that is present but not a time is not silently
            // treated as zero -- zero is a real instant, and using it would
            // place the edge at the epoch and quietly change every answer.
            // Falling back to `created_at` is the honest reading of "this
            // edge has no usable timestamp".
            _ => {}
        }
    }
    edge.created_at
}