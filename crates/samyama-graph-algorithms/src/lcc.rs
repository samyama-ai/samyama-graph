//! Local Clustering Coefficient (LCC)
//!
//! Computes the local clustering coefficient for each node.
//!
//! Undirected: LCC(v) = 2 * T(v) / (deg(v) * (deg(v) - 1))
//! Directed:   LCC(v) = T(v) / (deg(v) * (deg(v) - 1))
//!
//! where T(v) is the number of triangles (edges among neighbors) containing v,
//! and deg(v) is the undirected degree (union of successors + predecessors) for
//! undirected mode, or the number of distinct neighbors for directed mode.

use super::common::{GraphView, NodeId};
use std::collections::{HashMap, HashSet};
use rayon::prelude::*;

/// Result of LCC computation
#[derive(Debug, Clone)]
pub struct LccResult {
    /// Clustering coefficient per node
    pub coefficients: HashMap<NodeId, f64>,
    /// Global average clustering coefficient
    pub average: f64,
}

/// Compute local clustering coefficients for all nodes (undirected mode).
///
/// Uses undirected edges (union of successors + predecessors).
/// This is the backward-compatible entry point.
pub fn local_clustering_coefficient(view: &GraphView) -> LccResult {
    local_clustering_coefficient_directed(view, false)
}

/// Compute local clustering coefficients for all nodes.
///
/// When `directed=false`: uses undirected neighbor sets (union of successors +
/// predecessors), counts undirected edges among neighbors, divides by
/// `d*(d-1)/2`.
///
/// When `directed=true`: uses undirected neighbor sets for neighborhood
/// discovery, but counts *directed* edges (u→w) among neighbors, divides by
/// `d*(d-1)` (the maximum number of directed edges among d nodes).
pub fn local_clustering_coefficient_directed(view: &GraphView, directed: bool) -> LccResult {
    let n = view.node_count;
    if n == 0 {
        return LccResult { coefficients: HashMap::new(), average: 0.0 };
    }

    // Build undirected neighbor sets for each node (parallel for large graphs)
    let use_parallel = n >= 1000;

    let neighbors: Vec<HashSet<usize>> = if use_parallel {
        (0..n).into_par_iter().map(|idx| {
            let mut set = HashSet::new();
            for &s in view.successors(idx) { if s != idx { set.insert(s); } }
            for &p in view.predecessors(idx) { if p != idx { set.insert(p); } }
            set
        }).collect()
    } else {
        (0..n).map(|idx| {
            let mut set = HashSet::new();
            for &s in view.successors(idx) { if s != idx { set.insert(s); } }
            for &p in view.predecessors(idx) { if p != idx { set.insert(p); } }
            set
        }).collect()
    };

    // For directed mode, build successor sets for directed edge checking
    let successor_sets: Vec<HashSet<usize>> = if directed {
        if use_parallel {
            (0..n).into_par_iter().map(|idx| {
                let mut set = HashSet::new();
                for &s in view.successors(idx) { if s != idx { set.insert(s); } }
                set
            }).collect()
        } else {
            (0..n).map(|idx| {
                let mut set = HashSet::new();
                for &s in view.successors(idx) { if s != idx { set.insert(s); } }
                set
            }).collect()
        }
    } else {
        Vec::new()
    };

    // Compute LCC per node in parallel
    let per_node: Vec<(NodeId, f64)> = if use_parallel {
        (0..n).into_par_iter().map(|idx| {
            let deg = neighbors[idx].len();
            if deg < 2 {
                return (view.index_to_node[idx], 0.0);
            }
            let neighbor_vec: Vec<usize> = neighbors[idx].iter().cloned().collect();

            let cc = if directed {
                let mut directed_edges = 0usize;
                for i in 0..neighbor_vec.len() {
                    for j in 0..neighbor_vec.len() {
                        if i != j && successor_sets[neighbor_vec[i]].contains(&neighbor_vec[j]) {
                            directed_edges += 1;
                        }
                    }
                }
                directed_edges as f64 / (deg * (deg - 1)) as f64
            } else {
                let mut triangle_edges = 0usize;
                for i in 0..neighbor_vec.len() {
                    for j in (i + 1)..neighbor_vec.len() {
                        if neighbors[neighbor_vec[i]].contains(&neighbor_vec[j]) {
                            triangle_edges += 1;
                        }
                    }
                }
                triangle_edges as f64 / (deg * (deg - 1) / 2) as f64
            };
            (view.index_to_node[idx], cc)
        }).collect()
    } else {
        (0..n).map(|idx| {
            let deg = neighbors[idx].len();
            if deg < 2 {
                return (view.index_to_node[idx], 0.0);
            }
            let neighbor_vec: Vec<usize> = neighbors[idx].iter().cloned().collect();

            let cc = if directed {
                let mut directed_edges = 0usize;
                for i in 0..neighbor_vec.len() {
                    for j in 0..neighbor_vec.len() {
                        if i != j && successor_sets[neighbor_vec[i]].contains(&neighbor_vec[j]) {
                            directed_edges += 1;
                        }
                    }
                }
                directed_edges as f64 / (deg * (deg - 1)) as f64
            } else {
                let mut triangle_edges = 0usize;
                for i in 0..neighbor_vec.len() {
                    for j in (i + 1)..neighbor_vec.len() {
                        if neighbors[neighbor_vec[i]].contains(&neighbor_vec[j]) {
                            triangle_edges += 1;
                        }
                    }
                }
                triangle_edges as f64 / (deg * (deg - 1) / 2) as f64
            };
            (view.index_to_node[idx], cc)
        }).collect()
    };

    let mut coefficients = HashMap::with_capacity(n);
    let mut sum = 0.0;
    for (node_id, cc) in per_node {
        sum += cc;
        coefficients.insert(node_id, cc);
    }

    let average = if n > 0 { sum / n as f64 } else { 0.0 };

    LccResult { coefficients, average }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::common::GraphView;

    #[test]
    fn test_lcc_triangle() {
        // Complete triangle: 1-2, 2-3, 1-3
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let outgoing = vec![vec![1, 2], vec![0, 2], vec![0, 1]];
        let incoming = vec![vec![1, 2], vec![0, 2], vec![0, 1]];

        let view = GraphView::from_adjacency_list(3, index_to_node, node_to_index, outgoing, incoming, None);
        let result = local_clustering_coefficient(&view);

        // All nodes in a complete triangle have LCC = 1.0
        for (_node, cc) in &result.coefficients {
            assert!((cc - 1.0).abs() < 1e-10, "Complete triangle LCC should be 1.0, got {}", cc);
        }
        assert!((result.average - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_lcc_star() {
        // Star: center 1 connected to 2, 3, 4 (no edges among 2,3,4)
        let index_to_node = vec![1, 2, 3, 4];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() {
            node_to_index.insert(id, i);
        }

        let outgoing = vec![vec![1, 2, 3], vec![0], vec![0], vec![0]];
        let incoming = vec![vec![1, 2, 3], vec![0], vec![0], vec![0]];

        let view = GraphView::from_adjacency_list(4, index_to_node, node_to_index, outgoing, incoming, None);
        let result = local_clustering_coefficient(&view);

        // Center node: 3 neighbors, no edges among them -> LCC = 0
        assert!((result.coefficients[&1] - 0.0).abs() < 1e-10);
        // Leaf nodes: degree 1 -> LCC = 0
        assert!((result.coefficients[&2] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_lcc_empty() {
        let view = GraphView::from_adjacency_list(
            0, vec![], HashMap::new(), vec![], vec![], None,
        );
        let result = local_clustering_coefficient(&view);
        assert!(result.coefficients.is_empty());
        assert!((result.average - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_lcc_directed_triangle() {
        // Directed triangle: 1->2, 2->3, 3->1 (cycle)
        // Each node has 2 neighbors (undirected), and there is exactly 1 directed
        // edge among those 2 neighbors. max_edges = 2*(2-1) = 2.
        // LCC = 1/2 = 0.5 for each node.
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        // 0->1, 1->2, 2->0
        let outgoing = vec![vec![1], vec![2], vec![0]];
        let incoming = vec![vec![2], vec![0], vec![1]];

        let view = GraphView::from_adjacency_list(3, index_to_node, node_to_index, outgoing, incoming, None);
        let result = local_clustering_coefficient_directed(&view, true);

        // Node 0 (id=1): neighbors are {1, 2}. Directed edges among them: 1->2 = 1.
        // max = 2*1 = 2.  LCC = 1/2 = 0.5
        for (&_node, &cc) in &result.coefficients {
            assert!((cc - 0.5).abs() < 1e-10, "Directed cycle triangle LCC should be 0.5, got {}", cc);
        }
    }

    #[test]
    fn test_lcc_directed_complete_triangle() {
        // Fully connected directed triangle: all 6 directed edges present
        // Each node has 2 neighbors, 2 directed edges among them, max = 2.
        // LCC = 2/2 = 1.0
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let outgoing = vec![vec![1, 2], vec![0, 2], vec![0, 1]];
        let incoming = vec![vec![1, 2], vec![0, 2], vec![0, 1]];

        let view = GraphView::from_adjacency_list(3, index_to_node, node_to_index, outgoing, incoming, None);
        let result = local_clustering_coefficient_directed(&view, true);

        for (&_node, &cc) in &result.coefficients {
            assert!((cc - 1.0).abs() < 1e-10, "Fully connected directed triangle LCC should be 1.0, got {}", cc);
        }
    }
}
