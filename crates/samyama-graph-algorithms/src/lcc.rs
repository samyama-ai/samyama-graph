//! Local Clustering Coefficient (LCC)
//!
//! Computes the local clustering coefficient for each node.
//!
//! Undirected: LCC(v) = 2 * T(v) / (deg(v) * (deg(v) - 1))
//! Directed:   LCC(v) = T(v) / (d_tot(v) * (d_tot(v) - 1) - 2 * d_bi(v))
//!
//! where T(v) is the number of triangles (edges among neighbors) containing v,
//! and deg(v) is the undirected degree (union of successors + predecessors).
//!
//! The directed case is Fagiolo (2007), matching NetworkX and igraph: d_tot
//! counts a reciprocal neighbour twice and d_bi is the number of such
//! neighbours. It is NOT deg*(deg-1) over distinct neighbours, which is what
//! this module computed until #658 and which disagrees on exactly the nodes
//! that have a reciprocal edge.

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

/// Which directed clustering coefficient to compute.
///
/// There is more than one, and two of our own requirements ask for different
/// ones (#916):
///
/// * **ALGO-02** measures agreement with NetworkX and igraph, which both
///   implement Fagiolo (2007). `CH-ALGO-PARITY` has been green against it
///   since #658.
/// * **ALGO-08** measures agreement with LDBC Graphalytics, whose LCC is a
///   different formula with no reciprocal-edge correction at all.
///
/// Both implementations are correct for their own reference and neither is
/// correct for the other's, so the choice is a parameter rather than a hidden
/// decision -- which is also what ALGO-03 asks for: normalization documented
/// per algorithm. Picking one silently is how the next disagreement gets
/// introduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DirectedLcc {
    /// Fagiolo (2007). Denominator `d_tot(d_tot - 1) - 2*d_bi`; the `-2*d_bi`
    /// discounts reciprocal pairs, which cannot form the triangles the
    /// denominator is counting possibilities for.
    #[default]
    Fagiolo,
    /// LDBC Graphalytics. `|{(u,w) : u,w in N(v), u != w, (u,w) in E}| /
    /// (d(v) * (d(v)-1))`, where `N(v)` is the *set* union of in- and
    /// out-neighbours and the numerator counts **ordered** pairs joined by a
    /// directed edge. No reciprocal correction.
    Ldbc,
}

/// Fagiolo (2007) directed clustering coefficient, as NetworkX and igraph
/// compute it.
///
/// The denominator is the part that is easy to get wrong. It was
/// `deg * (deg - 1)` over the *distinct* neighbours, which counts a reciprocal
/// pair once and admits triangles that cannot exist. Fagiolo uses
///
/// ```text
/// d_tot(d_tot - 1) - 2 * d_bi
/// ```
///
/// where `d_tot = |preds| + |succs|` counts a reciprocal neighbour **twice**,
/// and `d_bi` is the number of neighbours that are both. Subtracting `2*d_bi`
/// removes the pairs a reciprocal edge cannot close.
///
/// Exactly the nodes with a reciprocal edge disagreed under the old formula —
/// 7 of 40 on the reference graph, every other node matching to 0.000e0
/// (#658). ALGO-02 names NetworkX and igraph as the references and they agree
/// with each other, so this is what the requirement asks for; ALGO-03 asks for
/// the convention to be written down, and this comment is that.
fn directed_clustering(
    idx: usize,
    predecessor_sets: &[HashSet<usize>],
    successor_sets: &[HashSet<usize>],
) -> f64 {
    let ipreds = &predecessor_sets[idx];
    let isuccs = &successor_sets[idx];

    // Every directed triangle through `idx`, counted once per orientation —
    // which is why the denominator carries a factor of two below.
    let mut triangles = 0usize;
    for &j in ipreds.iter().chain(isuccs.iter()) {
        let jpreds = &predecessor_sets[j];
        let jsuccs = &successor_sets[j];
        triangles += ipreds.intersection(jpreds).count()
            + ipreds.intersection(jsuccs).count()
            + isuccs.intersection(jpreds).count()
            + isuccs.intersection(jsuccs).count();
    }
    if triangles == 0 {
        return 0.0;
    }

    let d_tot = ipreds.len() + isuccs.len();
    let d_bi = ipreds.intersection(isuccs).count();
    let denom = (d_tot * (d_tot.saturating_sub(1))).saturating_sub(2 * d_bi);
    if denom == 0 {
        return 0.0;
    }
    triangles as f64 / (denom as f64 * 2.0)
}

/// LDBC Graphalytics' directed clustering coefficient.
///
/// Ordered pairs of distinct neighbours joined by a directed edge, over
/// `d(d-1)`. Both directions of a reciprocal pair count, and both slots of the
/// denominator are available to them -- which is exactly where this parts
/// company with Fagiolo.
///
/// Checked against the shipped `example-directed` reference: node 1 has
/// `N = {3,5,8}`, the edges `3->5, 3->8, 5->3, 5->8` among them, and
/// `4/6 = 0.6667`, which is what the reference file says.
fn ldbc_clustering(idx: usize, neighbours: &HashSet<usize>, successor_sets: &[HashSet<usize>]) -> f64 {
    let d = neighbours.len();
    if d < 2 {
        return 0.0;
    }
    let mut edges = 0usize;
    for &u in neighbours {
        if u == idx {
            continue;
        }
        for &w in successor_sets[u].intersection(neighbours) {
            if w != idx && w != u {
                edges += 1;
            }
        }
    }
    edges as f64 / (d * (d - 1)) as f64
}

/// Compute local clustering coefficients for all nodes.
///
/// When `directed=false`: uses undirected neighbor sets (union of successors +
/// predecessors), counts undirected edges among neighbors, divides by
/// `d*(d-1)/2`.
///
/// When `directed=true`: uses undirected neighbor sets for neighborhood
/// discovery, but counts *directed* edges (u→w) among neighbors, divides by
/// Fagiolo's `d_tot(d_tot - 1) - 2*d_bi` (see `directed_clustering`).
pub fn local_clustering_coefficient_directed(view: &GraphView, directed: bool) -> LccResult {
    local_clustering_coefficient_with(view, directed, DirectedLcc::Fagiolo)
}

/// As [`local_clustering_coefficient_directed`], choosing the directed
/// definition explicitly. See [`DirectedLcc`] for why there is a choice.
///
/// `definition` is ignored when `directed` is false: the undirected
/// coefficient is the same under both references.
pub fn local_clustering_coefficient_with(
    view: &GraphView,
    directed: bool,
    definition: DirectedLcc,
) -> LccResult {
    let n = view.node_count;
    if n == 0 {
        return LccResult { coefficients: HashMap::new(), average: 0.0 };
    }

    // GPU acceleration gate (opt-in via --features gpu; transparent CPU fallback).
    #[cfg(feature = "gpu")]
    {
        if n > crate::gpu_dispatch::min_gpu_nodes() && samyama_gpu::gpu_available() {
            {
                match samyama_gpu::gpu_lcc(
                    n,
                    &view.out_offsets,
                    &view.out_targets,
                    &view.in_offsets,
                    &view.in_sources,
                    directed,
                ) {
                    Ok(gpu_result) => {
                        let mut coefficients = HashMap::with_capacity(n);
                        for (idx, &cc) in gpu_result.coefficients.iter().enumerate() {
                            coefficients.insert(view.index_to_node[idx], cc);
                        }
                        return LccResult { coefficients, average: gpu_result.average };
                    }
                    Err(e) => tracing::warn!("GPU LCC failed, falling back to CPU: {}", e),
                }
            }
        }
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

    // Predecessor sets, needed alongside the successors for Fagiolo's
    // directed coefficient (#658).
    let predecessor_sets: Vec<HashSet<usize>> = if directed {
        (0..n)
            .map(|idx| {
                let mut set = HashSet::new();
                for &p in view.predecessors(idx) {
                    if p != idx {
                        set.insert(p);
                    }
                }
                set
            })
            .collect()
    } else {
        Vec::new()
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
                match definition {
                    DirectedLcc::Fagiolo => {
                        directed_clustering(idx, &predecessor_sets, &successor_sets)
                    }
                    DirectedLcc::Ldbc => {
                        ldbc_clustering(idx, &neighbors[idx], &successor_sets)
                    }
                }
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
                match definition {
                    DirectedLcc::Fagiolo => {
                        directed_clustering(idx, &predecessor_sets, &successor_sets)
                    }
                    DirectedLcc::Ldbc => {
                        ldbc_clustering(idx, &neighbors[idx], &successor_sets)
                    }
                }
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

    /// The shipped `example-directed` Graphalytics reference graph.
    fn example_directed() -> (GraphView, Vec<(u64, u64)>) {
        let edges: Vec<(u64, u64)> = vec![
            (1, 3), (1, 5), (2, 4), (2, 5), (2, 10), (3, 1), (3, 5), (3, 8),
            (3, 10), (5, 3), (5, 4), (5, 8), (6, 3), (6, 4), (7, 4), (8, 1),
            (9, 4),
        ];
        let ids: Vec<NodeId> = (1..=10).collect();
        let node_to_index: HashMap<NodeId, usize> =
            ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        let mut outgoing = vec![Vec::new(); 10];
        let mut incoming = vec![Vec::new(); 10];
        for &(a, b) in &edges {
            outgoing[node_to_index[&a]].push(node_to_index[&b]);
            incoming[node_to_index[&b]].push(node_to_index[&a]);
        }
        (
            GraphView::from_adjacency_list(10, ids, node_to_index, outgoing, incoming, None),
            edges,
        )
    }

    /// The expected-output file that ships beside the dataset.
    const LDBC_EXPECTED: [(NodeId, f64); 10] = [
        (1, 0.666_666_666_666_666_6),
        (2, 0.166_666_666_666_666_7),
        (3, 0.15),
        (4, 0.05),
        (5, 0.25),
        (6, 0.0),
        (7, 0.0),
        (8, 0.833_333_333_333_333_4),
        (9, 0.0),
        (10, 0.0),
    ];

    #[test]
    fn the_ldbc_definition_reproduces_the_graphalytics_reference() {
        // The bench validated against this file the whole time and nothing ran
        // it, so "Graphalytics 12/12" sat in the README while directed LCC
        // answered a different question (#916).
        let (view, _) = example_directed();
        let got = local_clustering_coefficient_with(&view, true, DirectedLcc::Ldbc);
        for (id, want) in LDBC_EXPECTED {
            let g = got.coefficients[&id];
            assert!(
                (g - want).abs() < 1e-9,
                "node {id}: LDBC expects {want}, got {g}"
            );
        }
    }

    #[test]
    fn fagiolo_stays_the_default_and_stays_different() {
        // ALGO-02 compares against NetworkX and igraph, which implement
        // Fagiolo; changing the default to satisfy ALGO-08 would have traded
        // one green suite for another.
        let (view, _) = example_directed();
        let fagiolo = local_clustering_coefficient_with(&view, true, DirectedLcc::Fagiolo);
        let default = local_clustering_coefficient_directed(&view, true);
        assert_eq!(
            fagiolo.coefficients[&1], default.coefficients[&1],
            "the unparameterised entry point is still Fagiolo"
        );
        // And it is genuinely a different formula, not a tolerance: node 1 has
        // one reciprocal pair, so Fagiolo divides by 6*5-2 rather than 6*5.
        assert!(
            (fagiolo.coefficients[&1] - LDBC_EXPECTED[0].1).abs() > 1e-3,
            "the two definitions disagree where reciprocal edges exist"
        );
    }

    #[test]
    fn the_definition_is_ignored_when_the_graph_is_undirected() {
        // Both references agree on the undirected coefficient, and
        // `example-undirected` passes under either. Asserting it here keeps a
        // future change to the dispatch from quietly splitting them.
        let (view, _) = example_directed();
        assert_eq!(
            local_clustering_coefficient_with(&view, false, DirectedLcc::Ldbc).coefficients,
            local_clustering_coefficient_with(&view, false, DirectedLcc::Fagiolo).coefficients,
        );
    }

    #[test]
    fn a_node_with_fewer_than_two_neighbours_is_zero_under_both() {
        // d(d-1) is zero there, and dividing by it is how an LCC becomes NaN.
        let (view, _) = example_directed();
        for def in [DirectedLcc::Ldbc, DirectedLcc::Fagiolo] {
            let got = local_clustering_coefficient_with(&view, true, def);
            for (id, cc) in &got.coefficients {
                assert!(cc.is_finite(), "node {id} under {def:?} is {cc}");
            }
            assert_eq!(got.coefficients[&7], 0.0, "{def:?}");
            assert_eq!(got.coefficients[&9], 0.0, "{def:?}");
        }
    }

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
