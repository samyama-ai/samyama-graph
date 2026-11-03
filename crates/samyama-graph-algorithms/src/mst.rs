//! Minimum Spanning Tree algorithms
//!
//! Implements Prim's algorithm for MST.

use super::common::{GraphView, NodeId};
use std::collections::{HashSet, BinaryHeap};
use std::cmp::Ordering;

pub struct MSTResult {
    pub total_weight: f64,
    pub edges: Vec<(NodeId, NodeId, f64)>, // (source, target, weight)
    /// How many components the forest spans.
    ///
    /// 1 is a spanning tree. More than 1 means the graph is disconnected and
    /// `edges` is a spanning *forest* -- which is the right answer, and is not
    /// what a caller expecting "the MST" will assume. Reporting it is what lets
    /// them tell the two apart (#1302).
    pub components: usize,
}

#[derive(Copy, Clone, PartialEq)]
struct EdgeState {
    weight: f64,
    source: usize,
    target: usize,
}

impl Eq for EdgeState {}

impl Ord for EdgeState {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap. Equal weights are then broken by target and
        // source index, ascending, so the tree is the same tree on every run.
        // Weight alone left the choice to the heap's internal order, which is
        // reproducible for one input and arbitrary as a rule -- so two graphs
        // that differ only in insertion order gave different trees of the same
        // total weight, and nothing said which was expected.
        other
            .weight
            .partial_cmp(&self.weight)
            .unwrap_or(Ordering::Equal)
            .then(other.target.cmp(&self.target))
            .then(other.source.cmp(&self.source))
    }
}

impl PartialOrd for EdgeState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Prim's Algorithm for Minimum Spanning Tree
///
/// Treats the graph as undirected: edge direction is ignored, and an edge is
/// found from either endpoint.
///
/// On a disconnected graph this returns the minimum spanning **forest** -- one
/// tree per component, `total_weight` summed over all of them, and
/// `components` saying how many there are. It used to return the tree of
/// whichever component held internal index 0 and say nothing, so a caller with
/// a disconnected graph got a correct MST of a graph they had not asked about
/// (#1302).
///
/// Roots are taken in ascending index order and equal weights break by index,
/// so the forest is the same forest on every run.
pub fn prim_mst(view: &GraphView) -> MSTResult {
    if view.node_count == 0 {
        return MSTResult { total_weight: 0.0, edges: Vec::new(), components: 0 };
    }

    let mut visited = HashSet::new();
    let mut mst_edges = Vec::new();
    let mut total_weight = 0.0;
    let mut components = 0;

    for start_idx in 0..view.node_count {
        if visited.contains(&start_idx) {
            continue;
        }
        components += 1;
        visited.insert(start_idx);

        let mut heap = BinaryHeap::new();
        add_edges(view, start_idx, &mut heap, &visited);

        while let Some(EdgeState { weight, source, target }) = heap.pop() {
            if visited.contains(&target) {
                continue;
            }

            visited.insert(target);
            mst_edges.push((
                view.index_to_node[source],
                view.index_to_node[target],
                weight
            ));
            total_weight += weight;

            add_edges(view, target, &mut heap, &visited);
        }
    }

    MSTResult {
        total_weight,
        edges: mst_edges,
        components,
    }
}

fn add_edges(view: &GraphView, u: usize, heap: &mut BinaryHeap<EdgeState>, visited: &HashSet<usize>) {
    // Check outgoing edges
    let u_out = view.successors(u);
    for (i, &v) in u_out.iter().enumerate() {
        if !visited.contains(&v) {
            let weight = view.weights(u).map(|w| w[i]).unwrap_or(1.0);
            heap.push(EdgeState { weight, source: u, target: v });
        }
    }

    // Check incoming edges (treat as undirected)
    let u_in = view.predecessors(u);
    for &_v in u_in.iter() {
         let v = _v; // explicit copy
         if !visited.contains(&v) {
            // Need to find weight in incoming list? 
            // GraphView structure: incoming[u] contains v implies edge v->u exists.
            
            let v_out = view.successors(v);
            if let Some(idx) = v_out.iter().position(|&x| x == u) {
                let weight = view.weights(v).map(|w| w[idx]).unwrap_or(1.0);
                heap.push(EdgeState { weight, source: u, target: v }); // "source" here is just the connection point in MST
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_prim_mst() {
        // Triangle: 1-2 (1), 2-3 (2), 1-3 (10)
        // MST should be 1-2, 2-3. Total 3.
        
        let node_count = 3;
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0); node_to_index.insert(2, 1); node_to_index.insert(3, 2);

        let mut outgoing = vec![vec![]; 3];
        let mut incoming = vec![vec![]; 3];
        let mut weights = vec![vec![]; 3];

        // 1->2 (1)
        outgoing[0].push(1); incoming[1].push(0); weights[0].push(1.0);
        // 2->1 (1) - Undirected explicitly stored?
        outgoing[1].push(0); incoming[0].push(1); weights[1].push(1.0);

        // 2->3 (2)
        outgoing[1].push(2); incoming[2].push(1); weights[1].push(2.0);
        // 3->2 (2)
        outgoing[2].push(1); incoming[1].push(2); weights[2].push(2.0);

        // 1->3 (10)
        outgoing[0].push(2); incoming[2].push(0); weights[0].push(10.0);
        // 3->1 (10)
        outgoing[2].push(0); incoming[0].push(2); weights[2].push(10.0);

        let view = GraphView::from_adjacency_list(
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            Some(weights),
        );

        let result = prim_mst(&view);
        assert_eq!(result.total_weight, 3.0);
        assert_eq!(result.edges.len(), 2);
        assert_eq!(result.components, 1, "a connected graph spans in one tree");
    }

    /// Two triangles with no edge between them.
    ///
    /// The old answer was the tree of whichever component held index 0: three
    /// nodes, two edges, weight 3, and nothing to say the other half of the
    /// graph existed. A caller asking for "the MST" of a disconnected graph
    /// cannot be given one -- there isn't one -- so the answer is the forest,
    /// and `components` is what tells them which they got (#1302).
    #[test]
    fn a_disconnected_graph_spans_as_a_forest() {
        // Component A: 1-2 (1), 2-3 (2), 1-3 (10)  -> tree weight 3
        // Component B: 4-5 (4), 5-6 (5), 4-6 (20)  -> tree weight 9
        let node_count = 6;
        let index_to_node: Vec<NodeId> = (1..=6).collect();
        let mut node_to_index = HashMap::new();
        for (i, id) in index_to_node.iter().enumerate() {
            node_to_index.insert(*id, i);
        }
        let mut outgoing = vec![vec![]; 6];
        let mut incoming = vec![vec![]; 6];
        let mut weights = vec![vec![]; 6];
        let mut link = |a: usize, b: usize, w: f64,
                        outgoing: &mut Vec<Vec<usize>>,
                        incoming: &mut Vec<Vec<usize>>,
                        weights: &mut Vec<Vec<f64>>| {
            outgoing[a].push(b);
            incoming[b].push(a);
            weights[a].push(w);
            outgoing[b].push(a);
            incoming[a].push(b);
            weights[b].push(w);
        };
        link(0, 1, 1.0, &mut outgoing, &mut incoming, &mut weights);
        link(1, 2, 2.0, &mut outgoing, &mut incoming, &mut weights);
        link(0, 2, 10.0, &mut outgoing, &mut incoming, &mut weights);
        link(3, 4, 4.0, &mut outgoing, &mut incoming, &mut weights);
        link(4, 5, 5.0, &mut outgoing, &mut incoming, &mut weights);
        link(3, 5, 20.0, &mut outgoing, &mut incoming, &mut weights);

        let view = GraphView::from_adjacency_list(
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            incoming,
            Some(weights),
        );

        let result = prim_mst(&view);
        assert_eq!(result.components, 2, "two components, so two trees");
        assert_eq!(result.edges.len(), 4, "n - components edges span a forest");
        assert_eq!(result.total_weight, 12.0, "3 for the first tree, 9 for the second");
    }

    /// An edgeless graph is n components and no edges, not one empty tree.
    #[test]
    fn isolated_nodes_are_components_of_their_own() {
        let node_count = 3;
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        for (i, id) in index_to_node.iter().enumerate() {
            node_to_index.insert(*id, i);
        }
        let view = GraphView::from_adjacency_list(
            node_count,
            index_to_node,
            node_to_index,
            vec![vec![]; 3],
            vec![vec![]; 3],
            None,
        );
        let result = prim_mst(&view);
        assert_eq!(result.components, 3);
        assert!(result.edges.is_empty());
        assert_eq!(result.total_weight, 0.0);
    }

    /// Equal weights must give the same tree every run, not a tree of the same
    /// weight. The heap used to decide, ordering on weight alone.
    #[test]
    fn equal_weights_break_the_same_way_every_time() {
        let build = || {
            let node_count = 4;
            let index_to_node = vec![1, 2, 3, 4];
            let mut node_to_index = HashMap::new();
            for (i, id) in index_to_node.iter().enumerate() {
                node_to_index.insert(*id, i);
            }
            let mut outgoing = vec![vec![]; 4];
            let mut incoming = vec![vec![]; 4];
            let mut weights = vec![vec![]; 4];
            // Every edge weighs 1, so every spanning tree ties.
            for (a, b) in [(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)] {
                outgoing[a].push(b);
                incoming[b].push(a);
                weights[a].push(1.0);
                outgoing[b].push(a);
                incoming[a].push(b);
                weights[b].push(1.0);
            }
            GraphView::from_adjacency_list(
                node_count,
                index_to_node,
                node_to_index,
                outgoing,
                incoming,
                Some(weights),
            )
        };

        let first = prim_mst(&build()).edges;
        for _ in 0..5 {
            assert_eq!(prim_mst(&build()).edges, first, "the tie-break is not stable");
        }
        assert_eq!(first.len(), 3);
    }
}
