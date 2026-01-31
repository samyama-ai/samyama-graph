//! Minimum Spanning Tree algorithms
//!
//! Implements Prim's algorithm for MST.

use super::common::{GraphView, NodeId};
use std::collections::{HashSet, BinaryHeap};
use std::cmp::Ordering;

pub struct MSTResult {
    pub total_weight: f64,
    pub edges: Vec<(NodeId, NodeId, f64)>, // (source, target, weight)
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
        // Reverse for min-heap
        other.weight.partial_cmp(&self.weight).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for EdgeState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Prim's Algorithm for Minimum Spanning Tree
///
/// Treats graph as undirected (ignores edge direction).
/// If graph is disconnected, returns MST of the component containing `start_node`
/// (or arbitrary node if not specified).
pub fn prim_mst(view: &GraphView) -> MSTResult {
    if view.node_count == 0 {
        return MSTResult { total_weight: 0.0, edges: Vec::new() };
    }

    let start_idx = 0; // Start from first node
    let mut visited = HashSet::new();
    let mut heap = BinaryHeap::new();
    let mut mst_edges = Vec::new();
    let mut total_weight = 0.0;

    visited.insert(start_idx);

    // Add initial edges
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

    MSTResult {
        total_weight,
        edges: mst_edges,
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
    }
}
