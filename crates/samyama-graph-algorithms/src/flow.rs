//! Network flow algorithms
//!
//! Implements Max Flow using Edmonds-Karp algorithm (BFS-based Ford-Fulkerson).

use super::common::{GraphView, NodeId};
use std::collections::{HashMap, VecDeque};

pub struct FlowResult {
    pub max_flow: f64,
}

/// Edmonds-Karp Algorithm for Max Flow
///
/// Assumes `view.weights` represents capacity.
/// If weights are missing, assumes capacity 1.0.
pub fn edmonds_karp(view: &GraphView, source: NodeId, sink: NodeId) -> Option<FlowResult> {
    let s_idx = *view.node_to_index.get(&source)?;
    let t_idx = *view.node_to_index.get(&sink)?;

    let n = view.node_count;
    
    // Build residual graph capacity matrix (or adjacency map for sparse)
    // Since GraphView is adjacency list, let's map it to a residual structure
    // (u_idx, v_idx) -> capacity
    // We need backward edges too.
    
    // Using a vector of hashmaps for residual graph: residual[u][v] = capacity
    let mut residual: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];

    for u in 0..n {
        let edges = &view.outgoing[u];
        let weights = view.weights.as_ref().map(|w| &w[u]);

        for (i, &v) in edges.iter().enumerate() {
            let cap = if let Some(w) = weights { w[i] } else { 1.0 };
            *residual[u].entry(v).or_insert(0.0) += cap;
            // Ensure backward edge exists in map with 0 capacity if not present
            residual[v].entry(u).or_insert(0.0);
        }
    }

    let mut total_flow = 0.0;

    loop {
        // Find path using BFS
        let mut parent = vec![None; n];
        let mut queue = VecDeque::new();
        queue.push_back(s_idx);
        
        // visited array not strictly needed if we check parent (but source has no parent)
        // using a separate visited set or checking if u == s or parent[u] is set
        let mut found_path = false;
        
        // Special marker for source parent to distinguish from unvisited
        // Actually, just use a visited bitset or map
        let mut visited = vec![false; n];
        visited[s_idx] = true;

        while let Some(u) = queue.pop_front() {
            if u == t_idx {
                found_path = true;
                break;
            }

            for (&v, &cap) in &residual[u] {
                if !visited[v] && cap > 1e-9 {
                    visited[v] = true;
                    parent[v] = Some(u);
                    queue.push_back(v);
                }
            }
        }

        if !found_path {
            break;
        }

        // Calculate path flow
        let mut path_flow = f64::INFINITY;
        let mut curr = t_idx;
        while curr != s_idx {
            let prev = parent[curr].unwrap();
            let cap = residual[prev][&curr];
            if cap < path_flow {
                path_flow = cap;
            }
            curr = prev;
        }

        // Update residual capacities
        curr = t_idx;
        while curr != s_idx {
            let prev = parent[curr].unwrap();
            
            *residual[prev].get_mut(&curr).unwrap() -= path_flow;
            *residual[curr].get_mut(&prev).unwrap() += path_flow;
            
            curr = prev;
        }

        total_flow += path_flow;
    }

    Some(FlowResult { max_flow: total_flow })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_edmonds_karp() {
        // 0 -> 1 (10)
        // 0 -> 2 (10)
        // 1 -> 2 (2)
        // 1 -> 3 (4)
        // 2 -> 4 (9)
        // 3 -> 5 (10)
        // 4 -> 3 (6)
        // 4 -> 5 (10)
        
        // Let's use a simpler diamond graph
        // S(1) -> A(2) (100)
        // S(1) -> B(3) (50)
        // A(2) -> B(3) (50)
        // A(2) -> T(4) (50)
        // B(3) -> T(4) (100)
        
        // Expected max flow:
        // S->A->T: 50
        // S->B->T: 50
        // S->A->B->T: 50
        // Total: 150?
        // S->A (100) splits to A->B (50) and A->T (50).
        // S->B (50) joins A->B (50) making 100 at B? No, capacity constraints.
        
        // S->A->T: Flow 50. Residual: S->A 50, A->T 0.
        // S->B->T: Flow 50. Residual: S->B 0, B->T 50.
        // S->A->B->T: S->A (50 left), A->B (50), B->T (50 left). Flow 50.
        // Total 150. Correct.

        let node_count = 4;
        let index_to_node = vec![1, 2, 3, 4];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() { node_to_index.insert(id, i); }

        let mut outgoing = vec![vec![]; 4];
        let mut weights = vec![vec![]; 4];

        // S->A
        outgoing[0].push(1); weights[0].push(100.0);
        // S->B
        outgoing[0].push(2); weights[0].push(50.0);
        // A->B
        outgoing[1].push(2); weights[1].push(50.0);
        // A->T
        outgoing[1].push(3); weights[1].push(50.0);
        // B->T
        outgoing[2].push(3); weights[2].push(100.0);

        let view = GraphView {
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            incoming: vec![vec![]; 4],
            weights: Some(weights),
        };

        let result = edmonds_karp(&view, 1, 4).unwrap();
        assert_eq!(result.max_flow, 150.0);
    }
}
