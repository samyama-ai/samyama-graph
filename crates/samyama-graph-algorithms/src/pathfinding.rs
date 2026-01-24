//! Pathfinding algorithms
//!
//! Implements REQ-ALGO-002 (BFS) and REQ-ALGO-003 (Dijkstra)

use super::common::{GraphView, NodeId};
use std::collections::{HashMap, VecDeque, BinaryHeap};
use std::cmp::Ordering;

/// Result of a pathfinding algorithm
#[derive(Debug, Clone)]
pub struct PathResult {
    pub source: NodeId,
    pub target: NodeId,
    pub path: Vec<NodeId>,
    pub cost: f64,
}

/// Breadth-First Search (Unweighted Shortest Path)
pub fn bfs(
    view: &GraphView,
    source: NodeId,
    target: NodeId,
) -> Option<PathResult> {
    let source_idx = *view.node_to_index.get(&source)?;
    let target_idx = *view.node_to_index.get(&target)?;

    let mut queue = VecDeque::new();
    let mut visited = HashMap::new(); // index -> parent_index
    
    queue.push_back(source_idx);
    visited.insert(source_idx, None);

    while let Some(current_idx) = queue.pop_front() {
        if current_idx == target_idx {
            // Reconstruct path
            let mut path = Vec::new();
            let mut curr = Some(target_idx);
            while let Some(idx) = curr {
                path.push(view.index_to_node[idx]);
                if let Some(parent) = visited.get(&idx) {
                    curr = *parent;
                } else {
                    curr = None;
                }
            }
            path.reverse();
            return Some(PathResult {
                source,
                target,
                cost: (path.len() - 1) as f64,
                path,
            });
        }

        for &next_idx in &view.outgoing[current_idx] {
            if !visited.contains_key(&next_idx) {
                visited.insert(next_idx, Some(current_idx));
                queue.push_back(next_idx);
            }
        }
    }

    None
}

/// State for Dijkstra priority queue
#[derive(Copy, Clone, PartialEq)]
struct State {
    cost: f64,
    node_idx: usize,
}

impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare costs reversed for min-heap
        other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra's Algorithm (Weighted Shortest Path)
///
/// Uses edge weights from GraphView if available, otherwise assumes 1.0.
pub fn dijkstra(
    view: &GraphView,
    source: NodeId,
    target: NodeId,
) -> Option<PathResult> {
    let source_idx = *view.node_to_index.get(&source)?;
    let target_idx = *view.node_to_index.get(&target)?;

    let mut dist = HashMap::new();
    let mut parent = HashMap::new();
    let mut heap = BinaryHeap::new();

    dist.insert(source_idx, 0.0);
    heap.push(State { cost: 0.0, node_idx: source_idx });

    while let Some(State { cost, node_idx }) = heap.pop() {
        if node_idx == target_idx {
            // Reconstruct path
            let mut path = Vec::new();
            let mut curr = Some(target_idx);
            while let Some(idx) = curr {
                path.push(view.index_to_node[idx]);
                curr = parent.get(&idx).cloned().flatten();
            }
            path.reverse();
            return Some(PathResult {
                source,
                target,
                path,
                cost,
            });
        }

        if cost > *dist.get(&node_idx).unwrap_or(&f64::INFINITY) {
            continue;
        }

        let edges = &view.outgoing[node_idx];
        let weights = view.weights.as_ref().map(|w| &w[node_idx]);

        for (i, &next_idx) in edges.iter().enumerate() {
            let weight = if let Some(w) = weights {
                w[i]
            } else {
                1.0
            };

            if weight < 0.0 { continue; }

            let next_cost = cost + weight;

            if next_cost < *dist.get(&next_idx).unwrap_or(&f64::INFINITY) {
                dist.insert(next_idx, next_cost);
                parent.insert(next_idx, Some(node_idx));
                heap.push(State { cost: next_cost, node_idx: next_idx });
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use crate::common::GraphView;

    #[test]
    fn test_bfs() {
        // 1->2->3
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let mut outgoing = vec![vec![]; 3];
        outgoing[0].push(1);
        outgoing[1].push(2);

        let view = GraphView {
            node_count: 3,
            index_to_node,
            node_to_index,
            outgoing,
            incoming: vec![vec![]; 3],
            weights: None,
        };

        let result = bfs(&view, 1, 3).unwrap();
        assert_eq!(result.path, vec![1, 2, 3]);
        assert_eq!(result.cost, 2.0);
    }

    #[test]
    fn test_dijkstra() {
        // 1->2 (10.0), 2->3 (5.0), 1->3 (50.0)
        let index_to_node = vec![1, 2, 3];
        let mut node_to_index = HashMap::new();
        node_to_index.insert(1, 0);
        node_to_index.insert(2, 1);
        node_to_index.insert(3, 2);

        let mut outgoing = vec![vec![]; 3];
        let mut weights = vec![vec![]; 3];

        outgoing[0].push(1); weights[0].push(10.0);
        outgoing[0].push(2); weights[0].push(50.0); // Direct 1->3
        outgoing[1].push(2); weights[1].push(5.0);

        let view = GraphView {
            node_count: 3,
            index_to_node,
            node_to_index,
            outgoing,
            incoming: vec![vec![]; 3],
            weights: Some(weights),
        };

        let result = dijkstra(&view, 1, 3).unwrap();
        assert_eq!(result.path, vec![1, 2, 3]);
        assert_eq!(result.cost, 15.0);
    }
}