//! Pathfinding algorithms
//!
//! Implements REQ-ALGO-002 (BFS) and REQ-ALGO-003 (Dijkstra)

use super::common::GraphView;
use crate::graph::{GraphStore, NodeId, PropertyValue};
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
    store: &GraphStore,
    source: NodeId,
    target: NodeId,
    edge_type: Option<&str>,
) -> Option<PathResult> {
    let mut queue = VecDeque::new();
    let mut visited = HashMap::new(); // node -> parent
    
    queue.push_back(source);
    visited.insert(source, None);

    while let Some(current) = queue.pop_front() {
        if current == target {
            // Reconstruct path
            let mut path = Vec::new();
            let mut curr = Some(target);
            while let Some(node) = curr {
                path.push(node);
                if let Some(parent) = visited.get(&node) {
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

        for edge in store.get_outgoing_edges(current) {
            if let Some(et) = edge_type {
                if edge.edge_type.as_str() != et {
                    continue;
                }
            }

            if !visited.contains_key(&edge.target) {
                visited.insert(edge.target, Some(current));
                queue.push_back(edge.target);
            }
        }
    }

    None
}

/// State for Dijkstra priority queue
#[derive(Copy, Clone, PartialEq)]
struct State {
    cost: f64,
    node: NodeId,
}

// Rust's BinaryHeap is max-heap, so we implement Ord reversed for min-heap behavior
impl Eq for State {}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare costs reversed
        other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Dijkstra's Algorithm (Weighted Shortest Path)
pub fn dijkstra(
    store: &GraphStore,
    source: NodeId,
    target: NodeId,
    weight_property: &str,
    edge_type: Option<&str>,
) -> Option<PathResult> {
    let mut dist = HashMap::new();
    let mut parent = HashMap::new();
    let mut heap = BinaryHeap::new();

    dist.insert(source, 0.0);
    heap.push(State { cost: 0.0, node: source });

    while let Some(State { cost, node }) = heap.pop() {
        if node == target {
            // Reconstruct path
            let mut path = Vec::new();
            let mut curr = Some(target);
            while let Some(n) = curr {
                path.push(n);
                curr = parent.get(&n).cloned().flatten();
            }
            path.reverse();
            return Some(PathResult {
                source,
                target,
                path,
                cost,
            });
        }

        // Optimization: if we found a shorter path to this node already, skip
        if cost > *dist.get(&node).unwrap_or(&f64::INFINITY) {
            continue;
        }

        for edge in store.get_outgoing_edges(node) {
            if let Some(et) = edge_type {
                if edge.edge_type.as_str() != et {
                    continue;
                }
            }

            // Get weight
            let weight = match edge.get_property(weight_property) {
                Some(PropertyValue::Integer(i)) => *i as f64,
                Some(PropertyValue::Float(f)) => *f,
                _ => 1.0, // Default weight if property missing or invalid type
            };

            if weight < 0.0 {
                // Dijkstra doesn't support negative weights
                continue; 
            }

            let next_cost = cost + weight;
            let next_node = edge.target;

            if next_cost < *dist.get(&next_node).unwrap_or(&f64::INFINITY) {
                dist.insert(next_node, next_cost);
                parent.insert(next_node, Some(node));
                heap.push(State { cost: next_cost, node: next_node });
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;

    #[test]
    fn test_bfs() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Node");
        let n2 = store.create_node("Node");
        let n3 = store.create_node("Node");

        // n1 -> n2 -> n3
        store.create_edge(n1, n2, "LINK").unwrap();
        store.create_edge(n2, n3, "LINK").unwrap();

        let result = bfs(&store, n1, n3, None).unwrap();
        assert_eq!(result.path, vec![n1, n2, n3]);
        assert_eq!(result.cost, 2.0);
    }

    #[test]
    fn test_dijkstra() {
        let mut store = GraphStore::new();
        let n1 = store.create_node("Node");
        let n2 = store.create_node("Node");
        let n3 = store.create_node("Node");

        // n1 -> n2 (weight 10)
        let e1 = store.create_edge(n1, n2, "LINK").unwrap();
        store.get_edge_mut(e1).unwrap().set_property("cost", 10.0);

        // n2 -> n3 (weight 5)
        let e2 = store.create_edge(n2, n3, "LINK").unwrap();
        store.get_edge_mut(e2).unwrap().set_property("cost", 5.0);

        // n1 -> n3 (weight 50) -- direct but expensive
        let e3 = store.create_edge(n1, n3, "LINK").unwrap();
        store.get_edge_mut(e3).unwrap().set_property("cost", 50.0);

        // Shortest path should be n1->n2->n3 (cost 15)
        let result = dijkstra(&store, n1, n3, "cost", None).unwrap();
        
        assert_eq!(result.path, vec![n1, n2, n3]);
        assert_eq!(result.cost, 15.0);
    }
}
