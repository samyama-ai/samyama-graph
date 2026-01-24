//! Community detection algorithms
//!
//! Implements REQ-ALGO-004 (Weakly Connected Components)

use super::common::{GraphView, NodeId};
use std::collections::HashMap;

/// Result of WCC algorithm
pub struct WccResult {
    /// Map of Component ID -> List of NodeIds
    pub components: HashMap<usize, Vec<NodeId>>,
    /// Map of NodeId -> Component ID
    pub node_component: HashMap<NodeId, usize>,
}

/// Union-Find data structure
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(size: usize) -> Self {
        UnionFind {
            parent: (0..size).collect(),
            rank: vec![0; size],
        }
    }

    fn find(&mut self, i: usize) -> usize {
        if self.parent[i] != i {
            self.parent[i] = self.find(self.parent[i]); // Path compression
        }
        self.parent[i]
    }

    fn union(&mut self, i: usize, j: usize) {
        let root_i = self.find(i);
        let root_j = self.find(j);

        if root_i != root_j {
            if self.rank[root_i] < self.rank[root_j] {
                self.parent[root_i] = root_j;
            } else if self.rank[root_i] > self.rank[root_j] {
                self.parent[root_j] = root_i;
            } else {
                self.parent[root_j] = root_i;
                self.rank[root_i] += 1;
            }
        }
    }
}

/// Weakly Connected Components (WCC)
///
/// Finds all disjoint subgraphs in the graph.
/// Ignores edge direction.
pub fn weakly_connected_components(view: &GraphView) -> WccResult {
    let n = view.node_count;
    let mut uf = UnionFind::new(n);

    // Iterate all edges and Union connected nodes
    for u_idx in 0..n {
        for &v_idx in &view.outgoing[u_idx] {
            uf.union(u_idx, v_idx);
        }
    }

    // Build results
    let mut components = HashMap::new();
    let mut node_component = HashMap::new();

    for i in 0..n {
        let root = uf.find(i);
        let node_id = view.index_to_node[i];
        
        components.entry(root).or_insert_with(Vec::new).push(node_id);
        node_component.insert(node_id, root);
    }

    WccResult {
        components,
        node_component,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_wcc() {
        // Manually build a GraphView for testing
        // Nodes: 1, 2, 3, 4, 5, 6
        // Edges: 1->2, 3->4->5, 6 (isolated)
        
        let node_count = 6;
        let index_to_node = vec![1, 2, 3, 4, 5, 6];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() {
            node_to_index.insert(id, i);
        }

        let mut outgoing = vec![vec![]; 6];
        // 1(0)->2(1)
        outgoing[0].push(1);
        // 3(2)->4(3)
        outgoing[2].push(3);
        // 4(3)->5(4)
        outgoing[3].push(4);

        let view = GraphView {
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            incoming: vec![vec![]; 6], // Not needed for WCC
            weights: None,
        };

        let result = weakly_connected_components(&view);

        assert_eq!(result.components.len(), 3);
        
        let c1 = *result.node_component.get(&1).unwrap();
        let c2 = *result.node_component.get(&2).unwrap();
        assert_eq!(c1, c2);

        let c3 = *result.node_component.get(&3).unwrap();
        let c4 = *result.node_component.get(&4).unwrap();
        let c5 = *result.node_component.get(&5).unwrap();
        assert_eq!(c3, c4);
        assert_eq!(c4, c5);
        assert_ne!(c1, c3);
    }
}