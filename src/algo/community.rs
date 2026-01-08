//! Community detection algorithms
//!
//! Implements REQ-ALGO-004 (Weakly Connected Components)

use crate::graph::{GraphStore, NodeId};
use std::collections::{HashMap, HashSet};

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
pub fn weakly_connected_components(
    store: &GraphStore,
    label: Option<&str>,
    edge_type: Option<&str>,
) -> WccResult {
    // 1. Map nodes to dense indices (0..N) using GraphView logic locally
    // (We iterate store directly to avoid building full adjacency lists if not needed, 
    // but we need the mapping)
    let nodes = if let Some(l) = label {
        store.get_nodes_by_label(&crate::graph::Label::new(l))
    } else {
        store.all_nodes()
    };

    let mut node_to_idx = HashMap::new();
    let mut idx_to_node = Vec::new();

    for (i, node) in nodes.iter().enumerate() {
        node_to_idx.insert(node.id, i);
        idx_to_node.push(node.id);
    }

    let n = idx_to_node.len();
    let mut uf = UnionFind::new(n);

    // 2. Iterate all edges and Union connected nodes
    for u_id in &idx_to_node {
        if let Some(&u_idx) = node_to_idx.get(u_id) {
            for edge in store.get_outgoing_edges(*u_id) {
                if let Some(et) = edge_type {
                    if edge.edge_type.as_str() != et {
                        continue;
                    }
                }

                if let Some(&v_idx) = node_to_idx.get(&edge.target) {
                    uf.union(u_idx, v_idx);
                }
            }
        }
    }

    // 3. Build results
    let mut components = HashMap::new();
    let mut node_component = HashMap::new();

    for i in 0..n {
        let root = uf.find(i);
        let node_id = idx_to_node[i];
        
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
    use crate::graph::GraphStore;

    #[test]
    fn test_wcc() {
        let mut store = GraphStore::new();
        
        // Component 1: 1-2
        let n1 = store.create_node("Node");
        let n2 = store.create_node("Node");
        store.create_edge(n1, n2, "LINK").unwrap();

        // Component 2: 3-4-5
        let n3 = store.create_node("Node");
        let n4 = store.create_node("Node");
        let n5 = store.create_node("Node");
        store.create_edge(n3, n4, "LINK").unwrap();
        store.create_edge(n4, n5, "LINK").unwrap();

        // Component 3: 6 (isolated)
        let n6 = store.create_node("Node");

        let result = weakly_connected_components(&store, None, None);

        assert_eq!(result.components.len(), 3);
        
        let c1 = *result.node_component.get(&n1).unwrap();
        let c2 = *result.node_component.get(&n2).unwrap();
        assert_eq!(c1, c2);

        let c3 = *result.node_component.get(&n3).unwrap();
        let c4 = *result.node_component.get(&n4).unwrap();
        assert_eq!(c3, c4);
        assert_ne!(c1, c3);
    }
}
