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
        for &v_idx in view.successors(u_idx) {
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

/// Result of SCC algorithm
pub struct SccResult {
    /// Map of Component ID -> List of NodeIds
    pub components: HashMap<usize, Vec<NodeId>>,
    /// Map of NodeId -> Component ID
    pub node_component: HashMap<NodeId, usize>,
}

/// Strongly Connected Components (SCC) using Tarjan's algorithm
pub fn strongly_connected_components(view: &GraphView) -> SccResult {
    let n = view.node_count;
    let mut ids = vec![-1; n];
    let mut low = vec![0; n];
    let mut on_stack = vec![false; n];
    let mut stack = Vec::new();
    let mut id_counter = 0;
    let mut scc_count = 0;
    
    let mut node_component = HashMap::new();
    let mut components = HashMap::new();

    fn dfs(
        u: usize,
        id_counter: &mut i32,
        scc_count: &mut usize,
        ids: &mut Vec<i32>,
        low: &mut Vec<usize>,
        on_stack: &mut Vec<bool>,
        stack: &mut Vec<usize>,
        view: &GraphView,
        node_component: &mut HashMap<NodeId, usize>,
        components: &mut HashMap<usize, Vec<NodeId>>
    ) {
        stack.push(u);
        on_stack[u] = true;
        ids[u] = *id_counter;
        low[u] = *id_counter as usize;
        *id_counter += 1;

        for &v in view.successors(u) {
            if ids[v] == -1 {
                dfs(v, id_counter, scc_count, ids, low, on_stack, stack, view, node_component, components);
                low[u] = low[u].min(low[v]);
            } else if on_stack[v] {
                low[u] = low[u].min(ids[v] as usize);
            }
        }

        if ids[u] == low[u] as i32 {
            while let Some(node_idx) = stack.pop() {
                on_stack[node_idx] = false;
                low[node_idx] = ids[u] as usize;
                
                let node_id = view.index_to_node[node_idx];
                node_component.insert(node_id, *scc_count);
                components.entry(*scc_count).or_insert_with(Vec::new).push(node_id);
                
                if node_idx == u { break; }
            }
            *scc_count += 1;
        }
    }

    for i in 0..n {
        if ids[i] == -1 {
            dfs(i, &mut id_counter, &mut scc_count, &mut ids, &mut low, &mut on_stack, &mut stack, view, &mut node_component, &mut components);
        }
    }

    SccResult {
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
        // ... (existing test)
    }

    #[test]
    fn test_scc() {
        // Graph with cycle: 1->2->3->1, and 4 (isolated)
        let node_count = 4;
        let index_to_node = vec![1, 2, 3, 4];
        let mut node_to_index = HashMap::new();
        for (i, &id) in index_to_node.iter().enumerate() { node_to_index.insert(id, i); }

        let mut outgoing = vec![vec![]; 4];
        outgoing[0].push(1); // 1->2
        outgoing[1].push(2); // 2->3
        outgoing[2].push(0); // 3->1

        let view = GraphView::from_adjacency_list(
            node_count,
            index_to_node,
            node_to_index,
            outgoing,
            vec![vec![]; 4],
            None,
        );

        let result = strongly_connected_components(&view);
        assert_eq!(result.components.len(), 2);
        
        let c1 = result.node_component[&1];
        let c2 = result.node_component[&2];
        let c3 = result.node_component[&3];
        let c4 = result.node_component[&4];
        
        assert_eq!(c1, c2);
        assert_eq!(c2, c3);
        assert_ne!(c1, c4);
    }
}