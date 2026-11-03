//! Community detection algorithms
//!
//! Weakly connected components. Counted towards ALGO-01 (algorithms callable
//! from Cypher).

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
///
/// Iterative, not recursive (#2043). The recursive form made one call per node
/// deeper in the search, so a large enough component overflowed the thread
/// stack -- 20,000 nodes with 10 edges each did it on a 2 MB server worker --
/// and a stack overflow cannot be caught: it aborted the whole server. The
/// search state now lives in `frames` on the heap. The steps, and so the
/// component numbering and the order of nodes within each component, are
/// exactly those of the recursive version.
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

    // One frame per node being visited: the node, and the index of the next
    // successor to look at. The recursive version kept the same two things in
    // its call stack.
    let mut frames: Vec<(usize, usize)> = Vec::new();

    // Entering `u`: what the recursive call did first.
    macro_rules! visit {
        ($u:expr) => {{
            let u = $u;
            stack.push(u);
            on_stack[u] = true;
            ids[u] = id_counter;
            low[u] = id_counter as usize;
            id_counter += 1;
            frames.push((u, 0));
        }};
    }

    for root in 0..n {
        if ids[root] != -1 {
            continue;
        }
        visit!(root);
        while let Some(&mut (u, ref mut next)) = frames.last_mut() {
            let successors = view.successors(u);
            // Skip ahead to the next unvisited successor, folding in the
            // `low` of every successor already on the stack on the way.
            let mut descend = None;
            while *next < successors.len() {
                let v = successors[*next];
                *next += 1;
                if ids[v] == -1 {
                    descend = Some(v);
                    break;
                } else if on_stack[v] {
                    low[u] = low[u].min(ids[v] as usize);
                }
            }
            if let Some(v) = descend {
                // The recursive call. `next` is already past `v`, so `u`
                // resumes at its next successor once `v` is finished.
                visit!(v);
                continue;
            }

            // Every successor of `u` is done: what the recursive call did
            // before returning.
            frames.pop();
            if ids[u] == low[u] as i32 {
                while let Some(node_idx) = stack.pop() {
                    on_stack[node_idx] = false;
                    low[node_idx] = ids[u] as usize;

                    let node_id = view.index_to_node[node_idx];
                    node_component.insert(node_id, scc_count);
                    components.entry(scc_count).or_insert_with(Vec::new).push(node_id);

                    if node_idx == u { break; }
                }
                scc_count += 1;
            }
            // Back in the caller: `low[u] = low[u].min(low[v])`.
            if let Some(&(parent, _)) = frames.last() {
                low[parent] = low[parent].min(low[u]);
            }
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

    // --- #2043: iterative SCC ----------------------------------------------

    /// The recursive Tarjan this module used before #2043, kept as the
    /// reference the iterative one must match exactly. Verbatim but for
    /// `or_default()` in place of `or_insert_with(Vec::new)`, the same thing.
    fn recursive_scc(view: &GraphView) -> SccResult {
        let n = view.node_count;
        let mut ids = vec![-1; n];
        let mut low = vec![0; n];
        let mut on_stack = vec![false; n];
        let mut stack = Vec::new();
        let mut id_counter = 0;
        let mut scc_count = 0;
        let mut node_component = HashMap::new();
        let mut components = HashMap::new();

        #[allow(clippy::too_many_arguments)]
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
            components: &mut HashMap<usize, Vec<NodeId>>,
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
                    components.entry(*scc_count).or_default().push(node_id);
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
        SccResult { components, node_component }
    }

    /// A view over `n` nodes (ids `100 + index`, so ids and indexes differ)
    /// with the given directed edges.
    fn view_of(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let index_to_node: Vec<NodeId> = (0..n as u64).map(|i| 100 + i).collect();
        let node_to_index = index_to_node.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        let mut outgoing = vec![Vec::new(); n];
        let mut incoming = vec![Vec::new(); n];
        for &(a, b) in edges {
            outgoing[a].push(b);
            incoming[b].push(a);
        }
        GraphView::from_adjacency_list(n, index_to_node, node_to_index, outgoing, incoming, None)
    }

    /// Exactly the same answer, not only the same partition: component
    /// numbers and the order of nodes inside each component are what callers
    /// see (`componentId`, CH-ALGO-PARITY), so they must not move.
    fn assert_same(view: &GraphView, what: &str) {
        let (a, b) = (strongly_connected_components(view), recursive_scc(view));
        assert_eq!(a.components, b.components, "{what}: components");
        assert_eq!(a.node_component, b.node_component, "{what}: node_component");
    }

    #[test]
    fn iterative_scc_matches_the_recursive_one_exactly() {
        // Hand-made shapes: empty, a self-loop, two cycles joined one way,
        // nested cycles, a node reached from two components.
        assert_same(&view_of(0, &[]), "empty");
        assert_same(&view_of(1, &[(0, 0)]), "self-loop");
        assert_same(&view_of(6, &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 3)]), "two cycles");
        assert_same(&view_of(5, &[(0, 1), (1, 2), (2, 1), (2, 3), (3, 0), (1, 4)]), "nested");
        assert_same(&view_of(5, &[(0, 1), (1, 0), (2, 3), (3, 2), (0, 4), (2, 4)]), "shared sink");

        // And many pseudo-random graphs, sparse to dense, with parallel edges
        // and self-loops left in.
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for case in 0..300 {
            let n = 1 + (next() % 60) as usize;
            let m = (next() % (3 * n as u64 + 1)) as usize;
            let edges: Vec<_> = (0..m)
                .map(|_| ((next() % n as u64) as usize, (next() % n as u64) as usize))
                .collect();
            assert_same(&view_of(n, &edges), &format!("random case {case} (n={n}, m={m})"));
        }
    }

    /// Run `f` on a thread with a deliberately small stack. The recursive
    /// version needs a frame per node of depth, so it overflows here long
    /// before the graphs below are done; the iterative one needs none.
    fn on_small_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        std::thread::Builder::new()
            .stack_size(64 * 1024)
            .spawn(f)
            .unwrap()
            .join()
            .unwrap()
    }

    const DEEP: usize = 200_000;

    #[test]
    fn a_cycle_deeper_than_the_stack_is_one_component_not_a_crash() {
        let result = on_small_stack(|| {
            let edges: Vec<_> = (0..DEEP).map(|i| (i, (i + 1) % DEEP)).collect();
            strongly_connected_components(&view_of(DEEP, &edges))
        });
        assert_eq!(result.components.len(), 1);
        assert_eq!(result.components[&0].len(), DEEP);
    }

    #[test]
    fn a_chain_deeper_than_the_stack_is_one_component_per_node() {
        let result = on_small_stack(|| {
            let edges: Vec<_> = (0..DEEP - 1).map(|i| (i, i + 1)).collect();
            strongly_connected_components(&view_of(DEEP, &edges))
        });
        assert_eq!(result.components.len(), DEEP);
        // The search finishes the far end of the chain first.
        assert_eq!(result.node_component[&(100 + DEEP as u64 - 1)], 0);
        assert_eq!(result.node_component[&100], DEEP - 1);
    }
}