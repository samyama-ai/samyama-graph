//! Structural questions answered by one depth-first walk (ALGO-01).
//!
//! | question | answer |
//! |---|---|
//! | is there an order respecting every edge? | [`topological_sort`] |
//! | is there a cycle, and where? | [`find_cycle`] |
//! | which edges, if cut, disconnect the graph? | [`bridges`] |
//! | which nodes, if removed, disconnect it? | [`articulation_points`] |
//!
//! The first two are the same question. A directed graph has a topological
//! order **exactly when** it has no cycle, so Kahn's algorithm answers both:
//! whatever it cannot place is what the cycle runs through. They are separate
//! entry points because a caller wants one of two different things back — an
//! order, or the evidence that there isn't one.
//!
//! The last two are also one algorithm. Tarjan's low-link walk finds bridges
//! and articulation points in the same pass, and they are the same property
//! seen from an edge and from a node: a bridge is an edge whose subtree cannot
//! reach back past it, an articulation point is a node with a child that
//! cannot.
//!
//! Bridges and articulation points are defined on the **undirected** graph.
//! "What breaks if this fails" does not care which way a dependency was
//! written down.

use crate::common::{GraphView, NodeId};

/// A topological order, or the cycle that makes one impossible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopoResult {
    /// Every node, in an order where each edge points forward.
    Order(Vec<NodeId>),
    /// The nodes that could not be placed. Every cycle in the graph is
    /// contained in this set.
    Cyclic(Vec<NodeId>),
}

/// Kahn's algorithm: repeatedly emit a node with no remaining incoming edge.
///
/// Ties are broken by node index so the order is deterministic. A topological
/// order is not unique, and returning a different valid one on each run makes
/// a test that compares orders flap and a user think the data changed.
pub fn topological_sort(view: &GraphView) -> TopoResult {
    let n = view.node_count;
    let mut indeg: Vec<usize> = (0..n).map(|i| view.in_degree(i)).collect();
    // A BinaryHeap of Reverse would do; for the sizes here a linear scan of
    // the ready set is simpler and the constant is irrelevant beside the
    // property reads a real query does.
    let mut order = Vec::with_capacity(n);
    let mut placed = vec![false; n];
    for _ in 0..n {
        let Some(v) = (0..n).find(|&i| !placed[i] && indeg[i] == 0) else {
            break;
        };
        placed[v] = true;
        order.push(view.index_to_node[v]);
        for &w in view.successors(v) {
            indeg[w] = indeg[w].saturating_sub(1);
        }
    }
    if order.len() == n {
        TopoResult::Order(order)
    } else {
        // Everything left has an incoming edge from something else left, which
        // is exactly the set the cycles run through.
        TopoResult::Cyclic((0..n).filter(|&i| !placed[i]).map(|i| view.index_to_node[i]).collect())
    }
}

/// One directed cycle, as the nodes along it, or `None` if the graph is
/// acyclic.
///
/// Returns a *witness* rather than a boolean. "There is a cycle somewhere in
/// your 200,000-node dependency graph" is not an actionable answer; the four
/// services in it are.
pub fn find_cycle(view: &GraphView) -> Option<Vec<NodeId>> {
    let n = view.node_count;
    // 0 unvisited, 1 on the current stack, 2 finished.
    let mut state = vec![0u8; n];
    let mut parent = vec![usize::MAX; n];

    for start in 0..n {
        if state[start] != 0 {
            continue;
        }
        // Explicit stack: a recursive DFS blows the real stack on a deep chain,
        // and a dependency graph is exactly where a 100,000-long chain lives.
        let mut stack: Vec<(usize, usize)> = vec![(start, 0)];
        state[start] = 1;
        while let Some((v, edge_i)) = stack.pop() {
            let succ = view.successors(v);
            if edge_i < succ.len() {
                stack.push((v, edge_i + 1));
                let w = succ[edge_i];
                match state[w] {
                    0 => {
                        state[w] = 1;
                        parent[w] = v;
                        stack.push((w, 0));
                    }
                    1 => {
                        // A back edge to something still on the stack: walk the
                        // parent chain from v back to w to recover the cycle.
                        let mut cycle = vec![view.index_to_node[w]];
                        let mut cur = v;
                        while cur != w && cur != usize::MAX {
                            cycle.push(view.index_to_node[cur]);
                            cur = parent[cur];
                        }
                        cycle.reverse();
                        return Some(cycle);
                    }
                    _ => {}
                }
            } else {
                state[v] = 2;
            }
        }
    }
    None
}

/// Bridges and articulation points, from one Tarjan low-link pass.
struct Tarjan {
    disc: Vec<usize>,
    low: Vec<usize>,
    timer: usize,
    bridges: Vec<(NodeId, NodeId)>,
    articulation: Vec<bool>,
    /// Which nodes started a DFS -- one per connected component.
    ///
    /// Tracked explicitly rather than inferred from `disc == 0`, which names
    /// only the *first* root. On a graph with two components the second root
    /// has a non-zero discovery time, was treated as an ordinary node, and was
    /// reported as an articulation point on the strength of having one child.
    /// Removing it cannot disconnect anything: its component simply gets
    /// smaller.
    is_root: Vec<bool>,
}

/// Every undirected neighbour of `i`, both directions.
fn undirected_neighbours(view: &GraphView, i: usize) -> Vec<usize> {
    let mut v: Vec<usize> = view.successors(i).to_vec();
    v.extend_from_slice(view.predecessors(i));
    v
}

fn tarjan(view: &GraphView) -> Tarjan {
    let n = view.node_count;
    let mut t = Tarjan {
        disc: vec![usize::MAX; n],
        low: vec![usize::MAX; n],
        timer: 0,
        bridges: Vec::new(),
        articulation: vec![false; n],
        is_root: vec![false; n],
    };

    for root in 0..n {
        if t.disc[root] != usize::MAX {
            continue;
        }
        // (node, parent, index into its neighbour list, children counted)
        let mut stack: Vec<(usize, usize, usize, usize)> = Vec::new();
        t.disc[root] = t.timer;
        t.low[root] = t.timer;
        t.timer += 1;
        t.is_root[root] = true;
        stack.push((root, usize::MAX, 0, 0));

        while let Some((v, parent, i, children)) = stack.pop() {
            let nb = undirected_neighbours(view, v);
            if i < nb.len() {
                let w = nb[i];
                stack.push((v, parent, i + 1, children));
                if w == v {
                    continue; // a self-loop is neither a bridge nor a cut
                }
                if t.disc[w] == usize::MAX {
                    t.disc[w] = t.timer;
                    t.low[w] = t.timer;
                    t.timer += 1;
                    // Count the child on the parent's frame.
                    if let Some(last) = stack.last_mut() {
                        last.3 += 1;
                    }
                    stack.push((w, v, 0, 0));
                } else if w != parent {
                    // A back edge. Only *one* edge to the parent may be
                    // ignored: with parallel edges `v == parent` twice, the
                    // second is a genuine back edge and the pair is not a
                    // bridge. Comparing on node identity alone would call it
                    // one.
                    t.low[v] = t.low[v].min(t.disc[w]);
                }
            } else {
                // v is finished; fold it into its parent.
                if parent != usize::MAX {
                    let lv = t.low[v];
                    t.low[parent] = t.low[parent].min(lv);
                    if lv > t.disc[parent] {
                        t.bridges.push((view.index_to_node[parent], view.index_to_node[v]));
                    }
                    // An articulation point, unless the parent is a DFS root
                    // -- a root is one only if it has more than one child,
                    // which is checked below.
                    if !t.is_root[parent] && lv >= t.disc[parent] {
                        t.articulation[parent] = true;
                    }
                }
                if v == root && children > 1 {
                    t.articulation[root] = true;
                }
            }
        }
    }
    t
}

/// Edges whose removal increases the number of connected components.
///
/// The single points of failure in a topology: cut one and something becomes
/// unreachable. Undirected, and each edge reported once as `(parent, child)`
/// in DFS order.
pub fn bridges(view: &GraphView) -> Vec<(NodeId, NodeId)> {
    let mut b = tarjan(view).bridges;
    b.sort();
    b
}

/// Nodes whose removal increases the number of connected components.
///
/// The node-level counterpart of a bridge, and usually the more useful of the
/// two operationally: a bridge tells you which link to duplicate, an
/// articulation point tells you which box to stop relying on.
pub fn articulation_points(view: &GraphView) -> Vec<NodeId> {
    let t = tarjan(view);
    (0..view.node_count)
        .filter(|&i| t.articulation[i])
        .map(|i| view.index_to_node[i])
        .collect()
}
