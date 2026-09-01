//! Structural decompositions for the H2 coverage target (ALGO-01).
//!
//! Every function here reads the **undirected collapse** and says so in its
//! signature via `bidirectional`, the same convention the shape metrics use:
//! `true` when the view stores each undirected edge once and the walk has to
//! go both ways, `false` when the view already holds both directions.
//! Detecting it is not possible — a balanced directed graph looks identical —
//! so the caller states it.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use crate::common::GraphView;

fn neighbours(view: &GraphView, u: usize, bidirectional: bool) -> Vec<usize> {
    let mut v: Vec<usize> = view.successors(u).to_vec();
    if bidirectional {
        v.extend_from_slice(view.predecessors(u));
    }
    v.sort_unstable();
    v.dedup();
    v.retain(|&x| x != u);
    v
}

/// Is the graph two-colourable, and if so, which side is each node on?
///
/// `None` when an odd cycle exists. The two sets are the useful output, not
/// just the yes/no: "these are the users and those are the items" is the thing
/// a caller wanted, and recomputing the split after a boolean answer means
/// doing the same traversal twice.
pub fn bipartite_sets(view: &GraphView, bidirectional: bool) -> Option<(Vec<usize>, Vec<usize>)> {
    let n = view.node_count;
    let mut colour: Vec<i8> = vec![-1; n];
    for s in 0..n {
        if colour[s] != -1 {
            continue;
        }
        colour[s] = 0;
        let mut q = VecDeque::from([s]);
        while let Some(u) = q.pop_front() {
            for v in neighbours(view, u, bidirectional) {
                if colour[v] == -1 {
                    colour[v] = 1 - colour[u];
                    q.push_back(v);
                } else if colour[v] == colour[u] {
                    return None; // odd cycle
                }
            }
        }
    }
    let left = (0..n).filter(|&i| colour[i] == 0).collect();
    let right = (0..n).filter(|&i| colour[i] == 1).collect();
    Some((left, right))
}

/// A maximal matching: edges sharing no endpoint, greedily chosen.
///
/// Maximal, not maximum. Greedy cannot be beaten by more than a factor of two
/// and runs in linear time, and the name says which guarantee is on offer --
/// calling a greedy result "maximum" is the kind of claim that survives until
/// someone counts.
///
/// Edges are considered in sorted order, so the matching is the same on every
/// run rather than an artefact of iteration order.
pub fn maximal_matching(view: &GraphView, bidirectional: bool) -> Vec<(usize, usize)> {
    let n = view.node_count;
    let mut edges: BTreeSet<(usize, usize)> = BTreeSet::new();
    for u in 0..n {
        for v in neighbours(view, u, bidirectional) {
            edges.insert((u.min(v), u.max(v)));
        }
    }
    let mut used = vec![false; n];
    let mut out = Vec::new();
    for (a, b) in edges {
        if !used[a] && !used[b] {
            used[a] = true;
            used[b] = true;
            out.push((a, b));
        }
    }
    out
}

/// A greedy proper colouring, in largest-degree-first order.
///
/// The number of colours is an upper bound on the chromatic number and not the
/// chromatic number itself, which is NP-hard. Reported as "colours used", so
/// nobody reads it as the minimum.
///
/// Largest-first because it is the ordering NetworkX defaults to and it is
/// usually better than arbitrary; ties by index so the result is stable.
pub fn greedy_colouring(view: &GraphView, bidirectional: bool) -> Vec<usize> {
    let n = view.node_count;
    let adj: Vec<Vec<usize>> = (0..n).map(|u| neighbours(view, u, bidirectional)).collect();
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by_key(|&i| (std::cmp::Reverse(adj[i].len()), i));
    let mut colour = vec![usize::MAX; n];
    for u in order {
        let taken: HashSet<usize> = adj[u].iter().filter_map(|&v| {
            (colour[v] != usize::MAX).then_some(colour[v])
        }).collect();
        colour[u] = (0..).find(|c| !taken.contains(c)).unwrap();
    }
    colour
}

/// A dominating set: every node is in it or adjacent to it.
///
/// Greedy, so minimal rather than minimum — "which machines do I need an agent
/// on to see the whole fleet" tolerates a set slightly larger than optimal and
/// does not tolerate waiting for an exact answer.
pub fn dominating_set(view: &GraphView, bidirectional: bool) -> Vec<usize> {
    let n = view.node_count;
    let adj: Vec<Vec<usize>> = (0..n).map(|u| neighbours(view, u, bidirectional)).collect();
    let mut dominated = vec![false; n];
    let mut chosen = Vec::new();
    loop {
        // The node covering the most still-undominated nodes, ties by index.
        let mut best = usize::MAX;
        let mut best_gain = 0usize;
        for u in 0..n {
            if chosen.contains(&u) {
                continue;
            }
            let gain = (!dominated[u]) as usize
                + adj[u].iter().filter(|&&v| !dominated[v]).count();
            if gain > best_gain {
                best_gain = gain;
                best = u;
            }
        }
        if best == usize::MAX || best_gain == 0 {
            break;
        }
        chosen.push(best);
        dominated[best] = true;
        for &v in &adj[best] {
            dominated[v] = true;
        }
    }
    chosen.sort_unstable();
    chosen
}

/// The k-truss: the subgraph where every edge sits in at least `k-2`
/// triangles.
///
/// A stricter cohesion notion than k-core, and the difference is the point: a
/// long cycle is 2-core and has no triangles at all, so k-core calls it
/// cohesive and k-truss does not. Returns the surviving nodes.
pub fn k_truss(view: &GraphView, k: usize, bidirectional: bool) -> Vec<usize> {
    let n = view.node_count;
    if k < 3 {
        // k-truss is defined for k >= 3; below that every edge trivially
        // qualifies and the answer is "every node with an edge", which is not
        // a truss and should not be reported as one.
        return (0..n).filter(|&u| !neighbours(view, u, bidirectional).is_empty()).collect();
    }
    let mut adj: Vec<HashSet<usize>> = (0..n)
        .map(|u| neighbours(view, u, bidirectional).into_iter().collect())
        .collect();
    loop {
        let mut drop: Vec<(usize, usize)> = Vec::new();
        for u in 0..n {
            for &v in &adj[u] {
                if u >= v {
                    continue;
                }
                let support = adj[u].intersection(&adj[v]).count();
                if support < k - 2 {
                    drop.push((u, v));
                }
            }
        }
        if drop.is_empty() {
            break;
        }
        for (u, v) in drop {
            adj[u].remove(&v);
            adj[v].remove(&u);
        }
    }
    (0..n).filter(|&u| !adj[u].is_empty()).collect()
}

/// Global transitivity: `3 * triangles / connected-triples`.
///
/// Not the same number as the average of the per-node clustering coefficients,
/// and the difference is not academic: averaging per-node values weights every
/// node equally, so a graph of many low-degree nodes and one dense hub reports
/// a low average and a high transitivity. NetworkX ships both; this is
/// `nx.transitivity`.
pub fn transitivity(view: &GraphView, bidirectional: bool) -> Option<f64> {
    let n = view.node_count;
    let adj: Vec<HashSet<usize>> = (0..n)
        .map(|u| neighbours(view, u, bidirectional).into_iter().collect())
        .collect();
    let mut triangles = 0usize;
    let mut triples = 0usize;
    for u in 0..n {
        let d = adj[u].len();
        triples += d * d.saturating_sub(1) / 2;
        for &v in &adj[u] {
            if v <= u {
                continue;
            }
            triangles += adj[u].intersection(&adj[v]).filter(|&&w| w > v).count();
        }
    }
    if triples == 0 {
        // 0/0. NetworkX returns 0 here; this returns None, because "no
        // triples" and "triples that never close" are different graphs and a
        // caller comparing two numbers should not have them collapse.
        return None;
    }
    Some(3.0 * triangles as f64 / triples as f64)
}

/// Global efficiency: the average of `1/distance` over all ordered pairs.
///
/// The repair for "average shortest path length" on a disconnected graph. That
/// average is infinite the moment one pair cannot reach another, so it is
/// undefined on exactly the graphs people most want to compare; efficiency
/// contributes 0 for an unreachable pair and stays finite.
pub fn global_efficiency(view: &GraphView, bidirectional: bool) -> Option<f64> {
    let n = view.node_count;
    if n < 2 {
        return None;
    }
    let mut total = 0.0;
    let mut dist = vec![usize::MAX; n];
    for s in 0..n {
        dist.iter_mut().for_each(|d| *d = usize::MAX);
        dist[s] = 0;
        let mut q = VecDeque::from([s]);
        while let Some(u) = q.pop_front() {
            for v in neighbours(view, u, bidirectional) {
                if dist[v] == usize::MAX {
                    dist[v] = dist[u] + 1;
                    q.push_back(v);
                }
            }
        }
        for (t, &d) in dist.iter().enumerate() {
            if t != s && d != usize::MAX {
                total += 1.0 / d as f64;
            }
        }
    }
    Some(total / (n * (n - 1)) as f64)
}

/// Square clustering: how often a node's neighbours close a *four*-cycle.
///
/// Ordinary clustering counts triangles, and a bipartite graph has none by
/// construction — so on a user-item graph, or any two-mode network, the
/// clustering coefficient is 0 everywhere and says nothing. Squares are the
/// shortest cycle such a graph can have, which is why this exists.
pub fn square_clustering(view: &GraphView, u: usize, bidirectional: bool) -> Option<f64> {
    let n = view.node_count;
    if u >= n {
        return None;
    }
    let nu = neighbours(view, u, bidirectional);
    if nu.len() < 2 {
        return None; // no pair of neighbours, so no square to close
    }
    let mut squares = 0.0;
    let mut denom = 0.0;
    for (i, &v) in nu.iter().enumerate() {
        let nv: HashSet<usize> = neighbours(view, v, bidirectional).into_iter().collect();
        for &w in nu.iter().skip(i + 1) {
            let nw: HashSet<usize> = neighbours(view, w, bidirectional).into_iter().collect();
            // Nodes other than u adjacent to both v and w close a square.
            let common: usize = nv.intersection(&nw).filter(|&&x| x != u).count();
            squares += common as f64;
            let deg_v = nv.len() - if nv.contains(&u) { 1 } else { 0 };
            let deg_w = nw.len() - if nw.contains(&u) { 1 } else { 0 };
            denom += (deg_v + deg_w) as f64 - common as f64;
        }
    }
    if denom == 0.0 {
        return None;
    }
    Some(squares / denom)
}

/// The rich-club coefficient at degree `k`: how densely the high-degree nodes
/// are connected to each other.
///
/// Whether the hubs form a club or merely each have many followers, which
/// degree distribution alone cannot answer — two graphs with identical degree
/// sequences differ completely here, and it is the difference between a
/// resilient core and a set of independent single points of failure.
pub fn rich_club_coefficient(view: &GraphView, k: usize, bidirectional: bool) -> Option<f64> {
    let n = view.node_count;
    let members: Vec<usize> = (0..n)
        .filter(|&u| neighbours(view, u, bidirectional).len() > k)
        .collect();
    if members.len() < 2 {
        return None; // fewer than two members is not a club
    }
    let set: HashSet<usize> = members.iter().copied().collect();
    let mut edges = 0usize;
    for &u in &members {
        for v in neighbours(view, u, bidirectional) {
            if v > u && set.contains(&v) {
                edges += 1;
            }
        }
    }
    let m = members.len();
    Some(2.0 * edges as f64 / (m * (m - 1)) as f64)
}

/// Biconnected components: maximal subgraphs with no articulation point.
///
/// Where `bridges` names the single edges whose loss disconnects, this names
/// the regions that survive *any* single node failure. A component of size 4
/// and four components of size 1 are very different resilience stories and
/// have the same bridge count.
///
/// Returns each component as a sorted node list, components sorted, so two
/// runs agree.
pub fn biconnected_components(view: &GraphView, bidirectional: bool) -> Vec<Vec<usize>> {
    let n = view.node_count;
    let adj: Vec<Vec<usize>> = (0..n).map(|u| neighbours(view, u, bidirectional)).collect();
    let mut disc = vec![usize::MAX; n];
    let mut low = vec![0usize; n];
    let mut timer = 0usize;
    let mut stack: Vec<(usize, usize)> = Vec::new();
    let mut out: Vec<Vec<usize>> = Vec::new();

    // Iterative, not recursive: a deep graph would blow the stack, and the
    // graphs this is for are exactly the long dependency chains.
    for root in 0..n {
        if disc[root] != usize::MAX {
            continue;
        }
        let mut work: Vec<(usize, usize, usize)> = vec![(root, usize::MAX, 0)];
        disc[root] = timer;
        low[root] = timer;
        timer += 1;
        while let Some(&mut (u, parent, ref mut i)) = work.last_mut() {
            if *i < adj[u].len() {
                let v = adj[u][*i];
                *i += 1;
                if v == parent {
                    continue;
                }
                if disc[v] == usize::MAX {
                    stack.push((u, v));
                    disc[v] = timer;
                    low[v] = timer;
                    timer += 1;
                    work.push((v, u, 0));
                } else if disc[v] < disc[u] {
                    stack.push((u, v));
                    low[u] = low[u].min(disc[v]);
                }
            } else {
                work.pop();
                if let Some(&(p, _, _)) = work.last() {
                    low[p] = low[p].min(low[u]);
                    if low[u] >= disc[p] {
                        let mut comp: HashSet<usize> = HashSet::new();
                        while let Some(&(a, b)) = stack.last() {
                            if disc[a] < disc[p] {
                                break;
                            }
                            comp.insert(a);
                            comp.insert(b);
                            stack.pop();
                            if (a, b) == (p, u) {
                                break;
                            }
                        }
                        if !comp.is_empty() {
                            let mut c: Vec<usize> = comp.into_iter().collect();
                            c.sort_unstable();
                            out.push(c);
                        }
                    }
                }
            }
        }
        stack.clear();
    }
    out.sort();
    out.dedup();
    out
}

/// Sorted, deduplicated helper for callers that want the collapse directly.
pub fn undirected_degree(view: &GraphView, u: usize, bidirectional: bool) -> usize {
    neighbours(view, u, bidirectional).len()
}

#[allow(dead_code)]
fn _keep(_: &HashMap<usize, usize>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NodeId;

    fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let index_to_node: Vec<NodeId> = (0..n).map(|i| i as NodeId).collect();
        let node_to_index: HashMap<NodeId, usize> = (0..n).map(|i| (i as NodeId, i)).collect();
        let (mut out, mut inc) = (vec![Vec::new(); n], vec![Vec::new(); n]);
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        GraphView::from_adjacency_list(n, index_to_node, node_to_index, out, inc, None)
    }

    #[test]
    fn a_square_is_bipartite_and_a_triangle_is_not() {
        // The textbook separator: even cycle yes, odd cycle no.
        let square = view(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
        let (l, r) = bipartite_sets(&square, true).expect("even cycle");
        assert_eq!((l, r), (vec![0, 2], vec![1, 3]));
        let triangle = view(3, &[(0, 1), (1, 2), (2, 0)]);
        assert!(bipartite_sets(&triangle, true).is_none());
    }

    #[test]
    fn matching_shares_no_endpoint() {
        let g = view(4, &[(0, 1), (1, 2), (2, 3)]);
        let m = maximal_matching(&g, true);
        let mut seen: HashSet<usize> = HashSet::new();
        for (a, b) in &m {
            assert!(seen.insert(*a) && seen.insert(*b), "endpoint reused: {m:?}");
        }
        // Greedy in sorted order takes (0,1) first, which blocks (1,2), then
        // takes (2,3). Two edges, and the same two every run.
        assert_eq!(m, vec![(0, 1), (2, 3)]);
    }

    #[test]
    fn colouring_gives_adjacent_nodes_different_colours() {
        let triangle = view(3, &[(0, 1), (1, 2), (2, 0)]);
        let c = greedy_colouring(&triangle, true);
        assert_eq!(c.iter().collect::<HashSet<_>>().len(), 3, "{c:?}");
        let square = view(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
        let c = greedy_colouring(&square, true);
        for (a, b) in [(0, 1), (1, 2), (2, 3), (3, 0)] {
            assert_ne!(c[a], c[b], "{c:?}");
        }
    }

    #[test]
    fn dominating_set_covers_every_node() {
        // A star: the centre alone dominates everything, and greedy must find
        // it rather than picking spokes.
        let star = view(5, &[(0, 1), (0, 2), (0, 3), (0, 4)]);
        assert_eq!(dominating_set(&star, true), vec![0]);
        let path = view(4, &[(0, 1), (1, 2), (2, 3)]);
        let d = dominating_set(&path, true);
        for u in 0..4 {
            let covered = d.contains(&u)
                || neighbours(&path, u, true).iter().any(|v| d.contains(v));
            assert!(covered, "node {u} uncovered by {d:?}");
        }
    }

    #[test]
    fn k_truss_drops_a_cycle_that_k_core_would_keep() {
        // A 4-cycle is 2-core -- every node has degree 2 -- and has no
        // triangles, so the 3-truss is empty. This is the case that makes
        // truss a different question from core rather than a rename.
        let cycle = view(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
        assert!(k_truss(&cycle, 3, true).is_empty(), "{:?}", k_truss(&cycle, 3, true));
        // A triangle survives: each edge is in one triangle, and 3-truss needs
        // k-2 = 1.
        let tri = view(3, &[(0, 1), (1, 2), (2, 0)]);
        assert_eq!(k_truss(&tri, 3, true), vec![0, 1, 2]);
    }

    #[test]
    fn transitivity_is_not_the_average_clustering() {
        // A triangle: every triple closes, so transitivity is 1.
        let tri = view(3, &[(0, 1), (1, 2), (2, 0)]);
        assert_eq!(transitivity(&tri, true), Some(1.0));
        // A star has triples and no triangles: 0, not undefined.
        let star = view(4, &[(0, 1), (0, 2), (0, 3)]);
        assert_eq!(transitivity(&star, true), Some(0.0));
    }

    #[test]
    fn global_efficiency_stays_finite_when_average_distance_would_not() {
        // 0-1 joined, 2 isolated. Average shortest path length is infinite
        // here; efficiency contributes 0 for the unreachable pairs and stays a
        // number, which is the whole reason it exists.
        let g = view(3, &[(0, 1)]);
        let e = global_efficiency(&g, true).expect("finite");
        assert!((e - 2.0 / 6.0).abs() < 1e-12, "{e}");
    }

    #[test]
    fn square_clustering_sees_what_triangle_clustering_cannot() {
        // A 4-cycle is bipartite: zero triangles, so ordinary clustering is 0
        // for every node and reports nothing. The square closes, so this is
        // positive.
        let cycle = view(4, &[(0, 1), (1, 2), (2, 3), (3, 0)]);
        let sq = square_clustering(&cycle, 0, true).expect("has a pair of neighbours");
        assert!(sq > 0.0, "{sq}");
    }

    #[test]
    fn rich_club_needs_at_least_two_members() {
        // One hub is not a club. Returning 0 would say "measured, not
        // clubbish" about a graph nothing was measured on.
        let star = view(4, &[(0, 1), (0, 2), (0, 3)]);
        assert!(rich_club_coefficient(&star, 2, true).is_none());
    }

    #[test]
    fn biconnected_components_split_at_an_articulation_point() {
        // Two triangles joined at node 2: removing 2 disconnects them, so they
        // are two biconnected components. `bridges` finds nothing here --
        // there is no single *edge* whose loss disconnects -- which is exactly
        // the resilience story it cannot tell.
        let g = view(5, &[(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 2)]);
        let comps = biconnected_components(&g, true);
        assert_eq!(comps.len(), 2, "{comps:?}");
        assert!(comps.contains(&vec![0, 1, 2]), "{comps:?}");
        assert!(comps.contains(&vec![2, 3, 4]), "{comps:?}");
    }

    #[test]
    fn transitivity_refuses_a_graph_with_no_triples() {
        // 0/0. Returning 0 would make "no triples" indistinguishable from
        // "triples that never close", and those are different graphs.
        let g = view(2, &[(0, 1)]);
        assert!(transitivity(&g, true).is_none());
    }
}
