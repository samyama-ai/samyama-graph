//! Similarity and structural-hole measures for the H2 coverage target.
//!
//! [`crate::link_prediction`] scores *unconnected* pairs, because that is what
//! link prediction is for. These score any pair, and two of them describe a
//! node's position rather than a pair at all — Burt's constraint and effective
//! size are about the holes around a node, which is a different question from
//! who it resembles.

use std::collections::{HashMap, HashSet};

use crate::common::GraphView;

fn nbrs(view: &GraphView, u: usize) -> HashSet<usize> {
    let mut s: HashSet<usize> = view.successors(u).iter().copied().collect();
    s.extend(view.predecessors(u).iter().copied());
    s.remove(&u);
    s
}

/// Overlap coefficient: `|A ∩ B| / min(|A|, |B|)`.
///
/// Jaccard divides by the union, so a small set inside a large one scores low
/// however completely it is contained. Overlap divides by the smaller set, so
/// containment scores 1 — the right answer when asking "is this node's
/// neighbourhood a subset of that one's", which is what a specialisation
/// hierarchy looks like.
pub fn overlap_coefficient(view: &GraphView, u: usize, v: usize) -> Option<f64> {
    if u >= view.node_count || v >= view.node_count {
        return None;
    }
    let (a, b) = (nbrs(view, u), nbrs(view, v));
    let m = a.len().min(b.len());
    if m == 0 {
        return None; // an isolated node resembles nothing, including itself
    }
    Some(a.intersection(&b).count() as f64 / m as f64)
}

/// Cosine similarity over neighbourhood indicator vectors.
///
/// `|A ∩ B| / sqrt(|A| * |B|)` — the geometric mean in the denominator rather
/// than Jaccard's union or overlap's minimum. Between the two: less forgiving
/// of size differences than overlap, more forgiving than Jaccard.
pub fn cosine_similarity(view: &GraphView, u: usize, v: usize) -> Option<f64> {
    if u >= view.node_count || v >= view.node_count {
        return None;
    }
    let (a, b) = (nbrs(view, u), nbrs(view, v));
    if a.is_empty() || b.is_empty() {
        return None;
    }
    Some(a.intersection(&b).count() as f64 / ((a.len() * b.len()) as f64).sqrt())
}

/// The `k` most similar nodes to each node, by Jaccard.
///
/// The nearest-neighbour graph, which link prediction cannot give: it ranks
/// *unconnected* pairs, so a node's most similar neighbour is excluded by
/// construction the moment they are joined. "Who else is like this" includes
/// the ones already connected.
///
/// Returns `(node, other, score)` with ties broken by index, so the graph is
/// the same on every run.
pub fn node_similarity(view: &GraphView, k: usize, cutoff: f64) -> Vec<(usize, usize, f64)> {
    let n = view.node_count;
    let sets: Vec<HashSet<usize>> = (0..n).map(|u| nbrs(view, u)).collect();
    let mut out = Vec::new();
    for u in 0..n {
        if sets[u].is_empty() {
            continue;
        }
        let mut scored: Vec<(usize, f64)> = (0..n)
            .filter(|&v| v != u && !sets[v].is_empty())
            .filter_map(|v| {
                let inter = sets[u].intersection(&sets[v]).count() as f64;
                let union = sets[u].union(&sets[v]).count() as f64;
                let j = if union == 0.0 { 0.0 } else { inter / union };
                (j >= cutoff && j > 0.0).then_some((v, j))
            })
            .collect();
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
        });
        scored.truncate(k);
        out.extend(scored.into_iter().map(|(v, s)| (u, v, s)));
    }
    out
}

/// Burt's effective size: neighbours, minus the redundancy among them.
///
/// A node with five neighbours who all know each other has an effective size
/// near 1 — it is one conversation wearing five hats. Degree cannot see that,
/// which is why a high-degree node inside a clique looks influential and is
/// not.
pub fn effective_size(view: &GraphView, u: usize) -> Option<f64> {
    if u >= view.node_count {
        return None;
    }
    let ego = nbrs(view, u);
    if ego.is_empty() {
        return None;
    }
    // NetworkX's unweighted form: n - 2t/n, where t is the number of edges
    // among the neighbours and n the neighbour count.
    let n = ego.len() as f64;
    let mut t = 0usize;
    for &a in &ego {
        for &b in &ego {
            if a < b && nbrs(view, a).contains(&b) {
                t += 1;
            }
        }
    }
    Some(n - 2.0 * t as f64 / n)
}

/// Burt's constraint: how much a node's contacts are tied up in each other.
///
/// The complement of effective size, and the one with the clearer reading:
/// high constraint means no structural holes to broker across, so the node has
/// little bargaining position however many neighbours it has.
pub fn constraint(view: &GraphView, u: usize) -> Option<f64> {
    let ego = nbrs(view, u);
    if ego.is_empty() {
        return None;
    }
    // p(u,j) = proportion of u's relations invested in j.
    let p = |a: usize, b: usize| -> f64 {
        let na = nbrs(view, a);
        if na.is_empty() || !na.contains(&b) {
            return 0.0;
        }
        1.0 / na.len() as f64
    };
    let mut total = 0.0;
    for &j in &ego {
        // (p_uj + sum_q p_uq * p_qj)^2, q over u's other neighbours
        let indirect: f64 = ego.iter().filter(|&&q| q != j && q != u)
            .map(|&q| p(u, q) * p(q, j))
            .sum();
        let c = p(u, j) + indirect;
        total += c * c;
    }
    Some(total)
}

/// Reciprocity: the fraction of edges whose reverse also exists.
///
/// A directed-only measure, and meaningless on an undirected view where every
/// edge is reciprocal by construction — so it reads the CSR as stored rather
/// than through a collapse, and a caller passing a doubled view gets 1.0,
/// which is the true answer for that graph.
pub fn reciprocity(view: &GraphView) -> Option<f64> {
    let n = view.node_count;
    let mut edges = 0usize;
    let mut mutual = 0usize;
    for u in 0..n {
        let succ: HashSet<usize> = view.successors(u).iter().copied().collect();
        for &v in &succ {
            if v == u {
                continue;
            }
            edges += 1;
            if view.successors(v).contains(&u) {
                mutual += 1;
            }
        }
    }
    (edges > 0).then(|| mutual as f64 / edges as f64)
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
    fn overlap_scores_containment_as_one_where_jaccard_does_not() {
        // 0's neighbours are {2}; 1's are {2,3,4}. 0 is wholly contained in 1,
        // so overlap is 1.0 while Jaccard would be 1/3. That gap is the reason
        // both exist.
        let g = view(5, &[(0, 2), (1, 2), (1, 3), (1, 4)]);
        assert_eq!(overlap_coefficient(&g, 0, 1), Some(1.0));
    }

    #[test]
    fn an_isolated_node_resembles_nothing() {
        // Not a guard for its own sake: |A| = 0 divides by zero, and 0.0 would
        // claim "measured, no similarity" about a pair nothing was measured on.
        let g = view(3, &[(0, 1)]);
        assert!(overlap_coefficient(&g, 2, 0).is_none());
        assert!(cosine_similarity(&g, 2, 0).is_none());
        assert!(effective_size(&g, 2).is_none());
        assert!(constraint(&g, 2).is_none());
    }

    #[test]
    fn node_similarity_includes_pairs_that_are_already_connected() {
        // 0 and 1 are joined *and* share neighbour 2. Link prediction excludes
        // this pair by construction; "who else is like this" must not.
        let g = view(3, &[(0, 1), (0, 2), (1, 2)]);
        let sims = node_similarity(&g, 5, 0.0);
        assert!(sims.iter().any(|&(a, b, _)| (a, b) == (0, 1)), "{sims:?}");
    }

    #[test]
    fn effective_size_collapses_inside_a_clique() {
        // Centre of a 4-clique: 3 neighbours who all know each other, so the
        // effective size is 3 - 2*3/3 = 1. Degree says 3; the node is one
        // conversation wearing three hats.
        let g = view(4, &[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        assert_eq!(effective_size(&g, 0), Some(1.0));
        // Star centre: 3 neighbours, none connected, so nothing is redundant.
        let star = view(4, &[(0, 1), (0, 2), (0, 3)]);
        assert_eq!(effective_size(&star, 0), Some(3.0));
    }

    #[test]
    fn constraint_is_higher_in_a_clique_than_a_star() {
        // The whole point of the measure: the star's centre brokers three
        // structural holes and the clique's centre brokers none, even though
        // both have degree 3.
        let clique = view(4, &[(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)]);
        let star = view(4, &[(0, 1), (0, 2), (0, 3)]);
        let (c, s) = (constraint(&clique, 0).unwrap(), constraint(&star, 0).unwrap());
        assert!(c > s, "clique {c} should be more constrained than star {s}");
    }

    #[test]
    fn reciprocity_counts_edges_whose_reverse_exists() {
        // 0<->1 mutual, 1->2 one-way: 2 of 3 directed edges are reciprocated.
        let g = view(3, &[(0, 1), (1, 0), (1, 2)]);
        let r = reciprocity(&g).unwrap();
        assert!((r - 2.0 / 3.0).abs() < 1e-12, "{r}");
        assert!(reciprocity(&view(2, &[])).is_none(), "no edges, no ratio");
    }
}
