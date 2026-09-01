//! Ranking algorithms beyond the six in [`crate::centrality`] (ALGO-01, H2).
//!
//! Every one here has a NetworkX equivalent, which is deliberate: ALGO-02 asks
//! for numerical parity against a reference implementation, and an algorithm
//! with no reference can only ever be checked against our own opinion of it.
//! Picking the ones NetworkX also implements is what makes the H2 coverage
//! target and the H2 parity target reachable by the same work.

use std::collections::HashMap;

use crate::common::{GraphView, NodeId};

/// Katz centrality: influence that decays with distance.
///
/// PageRank asks "where does a random surfer end up"; Katz asks "how many
/// walks reach this node, discounting longer ones by `alpha` each hop". The
/// difference matters on a graph with sinks — PageRank has to teleport out of
/// them and Katz does not, so a node whose only paths are long is ranked low
/// rather than being handed a share of the teleport mass.
///
/// `alpha` must be below the reciprocal of the largest eigenvalue or the sum
/// diverges. NetworkX leaves that to the caller and so does this, but it
/// reports non-convergence rather than returning the last iterate: a vector
/// that has not converged is still a well-formed vector, and returning one
/// silently is how a divergent parameter becomes a plausible-looking ranking.
pub fn katz_centrality(
    view: &GraphView,
    alpha: f64,
    beta: f64,
    max_iter: usize,
    tol: f64,
) -> Option<Vec<f64>> {
    let n = view.node_count;
    if n == 0 {
        return Some(Vec::new());
    }
    let mut x = vec![0.0_f64; n];
    for _ in 0..max_iter {
        let last = x.clone();
        // x_i = alpha * sum_{j->i} x_j + beta, i.e. over *incoming* edges,
        // matching NetworkX. Using successors here would rank by outgoing
        // influence, which is a different and rarely intended quantity.
        for i in 0..n {
            let mut acc = 0.0;
            for &j in view.predecessors(i) {
                acc += last[j];
            }
            x[i] = alpha * acc + beta;
        }
        let err: f64 = x.iter().zip(&last).map(|(a, b)| (a - b).abs()).sum();
        if err < n as f64 * tol {
            // NetworkX normalises to unit L2 norm at the end.
            let norm = x.iter().map(|v| v * v).sum::<f64>().sqrt();
            if norm > 0.0 {
                for v in x.iter_mut() {
                    *v /= norm;
                }
            }
            return Some(x);
        }
    }
    None
}

/// HITS: hubs and authorities, as a pair.
///
/// One algorithm with two outputs rather than two algorithms, because neither
/// score means anything without the other: a hub is a node pointing at good
/// authorities and an authority is one pointed at by good hubs, and the two
/// are defined by each other's fixed point.
///
/// Returns `(hubs, authorities)`, both normalised to sum to 1 as NetworkX
/// does.
pub fn hits(view: &GraphView, max_iter: usize, tol: f64) -> Option<(Vec<f64>, Vec<f64>)> {
    let n = view.node_count;
    if n == 0 {
        return Some((Vec::new(), Vec::new()));
    }
    let mut hubs = vec![1.0 / n as f64; n];
    let mut auth = vec![0.0_f64; n];
    for _ in 0..max_iter {
        let last_hubs = hubs.clone();
        // authority = sum of hubs pointing at me
        for a in auth.iter_mut() {
            *a = 0.0;
        }
        for i in 0..n {
            for &j in view.successors(i) {
                auth[j] += last_hubs[i];
            }
        }
        normalise_sum(&mut auth);
        // hub = sum of authorities I point at
        for h in hubs.iter_mut() {
            *h = 0.0;
        }
        for i in 0..n {
            for &j in view.successors(i) {
                hubs[i] += auth[j];
            }
        }
        normalise_sum(&mut hubs);
        let err: f64 = hubs.iter().zip(&last_hubs).map(|(a, b)| (a - b).abs()).sum();
        if err < tol {
            return Some((hubs, auth));
        }
    }
    None
}

fn normalise_sum(v: &mut [f64]) {
    let s: f64 = v.iter().sum();
    if s > 0.0 {
        for x in v.iter_mut() {
            *x /= s;
        }
    }
}

/// PageRank biased toward a set of source nodes.
///
/// The teleport lands on `sources` instead of uniformly, which turns a global
/// ranking into "important *from here*" — the query an impact analysis
/// actually asks. Plain PageRank cannot express it: it answers the same thing
/// for every starting point.
///
/// An empty or unreachable `sources` falls back to uniform teleport, which is
/// plain PageRank, and says so rather than dividing by zero.
pub fn personalised_page_rank(
    view: &GraphView,
    sources: &[usize],
    damping: f64,
    max_iter: usize,
    tol: f64,
) -> Vec<f64> {
    let n = view.node_count;
    if n == 0 {
        return Vec::new();
    }
    let mut teleport = vec![0.0_f64; n];
    let valid: Vec<usize> = sources.iter().copied().filter(|&s| s < n).collect();
    if valid.is_empty() {
        teleport.iter_mut().for_each(|t| *t = 1.0 / n as f64);
    } else {
        for &s in &valid {
            teleport[s] += 1.0 / valid.len() as f64;
        }
    }
    let mut rank = teleport.clone();
    let out_deg: Vec<usize> = (0..n).map(|i| view.successors(i).len()).collect();
    for _ in 0..max_iter {
        let last = rank.clone();
        // Dangling mass goes to the teleport distribution, not uniformly.
        // Spreading it uniformly would leak rank to nodes the personalisation
        // deliberately excluded, which is the bug that makes a personalised
        // ranking look suspiciously like the global one.
        let dangling: f64 = (0..n).filter(|&i| out_deg[i] == 0).map(|i| last[i]).sum();
        for r in rank.iter_mut() {
            *r = 0.0;
        }
        for i in 0..n {
            if out_deg[i] == 0 {
                continue;
            }
            let share = last[i] / out_deg[i] as f64;
            for &j in view.successors(i) {
                rank[j] += share;
            }
        }
        for i in 0..n {
            rank[i] = damping * (rank[i] + dangling * teleport[i]) + (1.0 - damping) * teleport[i];
        }
        let err: f64 = rank.iter().zip(&last).map(|(a, b)| (a - b).abs()).sum();
        if err < n as f64 * tol {
            break;
        }
    }
    rank
}

/// VoteRank: pick influential nodes that are not all in the same place.
///
/// Repeatedly elects the highest-voted node and then *suppresses its
/// neighbours' voting power*, so the result is a spread-out seed set rather
/// than a clique of mutually-reinforcing hubs. Top-k by degree or PageRank
/// gives the latter, which is why a seeding campaign built on those covers
/// less of the graph than its scores suggest.
///
/// Returns the elected nodes in election order.
pub fn vote_rank(view: &GraphView, k: usize) -> Vec<usize> {
    let n = view.node_count;
    if n == 0 || k == 0 {
        return Vec::new();
    }
    // NetworkX's decrement is 1/average-out-degree, computed once.
    let edges: usize = (0..n).map(|i| view.successors(i).len()).sum();
    if edges == 0 {
        return Vec::new();
    }
    let decrement = n as f64 / edges as f64;
    let mut voting_ability = vec![1.0_f64; n];
    let mut score = vec![0.0_f64; n];
    let mut elected: Vec<usize> = Vec::new();

    for _ in 0..k.min(n) {
        for s in score.iter_mut() {
            *s = 0.0;
        }
        for i in 0..n {
            if elected.contains(&i) {
                continue;
            }
            for &j in view.predecessors(i) {
                score[i] += voting_ability[j];
            }
        }
        for &e in &elected {
            score[e] = f64::NEG_INFINITY;
        }
        // Ties broken by index, so two runs elect the same set. An unstable
        // seed set makes a campaign unreproducible for no reason.
        let (best, best_score) = (0..n).fold((usize::MAX, f64::NEG_INFINITY), |(bi, bs), i| {
            if score[i] > bs { (i, score[i]) } else { (bi, bs) }
        });
        if best == usize::MAX || best_score <= 0.0 {
            break;
        }
        elected.push(best);
        voting_ability[best] = 0.0;
        for &j in view.predecessors(best) {
            voting_ability[j] = (voting_ability[j] - decrement).max(0.0);
        }
    }
    elected
}

/// Attach node ids to a score vector, ranked, ties by id.
pub fn ranked_scores(view: &GraphView, scores: &[f64]) -> Vec<(NodeId, f64)> {
    let mut out: Vec<(NodeId, f64)> = scores
        .iter()
        .enumerate()
        .map(|(i, &s)| (view.index_to_node[i], s))
        .collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
    out
}

/// Index lookup for a node id, for the operators that take one as an argument.
pub fn index_of(view: &GraphView, id: NodeId) -> Option<usize> {
    view.node_to_index.get(&id).copied()
}

#[allow(dead_code)]
fn _unused(_: &HashMap<NodeId, usize>) {}

#[cfg(test)]
mod tests {
    use super::*;

    /// `0 -> 1 -> 2`, a path. Chosen because every quantity here can be worked
    /// out by hand on it, so a test failure points at the algorithm rather
    /// than at whether the fixture was understood.
    fn path3() -> GraphView {
        view(3, &[(0, 1), (1, 2)])
    }

    fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let index_to_node: Vec<NodeId> = (0..n).map(|i| i as NodeId).collect();
        let node_to_index: HashMap<NodeId, usize> =
            (0..n).map(|i| (i as NodeId, i)).collect();
        let mut out = vec![Vec::new(); n];
        let mut inc = vec![Vec::new(); n];
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        GraphView::from_adjacency_list(n, index_to_node, node_to_index, out, inc, None)
    }

    #[test]
    fn katz_ranks_the_end_of_a_path_highest() {
        // On 0->1->2 with beta=1: node 0 has no in-edges so it scores beta;
        // node 1 scores beta + alpha*x0; node 2 scores beta + alpha*x1. The
        // order is strictly increasing along the path, which is the property
        // that distinguishes Katz from a plain in-degree count -- both 1 and 2
        // have in-degree 1.
        let g = path3();
        let k = katz_centrality(&g, 0.1, 1.0, 1000, 1e-12).expect("converges");
        assert!(k[2] > k[1], "{k:?}");
        assert!(k[1] > k[0], "{k:?}");
    }

    #[test]
    fn katz_reports_divergence_rather_than_the_last_iterate() {
        // alpha above 1/lambda_max makes the sum diverge. The honest answer is
        // "no", not a vector that looks like a ranking.
        let g = view(3, &[(0, 1), (1, 2), (2, 0), (0, 2), (2, 1), (1, 0)]);
        assert!(katz_centrality(&g, 5.0, 1.0, 50, 1e-12).is_none());
    }

    #[test]
    fn hits_separates_the_hub_from_the_authority() {
        // 0 and 1 both point at 2. 2 is the authority and has no out-edges;
        // 0 and 1 are hubs and receive nothing.
        let g = view(3, &[(0, 2), (1, 2)]);
        let (hubs, auth) = hits(&g, 500, 1e-12).expect("converges");
        assert!(auth[2] > auth[0] && auth[2] > auth[1], "auth {auth:?}");
        assert!(hubs[0] > hubs[2] && hubs[1] > hubs[2], "hubs {hubs:?}");
        // NetworkX normalises each vector to sum 1.
        assert!((auth.iter().sum::<f64>() - 1.0).abs() < 1e-9);
        assert!((hubs.iter().sum::<f64>() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn personalised_rank_favours_the_source_side() {
        // Two disjoint edges: 0->1 and 2->3. Personalising on 0 must put no
        // mass on the far component, because nothing connects them and the
        // teleport does not go there.
        let g = view(4, &[(0, 1), (2, 3)]);
        let r = personalised_page_rank(&g, &[0], 0.85, 500, 1e-12);
        assert!(r[0] + r[1] > 0.99, "mass should stay in the source component: {r:?}");
        assert!(r[2] + r[3] < 0.01, "{r:?}");
    }

    #[test]
    fn personalised_rank_with_no_sources_is_plain_page_rank() {
        // Stated as a test because the fallback is easy to write as a panic or
        // a division by zero, and "no sources" is what an empty filter gives.
        let g = path3();
        let a = personalised_page_rank(&g, &[], 0.85, 500, 1e-12);
        let b = personalised_page_rank(&g, &[99], 0.85, 500, 1e-12); // out of range
        assert_eq!(a.len(), 3);
        for (x, y) in a.iter().zip(&b) {
            assert!((x - y).abs() < 1e-12, "{a:?} vs {b:?}");
        }
    }

    #[test]
    fn voterank_spreads_out_instead_of_picking_one_neighbourhood() {
        // Two stars, 0 at the centre of {1,2,3} and 4 at the centre of
        // {5,6,7}. Top-2 by in-degree would be a coin flip between the two
        // centres and could return both spokes of one star; VoteRank must
        // return one centre from each, because electing the first suppresses
        // its own neighbourhood's voting power.
        let g = view(8, &[(1, 0), (2, 0), (3, 0), (5, 4), (6, 4), (7, 4)]);
        let elected = vote_rank(&g, 2);
        assert_eq!(elected.len(), 2, "{elected:?}");
        assert!(elected.contains(&0) && elected.contains(&4), "{elected:?}");
    }

    #[test]
    fn voterank_on_an_edgeless_graph_elects_nobody() {
        // Not an empty-input guard for its own sake: the decrement is
        // n/edges, so an edgeless graph divides by zero and would return
        // whatever NaN comparisons happen to yield.
        let g = view(4, &[]);
        assert!(vote_rank(&g, 3).is_empty());
    }

    #[test]
    fn ranked_scores_break_ties_by_id() {
        let g = view(3, &[]);
        let r = ranked_scores(&g, &[1.0, 1.0, 1.0]);
        assert_eq!(r.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec![0, 1, 2]);
    }
}
