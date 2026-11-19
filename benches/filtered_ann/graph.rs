//! Graph-reachability predicates for the filtered sweep (#1985): "the k
//! nearest among the nodes within two hops of X".
//!
//! The graph is a directed k-nearest-neighbour graph over the corpus -- each
//! node links to its `degree` nearest others -- so linked nodes are similar,
//! the way items in a real graph tend to be. Selectivity is set by the degree;
//! correlation with the query by where the traversal starts: at the corpus
//! node nearest the query (positive), at a seeded random node (none), or at
//! the farthest (negative). Reach sets therefore differ in size from anchor
//! to anchor, and the sweep reports the mean it measured rather than a target.
//!
//! "Within two hops" is `out(x) ∪ out(out(x))`, which is what
//! `(x)-[:T*1..2]->(n)` matches on a graph with no self-loops and no parallel
//! edges: it can contain `x` itself, by way of a neighbour that links back.
//!
//! Everything here is outside the timed region.
use super::super::ann_common::{Metric, Vectors};
use super::Correlation;

/// Each node's `degree` nearest other nodes by L2, nearest first, ties by
/// index. No self-loops and no repeats, so a prefix of a list is the graph
/// of a smaller degree.
pub fn knn_graph(base: &Vectors, degree: usize) -> Vec<Vec<u32>> {
    let n = base.len();
    (0..n)
        .map(|i| {
            let mut ranked: Vec<(f32, u32)> = (0..n)
                .filter(|&j| j != i)
                .map(|j| (Metric::L2.distance(base.row(i), base.row(j)), j as u32))
                .collect();
            let keep = degree.min(ranked.len());
            if keep < ranked.len() {
                ranked.select_nth_unstable_by(keep, |a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                ranked.truncate(keep);
            }
            ranked.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            ranked.into_iter().map(|(_, j)| j).collect()
        })
        .collect()
}

/// Nodes reachable from `x` in one or two hops over the first `degree` links
/// of each list, `x` itself excluded.
///
/// Excluded even when a link comes back to it (`x -> a -> x`, common in a
/// k-NN graph). Neo4j's relationship isomorphism would let `*1..2` return
/// `x` that way; this engine's does not, so the statements say `<> x` and
/// the oracle agrees with them whichever way that is settled.
pub fn two_hop(adj: &[Vec<u32>], x: usize, degree: usize) -> Vec<bool> {
    let mut mask = vec![false; adj.len()];
    for &a in adj[x].iter().take(degree) {
        mask[a as usize] = true;
        for &b in adj[a as usize].iter().take(degree) {
            mask[b as usize] = true;
        }
    }
    mask[x] = false;
    mask
}

/// Where the traversal for `query` starts.
pub fn anchor(base: &Vectors, query: &[f32], correlation: Correlation, seed: u64) -> usize {
    use rand::{Rng, SeedableRng};
    let by_distance = || (0..base.len()).map(|i| (Metric::L2.distance(query, base.row(i)), i));
    match correlation {
        Correlation::Positive => by_distance()
            .min_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)))
            .map(|(_, i)| i)
            .unwrap_or(0),
        Correlation::Negative => by_distance()
            .max_by(|a, b| a.0.total_cmp(&b.0).then(b.1.cmp(&a.1)))
            .map(|(_, i)| i)
            .unwrap_or(0),
        Correlation::None => rand::rngs::StdRng::seed_from_u64(seed).gen_range(0..base.len()),
    }
}

/// The edge type that carries the graph of `degree`.
pub fn edge_type(degree: usize) -> String {
    format!("N{degree}")
}

fn literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    format!("[{}]", parts.join(", "))
}

/// The statement a user writes today: search the index, then keep the hits
/// the anchor reaches. The traversal filters the top `k` after the search, so
/// it returns fewer than `k` whenever the global nearest lie outside the reach.
pub fn cypher_post(degree: usize, anchor_id: usize, query: &[f32], k: usize) -> String {
    format!(
        "CALL db.index.vector.queryNodes('V', 'v', {q}, {k}) YIELD node \
         MATCH (node)<-[:{t}*1..2]-(x:V {{id: {anchor_id}}}) WHERE node <> x \
         RETURN DISTINCT node.id AS id",
        q = literal(query),
        t = edge_type(degree),
    )
}

/// Traverse first, then rank the reach exactly. Always correct; costs a
/// distance per reached node.
pub fn cypher_exact(degree: usize, anchor_id: usize, query: &[f32], k: usize) -> String {
    format!(
        "MATCH (x:V {{id: {anchor_id}}})-[:{t}*1..2]->(n:V) WHERE n <> x WITH DISTINCT n \
         RETURN n.id AS id, vector.distance(n.v, {q}, 'EUCLIDEAN') AS d ORDER BY d, id LIMIT {k}",
        q = literal(query),
        t = edge_type(degree),
    )
}
