//! Community detection: Louvain, label propagation, and modularity (ALGO-01).
//!
//! `cdlp` already exists and is LDBC's *deterministic* label propagation,
//! specified so Graphalytics can compare engines. These are the two a user
//! reaches for instead:
//!
//! * **Louvain** optimises modularity directly and finds the community
//!   structure people mean when they say "communities". It is the standard.
//! * **Modularity** scores a partition, whoever produced it — so a caller can
//!   compare Louvain's answer against their own labels.
//!
//! There is deliberately **no second label-propagation implementation here.**
//! One was written and then deleted: a deterministic label propagation
//! collapses every connected graph to a single community, which is inherent
//! to the variant rather than a bug, and it scored Q = 0 where Louvain scored
//! 0.357 on the same graph. `cdlp` already is label propagation — LDBC's,
//! specified so Graphalytics can compare engines — so `algo.labelPropagation`
//! is routed there. Shipping a second, worse one to raise an algorithm count
//! would be padding.
//!
//! # Determinism
//!
//! Louvain is order-sensitive: the community a node lands in can
//! depend on which neighbour it examined first. Left to a hash set's
//! iteration order that makes two runs over the same graph disagree, which
//! reads to a user as the data having changed.
//!
//! So node order is the view's index order everywhere, and ties break by the
//! smallest community id. That costs nothing and makes the result
//! reproducible, which matters more here than in most algorithms because the
//! answer is a *labelling* and any relabelling looks like a different answer.

use std::collections::HashMap;

use crate::common::{GraphView, NodeId};

/// A partition: community id per node, in the view's index order.
pub type Communities = Vec<usize>;

/// Undirected neighbours with their edge weights, deduplicated by target.
///
/// Community detection is defined on the undirected graph, and a reciprocal
/// pair is one relationship rather than two — the same collapse the shape
/// metrics use.
fn adjacency(view: &GraphView) -> Vec<Vec<(usize, f64)>> {
    let n = view.node_count;
    let mut adj: Vec<HashMap<usize, f64>> = vec![HashMap::new(); n];
    for u in 0..n {
        let lo = view.out_offsets[u];
        let hi = view.out_offsets[u + 1];
        for slot in lo..hi {
            let v = view.out_targets[slot];
            if u == v {
                continue; // a self-loop joins no two communities
            }
            let w = view.weights.as_ref().map_or(1.0, |ws| ws[slot]);
            // `max` rather than `+`: a reciprocal pair is one relationship,
            // and summing would weight it double against every other edge.
            let e = adj[u].entry(v).or_insert(0.0);
            *e = e.max(w);
            let e = adj[v].entry(u).or_insert(0.0);
            *e = e.max(w);
        }
    }
    adj.into_iter()
        .map(|m| {
            let mut v: Vec<(usize, f64)> = m.into_iter().collect();
            v.sort_by_key(|&(t, _)| t); // deterministic order
            v
        })
        .collect()
}

/// Modularity of a partition, Newman's Q.
///
/// The fraction of edges inside communities, minus what that fraction would be
/// if the same degrees were wired at random. Positive means the partition finds
/// more internal structure than chance; around zero means it finds none.
///
/// Scoring a partition rather than producing one, so a caller can compare
/// Louvain's answer against labels they already have — which is the only way
/// to tell whether a community algorithm is telling them something they did
/// not know.
pub fn modularity(view: &GraphView, communities: &[usize]) -> Option<f64> {
    let adj = adjacency(view);
    let n = view.node_count;
    if communities.len() != n {
        return None;
    }
    let two_m: f64 = adj.iter().flat_map(|a| a.iter().map(|&(_, w)| w)).sum();
    if two_m == 0.0 {
        return None; // no edges: modularity is 0/0, not 0
    }
    let mut deg = vec![0.0; n];
    for (u, a) in adj.iter().enumerate() {
        deg[u] = a.iter().map(|&(_, w)| w).sum();
    }
    let mut inside = 0.0;
    let mut tot: HashMap<usize, f64> = HashMap::new();
    for (u, a) in adj.iter().enumerate() {
        *tot.entry(communities[u]).or_insert(0.0) += deg[u];
        for &(v, w) in a {
            if communities[u] == communities[v] {
                inside += w;
            }
        }
    }
    let q: f64 = inside / two_m
        - tot.values().map(|&t| (t / two_m) * (t / two_m)).sum::<f64>();
    Some(q)
}

/// Louvain: greedily move nodes to the neighbouring community that gains the
/// most modularity, then contract and repeat.
///
/// Returns the partition of the *original* nodes, with community ids
/// renumbered from zero in order of first appearance so two runs produce
/// identical labels rather than a relabelling of the same partition.
pub fn louvain(view: &GraphView, max_passes: usize) -> Communities {
    let n = view.node_count;
    let adj = adjacency(view);
    let mut node_to_comm: Vec<usize> = (0..n).collect();

    // The graph gets contracted each pass; `level` maps original node -> its
    // community in the current contracted graph.
    let mut level: Vec<usize> = (0..n).collect();
    let mut cur_adj = adj;
    let mut cur_n = n;

    for _ in 0..max_passes {
        let two_m: f64 = cur_adj.iter().flat_map(|a| a.iter().map(|&(_, w)| w)).sum();
        if two_m == 0.0 {
            break;
        }
        let mut comm: Vec<usize> = (0..cur_n).collect();
        let mut deg = vec![0.0; cur_n];
        for (u, a) in cur_adj.iter().enumerate() {
            deg[u] = a.iter().map(|&(_, w)| w).sum();
        }
        let mut tot = deg.clone();

        let mut improved = false;
        // One sweep in index order. More sweeps converge further; one is
        // enough to make progress and keeps the cost predictable, and the
        // outer pass loop provides the rest.
        for _sweep in 0..10 {
            let mut moved = false;
            for u in 0..cur_n {
                let cu = comm[u];
                // Weight from u into each neighbouring community.
                let mut links: HashMap<usize, f64> = HashMap::new();
                for &(v, w) in &cur_adj[u] {
                    // A self-loop is weight *inside* u, not weight from u to
                    // any community, so it must not count toward the gain of
                    // joining one. It still counts toward `deg[u]`, which is
                    // what makes a heavy super-node resist being absorbed.
                    if v == u {
                        continue;
                    }
                    *links.entry(comm[v]).or_insert(0.0) += w;
                }
                tot[cu] -= deg[u];
                let base = links.get(&cu).copied().unwrap_or(0.0) - tot[cu] * deg[u] / two_m;
                let (mut best, mut best_gain) = (cu, base);
                // Sorted, so a tie goes to the smallest community id rather
                // than to whichever the hash map happened to yield first.
                let mut cands: Vec<(usize, f64)> = links.into_iter().collect();
                cands.sort_by_key(|&(c, _)| c);
                for (c, w) in cands {
                    let gain = w - tot[c] * deg[u] / two_m;
                    if gain > best_gain + 1e-12 {
                        best_gain = gain;
                        best = c;
                    }
                }
                tot[best] += deg[u];
                if best != cu {
                    comm[u] = best;
                    moved = true;
                    improved = true;
                }
            }
            if !moved {
                break;
            }
        }
        if !improved {
            break;
        }

        // Renumber and contract.
        let mut remap: HashMap<usize, usize> = HashMap::new();
        for u in 0..cur_n {
            let next = remap.len();
            remap.entry(comm[u]).or_insert(next);
        }
        for l in level.iter_mut() {
            *l = remap[&comm[*l]];
        }
        let k = remap.len();
        let mut next_adj: Vec<HashMap<usize, f64>> = vec![HashMap::new(); k];
        for (u, a) in cur_adj.iter().enumerate() {
            for &(v, w) in a {
                let (cu, cv) = (remap[&comm[u]], remap[&comm[v]]);
                // Intra-community weight becomes a **self-loop**, it is not
                // dropped. That weight is what makes a community heavy and
                // resistant to being merged further; without it every
                // super-node carries only its external degree and the next
                // pass finds a merge profitable every time.
                //
                // On two triangles joined by one edge, pass 1 found the right
                // partition -- Q = 0.3571, exactly NetworkX's -- and pass 2
                // collapsed it to a single community at Q = 0. The symptom was
                // an algorithm that got *worse* the longer it ran.
                //
                // Each edge is visited twice here, once from each endpoint,
                // so an internal edge of weight w accumulates 2w on the
                // self-loop. That is exactly what a self-loop should
                // contribute to the super-node's degree.
                *next_adj[cu].entry(cv).or_insert(0.0) += w;
            }
        }
        cur_adj = next_adj
            .into_iter()
            .map(|m| {
                let mut v: Vec<(usize, f64)> = m.into_iter().collect();
                v.sort_by_key(|&(t, _)| t);
                v
            })
            .collect();
        cur_n = k;
        if k == 1 {
            break;
        }
    }
    node_to_comm.copy_from_slice(&level);
    renumber(&node_to_comm)
}

/// Renumber community ids from zero in order of first appearance.
///
/// Two runs that find the same partition must produce the same labels. Without
/// this they differ by a permutation, which compares unequal and reads as a
/// different answer.
fn renumber(labels: &[usize]) -> Communities {
    let mut remap: HashMap<usize, usize> = HashMap::new();
    labels
        .iter()
        .map(|&l| {
            let next = remap.len();
            *remap.entry(l).or_insert(next)
        })
        .collect()
}

/// Pair communities with node ids.
pub fn with_ids(view: &GraphView, comms: &[usize]) -> Vec<(NodeId, usize)> {
    comms
        .iter()
        .enumerate()
        .map(|(i, &c)| (view.index_to_node[i], c))
        .collect()
}
