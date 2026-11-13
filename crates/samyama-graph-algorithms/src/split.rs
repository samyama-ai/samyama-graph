//! Leakage-safe train/validation/test splits, and the check that proves one (ML-05).
//!
//! A random node split puts two records of the same entity, two members of one
//! tightly-knit community, or a record from next week on both sides of the
//! boundary. A model then scores well on the test set by recognising what it
//! was trained on, and the number is a lie nobody can see from the split.
//!
//! The splits here keep a whole **group** on one side:
//!
//! * **entity** -- groups are the connected components of the "same entity"
//!   edges (`SAME_AS` and the like), so a chain of any length stays together;
//! * **community** -- groups are Louvain communities;
//! * **source** -- groups share a value of a source property;
//! * **time** -- not grouped: nodes are ordered by a timestamp, the earliest go
//!   to train and the latest to test, and equal timestamps never straddle a
//!   boundary.
//!
//! The leakage checks do **not** reuse the split's grouping. `entity_leaks`
//! walks the edges up to `hops` away instead of reading components, and
//! `group_leaks`/`time_leaks` read the split membership a caller stored, which
//! may come from anywhere -- including a naive random split, which is the
//! negative control the check exists to catch.
//!
//! **Sizes are approximate when groups are large.** A group is never cut to hit
//! a ratio, so a split of 10 nodes where one entity owns 6 cannot be 80/10/10.
//! Groups are placed largest first, each into the split furthest below its
//! target, which keeps the overshoot to at most one group per split.

use crate::common::{GraphView, NodeId};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::collections::{BTreeMap, HashMap, VecDeque};

/// Split names for two and three ratios, in boundary order.
pub fn split_names(parts: usize) -> &'static [&'static str] {
    match parts {
        2 => &["train", "test"],
        _ => &["train", "val", "test"],
    }
}

/// Place whole groups into splits so each split's node count approaches
/// `ratios[s] * total`. Returns `(node, split index)` for every node.
///
/// Groups are shuffled by `seed`, then stably ordered largest first, so equal
/// sized groups land in a seed-dependent order and big groups are placed while
/// there is still room for them. Each goes to the split with the largest
/// remaining deficit; a split with ratio 0 never receives anything.
pub fn assign_groups(groups: &[Vec<NodeId>], ratios: &[f64], seed: u64) -> Vec<(NodeId, usize)> {
    let total: usize = groups.iter().map(Vec::len).sum();
    let mut order: Vec<usize> = (0..groups.len()).collect();
    order.shuffle(&mut rand::rngs::StdRng::seed_from_u64(seed));
    order.sort_by_key(|&g| std::cmp::Reverse(groups[g].len()));

    let targets: Vec<f64> = ratios.iter().map(|r| r * total as f64).collect();
    let mut counts = vec![0usize; ratios.len()];
    let mut out = Vec::with_capacity(total);
    for g in order {
        let s = (0..ratios.len())
            .filter(|&s| ratios[s] > 0.0)
            .max_by(|&a, &b| {
                let da = targets[a] - counts[a] as f64;
                let db = targets[b] - counts[b] as f64;
                // On a tie, the earlier split: `max_by` keeps the last maximum.
                da.total_cmp(&db).then(b.cmp(&a))
            })
            .expect("at least one ratio is positive");
        counts[s] += groups[g].len();
        out.extend(groups[g].iter().map(|&n| (n, s)));
    }
    out
}

/// Order nodes by time and cut at the cumulative ratios: train gets the
/// earliest, test the latest. Nodes with equal times share a split, so no
/// training record is ever later than, or level with, a test record.
pub fn time_split(times: &[(NodeId, f64)], ratios: &[f64]) -> Vec<(NodeId, usize)> {
    let mut sorted: Vec<(NodeId, f64)> = times.to_vec();
    sorted.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    let n = sorted.len();
    let mut cumulative = Vec::with_capacity(ratios.len());
    let mut acc = 0.0;
    for r in ratios {
        acc += r;
        cumulative.push((acc * n as f64).round() as usize);
    }

    let mut out = Vec::with_capacity(n);
    let (mut s, mut i) = (0usize, 0usize);
    while i < n {
        let mut j = i;
        while j < n && sorted[j].1 == sorted[i].1 {
            j += 1;
        }
        while s + 1 < ratios.len() && i >= cumulative[s] {
            s += 1;
        }
        out.extend(sorted[i..j].iter().map(|&(node, _)| (node, s)));
        i = j;
    }
    out
}

/// Two nodes no more than `hops` apart over the view's edges (either
/// direction) that sit in different splits: the same entity on both sides.
///
/// Every pair is reported once, smaller id first, with its distance. Nodes
/// missing from `split_of` are walked through but never reported.
pub fn entity_leaks(
    view: &GraphView,
    split_of: &HashMap<NodeId, usize>,
    hops: usize,
) -> Vec<(NodeId, NodeId, usize)> {
    let n = view.node_count;
    let mut out = Vec::new();
    let mut dist = vec![usize::MAX; n];
    let mut seen = Vec::new();
    for start in 0..n {
        let a = view.index_to_node[start];
        let Some(&sa) = split_of.get(&a) else {
            continue;
        };
        for &i in &seen {
            dist[i] = usize::MAX;
        }
        seen.clear();
        dist[start] = 0;
        seen.push(start);
        let mut queue = VecDeque::from([start]);
        while let Some(u) = queue.pop_front() {
            if dist[u] == hops {
                continue;
            }
            for &v in view.successors(u).iter().chain(view.predecessors(u)) {
                if dist[v] != usize::MAX {
                    continue;
                }
                dist[v] = dist[u] + 1;
                seen.push(v);
                queue.push_back(v);
                let b = view.index_to_node[v];
                if a < b && split_of.get(&b).is_some_and(|&sb| sb != sa) {
                    out.push((a, b, dist[v]));
                }
            }
        }
    }
    out.sort_unstable();
    out
}

/// A group found in more than one split.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupLeak {
    /// The group's key, as the caller rendered it.
    pub key: String,
    /// The earlier split, its smallest node, and how many of the group it holds.
    pub split_a: usize,
    pub node_a: NodeId,
    pub count_a: usize,
    /// The later split, likewise.
    pub split_b: usize,
    pub node_b: NodeId,
    pub count_b: usize,
}

/// One leak per group and pair of splits the group spans.
pub fn group_leaks(keys: &[(NodeId, String)], split_of: &HashMap<NodeId, usize>) -> Vec<GroupLeak> {
    // key -> split -> (smallest node, count); BTreeMaps so the output order is fixed.
    let mut by_key: BTreeMap<&str, BTreeMap<usize, (NodeId, usize)>> = BTreeMap::new();
    for (node, key) in keys {
        let Some(&s) = split_of.get(node) else {
            continue;
        };
        let slot = by_key
            .entry(key.as_str())
            .or_default()
            .entry(s)
            .or_insert((*node, 0));
        slot.0 = slot.0.min(*node);
        slot.1 += 1;
    }
    let mut out = Vec::new();
    for (key, splits) in by_key {
        let splits: Vec<(usize, (NodeId, usize))> = splits.into_iter().collect();
        for x in 0..splits.len() {
            for y in x + 1..splits.len() {
                let (sa, (na, ca)) = splits[x];
                let (sb, (nb, cb)) = splits[y];
                out.push(GroupLeak {
                    key: key.to_string(),
                    split_a: sa,
                    node_a: na,
                    count_a: ca,
                    split_b: sb,
                    node_b: nb,
                    count_b: cb,
                });
            }
        }
    }
    out
}

/// Every node in an earlier split whose time is after the earliest time in a
/// later split, paired with that earliest node: `(late, early, s_late, s_early)`.
///
/// Splits are ordered by index (train 0, then val, then test). Equal times are
/// not a leak -- the record is not from the future -- though `time_split`
/// never produces them across a boundary.
pub fn time_leaks(
    times: &[(NodeId, f64)],
    split_of: &HashMap<NodeId, usize>,
    parts: usize,
) -> Vec<(NodeId, NodeId, usize, usize)> {
    // The earliest (time, node) in each split.
    let mut earliest: Vec<Option<(f64, NodeId)>> = vec![None; parts];
    for &(node, t) in times {
        let Some(&s) = split_of.get(&node) else {
            continue;
        };
        let e = &mut earliest[s];
        if e.is_none_or(|(et, en)| t < et || (t == et && node < en)) {
            *e = Some((t, node));
        }
    }
    let mut out = Vec::new();
    for &(node, t) in times {
        let Some(&s) = split_of.get(&node) else {
            continue;
        };
        for (later, e) in earliest.iter().enumerate().skip(s + 1) {
            if let Some((et, en)) = e {
                if t > *et {
                    out.push((node, *en, s, later));
                }
            }
        }
    }
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let mut out = vec![Vec::new(); n];
        let mut inc = vec![Vec::new(); n];
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        let ids: Vec<NodeId> = (0..n as NodeId).collect();
        let index = ids.iter().enumerate().map(|(i, &id)| (id, i)).collect();
        GraphView::from_adjacency_list(n, ids, index, out, inc, None)
    }

    fn split_map(pairs: &[(NodeId, usize)]) -> HashMap<NodeId, usize> {
        pairs.iter().copied().collect()
    }

    #[test]
    fn groups_never_straddle_and_sizes_follow_the_ratios() {
        // 30 groups of sizes 1..=4 (1,2,3,4 repeating): 73 nodes.
        let mut groups = Vec::new();
        let mut next = 0u64;
        for g in 0..30 {
            let size = 1 + g % 4;
            groups.push((next..next + size as u64).collect::<Vec<_>>());
            next += size as u64;
        }
        let out = assign_groups(&groups, &[0.8, 0.1, 0.1], 7);
        assert_eq!(out.len(), 73);
        let of = split_map(&out);
        for g in &groups {
            let s = of[&g[0]];
            assert!(
                g.iter().all(|n| of[n] == s),
                "group {g:?} straddles a boundary"
            );
        }
        let mut counts = [0usize; 3];
        for (_, s) in &out {
            counts[*s] += 1;
        }
        // Targets 58.4 / 7.3 / 7.3; overshoot is at most one group (4 nodes).
        assert!((counts[0] as f64 - 58.4).abs() <= 4.0, "{counts:?}");
        assert!((counts[1] as f64 - 7.3).abs() <= 4.0, "{counts:?}");
        assert!((counts[2] as f64 - 7.3).abs() <= 4.0, "{counts:?}");
    }

    #[test]
    fn singleton_groups_hit_the_ratios_exactly_and_the_seed_decides_who_goes_where() {
        let groups: Vec<Vec<NodeId>> = (0..100).map(|i| vec![i]).collect();
        let a = assign_groups(&groups, &[0.7, 0.2, 0.1], 1);
        let b = assign_groups(&groups, &[0.7, 0.2, 0.1], 1);
        let c = assign_groups(&groups, &[0.7, 0.2, 0.1], 2);
        assert_eq!(a, b, "same seed, same split");
        assert_ne!(split_map(&a), split_map(&c), "a different seed moves nodes");
        let mut counts = [0usize; 3];
        for (_, s) in &a {
            counts[*s] += 1;
        }
        assert_eq!(counts, [70, 20, 10]);
    }

    #[test]
    fn a_zero_ratio_split_receives_nothing() {
        let groups: Vec<Vec<NodeId>> = (0..10).map(|i| vec![i]).collect();
        let out = assign_groups(&groups, &[0.5, 0.0, 0.5], 3);
        assert!(out.iter().all(|&(_, s)| s != 1));
    }

    #[test]
    fn time_split_puts_the_earliest_in_train_and_keeps_ties_together() {
        // Times 0..10, with nodes 7 and 8 sharing time 7.
        let times: Vec<(NodeId, f64)> = (0..10u64)
            .map(|i| (i, if i == 8 { 7.0 } else { i as f64 }))
            .collect();
        let out = time_split(&times, &[0.8, 0.2]);
        let of = split_map(&out);
        assert_eq!(of[&7], of[&8], "equal times share a split");
        for i in 0..7 {
            assert_eq!(of[&i], 0);
        }
        assert_eq!(of[&9], 1);
        assert!(time_leaks(&times, &of, 2).is_empty());
    }

    #[test]
    fn time_leaks_finds_a_training_record_from_the_future() {
        let times: Vec<(NodeId, f64)> = vec![(1, 1.0), (2, 2.0), (3, 9.0), (4, 5.0)];
        // Node 3 (t=9) is in train but test starts at t=5.
        let of = split_map(&[(1, 0), (2, 0), (3, 0), (4, 1)]);
        assert_eq!(time_leaks(&times, &of, 2), vec![(3, 4, 0, 1)]);
        // An equal time is not from the future.
        let times = vec![(1, 1.0), (2, 5.0), (4, 5.0)];
        let of = split_map(&[(1, 0), (2, 0), (4, 1)]);
        assert!(time_leaks(&times, &of, 2).is_empty());
    }

    #[test]
    fn entity_leaks_follow_chains_up_to_hops_in_either_direction() {
        // Chain 0 -> 1 <- 2 -> 3, plus an unlinked node 4.
        let v = view(5, &[(0, 1), (2, 1), (2, 3)]);
        // 0 and 3 are three hops apart, in different splits.
        let of = split_map(&[(0, 0), (1, 0), (2, 0), (3, 1), (4, 1)]);
        assert_eq!(
            entity_leaks(&v, &of, 3),
            vec![(0, 3, 3), (1, 3, 2), (2, 3, 1)]
        );
        assert_eq!(
            entity_leaks(&v, &of, 1),
            vec![(2, 3, 1)],
            "hops bounds the walk"
        );
        // Whole chain on one side: no leak, and the unlinked node never is one.
        let of = split_map(&[(0, 1), (1, 1), (2, 1), (3, 1), (4, 0)]);
        assert!(entity_leaks(&v, &of, 3).is_empty());
    }

    #[test]
    fn group_leaks_reports_each_group_once_per_pair_of_splits() {
        let keys: Vec<(NodeId, String)> = vec![
            (1, "a".into()),
            (2, "a".into()),
            (3, "a".into()),
            (4, "b".into()),
            (5, "b".into()),
        ];
        let of = split_map(&[(1, 0), (2, 1), (3, 2), (4, 0), (5, 0)]);
        let leaks = group_leaks(&keys, &of);
        assert_eq!(
            leaks.len(),
            3,
            "a spans three splits: three pairs; b is clean"
        );
        assert!(leaks.iter().all(|l| l.key == "a"));
        assert_eq!((leaks[0].node_a, leaks[0].node_b), (1, 2));
    }
}
