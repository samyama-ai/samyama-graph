//! GraphRAG retrieval: from the nodes most similar to a question, out through
//! the graph to the facts around them (AI-07, #1702).
//!
//! The vector search that picks the starting nodes ("seeds") happens in the
//! engine, which owns the index; it hands this module each seed's dense index
//! and similarity. What happens next is graph work, and is here:
//!
//! | Strategy | Answers | Rows |
//! |---|---|---|
//! | [`Strategy::Local`] | "tell me about this" | each seed and the nodes within `hops` of it, scored `similarity * decay^hops`, with the path from the seed |
//! | [`Strategy::Global`] | "what are the main themes" | the members of the `communities` communities whose seeds are most similar in total, each scored by its community's total |
//! | [`Strategy::Drift`] | both | the local rows, then the rest of each seed's community, scored one hop further out than `hops` |
//!
//! Every strategy walks the graph undirected: "what is connected to this
//! fact" does not care which way an edge was written. Nodes `admit` refuses --
//! the engine's time window -- are neither returned nor walked through, so a
//! path never passes through a fact outside the window.
//!
//! Deterministic: seeds are taken in the order given, ties in score are
//! broken by fewer hops and then by node index, and the walk is
//! [`crate::frontier`]'s, whose order is fixed.

use std::collections::HashMap;

use crate::common::GraphView;
use crate::frontier::{self, Bfs, Direction, Visit};

/// How a retrieval spreads from its seeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Local,
    Global,
    Drift,
}

impl Strategy {
    /// The names a caller uses, lower-case.
    pub const NAMES: [&'static str; 3] = ["local", "global", "drift"];

    pub fn parse(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "local" => Some(Self::Local),
            "global" => Some(Self::Global),
            "drift" => Some(Self::Drift),
            _ => None,
        }
    }

    /// Whether this strategy reads communities, so the caller knows to
    /// compute a partition.
    pub fn needs_communities(self) -> bool {
        matches!(self, Self::Global | Self::Drift)
    }
}

/// A starting node: its dense index in the view and its similarity to the
/// question, higher is closer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Seed {
    pub index: usize,
    pub similarity: f64,
}

/// How far and how much to retrieve.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetrievalConfig {
    pub strategy: Strategy,
    /// How many hops out from a seed the local walk goes.
    pub hops: usize,
    /// What one hop multiplies a score by, in `(0, 1]`.
    pub decay: f64,
    /// How many communities a global retrieval returns.
    pub communities: usize,
    /// The most rows returned.
    pub limit: usize,
}

/// One retrieved fact.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub index: usize,
    pub score: f64,
    /// Hops from `seed` along `path`; `None` for a node reached through its
    /// community rather than a walk.
    pub hops: Option<usize>,
    /// The seed it was reached from; `None` in a global retrieval.
    pub seed: Option<usize>,
    /// Its community, when the strategy reads communities.
    pub community: Option<usize>,
    /// Dense indexes from the seed to this node, both included; empty when
    /// `hops` is `None`.
    pub path: Vec<usize>,
}

/// Retrieve from `seeds`. `partition` is required by
/// [`Strategy::needs_communities`] strategies (each node's community, as
/// `louvain` returns it) and ignored by the others.
///
/// # Panics
/// When the strategy needs a partition and none is given, or the partition
/// does not cover the view.
pub fn retrieve(
    view: &GraphView,
    seeds: &[Seed],
    partition: Option<&[usize]>,
    admit: &dyn Fn(usize) -> bool,
    config: &RetrievalConfig,
) -> Vec<Hit> {
    let partition = if config.strategy.needs_communities() {
        let p = partition.expect("this strategy needs a partition");
        assert_eq!(
            p.len(),
            view.node_count,
            "the partition does not cover the view"
        );
        Some(p)
    } else {
        None
    };
    let seeds: Vec<Seed> = seeds.iter().copied().filter(|s| admit(s.index)).collect();
    let mut hits = match config.strategy {
        Strategy::Local => local(view, &seeds, admit, config),
        Strategy::Global => global(&seeds, partition.unwrap_or_default(), admit, config),
        Strategy::Drift => drift(view, &seeds, partition.unwrap_or_default(), admit, config),
    };
    if let Some(p) = partition {
        for h in &mut hits {
            h.community = Some(p[h.index]);
        }
    }
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(
                a.hops
                    .unwrap_or(usize::MAX)
                    .cmp(&b.hops.unwrap_or(usize::MAX)),
            )
            .then(a.index.cmp(&b.index))
    });
    hits.truncate(config.limit);
    hits
}

/// Is `candidate` a better reason to return a node than `current`? Higher
/// score, then fewer hops. Equal is not better, so the earlier seed keeps it.
fn better(candidate: &Hit, current: &Hit) -> bool {
    match candidate.score.total_cmp(&current.score) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => {
            candidate.hops.unwrap_or(usize::MAX) < current.hops.unwrap_or(usize::MAX)
        }
    }
}

fn local(
    view: &GraphView,
    seeds: &[Seed],
    admit: &dyn Fn(usize) -> bool,
    config: &RetrievalConfig,
) -> Vec<Hit> {
    let n = view.node_count;
    let mut best: HashMap<usize, Hit> = HashMap::new();
    let mut bfs = Bfs::new(n);
    let mut parent = vec![usize::MAX; n];
    for seed in seeds {
        bfs.walk(
            &[seed.index],
            |u, depth| {
                let more = depth < config.hops;
                frontier::neighbours(view, u, Direction::Both).filter(move |&w| more && admit(w))
            },
            |e| {
                if e.discovered {
                    parent[e.to] = e.from;
                }
                Visit::Continue
            },
        );
        for &v in bfs.reached() {
            let hops = bfs.depth(v).expect("a reached node has a depth");
            let mut path = vec![v];
            let mut at = v;
            while at != seed.index {
                at = parent[at];
                path.push(at);
            }
            path.reverse();
            let hit = Hit {
                index: v,
                score: seed.similarity * config.decay.powi(hops as i32),
                hops: Some(hops),
                seed: Some(seed.index),
                community: None,
                path,
            };
            match best.get(&v) {
                Some(current) if !better(&hit, current) => {}
                _ => {
                    best.insert(v, hit);
                }
            }
        }
        for &v in bfs.reached() {
            parent[v] = usize::MAX;
        }
    }
    best.into_values().collect()
}

/// Communities ranked by the total similarity of the seeds in them, highest
/// first; ties by the community's smallest seed index.
fn ranked_communities(seeds: &[Seed], partition: &[usize]) -> Vec<(usize, f64)> {
    let mut total: Vec<(usize, f64, usize)> = Vec::new();
    for s in seeds {
        let c = partition[s.index];
        match total.iter_mut().find(|(id, _, _)| *id == c) {
            Some(entry) => {
                entry.1 += s.similarity;
                entry.2 = entry.2.min(s.index);
            }
            None => total.push((c, s.similarity, s.index)),
        }
    }
    total.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.2.cmp(&b.2)));
    total.into_iter().map(|(c, sum, _)| (c, sum)).collect()
}

fn global(
    seeds: &[Seed],
    partition: &[usize],
    admit: &dyn Fn(usize) -> bool,
    config: &RetrievalConfig,
) -> Vec<Hit> {
    let chosen: Vec<(usize, f64)> = ranked_communities(seeds, partition)
        .into_iter()
        .take(config.communities)
        .collect();
    (0..partition.len())
        .filter(|&v| admit(v))
        .filter_map(|v| {
            let &(_, score) = chosen.iter().find(|(c, _)| *c == partition[v])?;
            Some(Hit {
                index: v,
                score,
                hops: None,
                seed: None,
                community: None,
                path: Vec::new(),
            })
        })
        .collect()
}

fn drift(
    view: &GraphView,
    seeds: &[Seed],
    partition: &[usize],
    admit: &dyn Fn(usize) -> bool,
    config: &RetrievalConfig,
) -> Vec<Hit> {
    let mut hits = local(view, seeds, admit, config);
    let mut taken: Vec<bool> = vec![false; view.node_count];
    for h in &hits {
        taken[h.index] = true;
    }
    // One step past the local walk: the community is a looser tie than any
    // path the walk followed, so it scores below everything the walk found
    // from the same seed.
    let community_decay = config.decay.powi(config.hops as i32 + 1);
    for seed in seeds {
        let c = partition[seed.index];
        for v in 0..view.node_count {
            if partition[v] != c || taken[v] || !admit(v) {
                continue;
            }
            taken[v] = true;
            hits.push(Hit {
                index: v,
                score: seed.similarity * community_decay,
                hops: None,
                seed: Some(seed.index),
                community: None,
                path: Vec::new(),
            });
        }
    }
    hits
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
        let (mut out, mut inc) = (vec![Vec::new(); n], vec![Vec::new(); n]);
        for &(a, b) in edges {
            out[a].push(b);
            inc[b].push(a);
        }
        GraphView::from_adjacency_list(
            n,
            (0..n as u64).collect(),
            (0..n).map(|i| (i as u64, i)).collect(),
            out,
            inc,
            None,
        )
    }

    /// Two communities: a chain 0 - 1 - 2 - 3 and a triangle 4 - 5 - 6, joined
    /// by the single edge 3 -> 4. 7 is alone.
    fn graph() -> (GraphView, Vec<usize>) {
        let v = view(8, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 4)]);
        (v, vec![0, 0, 0, 0, 1, 1, 1, 2])
    }

    fn config(strategy: Strategy) -> RetrievalConfig {
        RetrievalConfig {
            strategy,
            hops: 2,
            decay: 0.5,
            communities: 1,
            limit: 50,
        }
    }

    fn all(_: usize) -> bool {
        true
    }

    fn by_index(hits: &[Hit]) -> HashMap<usize, &Hit> {
        hits.iter().map(|h| (h.index, h)).collect()
    }

    #[test]
    fn local_walks_both_ways_to_the_hop_limit_with_decaying_scores_and_paths() {
        let (v, _) = graph();
        let hits = retrieve(
            &v,
            &[Seed {
                index: 2,
                similarity: 0.8,
            }],
            None,
            &all,
            &config(Strategy::Local),
        );
        let h = by_index(&hits);
        // 2 hops either way along the chain, and across 3 -> 4 into the triangle.
        let mut got: Vec<usize> = h.keys().copied().collect();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3, 4]);
        assert_eq!((h[&2].score, h[&2].hops), (0.8, Some(0)));
        assert_eq!((h[&1].score, h[&1].hops), (0.4, Some(1)));
        assert_eq!((h[&4].score, h[&4].hops), (0.2, Some(2)));
        assert_eq!(h[&4].path, vec![2, 3, 4]);
        assert_eq!(h[&0].path, vec![2, 1, 0], "a predecessor was not walked");
        assert!(hits
            .iter()
            .all(|x| x.seed == Some(2) && x.community.is_none()));
        // Highest score first.
        assert!(hits.windows(2).all(|w| w[0].score >= w[1].score));
    }

    #[test]
    fn a_node_near_two_seeds_keeps_its_best_reason() {
        let (v, _) = graph();
        let seeds = [
            Seed {
                index: 0,
                similarity: 0.9,
            },
            Seed {
                index: 5,
                similarity: 0.6,
            },
        ];
        let hits = retrieve(&v, &seeds, None, &all, &config(Strategy::Local));
        let h = by_index(&hits);
        // 4 is 1 hop from seed 5 (0.3) and 4 hops from seed 0 (out of range).
        assert_eq!((h[&4].seed, h[&4].score), (Some(5), 0.3));
        // 2 is 2 hops from seed 0 (0.225); seed 5 is 3 away.
        assert_eq!((h[&2].seed, h[&2].hops), (Some(0), Some(2)));
    }

    #[test]
    fn equal_scores_go_to_the_earlier_seed() {
        // 0 - 1 - 2 with equal seeds at both ends: 1 scores 0.25 from each,
        // one hop away, and the seed given first keeps it.
        let v = view(3, &[(0, 1), (1, 2)]);
        let seeds = [
            Seed {
                index: 0,
                similarity: 0.5,
            },
            Seed {
                index: 2,
                similarity: 0.5,
            },
        ];
        let hits = retrieve(&v, &seeds, None, &all, &config(Strategy::Local));
        assert_eq!(
            by_index(&hits)[&1].seed,
            Some(0),
            "the tie went to the later seed"
        );
        let swapped = [seeds[1], seeds[0]];
        let hits = retrieve(&v, &swapped, None, &all, &config(Strategy::Local));
        assert_eq!(by_index(&hits)[&1].seed, Some(2));
    }

    #[test]
    fn hops_zero_returns_only_the_seeds() {
        let (v, _) = graph();
        let c = RetrievalConfig {
            hops: 0,
            ..config(Strategy::Local)
        };
        let hits = retrieve(
            &v,
            &[
                Seed {
                    index: 2,
                    similarity: 0.8,
                },
                Seed {
                    index: 6,
                    similarity: 0.7,
                },
            ],
            None,
            &all,
            &c,
        );
        assert_eq!(hits.iter().map(|h| h.index).collect::<Vec<_>>(), vec![2, 6]);
    }

    #[test]
    fn a_refused_node_is_neither_returned_nor_walked_through() {
        let (v, _) = graph();
        let not_3 = |i: usize| i != 3;
        let c = RetrievalConfig {
            hops: 5,
            ..config(Strategy::Local)
        };
        let hits = retrieve(
            &v,
            &[Seed {
                index: 2,
                similarity: 1.0,
            }],
            None,
            &not_3,
            &c,
        );
        let got: Vec<usize> = by_index(&hits).keys().copied().collect();
        assert!(!got.contains(&3));
        assert!(!got.contains(&4), "the walk passed through a refused node");
        // A refused seed is not a seed.
        let none = retrieve(
            &v,
            &[Seed {
                index: 3,
                similarity: 1.0,
            }],
            None,
            &not_3,
            &c,
        );
        assert!(none.is_empty());
    }

    #[test]
    fn global_returns_the_communities_with_the_most_similar_seeds() {
        let (v, p) = graph();
        // Community 1 has two seeds (0.5 + 0.4 = 0.9); community 0 one (0.8).
        let seeds = [
            Seed {
                index: 1,
                similarity: 0.8,
            },
            Seed {
                index: 5,
                similarity: 0.5,
            },
            Seed {
                index: 6,
                similarity: 0.4,
            },
        ];
        let hits = retrieve(&v, &seeds, Some(&p), &all, &config(Strategy::Global));
        assert_eq!(
            hits.iter().map(|h| h.index).collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert!(hits
            .iter()
            .all(|h| (h.score - 0.9).abs() < 1e-12 && h.community == Some(1) && h.hops.is_none()));
        let two = RetrievalConfig {
            communities: 2,
            ..config(Strategy::Global)
        };
        let hits = retrieve(&v, &seeds, Some(&p), &all, &two);
        assert_eq!(
            hits.len(),
            7,
            "community 2 has no seed and must not be returned"
        );
        assert_eq!(hits.last().unwrap().score, 0.8);
    }

    #[test]
    fn drift_adds_the_rest_of_each_seeds_community_below_the_walk() {
        let (v, p) = graph();
        let c = RetrievalConfig {
            hops: 1,
            ..config(Strategy::Drift)
        };
        let hits = retrieve(
            &v,
            &[Seed {
                index: 0,
                similarity: 0.8,
            }],
            Some(&p),
            &all,
            &c,
        );
        let h = by_index(&hits);
        // Walked: 0, 1. Community 0 adds 2 and 3 at 0.8 * 0.5^2.
        assert_eq!((h[&1].hops, h[&1].score), (Some(1), 0.4));
        assert_eq!((h[&3].hops, h[&3].score), (None, 0.2));
        assert!(h[&3].path.is_empty());
        assert!(!h.contains_key(&4), "drift left the seed's community");
        assert!(hits.iter().all(|x| x.community == Some(0)));
    }

    #[test]
    fn the_limit_keeps_the_highest_scores() {
        let (v, _) = graph();
        let c = RetrievalConfig {
            limit: 2,
            ..config(Strategy::Local)
        };
        let hits = retrieve(
            &v,
            &[Seed {
                index: 2,
                similarity: 0.8,
            }],
            None,
            &all,
            &c,
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].index, 2);
        assert_eq!(hits[1].score, 0.4);
        assert_eq!(hits[1].index, 1, "ties by index");
    }

    #[test]
    fn no_seeds_retrieve_nothing() {
        let (v, p) = graph();
        for s in [Strategy::Local, Strategy::Global, Strategy::Drift] {
            assert!(
                retrieve(&v, &[], Some(&p), &all, &config(s)).is_empty(),
                "{s:?}"
            );
        }
    }

    #[test]
    fn strategy_names_parse_any_case_and_refuse_others() {
        assert_eq!(Strategy::parse("Local"), Some(Strategy::Local));
        assert_eq!(Strategy::parse("GLOBAL"), Some(Strategy::Global));
        assert_eq!(Strategy::parse("drift"), Some(Strategy::Drift));
        assert_eq!(Strategy::parse("nearby"), None);
        assert!(!Strategy::Local.needs_communities());
        assert!(Strategy::Global.needs_communities() && Strategy::Drift.needs_communities());
    }

    #[test]
    #[should_panic(expected = "needs a partition")]
    fn global_without_a_partition_is_a_caller_bug() {
        let (v, _) = graph();
        retrieve(
            &v,
            &[Seed {
                index: 0,
                similarity: 1.0,
            }],
            None,
            &all,
            &config(Strategy::Global),
        );
    }
}
