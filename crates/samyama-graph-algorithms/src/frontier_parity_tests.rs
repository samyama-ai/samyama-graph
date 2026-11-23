//! The algorithms moved onto [`crate::frontier`] answer exactly as they did
//! before the move (ALGO-13, #1695).
//!
//! Each fingerprint below was recorded from the hand-written implementations
//! on `dev` before any of them was touched, then left alone while they were
//! ported. A port that changes one bit of one answer -- a float summed in a
//! different order, a different tie broken, a predecessor list in a different
//! order -- changes the fingerprint.
//!
//! A fingerprint is FNV-1a over the `Debug` text of the answer. `Debug` prints
//! an `f64` as the shortest text that reads back as the same bits, so equal
//! text is equal bits. Hash maps are sorted first: their iteration order is
//! not part of an answer.
//!
//! The graphs are chosen to reach every branch the walks have: parallel
//! relationships, self-loops, nodes with no relationships, several
//! components, a bipartite graph with and without an odd cycle, and a DAG.

use std::collections::HashMap;

use crate::common::{GraphView, NodeId};
use crate::negative_sampling::{negative_samples, NegativeStrategy};
use crate::{
    all_pairs_hops, all_shortest_paths, average_neighbour_degree, betweenness_centrality, bfs,
    bfs_all_shortest_paths, bipartite_sets, closeness_centrality, degree_assortativity, diameter,
    eccentricity, global_efficiency, harmonic_centrality, maximal_matching, radius,
    transitive_closure, wiener_index,
};

fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..n as NodeId).collect();
    let node_to_index: HashMap<NodeId, usize> = (0..n).map(|i| (i as NodeId, i)).collect();
    let (mut out, mut inc) = (vec![Vec::new(); n], vec![Vec::new(); n]);
    for &(a, b) in edges {
        out[a].push(b);
        inc[b].push(a);
    }
    GraphView::from_adjacency_list(n, index_to_node, node_to_index, out, inc, None)
}

/// Pseudo-random edges with repeats and self-loops, no RNG dependency.
fn random_edges(n: usize, m: usize, seed: u64) -> Vec<(usize, usize)> {
    let mut x = seed | 1;
    let mut next = move || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x as usize
    };
    (0..m).map(|_| (next() % n, next() % n)).collect()
}

/// The graphs every algorithm is fingerprinted on.
fn graphs() -> Vec<(&'static str, GraphView)> {
    // Two components joined by nothing, plus an isolated node (index 24).
    let mut split = random_edges(12, 30, 7);
    split.extend(
        random_edges(12, 30, 9)
            .into_iter()
            .map(|(a, b)| (a + 12, b + 12)),
    );
    // A 4x4 grid: bipartite. The same grid with one diagonal: not.
    let mut grid = Vec::new();
    for r in 0..4 {
        for c in 0..4 {
            let i = r * 4 + c;
            if c < 3 {
                grid.push((i, i + 1));
            }
            if r < 3 {
                grid.push((i, i + 4));
            }
        }
    }
    let mut odd = grid.clone();
    odd.push((0, 5));
    // A DAG with several equally short routes.
    let dag = vec![
        (0, 1),
        (0, 2),
        (1, 3),
        (2, 3),
        (3, 4),
        (3, 5),
        (4, 6),
        (5, 6),
        (1, 5),
        (0, 6),
    ];
    vec![
        ("random", view(30, &random_edges(30, 90, 3))),
        ("dense", view(15, &random_edges(15, 120, 5))),
        ("split", view(25, &split)),
        ("grid", view(16, &grid)),
        ("odd", view(16, &odd)),
        ("dag", view(7, &dag)),
        ("empty", view(0, &[])),
        ("single", view(1, &[(0, 0)])),
    ]
}

fn fnv(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ b as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Every answer of every algorithm on every graph, as one text per algorithm.
fn answers() -> Vec<(&'static str, String)> {
    let mut out: Vec<(&'static str, String)> = Vec::new();
    let mut add = |name: &'static str, text: String| match out.iter_mut().find(|(n, _)| *n == name)
    {
        Some((_, all)) => all.push_str(&text),
        None => out.push((name, text)),
    };
    for (g, v) in graphs() {
        let n = v.node_count;
        for b in [true, false] {
            add(
                "closeness",
                format!("{g}{b}{:?}", closeness_centrality(&v, b)),
            );
            add(
                "harmonic",
                format!("{g}{b}{:?}", harmonic_centrality(&v, b)),
            );
            add(
                "betweenness",
                format!("{g}{b}{:?}", betweenness_centrality(&v, b)),
            );
            add("eccentricity", format!("{g}{b}{:?}", eccentricity(&v, b)));
            add("diameter", format!("{g}{b}{:?}", diameter(&v, b)));
            add("radius", format!("{g}{b}{:?}", radius(&v, b)));
            add(
                "average_neighbour_degree",
                format!("{g}{b}{:?}", average_neighbour_degree(&v, b)),
            );
            add(
                "degree_assortativity",
                format!("{g}{b}{:?}", degree_assortativity(&v, b)),
            );
            add(
                "global_efficiency",
                format!("{g}{b}{:?}", global_efficiency(&v, b)),
            );
            add(
                "bipartite_sets",
                format!("{g}{b}{:?}", bipartite_sets(&v, b)),
            );
            add(
                "maximal_matching",
                format!("{g}{b}{:?}", maximal_matching(&v, b)),
            );
        }
        let mut hops: Vec<_> = all_pairs_hops(&v).into_iter().collect();
        hops.sort_unstable();
        add("all_pairs_hops", format!("{g}{hops:?}"));
        add("wiener_index", format!("{g}{:?}", wiener_index(&v)));
        add(
            "transitive_closure",
            format!("{g}{:?}", transitive_closure(&v)),
        );
        for s in 0..n {
            for t in 0..n {
                let path = bfs(&v, s as NodeId, t as NodeId).map(|p| (p.path, p.cost));
                add("bfs", format!("{g}{s}{t}{path:?}"));
                let all: Vec<_> = bfs_all_shortest_paths(&v, s as NodeId, t as NodeId)
                    .into_iter()
                    .map(|p| (p.path, p.cost))
                    .collect();
                add("bfs_all_shortest_paths", format!("{g}{s}{t}{all:?}"));
                add(
                    "all_shortest_paths",
                    format!("{g}{s}{t}{:?}", all_shortest_paths(&v, s, t, 5)),
                );
            }
        }
        // Ids that are not in the view.
        add("bfs", format!("{g}{:?}", bfs(&v, 999, 0).map(|p| p.path)));
        add(
            "bfs_all_shortest_paths",
            format!("{g}{:?}", bfs_all_shortest_paths(&v, 0, 999).len()),
        );
        add(
            "all_shortest_paths",
            format!("{g}{:?}", all_shortest_paths(&v, 0, 999, 5)),
        );
        for k in [2, 3] {
            add(
                "negative_samples_khop",
                format!(
                    "{g}{k}{:?}",
                    negative_samples(&v, NegativeStrategy::KHop(k), 40, 11)
                ),
            );
        }
    }
    out
}

/// Recorded on `dev` at 7268884a, before the port.
const BEFORE: &[(&str, u64)] = &[
    ("closeness", 0x062d0aaf8bd8bed4),
    ("harmonic", 0xaabbca3558106015),
    ("betweenness", 0xb5e1313587ea1c20),
    ("eccentricity", 0x51bd9be0071c0388),
    ("diameter", 0x36e8ffe6848d195e),
    ("radius", 0xb856d4e01a3fb09b),
    ("average_neighbour_degree", 0xb2b208a34e2c2537),
    ("degree_assortativity", 0x9ecbf8642880807b),
    ("global_efficiency", 0x8e5942f3b9a0777d),
    ("bipartite_sets", 0x54c6258c4acbea29),
    ("maximal_matching", 0x00ae4259cd91e76b),
    ("all_pairs_hops", 0xdcd3a8ac7e2b8086),
    ("wiener_index", 0xa2de2e2c182506f2),
    ("transitive_closure", 0x39b91ded590da43b),
    ("bfs", 0x6d7cd191de918dd3),
    ("bfs_all_shortest_paths", 0x4a231673be4ec4f4),
    ("all_shortest_paths", 0xcb1bb16eb8416b45),
    ("negative_samples_khop", 0xabbd7901dd945193),
];

#[test]
fn ported_algorithms_answer_bit_for_bit_as_before() {
    let now: Vec<(&str, u64)> = answers()
        .iter()
        .map(|(name, text)| (*name, fnv(text)))
        .collect();
    assert_eq!(
        now.len(),
        BEFORE.len(),
        "the list of fingerprinted algorithms changed"
    );
    let changed: Vec<String> = now
        .iter()
        .zip(BEFORE)
        .filter(|((a_name, a), (b_name, b))| a_name != b_name || a != b)
        .map(|((name, a), (_, b))| format!("{name}: 0x{b:016x} before, 0x{a:016x} now"))
        .collect();
    assert!(
        changed.is_empty(),
        "answers changed:\n{}",
        changed.join("\n")
    );
}
