//! Coverage tests for the crate's public algorithms, run against the crate
//! alone. Node ids are `10 * (index + 1)` throughout so that a result reporting
//! an index where an id belongs (or the reverse) cannot pass by accident.

use std::collections::HashMap;

use crate::centrality::*;
use crate::centrality_extra::*;
use crate::common::{GraphView, NodeId};
use crate::community::{strongly_connected_components, weakly_connected_components};
use crate::community_detect::*;
use crate::link_prediction::*;
use crate::metrics::*;
use crate::pathfinding::{bfs, bfs_all_shortest_paths, dijkstra};
use crate::pathfinding_extra::*;
use crate::traversal::*;
use crate::{
    cdlp, count_triangles, local_clustering_coefficient, local_clustering_coefficient_with,
    page_rank, CdlpConfig, DirectedLcc, PageRankConfig,
};

fn id(i: usize) -> NodeId {
    10 * (i as NodeId + 1)
}

/// A view over `n` nodes and directed `edges`, each stored once, optionally
/// weighted.
fn wview(n: usize, edges: &[(usize, usize, f64)], weighted: bool) -> GraphView {
    let mut out = vec![Vec::new(); n];
    let mut w = vec![Vec::new(); n];
    let mut inc = vec![Vec::new(); n];
    for &(a, b, wt) in edges {
        out[a].push(b);
        w[a].push(wt);
        inc[b].push(a);
    }
    let index_to_node: Vec<NodeId> = (0..n).map(id).collect();
    let node_to_index: HashMap<NodeId, usize> = index_to_node
        .iter()
        .enumerate()
        .map(|(i, &x)| (x, i))
        .collect();
    GraphView::from_adjacency_list(
        n,
        index_to_node,
        node_to_index,
        out,
        inc,
        if weighted { Some(w) } else { None },
    )
}

fn view(n: usize, edges: &[(usize, usize)]) -> GraphView {
    let e: Vec<(usize, usize, f64)> = edges.iter().map(|&(a, b)| (a, b, 1.0)).collect();
    wview(n, &e, false)
}

/// `copies` disjoint copies of a small graph, for the parallel (n >= 1000)
/// code paths.
fn copies(copies: usize, n: usize, edges: &[(usize, usize)]) -> GraphView {
    let mut all = Vec::new();
    for c in 0..copies {
        for &(a, b) in edges {
            all.push((a + c * n, b + c * n));
        }
    }
    view(copies * n, &all)
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() < 1e-9
}

const PATH3: &[(usize, usize)] = &[(0, 1), (1, 2)];
const TRIANGLE: &[(usize, usize)] = &[(0, 1), (1, 2), (2, 0)];
const DIAMOND: &[(usize, usize)] = &[(0, 1), (0, 2), (1, 3), (2, 3)];

// ───────────────────────────────────────────────────────────── centrality

#[test]
fn degree_centrality_counts_in_plus_out_over_n_minus_one() {
    let v = view(3, PATH3);
    assert_eq!(degree_centrality(&v, true), vec![0.5, 1.0, 0.5]);
    // The flag is not read.
    assert_eq!(degree_centrality(&v, false), vec![0.5, 1.0, 0.5]);
    assert_eq!(degree_centrality(&view(1, &[]), true), vec![0.0]);
    assert!(degree_centrality(&view(0, &[]), true).is_empty());
}

#[test]
fn closeness_centrality_on_a_path_both_readings() {
    let v = view(3, PATH3);
    let und = closeness_centrality(&v, true);
    assert!(
        close(und[0], 2.0 / 3.0) && close(und[1], 1.0) && close(und[2], 2.0 / 3.0),
        "{und:?}"
    );
    // Directed: distance *into* the node, scaled by the reachable fraction.
    let dir = closeness_centrality(&v, false);
    assert!(
        close(dir[0], 0.0) && close(dir[1], 0.5) && close(dir[2], 2.0 / 3.0),
        "{dir:?}"
    );
    assert_eq!(closeness_centrality(&view(1, &[]), true), vec![0.0]);
}

#[test]
fn betweenness_centrality_matches_networkx_normalisation() {
    let v = view(3, PATH3);
    let und = betweenness_centrality(&v, true);
    assert!(
        close(und[1], 1.0) && close(und[0], 0.0) && close(und[2], 0.0),
        "{und:?}"
    );
    let dir = betweenness_centrality(&v, false);
    assert!(close(dir[1], 0.5), "{dir:?}");
    // Two equal routes 0 -> 3 split the dependency between 1 and 2.
    let d = betweenness_centrality(&view(4, DIAMOND), false);
    assert!(
        close(d[1], 0.5 / 6.0) && close(d[2], 0.5 / 6.0) && close(d[0], 0.0),
        "{d:?}"
    );
    assert_eq!(
        betweenness_centrality(&view(2, &[(0, 1)]), true),
        vec![0.0, 0.0]
    );
}

#[test]
fn ranked_sorts_by_score_then_node_id() {
    let v = view(3, &[]);
    let r = ranked(&v, &vec![0.5, 0.9, 0.5]);
    assert_eq!(r, vec![(id(1), 0.9), (id(0), 0.5), (id(2), 0.5)]);
}

#[test]
fn harmonic_centrality_sums_reciprocal_distances() {
    let v = view(3, PATH3);
    assert_eq!(harmonic_centrality(&v, true), vec![1.5, 2.0, 1.5]);
    assert_eq!(harmonic_centrality(&v, false), vec![0.0, 1.0, 1.5]);
}

#[test]
fn core_number_peels_a_triangle_with_a_pendant_and_ignores_self_loops() {
    let v = view(4, &[(0, 1), (1, 2), (2, 0), (3, 0), (1, 1)]);
    assert_eq!(core_number(&v, true), vec![2, 2, 2, 1]);
    assert!(core_number(&view(0, &[]), false).is_empty());
}

#[test]
fn eigenvector_centrality_converges_uniformly_on_a_triangle() {
    let v = view(3, TRIANGLE);
    let x = eigenvector_centrality(&v, true, 100, 1e-9).expect("converges");
    for s in &x {
        assert!(close(*s, 1.0 / 3f64.sqrt()), "{x:?}");
    }
    assert_eq!(
        eigenvector_centrality(&view(0, &[]), true, 10, 1e-6),
        Some(vec![])
    );
}

#[test]
fn eigenvector_centrality_refuses_when_there_is_no_principal_vector() {
    // No edges: the first iterate has zero norm.
    assert_eq!(eigenvector_centrality(&view(2, &[]), true, 10, 1e-6), None);
    // A directed path drains to zero after one step.
    assert_eq!(
        eigenvector_centrality(&view(2, &[(0, 1)]), false, 10, 1e-6),
        None
    );
    // No iterations allowed: nothing converged.
    assert_eq!(
        eigenvector_centrality(&view(3, TRIANGLE), true, 0, 1e-6),
        None
    );
}

// ─────────────────────────────────────────────────────── centrality extra

#[test]
fn katz_with_zero_beta_converges_to_the_zero_vector() {
    let x = katz_centrality(&view(3, TRIANGLE), 0.1, 0.0, 10, 1e-9).unwrap();
    assert_eq!(x, vec![0.0, 0.0, 0.0]);
    assert_eq!(
        katz_centrality(&view(0, &[]), 0.1, 1.0, 10, 1e-9),
        Some(vec![])
    );
}

#[test]
fn hits_on_an_edgeless_graph_is_all_zero_and_empty_graph_is_empty() {
    let (h, a) = hits(&view(3, &[]), 10, 1e-9).unwrap();
    assert_eq!(h, vec![0.0; 3]);
    assert_eq!(a, vec![0.0; 3]);
    assert_eq!(hits(&view(0, &[]), 10, 1e-9), Some((vec![], vec![])));
    // No iterations allowed: no converged answer.
    assert_eq!(hits(&view(3, TRIANGLE), 0, 1e-9), None);
}

#[test]
fn personalised_page_rank_and_vote_rank_on_empty_inputs() {
    assert!(personalised_page_rank(&view(0, &[]), &[], 0.85, 10, 1e-9).is_empty());
    assert!(vote_rank(&view(0, &[]), 3).is_empty());
    assert!(vote_rank(&view(3, TRIANGLE), 0).is_empty());
    assert!(vote_rank(&view(3, &[]), 2).is_empty());
}

#[test]
fn vote_rank_stops_when_no_candidate_has_votes_left() {
    // Only node 1 is pointed at; after electing it nobody has a vote.
    let v = view(3, &[(0, 1), (2, 1)]);
    assert_eq!(vote_rank(&v, 3), vec![1]);
}

#[test]
fn index_of_maps_ids_to_dense_indices() {
    let v = view(3, &[]);
    assert_eq!(index_of(&v, id(2)), Some(2));
    assert_eq!(index_of(&v, 7), None);
}

// ─────────────────────────────────────────────────────── community detect

/// Two triangles {0,1,2} and {3,4,5} joined by the edge 2-3.
fn two_triangles() -> GraphView {
    view(6, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3), (2, 3)])
}

#[test]
fn modularity_of_two_joined_triangles() {
    let v = two_triangles();
    let q = modularity(&v, &[0, 0, 0, 1, 1, 1]).unwrap();
    // 2m = 14, inside = 12, each side has degree total 7.
    assert!(close(q, 12.0 / 14.0 - 0.5), "{q}");
    // Everything in one community: Q = 1 - 1 = 0.
    assert!(close(modularity(&v, &[0; 6]).unwrap(), 0.0));
    assert_eq!(modularity(&v, &[0, 1]), None);
    assert_eq!(modularity(&view(3, &[]), &[0, 1, 2]), None);
}

#[test]
fn modularity_uses_the_heavier_of_parallel_edges_and_skips_self_loops() {
    let a = wview(2, &[(0, 1, 2.0), (1, 0, 5.0), (0, 0, 9.0)], true);
    let b = wview(2, &[(0, 1, 5.0)], true);
    assert_eq!(modularity(&a, &[0, 1]), modularity(&b, &[0, 1]));
    assert!(close(modularity(&b, &[0, 1]).unwrap(), -0.5));
}

#[test]
fn louvain_separates_two_joined_triangles() {
    let v = two_triangles();
    assert_eq!(louvain(&v, 10), vec![0, 0, 0, 1, 1, 1]);
    // Zero passes leaves every node alone.
    assert_eq!(louvain(&v, 0), vec![0, 1, 2, 3, 4, 5]);
    // No edges: nothing to merge.
    assert_eq!(louvain(&view(3, &[]), 5), vec![0, 1, 2]);
    // One triangle collapses to a single community and stops.
    assert_eq!(louvain(&view(3, TRIANGLE), 5), vec![0, 0, 0]);
}

#[test]
fn with_ids_pairs_communities_with_node_ids() {
    let v = view(3, &[]);
    assert_eq!(
        with_ids(&v, &[1, 0, 1]),
        vec![(id(0), 1), (id(1), 0), (id(2), 1)]
    );
}

// ─────────────────────────────────────────────────────────────── WCC / SCC

#[test]
fn weakly_connected_components_ignore_direction() {
    let v = view(5, &[(0, 1), (2, 1), (3, 4)]);
    let r = weakly_connected_components(&v);
    assert_eq!(r.components.len(), 2);
    let c = |i| r.node_component[&id(i)];
    assert_eq!(c(0), c(1));
    assert_eq!(c(1), c(2));
    assert_eq!(c(3), c(4));
    assert_ne!(c(0), c(3));
    let mut sizes: Vec<usize> = r.components.values().map(|v| v.len()).collect();
    sizes.sort();
    assert_eq!(sizes, vec![2, 3]);
}

#[test]
fn weakly_connected_components_union_by_rank_on_a_long_chain() {
    // Enough unions of equal- and unequal-rank trees to take every branch.
    let edges: Vec<(usize, usize)> = vec![(0, 1), (2, 3), (0, 2), (4, 0), (5, 6), (6, 4)];
    let r = weakly_connected_components(&view(7, &edges));
    assert_eq!(r.components.len(), 1);
    assert_eq!(r.components.values().next().unwrap().len(), 7);
}

#[test]
fn strongly_connected_components_split_a_cycle_from_its_tail() {
    let v = view(4, &[(0, 1), (1, 2), (2, 0), (2, 3)]);
    let r = strongly_connected_components(&v);
    assert_eq!(r.components.len(), 2);
    let c = |i| r.node_component[&id(i)];
    assert_eq!(c(0), c(1));
    assert_eq!(c(1), c(2));
    assert_ne!(c(2), c(3));
}

// ──────────────────────────────────────────────────────── link prediction

/// A square 0-1-3-2-0: the diagonals are the only unconnected pairs.
const SQUARE: &[(usize, usize)] = &[(0, 1), (0, 2), (1, 3), (2, 3)];

#[test]
fn predict_links_scores_the_diagonals_of_a_square() {
    let v = view(4, SQUARE);
    let cn = predict_links(&v, LinkScore::CommonNeighbours, 10);
    assert_eq!(
        cn,
        vec![
            PairScore {
                a: id(0),
                b: id(3),
                score: 2.0
            },
            PairScore {
                a: id(1),
                b: id(2),
                score: 2.0
            },
        ]
    );
    let j = predict_links(&v, LinkScore::Jaccard, 10);
    assert!(j.iter().all(|p| close(p.score, 1.0)), "{j:?}");
    let aa = predict_links(&v, LinkScore::AdamicAdar, 10);
    assert!(aa.iter().all(|p| close(p.score, 2.0 / 2f64.ln())), "{aa:?}");
    // The limit truncates after sorting.
    assert_eq!(predict_links(&v, LinkScore::CommonNeighbours, 1).len(), 1);
}

#[test]
fn score_one_handles_isolated_pairs_degree_one_neighbours_and_bad_indices() {
    let v = view(4, &[(0, 1)]);
    // Two isolated nodes: empty union.
    assert_eq!(score_one(&v, LinkScore::Jaccard, 2, 3), Some(0.0));
    // A node with itself shares its degree-1 neighbour, which adds nothing to
    // Adamic-Adar (log 1 = 0).
    assert_eq!(score_one(&v, LinkScore::AdamicAdar, 0, 0), Some(0.0));
    assert_eq!(score_one(&v, LinkScore::CommonNeighbours, 0, 9), None);
    assert_eq!(score_one(&v, LinkScore::CommonNeighbours, 9, 0), None);
}

// ─────────────────────────────────────────────────────────────── metrics

#[test]
fn eccentricity_diameter_and_radius_on_a_path() {
    let v = view(3, PATH3);
    assert_eq!(eccentricity(&v, true), vec![Some(2), Some(1), Some(2)]);
    assert_eq!(diameter(&v, true), Some(2));
    assert_eq!(radius(&v, true), Some(1));
    // Directed: node 1 cannot reach node 0, so nothing is finite.
    assert_eq!(eccentricity(&v, false), vec![Some(2), None, None]);
    assert_eq!(diameter(&v, false), None);
    assert_eq!(radius(&v, false), None);
    assert_eq!(diameter(&view(0, &[]), true), None);
    assert_eq!(radius(&view(0, &[]), true), None);
}

#[test]
fn average_neighbour_degree_on_a_path_with_an_isolated_node() {
    let v = view(4, PATH3);
    assert_eq!(average_neighbour_degree(&v, true), vec![2.0, 1.0, 2.0, 0.0]);
}

#[test]
fn degree_assortativity_of_a_star_is_minus_one() {
    let star = view(4, &[(0, 1), (0, 2), (0, 3)]);
    assert!(close(degree_assortativity(&star, true).unwrap(), -1.0));
    // Directed reading: out-neighbour sets only, each edge once.
    assert!(close(degree_assortativity(&star, false).unwrap(), -1.0));
    // Every degree equal: the variance is zero.
    assert_eq!(degree_assortativity(&view(3, TRIANGLE), true), None);
    assert_eq!(degree_assortativity(&view(3, &[]), true), None);
}

#[test]
fn ranked_opt_puts_larger_values_first_and_unknowns_last() {
    let v = view(3, &[]);
    let r = ranked_opt(&v, &[Some(1), None, Some(3)]);
    assert_eq!(r, vec![(id(2), Some(3)), (id(0), Some(1)), (id(1), None)]);
}

// ─────────────────────────────────────────────────── pathfinding (extra)

#[test]
fn all_shortest_paths_enumerates_both_routes_of_a_diamond() {
    let v = view(4, DIAMOND);
    assert_eq!(
        all_shortest_paths(&v, 0, 3, 10),
        vec![vec![id(0), id(1), id(3)], vec![id(0), id(2), id(3)]]
    );
    assert_eq!(all_shortest_paths(&v, 0, 3, 1).len(), 1);
    assert_eq!(all_shortest_paths(&v, 0, 0, 10), vec![vec![id(0)]]);
    assert!(all_shortest_paths(&v, 3, 0, 10).is_empty());
    assert!(all_shortest_paths(&v, 0, 9, 10).is_empty());
    assert!(all_shortest_paths(&v, 9, 0, 10).is_empty());
}

fn weighted_routes() -> GraphView {
    // 0 -> 3 three ways: via 1 (cost 2), via 2 (cost 4), direct (cost 5).
    wview(
        4,
        &[
            (0, 1, 1.0),
            (1, 3, 1.0),
            (0, 2, 2.0),
            (2, 3, 2.0),
            (0, 3, 5.0),
        ],
        true,
    )
}

#[test]
fn a_star_finds_the_cheapest_route_and_refuses_negative_weights() {
    let v = weighted_routes();
    let (path, cost) = a_star(&v, 0, 3, &[0.0; 4]).unwrap().unwrap();
    assert_eq!(path, vec![id(0), id(1), id(3)]);
    assert_eq!(cost, 2.0);
    // Wrong-length heuristic, bad index, unreachable target: no answer.
    assert_eq!(a_star(&v, 0, 3, &[0.0; 2]).unwrap(), None);
    assert_eq!(a_star(&v, 0, 9, &[0.0; 4]).unwrap(), None);
    assert_eq!(a_star(&v, 3, 0, &[0.0; 4]).unwrap(), None);
    let neg = wview(2, &[(0, 1, -1.0)], true);
    assert_eq!(a_star(&neg, 0, 1, &[0.0; 2]).unwrap_err().weight, -1.0);
}

#[test]
fn yens_k_shortest_lists_routes_in_cost_order() {
    let v = weighted_routes();
    let r = yens_k_shortest(&v, 0, 3, 10).unwrap();
    assert_eq!(
        r,
        vec![
            (vec![id(0), id(1), id(3)], 2.0),
            (vec![id(0), id(2), id(3)], 4.0),
            (vec![id(0), id(3)], 5.0),
        ]
    );
    assert_eq!(yens_k_shortest(&v, 0, 3, 2).unwrap().len(), 2);
    assert!(yens_k_shortest(&v, 0, 3, 0).unwrap().is_empty());
    assert!(yens_k_shortest(&v, 0, 9, 3).unwrap().is_empty());
    assert!(yens_k_shortest(&v, 3, 0, 3).unwrap().is_empty());
    let neg = wview(2, &[(0, 1, -2.0)], true);
    assert_eq!(yens_k_shortest(&neg, 0, 1, 1).unwrap_err().weight, -2.0);
}

#[test]
fn yens_on_an_unweighted_graph_counts_hops() {
    let v = view(4, DIAMOND);
    let r = yens_k_shortest(&v, 0, 3, 5).unwrap();
    assert_eq!(r.len(), 2);
    assert!(r.iter().all(|(p, c)| p.len() == 3 && *c == 2.0), "{r:?}");
}

#[test]
fn random_walk_follows_edges_and_stops_at_a_dead_end() {
    let v = view(3, TRIANGLE);
    let w = random_walk(&v, 0, 12, 42);
    assert_eq!(w.len(), 13);
    assert_eq!(w[0], id(0));
    for pair in w.windows(2) {
        let (a, b) = (v.node_to_index[&pair[0]], v.node_to_index[&pair[1]]);
        assert!(v.successors(a).contains(&b), "{pair:?} is not an edge");
    }
    // Same seed, same walk.
    assert_eq!(w, random_walk(&v, 0, 12, 42));
    assert_eq!(
        random_walk(&view(3, PATH3), 0, 10, 1),
        vec![id(0), id(1), id(2)]
    );
    assert!(random_walk(&v, 7, 3, 1).is_empty());
}

#[test]
fn article_rank_matches_one_hand_computed_step() {
    let v = view(2, &[(0, 1)]);
    let r = article_rank(&v, 0.85, 1);
    // avg out-degree 0.5; node 0 shares 0.85 * 0.5 / (1 + 0.5).
    assert!(close(r[0], 0.075), "{r:?}");
    assert!(close(r[1], 0.075 + 0.85 * 0.5 / 1.5), "{r:?}");
    assert!(article_rank(&view(0, &[]), 0.85, 5).is_empty());
}

// ─────────────────────────────────────────────────────── pathfinding

#[test]
fn bfs_returns_hop_path_and_none_for_unknown_or_unreachable() {
    let v = view(3, PATH3);
    let p = bfs(&v, id(0), id(2)).unwrap();
    assert_eq!(p.path, vec![id(0), id(1), id(2)]);
    assert_eq!(p.cost, 2.0);
    assert!(bfs(&v, id(2), id(0)).is_none());
    assert!(bfs(&v, 1, id(0)).is_none());
}

#[test]
fn dijkstra_skips_stale_heap_entries_and_reports_unreachable() {
    // 1 is first queued at cost 5, then improved to 2 through 2; the stale
    // entry is popped later and skipped.
    let v = wview(
        5,
        &[(0, 1, 5.0), (0, 2, 1.0), (2, 1, 1.0), (1, 3, 10.0)],
        true,
    );
    let p = dijkstra(&v, id(0), id(3)).unwrap().unwrap();
    assert_eq!(p.path, vec![id(0), id(2), id(1), id(3)]);
    assert_eq!(p.cost, 12.0);
    assert!(dijkstra(&v, id(0), id(4)).unwrap().is_none());
}

#[test]
fn dijkstra_on_an_unweighted_view_counts_hops_and_refuses_negative_weights() {
    let v = view(3, PATH3);
    assert_eq!(dijkstra(&v, id(0), id(2)).unwrap().unwrap().cost, 2.0);
    let neg = wview(2, &[(0, 1, -3.5)], true);
    let err = dijkstra(&neg, id(0), id(1)).unwrap_err();
    assert_eq!(err.weight, -3.5);
    let msg = err.to_string();
    assert!(msg.contains("-3.5") && msg.contains("negative"), "{msg}");
}

#[test]
fn bfs_all_shortest_paths_edge_cases() {
    let v = view(4, DIAMOND);
    assert!(bfs_all_shortest_paths(&v, 1, id(3)).is_empty());
    assert!(bfs_all_shortest_paths(&v, id(0), 1).is_empty());
    let same = bfs_all_shortest_paths(&v, id(1), id(1));
    assert_eq!(same.len(), 1);
    assert_eq!(same[0].path, vec![id(1)]);
    assert_eq!(same[0].cost, 0.0);
    let both = bfs_all_shortest_paths(&v, id(0), id(3));
    assert_eq!(both.len(), 2);
}

// ─────────────────────────────────────────────────────────── traversal

#[test]
fn topological_sort_orders_a_dag_and_names_the_cyclic_rest() {
    match topological_sort(&view(4, DIAMOND)) {
        TopoResult::Order(o) => {
            assert_eq!(o.len(), 4);
            let pos = |x| o.iter().position(|&y| y == id(x)).unwrap();
            assert!(pos(0) < pos(1) && pos(0) < pos(2) && pos(1) < pos(3) && pos(2) < pos(3));
        }
        other => panic!("expected an order, got {other:?}"),
    }
    // 3 -> 0 closes a cycle; 4 hangs off it and cannot be placed either.
    let v = view(5, &[(0, 1), (1, 2), (2, 0), (2, 4), (3, 3)]);
    match topological_sort(&v) {
        TopoResult::Cyclic(rest) => assert_eq!(rest, vec![id(0), id(1), id(2), id(3), id(4)]),
        other => panic!("expected a cycle, got {other:?}"),
    }
}

#[test]
fn find_cycle_returns_a_closed_walk_or_none() {
    assert_eq!(find_cycle(&view(4, DIAMOND)), None);
    let v = view(4, &[(3, 0), (0, 1), (1, 2), (2, 0)]);
    let c = find_cycle(&v).expect("a cycle");
    assert_eq!(c.len(), 3);
    for i in 0..c.len() {
        let (a, b) = (
            v.node_to_index[&c[i]],
            v.node_to_index[&c[(i + 1) % c.len()]],
        );
        assert!(v.successors(a).contains(&b), "{c:?} is not a cycle");
    }
}

#[test]
fn bridges_and_articulation_points_of_a_path_and_a_triangle() {
    let p = view(3, PATH3);
    assert_eq!(bridges(&p), vec![(id(0), id(1)), (id(1), id(2))]);
    assert_eq!(articulation_points(&p), vec![id(1)]);
    let t = view(3, TRIANGLE);
    assert!(bridges(&t).is_empty());
    assert!(articulation_points(&t).is_empty());
}

#[test]
fn parallel_edges_are_not_bridges_and_self_loops_are_ignored() {
    let v = view(3, &[(0, 1), (0, 1), (1, 2), (2, 2)]);
    assert_eq!(bridges(&v), vec![(id(1), id(2))]);
    assert_eq!(articulation_points(&v), vec![id(1)]);
}

#[test]
fn a_root_with_two_subtrees_is_an_articulation_point() {
    let v = view(3, &[(0, 1), (0, 2)]);
    assert_eq!(articulation_points(&v), vec![id(0)]);
}

// ───────────────────────────────────── parallel paths (n >= 1000 nodes)

#[test]
fn large_graphs_agree_with_their_components_on_the_parallel_paths() {
    // 334 disjoint triangles (1002 nodes) plus two isolated nodes.
    let k = 334;
    let big = {
        let base = copies(k, 3, TRIANGLE);
        let mut out: Vec<Vec<usize>> = (0..base.node_count)
            .map(|i| base.successors(i).to_vec())
            .collect();
        let mut inc: Vec<Vec<usize>> = (0..base.node_count)
            .map(|i| base.predecessors(i).to_vec())
            .collect();
        out.push(vec![]);
        out.push(vec![]);
        inc.push(vec![]);
        inc.push(vec![]);
        let n = base.node_count + 2;
        let ids: Vec<NodeId> = (0..n).map(id).collect();
        let map = ids.iter().enumerate().map(|(i, &x)| (x, i)).collect();
        GraphView::from_adjacency_list(n, ids, map, out, inc, None)
    };
    let n = big.node_count;
    assert!(n >= 1000);

    assert_eq!(count_triangles(&big), k);

    let pr = page_rank(&big, PageRankConfig::default());
    let total: f64 = pr.values().sum();
    assert!((total - 1.0).abs() < 1e-6, "PageRank mass {total}");
    // Every triangle node carries the same score; the isolated nodes carry
    // less (they receive only teleport and redistribution).
    let tri = pr[&id(0)];
    for i in 0..3 * k {
        assert!((pr[&id(i)] - tri).abs() < 1e-12);
    }
    assert!(pr[&id(n - 1)] < tri);

    let labels = cdlp(&big, &CdlpConfig::default()).labels;
    for c in 0..k {
        // Each triangle adopts the smallest id in it.
        for j in 0..3 {
            assert_eq!(labels[&id(3 * c + j)], id(3 * c), "triangle {c}");
        }
    }
    assert_eq!(labels[&id(n - 1)], id(n - 1));

    let lcc = local_clustering_coefficient(&big);
    for i in 0..3 * k {
        assert_eq!(lcc.coefficients[&id(i)], 1.0);
    }
    assert_eq!(lcc.coefficients[&id(n - 1)], 0.0);

    // Directed definitions on reciprocal triangles: compare the parallel
    // answer with the serial answer on a single copy.
    let recip: &[(usize, usize)] = &[(0, 1), (1, 0), (1, 2), (2, 1), (2, 0), (0, 2), (0, 3)];
    let one = view(4, recip);
    let many = copies(250, 4, recip);
    assert!(many.node_count >= 1000);
    for def in [DirectedLcc::Fagiolo, DirectedLcc::Ldbc] {
        let small = local_clustering_coefficient_with(&one, true, def);
        let large = local_clustering_coefficient_with(&many, true, def);
        for c in 0..250 {
            for j in 0..4 {
                assert!(
                    (large.coefficients[&id(4 * c + j)] - small.coefficients[&id(j)]).abs() < 1e-12,
                    "{def:?} copy {c} node {j}"
                );
            }
        }
        assert!((large.average - small.average).abs() < 1e-12);
    }
}

// ───────────────────────────────────────────── more edge cases, by module

#[test]
fn yens_breaks_cost_ties_by_path_order() {
    // Two second-best routes of cost 3: 0-1-2-4 and 0-2-4.
    let v = wview(
        5,
        &[
            (0, 1, 1.0),
            (1, 4, 1.0),
            (1, 2, 1.0),
            (0, 2, 2.0),
            (2, 4, 1.0),
        ],
        true,
    );
    let r = yens_k_shortest(&v, 0, 4, 3).unwrap();
    assert_eq!(
        r,
        vec![
            (vec![id(0), id(1), id(4)], 2.0),
            (vec![id(0), id(1), id(2), id(4)], 3.0),
            (vec![id(0), id(2), id(4)], 3.0),
        ]
    );
}

#[test]
fn weakly_connected_components_of_a_cycle_is_one_component() {
    // The closing edge unions two nodes already in the same set.
    let r = weakly_connected_components(&view(3, TRIANGLE));
    assert_eq!(r.components.len(), 1);
}

#[test]
fn prim_mst_of_an_empty_view_has_no_components() {
    let r = crate::prim_mst(&view(0, &[]));
    assert_eq!(r.components, 0);
    assert!(r.edges.is_empty());
    assert_eq!(r.total_weight, 0.0);
}

#[test]
fn bellman_ford_from_a_missing_source_knows_no_distance() {
    assert_eq!(
        crate::bellman_ford(&view(2, &[(0, 1)]), 5),
        Some(vec![None, None])
    );
}

#[test]
fn wiener_index_of_fewer_than_two_nodes_is_zero() {
    assert_eq!(crate::wiener_index(&view(1, &[])), Some(0.0));
    assert_eq!(crate::wiener_index(&view(0, &[])), Some(0.0));
}

#[test]
fn similarity_measures_on_a_square() {
    use crate::similarity_extra::*;
    let v = view(5, SQUARE);
    // Opposite corners share both neighbours.
    assert!(close(cosine_similarity(&v, 0, 3).unwrap(), 1.0));
    assert!(close(cosine_similarity(&v, 0, 1).unwrap(), 0.0));
    // Node 4 is isolated.
    assert_eq!(cosine_similarity(&v, 0, 4), None);
    assert_eq!(cosine_similarity(&v, 0, 9), None);
    assert_eq!(overlap_coefficient(&v, 9, 0), None);
    assert_eq!(effective_size(&v, 9), None);
    // The isolated node appears in no row of the similarity graph.
    let sim = node_similarity(&v, 5, 0.0);
    assert!(sim.iter().all(|&(a, b, _)| a != 4 && b != 4));
    assert!(sim.contains(&(0, 3, 1.0)));
}

#[test]
fn reciprocity_ignores_self_loops() {
    use crate::similarity_extra::reciprocity;
    let v = view(2, &[(0, 1), (1, 0), (0, 0)]);
    assert_eq!(reciprocity(&v), Some(1.0));
    assert_eq!(reciprocity(&view(1, &[(0, 0)])), None);
}

#[test]
fn structure_measures_at_their_boundaries() {
    use crate::structure_extra::*;
    let t = view(4, TRIANGLE);
    // k < 3 is not a truss: every node with an edge.
    assert_eq!(k_truss(&t, 2, true), vec![0, 1, 2]);
    assert_eq!(global_efficiency(&view(1, &[]), true), None);
    assert_eq!(square_clustering(&t, 9, true), None);
    // Node 3 has no neighbours at all.
    assert_eq!(square_clustering(&t, 3, true), None);
    // A star's centre: its leaves share nothing but the centre.
    let star = view(3, &[(0, 1), (0, 2)]);
    assert_eq!(square_clustering(&star, 0, true), None);
    // Every triangle node has degree 2 > 1: a complete club.
    assert_eq!(rich_club_coefficient(&t, 1, true), Some(1.0));
    assert_eq!(rich_club_coefficient(&t, 2, true), None);
    assert_eq!(undirected_degree(&t, 0, true), 2);
    assert_eq!(undirected_degree(&t, 0, false), 1);
    assert_eq!(undirected_degree(&t, 3, true), 0);
}

#[test]
fn temporal_errors_render_readable_messages() {
    use crate::TemporalError::*;
    let cases = [
        (
            Misaligned { edges: 3, times: 2 },
            "the view has 3 and 2 times were given",
        ),
        (NoSuchNode(7), "node index 7 is not in this graph"),
        (
            NoSuchEdge { from: 1, to: 2 },
            "no edge 1 -> 2 in this graph",
        ),
        (
            UntimedEdge { from: 0, to: 1 },
            "edge 0 -> 1 was given no time",
        ),
        (
            TooManyTimes {
                from: 0,
                to: 1,
                have: 1,
            },
            "more times given for 0 -> 1 than the 1",
        ),
    ];
    for (err, needle) in cases {
        let msg = err.to_string();
        assert!(msg.contains(needle), "{msg:?} lacks {needle:?}");
    }
}

#[test]
fn temporal_edges_from_pairs_rejects_an_unknown_target() {
    let v = view(2, &[(0, 1)]);
    let err = crate::TemporalEdges::from_pairs(&v, [(0, 5, 1)]).unwrap_err();
    assert_eq!(err, crate::TemporalError::NoSuchNode(5));
    let ok = crate::TemporalEdges::new(&v, vec![3]).unwrap();
    assert!(!ok.is_empty());
    assert_eq!(ok.len(), 1);
    let none = crate::TemporalEdges::new(&view(2, &[]), vec![]).unwrap();
    assert!(none.is_empty());
}

#[test]
fn arrival_times_report_reachability_and_counts() {
    // 0 -(5)-> 1 -(3)-> 2: the second edge fires before we arrive at 1.
    let v = view(4, &[(0, 1), (1, 2), (0, 3)]);
    let t = crate::TemporalEdges::new(&v, vec![5, 7, 3]).unwrap();
    // Slot order follows out_targets: (0,1)=5, (0,3)=7, (1,2)=3.
    let a = crate::earliest_arrival(&v, &t, &[0], 0).unwrap();
    assert!(a.reaches(0) && a.reaches(1) && a.reaches(3));
    assert!(!a.reaches(2));
    assert!(!a.reaches(99));
    assert_eq!(a.reached_count(&[0]), 2);
}

#[test]
fn symptom_explanation_takes_the_later_of_two_routes_and_skips_the_stale_one() {
    // 0 -> 2 directly at 50, or 0 -> 1 at 90 then 1 -> 2 at 95. With 2 seen
    // at 100 the latest 0 could have started is 90, found after 50 was
    // already queued.
    let v = view(3, &[(0, 2), (0, 1), (1, 2)]);
    let t = crate::TemporalEdges::from_pairs(&v, [(0, 2, 50), (0, 1, 90), (1, 2, 95)]).unwrap();
    let ex = crate::symptom_explanation(&v, &t, &[(2, 100)]).unwrap();
    let u = ex
        .iter()
        .find(|e| e.node == id(0))
        .expect("0 explains the symptom");
    assert_eq!(u.latest_onset, 90);
    let path = u.supporting_path.as_ref().unwrap();
    assert_eq!(path.nodes, vec![id(0), id(1), id(2)]);
    assert_eq!(path.edge_times, vec![90, 95]);
}

#[test]
fn fastrp_and_node2vec_on_empty_and_isolated_inputs() {
    use crate::{fastrp, node2vec, FastRpConfig, Node2VecConfig};
    assert!(fastrp(&view(0, &[]), &FastRpConfig::default()).is_empty());
    assert!(node2vec(&view(0, &[]), &Node2VecConfig::default()).is_empty());

    // Node 2 has no neighbours: nothing propagates to it, and normalising a
    // zero row leaves it zero rather than NaN.
    let v = view(3, &[(0, 1)]);
    let cfg = FastRpConfig {
        dimension: 8,
        ..FastRpConfig::default()
    };
    let e = fastrp(&v, &cfg);
    assert_eq!(e.len(), 3);
    assert!(e[2].iter().all(|&x| x == 0.0), "{:?}", e[2]);
    let norm: f64 = e[0].iter().map(|x| x * x).sum::<f64>().sqrt();
    assert!(close(norm, 1.0));

    let n2v = Node2VecConfig {
        dimension: 4,
        walk_length: 5,
        walks_per_node: 2,
        ..Node2VecConfig::default()
    };
    let emb = node2vec(&v, &n2v);
    assert_eq!(emb.len(), 3);
    assert!(emb
        .iter()
        .all(|r| r.len() == 4 && r.iter().all(|x| x.is_finite())));
}

#[test]
fn node2vec_treats_non_positive_bias_factors_as_one() {
    use crate::{node2vec, Node2VecConfig};
    let v = view(5, &[(0, 1), (1, 2), (2, 3), (3, 4), (4, 0), (0, 2)]);
    let base = Node2VecConfig {
        dimension: 6,
        walk_length: 6,
        walks_per_node: 3,
        ..Node2VecConfig::default()
    };
    let unit = node2vec(&v, &base);
    let zeroed = node2vec(
        &v,
        &Node2VecConfig {
            return_factor: 0.0,
            in_out_factor: -2.0,
            ..base
        },
    );
    assert_eq!(unit, zeroed);
}

#[test]
fn pca_default_solver_is_auto() {
    assert!(matches!(
        crate::PcaSolver::default(),
        crate::PcaSolver::Auto
    ));
}

#[test]
fn pca_transform_applies_scaling_and_skips_constant_columns() {
    use crate::{pca, PcaConfig};
    // Column 0 varies widely, column 1 a little, column 2 not at all.
    let data: Vec<Vec<f64>> = (0..8)
        .map(|i| vec![10.0 * i as f64, (i % 3) as f64, 4.0])
        .collect();
    let r = pca(
        &data,
        PcaConfig {
            n_components: 2,
            scale: true,
            ..PcaConfig::default()
        },
    );
    assert_eq!(r.std_dev[2], 0.0, "a constant column has zero spread");
    assert!(r.std_dev[0] > 1.0 && r.std_dev[1] != 1.0);

    let point = vec![35.0, 2.0, 4.0];
    let manual: Vec<f64> = r
        .components
        .iter()
        .map(|c| {
            (0..3)
                .map(|j| {
                    let mut v = point[j] - r.mean[j];
                    if r.std_dev[j] > 0.0 {
                        v /= r.std_dev[j];
                    }
                    v * c[j]
                })
                .sum()
        })
        .collect();
    let one = r.transform_one(&point);
    let many = r.transform(&[point.clone()]);
    for c in 0..r.components.len() {
        assert!((one[c] - manual[c]).abs() < 1e-9, "{one:?} vs {manual:?}");
        assert!(
            (many[0][c] - manual[c]).abs() < 1e-9,
            "{many:?} vs {manual:?}"
        );
    }
    assert!(r.transform(&[]).is_empty());
}

#[test]
fn randomized_pca_on_constant_data_finds_no_components() {
    use crate::{pca, PcaConfig, PcaSolver};
    let data = vec![vec![2.0, 2.0, 2.0]; 6];
    let r = pca(
        &data,
        PcaConfig {
            n_components: 2,
            solver: PcaSolver::Randomized {
                n_oversamples: 2,
                n_power_iters: 1,
            },
            ..PcaConfig::default()
        },
    );
    assert!(r.components.is_empty(), "{:?}", r.components);
    assert!(r.explained_variance.is_empty());
    assert!(r.transform(&data).is_empty());
}
