//! Re-run the algorithms against NetworkX's *recorded* answers (ALGO-02).
//!
//! Two different checks, and conflating them would be the mistake:
//!
//!   * **parity** — do we agree with NetworkX *today*? That needs Python,
//!     NetworkX and SciPy, and lives in the benchmarks repo's harness. It runs
//!     on a cadence.
//!   * **regression** — do we still agree with what NetworkX said *then*?
//!     That is this, it needs nothing but the engine, and it runs on every
//!     commit.
//!
//! `tests/algo-parity-reference.json` carries the date it was taken and
//! the NetworkX version, so a reader can see how old the agreement is. It is
//! re-recorded deliberately, by `harness/algo-parity/record_reference.py`
//! after a parity run — never silently.
//!
//!     cargo run --release --example algo_parity_check
//!
//! Exits non-zero on any disagreement.

use std::collections::HashMap;
use std::path::PathBuf;

use samyama_graph_algorithms::{
    all_shortest_paths, a_star, louvain, modularity, yens_k_shortest,
    betweenness_centrality, closeness_centrality, core_number, count_triangles,
    degree_centrality, eigenvector_centrality, harmonic_centrality,
    link_prediction::{score_one, LinkScore},
    average_neighbour_degree, degree_assortativity, diameter, eccentricity, radius,
    local_clustering_coefficient, local_clustering_coefficient_directed, page_rank, prim_mst,
    strongly_connected_components, weakly_connected_components, GraphView, NodeId,
    PageRankConfig,
};

/// Per-algorithm, and documented rather than one global epsilon: "within
/// tolerance" means different things for a probability vector and an integer.
const TOL_PAGERANK: f64 = 1e-9;
const TOL_CLUSTERING: f64 = 1e-9;
const TOL_MST: f64 = 1e-9;
const TOL_CENTRALITY: f64 = 1e-9;
/// Power iteration, not a closed form: looser than the exact scores and still
/// three orders tighter than any difference that would change a ranking.
const TOL_EIGENVECTOR: f64 = 1e-6;
/// Path costs are sums of the recorded edge weights, so the two sides do the
/// same additions in possibly different orders. A few ULPs, not a tolerance
/// for a different answer.
const TOL_PATH: f64 = 1e-9;
/// Modularity is a sum over every edge and every pair of community degrees;
/// on these graphs that is a few hundred terms.
const TOL_MODULARITY: f64 = 1e-9;
/// How many shortest paths the recorder enumerated per pair before stopping.
/// The engine is given the same cap, so a graph with an exponential number of
/// them is not the thing being compared.
const PATH_ENUM_CAP: usize = 64;

fn view_of(nodes: usize, edges: &[(usize, usize, f64)], directed: bool) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..nodes).map(|i| i as NodeId).collect();
    let node_to_index: HashMap<NodeId, usize> = (0..nodes).map(|i| (i as NodeId, i)).collect();
    let mut outgoing = vec![Vec::new(); nodes];
    let mut incoming = vec![Vec::new(); nodes];
    let mut weights = vec![Vec::new(); nodes];
    for &(a, b, w) in edges {
        outgoing[a].push(b);
        weights[a].push(w);
        incoming[b].push(a);
        if !directed {
            outgoing[b].push(a);
            weights[b].push(w);
            incoming[a].push(b);
        }
    }
    GraphView::from_adjacency_list(nodes, index_to_node, node_to_index, outgoing, incoming, Some(weights))
}

/// A view storing each edge exactly once, as `build_view` does in the engine.
/// `view_single`, but carrying the edge weights.
///
/// Kept apart from `view_single` rather than folded into it. The shape metrics
/// that use `view_single` are hop-counted by definition -- a diameter is a
/// number of edges -- so handing them weights would say something the metric
/// does not mean. Modularity, by contrast, is weighted whenever the graph is,
/// and NetworkX's `weight="weight"` is the convention recorded.
///
/// Passing the unweighted view to modularity was worth 0.013 of Q on the
/// 40-node graph and disagreed on all three: ours scored the graph as if every
/// edge were 1.0 while the recorded answer used the weights. Nothing about the
/// disagreement said "you compared two different graphs" -- it looked exactly
/// like a formula that was slightly wrong.
fn view_single_weighted(nodes: usize, edges: &[(usize, usize, f64)]) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..nodes).map(|i| i as NodeId).collect();
    let node_to_index: HashMap<NodeId, usize> = (0..nodes).map(|i| (i as NodeId, i)).collect();
    let mut outgoing = vec![Vec::new(); nodes];
    let mut incoming = vec![Vec::new(); nodes];
    let mut weights = vec![Vec::new(); nodes];
    for &(a, b, w) in edges {
        outgoing[a].push(b);
        weights[a].push(w);
        incoming[b].push(a);
    }
    GraphView::from_adjacency_list(
        nodes, index_to_node, node_to_index, outgoing, incoming, Some(weights),
    )
}

fn view_single(nodes: usize, edges: &[(usize, usize, f64)]) -> GraphView {
    let index_to_node: Vec<NodeId> = (0..nodes).map(|i| i as NodeId).collect();
    let node_to_index: HashMap<NodeId, usize> = (0..nodes).map(|i| (i as NodeId, i)).collect();
    let mut outgoing = vec![Vec::new(); nodes];
    let mut incoming = vec![Vec::new(); nodes];
    for &(a, b, _) in edges {
        outgoing[a].push(b);
        incoming[b].push(a);
    }
    GraphView::from_adjacency_list(nodes, index_to_node, node_to_index, outgoing, incoming, None)
}

fn main() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/algo-parity-reference.json");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {}: {e}", path.display());
            eprintln!("re-record it with harness/algo-parity/record_reference.py");
            std::process::exit(66);
        }
    };
    let doc: serde_json::Value = serde_json::from_str(&text).expect("reference file is JSON");

    println!(
        "ALGO-02 regression — against NetworkX {} recorded {}",
        doc["networkx_version"].as_str().unwrap_or("?"),
        doc["taken"].as_str().unwrap_or("?")
    );

    let mut failures: Vec<String> = Vec::new();
    let mut checks = 0usize;

    for g in doc["graphs"].as_array().expect("graphs") {
        let name = g["name"].as_str().unwrap_or("?");
        let directed = g["directed"].as_bool().unwrap_or(false);
        let nodes = g["nodes"].as_u64().unwrap_or(0) as usize;
        let edges: Vec<(usize, usize, f64)> = g["edges"]
            .as_array()
            .expect("edges")
            .iter()
            .map(|e| {
                let a = e[0].as_u64().unwrap() as usize;
                let b = e[1].as_u64().unwrap() as usize;
                (a, b, e[2].as_f64().unwrap())
            })
            .collect();
        let view = view_of(nodes, &edges, directed);
        let want = &g["networkx"];

        // Whole-graph shape, on a singly-stored view as the engine builds one.
        {
            let single = view_single(nodes, &edges);
            let ecc = eccentricity(&single, true);
            let te = &want["eccentricity"];
            let ok_e = if te.is_null() {
                ecc.iter().any(|x| x.is_none())
            } else {
                (0..nodes).all(|i| ecc[i] == te[i.to_string()].as_i64())
            };
            checks += 1;
            if !ok_e { failures.push(format!("{name} eccentricity disagrees")); }

            for (algo, ours) in [("diameter", diameter(&single, true)),
                                 ("radius", radius(&single, true))] {
                checks += 1;
                if ours != want[algo].as_i64() {
                    failures.push(format!("{name} {algo}: ours {ours:?} vs {:?}", want[algo]));
                }
            }

            let and = average_neighbour_degree(&single, true);
            let ta = &want["average_neighbor_degree"];
            let mut d = 0.0f64;
            for i in 0..nodes {
                d = d.max((and[i] - ta[i.to_string()].as_f64().unwrap_or(f64::NAN)).abs());
                checks += 1;
            }
            if d > TOL_CENTRALITY {
                failures.push(format!("{name} average_neighbor_degree max |delta| {d:.3e}"));
            }

            // `null` here means NetworkX returned NaN -- every edge joins
            // equal degrees -- and the engine returns None. Compared as
            // equals rather than skipped: agreeing that a value is undefined
            // is a real agreement.
            let ours_a = degree_assortativity(&single, true);
            let theirs_a = want["degree_assortativity"].as_f64();
            checks += 1;
            let ok_a = match (ours_a, theirs_a) {
                (None, None) => true,
                (Some(x), Some(y)) => (x - y).abs() < TOL_CENTRALITY,
                _ => false,
            };
            if !ok_a {
                failures.push(format!("{name} degree_assortativity: ours {ours_a:?} vs {theirs_a:?}"));
            }
            println!("  {}    {name:14} {:12} ecc/diam/rad/avgnbr/assort",
                     if ok_e && ok_a && d <= TOL_CENTRALITY { "ok" } else { "FAIL" }, "shape");
        }

        // Link prediction, over the same unconnected pairs the recorder used.
        // Keyed `"u-v"` with u < v; the key set *is* the pair list, so a pair
        // the recorder excluded is never asked about here either.
        for (algo, which) in [
            ("common_neighbours", LinkScore::CommonNeighbours),
            ("jaccard", LinkScore::Jaccard),
            ("adamic_adar", LinkScore::AdamicAdar),
        ] {
            let Some(theirs) = want.get(algo).and_then(|v| v.as_object()) else { continue };
            let single = view_single(nodes, &edges);
            let (mut worst, mut at) = (0.0f64, String::new());
            for (key, t) in theirs {
                let Some((a, b)) = key.split_once('-') else { continue };
                let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) else { continue };
                let ours = score_one(&single, which, a, b).unwrap_or(f64::NAN);
                let t = t.as_f64().unwrap_or(f64::NAN);
                checks += 1;
                let d = (ours - t).abs();
                if d > worst {
                    worst = d;
                    at = key.clone();
                }
                if d > TOL_CENTRALITY {
                    failures.push(format!("{name} {algo} pair {key}: ours {ours} vs networkx {t}"));
                }
            }
            println!("  {}    {name:14} {algo:12} max |delta| {worst:.3e} at pair {at} \
                      ({} pairs)", if worst <= TOL_CENTRALITY { "ok" } else { "FAIL" }, theirs.len());
        }

        // Centrality is checked against a view built the way the *engine*
        // builds one -- each edge stored once -- rather than against `view_of`,
        // which writes an undirected edge into both directions for the older
        // algorithms.
        //
        // The two conventions cannot share a call. On the doubled view,
        // `out + in` is twice the degree, while walking both directions
        // double-counts every shortest path and breaks betweenness alone,
        // leaving degree and closeness looking right. Detecting which view one
        // has is not possible either: equal in- and out-degree everywhere is
        // also true of a balanced directed graph. So the caller states it, and
        // this states it by building the view it means.
        let single = view_single(nodes, &edges);
        let bidir = !directed;
        for (algo, ours, tol) in [
            ("degree_centrality", degree_centrality(&single, bidir), TOL_CENTRALITY),
            ("closeness_centrality", closeness_centrality(&single, bidir), TOL_CENTRALITY),
            ("betweenness_centrality", betweenness_centrality(&single, bidir), TOL_CENTRALITY),
            ("harmonic_centrality", harmonic_centrality(&single, bidir), TOL_CENTRALITY),
            // Integers, so exact. A core number off by one is a different
            // answer, not a rounding difference.
            ("core_number", core_number(&single, bidir).iter().map(|&c| c as f64).collect(), 0.0),
            // Power iteration, so looser than the exact ones but far tighter
            // than the differences that matter. `None` here means it did not
            // converge, which is reported rather than compared -- see below.
            ("eigenvector_centrality",
             eigenvector_centrality(&single, bidir, 1000, 1e-10).unwrap_or_default(),
             TOL_EIGENVECTOR),
        ] {
            // A recorded `null` means NetworkX itself declined to answer, and
            // there is nothing to compare against. Skipping is right; scoring
            // it as agreement would be a check that cannot fail.
            if want.get(algo).is_some_and(|v| v.is_null()) {
                println!("  skip  {name:14} {:12} networkx did not converge",
                         algo.trim_end_matches("_centrality"));
                continue;
            }
            if ours.is_empty() {
                failures.push(format!("{name} {algo}: did not converge"));
                continue;
            }
            let Some(theirs) = want.get(algo) else { continue };
            let (mut worst, mut at) = (0.0f64, 0usize);
            for i in 0..nodes {
                let t = theirs[i.to_string()].as_f64().unwrap_or(f64::NAN);
                checks += 1;
                let d = (ours[i] - t).abs();
                if d > worst {
                    worst = d;
                    at = i;
                }
                if d > tol {
                    failures.push(format!(
                        "{name} {algo} node {i}: ours {:.12} vs networkx {t:.12}",
                        ours[i]
                    ));
                }
            }
            // Printed like the others: 600 silent checks are indistinguishable
            // from 600 skipped ones, which is the whole reason the count is
            // reported at the end.
            println!(
                "  {}    {name:14} {:12} max |delta| {worst:.3e} at node {at}",
                if worst <= tol { "ok" } else { "FAIL" },
                algo.trim_end_matches("_centrality"),
            );
        }

        let mut check_map = |label: &str, ours: &HashMap<NodeId, f64>, tol: f64| {
            checks += 1;
            let mut worst = 0.0f64;
            let mut worst_at = String::new();
            for (k, v) in want[label].as_object().expect("map") {
                let id: NodeId = k.parse::<u64>().expect("node id") as NodeId;
                let mine = ours.get(&id).copied().unwrap_or(f64::NAN);
                let d = (mine - v.as_f64().unwrap()).abs();
                if d > worst || mine.is_nan() {
                    worst = d;
                    worst_at = k.clone();
                }
            }
            let ok = worst <= tol;
            println!(
                "  {}  {:<14} {:<12} max |delta| {:.3e} at node {}",
                if ok { "ok  " } else { "FAIL" },
                name,
                label,
                worst,
                worst_at
            );
            if !ok {
                failures.push(format!("{name}/{label}: max |delta| {worst:.3e} > {tol:e}"));
            }
        };

        let tight = PageRankConfig { iterations: 200, tolerance: 1e-12, ..Default::default() };
        check_map("pagerank", &page_rank(&view, tight), TOL_PAGERANK);

        let lcc = if directed {
            local_clustering_coefficient_directed(&view, true)
        } else {
            local_clustering_coefficient(&view)
        };
        check_map("clustering", &lcc.coefficients, TOL_CLUSTERING);

        let mut check_exact = |label: &str, ours: i64| {
            let Some(w) = want[label].as_i64() else { return };
            checks += 1;
            let ok = ours == w;
            println!(
                "  {}  {:<14} {:<12} ours={} recorded={}",
                if ok { "ok  " } else { "FAIL" },
                name,
                label,
                ours,
                w
            );
            if !ok {
                failures.push(format!("{name}/{label}: ours={ours} recorded={w}"));
            }
        };
        check_exact("wcc_count", weakly_connected_components(&view).components.len() as i64);
        if directed {
            check_exact("scc_count", strongly_connected_components(&view).components.len() as i64);
        }
        check_exact("triangles", count_triangles(&view) as i64);

        // ---- Paths and community, on the singly-stored view.
        //
        // `view_of` doubles an undirected edge so both endpoints see it;
        // `view_single` stores it once. Path search wants the doubled one --
        // it has to be able to walk in either direction -- and modularity
        // wants to be told which convention it is reading, because the
        // denominator is the total edge weight and a doubled view has twice
        // as much of it. Getting that backwards halves or doubles Q without
        // failing anything.
        {
            let paths = &want["hop_distance"];
            let counts = &want["shortest_path_count"];
            let wdist = &want["weighted_distance"];
            if let Some(obj) = paths.as_object() {
                for (key, hops) in obj {
                    let mut it = key.split('-').map(|x| x.parse::<usize>().unwrap());
                    let (sv, tv) = (it.next().unwrap(), it.next().unwrap());

                    // Enumerated shortest paths: every one must have the
                    // recorded hop count, and there must be the recorded
                    // number of them. Checking only the count would pass a
                    // routine that returned the right number of wrong paths.
                    let ours = all_shortest_paths(&view, sv, tv, PATH_ENUM_CAP);
                    let want_hops = hops.as_u64().unwrap() as usize;
                    let want_n = counts[key].as_u64().unwrap() as usize;
                    checks += 1;
                    let lengths_ok = ours.iter().all(|p| p.len() == want_hops + 1);
                    if !(ours.len() == want_n && lengths_ok) {
                        failures.push(format!(
                            "{name}/all_shortest_paths {key}: ours={} paths (hops ok: {lengths_ok}), \
                             recorded={want_n} at {want_hops} hops",
                            ours.len()
                        ));
                    }

                    // A* with a zero heuristic is Dijkstra, which is what the
                    // recorder ran. A heuristic of our own would make this a
                    // test of the heuristic.
                    let zero = vec![0.0; nodes];
                    checks += 1;
                    let want_w = wdist[key].as_f64().unwrap();
                    match a_star(&view, sv, tv, &zero) {
                        Some((_, cost)) if (cost - want_w).abs()
                            / want_w.abs().max(1e-12) <= TOL_PATH => {}
                        Some((_, cost)) => failures.push(format!(
                            "{name}/a_star {key}: ours={cost} recorded={want_w}")),
                        None => failures.push(format!(
                            "{name}/a_star {key}: no path, recorded={want_w}")),
                    }
                }
            }

            // Yen's, on the one pair the recorder ran it for. Lengths in
            // non-decreasing order, not the paths: two implementations break
            // ties between equal-length paths differently, and requiring the
            // same order would fail on a difference that is not an error.
            if let Some(obj) = want["simple_path_lengths"].as_object() {
                for (key, lens) in obj {
                    let mut it = key.split('-').map(|x| x.parse::<usize>().unwrap());
                    let (sv, tv) = (it.next().unwrap(), it.next().unwrap());
                    let want_lens: Vec<f64> =
                        lens.as_array().unwrap().iter().map(|x| x.as_f64().unwrap()).collect();
                    let ours = yens_k_shortest(&view, sv, tv, want_lens.len());
                    checks += 1;
                    let ok = ours.len() == want_lens.len()
                        && ours.iter().zip(&want_lens).all(|((_, c), w)| {
                            (c - w).abs() / w.abs().max(1e-12) <= TOL_PATH
                        });
                    if !ok {
                        failures.push(format!(
                            "{name}/yens {key}: ours={:?} recorded={want_lens:?}",
                            ours.iter().map(|(_, c)| *c).collect::<Vec<_>>()
                        ));
                    }
                }
            }

            // Modularity of a partition fixed by node index. Both sides score
            // the *same* partition, which is the only way this tests the
            // formula: Louvain is stochastic and seeded differently in the two
            // implementations, so comparing partitions would say nothing.
            if let Some(want_q) = want["modularity_of_index_mod_3"].as_f64() {
                let single = view_single_weighted(nodes, &edges);
                let parts: Vec<usize> = (0..nodes).map(|i| i % 3).collect();
                checks += 1;
                match modularity(&single, &parts) {
                    Some(q) if (q - want_q).abs() <= TOL_MODULARITY => {}
                    Some(q) => failures.push(format!(
                        "{name}/modularity: ours={q} recorded={want_q}")),
                    None => failures.push(format!(
                        "{name}/modularity: refused, recorded={want_q}")),
                }

                // Louvain's own answer is not compared to NetworkX's -- see
                // above -- but it must not be *worse* than an arbitrary
                // partition. A Louvain that scores below `i % 3` has stopped
                // maximising anything, which is the failure that a
                // partition-by-partition comparison could never state.
                checks += 1;
                let found = louvain(&single, 10);
                match modularity(&single, &found) {
                    Some(q) if q >= want_q - TOL_MODULARITY => {}
                    Some(q) => failures.push(format!(
                        "{name}/louvain: Q={q} is worse than the arbitrary \
                         partition's {want_q}")),
                    None => failures.push(format!("{name}/louvain: partition incomplete")),
                }
            }
        }

        checks += 1;
        let ours_mst = prim_mst(&view).total_weight;
        let want_mst = want["mst_total_weight"].as_f64().unwrap_or(f64::NAN);
        let rel = (ours_mst - want_mst).abs() / want_mst.abs().max(1e-12);
        let ok = rel <= TOL_MST;
        println!(
            "  {}  {:<14} {:<12} ours={ours_mst} recorded={want_mst} rel={rel:.3e}",
            if ok { "ok  " } else { "FAIL" },
            name,
            "mst_weight"
        );
        if !ok {
            failures.push(format!("{name}/mst_weight: ours={ours_mst} recorded={want_mst}"));
        }
    }

    println!("\n{}/{} checks agree with the recorded answers", checks - failures.len(), checks);
    if !failures.is_empty() {
        println!("\nthe algorithms have drifted from what NetworkX recorded:");
        for f in &failures {
            println!("  - {f}");
        }
        println!(
            "\nIf the change was deliberate, re-record with\n  \
             harness/algo-parity/record_reference.py  (after a live parity run)"
        );
        std::process::exit(1);
    }
}
