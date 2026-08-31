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
    betweenness_centrality, closeness_centrality, core_number, count_triangles,
    degree_centrality, eigenvector_centrality, harmonic_centrality,
    link_prediction::{score_one, LinkScore},
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
