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
    count_triangles, local_clustering_coefficient, local_clustering_coefficient_directed,
    page_rank, prim_mst, strongly_connected_components, weakly_connected_components, GraphView,
    NodeId, PageRankConfig,
};

/// Per-algorithm, and documented rather than one global epsilon: "within
/// tolerance" means different things for a probability vector and an integer.
const TOL_PAGERANK: f64 = 1e-9;
const TOL_CLUSTERING: f64 = 1e-9;
const TOL_MST: f64 = 1e-9;

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
