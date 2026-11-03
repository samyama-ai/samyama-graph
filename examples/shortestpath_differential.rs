//! Bidirectional shortestPath must agree with the one-sided walk on *length*.
//!
//! `shortestPath` returns **a** shortest path, so the specific path may
//! legitimately differ between the two algorithms when several are tied. The
//! length may not, and neither may reachability: a bidirectional search that
//! reverses the wrong way finds nothing on directed graphs and everything on
//! undirected ones, and both failures look like "a different path" until you
//! check the number.
//!
//! `allShortestPaths` still uses the one-sided walk, so it doubles as the
//! reference: on the same graph and endpoints, `min(len(allShortestPaths))`
//! must equal `len(shortestPath)`.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// Deterministic pseudo-random graph; no RNG crate, and the seed is printed
/// so a failure is reproducible.
fn graph(seed: u64, n: usize, edges: usize, directed_only: bool) -> GraphStore {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..n {
        let node = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", node, "id", PropertyValue::Integer(i as i64)).unwrap();
        ids.push(node);
    }
    let mut x = seed | 1;
    let mut next = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
    for _ in 0..edges {
        let a = (next() as usize) % n;
        let b = (next() as usize) % n;
        if a != b {
            let _ = s.create_edge(ids[a], ids[b], "R");
            if !directed_only && next() % 2 == 0 {
                let _ = s.create_edge(ids[b], ids[a], "R");
            }
        }
    }
    s
}

fn len_of(store: &GraphStore, cypher: &str) -> Option<i64> {
    let parsed = parse_query(cypher).ok()?;
    let batch = QueryExecutor::new(store).execute(&parsed).ok()?;
    batch.records.iter().filter_map(|r| match r.get("l") {
        Some(v) => format!("{v:?}").split("Integer(").nth(1)
            .and_then(|t| t.split(')').next()).and_then(|t| t.parse::<i64>().ok()),
        None => None,
    }).min()
}

fn main() {
    let mut checked = 0usize;
    let mut disagreed = Vec::new();
    let mut reachable = 0usize;

    for seed in 1u64..=40 {
        for (n, e, directed) in [(60usize, 120usize, true), (60, 90, false), (120, 400, true)] {
            let s = graph(seed, n, e, directed);
            for (a, b) in [(0i64, (n - 1) as i64), (1, 7), (3, (n / 2) as i64)] {
                for arrow in ["-[:R*]->", "-[:R*]-"] {
                    let one = format!(
                        "MATCH p = allShortestPaths((x:N {{id: {a}}}){arrow}(y:N {{id: {b}}})) \
                         RETURN length(p) AS l");
                    let two = format!(
                        "MATCH p = shortestPath((x:N {{id: {a}}}){arrow}(y:N {{id: {b}}})) \
                         RETURN length(p) AS l");
                    let (r_all, r_one) = (len_of(&s, &one), len_of(&s, &two));
                    checked += 1;
                    if r_all.is_some() { reachable += 1; }
                    if r_all != r_one {
                        disagreed.push(format!(
                            "seed={seed} n={n} e={e} directed={directed} {a}->{b} {arrow}: \
                             allShortestPaths={r_all:?} shortestPath={r_one:?}"));
                    }
                }
            }
        }
    }

    println!("cases checked : {checked}");
    println!("reachable     : {reachable}   (a run where nothing is reachable proves nothing)");
    println!("disagreements : {}", disagreed.len());
    for d in disagreed.iter().take(10) {
        println!("  {d}");
    }
    assert!(reachable > checked / 10, "fixture too sparse: only {reachable} reachable pairs");
    assert!(disagreed.is_empty(), "{} disagreements", disagreed.len());
    println!("\nOK: bidirectional shortestPath agrees with the one-sided reference on every case");
}
