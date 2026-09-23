//! Is snapshot restore linear in bytes, in nodes, or in edges? (#1174)
//!
//! #1174 records that restore is **95–99.7% of time to first answer** on a 12 MB
//! snapshot, that HW-06 (`server < 2 s at 10M edges`) is unmeasured, and that
//! extrapolating the 12 MB figure to the 10.4 GB snapshots we publish is "a
//! guess, not a measurement". It asks for three things, in order:
//!
//!   1. the curve, not a single number
//!   2. which term it is linear in — bytes, nodes or edges, which diverge
//!   3. an attribution across the phases, since export has one (#314, 0.77 MB/s)
//!      and import has none
//!
//! This answers 1–3 without the 10 GB files, which take hours to fetch. The trick
//! is that the question is not "how long does a 10 GB file take" but "which term
//! dominates" — and that is answered by varying nodes and edges **independently**
//! rather than by growing one realistic graph. A single scaling series cannot
//! separate the three candidates, because in a realistic graph all three grow at
//! once. Two series can:
//!
//!   * fixed edges, growing nodes
//!   * fixed nodes, growing edges
//!
//! Whichever series moves the clock is the term that matters, and the ratio
//! between them gives the per-node and per-edge costs to extrapolate with.
//!
//! Usage: cargo bench --bench restore_curve -- [--max-nodes N] [--max-edges N]

use std::time::Instant;

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::query::QueryEngine;
use samyama::snapshot::{export_tenant, import_tenant};

fn arg(args: &[String], name: &str) -> Option<usize> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

/// A store with exactly `nodes` nodes and `edges` edges.
///
/// Properties are the same shape at every size, so a byte total scales with the
/// counts rather than with how fat a row happens to be.
fn build(nodes: usize, edges: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::with_capacity(nodes);
    for i in 0..nodes {
        let mut props = PropertyMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert("name".to_string(), PropertyValue::String(format!("n{i:09}")));
        ids.push(store.create_node_with_properties(
            "default",
            vec![Label::new("N")],
            props,
        ));
    }
    // A deterministic spread rather than a chain, so adjacency is not one long
    // path -- a path is the cheapest possible shape to build and would flatter
    // the edge term.
    for e in 0..edges {
        let a = ids[e % nodes];
        let b = ids[(e * 7919 + 13) % nodes];
        let _ = store.create_edge(a, b, "E");
    }
    store
}

struct Row {
    nodes: usize,
    edges: usize,
    bytes: usize,
    restore_s: f64,
    first_answer_s: f64,
}

fn measure(nodes: usize, edges: usize) -> Row {
    let store = build(nodes, edges);
    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");
    let bytes = buf.len();

    // Restore into a fresh store, timed alone.
    let t = Instant::now();
    let mut restored = GraphStore::new();
    import_tenant(&mut restored, &buf[..]).expect("import");
    let restore_s = t.elapsed().as_secs_f64();

    // The cheapest useful query, which is #1174's "time to first answer" minus
    // restore. Counted after restore so the two are separable.
    let engine = QueryEngine::new();
    let t = Instant::now();
    let n = engine
        .execute("MATCH (n:N) RETURN count(n) AS n", &restored)
        .expect("query")
        .records
        .len();
    let query_s = t.elapsed().as_secs_f64();
    assert_eq!(n, 1, "the probe query must return a row");

    Row { nodes, edges, bytes, restore_s, first_answer_s: restore_s + query_s }
}

fn print_series(title: &str, rows: &[Row]) {
    println!("\n{title}");
    println!("{}", "-".repeat(96));
    println!(
        "{:>10} {:>10} {:>11} {:>11} {:>11} {:>12} {:>12}",
        "nodes", "edges", "MB", "restore s", "MB/s", "us/node", "us/edge"
    );
    for r in rows {
        let mb = r.bytes as f64 / 1e6;
        println!(
            "{:>10} {:>10} {:>11.2} {:>11.3} {:>11.1} {:>12.3} {:>12.3}",
            r.nodes,
            r.edges,
            mb,
            r.restore_s,
            mb / r.restore_s,
            r.restore_s * 1e6 / r.nodes.max(1) as f64,
            r.restore_s * 1e6 / r.edges.max(1) as f64,
        );
    }
}

/// Least-squares slope of y against x through the origin, which is what a "cost
/// per node" or "cost per edge" claim means.
fn slope(xs: &[f64], ys: &[f64]) -> f64 {
    let num: f64 = xs.iter().zip(ys).map(|(x, y)| x * y).sum();
    let den: f64 = xs.iter().map(|x| x * x).sum();
    if den == 0.0 { f64::NAN } else { num / den }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let max_nodes = arg(&args, "--max-nodes").unwrap_or(800_000);
    let max_edges = arg(&args, "--max-edges").unwrap_or(1_600_000);

    // Series A: edges fixed and small, nodes growing.
    let mut a = Vec::new();
    let mut n = max_nodes / 8;
    while n <= max_nodes {
        a.push(measure(n, 1_000));
        n *= 2;
    }
    print_series("A — edges fixed at 1,000, nodes growing", &a);

    // Series B: nodes fixed, edges growing.
    let mut b = Vec::new();
    let fixed_nodes = (max_nodes / 8).max(1_000);
    let mut e = max_edges / 8;
    while e <= max_edges {
        b.push(measure(fixed_nodes, e));
        e *= 2;
    }
    print_series(&format!("B — nodes fixed at {fixed_nodes}, edges growing"), &b);

    // --- which term is it -------------------------------------------------
    let per_node = slope(
        &a.iter().map(|r| r.nodes as f64).collect::<Vec<_>>(),
        &a.iter().map(|r| r.restore_s).collect::<Vec<_>>(),
    ) * 1e6;
    let per_edge = slope(
        &b.iter().map(|r| (r.edges - 1_000) as f64).collect::<Vec<_>>(),
        &b.iter().map(|r| r.restore_s - a[0].restore_s).collect::<Vec<_>>(),
    ) * 1e6;

    println!("\nwhich term dominates");
    println!("{}", "-".repeat(96));
    println!("marginal cost per node : {per_node:.3} us   (series A, edges held at 1,000)");
    println!("marginal cost per edge : {per_edge:.3} us   (series B, nodes held fixed)");

    // HW-06 asks for < 2 s at 10M edges. State what these two numbers predict,
    // and state that it is a prediction.
    let predicted = (10_000_000.0 * per_edge + 2_000_000.0 * per_node) / 1e6;
    println!(
        "\nHW-06 (< 2 s at 10M edges), predicted from these slopes with 2M nodes: {predicted:.1} s"
    );
    println!(
        "That is an extrapolation from a {:.0} MB measurement, not a measurement of a 10M-edge \
         snapshot. It is here to be falsified by the real files, which is what #1174 asks for.",
        b.last().map(|r| r.bytes as f64 / 1e6).unwrap_or(0.0)
    );

    println!("\nrestore as a share of time to first answer");
    println!("{}", "-".repeat(96));
    for r in a.iter().chain(b.iter()) {
        println!(
            "{:>10} nodes {:>10} edges   restore {:>6.1}% of first answer",
            r.nodes,
            r.edges,
            100.0 * r.restore_s / r.first_answer_s
        );
    }
}
