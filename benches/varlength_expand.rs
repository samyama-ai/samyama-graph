//! What a variable-length expansion costs per edge visited (#520).
//!
//! `CH-PROFILE-01` put **98.4% of LDBC IC1 in `VarLengthExpand`** — 582 ms to
//! produce 9,858 rows from `(p)-[:KNOWS*1..3]-(friend)` over a relation of only
//! 219,450 edges. An upper bound on the edges such a BFS can visit is the whole
//! relation twice (both directions), so that is roughly **1.4 µs per edge
//! visited**, against tens of nanoseconds for walking a CSR slice.
//!
//! The same operator at depth 2 costs 7–9 µs per *output row* and at depth 3
//! costs 59 — 3× the rows for 22× the time. Two points do not make a curve, and
//! the issue asks for one before anything is optimised.
//!
//! This sweeps the depth on a graph of known degree, so:
//!
//!   * **edges visited is computable**, not estimated — the BFS deduplicates by
//!     node, so it visits every edge incident to a reached node exactly once
//!     per direction, and the bench reports reached-node counts alongside;
//!   * the per-edge cost can be compared across depths, which separates "each
//!     hop is expensive" from "the last hop reaches most of the graph".
//!
//!   cargo bench --bench varlength_expand
//!   cargo bench --bench varlength_expand -- --nodes 50000 --degree 20

use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// A regular graph with scrambled targets: every node has exactly `degree`
/// outgoing edges, aimed by a cheap hash so the neighbourhood **expands** with
/// depth instead of creeping.
///
/// Regular rather than skewed on purpose. LDBC's degree distribution is heavy
/// tailed, which is realistic and makes "per edge visited" impossible to read;
/// the question here is the constant, and a constant is easiest to see where
/// the frontier size is predictable.
///
/// A first draft placed targets at fixed strides (`i + 1 + d * 7919`). Every
/// node then had the *same* offsets, so the set reachable in k hops was the set
/// of k-fold sums of a fixed offset list — which grows polynomially. It reached
/// 296 nodes of 20,000 at depth 4 and measured a per-edge cost 28× below LDBC's,
/// because nothing ever left cache. A fixture that does not fan out does not
/// exercise the thing being measured.
/// `noise` is the parameter that makes this resemble LDBC rather than a
/// textbook BFS.
///
/// In LDBC a `Person` is not a node with 41 `KNOWS` edges. It is a node with 41
/// `KNOWS` edges **and** an incoming `HAS_CREATOR` from every post and comment
/// it wrote, an outgoing `HAS_INTEREST` per tag, `LIKES`, `IS_LOCATED_IN`,
/// `HAS_MEMBER` — `KNOWS` is 219,450 edges of a 21.1M-edge graph, and the ones
/// incident to a Person are dominated by the other types.
///
/// A traversal that enumerates every incident edge and *then* filters by type
/// pays for all of them. A fixture with one edge type cannot show that, which
/// is why the first version of this bench measured 15–45 ns per edge against
/// LDBC's ~1.4 µs and looked like the issue was wrong.
fn scrambled_regular(nodes: usize, degree: usize, noise: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..nodes).map(|_| store.create_node("N")).collect();
    let pick = |i: usize, d: usize, salt: u64| -> usize {
        let x = (i as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add((d as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9))
            .wrapping_add(salt);
        let x = x ^ (x >> 31);
        (x % nodes as u64) as usize
    };
    for (i, &src) in ids.iter().enumerate() {
        for d in 0..degree {
            let target = ids[pick(i, d, 0)];
            if target != src {
                let _ = store.create_edge(src, target, "KNOWS");
            }
        }
        for d in 0..noise {
            let target = ids[pick(i, d, 0x5DEE_CE66)];
            if target != src {
                let _ = store.create_edge(src, target, "NOISE");
            }
        }
    }
    store
}

/// Distinct nodes reached, and how long the query took.
fn run(store: &GraphStore, cypher: &str) -> (usize, f64) {
    let query = parse_query(cypher).expect("query should parse");
    // Warm, so the first depth does not pay for statistics the others skip.
    let _ = QueryExecutor::new(store).execute(&query).expect("query should run");

    let started = Instant::now();
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    (batch.records.len(), ms)
}

/// Exclusive time in `VarLengthExpand`, from `PROFILE` (#517), so the scan and
/// projection around it are not folded in.
fn expand_self_ms(store: &GraphStore, cypher: &str) -> Option<f64> {
    let query = parse_query(&format!("PROFILE {cypher}")).ok()?;
    let batch = QueryExecutor::new(store).execute(&query).ok()?;
    let text = match batch.records.first()?.get("plan")? {
        Value::Property(samyama::graph::PropertyValue::String(t)) => t.clone(),
        _ => return None,
    };
    text.lines()
        .skip_while(|l| !l.contains("Hottest operators"))
        .find(|l| l.contains("VarLengthExpand"))
        .and_then(|l| l.split_whitespace().find(|w| w.ends_with("ms")))
        .and_then(|w| w.trim_end_matches("ms").parse().ok())
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse().ok())
    };
    let nodes = arg("--nodes").unwrap_or(20_000);
    let degree = arg("--degree").unwrap_or(10);
    let max_depth = arg("--max-depth").unwrap_or(4);
    // Edges of a type the pattern does not want, per node. LDBC's ratio of
    // non-KNOWS to KNOWS edges incident to a Person is far higher than this.
    let noise = arg("--noise").unwrap_or(0);

    eprintln!(
        "Building {nodes} nodes: out-degree {degree} KNOWS + {noise} NOISE ({} edges)…",
        nodes * (degree + noise)
    );
    let started = Instant::now();
    let store = scrambled_regular(nodes, degree, noise);
    eprintln!("built in {:.1}s\n", started.elapsed().as_secs_f64());

    // Anchor on a fixed node so every depth asks the same question.
    let anchor = 1;

    println!(
        "{:<10} {:>10} {:>12} {:>14} {:>16} {:>12}",
        "pattern", "reached", "expand ms", "µs per row", "edges visited", "ns per edge"
    );
    println!("{:-<10} {:->10} {:->12} {:->14} {:->16} {:->12}", "", "", "", "", "", "");

    let mut previous_reached = 0usize;
    for depth in 1..=max_depth {
        let cypher = format!(
            "MATCH (p:N)-[:KNOWS*1..{depth}]-(f:N) WHERE id(p) = {anchor} RETURN f"
        );
        let (reached, _wall_ms) = run(&store, &cypher);
        let expand_ms = expand_self_ms(&store, &cypher).unwrap_or(f64::NAN);

        // The BFS deduplicates by node, so every reached node has its incident
        // edges enumerated exactly once per direction. Undirected pattern, so
        // both directions: out-degree plus in-degree, which average to
        // `degree` each over a regular graph.
        // Every reached node has *all* its incident edges enumerated, both
        // directions, whatever their type -- which is the point when `noise`
        // is set.
        let _ = previous_reached;
        let edges_visited = reached * (degree + noise) * 2;

        println!(
            "{:<10} {:>10} {:>12.1} {:>14.2} {:>16} {:>12.1}",
            format!("*1..{depth}"),
            reached,
            expand_ms,
            expand_ms * 1000.0 / reached.max(1) as f64,
            edges_visited,
            expand_ms * 1e6 / edges_visited.max(1) as f64,
        );
        previous_reached = reached;
    }

    println!();
    println!("`edges visited` counts each reached node's incident edges once per direction.");
    println!("It is an estimate on a regular graph and an upper bound on any graph, so the");
    println!("ns-per-edge column is a floor rather than an exact figure.");
    println!();
    println!("LDBC IC1 measured 582 ms for 9,858 reached nodes over a 219,450-edge relation");
    println!("at *1..3 -- roughly 1.4 µs per edge visited (#520).");

    bench_setup::report_drift(calibration);
}
