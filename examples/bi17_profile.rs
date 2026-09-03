//! Where BI-17's time actually goes (#1078).
//!
//! Four hypotheses about this query have been tested and all four were wrong,
//! so this one does not propose a fifth. It runs the query under conditions
//! that let a profiler attribute the time, and prints the shape of the work
//! (candidates considered vs rows emitted) so the ratio between them is a
//! measured number rather than an estimate.
//!
//!   cargo run --release --example bi17_profile -- --data-dir <sf1-dir>
//!   perf record -g ./target/release/examples/bi17_profile --data-dir <dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;

/// The real BI-17, bounded on the outer scan so it terminates while still
/// running the full triangle logic for every row the bound keeps.
fn bounded(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
         WHERE a.id < {limit} AND a.id < b.id AND b.id < c.id
         RETURN count(a) AS triangles"
    )
}

/// One hop. The point of having it is to get a cost *per emitted row* that
/// owes nothing to the triangle shape, so the two- and three-hop numbers have
/// something to be compared against.
fn one_hop(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)
         WHERE a.id < {limit} AND a.id < b.id
         RETURN count(a) AS pairs"
    )
}

/// The same shape with the closing hop removed: paths, not triangles. The
/// difference between this and `bounded` is what closing the cycle costs; the
/// value of this one alone is how many candidates the closing hop is handed.
fn open_path(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)
         WHERE a.id < {limit} AND a.id < b.id AND b.id < c.id
         RETURN count(a) AS candidates"
    )
}

/// Trials per query. One run of each was not enough: the closing-hop cost is a
/// difference between two whole-query timings, and at ~0.2 s each the run-to-run
/// spread swamped it. The first version reported 2135, 344, **-257** and 93 ns
/// per candidate across four bounds of the same run -- a negative cost is not a
/// small cost, it is a measurement that cannot see what it is subtracting.
const TRIALS: usize = 7;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn run(graph: &GraphStore, label: &str, cypher: &str) -> Option<(i64, f64)> {
    let q = match parse_query(cypher) {
        Ok(q) => q,
        Err(e) => {
            println!("{label:<26} parse failed: {e}");
            return None;
        }
    };
    // Warm once, then take the median of TRIALS. The first execution of any
    // query here pays one-off costs -- caches, lazily built indexes -- which is
    // why the smallest bound used to report 84,000 ns/row and look like a
    // finding.
    let _ = QueryExecutor::new(graph).execute(&q);
    let mut times = Vec::with_capacity(TRIALS);
    let mut last = None;
    for _ in 0..TRIALS {
        let t0 = std::time::Instant::now();
        last = Some(QueryExecutor::new(graph).execute(&q));
        times.push(t0.elapsed().as_secs_f64());
    }
    let secs_med = median(times);
    let t = std::time::Instant::now();
    match last.unwrap() {
        Ok(out) => {
            let v = out.records.first().and_then(|r| r.values().next().cloned());
            let n = format!("{v:?}")
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<i64>()
                .ok();
            let _ = t;
            let secs = secs_med;
            let rate = n.map(|v| v as f64 / secs).unwrap_or(0.0);
            println!(
                "{label:<26} {secs:>8.3}s  rows={:>9}  {:>10.0} rows/s  {:>6.0} ns/row",
                n.unwrap_or(-1), rate, if rate > 0.0 { 1e9 / rate } else { 0.0 }
            );
            n.map(|v| (v, secs))
        }
        Err(e) => {
            println!("{label:<26} failed: {e}");
            None
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .expect("--data-dir <path> is required");

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    ldbc_bi_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded");

    // Growing bounds, so the scaling is visible rather than assumed: if the
    // cost per candidate is flat, time tracks the candidate count exactly, and
    // a departure from that is itself the finding.
    // `perf` is unavailable here (perf_event_paranoid=4, and sysctl is not
    // ours to change), so the attribution has to come from the shape of the
    // numbers rather than from a profile. Rows per second at one, two and
    // three hops answers the question that matters: if it is roughly constant,
    // the cost is per-row machinery -- record construction, cloning,
    // allocation -- and not anything about traversal depth or the closing hop.
    for limit in [200u64, 400, 800, 1600] {
        println!("--- a.id < {limit} ---");
        let pairs = run(&graph, "  1 hop  (a-b)", &one_hop(limit));
        let cands = run(&graph, "  2 hops (a-b-c)", &open_path(limit));
        let tris = run(&graph, "  3 hops (triangle)", &bounded(limit));
        // The triangle line's own `ns/row` is per *emitted triangle*, which is
        // one output per ~23 candidates and therefore reads as an alarming
        // five-figure number that means nothing. What closing the cycle
        // actually costs is the difference between the three-hop and two-hop
        // times spread over the candidates the closing hop was handed, so
        // derive that here rather than leaving the misleading column to be
        // quoted.
        if let (Some((c, c_s)), Some((t, t_s))) = (cands, tris) {
            if t > 0 {
                println!("  candidates per triangle: {:.1}", c as f64 / t as f64);
            }
            if c > 0 {
                println!(
                    "  closing hop:             {:>6.0} ns per candidate  ({:.3}s - {:.3}s over {c} candidates)",
                    (t_s - c_s) / c as f64 * 1e9, t_s, c_s
                );
            }
        }
        let _ = pairs;
    }
    Ok(())
}
