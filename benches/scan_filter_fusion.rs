//! What fusing a filter into its node scan saves per scanned row (#1615).
//!
//! `MATCH (p:P) WHERE ... RETURN count(*)` scans the label and tests every
//! node. Unfused, the scan builds a record for each node and hands the batch to
//! a separate `Filter`, which drops the ones that fail; fused, the scan tests
//! each node on one reused record and builds a row only for those that pass.
//!
//! Both plans run **interleaved in one process**, switched by
//! `SAMYAMA_SCAN_FILTER_FUSION`: the difference is per-row work against
//! allocation, which moves with the host, and two separate runs drift by more
//! than the effect (#529). The figure is ns per scanned row of the whole query,
//! and the reduction is the fused plan's saving on it.
//!
//!   cargo bench --bench scan_filter_fusion
//!   cargo bench --bench scan_filter_fusion -- --rows 2000000

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

#[path = "common/bench_setup.rs"]
mod bench_setup;

fn fixture(rows: usize) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..rows {
        let id = store.create_node("P");
        let _ = store.set_node_property(
            "default",
            id,
            "v",
            PropertyValue::Integer((i % 1000) as i64),
        );
        let city = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"][i % 10];
        let _ = store.set_node_property("default", id, "city", PropertyValue::String(city.into()));
    }
    store
}

/// Minimum of `runs`, in ms, with fusion `on` or off.
fn time(store: &GraphStore, cypher: &str, runs: usize, on: bool) -> f64 {
    if on {
        std::env::remove_var("SAMYAMA_SCAN_FILTER_FUSION");
    } else {
        std::env::set_var("SAMYAMA_SCAN_FILTER_FUSION", "off");
    }
    let query = parse_query(cypher).expect("query should parse");
    let _ = QueryExecutor::new(store)
        .execute(&query)
        .expect("query should run");
    (0..runs)
        .map(|_| {
            let started = Instant::now();
            let _ = QueryExecutor::new(store)
                .execute(&query)
                .expect("query should run");
            started.elapsed().as_secs_f64() * 1000.0
        })
        .fold(f64::INFINITY, f64::min)
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
    let rows = arg("--rows").unwrap_or(1_000_000);
    let runs = arg("--runs").unwrap_or(5);

    eprintln!("Building {rows} rows…");
    let store = fixture(rows);

    let cases: &[(&str, &str)] = &[
        ("city = 'a' (10% pass)", "WHERE p.city = 'a'"),
        ("v < 10 (1% pass)", "WHERE p.v < 10"),
        ("v < 500 (50% pass)", "WHERE p.v < 500"),
        ("v >= 0 (all pass)", "WHERE p.v >= 0"),
        (
            "two conjuncts (5% pass)",
            "WHERE p.v < 500 AND p.city = 'b'",
        ),
    ];

    println!("{rows} rows, minimum of {runs}, interleaved.\n");
    println!(
        "{:<26} {:>12} {:>12} {:>10}",
        "predicate", "unfused", "fused", "reduction"
    );
    println!("{:-<26} {:->12} {:->12} {:->10}", "", "", "", "");
    for (label, where_clause) in cases {
        let cypher = format!("MATCH (p:P) {where_clause} RETURN count(*) AS c");
        let (mut fused, mut unfused) = (f64::INFINITY, f64::INFINITY);
        for _ in 0..2 {
            unfused = unfused.min(time(&store, &cypher, runs, false));
            fused = fused.min(time(&store, &cypher, runs, true));
        }
        let per_row = |ms: f64| ms * 1e6 / rows as f64;
        println!(
            "{:<26} {:>10.1}ns {:>10.1}ns {:>9.1}%",
            label,
            per_row(unfused),
            per_row(fused),
            (1.0 - fused / unfused) * 100.0,
        );
    }
    std::env::remove_var("SAMYAMA_SCAN_FILTER_FUSION");

    println!();
    println!("ns per scanned row for the whole query; `reduction` is what fusion saves on it.");
    bench_setup::report_drift(calibration);
}
