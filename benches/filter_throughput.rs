//! What a `WHERE` costs per row, by predicate shape (#559).
//!
//! `FilterOperator` filters a batch across threads when the predicate is
//! expensive enough to pay for moving records between cores. It used to make
//! that decision from the **batch size** — which says nothing about predicate
//! cost, and with a batch size of 65,536 meant always. Measured, that lost
//! 1.4-1.8× on every predicate a real query writes.
//!
//! The reason is worth stating because it is not obvious: a `Record` holds
//! `Arc<str>` binding names, so moving records across threads churns atomic
//! refcounts on cache lines every thread shares. Against a predicate as cheap
//! as one comparison there is nothing to amortise that against.
//!
//! This sweeps predicate shapes at a fixed row count and runs **both sides of
//! the threshold, interleaved in one process**, reporting ns per input row over
//! a no-filter baseline so the number is the filter's own cost.
//!
//! Interleaving is not fussiness. The effect is a ratio between per-row work
//! and cross-core coordination, so it moves with the host; comparing two
//! separate benchmark runs on an otherwise idle dedicated box measured 16%
//! drift, enough to invert the ordering of adjacent cases (#529). Both settings
//! must see the same host, in the same run, alternating.
//!
//!   cargo bench --bench filter_throughput
//!   cargo bench --bench filter_throughput -- --rows 2000000

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// One hub with `rows` items pointing at it, so a single expand produces every
/// row and the filter is the only thing that varies.
fn fixture(rows: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let hub = store.create_node("Forum");
    let _ = store.set_node_property("default", hub, "fid", PropertyValue::Integer(0));
    for i in 0..rows {
        let item = store.create_node("Item");
        let _ = store.set_node_property("default", item, "v", PropertyValue::Integer((i % 977) as i64));
        let _ = store.set_node_property("default", item, "w", PropertyValue::Integer((i % 13) as i64));
        let _ = store.set_node_property(
            "default",
            item,
            "name",
            PropertyValue::String(format!("item-number-{i}-with-some-length")),
        );
        store.create_edge(item, hub, "IN").unwrap();
    }
    store
}

/// Minimum of `runs`, in ms, at a given threshold setting.
///
/// Minimum rather than median because the question is what the work costs, and
/// every sample above the floor is something else the host was doing.
fn time(store: &GraphStore, cypher: &str, runs: usize, threshold: &str) -> f64 {
    std::env::set_var("SAMYAMA_FILTER_PARALLEL_COST", threshold);
    let query = parse_query(cypher).expect("query should parse");
    let _ = QueryExecutor::new(store).execute(&query).expect("query should run");
    (0..runs)
        .map(|_| {
            let started = Instant::now();
            let _ = QueryExecutor::new(store).execute(&query).expect("query should run");
            started.elapsed().as_secs_f64() * 1000.0
        })
        .fold(f64::INFINITY, f64::min)
}

/// Force parallel / force sequential, whatever the shipped threshold is.
const ALWAYS: &str = "0";
const NEVER: &str = "4000000000";

/// The cost the planner computes, recovered for display by bisecting the
/// threshold. Cheaper than exposing the function itself, and it cannot drift
/// from what the planner actually uses.
fn predicate_cost_of(cypher: &str) -> u32 {
    use samyama::query::executor::operator::FilterOperator;
    let predicate = parse_query(cypher).unwrap().where_clause.unwrap().predicate;
    let (mut low, mut high) = (0u32, 1_000u32);
    while low < high {
        let mid = (low + high + 1) / 2;
        std::env::set_var("SAMYAMA_FILTER_PARALLEL_COST", mid.to_string());
        if FilterOperator::predicate_is_parallel(&predicate) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    std::env::remove_var("SAMYAMA_FILTER_PARALLEL_COST");
    low
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok())
    };
    let rows = arg("--rows").unwrap_or(1_000_000);
    let runs = arg("--runs").unwrap_or(3);

    eprintln!("Building {rows} rows…");
    let started = Instant::now();
    let store = fixture(rows);
    eprintln!("built in {:.1}s\n", started.elapsed().as_secs_f64());

    let base_query = "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN count(i) AS c";
    let baseline = time(&store, base_query, runs, NEVER);

    let cases: &[(&str, &str)] = &[
        ("nothing to evaluate", "WHERE 1 = 1"),
        ("one int compare", "WHERE i.v > 500"),
        ("two int compares", "WHERE i.v > 500 AND i.w > 5"),
        ("one string CONTAINS", "WHERE i.name CONTAINS \"99\""),
        ("a call, then CONTAINS", "WHERE toUpper(i.name) CONTAINS \"99\""),
        (
            "four conjuncts, two calls",
            "WHERE i.v > 100 AND i.w > 2 AND i.name CONTAINS \"9\" AND toUpper(i.name) CONTAINS \"ITEM\"",
        ),
    ];

    println!("{rows} rows, minimum of {runs}, interleaved. Baseline (no WHERE): {baseline:.1} ms\n");
    println!(
        "{:<28} {:>7} {:>12} {:>12} {:>9}  {}",
        "predicate", "cost", "parallel", "sequential", "ratio", "ships as"
    );
    println!("{:-<28} {:->7} {:->12} {:->12} {:->9}  {:-<10}", "", "", "", "", "", "");

    for (label, where_clause) in cases {
        let cypher = format!("MATCH (f:Forum)<-[:IN]-(i:Item) {where_clause} RETURN count(i) AS c");
        let predicate = parse_query(&cypher).unwrap().where_clause.unwrap().predicate;

        // Alternate, so a host that drifts during the run drifts through both.
        let mut par = f64::INFINITY;
        let mut seq = f64::INFINITY;
        for _ in 0..2 {
            par = par.min(time(&store, &cypher, runs, ALWAYS));
            seq = seq.min(time(&store, &cypher, runs, NEVER));
        }
        let par_ns = (par - baseline).max(0.0) * 1e6 / rows as f64;
        let seq_ns = (seq - baseline).max(0.0) * 1e6 / rows as f64;

        std::env::remove_var("SAMYAMA_FILTER_PARALLEL_COST");
        let ships_parallel =
            samyama::query::executor::operator::FilterOperator::predicate_is_parallel(&predicate);

        // A ratio between two numbers that are both in the noise says nothing.
        // `WHERE 1 = 1` costs a handful of nanoseconds either way, and printing
        // "0.05x" for it invites someone to go fix a threshold that is right.
        let ratio = if par_ns.max(seq_ns) < 20.0 {
            "     -".to_string()
        } else if ships_parallel {
            format!("{:>5.2}x", seq_ns / par_ns.max(0.01))
        } else {
            format!("{:>5.2}x", par_ns / seq_ns.max(0.01))
        };

        println!(
            "{:<28} {:>7} {:>11.0}n {:>11.0}n {:>9}  {}",
            label,
            predicate_cost_of(&cypher),
            par_ns,
            seq_ns,
            ratio,
            if ships_parallel { "parallel" } else { "sequential" },
        );
    }

    println!();
    println!("`ns` columns are over the no-filter baseline, so they are the filter's own cost.");
    println!("`ratio` is how much the shipped choice beats the other one, and is omitted where");
    println!("both sides are in the noise. Above 1.00x means");
    println!("the threshold picked the faster path for that shape. A shape whose ratio drops");
    println!("below 1.00x is the threshold being wrong on this host, and is the signal to");
    println!("re-fit it; tests/filter_parallel_threshold.rs pins the classification itself.");

    bench_setup::report_drift(calibration);
}
