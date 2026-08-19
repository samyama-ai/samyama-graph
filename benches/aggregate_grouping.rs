//! What a hash group-by costs per input row (#521).
//!
//! `CH-PROFILE-01` put **60.1% of LDBC IC5 in `Aggregate`** — 1,947 ms to fold
//! 1,678,980 input rows into 96,862 groups, or ~1.16 µs per input row. On the
//! same run the `Expand` that *produced* those rows cost 0.55 µs each, so the
//! aggregate cost more than twice as much per row as the traversal, while doing
//! strictly less work per row. A hash aggregation over an integer key should be
//! in the tens of nanoseconds.
//!
//! IC5's group key is `(forum.id, forum.title)` — two properties of one node.
//! That shape is the point of this bench, because it is where the three costs
//! separate:
//!
//!   * **one key vs two.** A single key hashes a `Value`; two keys allocate a
//!     `Vec<Value>` per input row.
//!   * **integer vs string.** A string key clones the string per row.
//!   * **groups vs rows.** Resolving a property per *row* rather than per
//!     *group* is a 17× multiplier at IC5's ratio, and the ratio is the whole
//!     question.
//!
//! So the sweep varies the key and holds the row count fixed, and reports
//! ns/row against a `count(*)` with no grouping at all as the floor.
//!
//!   cargo bench --bench aggregate_grouping
//!   cargo bench --bench aggregate_grouping -- --rows 2000000 --groups 100000

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// `rows` Item nodes over `groups` distinct Forum nodes, each Item pointing at
/// one Forum. Aggregating over `(f)<-[:IN]-(i)` then reproduces IC5's shape:
/// many rows, far fewer groups, key drawn from the low-cardinality side.
fn fixture(rows: usize, groups: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let forums: Vec<_> = (0..groups)
        .map(|g| {
            let id = store.create_node("Forum");
            let _ = store.set_node_property(
                "default",
                id,
                "fid".to_string(),
                PropertyValue::Integer(g as i64),
            );
            // Long enough that cloning it per row is visible, as LDBC forum
            // titles are ("Wall of …", "Album N of …").
            let _ = store.set_node_property(
                "default",
                id,
                "title".to_string(),
                PropertyValue::String(format!("Wall of Person Number {g} With A Realistic Length")),
            );
            id
        })
        .collect();

    for i in 0..rows {
        let item = store.create_node("Item");
        let _ = store.set_node_property(
            "default",
            item,
            "v".to_string(),
            PropertyValue::Integer((i % 977) as i64),
        );
        store.create_edge(item, forums[i % groups], "IN").unwrap();
    }
    store
}

/// The top operator that actually folds the rows, from EXPLAIN.
///
/// Printed per case because of a trap worth naming: the planner rewrites
/// `RETURN f.x, count(i)` over an expand into `AdjacencyCountAggregate`, which
/// reads degrees off the adjacency index and never groups anything. The first
/// version of this bench reported 88 ns/row for the IC5 shape and looked like
/// #521 was wrong -- it was measuring the rewrite. Any case whose operator is
/// not `Aggregate` says nothing about hash aggregation.
fn fold_operator(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("EXPLAIN should run");
    let text = match batch.records[0].get("plan") {
        Some(samyama::query::executor::Value::Property(PropertyValue::String(t))) => t.clone(),
        _ => return "?".into(),
    };
    for name in ["AdjacencyCountAggregate", "Aggregate"] {
        if text.contains(name) {
            return name.into();
        }
    }
    "none".into()
}

/// Median of `runs` timings, in ms. Median rather than min because the question
/// is the typical cost, and rather than mean because one page fault should not
/// move it.
fn time(store: &GraphStore, cypher: &str, runs: usize) -> (usize, f64) {
    let query = parse_query(cypher).expect("query should parse");
    // Warm: the first execution pays for statistics the rest skip.
    let warm = QueryExecutor::new(store).execute(&query).expect("query should run");

    let mut samples: Vec<f64> = (0..runs)
        .map(|_| {
            let started = Instant::now();
            let _ = QueryExecutor::new(store).execute(&query).expect("query should run");
            started.elapsed().as_secs_f64() * 1000.0
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (warm.records.len(), samples[samples.len() / 2])
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok())
    };
    let rows = arg("--rows").unwrap_or(1_000_000);
    let groups = arg("--groups").unwrap_or(50_000);
    let runs = arg("--runs").unwrap_or(5);

    eprintln!("Building {rows} rows over {groups} groups…");
    let started = Instant::now();
    let store = fixture(rows, groups);
    eprintln!("built in {:.1}s\n", started.elapsed().as_secs_f64());

    // Every case folds the same `rows` input rows. Only the key differs.
    let cases: Vec<(&str, String)> = vec![
        (
            "no grouping (floor)",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN count(i) AS c".into(),
        ),
        (
            "1 key, node identity",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f, count(i) AS c".into(),
        ),
        (
            "1 key, int property",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f.fid, count(i) AS c".into(),
        ),
        (
            "1 key, string property",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f.title, count(i) AS c".into(),
        ),
        (
            "2 keys, int + string (IC5)",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f.fid, f.title, count(i) AS c".into(),
        ),
        (
            "1 key int, + sum",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f.fid, sum(i.v) AS s".into(),
        ),
        (
            "1 key string, + sum",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f.title, sum(i.v) AS s".into(),
        ),
        (
            "2 keys + sum, not count",
            "MATCH (f:Forum)<-[:IN]-(i:Item) RETURN f.fid, f.title, sum(i.v) AS s".into(),
        ),
    ];

    println!("{rows} input rows, {groups} groups, median of {runs}\n");
    println!(
        "{:<28} {:>8} {:>12} {:>14}  {}",
        "case", "groups", "median ms", "ns per row", "operator"
    );
    println!("{:-<28} {:->8} {:->12} {:->14}  {:-<24}", "", "", "", "", "");

    for (label, cypher) in &cases {
        let (out_rows, ms) = time(&store, cypher, runs);
        println!(
            "{:<28} {:>8} {:>12.1} {:>14.0}  {}",
            label,
            out_rows,
            ms,
            ms * 1e6 / rows as f64,
            fold_operator(&store, cypher),
        );
    }

    println!();
    println!("The `no grouping` row is the floor: same input, same traversal, no hash map.");
    println!("Subtracting it from the others isolates the grouping cost itself.");
    println!();
    println!("Read the operator column first. Cases folded by `AdjacencyCountAggregate` never");
    println!("built a group and say nothing about hash aggregation -- they are kept because");
    println!("the rewrite is real and worth seeing, not because they measure grouping.");
    println!();
    println!("LDBC IC5 measured 1,947 ms over 1,678,980 input rows into 96,862 groups on the");
    println!("`2 keys, int + string` shape -- ~1,160 ns per row (#521).");

    bench_setup::report_drift(calibration);
}
