//! Hash-aggregation throughput, in nanoseconds per input row (#521).
//!
//! `CH-PROFILE-01` put **60.1% of LDBC IC5 in `Aggregate`** — 1,947 ms to fold
//! 1,678,980 rows into 96,862 groups, about 1.16 µs per input row. For
//! contrast the `Expand` that produced those rows managed 0.55 µs each, so the
//! aggregate cost more than twice as much per row as the traversal, while
//! doing strictly less work per row.
//!
//! That is a constant, not an algorithm, and a constant needs a reproducer
//! that does not require a 21M-edge dataset to observe. This isolates the four
//! shapes the operator actually implements, so the next change has a number to
//! move rather than a hypothesis to act on:
//!
//! | shape | path in `AggregateOperator` |
//! |---|---|
//! | no `GROUP BY` | `execute_all_no_group` — no map at all |
//! | one key | `execute_all_single_key` — `FxHashMap<Value, _>` |
//! | two integer keys | `execute_all` — `FxHashMap<Vec<Value>, _>` |
//! | integer + string keys | `execute_all` — IC5's shape |
//!
//! The differences between rows of that table are the whole point. The gap
//! between one key and two says what the per-row `Vec` costs; the gap between
//! two integers and integer-plus-string says what resolving and cloning a
//! `String` property per row costs; and the gap between "no group by" and "one
//! key" says what the hashing itself costs.
//!
//!   cargo bench --bench aggregate_throughput
//!   cargo bench --bench aggregate_throughput -- --rows 2000000 --groups 100000

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// `rows` nodes spread over `groups` distinct values, which is the shape that
/// matters: a group-by whose every row is its own group measures allocation,
/// and one with a single group measures nothing.
fn fixture(rows: usize, groups: usize) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..rows {
        let id = store.create_node("Row");
        let group = (i % groups) as i64;
        store.set_column_property(id, "gid", PropertyValue::Integer(group));
        // A second integer key **functionally determined by the first**, so
        // the two-key shape produces exactly as many groups as the one-key
        // shape. An independent second key would multiply the group count and
        // the comparison would be measuring map growth rather than the cost of
        // building a composite key -- which is what a first draft of this
        // bench did, reporting 500,000 groups against 25,000 and a 3.8x
        // difference that was mostly the extra groups.
        store.set_column_property(id, "gid2", PropertyValue::Integer(group * 2));
        // A string key of a realistic width -- LDBC's `forum.title` is a short
        // phrase, not a single character, and the clone cost scales with it.
        store.set_column_property(
            id,
            "gname",
            PropertyValue::String(format!("group-{group:08}")),
        );
        store.set_column_property(id, "weight", PropertyValue::Integer((i % 97) as i64));
    }
    store
}

/// Exclusive time in the `Aggregate` operator, from `PROFILE` (#517).
///
/// Timing the whole query would fold in the scan that feeds it, which on these
/// shapes is a large and varying share. The per-operator attribution exists
/// precisely so a constant like this can be isolated.
fn aggregate_self_ms(store: &GraphStore, cypher: &str) -> Option<f64> {
    let query = parse_query(&format!("PROFILE {cypher}")).ok()?;
    let batch = QueryExecutor::new(store).execute(&query).ok()?;
    let text = match batch.records.first()?.get("plan")? {
        samyama::query::executor::Value::Property(PropertyValue::String(t)) => t.clone(),
        _ => return None,
    };
    // The ranked section prints `  1. Aggregate   1946.54ms   60.1%  ...`.
    text.lines()
        .skip_while(|l| !l.contains("Hottest operators"))
        .find(|l| l.contains("Aggregate"))
        .and_then(|l| l.split_whitespace().find(|w| w.ends_with("ms")))
        .and_then(|w| w.trim_end_matches("ms").parse().ok())
}

fn time_query(store: &GraphStore, cypher: &str, rows: usize) -> (f64, usize) {
    let query = parse_query(cypher).expect("query should parse");
    // Warm once: the first run pays for statistics and any lazy index work,
    // which is not what this is measuring.
    let _ = QueryExecutor::new(store).execute(&query).expect("query should run");

    let started = Instant::now();
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    let elapsed = started.elapsed();

    let ns_per_row = elapsed.as_secs_f64() * 1e9 / rows as f64;
    (ns_per_row, batch.records.len())
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
    let groups = arg("--groups").unwrap_or(50_000);

    eprintln!("Building {rows} rows over {groups} groups…");
    let build_start = Instant::now();
    let store = fixture(rows, groups);
    eprintln!("built in {:.1}s\n", build_start.elapsed().as_secs_f64());

    let cases: Vec<(&str, &str)> = vec![
        (
            "no GROUP BY, scanning (global sum)",
            "MATCH (r:Row) RETURN sum(r.weight) AS s",
        ),
        (
            "one integer key",
            "MATCH (r:Row) RETURN r.gid AS g, count(r) AS c",
        ),
        (
            "two integer keys, same group count",
            "MATCH (r:Row) RETURN r.gid AS g, r.gid2 AS g2, count(r) AS c",
        ),
        (
            "integer + string keys (IC5's shape)",
            "MATCH (r:Row) RETURN r.gid AS g, r.gname AS n, count(r) AS c",
        ),
        (
            "one integer key, sum over a property",
            "MATCH (r:Row) RETURN r.gid AS g, sum(r.weight) AS s",
        ),
    ];

    println!(
        "{:<42} {:>10} {:>12} {:>10} {:>10}",
        "shape", "ns/row", "agg ns/row", "query ms", "groups"
    );
    println!("{:-<42} {:->10} {:->12} {:->10} {:->10}", "", "", "", "", "");
    for (name, cypher) in &cases {
        let (ns_per_row, out_rows) = time_query(&store, cypher, rows);
        // `agg ns/row` is the operator alone; `ns/row` is the whole query,
        // scan included. The gap between them is what the aggregate is *not*
        // responsible for.
        let agg = aggregate_self_ms(&store, cypher)
            .map(|ms| ms * 1e6 / rows as f64)
            .unwrap_or(f64::NAN);
        println!(
            "{:<42} {:>10.1} {:>12.1} {:>10.0} {:>10}",
            name,
            ns_per_row,
            agg,
            ns_per_row * rows as f64 / 1e6,
            out_rows
        );
    }

    println!();
    println!("Read the differences, not the absolutes:");
    println!("  one key   - no group by  = hashing and the map");
    println!("  two keys  - one key      = the per-row Vec<Value> the general path builds");
    println!("  int+str   - two ints     = resolving and cloning a String property per row");
    println!("  count     - sum          = evaluating the aggregate's argument per row");
    println!();
    println!("LDBC IC5 measured 1,160 ns/row over 1,678,980 rows into 96,862 groups (#521),");
    println!("against an Expand feeding it at 550 ns/row.");

    bench_setup::report_drift(calibration);
}
