//! An aggregate over an expansion holds a bounded slice of it, not the rows it
//! counts (#2274).
//!
//! `MATCH (a:P)-[:R]->(b) RETURN count(b)` returns one row, and peaked at
//! 13.9 MB over 300,000 edges (`docs/QUERY-MEMORY.md`, finding 3): the
//! aggregate pulled its input 65,536 rows at a time, so the peak grew with the
//! row count until it reached that batch. It now pulls a few thousand at a
//! time.
//!
//! Memory is asserted as a **ratio** measured in this process, never as a
//! byte figure: the same query on a graph with ten times the edges must not
//! peak at anything like ten times the memory. Before the change it did.
//!
//! The result half checks that folding in smaller batches changes no answer:
//! every aggregate here is compared with the same figure computed in Rust from
//! the rows the expansion returns, over row counts that span several batches.
//!
//! Its own binary, because the counting allocator is global, and its tests one
//! at a time, because the counts are process-wide.

#[path = "../benches/common/peak_alloc.rs"]
mod peak_alloc;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use peak_alloc::measure;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::query::{BoundParams, QueryEngine, RecordBatch};

static SERIAL: Mutex<()> = Mutex::new(());

/// `nodes` `:P` nodes, each with `fanout` outgoing `:R` edges. Every
/// `null_every`-th node has no `i`, so the aggregates meet nulls; 0 means none
/// is missing.
fn graph(nodes: usize, fanout: usize, null_every: usize) -> GraphStore {
    let mut g = GraphStore::new();
    let ids: Vec<_> = (0..nodes)
        .map(|i| {
            let id = g.create_node("P");
            if null_every == 0 || i % null_every != 0 {
                g.set_node_property("default", id, "i", PropertyValue::Integer(i as i64))
                    .unwrap();
            }
            id
        })
        .collect();
    for k in 1..=fanout {
        for i in 0..nodes {
            g.create_edge(ids[i], ids[(i * k + 1) % nodes], "R")
                .unwrap();
        }
    }
    g
}

fn run(g: &GraphStore, q: &str) -> RecordBatch {
    QueryEngine::new()
        .execute_with_params_within(q, g, &BoundParams::new(), None)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn int(v: Option<&Value>) -> Option<i64> {
    match v {
        Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
        other => panic!("expected an integer or null, got {other:?}"),
    }
}

fn float(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Property(PropertyValue::Float(f))) => Some(*f),
        Some(Value::Property(PropertyValue::Integer(n))) => Some(*n as f64),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
        other => panic!("expected a number or null, got {other:?}"),
    }
}

/// The one row of a global aggregate.
fn single(g: &GraphStore, q: &str) -> samyama::query::executor::Record {
    let mut batch = run(g, q);
    assert_eq!(batch.records.len(), 1, "{q}");
    batch.records.remove(0)
}

/// The `v` column of every row the expansion returns, nulls included.
fn column(g: &GraphStore, q: &str) -> Vec<Option<i64>> {
    run(g, q).records.iter().map(|r| int(r.get("v"))).collect()
}

/// Fails unless the plan reaches `AggregateOperator` over an `Expand`. The
/// planner answers some counts from statistics or adjacency degrees instead,
/// and a test landing on one of those would prove nothing about the aggregate.
fn assert_aggregate_over_expand(g: &GraphStore, q: &str) {
    let query = parse_query(&format!("EXPLAIN {q}")).expect("query should parse");
    let batch = QueryExecutor::new(g)
        .execute(&query)
        .expect("EXPLAIN should run");
    let plan = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        plan.contains("Aggregate (") && plan.contains("Expand"),
        "{q} is not an aggregate over an expansion:\n{plan}"
    );
}

const HOPS: &[(&str, &str)] = &[
    ("MATCH (a:P)-[:R]->(c)", "1-hop"),
    ("MATCH (a:P)-[:R]->()-[:R]->(c)", "2-hop"),
    ("MATCH (a:P)-[:R]->()-[:R]->()-[:R]->(c)", "3-hop"),
];

#[test]
fn count_sum_min_max_avg_match_the_rows_they_fold() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // 6,000 1-hop rows, about 18,000 2-hop and 54,000 3-hop: one batch and
    // then several.
    let g = graph(2_000, 3, 10);
    for (pattern, name) in HOPS {
        let values = column(&g, &format!("{pattern} RETURN c.i AS v"));
        let present: Vec<i64> = values.iter().flatten().copied().collect();
        assert!(
            present.len() < values.len(),
            "{name}: the graph should give the aggregates nulls"
        );

        let q = format!(
            "{pattern} RETURN count(c) AS rows, count(*) AS star, count(c.i) AS n, \
             sum(c.i) AS s, min(c.i) AS lo, max(c.i) AS hi, avg(c.i) AS mean, \
             count(DISTINCT c.i) AS d"
        );
        assert_aggregate_over_expand(&g, &q);
        let r = single(&g, &q);
        assert_eq!(
            int(r.get("rows")),
            Some(values.len() as i64),
            "{name}: count(c)"
        );
        assert_eq!(
            int(r.get("star")),
            Some(values.len() as i64),
            "{name}: count(*)"
        );
        assert_eq!(
            int(r.get("n")),
            Some(present.len() as i64),
            "{name}: count skips nulls"
        );
        assert_eq!(
            int(r.get("s")),
            Some(present.iter().sum::<i64>()),
            "{name}: sum"
        );
        assert_eq!(
            int(r.get("lo")),
            present.iter().min().copied(),
            "{name}: min"
        );
        assert_eq!(
            int(r.get("hi")),
            present.iter().max().copied(),
            "{name}: max"
        );
        let mean = present.iter().sum::<i64>() as f64 / present.len() as f64;
        let got = float(r.get("mean")).expect("avg of a non-empty column");
        assert!(
            (got - mean).abs() < 1e-9 * mean.abs().max(1.0),
            "{name}: avg {got} != {mean}"
        );
        let distinct: BTreeSet<i64> = present.iter().copied().collect();
        assert_eq!(
            int(r.get("d")),
            Some(distinct.len() as i64),
            "{name}: count(DISTINCT)"
        );
    }
}

#[test]
fn grouped_aggregates_match_the_rows_they_fold() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph(2_000, 3, 10);
    let pattern = "MATCH (a:P)-[:R]->()-[:R]->(c)";

    // An expression key, a null group among them, read the general way.
    let mut want: BTreeMap<Option<i64>, (i64, i64)> = BTreeMap::new();
    for v in column(&g, &format!("{pattern} RETURN c.i AS v")) {
        let e = want.entry(v.map(|x| x % 7)).or_default();
        e.0 += 1;
        e.1 += v.unwrap_or(0);
    }
    let q = format!("{pattern} RETURN c.i % 7 AS k, count(c) AS n, sum(c.i) AS s");
    assert_aggregate_over_expand(&g, &q);
    let got: BTreeMap<Option<i64>, (i64, i64)> = run(&g, &q)
        .records
        .iter()
        .map(|r| {
            (
                int(r.get("k")),
                (int(r.get("n")).unwrap(), int(r.get("s")).unwrap_or(0)),
            )
        })
        .collect();
    assert_eq!(got, want, "grouped on an expression");

    // Keys that are properties of one node: grouped on its identity, then
    // merged. Two nodes with no `i` share the null group.
    let mut want: BTreeMap<Option<i64>, i64> = BTreeMap::new();
    for v in column(&g, &format!("{pattern} RETURN a.i AS v")) {
        *want.entry(v).or_default() += 1;
    }
    let q = format!("{pattern} RETURN a.i AS k, count(c) AS n, max(c.i) AS m");
    assert_aggregate_over_expand(&g, &q);
    let got: BTreeMap<Option<i64>, i64> = run(&g, &q)
        .records
        .iter()
        .map(|r| (int(r.get("k")), int(r.get("n")).unwrap()))
        .collect();
    assert_eq!(got, want, "grouped on a node's property");
}

#[test]
fn an_aggregate_after_with_and_over_an_optional_match_is_unchanged() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph(2_000, 3, 10);
    let rows = column(&g, "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN c.i AS v");

    let r = single(
        &g,
        "MATCH (a:P)-[:R]->()-[:R]->(c) WITH count(c) AS n, sum(c.i) AS s RETURN n, s",
    );
    assert_eq!(int(r.get("n")), Some(rows.len() as i64), "WITH count");
    assert_eq!(
        int(r.get("s")),
        Some(rows.iter().flatten().sum::<i64>()),
        "WITH sum"
    );

    // An unmatched optional row binds `x` to null: counted by count(*), not
    // by count(x).
    let r = single(
        &g,
        "MATCH (a:P) OPTIONAL MATCH (a)-[:S]->(x) RETURN count(x) AS n, count(*) AS star",
    );
    assert_eq!(
        int(r.get("n")),
        Some(0),
        "count of an all-null optional variable"
    );
    assert_eq!(
        int(r.get("star")),
        Some(2_000),
        "count(*) keeps the unmatched rows"
    );

    // No rows at all: count and sum are 0, min and avg are null.
    let r = single(&g, "MATCH (a:P)-[:S]->(c) RETURN count(c) AS n, sum(c.i) AS s, min(c.i) AS lo, avg(c.i) AS mean");
    assert_eq!(int(r.get("n")), Some(0));
    assert_eq!(int(r.get("s")), Some(0), "sum of nothing is 0");
    assert_eq!(int(r.get("lo")), None);
    assert_eq!(float(r.get("mean")), None);
}

/// Peak bytes of the query's second run on `g`, and its one integer result.
fn peak_of(g: &GraphStore, q: &str, col: &str) -> (isize, i64) {
    let e = QueryEngine::new();
    let none = BoundParams::new();
    // Once to warm what is meant to be kept: the parse cache, statistics.
    let _ = e.execute_with_params_within(q, g, &none, None);
    let (cost, rows) = measure(|| e.execute_with_params_within(q, g, &none, None));
    assert_eq!(rows, Ok(1), "{q}");
    let out = e.execute_with_params_within(q, g, &none, None).unwrap();
    (cost.peak_bytes, int(out.records[0].get(col)).unwrap())
}

#[test]
fn an_aggregate_peaks_at_a_batch_not_at_the_rows_it_counts() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let small = graph(1_000, 3, 0);
    let large = graph(10_000, 3, 0);

    let mut queries: Vec<(String, &str)> = HOPS
        .iter()
        .map(|(pattern, _)| (format!("{pattern} RETURN count(c) AS n"), "n"))
        .collect();
    // Not a bare count, so every row is read rather than tallied by the batch.
    queries.push((
        "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN count(c) AS n, sum(c.i) AS s, \
         min(c.i) AS lo, max(c.i) AS hi, avg(c.i) AS m"
            .to_string(),
        "n",
    ));
    // Grouped, on a key with a fixed number of groups.
    queries.push((
        "MATCH (a:P)-[:R]->()-[:R]->(c) WITH c.i % 5 AS k, count(c) AS n RETURN sum(n) AS n"
            .to_string(),
        "n",
    ));

    for (q, col) in &queries {
        if !q.starts_with("MATCH (a:P)-[:R]->()-[:R]->(c) WITH") {
            assert_aggregate_over_expand(&large, q);
        }
        let (small_peak, small_rows) = peak_of(&small, q, col);
        let (large_peak, large_rows) = peak_of(&large, q, col);
        let work = large_rows as f64 / small_rows as f64;
        let memory = large_peak as f64 / small_peak as f64;
        // The baseline: ten times the rows were folded.
        assert!(
            work >= 9.0,
            "{q}: {small_rows} -> {large_rows} rows, the large graph should fold ~10x"
        );
        // Before #2274 this was ~10x as well: the whole expansion up to
        // 65,536 rows was held at once. The label scan's node ids still grow
        // with the graph, so not 1x either.
        assert!(
            memory < 3.0,
            "{q}: {work:.1}x the rows peaked at {memory:.2}x the memory \
                 ({small_peak} -> {large_peak} bytes); the aggregate is holding its input"
        );
    }
}
