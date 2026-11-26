//! A query gives back what it allocated (#2084).
//!
//! Checked in bytes, with a counting allocator, so the result does not depend
//! on the machine: after a query that succeeds, fails part way, or runs out of
//! time, and after the same query a hundred times, the heap is where it was.
//! The figures for how much a query peaks at are the benchmark's
//! (`benches/query_memory.rs`); this is the part that can be a gate.
//!
//! Its own binary, because the allocator is global, and its tests one at a
//! time, because the counts are process-wide.

#[path = "../benches/common/peak_alloc.rs"]
mod peak_alloc;

use std::sync::Mutex;
use std::time::Duration;

use peak_alloc::{measure, Cost};
use samyama::graph::GraphStore;
use samyama::query::{BoundParams, QueryEngine};

static SERIAL: Mutex<()> = Mutex::new(());

/// A query's working set is in kilobytes to megabytes on this graph; what it
/// may leave behind is far below that. Bytes, not a ratio, so a leak of one
/// row per run is visible.
const RETAINED_TOLERANCE: isize = 16 * 1024;

/// `nodes` labelled nodes with two properties, each with `fanout` outgoing
/// edges. Built through the store, not Cypher: a join building the edges
/// would be quadratic, and the test is about the queries, not the build.
fn graph(nodes: usize, fanout: usize) -> GraphStore {
    use samyama::graph::PropertyValue;
    let mut g = GraphStore::new();
    let ids: Vec<_> = (0..nodes)
        .map(|i| {
            let id = g.create_node("P");
            g.set_node_property("default", id, "i", PropertyValue::Integer(i as i64)).unwrap();
            g.set_node_property("default", id, "name", PropertyValue::String(format!("n{i}"))).unwrap();
            id
        })
        .collect();
    for k in 1..=fanout {
        for i in 0..nodes {
            g.create_edge(ids[i], ids[(i * k + 1) % nodes], "R").unwrap();
        }
    }
    g
}

fn run(e: &QueryEngine, g: &GraphStore, q: &str, limit: Option<Duration>) -> (Cost, Result<usize, ()>) {
    measure(|| e.execute_with_params_within(q, g, &BoundParams::new(), limit))
}

const QUERIES: &[&str] = &[
    "MATCH (a:P)-[:R]->(b) RETURN count(b) AS c",
    "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN count(c) AS c",
    "MATCH (a:P)-[:R]->(b) RETURN a.i AS a, collect(b.i) AS bs",
    "MATCH (a:P) RETURN a.name AS n ORDER BY n DESC",
    "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN DISTINCT c.i AS i",
    "MATCH (a:P)-[:R]->(b) RETURN a.i AS a, b.i AS b",
];

#[test]
fn a_query_that_succeeds_gives_back_what_it_allocated() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph(2_000, 3);
    let e = QueryEngine::new();
    for q in QUERIES {
        // Once to warm what is meant to be kept: the parse cache, statistics.
        let _ = run(&e, &g, q, None);
        let (cost, rows) = run(&e, &g, q, None);
        assert!(rows.is_ok_and(|n| n > 0), "{q}: {rows:?}");
        assert!(cost.peak_bytes > 0, "{q}: the measurement saw nothing");
        assert!(cost.retained_bytes <= RETAINED_TOLERANCE,
                "{q}: kept {} bytes after its result was dropped ({cost:?})", cost.retained_bytes);
    }
}

#[test]
fn a_query_that_fails_part_way_gives_back_what_it_allocated() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph(2_000, 3);
    let e = QueryEngine::new();
    // Fails after the expand has produced rows: division by zero per row.
    let q = "MATCH (a:P)-[:R]->(b) RETURN a.i / (b.i - b.i) AS x";
    let _ = run(&e, &g, q, None);
    let (cost, out) = run(&e, &g, q, None);
    assert!(out.is_err(), "the query should fail");
    assert!(cost.retained_bytes <= RETAINED_TOLERANCE,
            "a failed query kept {} bytes ({cost:?})", cost.retained_bytes);
}

#[test]
fn a_query_that_runs_out_of_time_gives_back_what_it_allocated() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph(2_000, 3);
    let e = QueryEngine::new();
    // Three hops and a sort over them: far longer than a millisecond in any build.
    let q = "MATCH (a:P)-[:R]->()-[:R]->()-[:R]->(d) RETURN d.name AS n ORDER BY n";
    // Warmed with the same statement: its first run adds a parse-cache entry,
    // which is kept on purpose (measured: 172 KB once, then 0 per run).
    let _ = run(&e, &g, q, Some(Duration::from_millis(1)));
    let (cost, out) = run(&e, &g, q, Some(Duration::from_millis(1)));
    assert!(out.is_err(), "the deadline should stop it");
    assert!(cost.retained_bytes <= RETAINED_TOLERANCE,
            "a timed-out query kept {} bytes ({cost:?})", cost.retained_bytes);
}

#[test]
fn running_a_query_a_hundred_times_does_not_grow_the_heap() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph(1_000, 3);
    let e = QueryEngine::new();
    for q in [QUERIES[1], QUERIES[2], QUERIES[4]] {
        let _ = run(&e, &g, q, None);
        let start = peak_alloc::live();
        for _ in 0..100 {
            let (_, rows) = run(&e, &g, q, None);
            assert!(rows.is_ok(), "{q}");
        }
        let grew = peak_alloc::live() - start;
        assert!(grew <= RETAINED_TOLERANCE,
                "{q}: the heap grew {grew} bytes over 100 runs, {} per run", grew / 100);
    }
}
