//! A query's time limit bounds its memory as well as its time (#2273).
//!
//! `docs/QUERY-MEMORY.md`, finding 2: a query given a 1 ms limit reached
//! exactly the peak it reached with no limit, and was stopped only then. The
//! operators that hold their whole input (sort, aggregation, `WITH`) pulled it
//! in 65,536-row batches and looked at the deadline every ten of them, so the
//! intermediate was built before anything could refuse it.
//!
//! Asserted as a ratio of two peaks measured in this process -- a query under
//! a limit that has already passed against the same query with none -- with
//! the counting allocator from #2271. No wall-clock bound: a limit of one
//! nanosecond has passed by the time the first batch is built on any machine
//! and in any build profile, so what is measured is how much a stopped query
//! got to allocate, not how fast it ran.
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

/// Nodes in the graph: ten of the 4,096-row batches the materialising
/// operators now pull, and fewer than one of the 65,536-row batches they used
/// to. Under the old batch a stopped query had always built its whole input,
/// so the ratios below were 1.0.
const NODES: usize = 40_000;

/// A limit that has passed before the first batch is complete.
const EXPIRED: Duration = Duration::from_nanos(1);

/// The stopped query may hold at most this fraction of the unbounded peak.
/// Measured expectation is about one batch in ten; half leaves room for the
/// scan's node list and the plan, which both runs share.
const MAX_RATIO: f64 = 0.5;

/// `NODES` labelled nodes with two properties, each with three outgoing
/// edges. Built through the store, as `query_memory_released.rs` does.
fn graph() -> GraphStore {
    use samyama::graph::PropertyValue;
    let mut g = GraphStore::new();
    let ids: Vec<_> = (0..NODES)
        .map(|i| {
            let id = g.create_node("P");
            g.set_node_property("default", id, "i", PropertyValue::Integer(i as i64))
                .unwrap();
            g.set_node_property(
                "default",
                id,
                "name",
                PropertyValue::String(format!("n{i}")),
            )
            .unwrap();
            id
        })
        .collect();
    for k in 1..=3 {
        for i in 0..NODES {
            g.create_edge(ids[i], ids[(i * k + 1) % NODES], "R")
                .unwrap();
        }
    }
    g
}

fn run(
    e: &QueryEngine,
    g: &GraphStore,
    q: &str,
    limit: Option<Duration>,
) -> (Cost, Result<usize, ()>) {
    measure(|| e.execute_with_params_within(q, g, &BoundParams::new(), limit))
}

/// The peak of `q` with no limit and under an expired one, after checking that
/// the limited run was stopped by its deadline and not by something else.
fn peaks(q: &str) -> (isize, isize) {
    let g = graph();
    let e = QueryEngine::new();
    // Warm the parse cache, which keeps its entry on purpose.
    let _ = run(&e, &g, q, None);

    let (free, rows) = run(&e, &g, q, None);
    assert!(
        rows.is_ok_and(|n| n > 0),
        "{q}: the unbounded run must succeed: {rows:?}"
    );

    let err = e
        .execute_with_params_within(q, &g, &BoundParams::new(), Some(EXPIRED))
        .expect_err("a query under an expired limit must be stopped");
    assert!(
        err.to_string().contains("timed out"),
        "{q}: stopped, but not by the deadline: {err}"
    );

    let (cut, out) = run(&e, &g, q, Some(EXPIRED));
    assert!(out.is_err(), "{q}: the limited run must be stopped");
    (free.peak_bytes, cut.peak_bytes)
}

fn assert_bounded(q: &str) {
    let (free, cut) = peaks(q);
    assert!(free > 0, "{q}: the measurement saw nothing");
    let ratio = cut as f64 / free as f64;
    assert!(
        ratio < MAX_RATIO,
        "{q}: stopped by its time limit at a peak of {cut} bytes, {ratio:.2} of the \
         {free} bytes it peaks at with no limit -- the limit stopped it after it had \
         built its intermediate, not while building it"
    );
}

/// `ORDER BY` over every row: the sort buffers its whole input before it
/// orders any of it, which is the 59 MB of the issue.
#[test]
fn a_time_limit_stops_a_sort_before_its_peak() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    assert_bounded("MATCH (a:P) RETURN a.name AS n ORDER BY n DESC");
}

/// Aggregation state that grows with its input: one list per group.
#[test]
fn a_time_limit_stops_an_aggregation_before_its_peak() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    assert_bounded("MATCH (a:P)-[:R]->(b) RETURN a.i AS a, collect(b.i) AS bs");
}

/// A `WITH` barrier holds its whole input too.
#[test]
fn a_time_limit_stops_a_with_barrier_before_its_peak() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    assert_bounded("MATCH (a:P)-[:R]->(b) WITH a.i AS x, b.name AS y ORDER BY y RETURN x, y");
}

/// The converse: checking the deadline more often must not stop a query whose
/// limit has not passed, nor change what it returns.
#[test]
fn a_limit_that_has_not_passed_changes_nothing() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let g = graph();
    let e = QueryEngine::new();
    let hour = Some(Duration::from_secs(3600));
    for q in [
        "MATCH (a:P) RETURN a.name AS n ORDER BY n DESC",
        "MATCH (a:P)-[:R]->(b) RETURN a.i AS a, collect(b.i) AS bs ORDER BY a",
        "MATCH (a:P)-[:R]->()-[:R]->(c) RETURN count(c) AS c",
    ] {
        let free = e
            .execute_with_params_within(q, &g, &BoundParams::new(), None)
            .expect(q);
        let limited = e
            .execute_with_params_within(q, &g, &BoundParams::new(), hour)
            .expect(q);
        assert_eq!(free.records.len(), limited.records.len(), "{q}");
        // `Record` has no `PartialEq`; its debug form names every binding.
        assert_eq!(
            format!("{:?}", free.records),
            format!("{:?}", limited.records),
            "{q}"
        );
    }
}
