//! What an output column's *name* costs the allocator per row (#612).
//!
//! `Record::bind` takes `impl Into<Arc<str>>`. `ProjectOperator` and
//! `AggregateOperator` held their aliases as `String` and bound `alias.clone()`,
//! so every output column of every row paid two calls, one to copy the `String`
//! and one to build the `Arc<str>` from it, before its value was even looked at.
//! `ExpandOperator` stopped doing this for its variables in #564. On LDBC IC5
//! (78,295 groups, three output columns, rebound again by the projection after
//! the aggregate) it is about 0.9M of the query's 2.86M allocator calls.
//!
//! The measurement is a difference: the same query with one output column and
//! with three, all integers, so each extra column costs exactly its name and
//! its slot in the record. Both should now be free.
//!
//! Counts calls through a global allocator, so this file holds one test.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static CALLS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static G: Counting = Counting;

const ROWS: usize = 2_000;

fn build() -> GraphStore {
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");
    for i in 0..ROWS {
        let p = store.create_node("P");
        store.set_node_property("default", p, "i", i as i64).unwrap();
        store.set_node_property("default", p, "j", (i % 13) as i64).unwrap();
        store.create_edge(hub, p, "KNOWS").unwrap();
    }
    store
}

fn calls_per_row(store: &GraphStore, cypher: &str) -> f64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    // Warm once: plan caches and type indexes are not what is measured.
    QueryExecutor::new(store).execute(&q).unwrap();
    let before = CALLS.load(Ordering::Relaxed);
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let calls = CALLS.load(Ordering::Relaxed) - before;
    assert_eq!(out.records.len(), ROWS, "`{cypher}`");
    calls as f64 / ROWS as f64
}

const MATCH: &str = "MATCH (h:Hub)-[:KNOWS]->(p:P)";

/// The same rows, matched undirected, for the aggregate arms.
///
/// A difference only measures a per-column cost while both arms run the same
/// plan. A *single* `count()` over a single directed segment is the ADR-017
/// adjacency-count shape, and since #304 `count(*)` reaches it too — so the
/// one-column arm was rewritten to `AdjacencyCountAggregate`, which never
/// builds the generic aggregate's records, while the three-column arm was not.
/// It read 3.05 calls/row against 6.11 and the test reported 1.53 per extra
/// column, which was the plan difference.
///
/// `adjacency_agg_detector::detect` declines `--`, so both arms take the
/// generic `Aggregate` and the interval is a column cost again. `h` has only
/// outgoing `:KNOWS`, so the undirected pattern matches the same 2,000 pairs.
///
/// Widening the interval instead (two columns against four) does not work:
/// `Record`'s bindings `Vec` grows between three and four, and `origin/main`
/// reads 1.01 calls per extra column across that step and 0.01 across the
/// one-to-three one. The step is not what this test is about.
const AGG_MATCH: &str = "MATCH (h:Hub)-[:KNOWS]-(p:P)";

#[test]
fn an_output_column_name_costs_no_allocation_per_row() {
    let store = build();

    // Projection: one integer column, then three.
    let one = calls_per_row(&store, &format!("{MATCH} RETURN p.i AS a"));
    let three = calls_per_row(&store, &format!("{MATCH} RETURN p.i AS a, p.j AS b, p.i AS c"));
    let per_projected = (three - one) / 2.0;

    // Aggregation with one group per row (`p.i` is unique), so a per-group cost
    // is a per-row cost. One aggregate column, then three; the projection that
    // follows the aggregate rebinds them all, so this covers both operators.
    let agg_one = calls_per_row(&store, &format!("{AGG_MATCH} RETURN p.i AS g, count(*) AS n"));
    let agg_three = calls_per_row(
        &store,
        &format!("{AGG_MATCH} RETURN p.i AS g, count(*) AS n, count(*) AS m, count(*) AS o"),
    );
    let per_aggregate = (agg_three - agg_one) / 2.0;

    eprintln!(
        "calls/row -- project: 1 col {one:.2}, 3 cols {three:.2} ({per_projected:.2} per extra column); \
         aggregate: 1 col {agg_one:.2}, 3 cols {agg_three:.2} ({per_aggregate:.2} per extra column)"
    );
    assert!(per_projected < 0.1, "each extra projected column costs {per_projected:.2} allocator calls per row");
    assert!(per_aggregate < 0.1, "each extra aggregate column costs {per_aggregate:.2} allocator calls per row");
}
