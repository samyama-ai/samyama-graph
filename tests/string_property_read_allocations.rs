//! What `ORDER BY` costs the allocator per row, and what a string property
//! read costs beyond the value it returns (samyama-graph#750).
//!
//! #750 profiled LDBC IS3 at 69% allocator and measured a string column at
//! +139 ns per row to project and +264 ns as a sort key, against an integer.
//! Counting calls per row, string against integer, at identical structure:
//!
//! - Projecting a string costs one call per row more than an integer: the
//!   returned copy. Nothing beyond it.
//! - `Sort` itself added two calls per row for any key type: a `Vec` for the
//!   row's key, and a clone of every row as it was handed on. Both are gone.
//! - A string sort key still costs one copy per row: the column hands back an
//!   owned `PropertyValue::String`. Removing it needs a borrowed read from the
//!   column; that is still open on #750, and this test records it rather than
//!   bounding it.
//!
//! Counts calls through a global allocator, so this file holds one test.

use samyama::graph::{GraphStore, PropertyValue};
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
        let k = i * 7919 % ROWS;
        store.set_node_property("default", p, "i", k as i64).unwrap();
        store.set_node_property("default", p, "j", (k % 13) as i64).unwrap();
        store
            .set_node_property("default", p, "s", PropertyValue::String(format!("name-{k:06}")))
            .unwrap();
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

#[test]
fn sorting_adds_no_allocation_per_row_and_a_string_read_one_copy() {
    let store = build();
    let project = |k: &str| calls_per_row(&store, &format!("{MATCH} RETURN p.{k} AS v"));
    let sorted = |k: &str| calls_per_row(&store, &format!("{MATCH} RETURN p.{k} AS v ORDER BY p.{k}"));

    let (project_int, project_str) = (project("i"), project("s"));
    let (sort_int, sort_str) = (sorted("i"), sorted("s"));
    let two_keys = calls_per_row(&store, &format!("{MATCH} RETURN p.i AS v ORDER BY p.j, p.i"));
    let three_keys = calls_per_row(&store, &format!("{MATCH} RETURN p.i AS v ORDER BY p.j, p.i, p.j"));
    eprintln!(
        "calls/row -- project: int {project_int:.2}, string {project_str:.2}; \
         ORDER BY: int {sort_int:.2}, string {sort_str:.2}; two int keys {two_keys:.2}; three {three_keys:.2}"
    );

    // A projected string costs its returned copy and nothing more.
    let projected_extra = project_str - project_int;
    assert!((projected_extra - 1.0).abs() < 0.05, "projecting a string cost {projected_extra:.2} calls/row over an integer");

    // Sort's own cost per row, beyond the projection. It was 2.01: a key `Vec`
    // and a record clone. One and two keys are held inline now.
    let sort_overhead = sort_int - project_int;
    assert!(sort_overhead < 0.1, "ORDER BY an integer adds {sort_overhead:.2} allocator calls per row");
    let two_overhead = two_keys - project_int;
    assert!(two_overhead < 0.1, "ORDER BY two integers adds {two_overhead:.2} allocator calls per row");
    // Three keys go to the heap: one call per row, no more.
    let three_overhead = three_keys - project_int;
    assert!(three_overhead < 1.1, "ORDER BY three integers adds {three_overhead:.2} allocator calls per row");

    // A string sort key is borrowed from its column: it costs nothing beyond
    // the returned value. It cost one copy per row.
    let string_key_extra = (sort_str - sort_int) - projected_extra;
    eprintln!("a string sort key costs {string_key_extra:.2} copies per row beyond the returned value");
    assert!(string_key_extra < 0.05, "a string sort key copies its value: {string_key_extra:.2} per row");
}
