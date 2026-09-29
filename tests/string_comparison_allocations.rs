//! A string comparison reads its operands in place (samyama-graph#750).
//!
//! `WHERE p.name = 'x'` copied the property out of its string column and the
//! literal out of the plan -- two allocator calls per row -- only to compare the
//! two and drop both. Counted per row against the same query over an integer
//! property, which allocates nothing for its comparison:
//!
//! | predicate                                        | before | after |
//! |--------------------------------------------------|-------:|------:|
//! | `p.s <> 'zz'`, `STARTS WITH`, `CONTAINS`, `>=`   |  +2.01 | +0.01 |
//! | `p.s = p.s`, `p.s <> p.t`                        |  +2.01 | +0.01 |
//! | `r.e <> 'zz'` (relationship)                     |  +2.00 | +0.00 |
//! | `RETURN p.s = '...'`                             |  +2.00 | +0.00 |
//!
//! (`ENDS WITH ''` was +1.00: an empty literal's copy does not allocate.)
//!
//! What this does **not** remove is the copy of a string that is *returned*:
//! `PropertyValue::String(String)` owns its bytes, so every string in a result
//! is one allocation. That is the representation change #750 describes, and
//! the last assertion records it rather than bounding it away.
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

fn build() -> (GraphStore, samyama::graph::NodeId) {
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");
    for i in 0..ROWS {
        let p = store.create_node("P");
        let k = i * 7919 % ROWS;
        store.set_node_property("default", p, "i", k as i64).unwrap();
        store
            .set_node_property("default", p, "s", PropertyValue::String(format!("name-{k:06}")))
            .unwrap();
        store
            .set_node_property("default", p, "t", PropertyValue::String(format!("last-{:06}", ROWS - k)))
            .unwrap();
        let e = store.create_edge(hub, p, "KNOWS").unwrap();
        store
            .set_edge_property(e, "e", PropertyValue::String(format!("since-{k:06}")))
            .unwrap();
    }
    (store, hub)
}

fn run(store: &GraphStore, cypher: &str) -> samyama::query::RecordBatch {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    QueryExecutor::new(store).execute(&q).unwrap_or_else(|e| panic!("`{cypher}`: {e}"))
}

fn calls_per_row(store: &GraphStore, cypher: &str) -> f64 {
    // Warm once: plan caches and type indexes are not what is measured.
    run(store, cypher);
    let before = CALLS.load(Ordering::Relaxed);
    let out = run(store, cypher);
    let calls = CALLS.load(Ordering::Relaxed) - before;
    assert_eq!(out.records.len(), ROWS, "`{cypher}`");
    calls as f64 / ROWS as f64
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    run(store, cypher).records.len()
}

const MATCH: &str = "MATCH (h:Hub)-[r:KNOWS]->(p:P)";

#[test]
fn a_string_comparison_copies_neither_operand() {
    let (store, _) = build();
    let rows = |q: &str| calls_per_row(&store, &format!("{MATCH} {q}"));

    // The same shape over an integer: the comparison allocates nothing there.
    let int_filter = rows("WHERE p.i <> -1 RETURN p.i AS v");
    let int_project = rows("RETURN p.i AS v, p.i = 3 AS w");

    let filters = [
        "WHERE p.s <> 'zz' RETURN p.i AS v",
        "WHERE p.s STARTS WITH 'name' RETURN p.i AS v",
        "WHERE p.s ENDS WITH '' RETURN p.i AS v",
        "WHERE p.s CONTAINS 'e-' RETURN p.i AS v",
        "WHERE p.s >= 'name' RETURN p.i AS v",
        "WHERE 'zz' > p.s RETURN p.i AS v",
        "WHERE p.s = p.s RETURN p.i AS v",
        "WHERE p.s <> p.t RETURN p.i AS v",
        "WHERE r.e <> 'zz' RETURN p.i AS v",
        // Under an ORDER BY of another property, which is where #593 asks the
        // filter to keep what it read; `s` is not that property here.
        "WHERE p.s <> 'zz' RETURN p.i AS v ORDER BY p.i",
    ];
    let mut worst = 0.0f64;
    for q in filters {
        let extra = rows(q) - int_filter;
        eprintln!("{extra:+.2} calls/row over an integer filter -- {q}");
        worst = worst.max(extra);
    }
    let projected = rows("RETURN p.i AS v, p.s = 'name-000003' AS w") - int_project;
    eprintln!("{projected:+.2} calls/row over an integer comparison -- RETURN p.s = '...'");

    // The comparison itself costs nothing per row. It cost 2: the property's
    // copy and the literal's.
    assert!(worst < 0.05, "a string comparison in WHERE still allocates {worst:.2} calls/row");
    assert!(projected < 0.05, "a string comparison in RETURN still allocates {projected:.2} calls/row");

    // Left open (#750): a returned string is an owned copy. IS3's shape --
    // two string columns, sorted by both -- costs exactly those two copies and
    // nothing for the sort keys.
    let is3 = rows("RETURN p.i AS id, p.s AS a, p.t AS b ORDER BY p.s, p.t");
    let is3_int = rows("RETURN p.i AS id, p.i AS a, p.i AS b ORDER BY p.i, p.i");
    let returned = is3 - is3_int;
    eprintln!("IS3 shape: {is3:.2} calls/row, {returned:.2} of them the returned strings");
    assert!((returned - 2.0).abs() < 0.05, "IS3 shape costs {returned:.2} string copies per row, expected the 2 returned");

    // Same answers as before. Counted with the data's own ordering: `k` runs
    // over 0..ROWS exactly once, so `name-000000`..`name-000009` are ten rows.
    let q = |w: &str| count(&store, &format!("{MATCH} WHERE {w} RETURN p.i"));
    assert_eq!(q("p.s < 'name-000010'"), 10);
    assert_eq!(q("p.s <= 'name-000010'"), 11);
    assert_eq!(q("p.s > 'name-001989'"), 10);
    assert_eq!(q("p.s = 'name-000042'"), 1);
    assert_eq!(q("p.s <> 'name-000042'"), ROWS - 1);
    assert_eq!(q("p.s STARTS WITH 'name-0019'"), 100);
    assert_eq!(q("p.s ENDS WITH '99'"), 20);
    assert_eq!(q("p.s CONTAINS '-00012'"), 10);
    assert_eq!(q("r.e = 'since-000042'"), 1);
    assert_eq!(q("p.s = p.t"), 0);
    // Not a string on one side: the ordinary path, with its ordinary answers.
    assert_eq!(q("p.s = 1"), 0);
    assert_eq!(q("p.s < 1"), 0, "a string does not order against a number: null");
    assert_eq!(q("p.missing = 'x'"), 0, "an absent property compares as null");
    assert_eq!(q("NOT (p.missing <> 'x')"), 0, "and stays null under NOT");
    let out = run(&store, &format!("{MATCH} WHERE p.i = 42 RETURN p.s = 'name-000042' AS a, p.s < 'a' AS b, p.missing = 'x' AS c"));
    let rec = &out.records[0];
    assert_eq!(rec.get("a").and_then(|v| v.as_property()), Some(&PropertyValue::Boolean(true)));
    assert_eq!(rec.get("b").and_then(|v| v.as_property()), Some(&PropertyValue::Boolean(false)));
    assert!(rec.get("c").map_or(true, |v| v.is_null()), "null, not false");

    // A column that stops being all strings falls back, and still answers.
    let (mut mixed, hub) = build();
    let odd = mixed.create_node("P");
    mixed.set_node_property("default", odd, "s", 7i64).unwrap();
    mixed.create_edge(hub, odd, "KNOWS").unwrap();
    let q = |w: &str| count(&mixed, &format!("MATCH (h:Hub)-[:KNOWS]->(p:P) WHERE {w} RETURN p.i"));
    assert_eq!(q("p.s < 'name-000010'"), 10);
    assert_eq!(q("p.s = 7"), 1);
}
