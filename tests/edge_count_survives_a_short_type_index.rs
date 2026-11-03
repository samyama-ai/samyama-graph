//! `MATCH ()-[r:TYPE]->() RETURN count(r)` must not answer from a short index.
//!
//! The shortcut added for #304 (`EdgeCountOperator`) reads
//! `statistics().edge_type_counts`, which `compute_statistics` builds by
//! summing the lengths of `edge_type_index`. `create_edge_stub` -- the
//! bulk-load path -- deliberately does **not** maintain that index, yet it does
//! call `invalidate_statistics_cache()`, so the next `statistics()` recomputes
//! the per-type counts from an index that is missing every stub edge. The count
//! then comes back *low*: a fast, confident, wrong answer.
//!
//! `type_adjacency()` already guards the sibling read with
//! `edge_type_index_is_complete()`, whose comment says a mid-load graph "would
//! silently produce a short index, which is a wrong answer, not a slow one".
//! The count shortcut gets the same guard: fall back to the scan whenever the
//! index does not account for every edge.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

fn count(store: &GraphStore, query: &str) -> i64 {
    let engine = QueryEngine::new();
    let batch = engine
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}\n  {e}"));
    match batch.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{query}: expected an integer count, got {other:?}"),
    }
}

/// Mixed on purpose. Stubs alone leave `edge_type_index` empty, and an absent
/// entry already reads as 0 -- wrong, but wrong in a way that looks like "no
/// such type". One ordinary edge of the same type puts a *short* set in the
/// index: enough to look usable, and wrong by the number of stubs.
fn mixed() -> GraphStore {
    let mut store = GraphStore::new();
    let a: Vec<_> = (0..4).map(|_| store.create_node("Article")).collect();
    let b: Vec<_> = (0..4).map(|_| store.create_node("Author")).collect();

    // 1 indexed CITES + 3 stub CITES = 4.
    store.create_edge(a[0], a[1], "CITES").unwrap();
    store.create_edge_stub(a[1], a[2], "CITES").unwrap();
    store.create_edge_stub(a[2], a[3], "CITES").unwrap();
    store.create_edge_stub(a[3], a[0], "CITES").unwrap();

    // 2 stub WROTE, none indexed.
    store.create_edge_stub(a[0], b[0], "WROTE").unwrap();
    store.create_edge_stub(a[1], b[1], "WROTE").unwrap();

    store
}

#[test]
fn a_stub_loaded_graph_counts_every_edge_of_a_type() {
    let store = mixed();
    assert_eq!(store.edge_count(), 6, "fixture");

    assert_eq!(
        count(&store, "MATCH ()-[r:CITES]->() RETURN count(r) AS c"),
        4,
        "3 of the 4 CITES edges are stubs and are in no edge-type index"
    );
    assert_eq!(
        count(&store, "MATCH ()-[r:WROTE]->() RETURN count(r) AS c"),
        2,
        "every WROTE edge is a stub, so the index has no entry for the type at all"
    );
    assert_eq!(
        count(&store, "MATCH ()-[r]->() RETURN count(r) AS c"),
        6,
        "the untyped total is summed from the same short index"
    );
    assert_eq!(count(&store, "MATCH ()-[r:CITES]->() RETURN count(*) AS c"), 4);

    // A type nobody carries is still 0 -- the fallback scan must not invent rows.
    assert_eq!(count(&store, "MATCH ()-[r:NOSUCH]->() RETURN count(r) AS c"), 0);
}

/// Once `rebuild_edge_type_index` has repaired the index -- what
/// `finish_bulk_load` does at the end of a snapshot import -- the answer must
/// be the same.
#[test]
fn repairing_the_index_does_not_change_the_answer() {
    let mut store = mixed();
    let before = (
        count(&store, "MATCH ()-[r:CITES]->() RETURN count(r) AS c"),
        count(&store, "MATCH ()-[r:WROTE]->() RETURN count(r) AS c"),
        count(&store, "MATCH ()-[r]->() RETURN count(r) AS c"),
    );
    store.rebuild_edge_type_index();
    let after = (
        count(&store, "MATCH ()-[r:CITES]->() RETURN count(r) AS c"),
        count(&store, "MATCH ()-[r:WROTE]->() RETURN count(r) AS c"),
        count(&store, "MATCH ()-[r]->() RETURN count(r) AS c"),
    );
    assert_eq!(before, after, "the index is a cache, not a source of truth");
    assert_eq!(after, (4, 2, 6));
}

/// The guard must not cost the fast path. With no stubs in the graph the
/// `EdgeCount` operator still answers from metadata.
#[test]
fn an_ordinary_graph_keeps_the_metadata_shortcut() {
    let mut store = GraphStore::new();
    let a: Vec<_> = (0..4).map(|_| store.create_node("Article")).collect();
    for i in 0..4 {
        store.create_edge(a[i], a[(i + 1) % 4], "CITES").unwrap();
    }

    let engine = QueryEngine::new();
    let plan = format!(
        "{:?}",
        engine
            .execute("EXPLAIN MATCH ()-[r:CITES]->() RETURN count(r) AS c", &store)
            .unwrap()
            .records[0]
            .get("plan")
    );
    assert!(plan.contains("EdgeCount"), "expected the O(1) path, plan was: {plan}");
    assert_eq!(count(&store, "MATCH ()-[r:CITES]->() RETURN count(r) AS c"), 4);
}

/// The grouped form reads the same statistics, and a stub-loaded graph does not
/// merely undercount it -- a type with no indexed edge at all is dropped from
/// `edge_type_counts`, so the query loses the whole row.
#[test]
fn the_grouped_form_is_short_a_row_without_the_guard() {
    let store = mixed();
    let engine = QueryEngine::new();
    let batch = engine
        .execute("MATCH ()-[r]->() RETURN type(r) AS t, count(r) AS c", &store)
        .expect("grouped count");
    let mut got: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|rec| {
            let t = match rec.get("t") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("expected a type name, got {other:?}"),
            };
            let c = match rec.get("c") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("expected a count, got {other:?}"),
            };
            (t, c)
        })
        .collect();
    got.sort();
    assert_eq!(
        got,
        vec![("CITES".to_string(), 4), ("WROTE".to_string(), 2)],
        "WROTE is entirely stubs, so it has no entry in the edge-type index"
    );
}
