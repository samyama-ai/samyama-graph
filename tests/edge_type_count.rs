//! `MATCH ()-[r:TYPE]->() RETURN count(r)` is answered from metadata (#304).
//!
//! Node label counts have had an O(1) path for a while, and so has the *grouped* edge form
//! (`RETURN type(r), count(r)`). A count filtered to a single edge type did not, and fell
//! through to a full Expand + Aggregate — on a 1.22B-edge federation, a 120s timeout for a
//! question the statistics already answer, while the structurally more complex grouped
//! query returned instantly.
//!
//! The risk in a shortcut like this is not slowness, it is a *wrong* count when the pattern
//! says more than the metadata knows. Most of what follows checks it declines to fire.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

/// 10 :A, 5 :B. CITES: 10 A->A and 5 A->B. LIKES: 3 A->A.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let a: Vec<_> = (0..10)
        .map(|i| {
            let id = store.create_node("A");
            store
                .get_node_mut(id)
                .unwrap()
                .set_property("i", PropertyValue::Integer(i));
            id
        })
        .collect();
    let b: Vec<_> = (0..5).map(|_| store.create_node("B")).collect();

    for i in 0..10 {
        store.create_edge(a[i], a[(i + 1) % 10], "CITES").unwrap();
    }
    for i in 0..5 {
        store.create_edge(a[i], b[i], "CITES").unwrap();
    }
    for i in 0..3 {
        store.create_edge(a[i], a[(i + 2) % 10], "LIKES").unwrap();
    }
    store
}

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

#[test]
fn a_single_edge_type_count_is_answered_from_metadata() {
    let store = fixture();

    assert_eq!(count(&store, "MATCH ()-[r:CITES]->() RETURN count(r) AS c"), 15);
    assert_eq!(count(&store, "MATCH ()-[r:LIKES]->() RETURN count(r) AS c"), 3);
    assert_eq!(count(&store, "MATCH ()-[r]->() RETURN count(r) AS c"), 18);
    assert_eq!(count(&store, "MATCH ()-[r:CITES]->() RETURN count(*) AS c"), 15);

    // an edge type nobody has used is 0, not an error and not the total
    assert_eq!(count(&store, "MATCH ()-[r:NOSUCH]->() RETURN count(r) AS c"), 0);

    // and it really is the shortcut
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
}

#[test]
fn the_shortcut_declines_when_the_pattern_says_more_than_the_metadata_knows() {
    // Each of these constrains the count in a way edge-type totals cannot express. Firing
    // the shortcut here would return a fast, confident, wrong number — which is worse than
    // the timeout it replaces.
    let store = fixture();

    assert_eq!(count(&store, "MATCH ()-[r:CITES]->(:B) RETURN count(r) AS c"), 5);
    assert_eq!(count(&store, "MATCH (:A)-[r:CITES]->(:A) RETURN count(r) AS c"), 10);
    assert_eq!(
        count(&store, "MATCH (x)-[r:CITES]->() WHERE x.i = 0 RETURN count(r) AS c"),
        2
    );
    assert_eq!(
        count(&store, "MATCH (x {i: 0})-[r:CITES]->() RETURN count(r) AS c"),
        2
    );

    let engine = QueryEngine::new();
    for query in [
        "EXPLAIN MATCH ()-[r:CITES]->(:B) RETURN count(r) AS c",
        "EXPLAIN MATCH (x)-[r:CITES]->() WHERE x.i = 0 RETURN count(r) AS c",
    ] {
        let plan = format!("{:?}", engine.execute(query, &store).unwrap().records[0].get("plan"));
        assert!(
            !plan.contains("EdgeCount"),
            "shortcut must not fire for {query}, plan was: {plan}"
        );
    }
}

#[test]
fn the_grouped_form_and_label_counts_are_unaffected() {
    let store = fixture();
    let engine = QueryEngine::new();

    let grouped = engine
        .execute("MATCH ()-[r]->() RETURN type(r) AS t, count(r) AS c", &store)
        .unwrap();
    assert_eq!(grouped.records.len(), 2, "one row per edge type");

    assert_eq!(count(&store, "MATCH (a:A) RETURN count(a) AS c"), 10);
    assert_eq!(count(&store, "MATCH (b:B) RETURN count(b) AS c"), 5);
}
