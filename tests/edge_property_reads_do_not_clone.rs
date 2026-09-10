//! Reading one relationship property must not cost the whole relationship.
//!
//! `WHERE r.p > x ... ORDER BY r.p` read `r.p` by building an owned `Edge` --
//! type string and entire property map cloned -- then reading one key and
//! dropping the rest, and did so once more just to check the edge still
//! existed. Edge properties written by CREATE, MERGE and the bulk loaders live
//! only in the row map, so the column read in front of that fallback always
//! missed. The cost of a scan therefore grew with how many *other* properties
//! each relationship carried: FinBench CR-8 paid it on 2M `DEPOSIT` edges.
//!
//! No wall-clock bound (see CLAUDE.md, "Do not assert wall-clock times"): the
//! same query runs over two relationship types that differ only in how many
//! unread properties each edge carries, in one process, and the ratio is
//! asserted.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;
use std::time::Instant;

const EDGES: usize = 20_000;
const WIDE: usize = 40;

fn build() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    for i in 0..EDGES {
        // Written the way the bulk loaders write them: the row map only.
        let narrow = store.create_edge(a, b, "NARROW").unwrap();
        store.set_edge_property_sparse(narrow, "p", PropertyValue::Float(i as f64));

        let wide = store.create_edge(a, b, "WIDE").unwrap();
        store.set_edge_property_sparse(wide, "p", PropertyValue::Float(i as f64));
        for k in 0..WIDE {
            store.set_edge_property_sparse(
                wide,
                format!("unread_{k}"),
                PropertyValue::String(format!("value {k} of edge {i}")),
            );
        }
    }
    store
}

fn cypher(ty: &str) -> String {
    format!("MATCH (:A)-[r:{ty}]->(:B) WHERE r.p > 10.0 RETURN r.p AS p ORDER BY r.p DESC LIMIT 5")
}

fn best_of(store: &GraphStore, q: &str, runs: usize) -> (f64, Vec<String>) {
    let engine = QueryEngine::new();
    let mut best = f64::MAX;
    let mut rows = Vec::new();
    for _ in 0..runs {
        let t = Instant::now();
        let batch = engine.execute(q, store).unwrap();
        best = best.min(t.elapsed().as_secs_f64());
        rows = batch
            .records
            .iter()
            .map(|r| format!("{:?}", r.get("p")))
            .collect();
    }
    (best, rows)
}

#[test]
fn unread_properties_do_not_make_a_property_read_slower() {
    let store = build();
    // Warm both once so neither pays first-touch costs the other does not.
    best_of(&store, &cypher("NARROW"), 1);
    best_of(&store, &cypher("WIDE"), 1);

    let (narrow, narrow_rows) = best_of(&store, &cypher("NARROW"), 3);
    let (wide, wide_rows) = best_of(&store, &cypher("WIDE"), 3);

    // Same answers first: a fast wrong result must not pass.
    assert_eq!(narrow_rows.len(), 5);
    assert_eq!(narrow_rows, wide_rows);

    let ratio = wide / narrow;
    eprintln!(
        "narrow {:.1} ms, wide {:.1} ms, ratio {ratio:.2}",
        narrow * 1e3,
        wide * 1e3
    );
    // Cloning a 41-entry map per read against a 1-entry map made WIDE many
    // times slower; reading one key in place makes them the same work.
    assert!(
        ratio < 2.0,
        "WIDE edges are {ratio:.2}x slower to filter and sort on one property \
         than NARROW ones: each read is paying for the {WIDE} properties it \
         does not read"
    );
}

#[test]
fn a_deleted_relationship_is_still_reported_as_deleted() {
    // `has_edge` replaced `get_edge(..).is_none()` in the deleted-entity check
    // (#905); the two must agree.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let e = store.create_edge(a, b, "R").unwrap();
    store.set_edge_property_sparse(e, "p", PropertyValue::Integer(1));
    let err = engine
        .execute_mut(
            "MATCH ()-[r:R]->() DELETE r RETURN r.p",
            &mut store,
            "default",
        )
        .unwrap_err();
    assert!(format!("{err}").contains("relationship"), "{err}");
}
