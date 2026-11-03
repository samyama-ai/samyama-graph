//! A label scan yields ascending node id, with or without a `LIMIT` (#1364).
//!
//! Cypher promises no order without `ORDER BY`, so an unstable scan was legal.
//! It was still wrong to ship, for a reason that is not about order at all:
//!
//! ```text
//! MATCH (c:Company) RETURN c.name AS name           -> Acme, Globex, Initech, Umbrella
//! MATCH (c:Company) RETURN c.name AS name LIMIT 100 -> Umbrella, Initech, Globex, Acme
//! MATCH (c:Company) RETURN c.name AS name SKIP 2 LIMIT 2 -> a different pair each store
//! ```
//!
//! The unlimited scan sorted its ids; the limited one took an arbitrary subset
//! of a `HashSet`. So `SKIP`/`LIMIT` paging without `ORDER BY` drew page 2 from
//! a different ordering than page 1, and could skip a row or return one twice.
//! Neo4j has the same freedom and a far more stable scan in practice, so a
//! query ported from it starts dropping rows here with nothing looking wrong.
//!
//! The limited path now walks the label bitset, whose bits are already in
//! ascending id order, and stops at the nth set bit — so it is a prefix of the
//! unlimited order without sorting the label and without giving up the early
//! termination the pushdown exists for.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

const NAMES: [&str; 8] = [
    "Acme", "Globex", "Initech", "Umbrella", "Soylent", "Tyrell", "Wayne", "Cyberdyne",
];

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for n in NAMES {
        engine
            .execute_mut(
                &format!("CREATE (:Company {{name: '{n}'}})"),
                &mut store,
                "default",
            )
            .expect("create");
    }
    store
}

fn names(store: &GraphStore, query: &str) -> Vec<String> {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("{query}: {e}"))
        .records
        .iter()
        .map(|r| match &r.bindings()[0].1 {
            samyama::query::executor::record::Value::Property(
                samyama::graph::PropertyValue::String(s),
            ) => s.clone(),
            other => panic!("expected a string, got {other:?}"),
        })
        .collect()
}

const ALL: &str = "MATCH (c:Company) RETURN c.name AS name";

#[test]
fn a_limit_returns_the_first_rows_of_the_unlimited_query() {
    // The defect, measured: this returned a different four names, in a
    // different order, from the first four of the query without the LIMIT.
    let s = store();
    let all = names(&s, ALL);
    let limited = names(&s, "MATCH (c:Company) RETURN c.name AS name LIMIT 4");
    assert_eq!(limited, all[..4], "LIMIT k is not the first k");
}

#[test]
fn paging_without_order_by_visits_every_row_exactly_once() {
    // The consequence that is not about order. Three pages of three over eight
    // rows must be the eight rows, each once.
    let s = store();
    let mut paged = Vec::new();
    for page in 0..3 {
        paged.extend(names(
            &s,
            &format!(
                "MATCH (c:Company) RETURN c.name AS name SKIP {} LIMIT 3",
                page * 3
            ),
        ));
    }
    let mut sorted = paged.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        NAMES.len(),
        "paging skipped or repeated a row: {paged:?}"
    );
    assert_eq!(paged, names(&s, ALL), "the pages are not the scan order");
}

#[test]
fn two_stores_built_the_same_way_scan_in_the_same_order() {
    // The reported symptom: the order varied between stores because the
    // `HashSet`'s iteration order did. Ten builds, one order.
    let expected = names(&store(), "MATCH (c:Company) RETURN c.name AS name LIMIT 5");
    for trial in 0..10 {
        assert_eq!(
            names(&store(), "MATCH (c:Company) RETURN c.name AS name LIMIT 5"),
            expected,
            "store {trial} scanned in a different order"
        );
    }
}

#[test]
fn a_limit_larger_than_the_label_returns_everything() {
    // The half that keeps the bitset walk honest: a loop that stopped at the
    // nth set bit and never handled "fewer than n bits" would return short.
    let s = store();
    assert_eq!(
        names(&s, "MATCH (c:Company) RETURN c.name AS name LIMIT 1000").len(),
        NAMES.len()
    );
    assert_eq!(names(&s, "MATCH (c:Company) RETURN c.name AS name LIMIT 0").len(), 0);
}

#[test]
fn a_label_nobody_carries_still_matches_nothing() {
    // `label_bitset` returns None for an absent label, which must stay
    // "matches nothing" and not become "matches everything".
    let s = store();
    assert_eq!(
        names(&s, "MATCH (c:NoSuchLabel) RETURN c.name AS name LIMIT 10").len(),
        0
    );
}

#[test]
fn a_deleted_node_is_not_scanned_after_the_bitset_was_built() {
    // The bitset is derived and cached. A scan that read a stale one would
    // return a node that is gone — the failure mode two structures always
    // have, and the reason the cache is dropped on every label change.
    let mut s = store();
    let engine = QueryEngine::new();
    let before = names(&s, "MATCH (c:Company) RETURN c.name AS name LIMIT 8");
    assert_eq!(before.len(), 8);
    engine
        .execute_mut(
            "MATCH (c:Company {name: 'Acme'}) DETACH DELETE c",
            &mut s,
            "default",
        )
        .expect("delete");
    let after = names(&s, "MATCH (c:Company) RETURN c.name AS name LIMIT 8");
    assert_eq!(after.len(), 7, "the deleted node came back: {after:?}");
    assert!(!after.contains(&"Acme".to_string()));
}
