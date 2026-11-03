//! `SET n = {…}` and `SET n += {…}` — whole-entity assignment (openCypher TCK).
//!
//! `SET n += $props` is the standard upsert idiom, and neither spelling
//! parsed. The two differ only in what happens to properties the right-hand
//! side does *not* mention — `+=` leaves them, `=` removes them — so every
//! test here checks a property that is absent from the assignment as well as
//! the ones that are present. Checking only the assigned keys would pass
//! against an implementation that did nothing but merge.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run");
}

/// One row's named properties, as `Option<i64>` so absence is distinguishable
/// from zero — which is the entire subject of this file.
fn props(store: &GraphStore, cypher: &str, keys: &[&str]) -> Vec<Option<i64>> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    let r = &batch.records[0];
    keys.iter()
        .map(|k| match r.get(k) {
            Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
            Some(Value::Property(PropertyValue::Null)) | None => None,
            other => panic!("expected an integer or null in {k}, got {other:?}"),
        })
        .collect()
}

#[test]
fn plus_equals_merges_and_leaves_the_rest_alone() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:P {a: 1, b: 2})");
    write(&mut store, "MATCH (n:P) SET n += {b: 20, c: 3}");
    assert_eq!(
        props(&store, "MATCH (n:P) RETURN n.a AS a, n.b AS b, n.c AS c", &["a", "b", "c"]),
        vec![Some(1), Some(20), Some(3)],
        "a untouched, b overwritten, c added"
    );
}

#[test]
fn equals_replaces_and_removes_what_is_not_mentioned() {
    // The half that distinguishes the two operators. An implementation that
    // merged for both would pass every "the new value is there" assertion.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:P {a: 1, b: 2})");
    write(&mut store, "MATCH (n:P) SET n = {z: 9}");
    assert_eq!(
        props(&store, "MATCH (n:P) RETURN n.a AS a, n.b AS b, n.z AS z", &["a", "b", "z"]),
        vec![None, None, Some(9)],
        "a and b are gone; only z survives"
    );
}

#[test]
fn assigning_one_entity_to_another_copies_its_properties() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:A {x: 1, y: 2}), (:B {q: 7})");
    write(&mut store, "MATCH (a:B), (b:A) SET a = b");
    assert_eq!(
        props(&store, "MATCH (n:B) RETURN n.x AS x, n.y AS y, n.q AS q", &["x", "y", "q"]),
        vec![Some(1), Some(2), None],
        "B takes A's properties and loses its own"
    );
}

#[test]
fn plus_equals_from_an_entity_keeps_both_sides() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:A {x: 1}), (:B {q: 7})");
    write(&mut store, "MATCH (a:B), (b:A) SET a += b");
    assert_eq!(
        props(&store, "MATCH (n:B) RETURN n.x AS x, n.q AS q", &["x", "q"]),
        vec![Some(1), Some(7)],
    );
}

#[test]
fn a_relationship_can_be_assigned_too() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:A)-[:R {a: 1, b: 2}]->(:B)");
    write(&mut store, "MATCH ()-[r:R]->() SET r += {b: 20, c: 3}");
    assert_eq!(
        props(&store, "MATCH ()-[r:R]->() RETURN r.a AS a, r.b AS b, r.c AS c", &["a", "b", "c"]),
        vec![Some(1), Some(20), Some(3)],
    );

    write(&mut store, "MATCH ()-[r:R]->() SET r = {only: 5}");
    assert_eq!(
        props(&store, "MATCH ()-[r:R]->() RETURN r.a AS a, r.only AS only", &["a", "only"]),
        vec![None, Some(5)],
    );
}

#[test]
fn an_empty_map_clears_every_property() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:P {a: 1, b: 2})");
    write(&mut store, "MATCH (n:P) SET n = {}");
    assert_eq!(
        props(&store, "MATCH (n:P) RETURN n.a AS a, n.b AS b", &["a", "b"]),
        vec![None, None],
    );
}

#[test]
fn per_property_set_still_works_beside_it() {
    // `set_item` is tried before the entity form because it needs a `.`;
    // this pins that the ordering did not break the commoner spelling.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:P {a: 1})");
    write(&mut store, "MATCH (n:P) SET n.a = 5, n.b = 6");
    assert_eq!(
        props(&store, "MATCH (n:P) RETURN n.a AS a, n.b AS b", &["a", "b"]),
        vec![Some(5), Some(6)],
    );
}

#[test]
fn setting_a_label_still_parses_as_a_label() {
    // `SET n:Admin` and `SET n = …` both start with a bare variable, so the
    // grammar's alternative order decides which wins.
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:P {a: 1})");
    write(&mut store, "MATCH (n:P) SET n:Admin");
    let q = parse_query("MATCH (n:Admin) RETURN count(n) AS c").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    assert!(matches!(
        batch.records[0].get("c"),
        Some(Value::Property(PropertyValue::Integer(1)))
    ));
}
