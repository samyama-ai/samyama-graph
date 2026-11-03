//! A list may hold nodes and relationships, and stays usable afterwards.
//!
//! `[a, 1]` where `a` is a node is not expressible as a `PropertyValue`, which
//! cannot hold an entity. The list therefore has to be a `Value::List` — and
//! everything that consumes a list has to accept one, or the entity is lost at
//! the first thing that touches it.
//!
//! Fourteen openCypher TCK scenarios depend on this: the four quantifiers over
//! lists of nodes and of relationships (Quantifier1-4 scenario 8 and 9),
//! `head()`/`last()`/`tail()` over such a list (Return4, With4, Match9), and
//! the `labels()`/`type()` "type Any" scenarios, which build a mixed list and
//! index into it.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn value_of(store: &GraphStore, cypher: &str) -> Value {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    assert!(!out.records.is_empty(), "`{cypher}` returned no rows");
    out.records[0].get("x").cloned().unwrap_or(Value::Null)
}

fn seeded() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:Foo {name: 'one'})");
    store
}

#[test]
fn a_list_literal_can_hold_a_node() {
    let store = seeded();
    match value_of(&store, "MATCH (a) RETURN [a, 1] AS x") {
        Value::List(items) => {
            assert_eq!(items.len(), 2, "both elements survive");
            assert!(
                matches!(items[0], Value::Node(..) | Value::NodeRef(_)),
                "the node must still be a node, got {:?}",
                items[0]
            );
        }
        other => panic!("expected a list holding the node, got {other:?}"),
    }
}

#[test]
fn indexing_into_a_list_of_entities_returns_the_element() {
    let store = seeded();
    // The integer element, reached past a node — this is Graph3 [9]'s shape.
    assert_eq!(
        value_of(&store, "MATCH (a) RETURN [a, 1][1] AS x"),
        Value::Property(samyama::graph::PropertyValue::Integer(1))
    );
    let first = value_of(&store, "MATCH (a) RETURN [a, 1][0] AS x");
    assert!(
        matches!(first, Value::Node(..) | Value::NodeRef(_)),
        "index 0 should be the node, got {first:?}"
    );
}

#[test]
fn list_functions_accept_a_list_of_entities() {
    let store = seeded();
    assert_eq!(
        value_of(&store, "MATCH (a) RETURN size([a, 1]) AS x"),
        Value::Property(samyama::graph::PropertyValue::Integer(2))
    );
    let head = value_of(&store, "MATCH (a) RETURN head([a]) AS x");
    assert!(
        matches!(head, Value::Node(..) | Value::NodeRef(_)),
        "head of a list of nodes is a node, got {head:?}"
    );
    let last = value_of(&store, "MATCH (a) RETURN last([1, a]) AS x");
    assert!(
        matches!(last, Value::Node(..) | Value::NodeRef(_)),
        "last of a mixed list is the node, got {last:?}"
    );
    match value_of(&store, "MATCH (a) RETURN tail([1, a]) AS x") {
        Value::List(items) => assert_eq!(items.len(), 1),
        other => panic!("tail should be a one-element list, got {other:?}"),
    }
}

#[test]
fn a_list_of_entities_survives_a_with_projection() {
    let store = seeded();
    // WITH re-binds the value; if the projection flattens it to a
    // PropertyValue the node is gone by the time anything reads it back.
    let v = value_of(&store, "MATCH (a) WITH [a, 1] AS list RETURN list[0] AS x");
    assert!(
        matches!(v, Value::Node(..) | Value::NodeRef(_)),
        "the node must survive WITH, got {v:?}"
    );
}

#[test]
fn quantifiers_range_over_a_list_of_entities() {
    // all/any/none/single took the PropertyValue path only, and a list that
    // holds entities fell through to `false` — silently, for every one of the
    // four. A wrong boolean is worse than a refusal: the caller branches on it.
    // Quantifier1-4 scenario 8 and 9 in the TCK.
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:A {name: 'a'})");
    run(&mut store, "CREATE (:B {name: 'b'})");

    let t = Value::Property(samyama::graph::PropertyValue::Boolean(true));
    let f = Value::Property(samyama::graph::PropertyValue::Boolean(false));

    // A list holding two nodes, one of which is named 'a'.
    let q = |tail: &str| format!("MATCH (a:A), (b:B) WITH [a, b] AS ns RETURN {tail} AS x");

    assert_eq!(value_of(&store, &q("any(n IN ns WHERE n.name = 'a')")), t);
    assert_eq!(value_of(&store, &q("none(n IN ns WHERE n.name = 'a')")), f);
    assert_eq!(value_of(&store, &q("all(n IN ns WHERE n.name = 'a')")), f);
    assert_eq!(value_of(&store, &q("single(n IN ns WHERE n.name = 'a')")), t);

    // And the all-true / all-false ends, so a fix cannot just invert.
    assert_eq!(value_of(&store, &q("all(n IN ns WHERE n.name IS NOT NULL)")), t);
    assert_eq!(value_of(&store, &q("none(n IN ns WHERE n.name = 'zz')")), t);
}
