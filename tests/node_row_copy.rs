//! A node property is stored once, in the column (#1188, step 3 of #1200).
//!
//! `set_node_property` and `create_node_with_properties` wrote every value to
//! the column store **and** to the node's row map: 2,109 MB of duplication on
//! LDBC SF1, 26.8% of the live heap. Store writes now leave the row empty.
//!
//! What a caller can observe must not change: a property read, the merged map
//! `RETURN n` builds, and the index, which has to learn the value a write
//! replaced from wherever that value lives. The index case is the one that
//! breaks silently: with the old value taken from the row, an empty row reports
//! "nothing replaced", the index keeps the stale entry, and a lookup by the old
//! value still finds the node.

use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn int(i: i64) -> Option<PropertyValue> {
    Some(PropertyValue::Integer(i))
}

fn run(store: &mut GraphStore, cypher: &str) {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut mutating = MutQueryExecutor::new(store, "default".to_string());
    mutating.execute(&query).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
}

fn count(store: &GraphStore, cypher: &str) -> PropertyValue {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{cypher}: {other:?}"),
    }
}

#[test]
fn create_with_properties_keeps_no_row_copy() {
    let mut store = GraphStore::new();
    let mut props = PropertyMap::new();
    props.insert("x".to_string(), PropertyValue::Integer(1));
    let n = store.create_node_with_properties("default", vec![Label::new("N")], props);

    assert!(store.get_node(n).unwrap().properties.is_empty(), "the row holds a second copy");
    assert_eq!(store.node_property(n, "x"), int(1));
    assert_eq!(store.node_materialized(n).unwrap().properties.get("x").cloned(), int(1));
}

#[test]
fn a_property_write_keeps_no_row_copy() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store.set_node_property("default", n, "x", 1i64).unwrap();
    store.set_node_property("default", n, "x", 2i64).unwrap();

    assert!(store.get_node(n).unwrap().properties.is_empty(), "the row holds a second copy");
    assert_eq!(store.node_property(n, "x"), int(2));
}

#[test]
fn a_store_write_supersedes_a_value_written_to_the_row_directly() {
    // `get_node_mut().set_property` writes the row only. A later store write
    // must win in the merged map too, not only in the column.
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store.get_node_mut(n).unwrap().set_property("x", 1i64);
    store.set_node_property("default", n, "x", 2i64).unwrap();

    assert_eq!(store.node_property(n, "x"), int(2));
    assert_eq!(
        store.node_materialized(n).unwrap().properties.get("x").cloned(),
        int(2),
        "the merged map kept the row's superseded value"
    );
}

#[test]
fn a_row_only_value_is_still_read_and_both_views_agree() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store.set_node_property("default", n, "x", 1i64).unwrap();
    store.get_node_mut(n).unwrap().set_property("y", 7i64);

    let merged = store.node_materialized(n).unwrap().properties;
    assert_eq!(store.node_property(n, "y"), int(7));
    assert_eq!(merged.get("y").cloned(), int(7));
    assert_eq!(merged.get("x").cloned(), int(1));
}

#[test]
fn an_index_drops_the_value_a_write_replaced() {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE INDEX ON :N(x)");
    let n = store.create_node("N");
    store.set_node_property("default", n, "x", 1i64).unwrap();
    store.set_node_property("default", n, "x", 2i64).unwrap();

    assert_eq!(
        count(&store, "MATCH (n:N) WHERE n.x = 1 RETURN count(n) AS c"),
        PropertyValue::Integer(0),
        "the index still finds the node by the value it replaced"
    );
    assert_eq!(
        count(&store, "MATCH (n:N) WHERE n.x = 2 RETURN count(n) AS c"),
        PropertyValue::Integer(1)
    );
}
