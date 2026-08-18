//! `REMOVE n.prop` actually removes the property (#594).
//!
//! It reported success and left the value readable. A node's properties live
//! in the columnar store **and** in the per-node row map, and
//! `Value::resolve_property` reads the column first — `REMOVE` cleared only the
//! row, so the column's copy answered every subsequent read.
//!
//! That is the worst shape a bug can take here: a write that returns success,
//! changes nothing observable, and reports no problem. Someone removing a
//! property to correct bad data or clear a flag has every reason to believe it
//! worked.
//!
//! So these tests assert against **both stores**, not only against what a read
//! returns. A read-only assertion would have passed the moment the row was
//! cleared, if the column happened to be empty — which is exactly the case the
//! original code got right by accident.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> (GraphStore, samyama::graph::NodeId, samyama::graph::EdgeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let _ = store.set_node_property("default", a, "name".to_string(), PropertyValue::String("Ada".into()));
    let _ = store.set_node_property("default", a, "age".to_string(), PropertyValue::Integer(36));
    let b = store.create_node("P");
    let _ = store.set_node_property("default", b, "name".to_string(), PropertyValue::String("Bob".into()));
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    let _ = store.set_edge_property(e, "since", PropertyValue::Integer(2020));
    (store, a, e)
}

fn run(store: &mut GraphStore, cypher: &str) {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut mutating = MutQueryExecutor::new(store, "default".to_string());
    mutating.execute(&query).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
}

fn read(store: &GraphStore, cypher: &str) -> PropertyValue {
    let query = parse_query(cypher).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        _ => PropertyValue::Null,
    }
}

#[test]
fn removing_a_node_property_clears_both_stores() {
    let (mut store, a, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");

    assert_eq!(
        store.node_columns.get_property(a.as_u64() as usize, "age"),
        PropertyValue::Null,
        "the column still holds the value — this is the bug"
    );
    assert!(
        store.get_node(a).and_then(|n| n.get_property("age")).is_none(),
        "the row still holds the value"
    );
    assert_eq!(
        read(&store, "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r"),
        PropertyValue::Null
    );
}

#[test]
fn the_removed_property_disappears_from_keys() {
    // `keys()` reads the merged view, so it is the user-visible symptom that
    // does not depend on which store answered.
    let (mut store, _, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");

    match read(&store, "MATCH (p:P) WHERE p.name = \"Ada\" RETURN keys(p) AS r") {
        PropertyValue::Array(keys) => {
            let names: Vec<String> = keys
                .iter()
                .map(|k| match k {
                    PropertyValue::String(s) => s.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            assert_eq!(names, vec!["name"], "age should be gone");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn other_properties_survive() {
    let (mut store, a, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");
    assert_eq!(
        store.node_columns.get_property(a.as_u64() as usize, "name"),
        PropertyValue::String("Ada".into()),
        "removing one property must not disturb another"
    );
}

#[test]
fn other_nodes_survive() {
    // The column is shared across nodes, so clearing a slot must clear exactly
    // one row of it.
    let (mut store, _, _) = graph();
    let c = store.create_node("P");
    let _ = store.set_node_property("default", c, "age".to_string(), PropertyValue::Integer(7));

    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");
    assert_eq!(
        store.node_columns.get_property(c.as_u64() as usize, "age"),
        PropertyValue::Integer(7),
        "another node's value in the same column was cleared"
    );
}

#[test]
fn removing_an_edge_property_clears_both_stores() {
    let (mut store, _, e) = graph();
    run(&mut store, "MATCH ()-[r:KNOWS]->() REMOVE r.since");

    assert_eq!(
        store.edge_columns.get_property(e.as_u64() as usize, "since"),
        PropertyValue::Null
    );
    assert_eq!(
        read(&store, "MATCH ()-[r:KNOWS]->() RETURN r.since AS r"),
        PropertyValue::Null
    );
}

#[test]
fn removing_a_property_that_was_never_set_is_a_no_op() {
    let (mut store, a, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.nonexistent");
    // And nothing else was disturbed.
    assert_eq!(
        store.node_columns.get_property(a.as_u64() as usize, "age"),
        PropertyValue::Integer(36)
    );
}

#[test]
fn removing_twice_is_a_no_op() {
    let (mut store, _, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");
    assert_eq!(
        read(&store, "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r"),
        PropertyValue::Null
    );
}

#[test]
fn a_removed_property_can_be_set_again() {
    let (mut store, a, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p.age = 40");
    assert_eq!(
        store.node_columns.get_property(a.as_u64() as usize, "age"),
        PropertyValue::Integer(40)
    );
    assert_eq!(
        read(&store, "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r"),
        PropertyValue::Integer(40)
    );
}

#[test]
fn a_removed_property_no_longer_matches_a_filter() {
    // The consequence that matters in practice: a query selecting on the
    // property must stop finding the node.
    let (mut store, _, _) = graph();
    assert_eq!(
        read(&store, "MATCH (p:P) WHERE p.age = 36 RETURN count(p) AS r"),
        PropertyValue::Integer(1)
    );
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p.age");
    assert_eq!(
        read(&store, "MATCH (p:P) WHERE p.age = 36 RETURN count(p) AS r"),
        PropertyValue::Integer(0),
        "the node still matches on a property that was removed"
    );
}

#[test]
fn set_to_null_still_behaves_as_before() {
    // The contrast that revealed the bug. `SET p.age = null` already worked and
    // must keep working; it leaves an explicit null rather than removing the
    // key, which is a different thing from REMOVE and stays different.
    let (mut store, a, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p.age = null");
    assert_eq!(
        read(&store, "MATCH (p:P) WHERE p.name = \"Ada\" RETURN p.age AS r"),
        PropertyValue::Null
    );
    assert_eq!(store.node_columns.get_property(a.as_u64() as usize, "age"), PropertyValue::Null);
}
