//! Deriving a row without reallocating it (#562).
//!
//! `Vec::clone` allocates *exact* capacity, so a cloned record has
//! `len == cap` and the very next `bind` reallocates. Clone-then-bind is how
//! nearly every operator derives an output row from an input row, so nearly
//! every row in every query was paying for that: 79.8 ns to clone a 3-binding
//! record and 175.7 ns to clone and bind a fourth.
//!
//! `clone_with_capacity` is a pure optimisation — it must be indistinguishable
//! from `clone` in every observable way, which is what these assert. The
//! capacity itself is checked through `Record`'s behaviour rather than by
//! reaching into the `Vec`, so the test does not pin the representation.

use samyama::graph::{NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Record, Value};
use samyama::query::parser::parse_query;

fn sample() -> Record {
    let mut r = Record::new();
    r.bind("person", Value::NodeRef(NodeId::from(1u64)));
    r.bind("friend", Value::NodeRef(NodeId::from(2u64)));
    r.bind("n", Value::Property(PropertyValue::Integer(7)));
    r
}

#[test]
fn it_copies_every_binding() {
    let original = sample();
    let copy = original.clone_with_capacity(3);
    assert_eq!(copy.bindings(), original.bindings());
}

#[test]
fn zero_extra_is_a_plain_clone() {
    let original = sample();
    assert_eq!(original.clone_with_capacity(0).bindings(), original.clone().bindings());
}

#[test]
fn an_empty_record_survives_it() {
    let empty = Record::new();
    let copy = empty.clone_with_capacity(4);
    assert!(copy.bindings().is_empty());
}

#[test]
fn reserving_more_than_is_used_is_harmless() {
    // Operators reserve for bindings a pattern *may* have — an edge variable,
    // a path variable — and often bind fewer.
    let mut copy = sample().clone_with_capacity(10);
    copy.bind("extra", Value::Property(PropertyValue::Integer(1)));
    assert_eq!(copy.bindings().len(), 4);
    assert_eq!(
        copy.get("extra"),
        Some(&Value::Property(PropertyValue::Integer(1)))
    );
}

#[test]
fn rebinding_an_existing_name_still_replaces_rather_than_appends() {
    // The reserved room must not turn `bind` into a push. A second bind of the
    // same name replaces, and a record with two `person` entries would make
    // `get` return whichever came first.
    let mut copy = sample().clone_with_capacity(4);
    copy.bind("person", Value::NodeRef(NodeId::from(99u64)));
    assert_eq!(copy.bindings().len(), 3, "rebinding must not append");
    assert_eq!(copy.get("person"), Some(&Value::NodeRef(NodeId::from(99u64))));
}

#[test]
fn the_original_is_untouched() {
    let original = sample();
    let mut copy = original.clone_with_capacity(2);
    copy.bind("added", Value::Property(PropertyValue::Integer(5)));
    assert_eq!(original.bindings().len(), 3);
    assert!(original.get("added").is_none());
}

#[test]
fn an_expand_returns_the_same_rows_as_before() {
    // End to end: ExpandOperator now reserves before binding the target, the
    // edge variable and the path variable. Every combination of those, since
    // the reservation is computed from which of them the pattern names.
    let mut store = samyama::graph::GraphStore::new();
    let hub = store.create_node("Hub");
    for i in 0..200i64 {
        let n = store.create_node("N");
        let _ = store.set_node_property("default", n, "v".to_string(), PropertyValue::Integer(i));
        let e = store.create_edge(n, hub, "IN").unwrap();
        let _ = store.set_edge_property(e, "w", PropertyValue::Integer(i * 2));
    }

    let count = |cypher: &str| -> usize {
        let query = parse_query(cypher).expect("query should parse");
        QueryExecutor::new(&store).execute(&query).expect("query should run").records.len()
    };

    // No edge variable, no path variable.
    assert_eq!(count("MATCH (h:Hub)<-[:IN]-(n:N) RETURN n.v"), 200);
    // An edge variable.
    assert_eq!(count("MATCH (h:Hub)<-[e:IN]-(n:N) RETURN n.v, e.w"), 200);
    // A path variable.
    assert_eq!(count("MATCH p = (h:Hub)<-[:IN]-(n:N) RETURN length(p)"), 200);
    // Both.
    assert_eq!(count("MATCH p = (h:Hub)<-[e:IN]-(n:N) RETURN length(p), e.w"), 200);

    // And the values, not just the count — a mis-sized reservation that
    // silently dropped a binding would keep the row count right.
    let query = parse_query("MATCH (h:Hub)<-[e:IN]-(n:N) WHERE n.v = 5 RETURN e.w AS w").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].get("w"), Some(&Value::Property(PropertyValue::Integer(10))));
}

#[test]
fn unwind_still_binds_one_variable_per_item() {
    let store = samyama::graph::GraphStore::new();
    let query = parse_query("UNWIND [1, 2, 3] AS x RETURN x").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(batch.records.len(), 3);
    assert_eq!(
        batch.records[2].get("x"),
        Some(&Value::Property(PropertyValue::Integer(3)))
    );
}
