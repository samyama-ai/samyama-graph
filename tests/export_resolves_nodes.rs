//! Export renders a node's properties even when the row copy is empty (#545).
//!
//! A record carries a clone of the row copy taken at bind time. On a restored
//! graph that copy is empty for scalars, so a node exported inside a list or the
//! JSON fallback column rendered `"properties": {}`.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::{QueryEngine, Value};

fn restored(setup: &str) -> GraphStore {
    let engine = QueryEngine::new();
    let mut src = GraphStore::new();
    engine.execute_mut(setup, &mut src, "default").expect(setup);
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(&src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &buf[..]).expect("import");
    dst
}

fn first_node_props(v: &Value) -> Option<&std::collections::HashMap<String, PropertyValue>> {
    match v {
        Value::Node(_, n) => Some(&n.properties),
        Value::List(items) => items.iter().find_map(first_node_props),
        _ => None,
    }
}

#[test]
fn a_node_inside_a_list_carries_its_properties_after_resolve() {
    let store = restored(r#"CREATE (:P {name: "x", age: 3})"#);
    let engine = QueryEngine::new();
    let mut batch = engine.execute("MATCH (n:P) RETURN [n] AS xs", &store).unwrap();

    samyama::export::resolve_nodes(&mut batch, &store);

    let props = first_node_props(batch.records[0].get("xs").expect("xs"))
        .expect("a node inside the list");
    assert_eq!(props.get("name"), Some(&PropertyValue::String("x".into())), "{props:?}");
    assert_eq!(props.get("age"), Some(&PropertyValue::Integer(3)), "{props:?}");
}

/// Values that are not nodes pass through unchanged, so resolving cannot alter
/// what a non-node column exports.
#[test]
fn resolve_leaves_non_node_values_alone() {
    let store = restored(r#"CREATE (:P {name: "x"})"#);
    let engine = QueryEngine::new();
    let mut batch = engine.execute("MATCH (n:P) RETURN n.name AS s, 7 AS k", &store).unwrap();
    let before = format!("{:?}", batch.records[0].bindings());
    samyama::export::resolve_nodes(&mut batch, &store);
    assert_eq!(format!("{:?}", batch.records[0].bindings()), before);
}
