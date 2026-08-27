//! A list comprehension keeps entities, and `reduce` folds them (#863).
//!
//! ```text
//! [x IN collect(p) | head(nodes(x))]      [(:A), (:A)], was [null, null]
//! reduce(acc = 0, x IN nodes(p) | acc+1)  2,            was 0
//! ```
//!
//! Both are the `Value::List` versus `PropertyValue::Array` distinction on
//! paths #800 fixed only halfway. The comprehension forced every mapped value
//! into a `PropertyValue`, so an entity became `Null` — a list of the right
//! length full of nothing, indistinguishable from a projection that
//! legitimately produced nulls. `reduce` fell into its give-up arm and returned
//! the seed, which is a legitimate answer for an empty list.
//!
//! `eval_pattern_comprehension` already did this correctly, citing #662. The
//! fix is to make the list form behave like the pattern form sitting beside it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:A) CREATE (a)-[:T]->(:B), (a)-[:T]->(:C)").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn value(store: &GraphStore, cypher: &str) -> Value {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .first()
        .and_then(|r| r.get("r"))
        .cloned()
        .unwrap_or(Value::Null)
}

/// Nodes survive the projection. Asserted as **nodes**, not merely as
/// "not null": a list of two nulls has the right length and the wrong contents,
/// which is exactly what this returned.
#[test]
fn a_comprehension_projecting_nodes_returns_nodes() {
    let s = store();
    match value(&s, "MATCH p = (n)-->() RETURN [x IN collect(p) | head(nodes(x))] AS r") {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            for item in &items {
                assert!(
                    matches!(item, Value::NodeRef(_) | Value::Node(..)),
                    "expected a node, got {item:?}"
                );
            }
        }
        other => panic!("expected a list of nodes, got {other:?}"),
    }
}

/// Paths, relationships and nested entity lists too.
#[test]
fn every_entity_kind_survives_a_comprehension() {
    let s = store();
    for (cypher, want) in [
        ("MATCH p = (n)-->() RETURN [x IN collect(p) | x] AS r", 2),
        ("MATCH p = (n)-->() RETURN [x IN nodes(p) | x] AS r", 2),
        ("MATCH p = (n)-->() RETURN [x IN relationships(p) | x] AS r", 1),
        ("MATCH p = (n)-->() RETURN [x IN collect(p) | nodes(x)] AS r", 2),
    ] {
        match value(&s, cypher) {
            Value::List(items) => {
                assert_eq!(items.len(), want, "{cypher}");
                assert!(
                    !items.iter().any(|i| matches!(i, Value::Null | Value::Property(PropertyValue::Null))),
                    "{cypher} produced a null"
                );
            }
            other => panic!("{cypher}\n  got {other:?}"),
        }
    }
}

/// **A scalar comprehension still returns a `PropertyValue::Array`**, which is
/// what existing callers expect and what the storage layer can hold. A change
/// that returned `Value::List` unconditionally would satisfy everything above.
#[test]
fn a_scalar_comprehension_is_still_a_property_array() {
    let s = store();
    assert_eq!(
        value(&s, "RETURN [x IN [1, 2, 3] | x * 2] AS r"),
        Value::Property(PropertyValue::Array(vec![
            PropertyValue::Integer(2),
            PropertyValue::Integer(4),
            PropertyValue::Integer(6),
        ]))
    );
    // And a projection that genuinely yields nulls still does.
    assert_eq!(
        value(&s, "RETURN [x IN [1, 2] | null] AS r"),
        Value::Property(PropertyValue::Array(vec![PropertyValue::Null, PropertyValue::Null]))
    );
}

/// `reduce` folds an entity list instead of returning its seed.
#[test]
fn reduce_folds_a_list_of_entities() {
    let s = store();
    for (cypher, want) in [
        ("MATCH p = (n)-->() RETURN reduce(acc = 0, x IN nodes(p) | acc + 1) AS r", 2),
        ("MATCH p = (n)-->() RETURN reduce(acc = 0, x IN relationships(p) | acc + 1) AS r", 1),
        // Scalars unchanged.
        ("RETURN reduce(acc = 0, x IN [1, 2, 3] | acc + x) AS r", 6),
        // An empty list still gives the seed, which is the answer this used to
        // give for every list.
        ("RETURN reduce(acc = 7, x IN [] | acc + x) AS r", 7),
    ] {
        assert_eq!(
            value(&s, cypher),
            Value::Property(PropertyValue::Integer(want)),
            "{cypher}"
        );
    }
}
