//! A list property whose elements are not literals, on every write path (#831).
//!
//! `eval_expression` produces `PropertyValue::Array` only when every element
//! was already a literal. A function call or an arithmetic expression comes
//! back as `Value::List` instead, and six write paths each tested for
//! `Value::Property` separately — refusing it with three different messages,
//! skipping it silently, or storing `null`.
//!
//! Two things make this worth a table rather than a single case:
//!
//! * **`SET` was already correct**, so the value round-trips fine once it is
//!   in. That makes the defect look like a constructor problem instead of a
//!   write-path one.
//! * **`CREATE ()-[:R {xs: [...]}]->()` reported success and stored `null`.**
//!   No read can tell that from a property that was never set.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn exec(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
}

/// The single property value the store now holds for `xs`.
fn stored(store: &GraphStore, read: &str) -> PropertyValue {
    let q = parse_query(read).expect("read parses");
    let batch = QueryExecutor::new(store).execute(&q).expect("read runs");
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{read}\n  got {other:?}"),
    }
}

const LIST: &str = "[date({year: 1984, month: 10, day: 12})]";

fn expected() -> PropertyValue {
    PropertyValue::Array(vec![PropertyValue::Date(5398)])
}

/// Every way of writing a node property.
#[test]
fn every_node_write_path_stores_the_list() {
    for write in [
        format!("CREATE ({{xs: {LIST}}})"),
        format!("MERGE ({{xs: {LIST}}})"),
        format!("WITH {LIST} AS v CREATE ({{xs: v}})"),
        format!("UNWIND [1] AS i CREATE ({{xs: {LIST}}})"),
        format!("FOREACH (i IN [1] | CREATE ({{xs: {LIST}}}))"),
    ] {
        let mut store = GraphStore::new();
        exec(&mut store, &write);
        assert_eq!(stored(&store, "MATCH (n) RETURN n.xs AS r"), expected(), "{write}");
    }

    // SET was already correct and must stay so.
    let mut store = GraphStore::new();
    exec(&mut store, "CREATE ({id: 1})");
    exec(&mut store, &format!("MATCH (n) SET n.xs = {LIST}"));
    assert_eq!(stored(&store, "MATCH (n) RETURN n.xs AS r"), expected());
}

/// **The silent one.** This path reported success and stored `null`.
#[test]
fn a_relationship_property_is_not_silently_dropped() {
    let mut store = GraphStore::new();
    exec(&mut store, &format!("CREATE ()-[:R {{xs: {LIST}}}]->()"));
    assert_eq!(
        stored(&store, "MATCH ()-[e]->() RETURN e.xs AS r"),
        expected(),
        "the relationship's list property was stored as null"
    );
}

/// It was never about temporal values: any non-literal element does it.
#[test]
fn any_non_literal_element_is_affected() {
    for (list, want) in [
        ("[1 + 1]", PropertyValue::Array(vec![PropertyValue::Integer(2)])),
        ("[abs(-1)]", PropertyValue::Array(vec![PropertyValue::Integer(1)])),
        (
            "[toUpper('a'), 'b']",
            PropertyValue::Array(vec![
                PropertyValue::String("A".into()),
                PropertyValue::String("b".into()),
            ]),
        ),
    ] {
        let mut store = GraphStore::new();
        exec(&mut store, &format!("CREATE ({{xs: {list}}})"));
        assert_eq!(stored(&store, "MATCH (n) RETURN n.xs AS r"), want, "{list}");
    }
}

/// Lists that were already all-literal went down a different branch and must
/// keep working unchanged.
#[test]
fn all_literal_lists_are_undisturbed() {
    let mut store = GraphStore::new();
    exec(&mut store, "CREATE ({xs: [1, 2, 3]})");
    assert_eq!(
        stored(&store, "MATCH (n) RETURN n.xs AS r"),
        PropertyValue::Array(vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(2),
            PropertyValue::Integer(3)
        ])
    );
}
