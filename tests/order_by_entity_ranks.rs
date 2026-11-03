//! `ORDER BY` places nodes, relationships and paths (#917).
//!
//! The sort key was built with `Value::as_property()`. A node, a relationship
//! and a path have no `PropertyValue`, so each became `PropertyValue::Null` —
//! and every entity sorted as null: bunched at the end ascending, at the front
//! descending, indistinguishable from each other and from a genuinely missing
//! value.
//!
//! The query succeeded. The right rows came back, in the wrong order, and
//! nothing said so.
//!
//! openCypher's orderability, ascending:
//!
//! ```text
//! Map < Node < Relationship < List < Path < String < Boolean < Number < NaN < null
//! ```
//!
//! `graph::property::cypher_order` already documented that order in full,
//! including the three entity ranks — while taking a `PropertyValue`, which
//! cannot hold any of them. The comparison had to move up to `Value`.
//!
//! There are two sort sites: `SortOperator` and a second inside the `WITH`
//! path. Both are exercised here, because wiring only one is a mistake this
//! code has already made once.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// One node of each label, one relationship, so a projection can produce a
/// value of every type the order names.
fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("M");
    store.create_edge(a, b, "REL").unwrap();
    store
}

/// A coarse label for what a value *is*, which is all the type ordering cares
/// about.
fn kind(v: &Value) -> &'static str {
    match v {
        Value::Node(..) | Value::NodeRef(_) => "node",
        Value::Edge(..) | Value::EdgeRef(..) => "rel",
        Value::Path { .. } => "path",
        Value::List(_) | Value::Property(PropertyValue::Array(_)) => "list",
        Value::Map(_) | Value::Property(PropertyValue::Map(_)) => "map",
        Value::Property(PropertyValue::String(_)) => "string",
        Value::Property(PropertyValue::Boolean(_)) => "bool",
        Value::Property(PropertyValue::Float(f)) if f.is_nan() => "nan",
        Value::Property(PropertyValue::Integer(_) | PropertyValue::Float(_)) => "number",
        Value::Null | Value::Property(PropertyValue::Null) => "null",
        _ => "other",
    }
}

fn kinds(store: &GraphStore, cypher: &str) -> Vec<&'static str> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .iter()
        .map(|r| kind(r.get("x").expect("every row projects x")))
        .collect()
}

/// Every type openCypher orders, produced in one column in a scrambled order
/// so the sort has work to do.
const MIXED: &str = "MATCH (n:N)-[r:REL]->(m:M), p = (n)-[r]->(m) \
                     WITH [1.5, 'text', n, null, false, {a: 'map'}, ['list'], r, p] AS l \
                     UNWIND l AS x";

#[test]
fn ascending_puts_entities_where_cypher_puts_them() {
    let store = graph();
    assert_eq!(
        kinds(&store, &format!("{MIXED} RETURN x ORDER BY x")),
        vec!["map", "node", "rel", "list", "path", "string", "bool", "number", "null"]
    );
}

#[test]
fn descending_is_the_exact_reverse() {
    let store = graph();
    let mut want = kinds(&store, &format!("{MIXED} RETURN x ORDER BY x"));
    want.reverse();
    assert_eq!(kinds(&store, &format!("{MIXED} RETURN x ORDER BY x DESC")), want);
}

#[test]
fn the_with_path_sorts_the_same_way() {
    // A second ORDER BY implementation lives in the WITH path. Wiring only
    // `SortOperator` left every `WITH ... ORDER BY` on the old behaviour once
    // already, which is why this is asserted rather than assumed.
    //
    // The list is built in a WITH before the UNWIND because
    // `MATCH (n) UNWIND [n] AS x WITH x` raises VariableNotFound("n") -- a
    // separate defect (#927), unrelated to ordering.
    let store = graph();
    assert_eq!(
        kinds(&store, &format!("{MIXED} WITH x ORDER BY x RETURN x")),
        kinds(&store, &format!("{MIXED} RETURN x ORDER BY x"))
    );
}

#[test]
fn entities_of_different_kinds_do_not_collapse_together() {
    // The heart of the bug: a node, a relationship and a path all became null,
    // so all four of these sorted into one indistinguishable clump at the end.
    let store = graph();
    let got = kinds(&store, &format!("{MIXED} RETURN x ORDER BY x"));
    let tail: Vec<_> = got.iter().rev().take(4).copied().collect();
    assert_eq!(tail, vec!["null", "number", "bool", "string"]);
}

#[test]
fn ordering_by_a_node_is_stable_across_runs() {
    // openCypher leaves the order among nodes undefined, but a sort still
    // needs a total order two runs agree on, or `ORDER BY ... LIMIT` returns
    // different rows each time.
    let mut store = GraphStore::new();
    for _ in 0..8 {
        store.create_node("N");
    }
    let q = "MATCH (n:N) RETURN n AS x ORDER BY n";
    let first = ids(&store, q);
    assert_eq!(first, ids(&store, q));
    let mut sorted = first.clone();
    sorted.sort();
    assert_eq!(first, sorted, "nodes order by element id");
}

fn ids(store: &GraphStore, cypher: &str) -> Vec<u64> {
    let query = parse_query(cypher).unwrap();
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap()
        .records
        .iter()
        .map(|r| match r.get("x") {
            Some(Value::Node(id, _)) | Some(Value::NodeRef(id)) => id.as_u64(),
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn ordinary_property_ordering_is_unchanged() {
    let mut store = GraphStore::new();
    for (i, name) in ["c", "a", "b"].iter().enumerate() {
        let n = store.create_node("P");
        let _ = store.set_node_property("default", n, "name".to_string(),
                                        PropertyValue::String((*name).into()));
        let _ = store.set_node_property("default", n, "i".to_string(),
                                        PropertyValue::Integer(i as i64));
    }
    let query = parse_query("MATCH (p:P) RETURN p.name AS x ORDER BY p.name").unwrap();
    let names: Vec<String> = QueryExecutor::new(&store)
        .execute(&query)
        .unwrap()
        .records
        .iter()
        .map(|r| match r.get("x") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names, vec!["a", "b", "c"]);
}
