//! `ON CREATE`/`ON MATCH SET` support every form `SET` does, and assigning
//! null removes a property (#874).
//!
//! ```cypher
//! MERGE (a)-[r:TYPE]->(b) ON CREATE SET r = a   -- created r with no properties
//! SET n.prop = null                             -- left the key in place
//! ```
//!
//! The grammar always parsed `SET n = {…}` in a `MERGE` arm — `set_entry`
//! includes `set_entity_item` — but `parse_merge_clause` matched only
//! `set_item` and `set_label_item`, so the item **fell through its `match`**
//! and was discarded with no error.
//!
//! The plain `SET` clause handled the same form correctly, which is what hid
//! it: the defect reads as something about `MERGE` rather than a missing arm.
//! So every case below is asserted through `SET`, `ON CREATE SET` **and**
//! `ON MATCH SET`.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn exec(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
}

/// Sorted property keys of whatever the read query returns as `r`.
fn keys(store: &GraphStore, read: &str) -> Vec<String> {
    let q = parse_query(read).expect("read parses");
    let batch = QueryExecutor::new(store).execute(&q).expect("read runs");
    let mut out: Vec<String> = match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Array(items))) => items
            .iter()
            .filter_map(|i| match i {
                PropertyValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        other => panic!("{read}\n  got {other:?}"),
    };
    out.sort();
    out
}

fn two_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    exec(&mut store, "CREATE (:A {x: 1}), (:B {y: 2})");
    store
}

/// `=` replaces and `+=` merges, in a plain `SET`. These already worked and are
/// the reference the `MERGE` arms have to match.
#[test]
fn a_plain_set_replaces_or_merges() {
    let mut s = two_nodes();
    exec(&mut s, "MATCH (a:A), (b:B) SET b = a");
    assert_eq!(keys(&s, "MATCH (b:B) RETURN keys(b) AS r"), vec!["x"]);

    let mut s = two_nodes();
    exec(&mut s, "MATCH (a:A), (b:B) SET b += a");
    assert_eq!(keys(&s, "MATCH (b:B) RETURN keys(b) AS r"), vec!["x", "y"]);

    let mut s = two_nodes();
    exec(&mut s, "MATCH (b:B) SET b = {z: 3}");
    assert_eq!(keys(&s, "MATCH (b:B) RETURN keys(b) AS r"), vec!["z"]);
}

/// **`ON CREATE SET r = a`**, which was discarded entirely.
#[test]
fn on_create_set_copies_from_a_node_or_a_map() {
    let mut s = two_nodes();
    exec(&mut s, "MATCH (a:A), (b:B) MERGE (a)-[r:T]->(b) ON CREATE SET r = a");
    assert_eq!(keys(&s, "MATCH ()-[r:T]->() RETURN keys(r) AS r"), vec!["x"]);

    let mut s = two_nodes();
    exec(
        &mut s,
        "MATCH (a:A), (b:B) MERGE (a)-[r:T]->(b) ON CREATE SET r = {name: 'bar', name2: 'baz'}",
    );
    assert_eq!(keys(&s, "MATCH ()-[r:T]->() RETURN keys(r) AS r"), vec!["name", "name2"]);

    // `+=` on a node created by MERGE.
    let mut s = two_nodes();
    exec(&mut s, "MATCH (a:A) MERGE (n:N {p: 1}) ON CREATE SET n += a");
    assert_eq!(keys(&s, "MATCH (n:N) RETURN keys(n) AS r"), vec!["p", "x"]);
}

/// **`ON MATCH SET`** takes the same forms, on the branch where the pattern
/// already exists.
#[test]
fn on_match_set_copies_too() {
    let mut s = two_nodes();
    exec(&mut s, "MATCH (a:A), (b:B) CREATE (a)-[:T]->(b)");
    exec(&mut s, "MATCH (a:A), (b:B) MERGE (a)-[r:T]->(b) ON MATCH SET r = a");
    assert_eq!(keys(&s, "MATCH ()-[r:T]->() RETURN keys(r) AS r"), vec!["x"]);
}

/// Assigning null **removes** the property, on every path that sets one.
#[test]
fn setting_null_removes_the_property() {
    let mut s = two_nodes();
    exec(&mut s, "MATCH (b:B) SET b.y = null");
    assert_eq!(keys(&s, "MATCH (b:B) RETURN keys(b) AS r"), Vec::<String>::new());

    let mut s = two_nodes();
    exec(
        &mut s,
        "MATCH (a:A), (b:B) MERGE (a)-[r:T {k: 1}]->(b) ON CREATE SET r.k = null",
    );
    assert_eq!(keys(&s, "MATCH ()-[r:T]->() RETURN keys(r) AS r"), Vec::<String>::new());

    let mut s = two_nodes();
    exec(&mut s, "MERGE (n:N {p: 1}) ON CREATE SET n.p = null");
    assert_eq!(keys(&s, "MATCH (n:N) RETURN keys(n) AS r"), Vec::<String>::new());
}

/// A non-null value still sets, so "remove on null" has not become
/// "remove always".
#[test]
fn a_non_null_value_still_sets() {
    let mut s = two_nodes();
    exec(&mut s, "MATCH (b:B) SET b.z = 3");
    assert_eq!(keys(&s, "MATCH (b:B) RETURN keys(b) AS r"), vec!["y", "z"]);

    let mut s = two_nodes();
    exec(&mut s, "MERGE (n:N) ON CREATE SET n.p = 1");
    assert_eq!(keys(&s, "MATCH (n:N) RETURN keys(n) AS r"), vec!["p"]);
}
