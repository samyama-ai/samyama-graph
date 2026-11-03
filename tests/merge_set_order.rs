//! `ON CREATE SET` / `ON MATCH SET` apply their items in the order written (#1919).
//!
//! The MERGE half of #1896. `MergeClause` kept its set items in a vector per
//! kind, and the operators ran every `n.k = v` before every `n = {…}`. So
//! `MERGE (n {email: …}) ON CREATE SET n = row.props, n.source_id = row.id`,
//! the upsert MERGE exists for, stored no `source_id`, and every later
//! `MATCH (n {source_id: …})` found nothing.
//!
//! As in `tests/set_order.rs`, each test also reads back a property the
//! replace should have removed, so an implementation that stopped replacing
//! cannot pass.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

/// The named columns of every row `q` returns, sorted, `None` where absent.
fn rows(s: &GraphStore, q: &str, columns: &[&str]) -> Vec<Vec<Option<String>>> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<Vec<Option<String>>> = out
        .records
        .iter()
        .map(|r| {
            columns
                .iter()
                .map(|c| match r.get(c) {
                    Some(Value::Property(PropertyValue::String(x))) => Some(x.clone()),
                    Some(Value::Property(PropertyValue::Integer(i))) => Some(i.to_string()),
                    Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
                    other => panic!("unexpected value in {c}: {other:?}"),
                })
                .collect()
        })
        .collect();
    v.sort();
    v
}

fn some(v: &str) -> Option<String> {
    Some(v.to_string())
}

const NODE: &str = "MATCH (n:P) RETURN n.email AS email, n.name AS name, n.k AS k, n.x AS x";
const COLS: &[&str] = &["email", "name", "k", "x"];

#[test]
fn on_create_keeps_a_property_written_after_a_replace() {
    let mut s = GraphStore::new();
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON CREATE SET n = {email: 'a@x', name: 'A'}, n.k = 7");
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), some("7"), None]]);
}

#[test]
fn on_create_replaces_a_property_written_before_it() {
    let mut s = GraphStore::new();
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON CREATE SET n.k = 7, n = {email: 'a@x', name: 'A'}");
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), None, None]]);
}

#[test]
fn on_match_keeps_the_order_too() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {email: 'a@x', x: 1})");
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON MATCH SET n = {email: 'a@x', name: 'A'}, n.k = 7");
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), some("7"), None]]);

    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {email: 'a@x', x: 1})");
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON MATCH SET n.k = 7, n = {email: 'a@x', name: 'A'}");
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), None, None]]);
}

#[test]
fn on_match_applies_in_order_to_every_match() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {email: 'a@x', x: 1}), (:P {email: 'a@x', x: 2})");
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON MATCH SET n = {email: 'a@x', name: 'A'}, n.k = 7");
    assert_eq!(
        rows(&s, NODE, COLS),
        vec![
            vec![some("a@x"), some("A"), some("7"), None],
            vec![some("a@x"), some("A"), some("7"), None],
        ]
    );
}

#[test]
fn the_bulk_upsert_shape_keeps_its_source_id() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "UNWIND [{email: 'a@x', props: {email: 'a@x', name: 'A'}, id: 7}] AS row \
         MERGE (n:P {email: row.email}) \
         ON CREATE SET n = row.props, n.k = row.id",
    );
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), some("7"), None]]);
    assert_eq!(
        rows(&s, "MATCH (n:P {k: 7}) RETURN n.name AS name", &["name"]),
        vec![vec![some("A")]],
        "a later MATCH on the id finds the node"
    );
}

#[test]
fn a_second_merge_in_a_pipeline_keeps_the_order() {
    // Two MERGEs make the statement a pipeline, which plans MERGE through
    // its own path.
    let mut s = GraphStore::new();
    write(
        &mut s,
        "MERGE (c:C {id: 1}) \
         MERGE (n:P {email: 'a@x'}) ON CREATE SET n = {email: 'a@x', name: 'A'}, n.k = 7",
    );
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), some("7"), None]]);
}

#[test]
fn a_relationship_merge_keeps_the_order() {
    let rel = "MATCH (:A)-[r:R]->(:B) RETURN r.name AS name, r.k AS k, r.x AS x";
    let cols = &["name", "k", "x"];

    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A), (:B)");
    write(
        &mut s,
        "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) ON CREATE SET r = {name: 'e'}, r.k = 7",
    );
    assert_eq!(rows(&s, rel, cols), vec![vec![some("e"), some("7"), None]]);

    // ON MATCH, over an edge with a property the replace must remove.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A)-[:R {x: 1}]->(:B)");
    write(
        &mut s,
        "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) ON MATCH SET r = {name: 'e'}, r.k = 7",
    );
    assert_eq!(rows(&s, rel, cols), vec![vec![some("e"), some("7"), None]]);

    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A)-[:R {x: 1}]->(:B)");
    write(
        &mut s,
        "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) ON MATCH SET r.k = 7, r = {name: 'e'}",
    );
    assert_eq!(rows(&s, rel, cols), vec![vec![some("e"), None, None]]);
}

#[test]
fn labels_and_plus_equals_still_apply() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {email: 'a@x', x: 1})");
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON MATCH SET n += {name: 'A'}, n.k = 7, n:Seen");
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), some("A"), some("7"), some("1")]]);
    assert_eq!(
        rows(&s, "MATCH (n:Seen) RETURN n.email AS email", &["email"]),
        vec![vec![some("a@x")]]
    );
}

/// `ON MATCH SET n.k = null` removes `k`, as `SET n.k = null` does.
///
/// The single-node MERGE path used to skip a null value instead of removing
/// the property, so the old `k` survived a write that asked for it to go. Read
/// back through `keys(n)` as well as `n.k`: a property stored as null and a
/// property removed both read as null, and only the key count tells them apart.
#[test]
fn on_match_set_to_null_removes_the_property() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {email: 'a@x', k: 7})");
    write(&mut s, "MERGE (n:P {email: 'a@x'}) ON MATCH SET n.k = null");
    assert_eq!(rows(&s, NODE, COLS), vec![vec![some("a@x"), None, None, None]]);
    assert_eq!(
        rows(&s, "MATCH (n:P) RETURN size(keys(n)) AS c", &["c"]),
        vec![vec![some("1")]],
        "k was kept as a stored null rather than removed"
    );
}

/// `ON CREATE SET` may write to a variable bound before the MERGE.
///
/// `MATCH (a) MERGE (n …) ON CREATE SET a.x = 1` ignored the write to `a`: it
/// returned without error and `a.x` stayed unset, the silent write-loss of
/// #1896 and #1897. It now lands -- once, on the create, and not again on a
/// later match, where the `ON CREATE` branch does not run.
#[test]
fn on_create_set_writes_to_a_variable_bound_before_the_merge() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:A {id: 1})");
    write(&mut s, "MATCH (a:A {id: 1}) MERGE (n:P {email: 'b@x'}) ON CREATE SET a.x = 1");
    let a = "MATCH (a:A) RETURN a.x AS x";
    assert_eq!(rows(&s, a, &["x"]), vec![vec![some("1")]], "the ON CREATE write to `a` was dropped");
    assert_eq!(rows(&s, "MATCH (n:P) RETURN n.email AS e", &["e"]), vec![vec![some("b@x")]]);

    // Matched this time: ON CREATE does not run, so `a.x` keeps the 1.
    write(&mut s, "MATCH (a:A {id: 1}) MERGE (n:P {email: 'b@x'}) ON CREATE SET a.x = 2");
    assert_eq!(rows(&s, a, &["x"]), vec![vec![some("1")]]);
}
