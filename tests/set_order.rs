//! A SET applies its writes in the order they were written (#1896).
//!
//! `SET n = {…}` replaces every property and `SET n.k = v` sets one, so which
//! runs first decides the result. The executor used to run every `n.k = v`
//! before every `n = {…}`, whatever the query said: `SET n.k = 7, n = {…}` came
//! out right and `SET n = {…}, n.k = 7` silently lost `k`. The second is the
//! bulk-import shape `SET n = row.props, n.source_id = row.id`, and losing the
//! id loses every later MATCH on it.
//!
//! Every test reads back a property the statement should have *removed* as
//! well as the ones it should have kept: checking only the kept ones would
//! pass against an implementation that never replaced anything.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&p)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

/// The named columns of the single row `q` returns, `None` where absent.
fn row(s: &GraphStore, q: &str, columns: &[&str]) -> Vec<Option<String>> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    assert_eq!(out.records.len(), 1, "`{q}` should return one row");
    let r = &out.records[0];
    columns
        .iter()
        .map(|c| match r.get(c) {
            Some(Value::Property(PropertyValue::String(x))) => Some(x.clone()),
            Some(Value::Property(PropertyValue::Integer(i))) => Some(i.to_string()),
            Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => None,
            other => panic!("unexpected value in {c}: {other:?}"),
        })
        .collect()
}

fn some(v: &str) -> Option<String> {
    Some(v.to_string())
}

const READ: &str = "MATCH (n:T) RETURN n.name AS name, n.k AS k, n.x AS x";

#[test]
fn a_property_after_a_replace_is_kept() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (n:T) SET n = {name: 'A'}, n.k = 7");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), some("7"), None]);
}

#[test]
fn a_property_before_a_replace_is_replaced() {
    // The order the old executor got right. It must stay right.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (n:T) SET n.k = 7, n = {name: 'A'}");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), None, None]);
}

#[test]
fn a_later_set_clause_runs_after_an_earlier_one() {
    // Two SET clauses: unambiguous in any reading, and still reordered,
    // because every clause was flattened into one list.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (n:T) SET n = {name: 'A'} SET n.k = 7");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), some("7"), None]);

    let mut s = GraphStore::new();
    write(&mut s, "CREATE (n:T) SET n.k = 7 SET n = {name: 'A'}");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), None, None]);
}

#[test]
fn the_bulk_import_shape_keeps_its_source_id() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "UNWIND [{props: {name: 'A'}, fid: 7}] AS row \
         CREATE (n:T) SET n = row.props, n.k = row.fid",
    );
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), some("7"), None]);
    // The point of keeping it: a later MATCH on it finds the node.
    assert_eq!(row(&s, "MATCH (n:T {k: 7}) RETURN n.name AS name", &["name"]), vec![some("A")]);
}

#[test]
fn a_replace_on_a_matched_node_removes_what_came_before_it_only() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:T {x: 1, name: 'old'})");
    write(&mut s, "MATCH (n:T) SET n.x = 2, n = {name: 'A'}, n.k = 7");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), some("7"), None]);
}

#[test]
fn merge_is_unaffected_by_order() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:T {x: 1})");
    write(&mut s, "MATCH (n:T) SET n += {name: 'A'}, n.k = 7");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), some("7"), some("1")]);
}

#[test]
fn property_values_are_still_read_before_any_write() {
    // `SET a.x = a.y, a.y = a.x` swaps. Applying writes in order must not
    // start evaluating each value after the writes before it.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:T {k: 1, x: 2})");
    write(&mut s, "MATCH (n:T) SET n.k = n.x, n.x = n.k");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![None, some("2"), some("1")]);
}

#[test]
fn a_set_after_merge_applies_whole_entity_and_label_items() {
    // This path kept only `n.prop = v` items, so the replace and the label
    // were dropped without an error.
    let mut s = GraphStore::new();
    write(&mut s, "MERGE (n:T {x: 1}) SET n = {name: 'A'}, n.k = 7, n:Extra");
    assert_eq!(row(&s, READ, &["name", "k", "x"]), vec![some("A"), some("7"), None]);
    assert_eq!(
        row(&s, "MATCH (n:Extra) RETURN n.name AS name", &["name"]),
        vec![some("A")]
    );
}

