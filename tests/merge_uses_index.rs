//! MERGE finds an existing node through the property index.
//!
//! MERGE looked for its node by scanning every node with the first label and
//! comparing properties. On 200,000 `:N` indexed on `id`, 100 rows of
//! `UNWIND ... MERGE (n:N {id: i})` took 2.75 s -- 27.5 ms per row, a full
//! scan each -- so the upsert path of every batch load was quadratic.
//!
//! The candidates now come from the index when one of the pattern's labels
//! is indexed on one of its properties, and still pass the same match test.
//! Every outcome below is compared between an indexed store and a plain one.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into()).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out.records.iter().map(|r| format!("{:?}", r.get("c"))).collect();
    v.sort();
    v
}

/// Run `setup` then `stmt` on an indexed and a plain store; both must agree
/// on `check`, and must equal `expected`.
fn both(setup: &[&str], stmt: &str, check: &str, expected: usize) {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        for q in setup {
            write(s, q);
        }
    }
    write(&mut indexed, "CREATE INDEX ON :N(id)");
    write(&mut indexed, stmt);
    write(&mut plain, stmt);
    let (a, b) = (rows(&indexed, check), rows(&plain, check));
    assert_eq!(a, b, "indexed and plain disagree on `{check}` after `{stmt}`");
    let n = match &a[..] {
        [one] => one.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(n, format!("{:?}", Some(Value::Property(PropertyValue::Integer(expected as i64)))), "{stmt}");
}

const TEN: &str = "UNWIND range(1, 10) AS i CREATE (:N {id: i})";

#[test]
fn existing_nodes_are_matched() {
    both(&[TEN], "UNWIND range(1, 10) AS i MERGE (n:N {id: i}) SET n.seen = true", "MATCH (n:N) RETURN count(n) AS c", 10);
    both(&[TEN], "UNWIND range(1, 10) AS i MERGE (n:N {id: i}) SET n.seen = true",
         "MATCH (n:N) WHERE n.seen RETURN count(n) AS c", 10);
}

#[test]
fn missing_nodes_are_created_once() {
    both(&[TEN], "UNWIND [5, 11, 12, 11] AS i MERGE (n:N {id: i})", "MATCH (n:N) RETURN count(n) AS c", 12);
}

/// Strict equality, as the scan has it: an integer key does not match a float.
#[test]
fn integer_and_float_keys_behave_as_the_scan_does() {
    both(&["CREATE (:N {id: 1.0})"], "MERGE (n:N {id: 1})", "MATCH (n:N) RETURN count(n) AS c", 2);
}

/// Only `:N` is indexed; the other label must still be checked.
#[test]
fn every_label_is_still_checked() {
    both(&[TEN], "MERGE (n:N:M {id: 3})", "MATCH (n:N) RETURN count(n) AS c", 11);
    both(&["CREATE (:N:M {id: 3})"], "MERGE (n:N:M {id: 3})", "MATCH (n:N) RETURN count(n) AS c", 1);
}

#[test]
fn a_deleted_node_is_not_matched() {
    both(&[TEN, "MATCH (n:N {id: 3}) DETACH DELETE n"], "MERGE (n:N {id: 3})", "MATCH (n:N) RETURN count(n) AS c", 10);
}

#[test]
fn a_property_other_than_the_indexed_one_is_still_compared() {
    both(&["CREATE (:N {id: 1, k: 'a'})"], "MERGE (n:N {id: 1, k: 'b'})", "MATCH (n:N) RETURN count(n) AS c", 2);
}

#[test]
fn a_path_merge_reuses_indexed_endpoints_only_as_the_scan_does() {
    both(&[TEN, "MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:R]->(b)"],
         "MERGE (a:N {id: 1})-[:R]->(b:N {id: 2})", "MATCH ()-[r:R]->() RETURN count(r) AS c", 1);
    both(&[TEN], "MERGE (a:N {id: 1})-[:R]->(b:N {id: 2})", "MATCH (n:N) RETURN count(n) AS c", 12);
}
