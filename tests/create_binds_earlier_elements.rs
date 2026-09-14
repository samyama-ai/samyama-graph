//! A later element of a CREATE sees the nodes an earlier element created.
//!
//! The CREATE operator evaluated every property expression against an empty
//! row, so `CREATE (a {id: 0}), (:B {ref: a.id})` failed with "`ref` refers to
//! a variable that is not bound here". Those are the setups of TCK With2 [1],
//! WithSkipLimit1 [1] and WithSkipLimit2 [2], which were skipped because of it.
//! Neo4j binds `a` for the rest of the pattern; a reference to an element not
//! yet created is still an error.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) -> Result<(), String> {
    let p = parse_query(q).map_err(|e| e.to_string())?;
    MutQueryExecutor::new(s, "default".into()).execute(&p).map(|_| ()).map_err(|e| e.to_string())
}

fn int(s: &GraphStore, q: &str) -> i64 {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    match out.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("`{q}` gave {other:?}"),
    }
}

/// The TCK setup, verbatim.
#[test]
fn a_later_node_reads_an_earlier_one() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (a:End {num: 42, id: 0}), (:End {num: 3}), (:Begin {num: a.id})").unwrap();
    assert_eq!(int(&s, "MATCH (b:Begin) RETURN b.num AS c"), 0);
    assert_eq!(int(&s, "MATCH (n) RETURN count(n) AS c"), 3);
}

#[test]
fn a_chain_of_references() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (a:V {v: 1}), (b:V {v: a.v + 1}), (:V {v: b.v + 1})").unwrap();
    assert_eq!(int(&s, "MATCH (n:V) RETURN sum(n.v) AS c"), 6);
}

#[test]
fn a_node_created_with_a_relationship_is_readable_later_in_the_pattern() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (a:A {id: 5})-[:R]->(:B {ref: a.id})").unwrap();
    assert_eq!(int(&s, "MATCH (:A)-[:R]->(b:B) RETURN b.ref AS c"), 5);
}

/// A reference to an element not created yet, or to nothing, stays an error
/// and leaves nothing behind.
#[test]
fn forward_and_unbound_references_are_still_errors() {
    let mut s = GraphStore::new();
    assert!(write(&mut s, "CREATE (:X {v: b.v}), (b:Y {v: 1})").is_err());
    assert!(write(&mut s, "CREATE (:X {v: nobody.v})").is_err());
    assert_eq!(int(&s, "MATCH (n) RETURN count(n) AS c"), 0);
}

/// Control: constant expressions were always fine.
#[test]
fn a_constant_expression_still_works() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:C {v: 1 + 2})").unwrap();
    assert_eq!(int(&s, "MATCH (c:C) RETURN c.v AS c"), 3);
}

#[test]
fn a_relationship_property_reads_a_node_created_earlier_in_the_pattern() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (a:A {id: 5})-[:R {w: a.id}]->(:B)").unwrap();
    assert_eq!(int(&s, "MATCH (:A)-[r:R]->(:B) RETURN r.w AS c"), 5);
}
