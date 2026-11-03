//! CREATE reuses the variables a WITH projected (#940).
//!
//! ```cypher
//! MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[:T]->(b) RETURN a, b
//! ```
//!
//! answered `(), ()` — two **fresh blank nodes** — where the existing node was
//! expected twice. Without the WITH the same query reuses the bound variables
//! correctly.
//!
//! A WITH re-scopes: what it projects is all that exists after it, under the
//! names it gives them. The by-kind planning path built its "already bound"
//! set from `query.match_clauses` alone, so `a` appeared in no match clause and
//! CREATE classified it as new.
//!
//! The clause-pipeline path always got this right, which is the two-AST-shapes
//! split again — and this query parses into the by-kind fields. So these tests
//! count **nodes**, not rows: the defect is a write, and a query that returns
//! plausible-looking rows while adding two nodes it should not is the failure
//! worth pinning.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn one_node() -> GraphStore {
    let mut store = GraphStore::new();
    let n = store.create_node("");
    let _ = store.set_node_property("default", n, "num".to_string(), PropertyValue::Integer(1));
    store
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap();
    match QueryExecutor::new(store).execute(&q).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    }
}

fn nodes(store: &GraphStore) -> i64 {
    count(store, "MATCH (n) RETURN count(n) AS c")
}

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut ex = MutQueryExecutor::new(store, "default".to_string());
    ex.execute(&q).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
}

#[test]
fn an_aliased_variable_is_reused_not_recreated() {
    let mut store = one_node();
    run(&mut store, "MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[:T]->(b) RETURN a, b");
    assert_eq!(nodes(&store), 1, "only the relationship is new");
    assert_eq!(count(&store, "MATCH ()-[r:T]->() RETURN count(r) AS c"), 1);
}

#[test]
fn only_the_genuinely_new_node_is_created() {
    let mut store = one_node();
    run(&mut store, "MATCH (n) WITH n AS a CREATE (a)-[:T]->(x) RETURN a");
    assert_eq!(nodes(&store), 2, "`x` is new, `a` is not");
}

#[test]
fn the_returned_node_is_the_matched_one() {
    // The half that silently corrupts a follow-on write: the query returned
    // the blank node it had just made, so anything downstream operated on the
    // wrong node.
    let mut store = one_node();
    let q = parse_query("MATCH (n) WITH n AS a CREATE (a)-[:T]->(x) RETURN a.num AS c").unwrap();
    let mut ex = MutQueryExecutor::new(&mut store, "default".to_string());
    let batch = ex.execute(&q).unwrap();
    match batch.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => assert_eq!(*n, 1),
        other => panic!("expected the matched node's num, got {other:?}"),
    }
}

#[test]
fn an_unaliased_pass_through_is_reused_too() {
    let mut store = one_node();
    run(&mut store, "MATCH (n) WITH n CREATE (n)-[:T]->(x) RETURN n");
    assert_eq!(nodes(&store), 2);
}

#[test]
fn a_name_the_with_dropped_is_a_new_node() {
    // The other side of re-scoping, and the reason the fix reads the WITH
    // rather than adding its names to the match variables: `m` is not
    // projected, so it is out of scope and CREATE (m) makes a fresh node.
    let mut store = one_node();
    run(&mut store, "MATCH (n) MATCH (m) WITH n AS a CREATE (a)-[:T]->(m) RETURN a");
    assert_eq!(nodes(&store), 2);
}

#[test]
fn create_without_a_with_is_unchanged() {
    let mut store = one_node();
    run(&mut store, "MATCH (n) MATCH (m) CREATE (n)-[:T]->(m) RETURN n, m");
    assert_eq!(nodes(&store), 1);
}

#[test]
fn a_match_after_the_with_still_binds() {
    let mut store = one_node();
    run(&mut store, "MATCH (n) WITH n AS a MATCH (b) CREATE (a)-[:T]->(b) RETURN a");
    assert_eq!(nodes(&store), 1, "both ends were already bound");
}
