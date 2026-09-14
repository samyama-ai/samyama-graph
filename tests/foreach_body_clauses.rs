//! Every updating clause inside a FOREACH body runs (samyama-graph#465).
//!
//! The grammar accepted DELETE and REMOVE in the body and the parser then
//! dropped them, so `FOREACH (x IN ns | DETACH DELETE x)` reported success and
//! deleted nothing. `SET n:Label` and `SET n = {…}` were dropped the same way,
//! and a list of nodes (what `collect(n)` returns) was not iterated at all.
//! MERGE and a nested FOREACH did not parse, and CREATE of a relationship was
//! refused.
//!
//! The body is now an ordered list of clauses, run per element through the
//! same write operators the top-level clauses use.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) -> Result<(), String> {
    let p = parse_query(q).map_err(|e| e.to_string())?;
    MutQueryExecutor::new(s, "default".into())
        .execute(&p)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn int(s: &GraphStore, q: &str) -> i64 {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    match out.records[0].get("c") {
        Some(samyama::query::executor::Value::Property(samyama::graph::PropertyValue::Integer(i))) => *i,
        other => panic!("`{q}` gave {other:?}"),
    }
}

/// Two `:U` nodes, `k` 1 and 2.
fn two_users() -> GraphStore {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:U {k: 1}), (:U {k: 2})").unwrap();
    s
}

fn run(s: &mut GraphStore, q: &str) {
    write(s, q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

#[test]
fn delete_in_the_body_deletes() {
    let mut s = two_users();
    run(&mut s, "MATCH (n:U) WITH collect(n) AS ns FOREACH (x IN ns | DETACH DELETE x)");
    assert_eq!(int(&s, "MATCH (n:U) RETURN count(n) AS c"), 0);
}

#[test]
fn remove_in_the_body_removes() {
    let mut s = two_users();
    run(&mut s, "MATCH (n:U) FOREACH (x IN [1] | REMOVE n.k)");
    assert_eq!(int(&s, "MATCH (n:U) WHERE n.k IS NOT NULL RETURN count(n) AS c"), 0);
}

#[test]
fn set_label_in_the_body_sets_it() {
    let mut s = two_users();
    run(&mut s, "MATCH (n:U) FOREACH (x IN [1] | SET n:Tagged)");
    assert_eq!(int(&s, "MATCH (n:Tagged) RETURN count(n) AS c"), 2);
}

#[test]
fn a_list_of_nodes_is_iterated() {
    let mut s = two_users();
    run(&mut s, "MATCH (n:U) WITH collect(n) AS ns FOREACH (x IN ns | SET x.seen = true)");
    assert_eq!(int(&s, "MATCH (n:U) WHERE n.seen = true RETURN count(n) AS c"), 2);
}

#[test]
fn merge_in_the_body_matches_what_an_earlier_element_created() {
    let mut s = GraphStore::new();
    run(&mut s, "FOREACH (x IN [1, 2, 2] | MERGE (:A {v: x}))");
    assert_eq!(int(&s, "MATCH (n:A) RETURN count(n) AS c"), 2);
}

#[test]
fn merge_of_a_relationship_between_bound_nodes_happens_once() {
    let mut s = two_users();
    run(&mut s, "MATCH (a:U {k: 1}), (b:U {k: 2}) FOREACH (x IN [1, 2] | MERGE (a)-[:M]->(b))");
    assert_eq!(int(&s, "MATCH (:U {k: 1})-[r:M]->(:U {k: 2}) RETURN count(r) AS c"), 1);
}

#[test]
fn nested_foreach_runs_the_inner_body_per_pair() {
    let mut s = GraphStore::new();
    run(&mut s, "FOREACH (x IN [1, 2] | FOREACH (y IN [1, 2, 3] | CREATE (:B {v: x * 10 + y})))");
    assert_eq!(int(&s, "MATCH (n:B) RETURN count(n) AS c"), 6);
    assert_eq!(int(&s, "MATCH (n:B) RETURN sum(n.v) AS c"), 11 + 12 + 13 + 21 + 22 + 23);
}

/// The issue's own example: a relationship from a bound node to a new one.
#[test]
fn create_of_a_relationship_uses_the_bound_node() {
    let mut s = two_users();
    run(&mut s, "MATCH (p:U {k: 1}) FOREACH (tag IN ['a', 'b', 'c'] | CREATE (p)-[:TAGGED]->(:Tag {name: tag}))");
    assert_eq!(int(&s, "MATCH (:U {k: 1})-[r:TAGGED]->(:Tag) RETURN count(r) AS c"), 3);
    assert_eq!(int(&s, "MATCH (n) RETURN count(n) AS c"), 5);
}

/// Clauses run in the order written: REMOVE before the SET that reads it.
#[test]
fn body_clauses_run_in_written_order() {
    let mut s = two_users();
    run(&mut s, "MATCH (n:U {k: 1}) FOREACH (x IN [1] | REMOVE n.k SET n.k2 = coalesce(n.k, -1))");
    assert_eq!(int(&s, "MATCH (n:U) WHERE n.k2 = -1 RETURN count(n) AS c"), 1);
}

#[test]
fn an_empty_or_null_list_is_a_no_op() {
    let mut s = GraphStore::new();
    run(&mut s, "FOREACH (x IN [] | CREATE (:E))");
    run(&mut s, "FOREACH (x IN null | CREATE (:E))");
    assert_eq!(int(&s, "MATCH (n) RETURN count(n) AS c"), 0);
}

#[test]
fn a_non_list_is_an_error_not_a_no_op() {
    let mut s = GraphStore::new();
    assert!(write(&mut s, "FOREACH (x IN 5 | CREATE (:E))").is_err());
    assert_eq!(int(&s, "MATCH (n) RETURN count(n) AS c"), 0);
}

#[test]
fn return_inside_the_body_does_not_parse() {
    assert!(parse_query("FOREACH (x IN [1] | CREATE (:E) RETURN x)").is_err());
}

/// Control: what worked before still works.
#[test]
fn set_and_create_with_the_loop_variable_still_work() {
    let mut s = two_users();
    run(&mut s, "MATCH (n:U {k: 1}) FOREACH (x IN [1, 2, 3] | SET n.last = x)");
    run(&mut s, "FOREACH (i IN [1, 2] | CREATE (:T {i: i}))");
    assert_eq!(int(&s, "MATCH (n:U {k: 1}) RETURN n.last AS c"), 3);
    assert_eq!(int(&s, "MATCH (n:T) RETURN sum(n.i) AS c"), 3);
}
