//! Clauses in the order Cypher allows, not the order the grammar assumed (#617).
//!
//! Every statement rule in the grammar encodes one permitted clause order, with
//! writes at the end. Cypher does not work that way — a write may sit before a
//! `WITH`, and two writes may be separated by a projection — so
//! `MATCH (n) SET n.x = 1 WITH n RETURN n.x` was a syntax error.
//!
//! Underneath the syntax was a worse problem, and it is what most of these
//! tests are about. Making the grammar accept the query was not enough: the
//! default `next_mut` on a pass-through operator delegates to `next`, which
//! reads its input **read-only**, so a materialising operator severed
//! mutability for everything below it. The first working version of this
//! parsed the query, planned it correctly, ran it, returned rows — and did not
//! write. The store was unchanged and nothing said so.
//!
//! So the tests here assert the **graph after**, not just the rows returned.
//! A write that reports success and does nothing is the failure mode this
//! whole change had to avoid, and it is invisible to any test that only reads
//! the result set.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    run(&mut store, "CREATE (:P {n: 'a', x: 0, y: 9}), (:P {n: 'b', x: 0, y: 9})");
    store
}

fn run(store: &mut GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .expect("query should run")
        .records
        .len()
}

/// A single integer read back from the graph.
fn scalar(store: &GraphStore, cypher: &str) -> Option<i64> {
    let q = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&q).expect("query should run");
    match batch.records.first().and_then(|r| r.get("v")) {
        Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
        _ => None,
    }
}

fn count(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&q).expect("query should run").records.len()
}

#[test]
fn a_set_before_a_with_actually_writes() {
    // The whole point. This parsed, planned, ran and returned the *old* value
    // while leaving the store untouched.
    let mut store = fixture();
    run(&mut store, "MATCH (p:P) SET p.x = 1 WITH p RETURN p.x AS v");
    assert_eq!(scalar(&store, "MATCH (p:P {n: 'a'}) RETURN p.x AS v"), Some(1));
    assert_eq!(scalar(&store, "MATCH (p:P {n: 'b'}) RETURN p.x AS v"), Some(1));
}

#[test]
fn the_projection_after_the_write_sees_the_new_value() {
    // Separate from the above: the store can be correct while the rows the
    // caller gets are stale, and both were wrong here.
    let mut store = fixture();
    let q = parse_query("MATCH (p:P {n: 'a'}) SET p.x = 7 WITH p RETURN p.x AS v").unwrap();
    let batch = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("query should run");
    assert_eq!(
        batch.records[0].get("v"),
        Some(&Value::Property(PropertyValue::Integer(7)))
    );
}

#[test]
fn a_remove_before_a_with_actually_removes() {
    let mut store = fixture();
    run(&mut store, "MATCH (p:P) REMOVE p.y WITH p RETURN p.y AS v");
    assert_eq!(scalar(&store, "MATCH (p:P {n: 'a'}) RETURN p.y AS v"), None);
}

#[test]
fn a_delete_between_two_withs_actually_deletes() {
    let mut store = fixture();
    run(&mut store, "MATCH (p:P) WITH p DELETE p WITH 1 AS d RETURN d AS v");
    assert_eq!(count(&store, "MATCH (p:P) RETURN p"), 0);
}

#[test]
fn a_query_may_open_with_a_with() {
    // No reading clause at all — the pipeline starts from a single empty row.
    let store = GraphStore::new();
    let q = parse_query("WITH 1 AS a UNWIND [10, 20] AS b WITH a, b RETURN a + b AS v").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    let mut got: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("v") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![11, 21]);
}

#[test]
fn a_where_after_a_with_filters() {
    let store = GraphStore::new();
    let q = parse_query("WITH [1, 2, 3] AS xs UNWIND xs AS x WITH x WHERE x > 1 RETURN x AS v").unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).expect("query should run");
    assert_eq!(batch.records.len(), 2);
}

#[test]
fn an_unsupported_clause_position_is_refused_not_mis_planned() {
    // CREATE and MERGE are not threaded through the pipeline yet. The parser
    // accepts the order, so the planner must say no rather than fall back to
    // the by-kind fields — which are empty for these queries, and would be
    // read as "no CREATE at all".
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a) WITH a CREATE (b)").expect("this order parses");
    let err = MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect_err("must refuse rather than silently do nothing");
    let msg = err.to_string();
    assert!(msg.contains("CREATE"), "the message should name the clause: {msg}");
    assert_eq!(count(&store, "MATCH (n) RETURN n"), 0, "and it must not half-run");
}

#[test]
fn ordinary_queries_do_not_go_near_the_pipeline() {
    // The fallback only runs when every shape-specific rule has rejected the
    // input. If a common query started taking it, the blast radius of this
    // change would be the whole engine rather than a handful of clause orders.
    for cypher in [
        "MATCH (n) RETURN n",
        "MATCH (a)-[r]->(b) WHERE a.x = 1 RETURN a, r, b",
        "CREATE (n:P {x: 1}) RETURN n",
        "MATCH (n:P) SET n.x = 2",
        "UNWIND [1, 2] AS x RETURN x",
        "MERGE (n:P {x: 1})",
        "MATCH (n) DETACH DELETE n",
    ] {
        let q = parse_query(cypher).expect("should parse");
        assert!(
            !q.needs_clause_pipeline,
            "{cypher} was routed through the clause pipeline"
        );
    }
}

#[test]
fn the_clause_list_records_written_order() {
    let q = parse_query("MATCH (p:P) SET p.x = 1 WITH p RETURN p.x AS v").unwrap();
    let kinds: Vec<&str> = q.clauses.iter().map(|c| c.kind()).collect();
    assert_eq!(kinds, vec!["MATCH", "SET", "WITH", "RETURN"]);
}
