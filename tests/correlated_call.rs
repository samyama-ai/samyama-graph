//! `MATCH ... CALL { WITH a <body> } RETURN ...`: a subquery run once per row
//! (#1236).
//!
//! It was a parse error. The body is planned from a seed that yields the
//! current outer row, so it gets the ordinary planner; each row it returns is
//! joined to that outer row. A body that returns nothing drops the outer row;
//! an aggregating body returns one row per outer row. Every expected answer
//! here is Neo4j 2026.04.0's on the same graph.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(a)-[:K {w:1}]->(b)-[:K {w:2}]->(c)`, `(a)-[:L]->(c)`, `(d)-[:K {w:3}]->(d)`.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let q = "CREATE (a:P {n: 'a', v: 1}), (b:P {n: 'b', v: 2}), (c:P {n: 'c', v: 3}), (d:P {n: 'd', v: 4}), \
             (a)-[:K {w: 1}]->(b), (b)-[:K {w: 2}]->(c), (a)-[:L]->(c), (d)-[:K {w: 3}]->(d)";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    s
}

fn cell(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "null".into(),
        other => format!("{other:?}"),
    }
}

/// Every row as `col1|col2`, sorted.
fn check(q: &str, expected: &[&str]) {
    let s = graph();
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(&s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| out.columns.iter().map(|c| cell(r.get(c))).collect::<Vec<_>>().join("|"))
        .collect();
    got.sort();
    assert_eq!(got, expected, "{q}");
}

/// The issue's own query: an aggregating body answers every outer row, 0 included.
#[test]
fn an_aggregating_body_answers_every_row() {
    check(
        "MATCH (p:P) CALL { WITH p MATCH (p)-[:K]->(q) RETURN count(q) AS c } RETURN p.n, c",
        &["a|1", "b|1", "c|0", "d|1"],
    );
    check(
        "MATCH (p:P) CALL { WITH p MATCH (p)-[r]->(q) WHERE q.v > p.v RETURN sum(r.w) AS s } RETURN p.n, s",
        &["a|1", "b|2", "c|0", "d|0"],
    );
}

/// A body that returns nothing drops the outer row: `c` has no :K out.
#[test]
fn a_body_with_no_rows_drops_the_outer_row() {
    check(
        "MATCH (p:P) CALL { WITH p MATCH (p)-[:K]->(q) RETURN q.n AS qn } RETURN p.n, qn",
        &["a|b", "b|c", "d|d"],
    );
}

/// ORDER BY and LIMIT inside the body apply per outer row.
#[test]
fn order_and_limit_apply_per_row() {
    check(
        "MATCH (p:P) CALL { WITH p MATCH (p)-[]->(q) RETURN q.n AS qn ORDER BY qn DESC LIMIT 1 } RETURN p.n, qn",
        &["a|c", "b|c", "d|d"],
    );
}

#[test]
fn several_imports_and_with_star() {
    check(
        "MATCH (p:P), (x:P {n: 'c'}) CALL { WITH p, x MATCH (p)-[]->(x) RETURN count(*) AS hits } RETURN p.n, hits",
        &["a|1", "b|1", "c|0", "d|0"],
    );
    check(
        "MATCH (p:P) WHERE p.v <= 2 CALL { WITH * MATCH (p)-[]->(q) RETURN collect(q.n) AS qs } RETURN p.n, size(qs) AS n",
        &["a|2", "b|1"],
    );
}

/// The planner's count shortcuts read totals off the store. In a body whose
/// variables are imported they answered the graph-wide total: 4 per row here
/// for the first query, and the same for the others.
#[test]
fn a_counting_body_does_not_take_the_store_totals() {
    check(
        "MATCH (p:P), (x:P {n: 'c'}) CALL { WITH p, x MATCH (p)-[]->(x) RETURN count(*) AS hits } RETURN p.n, hits",
        &["a|1", "b|1", "c|0", "d|0"],
    );
    check(
        "MATCH (p:P) CALL { WITH p MATCH (p:P) RETURN count(p) AS c } RETURN p.n, c",
        &["a|1", "b|1", "c|1", "d|1"],
    );
    check(
        "MATCH (p:P) CALL { WITH p MATCH (p)-[r:K]->(q) RETURN count(r) AS c } RETURN p.n, c",
        &["a|1", "b|1", "c|0", "d|1"],
    );
}

/// Shapes this first version refuses, with a message rather than an answer.
#[test]
fn refused_shapes() {
    let s = graph();
    for q in [
        "MATCH (p:P) CALL { WITH nosuch MATCH (nosuch)-[]->(q) RETURN count(q) AS c } RETURN p.n, c",
        "MATCH (p:P) CALL { WITH p MATCH (p)-[]->(q) RETURN count(q) AS c } WITH p, c RETURN p.n, c",
        // A returned name already bound outside: Neo4j refuses both.
        "MATCH (p:P) CALL { WITH p MATCH (p)-[]->(q) RETURN p } RETURN p.n",
        "MATCH (p:P), (q:P) CALL { WITH p MATCH (p)-[]->(r) RETURN r AS q } RETURN p.n",
    ] {
        let r = parse_query(q).map_err(|e| e.to_string()).and_then(|p| QueryExecutor::new(&s).execute(&p).map_err(|e| e.to_string()));
        assert!(r.is_err(), "`{q}` should be refused");
    }
}
