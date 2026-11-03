//! `COUNT { pattern WHERE ... }`: how many matches (#1235).
//!
//! It was a parse error. It is the EXISTS walk counting every match instead of
//! stopping at the first, so it inherits what EXISTS gets right: several
//! patterns joined (#1244), a relationship used once across them, variable
//! lengths walked as trails. Every expected answer here is Neo4j 2026.04.0's
//! on the same graph.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(a)-[:K]->(b)-[:K]->(c)`, `(a)-[:L]->(c)`, `(d)-[:K]->(d)`.
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
        other => format!("{other:?}"),
    }
}

fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| out.columns.iter().map(|c| cell(r.get(c))).collect::<Vec<_>>().join("|"))
        .collect();
    got.sort();
    got
}

fn check(q: &str, expected: &[&str]) {
    assert_eq!(rows(&graph(), q), expected, "{q}");
}

/// `p.n|count` for a, b, c, d.
fn per_node(counts: [i64; 4]) -> Vec<String> {
    ["a", "b", "c", "d"].iter().zip(counts).map(|(n, c)| format!("{n}|{c}")).collect()
}

fn counts(body: &str, expected: [i64; 4]) {
    let q = format!("MATCH (p:P) RETURN p.n, COUNT {{ {body} }} AS c");
    let want = per_node(expected);
    let want: Vec<&str> = want.iter().map(String::as_str).collect();
    check(&q, &want);
}

#[test]
fn counts_matches_per_row() {
    counts("(p)-[]->()", [2, 1, 0, 1]);
    counts("(p)<-[]-()", [0, 1, 2, 1]);
    counts("MATCH (p)-[:K]->()", [1, 1, 0, 1]);
    counts("(p)-[:NOPE]->()", [0, 0, 0, 0]);
}

#[test]
fn with_a_where() {
    counts("(p)-[:K]->(q) WHERE q.v > 2", [0, 1, 0, 1]);
    counts("(p)-[]->(q) WHERE q.n = p.n", [0, 0, 0, 1]);
}

/// A self-loop is one relationship: once undirected, and not twice across two
/// patterns.
#[test]
fn self_loops_and_several_patterns() {
    counts("(p)-[]-()", [2, 2, 2, 1]);
    counts("(p)-[:K]->(q), (q)-[:K]->(r)", [1, 0, 0, 0]);
}

#[test]
fn variable_length_counts_trails() {
    counts("(p)-[:K*]->()", [2, 1, 0, 1]);
}

#[test]
fn as_a_value_in_where_with_set_and_aggregates() {
    check("MATCH (p:P) WHERE COUNT { (p)-[]->() } > 1 RETURN p.n", &["a"]);
    check(
        "MATCH (p:P) WITH p, COUNT { (p)-[:K]->() } AS k WHERE k > 0 RETURN p.n, k",
        &["a|1", "b|1", "d|1"],
    );
    check("MATCH (p:P) RETURN sum(COUNT { (p)-[]->() }) AS total", &["4"]);
    let mut s = graph();
    MutQueryExecutor::new(&mut s, "default".into())
        .execute(&parse_query("MATCH (p:P) SET p.deg = COUNT { (p)-[]->() }").unwrap())
        .unwrap();
    assert_eq!(rows(&s, "MATCH (p:P) RETURN p.n, p.deg"), per_node([2, 1, 0, 1]));
}

/// `count(*)` and `count(x)` are untouched.
#[test]
fn count_functions_still_parse() {
    check("MATCH (p:P) RETURN count(*) AS c", &["4"]);
    check("MATCH (p:P) RETURN count(p.v) AS c", &["4"]);
}
