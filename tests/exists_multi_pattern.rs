//! An EXISTS subquery with comma-separated patterns asks for all of them at
//! once (#1244).
//!
//! `eval_exists_subquery` tried each pattern on its own and answered true when
//! any one matched, evaluating the WHERE at the end of that one pattern. So
//! `EXISTS { (x)-[:T]->(), (x)-[:U]->() }` held for a node with either
//! relationship, `NOT EXISTS` was inverted with it, and a WHERE naming the
//! second pattern's variables failed with "Variable not found". Every expected
//! answer here is Neo4j 2026.04.0's on the same graph.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `a→b→c→d`, `a→d`, `d→e` over `:T`, and `e→a` over `:U`.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let q = "CREATE (a:N {n: 'a'}), (b:N {n: 'b'}), (c:N {n: 'c'}), (d:N {n: 'd'}), (e:N {n: 'e'}), \
             (a)-[:T]->(b), (b)-[:T]->(c), (c)-[:T]->(d), (a)-[:T]->(d), (d)-[:T]->(e), (e)-[:U]->(a)";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    s
}

fn cell(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        Some(Value::Property(PropertyValue::Boolean(b))) => b.to_string(),
        other => format!("{other:?}"),
    }
}

/// Every row as `col1|col2|...`, sorted.
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

#[test]
fn every_pattern_must_match() {
    // No node has both an outgoing :T and an outgoing :U.
    check("MATCH (x:N) WHERE EXISTS { (x)-[:T]->(), (x)-[:U]->() } RETURN x.n", &[]);
    check("MATCH (x:N) WHERE EXISTS { (x)-[:T]->(), (x)<-[:U]-() } RETURN x.n", &["a"]);
    check(
        "MATCH (x:N) RETURN x.n, EXISTS { (x)-[:T]->(), ()-[:U]->(x) } AS both",
        &["a|true", "b|false", "c|false", "d|false", "e|false"],
    );
}

#[test]
fn not_exists_is_the_complement() {
    check("MATCH (x:N) WHERE NOT EXISTS { (x)-[:T]->(), (x)-[:U]->() } RETURN x.n", &["a", "b", "c", "d", "e"]);
}

/// A later pattern continues from what an earlier one bound.
#[test]
fn a_later_pattern_uses_an_earlier_ones_variables() {
    check("MATCH (x:N) WHERE EXISTS { (x)-[:T]->(y), (y)-[:T]->(z) } RETURN x.n", &["a", "b", "c"]);
    check("MATCH (x:N) WHERE EXISTS { (x)-[:T]->(y), (y)-[:U]->(z) } RETURN x.n", &["d"]);
}

/// The WHERE sees every pattern's variables: no 2-cycle exists, so no rows,
/// not "Variable not found: z".
#[test]
fn the_where_sees_the_second_patterns_variables() {
    check("MATCH (x:N) WHERE EXISTS { (x)-[r1]->(y), (y)-[r2]->(z) WHERE z = x } RETURN x.n", &[]);
}

/// Unchanged: a single pattern, with and without a WHERE.
#[test]
fn controls() {
    check("MATCH (x:N) WHERE EXISTS { (x)-[:U]->() } RETURN x.n", &["e"]);
    check("MATCH (x:N) WHERE EXISTS { (x)-[:T]->(y)-[:T]->(z) WHERE z.n = 'e' } RETURN x.n", &["a", "c"]);
    check("MATCH (x:N) WHERE EXISTS { (x)-[*]->(x) } RETURN x.n", &["a", "b", "c", "d", "e"]);
}
