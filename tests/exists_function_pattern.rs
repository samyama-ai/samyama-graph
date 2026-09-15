//! `exists(<pattern>)` is the pattern predicate, not "is the argument null" (#1255).
//!
//! `exists` was an ordinary null-tolerant function answering "the argument is
//! not null". A pattern argument is itself a predicate that evaluates to true or
//! false, and `false` is not null, so `WHERE exists((n)-->())` was true on every
//! row and filtered nothing. Expected answers are Neo4j 2026.04.0's on the same
//! graph.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::graph::PropertyValue;

/// TCK ExistentialSubquery2 [2]'s graph, plus `(e:E)` with no `prop`.
/// Out-degrees: a 3, b 1, c 0, d 0, e 0.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    let q = "CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}), (a)-[:R]->(:C {prop: 2}), \
             (a)-[:R]->(d:D {prop: 3}), (b)-[:R]->(d), (:E)";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    s
}

/// The first label of every returned row's `l`, sorted.
fn labels(store: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(store).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| match r.get("l") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("`{q}`: l is {other:?}"),
        })
        .collect();
    got.sort();
    got
}

#[test]
fn the_function_form_of_a_pattern_is_the_pattern_predicate() {
    let s = graph();
    assert_eq!(labels(&s, "MATCH (n) WHERE exists((n)-->(:D)) RETURN labels(n)[0] AS l"), ["A", "B"]);
    assert_eq!(labels(&s, "MATCH (n) WHERE exists((n)-->()) RETURN labels(n)[0] AS l"), ["A", "B"]);
    assert_eq!(labels(&s, "MATCH (n) WHERE NOT exists((n)-->()) RETURN labels(n)[0] AS l"), ["C", "D", "E"]);
    // As a projected value, not only as a filter.
    assert_eq!(
        labels(&s, "MATCH (n) WITH n, exists((n)-[:R]->(:D)) AS e WHERE e RETURN labels(n)[0] AS l"),
        ["A", "B"],
    );
}

#[test]
fn the_other_spellings_agree_with_it() {
    let s = graph();
    for q in [
        "MATCH (n) WHERE exists { (n)-->(:D) } RETURN labels(n)[0] AS l",
        "MATCH (n) WHERE exists { MATCH (n)-->(:D) } RETURN labels(n)[0] AS l",
        "MATCH (n) WHERE (n)-->(:D) RETURN labels(n)[0] AS l",
    ] {
        assert_eq!(labels(&s, q), ["A", "B"], "{q}");
    }
}

#[test]
fn exists_on_a_property_is_still_is_not_null() {
    let s = graph();
    assert_eq!(
        labels(&s, "MATCH (n) WHERE exists(n.prop) RETURN labels(n)[0] AS l"),
        ["A", "B", "C", "D"],
    );
}
