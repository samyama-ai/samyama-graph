//! Pattern comprehensions walk the whole pattern (#1243).
//!
//! `[(x)-[...]->(y) | expr]` evaluated with a walker of its own, which
//! expanded one hop per segment from the *start* node and built a fresh
//! record per edge. So:
//! - a multi-hop pattern lost every variable after the first segment
//!   ("Variable not found: c");
//! - `*`, `*1..3` and `*3..3` were all one hop;
//! - a target the row had already bound was rebound;
//! - a named path held one edge.
//!
//! Every expected answer here is Neo4j 2026.04.0's on the same graph, as
//! recorded on #1243.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph(create: &str) -> GraphStore {
    let mut s = GraphStore::new();
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(create).unwrap()).unwrap();
    s
}

/// `a→b→c→d`, `a→d`, `d→e` over `:T`; `e→a` over `:U`. Two directed cycles:
/// `a→b→c→d→e→a` (5 edges) and `a→d→e→a` (3 edges).
fn cycles() -> GraphStore {
    graph(
        "CREATE (a:N {n: 'a'}), (b:N {n: 'b'}), (c:N {n: 'c'}), (d:N {n: 'd'}), (e:N {n: 'e'}), \
         (a)-[:T]->(b), (b)-[:T]->(c), (c)-[:T]->(d), (a)-[:T]->(d), (d)-[:T]->(e), (e)-[:U]->(a)",
    )
}

/// `(a)-[:K]->(b)-[:K]->(c)`, `(a)-[:L]->(c)`, `(d)-[:K]->(d)`.
fn chain() -> GraphStore {
    graph(
        "CREATE (a:P {n: 'a', v: 1}), (b:P {n: 'b', v: 2}), (c:P {n: 'c', v: 3}), (d:P {n: 'd', v: 4}), \
         (a)-[:K]->(b), (b)-[:K]->(c), (a)-[:L]->(c), (d)-[:K]->(d)",
    )
}

fn cell(v: Option<&Value>) -> String {
    fn prop(p: &PropertyValue) -> String {
        match p {
            PropertyValue::Integer(i) => i.to_string(),
            PropertyValue::String(s) => s.clone(),
            PropertyValue::Null => "null".into(),
            PropertyValue::Array(items) => format!("[{}]", items.iter().map(prop).collect::<Vec<_>>().join(",")),
            other => format!("{other:?}"),
        }
    }
    match v {
        Some(Value::Property(p)) => prop(p),
        Some(Value::List(items)) => format!("[{}]", items.iter().map(|i| cell(Some(i))).collect::<Vec<_>>().join(",")),
        Some(Value::Null) | None => "null".into(),
        other => format!("{other:?}"),
    }
}

/// Every row as `col1|col2|...`, sorted.
fn rows(store: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(store).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| out.columns.iter().map(|c| cell(r.get(c))).collect::<Vec<_>>().join("|"))
        .collect();
    got.sort();
    got
}

#[test]
fn a_variable_length_comprehension_yields_every_path() {
    let s = cycles();
    assert_eq!(
        rows(&s, "MATCH (x:N) RETURN x.n, size([(x)-[:T*]->(z) | z]) AS n"),
        ["a|6", "b|3", "c|2", "d|1", "e|0"],
    );
    // The same paths through MATCH, which was already right.
    assert_eq!(
        rows(&s, "MATCH (x:N)-[:T*]->(z) RETURN x.n, count(*) AS n"),
        ["a|6", "b|3", "c|2", "d|1"],
    );
}

#[test]
fn closed_trails_and_exact_length_cycles() {
    let s = cycles();
    assert_eq!(
        rows(&s, "MATCH (x:N) RETURN x.n, size([(x)-[*]->(x) | 1]) AS n"),
        ["a|2", "b|1", "c|1", "d|2", "e|2"],
    );
    assert_eq!(
        rows(&s, "MATCH (x:N) RETURN x.n, size([(x)-[*3..3]->(x) | 1]) AS n"),
        ["a|1", "b|0", "c|0", "d|1", "e|1"],
    );
}

#[test]
fn a_multi_hop_comprehension_binds_every_segment() {
    let s = chain();
    assert_eq!(rows(&s, "MATCH (a:P {n: 'a'}) RETURN [(a)-[:K]->(b)-[:K]->(c) | c.n] AS r"), ["[c]"]);
    assert_eq!(rows(&s, "MATCH (a:P {n: 'a'}) RETURN size([(a)-[:K]->(b)-[:K]->(c) | c]) AS r"), ["1"]);
    // WHERE sees a later segment's variable.
    assert_eq!(
        rows(&s, "MATCH (a:P {n: 'a'}) RETURN [(a)-[:K]->(b)-[:K]->(c) WHERE c.v > 2 | b.n] AS r"),
        ["[b]"],
    );
}

#[test]
fn a_bound_target_is_matched_not_rebound() {
    let s = chain();
    // Only the :L edge joins a to c.
    assert_eq!(rows(&s, "MATCH (a:P {n: 'a'}), (c:P {n: 'c'}) RETURN size([(a)-[]->(c) | 1]) AS r"), ["1"]);
    // Already right, and must stay so: the self-loop.
    assert_eq!(rows(&s, "MATCH (d:P {n: 'd'}) RETURN size([(d)-[:K]->(d) | 1]) AS r"), ["1"]);
}

#[test]
fn a_named_path_holds_the_whole_path() {
    let s = chain();
    assert_eq!(rows(&s, "MATCH (a:P {n: 'a'}) RETURN [p = (a)-[:K]->()-[:K]->() | length(p)] AS r"), ["[2]"]);
}
