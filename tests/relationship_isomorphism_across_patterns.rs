//! A relationship may appear once in a MATCH clause, across all of its
//! comma-separated patterns (#1233).
//!
//! #684 enforced this along one path. The patterns of one clause are planned
//! apart and joined, so each could claim the same relationship: over a single
//! self-loop `(d)-[:K]->(d)`, `MATCH (p)-[:K]->(q), (q)-[:K]->(r)` answered
//! `(d, d, d)`, and `MATCH (x)-[]->(y), (u)-[]->(v)` counted 16 pairs of 4
//! relationships where there are 12. Every expected answer here is Neo4j
//! 2026.04.0's on the same graph, the controls included.

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
fn two_patterns_cannot_share_the_self_loop() {
    check("MATCH (p)-[:K]->(q), (q)-[:K]->(r) RETURN p.n, q.n, r.n", &["a|b|c"]);
    check("MATCH (p:P {n: 'd'})-[e1]->(x), (x)-[e2]->(y) RETURN count(*)", &["0"]);
}

#[test]
fn named_and_anonymous_relationships() {
    check("MATCH (p)-[e:K]->(q), (q)-[]->(r) RETURN p.n, e.w, r.n", &["a|1|c"]);
    check("MATCH (p)-[e:K]->(q), (q)-[f]->(r) RETURN p.n, e.w, f.w", &["a|1|2"]);
    check("MATCH p1 = (a)-[:K]->(b), (b)-[:K]->(c) RETURN length(p1), c.n", &["1|c"]);
}

#[test]
fn untyped_patterns_count_distinct_pairs() {
    // 4 relationships: 12 ordered pairs of two different ones.
    check("MATCH (x)-[]->(y), (u)-[]->(v) RETURN count(*)", &["12"]);
}

/// Paths that start from a bound variable are chained onto the pipeline rather
/// than joined; the rule holds there too.
#[test]
fn patterns_from_a_bound_start() {
    check("MATCH (p:P {n: 'd'}) MATCH (p)-[:K]->(x), (p)-[:K]->(y) RETURN count(*)", &["0"]);
    check("MATCH (p:P {n: 'a'}) MATCH (p)-[]->(x), (p)-[]->(y) RETURN x.n, y.n", &["b|c", "c|b"]);
}

/// In an OPTIONAL MATCH the whole pattern fails, so the row keeps nulls.
#[test]
fn optional_match() {
    check(
        "MATCH (p:P) OPTIONAL MATCH (p)-[:K]->(q), (q)-[:K]->(r) RETURN p.n, q.n, r.n",
        &["a|b|c", "b|null|null", "c|null|null", "d|null|null"],
    );
    check(
        "MATCH (p:P) OPTIONAL MATCH (p)-[k1:K]->(q), (q)-[k2:K]->(r) RETURN p.n, k1.w, k2.w",
        &["a|1|2", "b|null|null", "c|null|null", "d|null|null"],
    );
    check("MATCH (p:P {n: 'd'}) OPTIONAL MATCH (p)-[e1]->(x), (x)-[e2]->(y) RETURN count(e2)", &["0"]);
}

/// Two parallel relationships between the same nodes are two relationships:
/// the patterns may bind both, in either order, but not one twice.
#[test]
fn parallel_relationships() {
    let mut s = GraphStore::new();
    let q = "CREATE (x:X {n: 'x'}), (y:Y {n: 'y'}), (x)-[:K]->(y), (x)-[:K]->(y)";
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(q).unwrap()).unwrap();
    let count = |q: &str| -> String {
        let out = QueryExecutor::new(&s).execute(&parse_query(q).unwrap()).unwrap();
        cell(out.records[0].get("c"))
    };
    assert_eq!(count("MATCH (p:X)-[:K]->(t1), (p)-[:K]->(t2) RETURN count(*) AS c"), "2");
    // A WHERE that fails when t1 = t2 leaves nothing, with or without the check.
    assert_eq!(count("MATCH (p:X)-[:K]->(t1), (p)-[:K]->(t2) WHERE t1.n < t2.n RETURN count(*) AS c"), "0");
}

/// The unordered-pairs idiom needs no check: `t1.name < t2.name` already
/// fails when the two relationships are one (#1233; BI-2's shape). Without
/// such a WHERE the check stays.
#[test]
fn a_where_that_keeps_the_endpoints_apart_needs_no_check() {
    let s = graph();
    let plan = |q: &str| -> String {
        let out = QueryExecutor::new(&s).execute(&parse_query(&format!("EXPLAIN {q}")).unwrap()).unwrap();
        match out.records[0].get("plan") {
            Some(Value::Property(PropertyValue::String(t))) => t.clone(),
            other => panic!("no plan: {other:?}"),
        }
    };
    let pairs = "MATCH (p)-[:K]->(t1), (p)-[:K]->(t2) WHERE t1.n < t2.n RETURN t1.n, t2.n";
    assert!(!plan(pairs).contains("__iso_rel"), "{}", plan(pairs));
    check(pairs, &[]);
    let any = "MATCH (p)-[:K]->(t1), (p)-[:K]->(t2) RETURN t1.n, t2.n";
    assert!(plan(any).contains("__iso_rel"), "{}", plan(any));
}

/// Controls, right before and after: separate clauses may reuse a relationship,
/// disjoint types never collide, and one path was already covered by #684.
#[test]
fn controls() {
    check("MATCH (p)-[:K]->(q) MATCH (q)-[:K]->(r) RETURN p.n, q.n, r.n", &["a|b|c", "d|d|d"]);
    check("MATCH (a)-[:K]->(b), (a)-[:L]->(c) RETURN a.n, b.n, c.n", &["a|b|c"]);
    check("MATCH (x)-[:K]->(y), (u)-[:L]->(v) RETURN count(*)", &["3"]);
    check("MATCH (p)-[:K]->(q)-[:K]->(r) RETURN p.n, q.n, r.n", &["a|b|c"]);
    check("MATCH (p)-[e1:K]->(q), (q)-[e2:K]->(r) WHERE e1 <> e2 RETURN p.n, q.n, r.n", &["a|b|c"]);
    check(
        "MATCH (p:P) OPTIONAL MATCH (p)-[:K]->(q), (p)-[:L]->(r) RETURN p.n, q.n, r.n",
        &["a|b|c", "b|null|null", "c|null|null", "d|null|null"],
    );
}
