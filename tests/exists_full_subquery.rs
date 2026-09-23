//! `EXISTS { MATCH ... WITH ... WHERE ... RETURN ... }`: a full query body,
//! run once per outer row (#1211).
//!
//! The grammar allowed only a pattern, a WHERE and an optional RETURN inside
//! `EXISTS { }`, so a body with `WITH` was a parse error. TCK
//! ExistentialSubquery2 [2] is one of the three scenarios that still errored.
//! The first three tests are the TCK's own. The rest are derived from Cypher's
//! semantics on the same graph, where the out-degrees are a 3, b 1, c 0, d 0.
//! In particular, an aggregation grouped on `n` over zero rows yields no rows,
//! so a node with no match has no group, and EXISTS is false for it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph(create: &str) -> GraphStore {
    let mut s = GraphStore::new();
    MutQueryExecutor::new(&mut s, "default".into()).execute(&parse_query(create).unwrap()).unwrap();
    s
}

/// TCK ExistentialSubquery2 [2]'s graph.
fn tck2() -> GraphStore {
    graph(
        "CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}), (a)-[:R]->(:C {prop: 2}), \
         (a)-[:R]->(d:D {prop: 3}), (b)-[:R]->(d)",
    )
}

/// The `prop` of every returned `n`, sorted, as `Label:prop`.
fn nodes(store: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(store).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| match r.get("n") {
            Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => {
                let node = store.get_node(*id).expect("returned node exists");
                let label = node.labels.iter().next().map(|l| l.as_str().to_string()).unwrap_or_default();
                let prop = match store.node_columns.get_property(id.as_u64() as usize, "prop") {
                    PropertyValue::Null => node.properties.get("prop").cloned().unwrap_or(PropertyValue::Null),
                    p => p,
                };
                format!("{label}:{prop:?}")
            }
            other => panic!("`{q}`: n is {other:?}"),
        })
        .collect();
    got.sort();
    got
}

#[test]
fn tck_1_full_existential_subquery() {
    let s = graph(
        "CREATE (a:A {prop: 1})-[:R]->(b:B {prop: 1}), (a)-[:R]->(:C {prop: 2}), (a)-[:R]->(:D {prop: 3})",
    );
    assert_eq!(
        nodes(&s, "MATCH (n) WHERE exists { MATCH (n)-->() RETURN true } RETURN n"),
        ["A:Integer(1)"],
    );
}

#[test]
fn tck_2_full_existential_subquery_with_aggregation() {
    let s = tck2();
    assert_eq!(
        nodes(
            &s,
            "MATCH (n) WHERE exists { MATCH (n)-->(m) WITH n, count(*) AS numConnections \
             WHERE numConnections = 3 RETURN true } RETURN n",
        ),
        ["A:Integer(1)"],
    );
}

#[test]
fn tck_3_an_update_inside_is_refused() {
    let s = tck2();
    let q = "MATCH (n) WHERE exists { MATCH (n)-->(m) SET m.prop = 'fail' } RETURN n";
    let r = parse_query(q).map_err(|e| e.to_string()).and_then(|p| QueryExecutor::new(&s).execute(&p).map_err(|e| e.to_string()));
    assert!(r.is_err(), "`{q}` should be refused");
}

#[test]
fn not_exists_over_a_full_body() {
    let s = tck2();
    assert_eq!(
        nodes(
            &s,
            "MATCH (n) WHERE NOT exists { MATCH (n)-->(m) WITH n, count(*) AS k WHERE k = 3 RETURN true } RETURN n",
        ),
        ["B:Integer(1)", "C:Integer(2)", "D:Integer(3)"],
    );
}

#[test]
fn a_body_threshold_keeps_each_outer_row_once() {
    let s = tck2();
    // a has 3 matches and b one; each is returned once, not once per inner row.
    assert_eq!(
        nodes(
            &s,
            "MATCH (n) WHERE exists { MATCH (n)-->(m) WITH n, count(*) AS k WHERE k >= 1 RETURN k } RETURN n",
        ),
        ["A:Integer(1)", "B:Integer(1)"],
    );
    // A full body with a plain RETURN of the inner variable, and no WITH.
    assert_eq!(
        nodes(&s, "MATCH (n) WHERE exists { MATCH (n)-->(m) WHERE m.prop = 3 RETURN m } RETURN n"),
        ["A:Integer(1)", "B:Integer(1)"],
    );
}

#[test]
fn the_pattern_form_is_unchanged() {
    let s = tck2();
    assert_eq!(nodes(&s, "MATCH (n) WHERE exists { (n)-->(:D) } RETURN n"), ["A:Integer(1)", "B:Integer(1)"]);
    // The function form `exists((n)-->())` is #1255, fixed and tested in #1256.
}
