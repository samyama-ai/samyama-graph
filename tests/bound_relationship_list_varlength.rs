//! A bound relationship list drives a var-length segment (#984).
//!
//! ```cypher
//! MATCH ()-[r1]->()-[r2]->()
//! WITH [r1, r2] AS rs LIMIT 1
//! MATCH (first)-[rs*]->(second)
//! RETURN first, second
//! ```
//!
//! failed outright with *"`rs` is bound to a collection and cannot be used as
//! a node or relationship in a pattern"*.
//!
//! That rule (#654) is correct for a single-hop `[rs]` -- a list is not a
//! relationship -- but a var-length segment binds a *list* of relationships,
//! so a list is exactly the right type. openCypher reads `[rs*]` with `rs`
//! bound as "the walk is precisely `rs`": one candidate path, and the only
//! question is whether it is legal.
//!
//! Two things had to change together. Narrowing the rule alone would have
//! turned the error into a wrong answer, because `VarLengthExpandOperator`
//! only ever *wrote* its relationship variable -- it would have searched, then
//! rebound `rs` to whatever it found, answering a different question quietly.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(a:A)-[:Y]->(b:B)-[:Y]->(c:C)`, the TCK's graph.
fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    let b = store.create_node_with_labels([Label::new("B")]);
    let c = store.create_node_with_labels([Label::new("C")]);
    store.create_edge(a, b, "Y").unwrap();
    store.create_edge(b, c, "Y").unwrap();
    store
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<Vec<Value>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let cols = r.columns.clone();
    r.records
        .iter()
        .map(|rec| cols.iter().map(|c| rec.get(c).cloned().unwrap_or(Value::Null)).collect())
        .collect()
}

fn labels(v: &Value, store: &GraphStore) -> Vec<String> {
    let id = v.node_id().expect("a node");
    let mut l: Vec<String> = store.get_node(id).unwrap().labels.iter()
        .map(|x| x.as_str().to_string()).collect();
    l.sort();
    l
}

#[test]
fn the_walk_is_exactly_the_bound_list() {
    let store = chain();
    let got = rows(&store,
        "MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 \
         MATCH (first)-[rs*]->(second) RETURN first, second");
    assert_eq!(got.len(), 1, "got {got:?}");
    assert_eq!(labels(&got[0][0], &store), ["A"]);
    assert_eq!(labels(&got[0][1], &store), ["C"]);
}

#[test]
fn it_agrees_with_bound_endpoints() {
    let store = chain();
    let got = rows(&store,
        "MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS first, b AS second LIMIT 1 \
         MATCH (first)-[rs*]->(second) RETURN first, second");
    assert_eq!(got.len(), 1, "got {got:?}");
    assert_eq!(labels(&got[0][1], &store), ["C"]);
}

#[test]
fn endpoints_bound_the_wrong_way_round_match_nothing() {
    let store = chain();
    // `first` is the far end and `second` the near one, so the walk would have
    // to run against every edge's direction. Cypher answers no rows -- not the
    // path reversed, and not an error.
    let got = rows(&store,
        "MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS second, b AS first LIMIT 1 \
         MATCH (first)-[rs*]->(second) RETURN first, second");
    assert!(got.is_empty(), "got {got:?}");
}

#[test]
fn the_list_is_not_rebound_by_the_match() {
    let store = chain();
    // The failure this guards is silent: search, then overwrite `rs` with what
    // was found. The row count would be right and `rs` would be wrong.
    let got = rows(&store,
        "MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 \
         MATCH (f)-[rs*]->(s) RETURN size(rs) AS n");
    assert_eq!(got.len(), 1, "got {got:?}");
    assert_eq!(format!("{:?}", got[0][0]).contains("Integer(2)"), true, "got {got:?}");
}

#[test]
fn a_length_bound_the_list_violates_matches_nothing() {
    let store = chain();
    for pattern in ["[rs*3..]", "[rs*..1]"] {
        let q = format!(
            "MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 \
             MATCH (f)-{pattern}->(s) RETURN f");
        assert!(rows(&store, &q).is_empty(), "{pattern} should match nothing");
    }
}

#[test]
fn a_type_filter_the_list_violates_matches_nothing() {
    let store = chain();
    let got = rows(&store,
        "MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 \
         MATCH (f)-[rs:NOPE*]->(s) RETURN f");
    assert!(got.is_empty(), "got {got:?}");
}

#[test]
fn a_single_hop_bound_to_a_list_is_still_an_error() {
    // #654's rule is untouched where it was right: a list is not one
    // relationship.
    let store = chain();
    let q = "MATCH ()-[r1]->() WITH [r1] AS rs LIMIT 1 MATCH (f)-[rs]->(s) RETURN f";
    let parsed = parse_query(q);
    let failed = parsed.is_err()
        || QueryExecutor::new(&store).execute(&parsed.unwrap()).is_err();
    assert!(failed, "a single-hop segment bound to a list must still be rejected");
}

#[test]
fn a_node_bound_to_a_list_is_still_an_error() {
    let store = chain();
    let q = "MATCH (n) WITH [n] AS us LIMIT 1 MATCH (us)-->(m) RETURN m";
    let parsed = parse_query(q);
    let failed = parsed.is_err()
        || QueryExecutor::new(&store).execute(&parsed.unwrap()).is_err();
    assert!(failed, "a node bound to a list must still be rejected");
}
