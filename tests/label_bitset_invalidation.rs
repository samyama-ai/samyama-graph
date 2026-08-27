//! The derived label bitsets must never outlive the label index (#730).
//!
//! `ExpandOperator` tests a far-end label with a dense bitset built from
//! `label_index` and cached. Cached is the dangerous word: two structures that
//! can disagree is how #491 happened, and here a stale bitset does not crash —
//! it silently drops rows that should match, or keeps rows that should not.
//!
//! So these tests all have the same shape: run a query so the bitset is built,
//! change the labels underneath it, and run the query again.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn run(store: &mut GraphStore, cypher: &str) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    match out.records.first().and_then(|r| r.get("n")) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("expected integer n, got {other:?}"),
    }
}

const OUT_TO_TARGET: &str = "MATCH (a:Src)-[:R]->(b:Target) RETURN count(b) AS n";

/// One source, one already-`:Target` neighbour.
fn base() -> GraphStore {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (:Src {name: \"s\"})");
    run(&mut s, "CREATE (:Target {name: \"t1\"})");
    run(&mut s, "MATCH (a:Src), (b:Target) CREATE (a)-[:R]->(b)");
    s
}

/// A node created after the bitset was built is visible to the next query.
#[test]
fn a_node_created_later_is_not_missed() {
    let mut s = base();
    assert_eq!(count(&s, OUT_TO_TARGET), 1);

    run(&mut s, "CREATE (:Target {name: \"t2\"})");
    run(
        &mut s,
        "MATCH (a:Src), (b:Target {name:\"t2\"}) CREATE (a)-[:R]->(b)",
    );
    assert_eq!(count(&s, OUT_TO_TARGET), 2, "the new :Target was invisible");
}

/// A label added to an existing node is visible to the next query.
#[test]
fn a_label_added_later_is_not_missed() {
    let mut s = base();
    run(&mut s, "CREATE (:Other {name: \"o\"})");
    run(
        &mut s,
        "MATCH (a:Src), (b:Other {name:\"o\"}) CREATE (a)-[:R]->(b)",
    );
    assert_eq!(count(&s, OUT_TO_TARGET), 1);

    run(&mut s, "MATCH (o:Other {name:\"o\"}) SET o:Target");
    assert_eq!(
        count(&s, OUT_TO_TARGET),
        2,
        "the newly labelled node was invisible"
    );
}

/// A label removed from a node stops matching.
#[test]
fn a_label_removed_later_stops_matching() {
    let mut s = base();
    assert_eq!(count(&s, OUT_TO_TARGET), 1);

    run(&mut s, "MATCH (t:Target {name:\"t1\"}) REMOVE t:Target");
    assert_eq!(
        count(&s, OUT_TO_TARGET),
        0,
        "a node kept matching a label it no longer has"
    );
}

/// A deleted node stops matching.
#[test]
fn a_deleted_node_stops_matching() {
    let mut s = base();
    assert_eq!(count(&s, OUT_TO_TARGET), 1);

    run(&mut s, "MATCH (t:Target {name:\"t1\"}) DETACH DELETE t");
    assert_eq!(count(&s, OUT_TO_TARGET), 0, "a deleted node still matched");
}

/// Removing the *last* member of a label makes the label absent, which matches
/// nothing — distinct from "no label required" (#592, #520).
#[test]
fn the_last_member_leaving_makes_the_label_match_nothing() {
    let mut s = base();
    assert_eq!(count(&s, OUT_TO_TARGET), 1);
    run(&mut s, "MATCH (t:Target {name:\"t1\"}) REMOVE t:Target");
    // And a pattern requiring it still matches nothing rather than everything.
    assert_eq!(count(&s, OUT_TO_TARGET), 0);
    assert_eq!(count(&s, "MATCH (a:Src)-[:R]->(b) RETURN count(b) AS n"), 1);
}

/// The bitset is indexed by node id, so a graph whose ids run past one 64-bit
/// word still answers correctly — an off-by-one in the word index would show
/// up only past the 64th node.
#[test]
fn a_graph_wider_than_one_word_is_indexed_correctly() {
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (:Src {name: \"s\"})");
    for i in 0..200 {
        run(&mut s, &format!("CREATE (:Filler {{i: {i}}})"));
    }
    // The only :Target sits well past the first word.
    run(&mut s, "CREATE (:Target {name: \"far\"})");
    run(&mut s, "MATCH (a:Src), (b:Target) CREATE (a)-[:R]->(b)");
    assert_eq!(count(&s, OUT_TO_TARGET), 1);
    assert_eq!(
        count(&s, "MATCH (a:Src)-[:R]->(b:Filler) RETURN count(b) AS n"),
        0
    );
}
