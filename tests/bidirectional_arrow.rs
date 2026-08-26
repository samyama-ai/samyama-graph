//! `<-->` is an edge in either direction (#868).
//!
//! ```text
//! MATCH p = (n)<-->(k)<-->(n) RETURN p    -- parse error
//! ```
//!
//! `edge_pattern` had three alternatives and none matched arrows on both ends;
//! `<-` matched the incoming form and left a stray `>`. The rule's own comment
//! already explained that alternative order is load-bearing — `-` `->` before
//! `-` `-` — and the both-ends form has the same requirement one step earlier.
//!
//! The second half matters more: `parse_edge` derives direction from the text
//! with `starts_with("<-")` → `Incoming`. Left alone, `<-->` would have parsed
//! and then been read as **incoming**, silently halving the matches rather than
//! failing. So the direction is asserted here by row count against a
//! single-direction graph, not by "it parses".

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:A)-[:R]->(b:B)").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"))
        .records
        .len()
}

/// One relationship, so an undirected pattern matches twice and a directed one
/// once. That difference is what distinguishes "parsed" from "parsed as the
/// right direction".
#[test]
fn both_ends_means_either_direction() {
    let s = store();
    assert_eq!(rows(&s, "MATCH (n)<-->(k) RETURN n, k"), 2);
    assert_eq!(rows(&s, "MATCH (n)--(k) RETURN n, k"), 2, "the equivalent spelling");
    assert_eq!(rows(&s, "MATCH (n)-->(k) RETURN n, k"), 1, "outgoing");
    assert_eq!(rows(&s, "MATCH (n)<--(k) RETURN n, k"), 1, "incoming");
}

/// With a bracket, on a single relationship.
#[test]
fn the_bracketed_forms() {
    let s = store();
    assert_eq!(rows(&s, "MATCH (n)<-[r]->(k) RETURN n, k"), 2);
    assert_eq!(rows(&s, "MATCH (n)<-[:R]->(k) RETURN n, k"), 2);
    assert_eq!(rows(&s, "MATCH p = (n)<-->(k) RETURN p"), 2);
}

/// A two-segment chain needs **two** relationships, because Cypher will not
/// reuse one within a single pattern.
///
/// My first version of this asserted 2 rows against the single-relationship
/// store above and got 0 — and 0 was right. The fixture and the expected counts
/// below are the TCK's own (`Match6` scenarios 12 and 13), not my arithmetic.
fn two_relationships() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:A), (b:B) CREATE (a)-[:T1]->(b), (b)-[:T2]->(a)")
        .expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

#[test]
fn a_chain_of_both_ends_arrows() {
    let s = two_relationships();
    assert_eq!(rows(&s, "MATCH p = (n)<-->(k)<-->(n) RETURN p"), 4);
}

/// **A both-ends arrow does not relax the arrow beside it.** The same chain
/// with a directed second segment matches half as often.
#[test]
fn a_mixed_chain_keeps_each_direction() {
    let s = two_relationships();
    assert_eq!(rows(&s, "MATCH p = (n)<-->(k)<--(n) RETURN p"), 2);
    assert_eq!(rows(&s, "MATCH p = (n)<-->(k)-->(n) RETURN p"), 2);
}

/// Relationship uniqueness still applies: one relationship cannot serve both
/// segments, which is why the single-relationship store yields nothing here.
#[test]
fn one_relationship_cannot_fill_two_segments() {
    let s = store();
    assert_eq!(rows(&s, "MATCH p = (n)<-->(k)<-->(n) RETURN p"), 0);
}
