//! Filtering an expansion's target by label (#592).
//!
//! `ExpandOperator` used to collect every incident edge and then `retain` the
//! ones whose target carried the pattern's labels, testing each with
//! `get_node(id).has_label(label)` — a `Vec` index, a version-chain walk, a
//! 128-byte `Node`, and a `HashSet<Label>` probe that hashes a **string**. At
//! 2.22M edges visited per LDBC IC9 run that was **26.7% of the profile**, the
//! largest single symbol, ahead of every property read.
//!
//! The labels are now applied during the walk, by probing `label_index` with
//! the target's `NodeId`. These tests are about the cases where "during" and
//! "afterwards" could differ, and where a set-membership test could differ from
//! asking the node:
//!
//! * **a label no node carries** — the set is absent, not empty, and the
//!   expansion must match nothing rather than everything;
//! * **more than one label** — every one must hold, not any;
//! * **`Direction::Both`** — the old code recovered the far end with an
//!   expression that consulted the store; getting the near/far end backwards
//!   here filters on the wrong node and is invisible on a symmetric fixture;
//! * **multi-label nodes**, since a node satisfying one required label and not
//!   another must be excluded.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// A hub with three kinds of neighbour, so a label filter has something to
/// exclude in both directions.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");
    let _ = store.set_node_property("default", hub, "name".to_string(), PropertyValue::String("h".into()));

    for i in 0..10i64 {
        // Outgoing: hub -> Post
        let post = store.create_node("Post");
        let _ = store.set_node_property("default", post, "n".to_string(), PropertyValue::Integer(i));
        store.create_edge(hub, post, "WROTE").unwrap();

        // Outgoing: hub -> Comment, same edge type
        let comment = store.create_node("Comment");
        let _ = store.set_node_property("default", comment, "n".to_string(), PropertyValue::Integer(i));
        store.create_edge(hub, comment, "WROTE").unwrap();

        // Incoming: Person -> hub
        let person = store.create_node("Person");
        let _ = store.set_node_property("default", person, "n".to_string(), PropertyValue::Integer(i));
        store.create_edge(person, hub, "LIKES").unwrap();
    }
    store
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    match batch.records.first().and_then(|r| r.get("c")) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        None => 0,
        other => panic!("{other:?}"),
    }
}

#[test]
fn an_outgoing_expansion_keeps_only_the_labelled_targets() {
    let store = fixture();
    // 20 WROTE edges, half to Post and half to Comment.
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]->(x) RETURN count(x) AS c"), 20);
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]->(x:Post) RETURN count(x) AS c"), 10);
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]->(x:Comment) RETURN count(x) AS c"), 10);
}

#[test]
fn an_incoming_expansion_filters_the_source_end() {
    let store = fixture();
    assert_eq!(count(&store, "MATCH (h:Hub)<-[:LIKES]-(x:Person) RETURN count(x) AS c"), 10);
    assert_eq!(count(&store, "MATCH (h:Hub)<-[:LIKES]-(x:Post) RETURN count(x) AS c"), 0);
}

#[test]
fn an_undirected_expansion_filters_the_far_end_not_the_near_one() {
    // The case that would be invisible on a symmetric fixture: `Both` walks
    // out-edges and in-edges, and the node to test is the *other* end each
    // time. Testing the hub instead would return everything.
    let store = fixture();
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]-(x:Post) RETURN count(x) AS c"), 10);
    assert_eq!(count(&store, "MATCH (h:Hub)-[:LIKES]-(x:Person) RETURN count(x) AS c"), 10);
    // And a label that only the *hub* carries must match nothing, which is what
    // filtering the near end by mistake would get wrong.
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]-(x:Hub) RETURN count(x) AS c"), 0);
}

#[test]
fn a_label_no_node_carries_matches_nothing() {
    // `label_index` has no entry, so the set is *absent* rather than empty.
    // Treating absent as "no constraint" would return every edge — the same
    // class of bug as an unknown edge type matching everything (#520).
    let store = fixture();
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]->(x:NoSuchLabel) RETURN count(x) AS c"), 0);
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]-(x:NoSuchLabel) RETURN count(x) AS c"), 0);
}

#[test]
fn every_required_label_must_hold_not_any() {
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");

    // One node with both labels, one with each.
    let both = store.create_node("Post");
    // Through the store, so `label_index` is maintained -- which is what the
    // expansion now probes.
    store.add_label_to_node("default", both, Label::new("Archived")).expect("add label");
    let only_post = store.create_node("Post");
    let only_archived = store.create_node("Archived");
    for t in [both, only_post, only_archived] {
        store.create_edge(hub, t, "WROTE").unwrap();
    }

    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]->(x:Post) RETURN count(x) AS c"), 2);
    assert_eq!(count(&store, "MATCH (h:Hub)-[:WROTE]->(x:Archived) RETURN count(x) AS c"), 2);
    assert_eq!(
        count(&store, "MATCH (h:Hub)-[:WROTE]->(x:Post:Archived) RETURN count(x) AS c"),
        1,
        "only the node carrying both"
    );
}

#[test]
fn an_unlabelled_pattern_keeps_everything() {
    let store = fixture();
    assert_eq!(count(&store, "MATCH (h:Hub)-[]-(x) RETURN count(x) AS c"), 30);
}

#[test]
fn a_multi_hop_pattern_filters_at_each_hop() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    for i in 0..5 {
        let b = store.create_node("B");
        store.create_edge(a, b, "R").unwrap();
        for j in 0..3 {
            let c = store.create_node(if j == 0 { "C" } else { "D" });
            store.create_edge(b, c, "R").unwrap();
        }
    }
    assert_eq!(count(&store, "MATCH (a:A)-[:R]->(b:B)-[:R]->(c:C) RETURN count(c) AS c"), 5);
    assert_eq!(count(&store, "MATCH (a:A)-[:R]->(b:B)-[:R]->(d:D) RETURN count(d) AS c"), 10);
    assert_eq!(count(&store, "MATCH (a:A)-[:R]->(b:D)-[:R]->(c) RETURN count(c) AS c"), 0);
}
