//! An O(1) count shortcut may not answer a query that writes (#993).
//!
//! ```cypher
//! MATCH (a:A) DELETE a RETURN count(*) AS n
//! ```
//!
//! returned `n = 2` and **left both nodes in place**.
//!
//! `use_label_count` replaces the entire plan with a `LabelCountOperator`
//! metadata read, discarding the operator tree built above it -- which is
//! where `DeleteOperator` lives. Its guards checked for a WHERE, a WITH,
//! multiple MATCHes, segments, inline properties, and `count(x.prop)` versus
//! `count(*)`. They did not check whether the query *writes*.
//!
//! The rule was already written down one guard lower in the same expression:
//! the shortcut may only fire when the pattern says nothing the metadata
//! cannot express. A write is the largest such thing there is.
//!
//! This is worse than a wrong answer. A wrong answer is at least an answer.
//! Here the caller asked for a deletion, got a plausible confirmation count
//! back, and the data is still there -- and the number returned is *correct
//! for the query as executed*, being the count of rows that matched.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;

fn two_a_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    for n in [1i64, 2] {
        let id = store.create_node_with_labels([Label::new("A")]);
        store.set_node_property("default", id, "num", PropertyValue::Integer(n)).unwrap();
    }
    store
}

fn run(store: &mut GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

#[test]
fn a_delete_with_an_aggregate_return_actually_deletes() {
    for q in [
        "MATCH (a:A) DELETE a RETURN count(*) AS n",
        "MATCH (a:A) DELETE a RETURN count(a) AS n",
        "MATCH (a:A) DETACH DELETE a RETURN count(*) AS n",
    ] {
        let mut store = two_a_nodes();
        assert_eq!(run(&mut store, q), 1, "{q} should return one aggregate row");
        assert_eq!(store.node_count(), 0, "{q} reported a count and deleted nothing");
    }
}

#[test]
fn a_set_with_an_aggregate_return_actually_sets() {
    let mut store = two_a_nodes();
    run(&mut store, "MATCH (a:A) SET a.num = 9 RETURN count(*) AS n");
    let all = store.all_nodes();
    assert!(
        all.iter().all(|n| n.properties.get("num") == Some(&PropertyValue::Integer(9))),
        "SET was discarded by the shortcut",
    );
}

#[test]
fn a_remove_with_an_aggregate_return_actually_removes() {
    let mut store = two_a_nodes();
    run(&mut store, "MATCH (a:A) REMOVE a.num RETURN count(*) AS n");
    let all = store.all_nodes();
    assert!(all.iter().all(|n| n.properties.get("num").is_none()), "REMOVE was discarded");
}

#[test]
fn the_shortcut_still_fires_for_a_read() {
    // The optimisation must survive: a plain count over a label is still the
    // O(1) metadata read, and still correct.
    let mut store = two_a_nodes();
    assert_eq!(run(&mut store, "MATCH (a:A) RETURN count(*) AS n"), 1);
    assert_eq!(store.node_count(), 2);
}
