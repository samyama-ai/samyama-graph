//! A DELETE finishes before the next clause reads the graph (#994).
//!
//! ```cypher
//! MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2.num
//! ```
//!
//! over two `:A` nodes returned `2` and `null` where Cypher returns two
//! nulls. The MERGE matched a node the DELETE had already been asked to
//! remove.
//!
//! Rows were pulled one at a time, so the first row's MERGE ran when only the
//! first node was gone and matched the second -- which was itself about to be
//! deleted. The openCypher scenario is named for precisely this: *"merges
//! should not be able to match on deleted nodes"*.
//!
//! There was already an eager barrier *below* the delete, from #899, which
//! materialises its **input** so a write cannot un-produce rows the read had
//! already matched. This adds one *above*, draining its **output**. The two
//! solve opposite halves of the same question and neither implies the other.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, Value};
use samyama::query::parser::parse_query;

fn two_a_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    for n in [1i64, 2] {
        let id = store.create_node_with_labels([Label::new("A")]);
        store.set_node_property("default", id, "num", PropertyValue::Integer(n)).unwrap();
    }
    store
}

fn run(store: &mut GraphStore, cypher: &str) -> Vec<Value> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    r.records.iter().map(|rec| rec.get(&c).cloned().unwrap_or(Value::Null)).collect()
}

#[test]
fn a_merge_cannot_match_a_node_the_same_query_deleted() {
    let mut store = two_a_nodes();
    let got = run(&mut store, "MATCH (a:A) DELETE a MERGE (a2:A) RETURN a2.num");
    assert_eq!(got.len(), 2, "got {got:?}");
    assert!(got.iter().all(|v| v.is_null()), "got {got:?}");
    // +1 created, -2 deleted.
    assert_eq!(store.node_count(), 1);
}

#[test]
fn a_match_after_a_delete_does_not_see_the_deleted_nodes() {
    let mut store = two_a_nodes();
    let got = run(&mut store, "MATCH (a:A) DELETE a WITH count(*) AS n MATCH (b:A) RETURN b.num");
    assert!(got.is_empty(), "got {got:?}");
}

#[test]
fn the_delete_still_does_not_un_produce_rows_the_read_matched() {
    // #899, the barrier on the other side. Deleting the edge must not stop the
    // second row from being produced.
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    let b = store.create_node_with_labels([Label::new("B")]);
    store.create_edge(a, b, "R").unwrap();
    let got = run(&mut store, "MATCH (x)-[r]-(y) DELETE r RETURN count(*) AS n");
    assert_eq!(format!("{:?}", got[0]).contains("Integer(2)"), true, "got {got:?}");
}

#[test]
fn a_plain_delete_still_deletes() {
    let mut store = two_a_nodes();
    run(&mut store, "MATCH (a:A) DELETE a RETURN count(*) AS n");
    assert_eq!(store.node_count(), 0);
}
