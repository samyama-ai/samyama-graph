//! A WHERE after a post-WITH OPTIONAL MATCH is a join condition (#978).
//!
//! ```cypher
//! MATCH (a1)-[r]->()
//! WITH r, a1 LIMIT 1
//! OPTIONAL MATCH (a2)<-[r]-(b2)
//! WHERE a1 = a2
//! RETURN a1, r, b2, a2
//! ```
//!
//! returned **no rows** where Cypher returns one, with `b2` and `a2` null.
//!
//! Cypher scopes the WHERE after an OPTIONAL MATCH to the optional match: a
//! row failing it keeps the left side and nulls the right. Applied as a filter
//! *above* the join it deletes the row entirely — which is exactly what #667
//! established for the pre-WITH decomposition.
//!
//! That fix never reached the post-WITH path, which built its
//! `LeftOuterJoinOperator` without a join predicate at all. The same rule, in
//! the copy that did not have it.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    let b = store.create_node_with_labels([Label::new("B")]);
    store.create_edge(a, b, "T").unwrap();
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

#[test]
fn a_failing_predicate_nulls_the_right_side_rather_than_dropping_the_row() {
    let store = chain();
    assert_eq!(
        rows(
            &store,
            "MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 \
             OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2",
        ),
        1
    );
}

#[test]
fn the_right_side_really_is_null() {
    // Row count alone would also pass if the predicate were simply ignored.
    let store = chain();
    let q = parse_query(
        "MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 \
         OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, b2, a2",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    let rec = &batch.records[0];
    assert!(rec.get("a1").is_some());
    for c in ["b2", "a2"] {
        assert!(
            matches!(rec.get(c), Some(Value::Null) | None),
            "{c} should be null: {:?}",
            rec.get(c)
        );
    }
}

#[test]
fn a_predicate_that_holds_still_matches() {
    // The direction a fix that always null-filled would break.
    let store = chain();
    let q = parse_query(
        "MATCH (a1)-[r]->(x) WITH r, a1, x LIMIT 1 \
         OPTIONAL MATCH (a2)-[r]->(b2) WHERE a1 = a2 RETURN a2, b2",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(
        !matches!(batch.records[0].get("a2"), Some(Value::Null) | None),
        "the predicate holds, so a2 is bound: {:?}",
        batch.records[0].get("a2")
    );
}

#[test]
fn a_predicate_on_the_optional_clause_alone_still_pushes_down() {
    // Naming only the optional clause's own variables, it belongs *inside*
    // that clause where it can anchor the scan — routing it to the join would
    // cost that. #667's own caveat.
    let store = chain();
    assert_eq!(
        rows(&store, "MATCH (n) WITH n LIMIT 1 OPTIONAL MATCH (b:Nope) WHERE b.x = 1 RETURN n, b"),
        1
    );
}

#[test]
fn a_pre_with_optional_match_is_unchanged() {
    // #667's original case, which already worked.
    let store = chain();
    assert_eq!(
        rows(&store, "MATCH (x:A) OPTIONAL MATCH (x)-[:T]->(y) WHERE y.val > 4 RETURN x, y"),
        1
    );
}
