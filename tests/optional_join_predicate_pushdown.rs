//! A join predicate must block the OPTIONAL MATCH pushdown (#982).
//!
//! ```cypher
//! MATCH (a)-[r {name: 'r1'}]-(b)
//! OPTIONAL MATCH (b)-[r2]-(c)
//! WHERE r <> r2
//! RETURN a, b, c
//! ```
//!
//! returned **3** rows where Cypher returns 2. The extra row was the one where
//! `r2` *is* `r` — the relationship the outer MATCH came in on, walked back the
//! other way. `r <> r2` exists precisely to exclude it.
//!
//! #726 pushes a single-segment OPTIONAL MATCH into an expand instead of a
//! join, and guarded that on the clause having no WHERE. But a predicate
//! spanning the optional side and an outer variable is classified as a *join*
//! predicate (#667), not a per-clause WHERE, so it leaves `per_match_where`
//! empty and the guard waved it through. The expand has nowhere to put a join
//! predicate, so `r <> r2` was not applied anywhere — a row count that is too
//! large by exactly the rows the predicate was written to remove.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(a)-[r1]->(b)-[r2]->(c)`, the two edges distinguishable by name.
fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node_with_labels([Label::new("A")]);
    let b = store.create_node_with_labels([Label::new("B")]);
    let c = store.create_node_with_labels([Label::new("C")]);
    let e1 = store.create_edge(a, b, "T").unwrap();
    let e2 = store.create_edge(b, c, "T").unwrap();
    store.set_edge_property(e1, "name", PropertyValue::String("r1".into())).unwrap();
    store.set_edge_property(e2, "name", PropertyValue::String("r2".into())).unwrap();
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

#[test]
fn the_predicate_excludes_the_relationship_matched_on() {
    let store = chain();
    let got = rows(
        &store,
        "MATCH (a)-[r {name: 'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2 RETURN a, b, c",
    );
    // (a)-[r1]-(b) matches both directions. From b, the only other edge is r2
    // to c; from a there is none, so that row nulls c.
    assert_eq!(got.len(), 2, "got {got:?}");
}

#[test]
fn a_row_with_no_surviving_match_still_emits_nulls() {
    let store = chain();
    let got = rows(
        &store,
        "MATCH (a)-[r {name: 'r2'}]->(b) OPTIONAL MATCH (b)-[r2]-(c) WHERE r <> r2 RETURN a, c",
    );
    // b is the far end (node c of the chain) and has only the one edge, which
    // the predicate removes. OPTIONAL MATCH keeps the row and nulls `c`.
    assert_eq!(got.len(), 1, "got {got:?}");
    assert_eq!(got[0][1], Value::Null, "c should be null: {got:?}");
}

#[test]
fn an_optional_match_with_no_predicate_still_pushes_down() {
    let store = chain();
    // The #726 pushdown must survive: no WHERE, so nothing to lose.
    let got = rows(&store, "MATCH (a)-[r {name: 'r1'}]-(b) OPTIONAL MATCH (b)-[r2]-(c) RETURN a, b, c");
    assert_eq!(got.len(), 3, "got {got:?}");
}
