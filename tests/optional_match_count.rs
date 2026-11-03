//! `count(var)` counts non-null bindings, including under OPTIONAL MATCH
//! (#600).
//!
//! `AggregateOperator` had a fast path that skipped evaluating a `count`
//! argument and just counted rows. Its precondition was "the argument cannot be
//! null per row — `count(*)`, or `count(var)` for a bound node or edge".
//!
//! `OPTIONAL MATCH` is precisely the case that breaks it: it binds the variable
//! to `Null` on a row that did not match. So
//! `MATCH (p) OPTIONAL MATCH (p)-[:KNOWS]->(f) RETURN p, count(f)` reported
//! **one friend for a person with none** — the natural way to write "how many
//! friends does each person have", answered wrongly and silently.
//!
//! The contrast that pinned it: `count(f.name)` was already correct at 2, and
//! `count(f)` said 3. Both should say 2.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Ada -> Bob -> Cy. Cy has no outgoing KNOWS, so it is the row that goes null.
fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let _ = store.set_node_property("default", a, "name".to_string(), PropertyValue::String("Ada".into()));
    let b = store.create_node("P");
    let _ = store.set_node_property("default", b, "name".to_string(), PropertyValue::String("Bob".into()));
    let c = store.create_node("P");
    let _ = store.set_node_property("default", c, "name".to_string(), PropertyValue::String("Cy".into()));
    store.create_edge(a, b, "KNOWS").unwrap();
    store.create_edge(b, c, "KNOWS").unwrap();
    store
}

fn scalar(store: &GraphStore, cypher: &str) -> i64 {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{cypher}: {other:?}"),
    }
}

/// `(name, count)` pairs, sorted, for a grouped query.
fn grouped(store: &GraphStore, cypher: &str) -> Vec<(String, i64)> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let mut out: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let name = match r.get("g") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("{other:?}"),
            };
            let n = match r.get("r") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("{other:?}"),
            };
            (name, n)
        })
        .collect();
    out.sort();
    out
}

#[test]
fn count_of_a_variable_ignores_the_unmatched_row() {
    let store = graph();
    assert_eq!(
        scalar(&store, "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(f) AS r"),
        2,
        "Cy matched nothing, so f is null on that row and must not be counted"
    );
}

#[test]
fn count_of_a_property_was_already_right_and_stays_right() {
    // The contrast that located the bug. If these two ever disagree again, the
    // fast path has been widened back.
    let store = graph();
    assert_eq!(
        scalar(&store, "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(f.name) AS r"),
        2
    );
}

#[test]
fn count_star_still_counts_rows() {
    // `count(*)` is the case the fast path is *for*, and it must keep counting
    // every row including the unmatched one.
    let store = graph();
    assert_eq!(
        scalar(&store, "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(*) AS r"),
        3
    );
}

#[test]
fn the_grouped_form_reports_zero_for_the_person_with_none() {
    // The shape that matters in practice: "how many friends does each person
    // have". Cy has none and was being reported as having one.
    let store = graph();
    assert_eq!(
        grouped(
            &store,
            "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS r"
        ),
        vec![("Ada".to_string(), 1), ("Bob".to_string(), 1), ("Cy".to_string(), 0)]
    );
}

#[test]
fn count_distinct_over_an_optional_null() {
    let store = graph();
    assert_eq!(
        scalar(
            &store,
            "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(DISTINCT f) AS r"
        ),
        2
    );
}

#[test]
fn a_required_match_is_unaffected() {
    // The common case, where no binding can be null. This is IC5's shape and
    // the one the fast path was introduced for, so it must not have changed
    // answer.
    let store = graph();
    assert_eq!(
        scalar(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN count(f) AS r"),
        2
    );
    assert_eq!(
        grouped(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS r"),
        vec![("Ada".to_string(), 1), ("Bob".to_string(), 1)],
        "a required match drops the unmatched row entirely, rather than nulling it"
    );
}

#[test]
fn counting_an_edge_variable_behaves_the_same_way() {
    let store = graph();
    assert_eq!(
        scalar(&store, "MATCH (p:P) OPTIONAL MATCH (p)-[e:KNOWS]->(:P) RETURN count(e) AS r"),
        2
    );
}

#[test]
fn two_counts_in_one_aggregate() {
    // Mixed arguments in one aggregate: the fast path is all-or-nothing, so a
    // `count(*)` beside a `count(var)` must not drag the variable back onto it.
    let store = graph();
    let query = parse_query(
        "MATCH (p:P) OPTIONAL MATCH (p)-[:KNOWS]->(f:P) RETURN count(*) AS a, count(f) AS b",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let get = |k: &str| match batch.records[0].get(k) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    };
    assert_eq!(get("a"), 3, "count(*) counts rows");
    assert_eq!(get("b"), 2, "count(f) counts non-null bindings");
}
