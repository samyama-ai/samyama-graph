//! The degree-counting rewrite answers the query that was asked (#601).
//!
//! `RETURN n.prop, count(neighbour)` over an expand is rewritten to
//! `AdjacencyCountAggregate`, which reads each source's **degree** off the
//! adjacency index. That is a good optimisation — it is why this shape does not
//! appear in profiles — but a degree is not a match count, and it was wrong two
//! ways at once:
//!
//! * it **ignored the neighbour's label**, because a degree counts every edge
//!   of the type whatever sits at the far end;
//! * it **emitted a row per scanned source**, so sources matching the pattern
//!   zero times appeared with count 0 — turning a required match into an
//!   optional one.
//!
//! The tell was that the same query grouped and ungrouped disagreed: `RETURN
//! count(f)` said 1 while `RETURN p.name, count(f)` said 2.
//!
//! These tests assert **the answer and that the rewrite is still taken**. Only
//! the first would let a future correctness fix silently disable the
//! optimisation; only the second would let it stay fast and wrong.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Ada KNOWS Bob (a `:P`) and Rex (an `:Animal`). Bob and Cy know nobody.
///
/// Built so the two defects give different wrong answers: ignoring the label
/// makes Ada 2 instead of 1, and keeping zero rows adds Bob and Cy.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let mut mk = |store: &mut GraphStore, label: &str, name: &str| {
        let id = store.create_node(label);
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let ada = mk(&mut store, "P", "Ada");
    let bob = mk(&mut store, "P", "Bob");
    let _cy = mk(&mut store, "P", "Cy");
    let rex = mk(&mut store, "Animal", "Rex");
    store.create_edge(ada, bob, "KNOWS").unwrap();
    store.create_edge(ada, rex, "KNOWS").unwrap();
    store
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<(String, i64)> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let mut out: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let g = match r.get("g") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("{other:?}"),
            };
            let n = match r.get("c") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("{other:?}"),
            };
            (g, n)
        })
        .collect();
    out.sort();
    out
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_neighbours_label_is_honoured() {
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c"),
        vec![("Ada".to_string(), 1)],
        "Rex is an :Animal and must not be counted for (f:P)"
    );
}

#[test]
fn an_unlabelled_neighbour_counts_everything() {
    // The shape the optimisation exists for, and the one it was always right
    // about. It must not have been broken by making the label case correct.
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (p:P)-[:KNOWS]->(f) RETURN p.name AS g, count(f) AS c"),
        vec![("Ada".to_string(), 2)]
    );
}

#[test]
fn a_source_matching_zero_times_yields_no_row() {
    // Bob and Cy know nobody. A *required* MATCH drops them; only
    // OPTIONAL MATCH would keep them with a zero.
    let store = fixture();
    let got = rows(&store, "MATCH (p:P)-[:KNOWS]->(f) RETURN p.name AS g, count(f) AS c");
    assert!(!got.iter().any(|(n, _)| n == "Bob" || n == "Cy"), "{got:?}");
}

#[test]
fn the_grouped_and_ungrouped_forms_agree() {
    // The tell that found this. Two ways of asking the same question must not
    // give different totals.
    let store = fixture();
    let grouped: i64 = rows(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c")
        .iter()
        .map(|(_, n)| n)
        .sum();

    let query = parse_query("MATCH (p:P)-[:KNOWS]->(f:P) RETURN count(f) AS c").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let ungrouped = match batch.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    };

    assert_eq!(grouped, ungrouped, "grouped {grouped} vs ungrouped {ungrouped}");
    assert_eq!(grouped, 1);
}

#[test]
fn the_rewrite_is_still_taken() {
    // Correctness without this would be easy: disable the rewrite. Asserted so
    // a future fix cannot quietly trade the optimisation away.
    let store = fixture();
    for cypher in [
        "MATCH (p:P)-[:KNOWS]->(f) RETURN p.name AS g, count(f) AS c",
        "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c",
    ] {
        let text = plan(&store, cypher);
        assert!(
            text.contains("AdjacencyCountAggregate"),
            "the rewrite was lost for `{cypher}`:\n{text}"
        );
    }
}

#[test]
fn an_incoming_pattern_is_filtered_too() {
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (f:P)<-[:KNOWS]-(p:P) RETURN f.name AS g, count(p) AS c"),
        vec![("Bob".to_string(), 1)],
        "Rex is not a :P, and Ada has no incoming KNOWS"
    );
}

#[test]
fn an_undirected_pattern_is_filtered_too() {
    let store = fixture();
    let got = rows(&store, "MATCH (p:P)-[:KNOWS]-(f:P) RETURN p.name AS g, count(f) AS c");
    assert_eq!(
        got,
        vec![("Ada".to_string(), 1), ("Bob".to_string(), 1)],
        "Ada-Bob counts once from each end; Rex and Cy never: {got:?}"
    );
}

#[test]
fn a_label_no_node_carries_counts_nothing() {
    // `label_index` has no entry, which is not the same as an empty one.
    let store = fixture();
    let got = rows(&store, "MATCH (p:P)-[:KNOWS]->(f:Ghost) RETURN p.name AS g, count(f) AS c");
    assert!(got.is_empty(), "{got:?}");
}

#[test]
fn the_answer_matches_expanding_the_pattern_by_hand() {
    // A differential check against the unrewritten form. `count(f.name)` is not
    // eligible for the rewrite, so it goes through the ordinary aggregate and
    // is the independent answer.
    let store = fixture();
    let by_rewrite = rows(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c");
    let by_aggregate = rows(
        &store,
        "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f.name) AS c",
    );
    assert_eq!(by_rewrite, by_aggregate);
}
