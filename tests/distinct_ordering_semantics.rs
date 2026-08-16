//! `RETURN DISTINCT` is evaluated before `ORDER BY`, `SKIP` and `LIMIT` (#522).
//!
//! openCypher's clause order for a `RETURN` is: project → deduplicate → order →
//! skip → limit. Deduplicating *after* the slice is a silent wrong answer: the
//! query succeeds, every row it returns is a valid row, and only the count is
//! wrong — which reads as sparse data rather than as a bug.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Ten people, eight of them in the same city.
///
/// The skew is the point: a fixture where distinct values are evenly spread
/// hides this bug, because the first n rows happen to contain n distinct
/// values. The original manual check passed for exactly that reason.
fn skewed_cities() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..10 {
        let id = store.create_node("Person");
        let city = if i < 8 {
            "Amsterdam"
        } else if i == 8 {
            "Berlin"
        } else {
            "Cairo"
        };
        let _ = store.set_node_property(
            "default",
            id,
            "city".to_string(),
            PropertyValue::String(city.to_string()),
        );
    }
    store
}

fn column(store: &GraphStore, cypher: &str, name: &str) -> Vec<String> {
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should execute");
    batch
        .records
        .iter()
        .map(|r| match r.get(name) {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn limit_counts_distinct_rows() {
    // Was `[Amsterdam]`: LIMIT 3 took three of the eight Amsterdams and
    // DISTINCT collapsed them to one.
    let rows = column(
        &skewed_cities(),
        "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c LIMIT 3",
        "c",
    );
    assert_eq!(rows, vec!["Amsterdam", "Berlin", "Cairo"]);
}

#[test]
fn limit_smaller_than_the_distinct_count_still_returns_exactly_limit() {
    let rows = column(
        &skewed_cities(),
        "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c LIMIT 2",
        "c",
    );
    assert_eq!(rows, vec!["Amsterdam", "Berlin"]);
}

#[test]
fn skip_counts_distinct_rows() {
    // Was `[Amsterdam, Berlin, Cairo]`: SKIP 1 dropped one raw Amsterdam, the
    // other seven deduplicated to one, and nothing was skipped at all.
    let rows = column(
        &skewed_cities(),
        "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c SKIP 1",
        "c",
    );
    assert_eq!(rows, vec!["Berlin", "Cairo"]);
}

#[test]
fn skip_and_limit_together_count_distinct_rows() {
    let rows = column(
        &skewed_cities(),
        "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c SKIP 1 LIMIT 1",
        "c",
    );
    assert_eq!(rows, vec!["Berlin"]);
}

#[test]
fn distinct_without_a_slice_is_unchanged() {
    let mut rows = column(&skewed_cities(), "MATCH (p:Person) RETURN DISTINCT p.city AS c", "c");
    rows.sort();
    assert_eq!(rows, vec!["Amsterdam", "Berlin", "Cairo"]);
}

#[test]
fn ordering_survives_deduplication() {
    // DistinctOperator is first-occurrence-wins and streaming, so it must not
    // disturb the order the sort produced. Descending, so a stable-but-wrong
    // implementation that re-sorted ascending would be caught.
    let rows = column(
        &skewed_cities(),
        "MATCH (p:Person) RETURN DISTINCT p.city AS c ORDER BY c DESC",
        "c",
    );
    assert_eq!(rows, vec!["Cairo", "Berlin", "Amsterdam"]);
}

#[test]
fn limit_without_distinct_is_unaffected() {
    let rows = column(&skewed_cities(), "MATCH (p:Person) RETURN p.city AS c LIMIT 3", "c");
    assert_eq!(rows.len(), 3, "a plain LIMIT still returns raw rows: {rows:?}");
}

#[test]
fn distinct_over_several_columns_deduplicates_on_the_whole_row() {
    let mut store = GraphStore::new();
    for i in 0..12 {
        let id = store.create_node("Person");
        // 3 distinct (city, band) pairs, heavily skewed towards the first.
        let (city, band) = if i < 9 {
            ("Amsterdam", 1)
        } else if i < 11 {
            ("Amsterdam", 2)
        } else {
            ("Berlin", 1)
        };
        let _ = store.set_node_property(
            "default",
            id,
            "city".to_string(),
            PropertyValue::String(city.to_string()),
        );
        let _ = store.set_node_property("default", id, "band".to_string(), PropertyValue::Integer(band));
    }

    let query = parse_query("MATCH (p:Person) RETURN DISTINCT p.city AS c, p.band AS b LIMIT 3").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(batch.records.len(), 3, "three distinct (city, band) pairs exist");
}

#[test]
fn the_plan_puts_distinct_below_the_limit() {
    // The property, not just the symptom: if a later change moves DISTINCT
    // back above the slice, this fails without needing a fixture skewed the
    // right way to expose it.
    let store = skewed_cities();
    let query = parse_query("EXPLAIN MATCH (p:Person) RETURN DISTINCT p.city AS c LIMIT 3").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let plan = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("expected a plan string, got {other:?}"),
    };

    let limit_at = plan.find("Limit").expect("plan should contain a Limit");
    let distinct_at = plan.find("Distinct").expect("plan should contain a Distinct");
    assert!(
        limit_at < distinct_at,
        "Limit must sit above Distinct in the tree, so Distinct runs first:\n{plan}"
    );
}
