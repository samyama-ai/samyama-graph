//! `PROFILE` attributes wall-clock to the operators that spent it
//! (`CH-PROFILE-01`).
//!
//! The gate this serves is stated as a fraction — *at least 90% of wall-clock
//! attributed* on the LDBC complex reads — so the properties worth testing are
//! the ones that fraction depends on: every operator in the plan appears, the
//! numbers nest correctly, and profiling does not change the answer.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// A graph with enough shape that a plan has several operators: a scan, an
/// expand, a filter, an aggregate and a sort.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let people: Vec<_> = (0..200)
        .map(|i| {
            let id = store.create_node("Person");
            let _ = store.set_node_property("default", id, "age".to_string(), PropertyValue::Integer(i % 60));
            let _ = store.set_node_property(
                "default",
                id,
                "name".to_string(),
                PropertyValue::String(format!("p{i}")),
            );
            id
        })
        .collect();
    for (i, &src) in people.iter().enumerate() {
        for d in 1..=6 {
            let _ = store.create_edge(src, people[(i + d * 7) % people.len()], "KNOWS");
        }
    }
    store
}

fn profile_text(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(cypher).expect("query should parse");
    let executor = QueryExecutor::new(store);
    let batch = executor.execute(&query).expect("PROFILE should execute");
    assert_eq!(batch.records.len(), 1, "PROFILE returns one row of plan text");
    match batch.records[0].get("plan") {
        Some(samyama::query::executor::Value::Property(PropertyValue::String(text))) => text.clone(),
        other => panic!("expected a plan string, got {other:?}"),
    }
}

#[test]
fn the_profile_names_every_operator_in_the_plan() {
    let store = fixture();
    let text = profile_text(
        &store,
        "PROFILE MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 20 \
         RETURN f.name, count(p) AS c ORDER BY c DESC LIMIT 10",
    );

    assert!(text.contains("--- Profile (per operator) ---"), "{text}");
    for operator in ["Scan", "Expand", "Filter"] {
        assert!(
            text.contains(operator),
            "the plan tree should name {operator}:\n{text}"
        );
    }
}

#[test]
fn the_report_states_how_much_of_the_wall_clock_it_attributed() {
    // Without this line the report cannot be checked against the gate, which
    // is written as a percentage rather than as a list of operators.
    let store = fixture();
    let text = profile_text(&store, "PROFILE MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name LIMIT 50");

    assert!(text.contains("attributed to operators"), "{text}");
    assert!(text.contains("Hottest operators by exclusive time"), "{text}");

    let line = text
        .lines()
        .find(|l| l.contains("attributed to operators"))
        .expect("attribution line");
    let pct: f64 = line
        .rsplit('(')
        .next()
        .and_then(|s| s.split('%').next())
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("could not read a percentage from {line:?}"));
    assert!(
        (0.0..=101.0).contains(&pct),
        "attributed fraction {pct} is not a percentage: {line}"
    );
}

#[test]
fn profiling_reports_the_same_rows_the_plain_query_returns() {
    // A profile of a plan that answers differently from the real one is worse
    // than no profile: it sends the reader to the wrong operator.
    let store = fixture();
    let cypher = "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 30 RETURN f.name LIMIT 25";

    let plain = {
        let query = parse_query(cypher).unwrap();
        QueryExecutor::new(&store).execute(&query).unwrap().records.len()
    };

    let text = profile_text(&store, &format!("PROFILE {cypher}"));
    let rows_line = text
        .lines()
        .find(|l| l.starts_with("Rows: "))
        .expect("the profile states its row count");
    assert!(
        rows_line.contains(&format!("Rows: {plain},")),
        "profile reported {rows_line:?} but the plain query returned {plain} rows"
    );
}

#[test]
fn the_instrumentation_cost_is_reported_rather_than_hidden() {
    // Two Instant::now() per next() is not free on a row-at-a-time plan. A
    // profile that presents its own inflated total as the query's latency is
    // how an optimisation gets chosen against a number that does not exist.
    let store = fixture();
    let text = profile_text(&store, "PROFILE MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name");

    assert!(
        text.contains("Uninstrumented execution of the same plan"),
        "the plain total must be printed alongside the instrumented one:\n{text}"
    );
    assert!(
        text.contains("take absolute latency from the benchmark"),
        "and the reader must be told which number to quote:\n{text}"
    );
}

#[test]
fn explain_is_unchanged_by_the_profiling_work() {
    // EXPLAIN must not start reporting times it did not measure.
    let store = fixture();
    let query = parse_query("EXPLAIN MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let text = match batch.records[0].get("plan") {
        Some(samyama::query::executor::Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("expected a plan string, got {other:?}"),
    };
    assert!(!text.contains("--- Profile (per operator) ---"), "{text}");
    assert!(text.contains("Expand"), "{text}");
}

#[test]
fn a_plan_with_one_operator_still_profiles() {
    let store = fixture();
    let text = profile_text(&store, "PROFILE MATCH (p:Person) RETURN p LIMIT 1");
    assert!(text.contains("--- Profile (per operator) ---"), "{text}");
    assert!(!text.contains("no operators instrumented"), "{text}");
}
