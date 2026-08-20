//! A single `WHERE` produces a single `Filter` (#519).
//!
//! The predicate decomposition hands each MATCH the conjuncts that reference
//! only its own variables, and the match planner attaches them inside the
//! subplan. The top-level `WHERE` filter then applied the whole predicate
//! again, so a one-MATCH query with a compound `WHERE` evaluated it twice per
//! row — on LDBC IC9, 389,461 rows through two identical `Filter`s, the upper
//! one removing nothing.
//!
//! These assert on `EXPLAIN` rather than on timings: the defect is a plan
//! shape, and a plan shape is checkable without a profiler.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let people: Vec<_> = (0..60)
        .map(|i| {
            let id = store.create_node("Person");
            let _ = store.set_node_property("default", id, "age".to_string(), PropertyValue::Integer(i % 50));
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
        let _ = store.create_edge(src, people[(i + 3) % people.len()], "KNOWS");
    }
    store
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("EXPLAIN should run");
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("expected a plan string, got {other:?}"),
    }
}

/// `Filter` operators in the plan tree.
///
/// EXPLAIN appends planner diagnostics and graph statistics below the tree,
/// and the diagnostics list every *candidate* plan -- which contain filters of
/// their own. Counting the whole output would count plans that were considered
/// and rejected.
fn count_filters(plan: &str) -> usize {
    plan.lines()
        .take_while(|l| !l.starts_with("---"))
        .filter(|l| l.trim_start().trim_start_matches("+- ").starts_with("Filter"))
        .count()
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).expect("query should parse");
    QueryExecutor::new(store).execute(&query).expect("query should run").records.len()
}

#[test]
fn a_compound_where_on_one_match_plans_one_filter() {
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 10 AND p.age < 40 RETURN f.name",
    );
    assert_eq!(
        count_filters(&text),
        1,
        "the same predicate was planted twice:\n{text}"
    );
}

#[test]
fn a_query_with_no_where_plans_no_filter() {
    let store = fixture();
    let text = plan(&store, "MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.name");
    assert_eq!(count_filters(&text), 0, "{text}");
}

#[test]
fn a_single_predicate_plans_one_filter() {
    let store = fixture();
    let text = plan(&store, "MATCH (p:Person) WHERE p.age > 10 RETURN p.name");
    assert!(count_filters(&text) <= 1, "{text}");
}

#[test]
fn deduplicating_does_not_change_the_answer() {
    // `x AND x` is idempotent, so dropping the second evaluation cannot change
    // the result -- but the whole point of the guard is that it only fires when
    // the predicates are genuinely equal, so this is worth asserting rather
    // than assuming.
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (p:Person) WHERE p.age > 10 AND p.age < 40 RETURN p.name"),
        rows(&store, "MATCH (p:Person) WHERE p.age > 10 AND p.age < 40 RETURN p.name"),
    );

    let expected = (0..60).filter(|i| (i % 50) > 10 && (i % 50) < 40).count();
    assert_eq!(
        rows(&store, "MATCH (p:Person) WHERE p.age > 10 AND p.age < 40 RETURN p.name"),
        expected
    );
}

#[test]
fn a_predicate_split_across_two_variables_plans_each_conjunct_once() {
    // The IC9 shape: one conjunct pushes to the scan, the other lands above
    // the expand, and the top-level WHERE used to apply both again -- three
    // evaluations of the same work.
    let store = fixture();
    let text = plan(
        &store,
        "MATCH (p:Person)-[:KNOWS]->(f:Person) WHERE p.age > 10 AND f.age < 40 RETURN f.name",
    );
    assert_eq!(count_filters(&text), 2, "one filter per conjunct, no re-application:\n{text}");
}

#[test]
fn a_where_after_an_optional_match_scopes_to_the_optional_match() {
    // This test used to assert the opposite — that the top-level filter must
    // be kept, so the row with a NULL `f` is rejected — and it was the stated
    // justification for re-applying the whole WHERE whenever an OPTIONAL MATCH
    // is present.
    //
    // That is not what Cypher does. `WHERE` after an `OPTIONAL MATCH` scopes
    // to the optional match: a person with no qualifying friend keeps their
    // row with `f` NULL, rather than being deleted. TCK MatchWhere6 [6] is the
    // same shape and Neo4j 5 returns all three rows, the null one included —
    // measured, not assumed (#667).
    //
    // The old expectation is what made `MATCH (x) OPTIONAL MATCH ... WHERE
    // y.val > 4` return one row where Cypher returns three.
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let _ = store.set_node_property("default", a, "age".to_string(), PropertyValue::Integer(20));
    let lonely = store.create_node("Person");
    let _ = store.set_node_property("default", lonely, "age".to_string(), PropertyValue::Integer(20));
    let b = store.create_node("Person");
    let _ = store.set_node_property("default", b, "age".to_string(), PropertyValue::Integer(90));
    let _ = store.create_edge(a, b, "KNOWS");

    // Every person survives: `a` with `f` bound to the 90-year-old, `lonely`
    // and `b` with `f` NULL because nothing satisfied the predicate.
    let cypher = "MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS]->(f:Person) WHERE f.age > 50 RETURN p";
    assert_eq!(
        rows(&store, cypher),
        3,
        "the WHERE scopes to the OPTIONAL MATCH; it nulls `f`, it does not drop rows"
    );
}

#[test]
fn a_predicate_spanning_two_matches_still_gets_its_own_filter() {
    // The guard must not fire here. The decomposition cannot push a
    // cross-MATCH predicate into either subplan, so the top-level filter is
    // the only thing applying it -- removing it would be a wrong answer, not
    // an optimisation.
    let store = fixture();
    let cypher = "MATCH (a:Person), (b:Person) WHERE a.age > b.age RETURN a.name, b.name LIMIT 5";
    let text = plan(&store, cypher);
    assert!(
        count_filters(&text) >= 1,
        "the cross-MATCH predicate must still be applied:\n{text}"
    );
    assert_eq!(rows(&store, cypher), 5, "and it must still return rows");
}

#[test]
fn a_where_that_is_only_partly_pushable_keeps_the_top_level_filter() {
    let store = fixture();
    let cypher = "MATCH (a:Person), (b:Person) WHERE a.age > 40 AND a.age > b.age RETURN a.name LIMIT 3";
    assert!(count_filters(&plan(&store, cypher)) >= 1);
    assert_eq!(rows(&store, cypher), 3);
}

#[test]
fn filtering_still_actually_filters() {
    // The failure mode of getting this wrong is a query that returns rows it
    // should have excluded, so check the predicate is doing work at all.
    let store = fixture();
    let all = rows(&store, "MATCH (p:Person) RETURN p.name");
    let filtered = rows(&store, "MATCH (p:Person) WHERE p.age > 45 RETURN p.name");
    assert!(filtered < all, "filtered {filtered} vs all {all}");
    assert_eq!(filtered, (0..60).filter(|i| (i % 50) > 45).count());
}
