//! An UNWIND written before a WITH runs before it (#927).
//!
//! ```cypher
//! MATCH (n:N) UNWIND [n] AS x WITH x RETURN x    -- VariableNotFound("n")
//! ```
//!
//! The parser only ever fills `query.unwind_clause` while no WITH has been
//! seen; anything after one goes to `post_with_unwind_clauses`. So an
//! `unwind_clause` is **always** written before the WITH. The planner decided
//! otherwise from `unwind_leading`, which answers a different question — "does
//! the *query* open with UNWIND" — and so sent `MATCH … UNWIND … WITH` down
//! the trailing path, where the barrier runs first and the unwind's list
//! expression then reads variables the WITH has already dropped.
//!
//! The other half is #785's trap, which this change walked straight into on
//! its first attempt: applying the unwind at the head *and* at the trailing
//! site runs it twice, and ten rows become a hundred. Both halves are tested
//! here, because a fix for one that breaks the other looks like progress in
//! the pass count.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("M");
    store.create_edge(a, b, "REL").unwrap();
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .len()
}

#[test]
fn a_with_after_an_unwind_does_not_lose_the_match_scope() {
    let store = graph();
    assert_eq!(rows(&store, "MATCH (n:N) UNWIND [n] AS x WITH x RETURN x"), 1);
}

#[test]
fn the_unwind_reads_variables_the_with_drops() {
    // The whole point: `[n, r]` is evaluated before the WITH projects them
    // away, so the WITH may legitimately keep neither.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n:N)-[r:REL]->(m:M) UNWIND [n, r] AS x WITH x RETURN x"),
        2
    );
}

#[test]
fn the_unwind_runs_exactly_once() {
    // Applying it at the head *and* at the trailing site gives the cross
    // product with itself: four rows here, a hundred in the TCK scenario that
    // caught it.
    let store = graph();
    assert_eq!(rows(&store, "MATCH (n:N) UNWIND [1, 2] AS x RETURN x"), 2);
    assert_eq!(rows(&store, "MATCH (n:N) UNWIND [1, 2] AS x WITH x RETURN x"), 2);
    assert_eq!(rows(&store, "UNWIND [1, 2] AS x RETURN x"), 2);
    assert_eq!(rows(&store, "UNWIND [1, 2] AS x WITH x RETURN x"), 2);
}

#[test]
fn consecutive_unwinds_still_stack() {
    let store = graph();
    assert_eq!(
        rows(&store, "UNWIND [1, 2] AS a UNWIND [3, 4] AS b RETURN a, b"),
        4
    );
    assert_eq!(
        rows(&store, "MATCH (n:N) UNWIND [1, 2] AS a UNWIND [3, 4] AS b WITH a, b RETURN a, b"),
        4
    );
}

#[test]
fn an_unwind_after_a_with_still_reads_what_the_with_projected() {
    // #785's own case, which lives in `post_with_unwind_clauses` and must stay
    // on the far side of the barrier.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n:N) WITH [1, 2, 3] AS l UNWIND l AS x RETURN x"),
        3
    );
}

#[test]
fn the_unwound_values_are_the_entities_not_placeholders() {
    let store = graph();
    let query =
        parse_query("MATCH (n:N)-[r:REL]->(m:M) UNWIND [n, r] AS x WITH x RETURN x").unwrap();
    let kinds: Vec<&'static str> = QueryExecutor::new(&store)
        .execute(&query)
        .unwrap()
        .records
        .iter()
        .map(|rec| match rec.get("x") {
            Some(Value::Node(..)) | Some(Value::NodeRef(_)) => "node",
            Some(Value::Edge(..)) | Some(Value::EdgeRef(..)) => "rel",
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(kinds, vec!["node", "rel"]);
}

#[test]
fn a_where_over_the_unwound_variable_still_filters() {
    // The predicate cannot be evaluated during match planning -- the variable
    // is not bound until the Unwind runs -- so it is held back and re-applied
    // above it. Moving the Unwind is exactly the change that could strand it.
    //
    // Both forms, because both route through the clause pipeline, which
    // rebuilds a by-kind query and calls the same planner -- so a fix that
    // only reached one of the two AST shapes would look like it worked.
    let store = graph();
    assert_eq!(
        rows(&store, "MATCH (n:N) UNWIND [1, 2, 3] AS x WHERE x > 1 RETURN x"),
        2
    );
    assert_eq!(
        rows(&store, "MATCH (n:N) UNWIND [1, 2, 3] AS x WHERE x > 1 WITH x RETURN x"),
        2
    );
    // And the filter is not applied twice or skipped: all three survive.
    assert_eq!(
        rows(&store, "MATCH (n:N) UNWIND [1, 2, 3] AS x WHERE x > 0 RETURN x"),
        3
    );
}
