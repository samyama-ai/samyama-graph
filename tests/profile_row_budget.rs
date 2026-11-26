//! `PROFILE` is refused by the row budget at the same point as the plain
//! statement (#2290).
//!
//! PROFILE runs the plan twice: once with every operator wrapped for timing,
//! then plain. The row budget wraps only operators that say they amplify
//! rows, and the timing wrapper used to answer "no" for all of them. So the
//! profiled run built a runaway cartesian product in full, and the refusal
//! came from the plain run afterwards: 60 s instead of 0.04 s on the issue's
//! 20,000 nodes.
//!
//! Measured in rows, not seconds: a work meter counts every row the plan's
//! operators produce, and the profiled statement is compared with the plain
//! one in the same process. Before the fix the profiled statement did the
//! whole 9,000,000-row product first, about 90 times the plain refusal's work.

use samyama::graph::GraphStore;
use samyama::query::executor::budget::WorkMeter;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

const NODES: usize = 3_000;
const BUDGET: u64 = 100_000;

fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    for _ in 0..NODES {
        store.create_node("U");
    }
    store
}

/// The refusal, and the rows produced before it.
fn refused(store: &GraphStore, cypher: &str) -> (String, u64) {
    let meter = WorkMeter::new(None);
    let err = QueryExecutor::new(store)
        .with_row_budget(BUDGET)
        .with_work_meter(meter.clone())
        .execute(&parse_query(cypher).expect("parses"))
        .map(|_| ())
        .expect_err("a 9,000,000-row product is past a 100,000-row budget");
    (format!("{err}"), meter.produced())
}

#[test]
fn profile_of_a_runaway_product_stops_where_the_plain_statement_does() {
    let store = fixture();
    let (plain, plain_rows) = refused(&store, "MATCH (a:U), (b:U) RETURN count(*)");
    assert!(plain.contains("RowBudgetExceeded"), "plain: {plain}");

    let (profiled, profiled_rows) = refused(&store, "PROFILE MATCH (a:U), (b:U) RETURN count(*)");
    assert!(
        profiled.contains("RowBudgetExceeded"),
        "profiled: {profiled}"
    );
    assert!(
        profiled.contains("CartesianProduct"),
        "names the operator: {profiled}"
    );
    assert!(
        profiled_rows <= plain_rows * 2,
        "PROFILE produced {profiled_rows} rows before its refusal, the plain statement \
         {plain_rows}: the profiled run is not budgeted"
    );
}

#[test]
fn profile_under_the_budget_still_reports_the_plan() {
    let store = fixture();
    let out = QueryExecutor::new(&store)
        .with_row_budget(BUDGET)
        .execute(&parse_query("PROFILE MATCH (a:U) RETURN count(a)").expect("parses"))
        .expect("a scan is not budgeted");
    assert_eq!(out.columns, vec!["plan".to_string()]);
}
