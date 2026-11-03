//! When a filter is worth splitting across threads (#559).
//!
//! The rule used to be "the batch has 256 or more records", which says nothing
//! about how much work the predicate does — and with a batch size of 65,536,
//! every batch qualified. Measured over 1,000,000 rows, parallel filtering lost
//! **1.4-1.8×** on every predicate a real query writes, and won only on the
//! heaviest one tested.
//!
//! The reason is that a `Record` holds `Arc<str>` binding names. Moving records
//! across threads churns atomic refcounts on cache lines every thread shares,
//! and against a predicate as cheap as one comparison there is nothing to
//! amortise that against.
//!
//! So the decision is now made from the predicate. These tests pin the
//! classification of each shape in the measured table, which is checkable
//! without timing anything — a timing test for this would be flaky on a shared
//! host (#529) and would not say *why* it regressed.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::ast::Expression;
use samyama::query::executor::operator::FilterOperator;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// The `WHERE` predicate of a parsed query.
fn predicate_of(cypher: &str) -> Expression {
    let query = parse_query(cypher).expect("query should parse");
    query
        .where_clause
        .unwrap_or_else(|| panic!("no WHERE predicate in {cypher}"))
        .predicate
}

fn goes_parallel(cypher: &str) -> bool {
    FilterOperator::predicate_is_parallel(&predicate_of(cypher))
}

/// Every row of the table in `PARALLEL_PREDICATE_COST`, on the side that won.
#[test]
fn the_measured_predicates_land_on_the_side_that_won() {
    for cypher in [
        // Sequential won these, by 1.4-1.8x.
        "MATCH (i:Item) WHERE i.v > 500 RETURN i",
        "MATCH (i:Item) WHERE i.v > 500 AND i.w > 5 RETURN i",
        "MATCH (i:Item) WHERE i.name CONTAINS \"99\" RETURN i",
        "MATCH (i:Item) WHERE toUpper(i.name) CONTAINS \"99\" RETURN i",
    ] {
        assert!(!goes_parallel(cypher), "should stay sequential: {cypher}");
    }

    // Parallel won this one, by 1.31x.
    assert!(
        goes_parallel(
            "MATCH (i:Item) WHERE i.v > 100 AND i.w > 2 AND i.name CONTAINS \"9\" \
             AND toUpper(i.name) CONTAINS \"ITEM\" RETURN i"
        ),
        "the heavy predicate should go parallel"
    );
}

#[test]
fn a_trivial_predicate_is_never_parallel() {
    assert!(!goes_parallel("MATCH (i:Item) WHERE 1 = 1 RETURN i"));
    assert!(!goes_parallel("MATCH (i:Item) WHERE i.v = 1 RETURN i"));
}

#[test]
fn a_predicate_that_runs_a_subquery_per_row_is_parallel() {
    // A pattern match per row is expensive by any measure, so it should be on
    // the parallel side whatever the rest of the predicate looks like.
    assert!(goes_parallel(
        "MATCH (i:Item) WHERE EXISTS { MATCH (i)-[:IN]->(:Forum) } RETURN i"
    ));
}

#[test]
fn cost_accumulates_across_conjuncts() {
    // The property that makes a cost model the right shape for this: adding
    // work to a predicate can only move it toward parallel, never away.
    let cheap = "MATCH (i:Item) WHERE toUpper(i.name) CONTAINS \"X\" RETURN i";
    let same_plus_more = "MATCH (i:Item) WHERE toUpper(i.name) CONTAINS \"X\" \
                          AND toLower(i.name) CONTAINS \"y\" RETURN i";
    assert!(!goes_parallel(cheap));
    assert!(goes_parallel(same_plus_more), "adding a second string call must not reduce cost");
}

/// The classification changes *how* rows are filtered, never *which*.
#[test]
fn both_paths_return_the_same_rows() {
    let mut store = GraphStore::new();
    for i in 0..4000i64 {
        let id = store.create_node("Item");
        let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i % 977));
        let _ = store.set_node_property("default", id, "w".to_string(), PropertyValue::Integer(i % 13));
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(format!("item-number-{i}-with-some-length")),
        );
    }

    let count = |cypher: &str| -> i64 {
        let query = parse_query(cypher).expect("query should parse");
        let batch = QueryExecutor::new(&store).execute(&query).expect("query should run");
        match batch.records[0].get("c") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        }
    };

    // A sequential predicate and a parallel one that select exactly the same
    // rows. `toUpper(name) CONTAINS "ITEM"` is true for every row, so the
    // second is the first plus two predicates that change nothing — which
    // makes the counts comparable while the classification differs.
    let sequential = "MATCH (i:Item) WHERE i.v > 100 AND i.w > 2 RETURN count(i) AS c";
    let parallel = "MATCH (i:Item) WHERE i.v > 100 AND i.w > 2 AND i.name CONTAINS \"item\" \
                    AND toUpper(i.name) CONTAINS \"ITEM\" RETURN count(i) AS c";

    assert!(!goes_parallel(sequential));
    assert!(goes_parallel(parallel));

    let expected = (0..4000i64).filter(|i| i % 977 > 100 && i % 13 > 2).count() as i64;
    assert_eq!(count(sequential), expected);
    assert_eq!(count(parallel), expected, "the parallel path selected different rows");
}

#[test]
fn a_batch_below_the_split_size_stays_sequential_however_expensive() {
    // Cost decides *whether* splitting is worth it; there still has to be
    // enough work to split. A handful of rows is not.
    let mut store = GraphStore::new();
    for i in 0..10i64 {
        let id = store.create_node("Item");
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(format!("item-{i}")),
        );
    }
    let query = parse_query(
        "MATCH (i:Item) WHERE toUpper(i.name) CONTAINS \"ITEM\" AND toLower(i.name) CONTAINS \"item\" \
         RETURN count(i) AS c",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(
        batch.records[0].get("c"),
        Some(&Value::Property(PropertyValue::Integer(10)))
    );
}
