//! `SKIP` and `LIMIT` take an expression (#912).
//!
//! ```cypher
//! MATCH (n) RETURN n LIMIT toInteger(ceil(1.7))   -- parse error
//! MATCH (n) WITH n SKIP toInteger(rand() * 9) ...  -- parse error
//! ```
//!
//! The grammar accepted an integer literal and nothing else, so these failed to
//! parse at all.
//!
//! Cypher requires the expression **not to depend on variables**, and that rule
//! is what makes folding it at parse time correct rather than a shortcut: it is
//! evaluated once, against no row, so `LIMIT toInteger(rand() * 9)` picks one
//! number for the whole query instead of a different one per row — which is the
//! specified behaviour. The rest of the engine goes on receiving the `usize` it
//! already expected, so nothing downstream changed.
//!
//! Six parse sites did this inline, each testing for `Rule::integer` and
//! silently ignoring anything else. They share one function now, because
//! "silently ignoring anything else" is how a LIMIT goes missing.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn ten_nodes() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("UNWIND range(1, 10) AS i CREATE ({nr: i})").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` parses: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` runs: {e}"))
        .records
        .len()
}

#[test]
fn limit_takes_an_expression() {
    let store = ten_nodes();
    assert_eq!(rows(&store, "MATCH (n) RETURN n LIMIT 2"), 2, "the literal still works");
    assert_eq!(rows(&store, "MATCH (n) RETURN n LIMIT toInteger(ceil(1.7))"), 2);
    assert_eq!(rows(&store, "MATCH (n) RETURN n LIMIT 1 + 1"), 2);
    assert_eq!(rows(&store, "MATCH (n) RETURN n LIMIT toInteger('3')"), 3);
}

#[test]
fn skip_takes_an_expression() {
    let store = ten_nodes();
    assert_eq!(rows(&store, "MATCH (n) RETURN n SKIP 8"), 2);
    assert_eq!(rows(&store, "MATCH (n) RETURN n SKIP toInteger(ceil(7.2))"), 2);
    assert_eq!(rows(&store, "MATCH (n) WITH n SKIP 4 RETURN n"), 6);
    assert_eq!(rows(&store, "MATCH (n) WITH n SKIP toInteger(2 * 2) RETURN n"), 6);
}

/// Evaluated once, not per row — so a random bound still trims to a single
/// number and the query returns a consistent count.
#[test]
fn a_random_bound_is_evaluated_once() {
    let store = ten_nodes();
    for _ in 0..8 {
        let n = rows(&store, "MATCH (n) WITH n SKIP toInteger(rand() * 9) RETURN n");
        assert!((1..=10).contains(&n), "got {n} rows, which is not a single consistent trim");
    }
}

/// A bound that cannot be a row count says so, rather than being dropped.
///
/// The inline sites ignored anything that was not `Rule::integer`, so an
/// unusable bound became *no bound at all* — the query silently returned every
/// row.
#[test]
fn an_unusable_bound_is_refused_not_dropped() {
    for cypher in [
        "MATCH (n) RETURN n LIMIT 'two'",
        "MATCH (n) RETURN n LIMIT 1.5",
        "MATCH (n) RETURN n LIMIT -1",
        "MATCH (n) RETURN n SKIP -1",
    ] {
        assert!(parse_query(cypher).is_err(), "accepted `{cypher}`");
    }
}

/// A bound that names a variable is refused: it would have to depend on the
/// rows it is trimming.
#[test]
fn a_bound_that_depends_on_a_row_is_refused() {
    assert!(parse_query("MATCH (n) RETURN n LIMIT n.count").is_err());
}
