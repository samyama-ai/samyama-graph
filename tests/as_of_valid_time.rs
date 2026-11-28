//! `MATCH <pattern> AS OF <time>` keeps what is valid at that time (NDS-03,
//! #1663).
//!
//! It parsed as an error before this, which is the one NDS-03 probe that
//! failed. The period is `[valid_from, valid_to)` on every node and
//! relationship the pattern names; a missing bound is open. This is valid
//! time, spec 04's H1 for NDS-03 -- not the graph as the database held it at a
//! past moment, which is H2/H3.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn dt(s: &str) -> String {
    format!("datetime('{s}T00:00:00Z')")
}

/// Four `:E`, one per shape of period, and two `:R` between them.
///
/// | id | valid_from | valid_to   |
/// |----|------------|------------|
/// | 1  | 2024-01-01 | 2024-07-01 |
/// | 2  | 2024-07-01 | (open)     |
/// | 3  | (open)     | 2024-03-01 |
/// | 4  | (open)     | (open)     |
///
/// `(1)-[:R {id: 10, valid_from: 2024-05-01}]->(4)` and `(2)-[:R {id: 11}]->(4)`.
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let q = format!(
        "CREATE (a:E {{id: 1, valid_from: {}, valid_to: {}}}), \
                (b:E {{id: 2, valid_from: {}}}), \
                (:E {{id: 3, valid_to: {}}}), \
                (d:E {{id: 4}}), \
                (a)-[:R {{id: 10, valid_from: {}}}]->(d), \
                (b)-[:R {{id: 11}}]->(d)",
        dt("2024-01-01"),
        dt("2024-07-01"),
        dt("2024-07-01"),
        dt("2024-03-01"),
        dt("2024-05-01"),
    );
    MutQueryExecutor::new(&mut s, "default".into())
        .execute(&parse_query(&q).unwrap())
        .unwrap();
    s
}

fn ids(s: &GraphStore, q: &str) -> Vec<i64> {
    let batch = QueryExecutor::new(s)
        .execute(&parse_query(q).unwrap_or_else(|e| panic!("{q}: {e}")))
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let mut v: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("{q}: {other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn as_of_keeps_the_nodes_whose_period_holds_the_time() {
    let s = store();
    let at = |d: &str| {
        ids(
            &s,
            &format!("MATCH (n:E) AS OF {} RETURN n.id AS id", dt(d)),
        )
    };
    assert_eq!(at("2024-02-01"), vec![1, 3, 4]);
    assert_eq!(at("2024-06-01"), vec![1, 4]);
    assert_eq!(at("2024-08-01"), vec![2, 4]);
}

/// `valid_from` is inclusive and `valid_to` exclusive, so at the instant one
/// period ends and the next begins exactly one of them holds.
#[test]
fn the_period_is_closed_at_the_start_and_open_at_the_end() {
    let s = store();
    let q = format!("MATCH (n:E) AS OF {} RETURN n.id AS id", dt("2024-07-01"));
    assert_eq!(ids(&s, &q), vec![2, 4]);
}

/// A node with neither bound is valid at every time: data written before
/// anyone thought about time must not vanish from an `AS OF` query.
#[test]
fn an_entity_with_no_period_is_always_valid() {
    let s = store();
    for d in ["1900-01-01", "2024-06-01", "2999-01-01"] {
        let q = format!("MATCH (n:E {{id: 4}}) AS OF {} RETURN n.id AS id", dt(d));
        assert_eq!(ids(&s, &q), vec![4], "{d}");
    }
}

/// Every element the pattern names is filtered -- the relationship as well as
/// the nodes at either end.
#[test]
fn as_of_filters_relationships_and_both_endpoints() {
    let s = store();
    let at = |d: &str| {
        ids(
            &s,
            &format!("MATCH (a:E)-[r:R]->(b:E) AS OF {} RETURN r.id AS id", dt(d)),
        )
    };
    // Edge 10 opens 2024-05-01; node 2 (edge 11's source) opens 2024-07-01.
    assert_eq!(at("2024-04-01"), Vec::<i64>::new());
    assert_eq!(at("2024-06-01"), vec![10]);
    // Node 1 has closed, taking edge 10 with it; node 2 has opened.
    assert_eq!(at("2024-08-01"), vec![11]);
}

/// `AS OF` and `WHERE` both apply.
#[test]
fn as_of_composes_with_where() {
    let s = store();
    let q = format!(
        "MATCH (n:E) AS OF {} WHERE n.id > 1 RETURN n.id AS id",
        dt("2024-02-01")
    );
    assert_eq!(ids(&s, &q), vec![3, 4]);
}

/// The answer is the one the spelled-out predicate gives.
#[test]
fn as_of_answers_what_the_written_out_predicate_answers() {
    let s = store();
    for d in [
        "2023-12-31",
        "2024-01-01",
        "2024-03-01",
        "2024-06-30",
        "2024-07-01",
        "2025-01-01",
    ] {
        let t = dt(d);
        let as_of = ids(&s, &format!("MATCH (n:E) AS OF {t} RETURN n.id AS id"));
        let written = ids(
            &s,
            &format!(
                "MATCH (n:E) WHERE (n.valid_from IS NULL OR n.valid_from <= {t}) \
                 AND (n.valid_to IS NULL OR n.valid_to > {t}) RETURN n.id AS id"
            ),
        );
        assert_eq!(as_of, written, "{d}");
    }
}

/// A pattern that names nothing gives `AS OF` nothing to filter. Running it as
/// though `AS OF` were absent would answer a different question silently.
#[test]
fn as_of_over_a_pattern_that_names_nothing_is_refused() {
    let err = parse_query(&format!(
        "MATCH (:E) AS OF {} RETURN count(*) AS c",
        dt("2024-06-01")
    ))
    .err()
    .map(|e| e.to_string())
    .unwrap_or_default();
    assert!(err.contains("AS OF"), "{err}");
}

/// The NDS-03 probe's exact text, which was a parse error.
#[test]
fn the_nds03_probe_runs() {
    let s = store();
    let q = "MATCH (n:N) AS OF datetime(\"2024-06-01T00:00:00Z\") RETURN count(n)";
    QueryExecutor::new(&s)
        .execute(&parse_query(q).unwrap_or_else(|e| panic!("{e}")))
        .unwrap_or_else(|e| panic!("{e}"));
}
