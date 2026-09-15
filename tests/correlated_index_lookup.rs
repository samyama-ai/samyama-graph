//! A batch lookup probes the index once per row (samyama-graph#1219).
//!
//! `UNWIND $rows AS r MATCH (n:N) WHERE n.id = r.id` never used the index on
//! `:N(id)`: the equality spans the MATCH and the UNWIND, so it was applied
//! as a filter after a cartesian product of every row with every `:N`.
//! Measured on 200,000 nodes, 1,000 rows took 63 s, where 1,000 single
//! lookups took 10 ms.
//!
//! For one labelled node with no relationships, pinned by an indexed equality
//! to an expression over variables already bound, the planner now probes the
//! index per row. The answers must be exactly those of the scan; every test
//! below compares the two.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into()).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

/// Column `c`, sorted.
fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::String(x))) => x.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect();
    v.sort();
    v
}

fn plan(s: &GraphStore, q: &str) -> String {
    let p = parse_query(&format!("EXPLAIN {q}")).unwrap();
    let out = QueryExecutor::new(s).execute(&p).unwrap();
    match out.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("no plan: {other:?}"),
    }
}

/// 500 `:N {id, v}`, with and without an index on `:N(id)`.
fn stores() -> (GraphStore, GraphStore) {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "UNWIND range(1, 500) AS i CREATE (:N {id: i, v: 'v' + toString(i)})");
        write(s, "CREATE (:A {k: 9}), (:A {k: 11})");
    }
    write(&mut indexed, "CREATE INDEX ON :N(id)");
    (indexed, plain)
}

fn same_both_ways(q: &str, expected: &[&str]) {
    let (indexed, plain) = stores();
    assert_eq!(rows(&indexed, q), expected, "indexed: {q}");
    assert_eq!(rows(&plain, q), expected, "scan: {q}");
}

#[test]
fn duplicates_missing_and_null_keys() {
    same_both_ways("UNWIND [3, 7, 7, 999, null] AS i MATCH (n:N) WHERE n.id = i RETURN n.id AS c", &["3", "7", "7"]);
}

#[test]
fn operands_either_way_round_and_a_map_key() {
    same_both_ways("UNWIND [{id: 5}, {id: 6}] AS r MATCH (n:N) WHERE r.id = n.id RETURN n.v AS c", &["v5", "v6"]);
}

#[test]
fn a_filter_on_the_node_still_applies() {
    same_both_ways(
        "UNWIND [1, 2, 3, 4] AS i MATCH (n:N) WHERE n.id = i AND n.id % 2 = 0 RETURN n.id AS c",
        &["2", "4"],
    );
}

#[test]
fn after_a_with() {
    same_both_ways("MATCH (a:A) WITH a.k AS k MATCH (n:N) WHERE n.id = k RETURN n.id AS c", &["11", "9"]);
}

#[test]
fn a_string_key() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:S {k: 'a'}), (:S {k: 'b'})");
    write(&mut s, "CREATE INDEX ON :S(k)");
    assert_eq!(rows(&s, "UNWIND ['b', 'x'] AS key MATCH (s:S) WHERE s.k = key RETURN s.k AS c"), vec!["b"]);
}

#[test]
fn a_deleted_node_is_not_found() {
    let (mut indexed, _) = stores();
    write(&mut indexed, "MATCH (n:N {id: 3}) DETACH DELETE n");
    assert_eq!(rows(&indexed, "UNWIND [3, 4] AS i MATCH (n:N) WHERE n.id = i RETURN n.id AS c"), vec!["4"]);
}

/// The point of the change: the indexed batch lookup is planned as a probe.
#[test]
fn the_index_is_probed_per_row_when_there_is_one() {
    let (indexed, plain) = stores();
    let q = "UNWIND [3, 7] AS i MATCH (n:N) WHERE n.id = i RETURN n.id AS c";
    assert!(plan(&indexed, q).contains("CorrelatedIndexLookup"), "{}", plan(&indexed, q));
    assert!(!plan(&plain, q).contains("CorrelatedIndexLookup"), "{}", plan(&plain, q));
    let after_with = "MATCH (a:A) WITH a.k AS k MATCH (n:N) WHERE n.id = k RETURN n.id AS c";
    assert!(plan(&indexed, after_with).contains("CorrelatedIndexLookup"), "{}", plan(&indexed, after_with));
}

/// A pattern with one relationship probes the index, then expands from the
/// node found (`tests/lookup_then_hop.rs` covers the shapes).
#[test]
fn a_pattern_with_a_relationship_probes_then_expands() {
    let (mut indexed, _) = stores();
    write(&mut indexed, "MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:R]->(b)");
    let q = "UNWIND [1] AS i MATCH (n:N)-[:R]->(m) WHERE n.id = i RETURN m.id AS c";
    assert!(plan(&indexed, q).contains("CorrelatedIndexLookup"), "{}", plan(&indexed, q));
    assert_eq!(rows(&indexed, q), vec!["2"]);
}
