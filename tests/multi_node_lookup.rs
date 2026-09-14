//! A batch lookup of several nodes in one MATCH probes the index per node.
//!
//! The bulk relationship load,
//! `UNWIND $rels AS r MATCH (a:N {id: r.src}), (b:N {id: r.dst}) CREATE (a)-[:R]->(b)`,
//! was planned as a cartesian product of two full label scans under the
//! UNWIND, then filtered: 200,000 x 200,000 pairs per row, which never
//! finishes. #1222 probes the index for a MATCH of one node; this covers a
//! MATCH whose every pattern is one node, each pinned by an indexed equality
//! to the unwound value or to a node looked up before it. Every answer here
//! is compared with the scan's on a graph small enough to scan.

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
    let out = QueryExecutor::new(s).execute(&parse_query(&format!("EXPLAIN {q}")).unwrap()).unwrap();
    match out.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("no plan: {other:?}"),
    }
}

/// 30 `:N {id, next}`, with and without an index on `:N(id)`.
fn stores() -> (GraphStore, GraphStore) {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "UNWIND range(1, 30) AS i CREATE (:N {id: i, next: i + 1, name: 'n' + toString(i)})");
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
fn two_lookups_from_one_row() {
    same_both_ways(
        "UNWIND [{s: 1, d: 2}, {s: 5, d: 9}, {s: 7, d: 99}] AS r \
         MATCH (a:N), (b:N) WHERE a.id = r.s AND b.id = r.d RETURN a.name + '-' + b.name AS c",
        &["n1-n2", "n5-n9"],
    );
}

#[test]
fn the_second_key_reads_the_first_node() {
    same_both_ways(
        "UNWIND [3, 10] AS i MATCH (a:N), (b:N) WHERE a.id = i AND b.id = a.next RETURN b.name AS c",
        &["n11", "n4"],
    );
}

#[test]
fn a_filter_on_a_node_still_applies() {
    same_both_ways(
        "UNWIND [1, 2, 3, 4] AS i MATCH (a:N), (b:N) WHERE a.id = i AND b.id = i + 1 AND b.id % 2 = 0 \
         RETURN a.name AS c",
        &["n1", "n3"],
    );
}

/// The bulk relationship load, inline form (legal since #1218).
#[test]
fn bulk_relationship_load_creates_exactly_the_edges() {
    let (mut indexed, mut plain) = stores();
    for s in [&mut indexed, &mut plain] {
        write(
            s,
            "UNWIND [{src: 1, dst: 2}, {src: 2, dst: 3}, {src: 4, dst: 99}] AS r \
             MATCH (a:N {id: r.src}), (b:N {id: r.dst}) CREATE (a)-[:R]->(b)",
        );
    }
    let q = "MATCH (a:N)-[:R]->(b:N) RETURN toString(a.id) + '>' + toString(b.id) AS c";
    assert_eq!(rows(&indexed, q), vec!["1>2", "2>3"]);
    assert_eq!(rows(&plain, q), vec!["1>2", "2>3"]);
}

/// The point of the change: each node is a probe, not a scan.
#[test]
fn each_node_is_probed_when_indexed() {
    let (indexed, plain) = stores();
    let q = "UNWIND [1, 2] AS i MATCH (a:N), (b:N) WHERE a.id = i AND b.id = i + 1 RETURN count(*) AS c";
    let p = plan(&indexed, q);
    assert_eq!(p.matches("CorrelatedIndexLookup").count(), 2, "{p}");
    assert!(!p.contains("CartesianProduct"), "{p}");
    assert!(!plan(&plain, q).contains("CorrelatedIndexLookup"));
}

/// A node with no key of its own keeps today's plan, and its answer.
#[test]
fn a_node_without_a_key_keeps_its_plan() {
    let (indexed, _) = stores();
    let q = "UNWIND [1] AS i MATCH (a:N), (b:N) WHERE a.id = i AND b.id > 28 RETURN b.name AS c";
    assert!(!plan(&indexed, q).contains("CorrelatedIndexLookup"));
    assert_eq!(rows(&indexed, q), vec!["n29", "n30"]);
}
