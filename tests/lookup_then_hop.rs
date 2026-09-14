//! A batch lookup followed by one hop probes the index per row.
//!
//! `UNWIND $rows AS r MATCH (a:N {id: r.id})-[:R]->(b)` was planned as the
//! UNWIND over an expand from a full scan of `:N`, filtered afterwards: every
//! relationship of the label walked once per row. #1222/#1223 probe the index
//! for a MATCH of bare nodes; this adds one relationship hop off a looked-up
//! node. Every answer here is compared with the scan's on a graph small
//! enough to scan.

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

/// 30 `:N {id}` in a chain `(i)-[:R {w: i}]->(i+1)`, and `(i)-[:S]->(:M {id: i, k: i % 2})`
/// for i in 1..=10; with and without an index on `:N(id)`.
fn stores() -> (GraphStore, GraphStore) {
    let mut indexed = GraphStore::new();
    let mut plain = GraphStore::new();
    for s in [&mut indexed, &mut plain] {
        write(s, "UNWIND range(1, 30) AS i CREATE (:N {id: i})");
        write(s, "MATCH (a:N), (b:N) WHERE b.id = a.id + 1 CREATE (a)-[:R {w: a.id}]->(b)");
        write(s, "MATCH (a:N) WHERE a.id <= 10 CREATE (a)-[:S]->(:M {id: a.id, k: a.id % 2})");
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
fn outgoing_hop() {
    // 30 has no outgoing :R; 99 is not a node.
    same_both_ways("UNWIND [1, 5, 30, 99] AS i MATCH (a:N {id: i})-[:R]->(b) RETURN b.id AS c", &["2", "6"]);
}

#[test]
fn incoming_and_undirected_hops() {
    same_both_ways("UNWIND [1, 5] AS i MATCH (a:N {id: i})<-[:R]-(b) RETURN b.id AS c", &["4"]);
    same_both_ways("UNWIND [5] AS i MATCH (a:N {id: i})-[:R]-(b) RETURN b.id AS c", &["4", "6"]);
}

#[test]
fn relationship_variable_and_row_property_key() {
    same_both_ways("UNWIND [3, 4] AS i MATCH (a:N {id: i})-[r:R]->(b) RETURN r.w AS c", &["3", "4"]);
    same_both_ways("UNWIND [{s: 2}, {s: 7}] AS r MATCH (a:N {id: r.s})-[:R]->(b) RETURN b.id AS c", &["3", "8"]);
}

#[test]
fn target_label_types_and_inline_properties() {
    same_both_ways(
        "UNWIND [1, 2, 3, 4] AS i MATCH (a:N {id: i})-[:S|R]->(b:M {k: 0}) RETURN b.id AS c",
        &["2", "4"],
    );
    same_both_ways("UNWIND [1, 2] AS i MATCH (a:N {id: i})-[:S|R]->(b) RETURN labels(b)[0] AS c", &["M", "M", "N", "N"]);
}

#[test]
fn where_on_the_target_and_repeated_rows() {
    same_both_ways("UNWIND range(1, 10) AS i MATCH (a:N {id: i})-[:R]->(b) WHERE b.id > 8 RETURN b.id AS c", &["10", "11", "9"]);
    same_both_ways("UNWIND [2, 2] AS i MATCH (a:N {id: i})-[:R]->(b) RETURN b.id AS c", &["3", "3"]);
}

#[test]
fn a_hop_next_to_a_bare_node() {
    same_both_ways(
        "UNWIND [1, 4] AS i MATCH (a:N {id: i})-[:R]->(b), (d:N {id: i + 2}) RETURN b.id * 100 + d.id AS c",
        &["203", "506"],
    );
}

#[test]
fn a_later_match_correlated_to_an_earlier_one() {
    same_both_ways("MATCH (x:M {id: 3}) MATCH (a:N {id: x.id})-[:R]->(b) RETURN b.id AS c", &["4"]);
}

/// Shapes this does not plan must still answer correctly.
#[test]
fn shapes_left_to_the_general_plan() {
    same_both_ways("UNWIND [1] AS i MATCH (a:N {id: i})-[:R*1..2]->(b) RETURN b.id AS c", &["2", "3"]);
    same_both_ways("UNWIND [1, 2] AS i MATCH (a:N {id: i})-[:R]->(a) RETURN a.id AS c", &[]);
    same_both_ways("UNWIND [1] AS i MATCH (a:N {id: i})-[:R]->(b)-[:R]->(d) RETURN d.id AS c", &["3"]);
    same_both_ways("UNWIND [1] AS i MATCH p = (a:N {id: i})-[:R]->(b) RETURN length(p) AS c", &["1"]);
    same_both_ways("UNWIND [3] AS i MATCH (a:N {id: i})-[:R {w: 3}]->(b) RETURN b.id AS c", &["4"]);
    same_both_ways("UNWIND [1] AS i OPTIONAL MATCH (a:N)-[:S]->(b:M {k: 0}) WHERE a.id = i RETURN count(b) AS c", &["0"]);
}

#[test]
fn writes_after_the_hop() {
    let (mut indexed, mut plain) = stores();
    for s in [&mut indexed, &mut plain] {
        write(s, "UNWIND [1, 2, 30] AS i MATCH (a:N {id: i})-[:R]->(b) CREATE (b)-[:T]->(:Z {from: i})");
        assert_eq!(rows(s, "MATCH (b:N)-[:T]->(z:Z) RETURN b.id * 100 + z.from AS c"), vec!["201", "302"]);
    }
}

#[test]
fn the_plan_probes_the_index_instead_of_scanning() {
    let (indexed, _) = stores();
    let p = plan(&indexed, "UNWIND [1, 5] AS i MATCH (a:N {id: i})-[:R]->(b) RETURN b.id");
    assert!(p.contains("IndexLookup"), "{p}");
    assert!(!p.contains("NodeScan"), "{p}");
}
