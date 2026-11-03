//! `CREATE (a), (b) RETURN a, b` must bind every path's variables, not the
//! first path's (#614).
//!
//! `CREATE (a:X {n:1}), (b:Y {n:2}) RETURN a.n, b.n` failed with
//! "Variable not found: b" while the single-path form worked. Comma-separated
//! paths in one CREATE are the ordinary way to make several disconnected
//! nodes, so this is a common shape rather than a corner.
//!
//! The nodes were created correctly — only the binding was lost, which is why
//! it surfaced as a RETURN error rather than missing data.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(store: &mut GraphStore, cypher: &str) -> Vec<Vec<(String, Value)>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = MutQueryExecutor::new(store, "default".to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    let cols = out.columns.clone();
    out.records
        .iter()
        .map(|r| cols.iter().map(|c| (c.clone(), r.get(c).cloned().unwrap_or(Value::Null))).collect())
        .collect()
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap();
    match QueryExecutor::new(store).execute(&q).unwrap().records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("expected integer, got {other:?}"),
    }
}

#[test]
fn two_comma_separated_paths_bind_both_variables() {
    let mut store = GraphStore::new();
    let rows = write(&mut store, "CREATE (a:X {n:1}), (b:Y {n:2}) RETURN a.n, b.n");
    assert_eq!(rows.len(), 1, "one row, got {rows:?}");
    let vals: Vec<&Value> = rows[0].iter().map(|(_, v)| v).collect();
    assert_eq!(vals[0], &Value::Property(PropertyValue::Integer(1)), "a.n");
    assert_eq!(vals[1], &Value::Property(PropertyValue::Integer(2)), "b.n");
}

#[test]
fn three_paths_bind_all_three() {
    // Not just the second: a fix that special-cased two would pass the test
    // above and still be wrong.
    let mut store = GraphStore::new();
    let rows = write(
        &mut store,
        "CREATE (a:X {n:1}), (b:Y {n:2}), (c:Z {n:3}) RETURN a.n, b.n, c.n",
    );
    let vals: Vec<&Value> = rows[0].iter().map(|(_, v)| v).collect();
    assert_eq!(vals[0], &Value::Property(PropertyValue::Integer(1)));
    assert_eq!(vals[1], &Value::Property(PropertyValue::Integer(2)));
    assert_eq!(vals[2], &Value::Property(PropertyValue::Integer(3)));
}

#[test]
fn a_relationship_path_beside_a_node_path_binds_both() {
    let mut store = GraphStore::new();
    let rows = write(
        &mut store,
        "CREATE (a:X {n:1})-[:R]->(b:X {n:2}), (c:Z {n:3}) RETURN a.n, b.n, c.n",
    );
    let vals: Vec<&Value> = rows[0].iter().map(|(_, v)| v).collect();
    assert_eq!(vals[0], &Value::Property(PropertyValue::Integer(1)));
    assert_eq!(vals[1], &Value::Property(PropertyValue::Integer(2)));
    assert_eq!(vals[2], &Value::Property(PropertyValue::Integer(3)));
}

#[test]
fn the_nodes_are_created_regardless() {
    // The data was always right; only the binding was lost. Pinning this so a
    // fix to the binding cannot quietly change what gets written.
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (a:X {n:1}), (b:Y {n:2})").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string()).execute(&q).unwrap();
    assert_eq!(count(&store, "MATCH (n:X) RETURN count(n) AS c"), 1);
    assert_eq!(count(&store, "MATCH (n:Y) RETURN count(n) AS c"), 1);
}

#[test]
fn a_single_path_still_binds_its_variables() {
    // Control: the form that already worked.
    let mut store = GraphStore::new();
    let rows = write(&mut store, "CREATE (a:X {n:1}) RETURN a.n");
    assert_eq!(rows[0][0].1, Value::Property(PropertyValue::Integer(1)));
}
