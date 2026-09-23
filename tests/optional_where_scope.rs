//! A WHERE after an OPTIONAL MATCH belongs to the optional match, whatever
//! variables it names (#1231).
//!
//! A row failing it keeps its outer bindings and gets nulls. The planner scoped
//! each conjunct by the variables it names, so one naming only outer variables
//! -- `OPTIONAL MATCH (y:Y) WHERE x.v > 1` -- was taken for a WHERE on the
//! outer MATCH and deleted the outer rows, and `WHERE false` deleted all of
//! them. Every expected answer here is Neo4j 2026.04.0's on the same graph,
//! including the controls: a WHERE on the plain MATCH still filters.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    MutQueryExecutor::new(s, "default".into()).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

fn cell(v: Option<&Value>) -> String {
    match v {
        Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
        Some(Value::Property(PropertyValue::Null)) | Some(Value::Null) | None => "null".into(),
        other => format!("{other:?}"),
    }
}

/// `:X {v: 1..3}`, `:Y {w: 10}`, `(:X {v: 2})-[:E]->(:Z {z: 7})`.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:X {v: 1}), (:X {v: 2}), (:X {v: 3}), (:Y {w: 10})");
    write(&mut s, "MATCH (x:X {v: 2}) CREATE (x)-[:E]->(:Z {z: 7})");
    s
}

/// Every row as `col1|col2|...`, sorted.
fn check(q: &str, expected: &[&str]) {
    let s = graph();
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(&s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| out.columns.iter().map(|c| cell(r.get(c))).collect::<Vec<_>>().join("|"))
        .collect();
    got.sort();
    assert_eq!(got, expected, "{q}");
}

#[test]
fn a_where_naming_only_the_outer_match() {
    check("MATCH (x:X) OPTIONAL MATCH (y:Y) WHERE x.v > 1 RETURN x.v, y.w", &["1|null", "2|10", "3|10"]);
}

#[test]
fn a_where_naming_only_an_unwound_variable() {
    check("UNWIND [1, 2, 3] AS i OPTIONAL MATCH (y:Y) WHERE i > 1 RETURN i, y.w", &["1|null", "2|10", "3|10"]);
    check(
        "UNWIND [1, 2, 3] AS i OPTIONAL MATCH (y:Y) WHERE i > 1 WITH i, y RETURN i, y.w",
        &["1|null", "2|10", "3|10"],
    );
}

#[test]
fn a_where_naming_only_the_shared_variable_of_a_hop() {
    check("MATCH (x:X) OPTIONAL MATCH (x)-[:E]->(z) WHERE x.v > 1 RETURN x.v, z.z", &["1|null", "2|7", "3|null"]);
}

#[test]
fn a_constant_where() {
    check("MATCH (x:X) OPTIONAL MATCH (y:Y) WHERE false RETURN x.v, y.w", &["1|null", "2|null", "3|null"]);
}

#[test]
fn after_a_with() {
    check("MATCH (x:X) WITH x OPTIONAL MATCH (y:Y) WHERE x.v > 1 RETURN x.v, y.w", &["1|null", "2|10", "3|10"]);
}

#[test]
fn a_conjunction_of_outer_and_optional_parts() {
    check(
        "MATCH (x:X) OPTIONAL MATCH (y:Y) WHERE x.v > 1 AND y.w = 10 RETURN x.v, y.w",
        &["1|null", "2|10", "3|10"],
    );
}

#[test]
fn two_optional_matches_each_with_its_own_where() {
    check(
        "MATCH (x:X) OPTIONAL MATCH (y:Y) WHERE x.v > 1 OPTIONAL MATCH (x)-[:E]->(z) WHERE x.v < 3 \
         RETURN x.v, y.w, z.z",
        &["1|null|null", "2|10|7", "3|10|null"],
    );
}

/// Controls, already right: a WHERE on the plain MATCH filters, a spanning
/// WHERE is the join condition (#667), and one on the optional side alone
/// filters inside it.
#[test]
fn controls() {
    check("MATCH (x:X) WHERE x.v > 1 OPTIONAL MATCH (y:Y) RETURN x.v, y.w", &["2|10", "3|10"]);
    check(
        "MATCH (x:X) WHERE x.v > 1 OPTIONAL MATCH (y:Y) WHERE x.v > 1 RETURN x.v, y.w",
        &["2|10", "3|10"],
    );
    check("MATCH (x:X) OPTIONAL MATCH (y:Y) WHERE y.w = x.v * 5 RETURN x.v, y.w", &["1|null", "2|10", "3|null"]);
    check("MATCH (x:X) OPTIONAL MATCH (x)-[:E]->(z) WHERE z.z > 5 RETURN x.v, z.z", &["1|null", "2|7", "3|null"]);
}
