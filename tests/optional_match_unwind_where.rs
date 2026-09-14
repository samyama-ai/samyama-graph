//! An OPTIONAL MATCH whose WHERE names an UNWIND variable keeps every
//! unwound row (#1229).
//!
//! `UNWIND $rows AS i OPTIONAL MATCH (a:N) WHERE a.id = i` is the batch
//! "look each row up if it exists" idiom. The WHERE belongs to the optional
//! match, so a row it finds nothing for survives with nulls. Main deleted
//! that row when the OPTIONAL MATCH came straight after the UNWIND (the
//! conjunct became a filter above the join), and ignored the WHERE entirely
//! after a WITH (6 rows instead of 2). Every expected answer here is Neo4j
//! 2026.04.0's on the same graph, and each runs with and without an index.

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

/// Every row as `col1|col2|...`, sorted.
fn rows(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let mut v: Vec<String> = out
        .records
        .iter()
        .map(|r| out.columns.iter().map(|c| cell(r.get(c))).collect::<Vec<_>>().join("|"))
        .collect();
    v.sort();
    v
}

/// `:N {id: 1..3}`, `(1)-[:R]->(2)`, `:K {k: 1}`, `:K {k: 99}`; plain and indexed on `:N(id)`.
fn stores() -> [GraphStore; 2] {
    let mut out = [GraphStore::new(), GraphStore::new()];
    for s in out.iter_mut() {
        write(s, "UNWIND range(1, 3) AS i CREATE (:N {id: i})");
        write(s, "CREATE (:K {k: 1}), (:K {k: 99})");
        write(s, "MATCH (a:N {id: 1}), (b:N {id: 2}) CREATE (a)-[:R]->(b)");
    }
    write(&mut out[1], "CREATE INDEX ON :N(id)");
    out
}

fn check(q: &str, expected: &[&str]) {
    for (s, what) in stores().iter().zip(["plain", "indexed"]) {
        assert_eq!(rows(s, q), expected, "{what}: {q}");
    }
}

#[test]
fn straight_after_the_unwind() {
    check("UNWIND [1, 99] AS i OPTIONAL MATCH (a:N) WHERE a.id = i RETURN i, a.id", &["1|1", "99|null"]);
}

#[test]
fn after_a_with() {
    check("UNWIND [1, 99] AS i WITH i OPTIONAL MATCH (a:N) WHERE a.id = i RETURN i, a.id", &["1|1", "99|null"]);
}

#[test]
fn with_a_hop() {
    check(
        "UNWIND [1, 3, 99] AS i OPTIONAL MATCH (a:N)-[:R]->(b) WHERE a.id = i RETURN i, b.id",
        &["1|2", "3|null", "99|null"],
    );
}

#[test]
fn counting_the_rows() {
    check("UNWIND [1, 99] AS i OPTIONAL MATCH (a:N) WHERE a.id = i RETURN count(*) AS c", &["2"]);
}

/// Already right on main: the outer side is a MATCH, not an UNWIND (#667).
#[test]
fn after_a_match() {
    check("MATCH (x:K) OPTIONAL MATCH (a:N) WHERE a.id = x.k RETURN x.k, a.id", &["1|1", "99|null"]);
}

/// Already right on main: the WHERE names only the optional side.
#[test]
fn a_where_on_the_optional_side_only() {
    check("UNWIND [1, 2] AS i OPTIONAL MATCH (a:N) WHERE a.id = 5 RETURN i, a.id", &["1|null", "2|null"]);
}

/// The inline form means the WHERE form; it was refused until the WHERE form
/// was right. Neo4j 2026.04 answers the first query this way; the other two are
/// the WHERE forms above written inline.
#[test]
fn an_inline_property_from_the_row() {
    check("UNWIND [1, 99] AS i OPTIONAL MATCH (a:N {id: i}) RETURN i, a.id", &["1|1", "99|null"]);
    check(
        "UNWIND [1, 3, 99] AS i OPTIONAL MATCH (a:N {id: i})-[:R]->(b) RETURN i, b.id",
        &["1|2", "3|null", "99|null"],
    );
    check("MATCH (x:K) OPTIONAL MATCH (a:N {id: x.k}) RETURN x.k, a.id", &["1|1", "99|null"]);
}

/// The upsert idiom: create what the lookup did not find.
#[test]
fn creating_what_was_not_found() {
    for mut s in stores() {
        write(&mut s, "UNWIND [1, 99] AS i OPTIONAL MATCH (a:N) WHERE a.id = i WITH i, a WHERE a IS NULL CREATE (:N {id: i})");
        assert_eq!(rows(&s, "MATCH (n:N) RETURN n.id AS c"), vec!["1", "2", "3", "99"]);
    }
}
