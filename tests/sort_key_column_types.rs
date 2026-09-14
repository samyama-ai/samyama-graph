//! ORDER BY reads its key correctly whatever the key column holds.
//!
//! The sort key is read through a property cursor that borrows a string
//! straight from a string column (#750) and otherwise reads the value. The
//! cursor remembers whether its column holds strings, so a non-string key --
//! a date, a number -- does not try the borrow on every row. These pin the
//! answer for a string column, a date column, a number column, and a column
//! that holds strings and numbers together, where Cypher orders strings first.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn write(s: &mut GraphStore, q: &str) {
    MutQueryExecutor::new(s, "default".into()).execute(&parse_query(q).unwrap()).unwrap_or_else(|e| panic!("`{q}`: {e}"));
}

fn col(s: &GraphStore, q: &str) -> Vec<String> {
    let p = parse_query(q).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    let out = QueryExecutor::new(s).execute(&p).unwrap_or_else(|e| panic!("`{q}`: {e}"));
    out.records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::String(x))) => x.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_string_key() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:N {k: 'pear', i: 1}), (:N {k: 'apple', i: 2}), (:N {k: 'fig', i: 3})");
    assert_eq!(col(&s, "MATCH (n:N) RETURN n.k AS c ORDER BY n.k"), vec!["apple", "fig", "pear"]);
    assert_eq!(col(&s, "MATCH (n:N) RETURN n.i AS c ORDER BY n.k DESC"), vec!["1", "3", "2"]);
}

#[test]
fn a_date_key() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:M {d: datetime('2020-03-01T00:00Z'), i: 1}), (:M {d: datetime('2019-01-01T00:00Z'), i: 2}), (:M {d: datetime('2021-07-04T00:00Z'), i: 3})");
    assert_eq!(col(&s, "MATCH (m:M) RETURN m.i AS c ORDER BY m.d DESC"), vec!["3", "1", "2"]);
}

#[test]
fn a_number_key() {
    let mut s = GraphStore::new();
    write(&mut s, "UNWIND [5, 2, 9, 1] AS v CREATE (:P {v: v})");
    assert_eq!(col(&s, "MATCH (p:P) RETURN p.v AS c ORDER BY p.v"), vec!["1", "2", "5", "9"]);
}

/// Strings sort before numbers in Cypher's ORDER BY.
#[test]
fn a_column_holding_strings_and_numbers() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:Q {v: 'b', i: 1}), (:Q {v: 3, i: 2}), (:Q {v: 'a', i: 3}), (:Q {v: 1, i: 4})");
    assert_eq!(col(&s, "MATCH (q:Q) RETURN q.i AS c ORDER BY q.v"), vec!["3", "1", "4", "2"]);
}
