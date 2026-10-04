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

/// The empty string and non-ASCII values order as their owned form does.
///
/// The key is borrowed from the column rather than copied (#750), so the
/// comparison is `str::cmp` where it used to be `PropertyValue`'s `Ord` over
/// an owned `String`. Those agree -- both are byte-wise over UTF-8 -- and this
/// pins that they do rather than asserting a hand-written order, which would
/// only restate whichever one was written down. The empty string is the case
/// that distinguishes a borrowed read from an absent property: both are
/// falsy-looking, and only one of them is a value.
#[test]
fn an_empty_and_non_ascii_string_key() {
    let values = ["", "zebra", "Zebra", "éclair", "日本", "a", "Ähre", "apple"];
    let mut s = GraphStore::new();
    for (i, v) in values.iter().enumerate() {
        write(&mut s, &format!("CREATE (:S {{k: '{v}', i: {i}}})"));
    }

    // `sort` on `&str` is byte-wise `str::cmp` — the comparison the borrowed
    // key uses, and the one an owned `String` key used before it.
    let mut expected: Vec<&str> = values.to_vec();
    expected.sort();
    assert_eq!(col(&s, "MATCH (n:S) RETURN n.k AS c ORDER BY n.k"), expected);

    // Descending is the same order reversed, and the empty string is a row at
    // the other end rather than a dropped one.
    expected.reverse();
    assert_eq!(col(&s, "MATCH (n:S) RETURN n.k AS c ORDER BY n.k DESC"), expected);

    // The projected value is the value, not a prefix or a copy that lost its
    // multi-byte characters.
    assert_eq!(col(&s, "MATCH (n:S) WHERE n.i = 4 RETURN n.k AS c"), vec!["日本"]);
}

/// An absent string property is null, and null sorts after every string --
/// which is what distinguishes it from the empty string above.
#[test]
fn an_absent_string_key_is_not_the_empty_string() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:T {k: '', i: 1}), (:T {i: 2}), (:T {k: 'a', i: 3})");
    assert_eq!(col(&s, "MATCH (n:T) RETURN n.i AS c ORDER BY n.k"), vec!["1", "3", "2"]);
}

/// A relationship property reaches the cursor's edge branch, which caches
/// whether the relationship column holds strings separately from the node one.
#[test]
fn a_string_key_on_a_relationship() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:U {n: 1}), (:U {n: 2}), (:U {n: 3})");
    write(&mut s, "MATCH (a:U {n: 1}), (b:U {n: 2}), (c:U {n: 3}) CREATE (a)-[:R {k: 'pear'}]->(b), (a)-[:R {k: ''}]->(c), (b)-[:R {k: 'ähre'}]->(c)");
    assert_eq!(
        col(&s, "MATCH ()-[r:R]->() RETURN r.k AS c ORDER BY r.k"),
        vec!["", "pear", "ähre"]
    );
}
