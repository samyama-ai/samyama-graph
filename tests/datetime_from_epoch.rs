//! `datetime.fromepoch` and `datetime.fromepochmillis` (#1003).
//!
//! ```cypher
//! RETURN datetime.fromepoch(416779, 999999999),
//!        datetime.fromepochmillis(237821673987)
//! ```
//!
//! failed with `Unknown function`. Both name an instant in UTC, so the result
//! is a `ZonedDateTime` at offset 0 with no zone id -- an epoch has no
//! locality to attach.
//!
//! Nanoseconds are carried whole rather than folded into the seconds:
//! `datetime.fromepoch(416779, 999999999)` is `1970-01-05T19:46:19.999999999Z`,
//! which no millisecond-based representation can express at all.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn text(cypher: &str) -> String {
    let store = GraphStore::new();
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let c = r.columns[0].clone();
    let v = format!("{:?}", r.records[0].get(&c));
    let a = v.find('"').expect("a string result");
    v[a + 1..v.rfind('"').unwrap()].to_string()
}

fn fails(cypher: &str) -> bool {
    let store = GraphStore::new();
    match parse_query(cypher) {
        Err(_) => true,
        Ok(q) => QueryExecutor::new(&store).execute(&q).is_err(),
    }
}

#[test]
fn from_epoch_keeps_nanosecond_precision() {
    assert_eq!(text("RETURN toString(datetime.fromepoch(416779, 999999999)) AS d"),
               "1970-01-05T19:46:19.999999999Z");
}

#[test]
fn from_epoch_millis() {
    assert_eq!(text("RETURN toString(datetime.fromepochmillis(237821673987)) AS d"),
               "1977-07-15T13:34:33.987Z");
}

#[test]
fn a_negative_epoch_millisecond_is_an_instant_before_1970() {
    // `div_euclid`, not `/`. Truncating toward zero would put -1ms in second
    // zero with a negative nanosecond remainder, which is not a time.
    assert_eq!(text("RETURN toString(datetime.fromepochmillis(-1)) AS d"),
               "1969-12-31T23:59:59.999Z");
}

#[test]
fn the_epoch_itself() {
    assert_eq!(text("RETURN toString(datetime.fromepoch(0, 0)) AS d"), "1970-01-01T00:00Z");
}

#[test]
fn a_nanosecond_outside_its_range_is_refused() {
    assert!(fails("RETURN datetime.fromepoch(0, 1000000000)"));
    assert!(fails("RETURN datetime.fromepoch(0, -1)"));
}

#[test]
fn the_wrong_arity_is_refused() {
    assert!(fails("RETURN datetime.fromepoch(0)"));
    assert!(fails("RETURN datetime.fromepochmillis(0, 0)"));
}
