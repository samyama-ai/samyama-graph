//! Extreme years survive construction (#814).
//!
//! ```text
//! localdatetime({year: 1, month: 1, day: 1, hour: 1, ...})
//!   expected 0001-01-01T01:01:01.000000001
//!   got      1754-08-30T23:44:42.128654849
//! ```
//!
//! The composite constructors computed `days * 86_400 * 1_000_000_000` before
//! splitting it. **That product spans only about ±292 years from 1970**, so
//! every value outside roughly 1678..2262 wrapped silently — and came back as a
//! perfectly well-formed date-time in the wrong century.
//!
//! It surfaced as a *sorting* failure. `WithOrderBy1` sorts local date-times
//! and got the order wrong, which reads as a comparison bug; the comparison was
//! fine and the values were corrupt before they reached it. 24 scenarios across
//! WithOrderBy1 and WithOrderBy2 were failing on this.
//!
//! Seconds are the wider unit — ±292 *billion* years — so the days now go
//! through seconds and only the sub-day remainder is counted in nanoseconds.
//! `date()` was always correct because it never left days.

use samyama::graph::GraphStore;
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rendered(expr: &str) -> String {
    let store = GraphStore::new();
    let cypher = format!("RETURN {expr} AS r");
    let q = parse_query(&cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(&store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(p)) => p.to_cypher_string(),
        other => panic!("{cypher}\n  got {other:?}"),
    }
}

/// Both ends of the TCK's range, which straddle the i64-nanosecond limit.
#[test]
fn extreme_years_survive_construction() {
    assert_eq!(
        rendered("localdatetime({year:1,month:1,day:1,hour:1,minute:1,second:1,nanosecond:1})"),
        "0001-01-01T01:01:01.000000001"
    );
    assert_eq!(
        rendered("localdatetime({year:9999,month:9,day:9,hour:9,minute:59,second:59,nanosecond:999999999})"),
        "9999-09-09T09:59:59.999999999"
    );
    assert_eq!(
        rendered("datetime({year:1,month:1,day:1,hour:1,timezone:'+01:00'})"),
        "0001-01-01T01:00+01:00"
    );
    assert_eq!(rendered("datetime({year:9999,month:9,day:9,hour:9,timezone:'Z'})"), "9999-09-09T09:00Z");
}

/// Ordinary years are undisturbed — the fix must not shift the common case.
#[test]
fn ordinary_years_are_unchanged() {
    assert_eq!(
        rendered("localdatetime({year:1984,month:10,day:11,hour:12,minute:30,second:14,nanosecond:12})"),
        "1984-10-11T12:30:14.000000012"
    );
    assert_eq!(
        rendered("datetime({year:2015,month:7,day:21,hour:21,minute:40,timezone:'+01:00'})"),
        "2015-07-21T21:40+01:00"
    );
    assert_eq!(rendered("localdatetime({year:1970,month:1,day:1})"), "1970-01-01T00:00");
}

/// **Sorting was where this showed up, and it is worth pinning there too.**
///
/// The order looked wrong; the comparison was correct and the values were
/// corrupt before reaching it. A test that only checked construction would not
/// have connected the two.
#[test]
fn extreme_years_sort_correctly() {
    use samyama::query::executor::MutQueryExecutor;
    let mut store = GraphStore::new();
    let setup = "CREATE (:A {d: localdatetime({year:1984,month:10,day:11,hour:12,minute:30})}), \
                        (:A {d: localdatetime({year:1,month:1,day:1,hour:1,minute:1})}), \
                        (:A {d: localdatetime({year:9999,month:9,day:9,hour:9,minute:59})}), \
                        (:A {d: localdatetime({year:1980,month:12,day:11,hour:12,minute:31})})";
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&parse_query(setup).expect("setup parses"))
        .expect("setup runs");

    let q = parse_query("MATCH (a:A) RETURN a.d AS d ORDER BY a.d").expect("parses");
    let batch = QueryExecutor::new(&store).execute(&q).expect("runs");
    let got: Vec<String> = batch
        .records
        .iter()
        .filter_map(|r| match r.get("d") {
            Some(Value::Property(p)) => Some(p.to_cypher_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        got,
        vec![
            "0001-01-01T01:01",
            "1980-12-11T12:31",
            "1984-10-11T12:30",
            "9999-09-09T09:59",
        ]
    );
}
