//! `x WITHIN <duration>`: `x` falls in the last `<duration>` (NDS-03b, #1665).
//!
//! It parsed as an error before this, which is the one NDS-03b probe that
//! failed. `x` is a time, or a series -- a list of samples, each a time or a
//! `[time, value, ...]` list -- and a series is within the window when any
//! sample is. Three-valued like every comparison: what cannot be decided is
//! null, not false.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn ago(unit: &str, n: i64) -> String {
    format!("datetime() - duration({{{unit}: {n}}})")
}

/// | id | t            | series (sample times) |
/// |----|--------------|-----------------------|
/// | 1  | 2 days ago   |                       |
/// | 2  | 30 days ago  |                       |
/// | 3  |              |                       |
/// | 4  |              | 40 days, 3 hours ago  |
/// | 5  |              | 40 days ago           |
/// | 6  | 1 day ahead  |                       |
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let q = format!(
        "CREATE (:S {{id: 1, t: {}}}), (:S {{id: 2, t: {}}}), (:S {{id: 3}}), \
                (:S {{id: 4, series: [[{}, 1.0], [{}, 2.0]]}}), \
                (:S {{id: 5, series: [[{}, 1.0]]}}), \
                (:S {{id: 6, t: datetime() + duration({{days: 1}})}})",
        ago("days", 2),
        ago("days", 30),
        ago("days", 40),
        ago("hours", 3),
        ago("days", 40),
    );
    MutQueryExecutor::new(&mut s, "default".into())
        .execute(&parse_query(&q).unwrap())
        .unwrap();
    s
}

fn run(s: &GraphStore, q: &str) -> Result<Vec<Value>, String> {
    let query = parse_query(q).map_err(|e| e.to_string())?;
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .map_err(|e| e.to_string())?;
    Ok(batch
        .records
        .iter()
        .map(|r| r.get("id").cloned().unwrap_or(Value::Null))
        .collect())
}

fn ids(s: &GraphStore, q: &str) -> Vec<i64> {
    let mut v: Vec<i64> = run(s, q)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .into_iter()
        .map(|v| match v {
            Value::Property(PropertyValue::Integer(i)) => i,
            other => panic!("{q}: {other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn a_time_is_within_when_it_falls_in_the_last_duration() {
    let s = store();
    let q =
        |d: i64| format!("MATCH (n:S) WHERE n.t WITHIN duration({{days: {d}}}) RETURN n.id AS id");
    assert_eq!(ids(&s, &q(7)), vec![1]);
    assert_eq!(ids(&s, &q(60)), vec![1, 2]);
}

/// The window ends at now: a time in the future is not "within the last d".
#[test]
fn a_future_time_is_not_within() {
    let s = store();
    let q = "MATCH (n:S {id: 6}) RETURN n.t WITHIN duration({days: 365}) AS id";
    assert_eq!(
        run(&s, q).unwrap(),
        vec![Value::Property(PropertyValue::Boolean(false))]
    );
}

/// A series is within when any of its samples is.
#[test]
fn a_series_is_within_when_any_sample_is() {
    let s = store();
    let q = |d: i64| {
        format!("MATCH (n:S) WHERE n.series WITHIN duration({{days: {d}}}) RETURN n.id AS id")
    };
    assert_eq!(ids(&s, &q(7)), vec![4]);
    assert_eq!(ids(&s, &q(60)), vec![4, 5]);
}

/// Null in, null out -- so `NOT ... WITHIN` does not pick up the nodes that
/// have no time at all.
#[test]
fn a_missing_time_is_null_not_false() {
    let s = store();
    let q = "MATCH (n:S) WHERE NOT n.t WITHIN duration({days: 7}) RETURN n.id AS id";
    assert_eq!(ids(&s, q), vec![2, 6]);
    let q = "RETURN null WITHIN duration({days: 1}) AS id";
    assert_eq!(run(&s, q).unwrap(), vec![Value::Null]);
}

/// `WITHIN` binds like a comparison: tighter than `OR`.
#[test]
fn within_binds_like_a_comparison() {
    let s = store();
    let q = "MATCH (n:S) WHERE n.t WITHIN duration({days: 7}) OR n.id = 3 RETURN n.id AS id";
    assert_eq!(ids(&s, q), vec![1, 3]);
}

/// The right-hand side is a duration; anything else is an error naming
/// `WITHIN`, not a silent false.
#[test]
fn a_non_duration_window_is_an_error() {
    let s = store();
    let err = run(&s, "RETURN datetime() WITHIN 5 AS id").unwrap_err();
    assert!(err.contains("WITHIN"), "{err}");
}

/// `within` is still a name: only the operator position reads it as `WITHIN`.
#[test]
fn within_remains_usable_as_a_property_name() {
    let mut s = GraphStore::new();
    MutQueryExecutor::new(&mut s, "default".into())
        .execute(&parse_query("CREATE (:W {within: 7})").unwrap())
        .unwrap();
    assert_eq!(ids(&s, "MATCH (w:W) RETURN w.within AS id"), vec![7]);
}

/// The NDS-03b probe's exact text, which was a parse error.
#[test]
fn the_nds03b_probe_runs() {
    let s = store();
    run(
        &s,
        "MATCH (n:N) WHERE n.series WITHIN duration({days: 7}) RETURN count(n)",
    )
    .unwrap_or_else(|e| panic!("{e}"));
}
