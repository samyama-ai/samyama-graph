//! An `ORDER BY` key that fails to evaluate is an error, not a key of null
//! (samyama-graph#987).
//!
//! Both sorts folded an evaluation error to `Null`. Every key then compared
//! equal, the sort did nothing, and the rows came back in input order with the
//! right count and the right contents -- a wrong answer indistinguishable from a
//! right one. `RETURN x - 'a'` is a type error; `ORDER BY x - 'a'` returned the
//! rows unsorted.
//!
//! A *missing* value is still null, and still sorts: that is what Cypher means
//! by a key over an absent property. Only an evaluation error changes.

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

fn run(cypher: &str) -> Result<Vec<String>, String> {
    let store = GraphStore::new();
    let q = parse_query(cypher).map_err(|e| format!("parse: {e}"))?;
    let out = QueryExecutor::new(&store).execute(&q).map_err(|e| e.to_string())?;
    Ok(out
        .records
        .iter()
        .map(|r| format!("{:?}", out.columns.iter().map(|c| r.get(c).cloned()).collect::<Vec<_>>()))
        .collect())
}

fn ints(vals: &[i64]) -> Vec<String> {
    vals.iter().map(|v| format!("[Some(Property(Integer({v})))]")).collect()
}

#[test]
fn a_failing_return_order_by_key_is_an_error() {
    for q in [
        "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x - 'a'",
        "UNWIND [3, 1, 2] AS x RETURN x ORDER BY 'a' / x",
        "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x, -'a'",
        "UNWIND [3, 1, 2] AS x RETURN x ORDER BY 'a' / x LIMIT 2",
    ] {
        let got = run(q);
        assert!(
            matches!(&got, Err(e) if e.contains("Type error")),
            "`{q}` should fail with a type error, got {got:?}"
        );
    }
}

#[test]
fn a_failing_with_order_by_key_is_an_error() {
    for q in [
        "UNWIND [3, 1, 2] AS x WITH x ORDER BY 'a' / x RETURN x",
        "UNWIND [3, 1, 2] AS x WITH x ORDER BY x - 'a' LIMIT 2 RETURN x",
    ] {
        let got = run(q);
        assert!(
            matches!(&got, Err(e) if e.contains("Type error")),
            "`{q}` should fail with a type error, got {got:?}"
        );
    }
}

#[test]
fn valid_keys_still_sort() {
    assert_eq!(run("UNWIND [3, 1, 2] AS x RETURN x ORDER BY x").unwrap(), ints(&[1, 2, 3]));
    assert_eq!(run("UNWIND [3, 1, 2] AS x RETURN x ORDER BY -x").unwrap(), ints(&[3, 2, 1]));
    assert_eq!(run("UNWIND [3, 1, 2] AS x WITH x ORDER BY x DESC RETURN x").unwrap(), ints(&[3, 2, 1]));
    assert_eq!(run("UNWIND [3, 1, 2] AS x RETURN x ORDER BY x LIMIT 2").unwrap(), ints(&[1, 2]));
}

/// A sort key that repeats a projected aggregate inside arithmetic resolves to
/// the projection, and sorts by it. It used to be evaluated after the
/// aggregation, where its variable is gone; the error was folded to null and
/// the sort did nothing -- invisible with one group, wrong with several.
#[test]
fn a_projected_aggregate_inside_an_order_by_key_sorts() {
    let q = "UNWIND [{g: 'a', v: 1}, {g: 'b', v: 9}, {g: 'c', v: 5}, {g: 'a', v: 3}] AS m \
             WITH m.g AS g, m.v AS v \
             RETURN g, avg(v) AS a ORDER BY 0 - avg(v)";
    let got = run(q).unwrap_or_else(|e| panic!("`{q}` should run: {e}"));
    let groups: Vec<&str> = got
        .iter()
        .map(|r| if r.contains("\"b\"") { "b" } else if r.contains("\"c\"") { "c" } else { "a" })
        .collect();
    assert_eq!(groups, vec!["b", "c", "a"], "not sorted by descending average: {got:?}");
    let q = "UNWIND [{g: 'a', v: 1}, {g: 'b', v: 9}, {g: 'c', v: 5}, {g: 'a', v: 3}] AS m \
             WITH m.g AS g, m.v AS v \
             RETURN g, avg(v) AS a ORDER BY 38 + avg(v) - 1000";
    let got = run(q).unwrap_or_else(|e| panic!("`{q}` should run: {e}"));
    assert!(got[0].contains("\"a\"") && got[2].contains("\"b\""), "not sorted by ascending average: {got:?}");
}

/// Absent is not an error: a missing map key sorts as null, last ascending.
#[test]
fn a_missing_value_is_still_null_and_still_sorts() {
    let got = run("UNWIND [{a: 2}, {b: 9}, {a: 1}] AS m RETURN m.a AS a ORDER BY m.a").unwrap();
    assert_eq!(
        got,
        vec![
            "[Some(Property(Integer(1)))]".to_string(),
            "[Some(Property(Integer(2)))]".to_string(),
            "[Some(Property(Null))]".to_string(),
        ]
    );
    let got = run("UNWIND [{a: 2}, {b: 9}, {a: 1}] AS m WITH m ORDER BY m.a RETURN m.a AS a").unwrap();
    assert_eq!(got.len(), 3);
    assert_eq!(got[0], "[Some(Property(Integer(1)))]");
}
