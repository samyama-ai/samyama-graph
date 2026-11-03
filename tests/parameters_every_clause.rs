//! A `$parameter` is bound wherever an expression may appear.
//!
//! `substitute_params` rewrote parameters into literals in WHERE, RETURN,
//! WITH, ORDER BY and SET only. Everywhere else the parameter reached the
//! operators unresolved and failed with "Unresolved parameter": a DELETE of
//! `list[$i]`, `UNWIND $rows`, a MERGE or CREATE property, FOREACH's list.
//! SKIP and LIMIT were evaluated at parse time, before any parameter exists,
//! so `SKIP $s` was a parse error. Found by the TCK once the harness passed
//! parameters (Delete5 [1]/[2], Unwind1 [14], ReturnSkipLimit1 [2],
//! ReturnSkipLimit3 [2], WithSkipLimit3 [2]).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use std::collections::HashMap;

type Params = HashMap<String, PropertyValue>;

fn params(pairs: &[(&str, PropertyValue)]) -> Params {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn write(s: &mut GraphStore, q: &str, p: Params) -> Result<(), String> {
    let parsed = parse_query(q).map_err(|e| e.to_string())?;
    MutQueryExecutor::new(s, "default".into())
        .with_params(p)
        .execute(&parsed)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn column(s: &GraphStore, q: &str, p: Params) -> Result<Vec<String>, String> {
    let parsed = parse_query(q).map_err(|e| e.to_string())?;
    let out = QueryExecutor::new(s).with_params(p).execute(&parsed).map_err(|e| e.to_string())?;
    Ok(out
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::String(x))) => x.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect())
}

fn count(s: &GraphStore, q: &str) -> String {
    column(s, q, Params::new()).unwrap_or_else(|e| panic!("`{q}`: {e}"))[0].clone()
}

fn int(i: i64) -> PropertyValue {
    PropertyValue::Integer(i)
}

fn five_names() -> GraphStore {
    let mut s = GraphStore::new();
    write(&mut s, "UNWIND ['a', 'b', 'c', 'd', 'e'] AS x CREATE (:N {name: x})", Params::new()).unwrap();
    s
}

#[test]
fn delete_of_a_list_element_by_parameter() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:U {k: 1}), (:U {k: 2}), (:U {k: 3})", Params::new()).unwrap();
    write(&mut s, "MATCH (u:U) WITH u ORDER BY u.k WITH collect(u) AS us DETACH DELETE us[$i]", params(&[("i", int(1))]))
        .unwrap();
    assert_eq!(count(&s, "MATCH (u:U) RETURN count(u) AS c"), "2");
    assert_eq!(count(&s, "MATCH (u:U {k: 2}) RETURN count(u) AS c"), "0");
}

#[test]
fn unwind_of_a_parameter_list_then_merge() {
    let mut s = GraphStore::new();
    let row = |id: i64| PropertyValue::Map([("id".to_string(), int(id))].into_iter().collect());
    let rows = PropertyValue::Array(vec![row(1), row(2), row(1)]);
    write(&mut s, "UNWIND $rows AS r MERGE (:M {id: r.id})", params(&[("rows", rows)])).unwrap();
    assert_eq!(count(&s, "MATCH (m:M) RETURN count(m) AS c"), "2");
}

#[test]
fn create_and_merge_properties_by_parameter() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:P {name: $n})", params(&[("n", PropertyValue::String("x".into()))])).unwrap();
    write(&mut s, "MERGE (:P {name: $n})", params(&[("n", PropertyValue::String("x".into()))])).unwrap();
    assert_eq!(count(&s, "MATCH (p:P {name: 'x'}) RETURN count(p) AS c"), "1");
}

#[test]
fn foreach_over_a_parameter_list() {
    let mut s = GraphStore::new();
    let xs = PropertyValue::Array(vec![int(1), int(2), int(3)]);
    write(&mut s, "FOREACH (x IN $xs | CREATE (:F {v: x}))", params(&[("xs", xs)])).unwrap();
    assert_eq!(count(&s, "MATCH (f:F) RETURN sum(f.v) AS c"), "6");
}

#[test]
fn skip_and_limit_by_parameter_after_return() {
    let s = five_names();
    let got = column(
        &s,
        "MATCH (n:N) RETURN n.name AS c ORDER BY c SKIP $s LIMIT $l",
        params(&[("s", int(2)), ("l", int(2))]),
    )
    .unwrap();
    assert_eq!(got, vec!["c", "d"]);
}

#[test]
fn skip_and_limit_by_parameter_after_with() {
    let s = five_names();
    let got = column(
        &s,
        "MATCH (n:N) WITH n ORDER BY n.name SKIP $s LIMIT $l RETURN n.name AS c",
        params(&[("s", int(1)), ("l", int(3))]),
    )
    .unwrap();
    assert_eq!(got, vec!["b", "c", "d"]);
}

/// A parameter the caller did not supply is an error, not a missing SKIP.
#[test]
fn an_unsupplied_skip_parameter_is_an_error() {
    let s = five_names();
    assert!(column(&s, "MATCH (n:N) RETURN n.name AS c SKIP $s", Params::new()).is_err());
    assert!(column(&s, "MATCH (n:N) RETURN n.name AS c SKIP $s", params(&[("s", PropertyValue::String("x".into()))]))
        .is_err());
}

/// Control: the clauses that were covered still are.
#[test]
fn where_and_return_parameters_still_work() {
    let s = five_names();
    let got = column(&s, "MATCH (n:N) WHERE n.name = $x RETURN n.name + $y AS c", params(&[
        ("x", PropertyValue::String("b".into())),
        ("y", PropertyValue::String("!".into())),
    ]))
    .unwrap();
    assert_eq!(got, vec!["b!"]);
}

/// A parameter inside a FOREACH body -- a CREATE property, a SET value, a
/// nested FOREACH's list -- is bound like one anywhere else.
#[test]
fn parameters_inside_a_foreach_body() {
    let mut s = GraphStore::new();
    write(
        &mut s,
        "FOREACH (x IN [1, 2] | CREATE (:F {v: x, tag: $t}))",
        params(&[("t", PropertyValue::String("k".into()))]),
    )
    .unwrap();
    assert_eq!(count(&s, "MATCH (f:F {tag: 'k'}) RETURN count(f) AS c"), "2");

    write(
        &mut s,
        "FOREACH (x IN [1] | FOREACH (y IN $ys | CREATE (:G {v: y})))",
        params(&[("ys", PropertyValue::Array(vec![int(5), int(6)]))]),
    )
    .unwrap();
    assert_eq!(count(&s, "MATCH (g:G) RETURN sum(g.v) AS c"), "11");

    write(&mut s, "MATCH (f:F) FOREACH (x IN [1] | SET f.p = $p)", params(&[("p", int(9))])).unwrap();
    assert_eq!(count(&s, "MATCH (f:F) WHERE f.p = 9 RETURN count(f) AS c"), "2");
}
