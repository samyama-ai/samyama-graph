//! `WITH … ORDER BY` sees the scope in front of the projection (#970).
//!
//! ```cypher
//! MATCH (a:A)
//! WITH a, a.num + a.num2 AS sum
//! WITH a, a.num2 % 3 AS mod
//!   ORDER BY sum
//!   LIMIT 3
//! RETURN a, mod
//! ```
//!
//! `sum` is projected by the *first* WITH and not by the second, and Cypher
//! allows the ORDER BY to name it: after a WITH, ORDER BY sees the projected
//! aliases **and** the scope in front of them.
//!
//! Sorting the projected rows alone evaluated `sum` to null on every row, so
//! the order was whatever the input order happened to be — and the `LIMIT 3`
//! then kept the wrong three. Nothing errored; the query returned three rows
//! that looked entirely plausible.
//!
//! The pre-projection bindings the sort needs are carried under a private
//! prefix and stripped before the rows leave, so they cannot become columns or
//! re-enter scope for the next clause.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// The openCypher `WithOrderBy4` [8] fixture.
fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    for (num, num2) in [(1i64, 4i64), (5, 2), (9, 0), (3, 3), (7, 1)] {
        let n = store.create_node_with_labels([Label::new("A")]);
        let _ = store.set_node_property("default", n, "num".to_string(), PropertyValue::Integer(num));
        let _ = store.set_node_property("default", n, "num2".to_string(), PropertyValue::Integer(num2));
    }
    store
}

fn column(store: &GraphStore, cypher: &str, col: &str) -> Vec<i64> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .iter()
        .map(|r| match r.get(col) {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{col}: {other:?}"),
        })
        .collect()
}

#[test]
fn order_by_can_name_a_variable_the_projection_dropped() {
    // sums are 5, 7, 9, 6, 8; the three smallest are 5, 6, 7 -> num 1, 3, 5.
    let store = graph();
    let got = column(
        &store,
        "MATCH (a:A) WITH a, a.num + a.num2 AS sum \
         WITH a, a.num2 % 3 AS mod ORDER BY sum LIMIT 3 RETURN a.num AS n",
        "n",
    );
    assert_eq!(got, vec![1, 3, 5]);
}

#[test]
fn the_projected_alias_still_wins() {
    // `mod` exists only in the projection. Sorting by it must use it, not
    // anything carried in.
    let store = graph();
    let got = column(
        &store,
        "MATCH (a:A) WITH a, a.num2 % 3 AS mod ORDER BY mod, a.num RETURN a.num AS n",
        "n",
    );
    assert_eq!(got, vec![3, 9, 1, 7, 5]);
}

#[test]
fn the_carried_bindings_do_not_become_columns() {
    // They are the sort's business only. Left in place they would appear as
    // extra columns in the result.
    let store = graph();
    let q = parse_query(
        "MATCH (a:A) WITH a, a.num + a.num2 AS sum \
         WITH a.num2 % 3 AS mod ORDER BY sum LIMIT 1 RETURN mod",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&q).unwrap();
    for (name, _) in batch.records[0].bindings() {
        assert!(!name.starts_with("__orderby_carry_"), "{name} leaked");
    }
}

#[test]
fn the_carried_bindings_do_not_re_enter_scope() {
    // The worse failure: a name the next clause never projected coming back
    // into scope. Referencing `sum` after the second WITH must not resolve.
    let store = graph();
    let q = parse_query(
        "MATCH (a:A) WITH a, a.num + a.num2 AS sum \
         WITH a.num2 % 3 AS mod ORDER BY sum RETURN mod, sum",
    )
    .expect("this parses; scope is decided at run time here");
    let err = QueryExecutor::new(&store).execute(&q).unwrap_err();
    assert!(
        format!("{err:?}").contains("sum"),
        "`sum` must stay out of scope after the second WITH, got {err:?}"
    );
}

#[test]
fn an_ordinary_order_by_on_a_projected_value_is_unchanged() {
    let store = graph();
    assert_eq!(
        column(&store, "MATCH (a:A) WITH a.num AS n ORDER BY n RETURN n", "n"),
        vec![1, 3, 5, 7, 9]
    );
    assert_eq!(
        column(&store, "MATCH (a:A) WITH a.num AS n ORDER BY n DESC RETURN n", "n"),
        vec![9, 7, 5, 3, 1]
    );
}

#[test]
fn order_by_a_property_of_a_projected_node_still_works() {
    let store = graph();
    assert_eq!(
        column(&store, "MATCH (a:A) WITH a ORDER BY a.num LIMIT 2 RETURN a.num AS n", "n"),
        vec![1, 3]
    );
}

// ---------------------------------------------------------------------------
// A WITH's WHERE has the identical scope rule.
//
//   WITH a.name2 AS name WHERE name = 'B' OR a.name2 = 'C'
//
// filters on the projected alias *and* on `a`, which the projection dropped.
// With `a` gone the second disjunct was an ordinary `false` — not a null — so
// a fallback keyed on null would never have fired. That is why the widening is
// unconditional.

fn named() -> GraphStore {
    let mut store = GraphStore::new();
    for v in ["A", "B", "C"] {
        let n = store.create_node("");
        let _ = store.set_node_property(
            "default", n, "name2".to_string(), PropertyValue::String(v.into()));
    }
    store
}

fn strings(store: &GraphStore, cypher: &str, col: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut out: Vec<String> = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .iter()
        .map(|r| match r.get(col) {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => panic!("{col}: {other:?}"),
        })
        .collect();
    out.sort();
    out
}

#[test]
fn a_with_where_sees_both_scopes() {
    let store = named();
    assert_eq!(
        strings(
            &store,
            "MATCH (a) WITH a.name2 AS name WHERE name = \"B\" OR a.name2 = \"C\" RETURN *",
            "name",
        ),
        vec!["B", "C"]
    );
}

#[test]
fn a_with_where_on_the_alias_alone_is_unchanged() {
    let store = named();
    assert_eq!(
        strings(&store, "MATCH (a) WITH a.name2 AS name WHERE name = \"B\" RETURN *", "name"),
        vec!["B"]
    );
}

#[test]
fn a_with_where_that_matches_nothing_still_returns_nothing() {
    // The direction an unconditional widening could break: a predicate that
    // is legitimately false everywhere must stay false.
    let store = named();
    let q = parse_query("MATCH (a) WITH a.name2 AS name WHERE name = \"Z\" RETURN *").unwrap();
    assert_eq!(QueryExecutor::new(&store).execute(&q).unwrap().records.len(), 0);
}

#[test]
fn the_projected_alias_shadows_the_carried_name() {
    // `name` is projected from `a.name2`; a carried `a` must not change what
    // `name` means.
    let store = named();
    assert_eq!(
        strings(
            &store,
            "MATCH (a) WITH a.name2 AS name WHERE name <> \"A\" AND a.name2 <> \"C\" RETURN *",
            "name",
        ),
        vec!["B"]
    );
}
