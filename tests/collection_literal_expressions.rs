//! A list or map literal can hold an expression, not only a literal.
//!
//! `list` and `map` in the grammar were built from `value`, which is literals
//! only. So all of these were parse errors:
//!
//! ```cypher
//! RETURN [abs(1)]
//! UNWIND [date({year: 1910, month: 5, day: 6})] AS d RETURN d
//! MATCH (u:User) WITH {key: u} AS nodes DELETE nodes.key
//! ```
//!
//! They looked like three unrelated gaps — a function gap, a temporal gap and
//! a DELETE gap — and were one missing pair of AST variants. The map case
//! blocked every Delete5 scenario in the TCK, all of which reach the thing
//! they delete through a map (#654).
//!
//! The literal forms are tried **first**, so an all-literal collection still
//! produces a `PropertyValue` and nothing downstream of that changed. Only
//! collections the literal form cannot express take the new path — which is
//! also why the tests below assert the old shape is preserved.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn value_of(store: &GraphStore, cypher: &str) -> Value {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"));
    assert!(!out.records.is_empty(), "`{cypher}` returned no rows");
    out.records[0].get("x").cloned().unwrap_or(Value::Null)
}

#[test]
fn a_list_literal_can_hold_a_function_call() {
    let store = GraphStore::new();
    match value_of(&store, "RETURN [abs(1), 2 * 3] AS x") {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::Property(PropertyValue::Integer(1)));
            assert_eq!(items[1], Value::Property(PropertyValue::Integer(6)));
        }
        other => panic!("expected a list, got {other:?}"),
    }
}

#[test]
fn an_all_literal_collection_keeps_its_old_shape() {
    // The reason this change is safe: the literal rules are tried first, so
    // every existing consumer of `PropertyValue::Array` and
    // `PropertyValue::Map` sees exactly what it saw before.
    let store = GraphStore::new();
    assert!(matches!(
        value_of(&store, "RETURN [1, 2] AS x"),
        Value::Property(PropertyValue::Array(_))
    ));
    assert!(matches!(
        value_of(&store, "RETURN {a: 1} AS x"),
        Value::Property(PropertyValue::Map(_))
    ));
}

#[test]
fn unwind_iterates_a_list_of_expressions() {
    // Without an arm for the new value shape, UNWIND iterated nothing and
    // returned no rows — success, with the loop body never running.
    let store = GraphStore::new();
    let q = parse_query("UNWIND [abs(1), abs(-2)] AS v RETURN v AS x").expect("should parse");
    let out = QueryExecutor::new(&store).execute(&q).expect("should run");
    let got: Vec<i64> = out
        .records
        .iter()
        .filter_map(|r| match r.get("x") {
            Some(Value::Property(PropertyValue::Integer(n))) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(got, vec![1, 2]);
}

#[test]
fn a_map_literal_can_hold_a_node() {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:User)").expect("should parse");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("should run");
    match value_of(&store, "MATCH (u:User) WITH {key: u} AS m RETURN m AS x") {
        Value::Map(entries) => {
            assert_eq!(entries.len(), 1);
            assert!(matches!(entries.get("key"), Some(Value::NodeRef(_) | Value::Node(..))));
        }
        other => panic!("expected a map holding a node, got {other:?}"),
    }
}

#[test]
fn a_collection_cannot_be_used_as_a_node_in_a_pattern() {
    // Cypher calls this a VariableTypeConflict. It was unreachable while `[n]`
    // did not parse — the TCK scenario asserting the error passed because the
    // query failed for an unrelated reason. Making the literal work removed
    // that accident, so the rule has to be real now.
    for cypher in [
        "MATCH (n) WITH [n] AS users MATCH (users)-->(m) RETURN m",
        "MATCH (n) WITH {k: n} AS users MATCH (users)-->(m) RETURN m",
    ] {
        let err = parse_query(cypher).expect_err(&format!("`{cypher}` must be refused"));
        assert!(
            err.to_string().contains("users"),
            "the message should name the variable: {err}"
        );
    }
}

#[test]
fn a_variable_rebound_to_something_else_is_usable_again() {
    // The conflict is about what the name currently holds, not about the name
    // ever having held a list.
    assert!(parse_query(
        "MATCH (n) WITH [n] AS users WITH users[0] AS users MATCH (users)-->(m) RETURN m"
    )
    .is_ok());
    assert!(parse_query("MATCH (n) WITH [n] AS users RETURN users").is_ok());
}
