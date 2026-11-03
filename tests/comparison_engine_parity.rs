//! Every comparison rule holds in a `MATCH`'s `WHERE` too (#860).
//!
//! ```text
//! MATCH ()-[a]->() MATCH ()-[b]->() WHERE a = b     TypeError
//! MATCH ()-[a]->() MATCH ()-[b]->() RETURN a = b    true
//! ```
//!
//! `FilterOperator::evaluate_binary_op` was a second implementation — 67 lines
//! against the free function's 346 — and its own comment already said so. It
//! agreed on the easy things and diverged on every rule added since: entity
//! identity, `cypher_equals`' three-valued list and map equality, the NaN and
//! list-ordering rules (#855), the entity-ordering rule (#840), and
//! integer-float equality. So every comparison rule fixed this cycle applied
//! everywhere **except the clause most queries filter in**.
//!
//! The parity test below is the point of this file: the same expression is
//! evaluated in a `MATCH … WHERE`, after a `WITH`, and in a `RETURN`, and the
//! three must agree. A test that checked only one of them is what let the two
//! engines drift for as long as they did.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn store() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query("CREATE (:A {n: 1, f: 1.0})-[:R]->(:B {n: 2})").expect("setup parses");
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .expect("setup runs");
    store
}

fn truth(store: &GraphStore, cypher: &str) -> Option<bool> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}\n  parse: {e:?}"));
    let batch = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}\n  exec: {e:?}"));
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Boolean(b))) => Some(*b),
        _ => None,
    }
}

/// **The parity check.** One predicate, three clauses, one answer.
///
/// `MATCH … WHERE` is evaluated by the filter; `WITH … WHERE` and `RETURN` go
/// through the general evaluator.
///
/// The predicates here are over **entities**, deliberately. I first wrote this
/// over property values — `1 = 1.0`, list equality, NaN — and it passed against
/// the old filter engine as well as the new one, because that engine's
/// `coerced_eq` and `compare_*` helpers had kept up on scalars. What it had
/// never gained was the entity arms. A parity test over the cases that already
/// agreed would have been the sort of check that cannot fail.
#[test]
fn an_entity_predicate_means_the_same_in_every_clause() {
    let s = store();
    for pred in [
        "a = a",
        "a <> a",
        "a = b",
        "id(a) = id(b)",
    ] {
        let in_where =
            truth(&s, &format!("MATCH ()-[a]->() MATCH ()-[b]->() WHERE {pred} RETURN true AS r"));
        let after_with = truth(
            &s,
            &format!("MATCH ()-[a]->() MATCH ()-[b]->() WITH a, b WHERE {pred} RETURN true AS r"),
        );
        let in_return =
            truth(&s, &format!("MATCH ()-[a]->() MATCH ()-[b]->() RETURN {pred} AS r"));
        assert_eq!(
            in_where == Some(true),
            in_return == Some(true),
            "`{pred}`: MATCH…WHERE disagrees with RETURN ({in_return:?})"
        );
        assert_eq!(
            after_with == Some(true),
            in_return == Some(true),
            "`{pred}`: WITH…WHERE disagrees with RETURN ({in_return:?})"
        );
    }
}

/// The scalar rules, checked in all three clauses too. These already agreed
/// before the delegation — they are here so that a *future* divergence in
/// either engine is caught, not because they caught this one.
#[test]
fn a_scalar_predicate_means_the_same_in_every_clause() {
    let s = store();
    for pred in [
        "1 = 1.0",
        "[1, 2] = [1, 2]",
        "0.0 / 0.0 < 1",
        "[1, 0] >= [1]",
        "2 ^ 3 = 8.0",
        "1 IN [1.0]",
    ] {
        let in_where = truth(&s, &format!("MATCH (x) WHERE {pred} RETURN true AS r LIMIT 1"));
        let in_return = truth(&s, &format!("RETURN {pred} AS r"));
        assert_eq!(
            in_where == Some(true),
            in_return == Some(true),
            "`{pred}`: MATCH…WHERE disagrees with RETURN ({in_return:?})"
        );
    }
}

/// A number equals a number across the two representations.
#[test]
fn an_integer_equals_an_equal_float() {
    let s = store();
    assert_eq!(truth(&s, "RETURN 1 = 1.0 AS r"), Some(true));
    assert_eq!(truth(&s, "RETURN 1.0 = 1 AS r"), Some(true));
    assert_eq!(truth(&s, "RETURN 1 = 1.5 AS r"), Some(false));
    assert_eq!(truth(&s, "RETURN 1 <> 1.0 AS r"), Some(false));
    // Not a number: still false, not an error.
    assert_eq!(truth(&s, "RETURN 1 = '1' AS r"), Some(false));
    assert_eq!(truth(&s, "RETURN 1 = true AS r"), Some(false));
    // NaN equals nothing, itself included.
    assert_eq!(truth(&s, "RETURN 1 = 0.0 / 0.0 AS r"), Some(false));
}

/// All three entity kinds have identity, and an entity is never equal to a
/// non-entity — false, not an error.
#[test]
fn every_entity_kind_has_identity() {
    let s = store();
    assert_eq!(truth(&s, "MATCH ()-[r]->() RETURN r = r AS r"), Some(true));
    assert_eq!(truth(&s, "MATCH ()-[r]->() RETURN r <> r AS r"), Some(false));
    assert_eq!(truth(&s, "MATCH (a)-[]->() RETURN a = a AS r"), Some(true));
    assert_eq!(
        truth(&s, "MATCH p1 = ()-->() MATCH p2 = ()-->() RETURN p1 = p2 AS r"),
        Some(true)
    );
    assert_eq!(truth(&s, "MATCH (a), ()-[r]->() RETURN a = r AS r"), Some(false));
}

/// The shape the TCK scenario uses, which the filter engine could not run.
#[test]
fn a_relationship_compares_across_a_with_barrier() {
    let s = store();
    let cypher = "MATCH ()-[a]->() WITH a MATCH ()-[b]->() WHERE a = b RETURN count(b) AS r";
    let q = parse_query(cypher).expect("parses");
    let batch = QueryExecutor::new(&s).execute(&q).expect("runs");
    assert_eq!(
        batch.records.first().and_then(|r| r.get("r")),
        Some(&Value::Property(PropertyValue::Integer(1)))
    );
}
