//! Coverage tests for the expression evaluator, function library and the
//! leaf operators at the top of `operator.rs` (everything before
//! `FilterOperator`). Most are driven through real Cypher on a small store.

use super::*;
use crate::query::QueryEngine;

/// Evaluate `RETURN <expr> AS v` on an empty store.
fn ev(expr: &str) -> Result<PropertyValue, String> {
    ev_on(&GraphStore::new(), expr)
}

/// Evaluate `RETURN <expr> AS v` through the engine, and the same parsed
/// expression through the standalone `eval_expression` on an empty record.
/// The two must agree: both succeed with the same value, or both fail.
fn ev_on(store: &GraphStore, expr: &str) -> Result<PropertyValue, String> {
    let q = format!("RETURN {expr} AS v");
    let engine = QueryEngine::new()
        .execute(&q, store)
        .map_err(|e| e.to_string());
    let direct = direct_eval(store, expr);
    match (&engine, &direct) {
        (Ok(_), Ok(_)) | (Err(_), Err(_)) => {}
        _ => panic!("`{expr}`: engine {engine:?} but eval_expression {direct:?}"),
    }
    let batch = engine?;
    assert_eq!(
        batch.records.len(),
        1,
        "`{q}` returned {} rows",
        batch.records.len()
    );
    let v = batch.records[0].get("v").expect("column v").clone();
    let p = match v {
        Value::Property(p) => p,
        Value::Null => PropertyValue::Null,
        other => panic!("`{q}` returned a non-property value {other:?}"),
    };
    let d = direct.unwrap();
    let same = |a: &PropertyValue, b: &PropertyValue| match (a, b) {
        (PropertyValue::Float(x), PropertyValue::Float(y)) if x.is_nan() && y.is_nan() => true,
        _ => a == b,
    };
    assert!(
        same(&p, &d),
        "`{expr}`: engine {p:?} but eval_expression {d:?}"
    );
    Ok(p)
}

/// The expression of `RETURN <expr>` evaluated by `eval_expression` directly.
fn direct_eval(store: &GraphStore, expr: &str) -> Result<PropertyValue, String> {
    let q = crate::query::parser::parse_query(&format!("RETURN {expr} AS v"))
        .map_err(|e| e.to_string())?;
    let e = q
        .return_clause
        .expect("RETURN")
        .items
        .into_iter()
        .next()
        .unwrap()
        .expression;
    match eval_expression(&e, &Record::new(), store) {
        Ok(Value::Property(p)) => Ok(p),
        Ok(Value::Null) => Ok(PropertyValue::Null),
        Ok(other) => Err(format!("non-property value {other:?}")),
        Err(e) => Err(e.to_string()),
    }
}

fn parse_expr(expr: &str) -> Expression {
    let q = crate::query::parser::parse_query(&format!("RETURN {expr} AS v"))
        .unwrap_or_else(|e| panic!("`{expr}` does not parse: {e}"));
    q.return_clause
        .expect("RETURN")
        .items
        .into_iter()
        .next()
        .unwrap()
        .expression
}

/// Evaluate and render with `toString`-like Debug-free formatting for scalars.
fn ok(expr: &str) -> PropertyValue {
    ev(expr).unwrap_or_else(|e| panic!("`{expr}` failed: {e}"))
}

fn err(expr: &str) -> String {
    match ev(expr) {
        Ok(v) => panic!("`{expr}` should fail, returned {v:?}"),
        Err(e) => e,
    }
}

fn s(expr: &str) -> String {
    match ok(expr) {
        PropertyValue::String(s) => s,
        other => panic!("`{expr}` is not a string: {other:?}"),
    }
}

fn i(expr: &str) -> i64 {
    match ok(expr) {
        PropertyValue::Integer(n) => n,
        other => panic!("`{expr}` is not an integer: {other:?}"),
    }
}

fn f(expr: &str) -> f64 {
    match ok(expr) {
        PropertyValue::Float(x) => x,
        other => panic!("`{expr}` is not a float: {other:?}"),
    }
}

fn b(expr: &str) -> bool {
    match ok(expr) {
        PropertyValue::Boolean(x) => x,
        other => panic!("`{expr}` is not a boolean: {other:?}"),
    }
}

fn null(expr: &str) {
    assert_eq!(ok(expr), PropertyValue::Null, "`{expr}` should be null");
}

/// Rows of a query as a list of the column `c` rendered via PropertyValue.
fn rows(store: &GraphStore, q: &str, c: &str) -> Vec<PropertyValue> {
    let batch = QueryEngine::new()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("`{q}`: {e}"));
    batch
        .records
        .iter()
        .map(|r| match r.get(c).expect("column") {
            Value::Property(p) => p.clone(),
            Value::Null => PropertyValue::Null,
            other => panic!("`{q}` column {c} not a property: {other:?}"),
        })
        .collect()
}

fn run_mut(store: &mut GraphStore, q: &str) -> Result<RecordBatch, String> {
    QueryEngine::new()
        .execute_mut(q, store, "default")
        .map_err(|e| e.to_string())
}

#[test]
fn helpers_smoke() {
    assert_eq!(i("1 + 2"), 3);
}

// ---------------------------------------------------------------------------
// small pure helpers
// ---------------------------------------------------------------------------

#[test]
fn write_error_classifies_quota_as_coded() {
    let e = write_error(crate::graph::GraphError::QuotaExceeded(
        "too many nodes".into(),
    ));
    match e {
        ExecutionError::Coded { code, message } => {
            assert_eq!(code, crate::query::error_code::QUOTA_EXCEEDED);
            assert!(message.contains("too many nodes"), "{message}");
        }
        other => panic!("expected Coded, got {other:?}"),
    }
    let e = write_error(crate::graph::GraphError::NodeNotFound(NodeId::new(7)));
    assert!(matches!(e, ExecutionError::GraphError(_)), "{e:?}");
}

#[test]
fn check_deadline_errors_once_the_deadline_has_passed() {
    set_query_deadline(Some(
        std::time::Instant::now() - std::time::Duration::from_secs(1),
    ));
    let r = check_deadline();
    set_query_deadline(None);
    match r {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("timed out"), "{m}"),
        other => panic!("expected timeout, got {other:?}"),
    }
    assert!(check_deadline().is_ok());
    set_query_deadline(Some(
        std::time::Instant::now() + std::time::Duration::from_secs(3600),
    ));
    let r = check_deadline();
    set_query_deadline(None);
    assert!(r.is_ok());
}

#[test]
fn node_id_of_only_answers_for_nodes() {
    assert_eq!(
        node_id_of(&Value::NodeRef(NodeId::new(3))),
        Some(NodeId::new(3))
    );
    assert_eq!(
        node_id_of(&Value::Property(PropertyValue::Integer(3))),
        None
    );
    assert_eq!(
        value_node_id(&Value::NodeRef(NodeId::new(4))),
        Some(NodeId::new(4))
    );
    assert_eq!(value_node_id(&Value::Null), None);
}

#[test]
fn cypher_equals_three_valued_on_lists_and_maps() {
    use PropertyValue as P;
    let arr = |v: Vec<P>| P::Array(v);
    let map = |v: Vec<(&str, P)>| P::Map(v.into_iter().map(|(k, v)| (k.to_string(), v)).collect());
    assert_eq!(cypher_equals(&P::Null, &P::Integer(1)), None);
    assert_eq!(
        cypher_equals(&arr(vec![P::Integer(1)]), &arr(vec![P::Null])),
        None
    );
    assert_eq!(
        cypher_equals(
            &arr(vec![P::Integer(1), P::Null]),
            &arr(vec![P::Integer(2), P::Integer(3)])
        ),
        Some(false)
    );
    assert_eq!(
        cypher_equals(
            &arr(vec![P::Integer(1)]),
            &arr(vec![P::Integer(1), P::Null])
        ),
        Some(false)
    );
    assert_eq!(
        cypher_equals(&arr(vec![P::Integer(1)]), &arr(vec![P::Float(1.0)])),
        Some(true)
    );
    assert_eq!(
        cypher_equals(
            &map(vec![("a", P::Integer(1))]),
            &map(vec![("b", P::Integer(1))])
        ),
        Some(false)
    );
    assert_eq!(
        cypher_equals(
            &map(vec![("a", P::Integer(1))]),
            &map(vec![("a", P::Integer(2))])
        ),
        Some(false)
    );
    assert_eq!(
        cypher_equals(&map(vec![("a", P::Null)]), &map(vec![("a", P::Integer(2))])),
        None
    );
    assert_eq!(
        cypher_equals(
            &map(vec![("a", P::Integer(1))]),
            &map(vec![("a", P::Integer(1))])
        ),
        Some(true)
    );
    assert_eq!(cypher_equals(&P::Integer(2), &P::Float(2.5)), Some(false));
    assert_eq!(
        cypher_equals(&P::Float(f64::NAN), &P::Integer(0)),
        Some(false)
    );
    assert_eq!(
        cypher_equals(&P::String("a".into()), &P::String("a".into())),
        Some(true)
    );
}

#[test]
fn java_double_string_matches_java_formatting() {
    assert_eq!(java_double_string(f64::NAN), "NaN");
    assert_eq!(java_double_string(f64::INFINITY), "Infinity");
    assert_eq!(java_double_string(f64::NEG_INFINITY), "-Infinity");
    assert_eq!(java_double_string(0.0), "0.0");
    assert_eq!(java_double_string(-0.0), "-0.0");
    assert_eq!(java_double_string(1.0), "1.0");
    assert_eq!(java_double_string(1.5), "1.5");
    assert_eq!(java_double_string(1e20), "1.0E20");
    assert_eq!(java_double_string(1.25e-4), "1.25E-4");
}

#[test]
fn normalized_duration_carries_nanos_with_truncation() {
    assert_eq!(
        normalized_duration(1, 2, 3, 1_500_000_000),
        PropertyValue::Duration {
            months: 1,
            days: 2,
            seconds: 4,
            nanos: 500_000_000
        }
    );
    assert_eq!(
        normalized_duration(0, 0, 0, -1),
        PropertyValue::Duration {
            months: 0,
            days: 0,
            seconds: 0,
            nanos: -1
        }
    );
}

#[test]
fn ordered_float_sorts_missing_columns_last() {
    let mut r = Record::new();
    r.bind("s", Value::Property(PropertyValue::Float(2.5)));
    assert!(ordered_float(&r, "s").0 == 2.5);
    assert!(ordered_float(&r, "missing").0 == f64::NEG_INFINITY);
}

#[test]
fn unbound_names_the_variables_in_scope() {
    let empty = Record::new();
    assert!(matches!(unbound(&empty, "x"), ExecutionError::VariableNotFound(n) if n == "x"));
    let mut r = Record::new();
    r.bind("a", Value::Null);
    match unbound(&r, "x") {
        ExecutionError::VariableNotFoundInScope { name, in_scope } => {
            assert_eq!(name, "x");
            assert_eq!(in_scope, "a");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn str_comparison_covers_every_string_operator() {
    assert_eq!(str_comparison(&BinaryOp::Eq, "a", "a"), Some(true));
    assert_eq!(str_comparison(&BinaryOp::Ne, "a", "a"), Some(false));
    assert_eq!(str_comparison(&BinaryOp::Lt, "a", "b"), Some(true));
    assert_eq!(str_comparison(&BinaryOp::Le, "b", "b"), Some(true));
    assert_eq!(str_comparison(&BinaryOp::Gt, "a", "b"), Some(false));
    assert_eq!(str_comparison(&BinaryOp::Ge, "b", "a"), Some(true));
    assert_eq!(
        str_comparison(&BinaryOp::StartsWith, "abc", "ab"),
        Some(true)
    );
    assert_eq!(str_comparison(&BinaryOp::EndsWith, "abc", "bc"), Some(true));
    assert_eq!(str_comparison(&BinaryOp::Contains, "abc", "x"), Some(false));
    assert_eq!(str_comparison(&BinaryOp::Add, "a", "b"), None);
}

#[test]
fn type_name_of_names_every_value_kind() {
    assert_eq!(type_name_of(&Value::Null), "null");
    assert_eq!(type_name_of(&Value::NodeRef(NodeId::new(1))), "Node");
    assert_eq!(
        type_name_of(&Value::Path {
            nodes: vec![],
            edges: vec![]
        }),
        "Path"
    );
    assert_eq!(type_name_of(&Value::List(vec![])), "List");
    assert_eq!(type_name_of(&Value::Map(Default::default())), "Map");
    assert_eq!(
        type_name_of(&Value::EdgeRef(
            crate::graph::EdgeId::new(1),
            NodeId::new(1),
            NodeId::new(2),
            EdgeType::new("R")
        )),
        "Relationship"
    );
}

// ---------------------------------------------------------------------------
// binary / unary operators through Cypher
// ---------------------------------------------------------------------------

#[test]
fn arithmetic_mixed_int_float() {
    assert_eq!(f("1 + 2.5"), 3.5);
    assert_eq!(f("2.5 + 1"), 3.5);
    assert_eq!(f("1.5 + 2.5"), 4.0);
    assert_eq!(f("5 - 1.5"), 3.5);
    assert_eq!(f("5.5 - 1"), 4.5);
    assert_eq!(f("5.5 - 1.5"), 4.0);
    assert_eq!(f("2 * 1.5"), 3.0);
    assert_eq!(f("1.5 * 2"), 3.0);
    assert_eq!(f("1.5 * 2.0"), 3.0);
    assert_eq!(f("3 / 2.0"), 1.5);
    assert_eq!(f("3.0 / 2"), 1.5);
    assert_eq!(f("3.0 / 2.0"), 1.5);
    assert_eq!(f("7 % 2.5"), 2.0);
    assert_eq!(f("7.5 % 2"), 1.5);
    assert_eq!(f("7.5 % 2.5"), 0.0);
    assert_eq!(i("7 % 3"), 1);
    assert_eq!(f("2 ^ 3"), 8.0);
    assert_eq!(f("2 ^ -1"), 0.5);
}

#[test]
fn arithmetic_overflow_and_zero_division_are_errors() {
    assert!(err("9223372036854775807 + 1").contains("out of range"));
    assert!(err("-9223372036854775807 - 10").contains("out of range"));
    assert!(err("9223372036854775807 * 2").contains("out of range"));
    assert!(err("1 / 0").contains("Division by zero"));
    assert!(err("1 % 0").contains("Modulo by zero"));
    assert!(err("duration('P1D') / 0").contains("divide a duration by zero"));
}

#[test]
fn arithmetic_type_errors() {
    assert!(err("true + 1").contains("`+`"));
    assert!(err("true - 1").contains("Sub"));
    assert!(err("'a' * 2").contains("Mul"));
    assert!(err("'a' / 2").contains("Div"));
    assert!(err("'a' % 2").contains("Mod"));
    assert!(err("'a' ^ 2").contains("^"));
    // Literals of the wrong type are refused when the query is parsed...
    assert!(err("1 XOR true").contains("XOR"));
    // ...values only known at run time reach the evaluator.
    assert!(err("[1][0] XOR true").contains("XOR"));
    assert!(err("[1][0] AND true").contains("AND"));
    assert!(err("[1][0] OR false").contains("OR"));
    assert!(err("1 =~ 'x'").contains("=~"));
    assert!(err("'a' =~ '('").contains("Invalid regex"));
    assert!(err("1 IN 2").contains("IN requires a list"));
}

#[test]
fn null_propagates_through_arithmetic_and_logic() {
    null("null + 1");
    null("1 - null");
    null("null * 2");
    null("2 / null");
    null("null % 2");
    null("null ^ 2");
    null("null XOR true");
    null("null AND true");
    null("true AND null");
    assert!(!b("false AND null"));
    assert!(!b("null AND false"));
    null("null OR false");
    assert!(b("null OR true"));
    assert!(b("true OR null"));
    null("'a' =~ null");
    null("null = 1");
    null("1 <> null");
    null("null < 1");
    null("[1] + null");
}

#[test]
fn xor_and_regex_on_proper_operands() {
    assert!(b("true XOR false"));
    assert!(!b("true XOR true"));
    assert!(b("'abc' =~ 'a.c'"));
    assert!(!b("'abc' =~ 'x'"));
}

#[test]
fn string_number_concatenation_uses_java_float_format() {
    assert_eq!(s("'a' + 1"), "a1");
    assert_eq!(s("1 + 'a'"), "1a");
    assert_eq!(s("'a' + 1.0"), "a1.0");
    assert_eq!(s("1.5 + 'a'"), "1.5a");
}

#[test]
fn list_concatenation_and_append() {
    let ints =
        |v: &[i64]| PropertyValue::Array(v.iter().map(|x| PropertyValue::Integer(*x)).collect());
    assert_eq!(ok("[1, 2] + [3]"), ints(&[1, 2, 3]));
    assert_eq!(ok("[1] + 2"), ints(&[1, 2]));
    assert_eq!(ok("0 + [1]"), ints(&[0, 1]));
    null("null + [1]");
}

#[test]
fn list_of_nodes_concatenates_as_entity_list() {
    let mut store = GraphStore::new();
    store.create_node("A");
    let batch = QueryEngine::new()
        .execute(
            "MATCH (a:A) RETURN [a] + [1] AS l, 2 + [a] AS m, [a] + a AS k",
            &store,
        )
        .unwrap();
    let r = &batch.records[0];
    match r.get("l").unwrap() {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert!(items[0].is_node());
        }
        other => panic!("{other:?}"),
    }
    match r.get("m").unwrap() {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert!(items[1].is_node());
        }
        other => panic!("{other:?}"),
    }
    match r.get("k").unwrap() {
        Value::List(items) => assert_eq!(items.len(), 2),
        other => panic!("{other:?}"),
    }
}

#[test]
fn comparisons_across_types_and_nan() {
    null("1 < 'a'");
    assert!(!b("0.0/0.0 < 1"));
    assert!(!b("1 >= 0.0/0.0"));
    null("0.0/0.0 < 'a'");
    assert!(b("1 < 1.5"));
    assert!(b("1.5 > 1"));
    assert!(b("1.5 <= 1.5"));
    assert!(b("true > false"));
    assert!(b("[1, 0] >= [1]"));
    assert!(b("[1, 2] < [1, 3]"));
    null("[1, null] < [1, 2]");
    assert!(b("duration('P1D') < duration('P2D')"));
    assert!(b("date('2020-01-01') < date('2020-01-02')"));
    assert!(b("localtime('10:00') < localtime('11:00')"));
    assert!(b("time('10:00+01:00') < time('10:00Z')"));
    assert!(b(
        "localdatetime('2020-01-01T10:00') < localdatetime('2020-01-01T10:01')"
    ));
    assert!(b(
        "datetime('2020-01-01T10:00Z') <= datetime('2020-01-01T10:00Z')"
    ));
    null("date('2020-01-01') < localtime('10:00')");
}

#[test]
fn unary_operators() {
    assert!(b("NOT false"));
    null("NOT null");
    assert!(err("NOT 1").contains("NOT requires boolean"));
    assert_eq!(f("-(1.5)"), -1.5);
    null("-null");
    assert!(err("-'a'").contains("Negation"));
    assert!(err("-(-9223372036854775807 - 1)").contains("out of range"));
    assert!(b("null IS NULL"));
    assert!(!b("1 IS NULL"));
    assert!(b("1 IS NOT NULL"));
}

fn one_on(store: &GraphStore, q: &str) -> PropertyValue {
    let got = rows(store, q, "v");
    assert_eq!(got.len(), 1, "`{q}` gave {got:?}");
    got.into_iter().next().unwrap()
}

#[test]
fn entity_comparison_and_arithmetic() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b2 = store.create_node("A");
    store.create_edge(a, b2, "R").unwrap();
    let one = |e: &str| {
        one_on(
            &store,
            &format!("MATCH p = (a:A)-[r:R]->(b:A) RETURN {e} AS v"),
        )
    };
    assert_eq!(one("r = r"), PropertyValue::Boolean(true));
    assert_eq!(one("p = p"), PropertyValue::Boolean(true));
    assert_eq!(one("a <> b"), PropertyValue::Boolean(true));
    assert_eq!(one("a = 1"), PropertyValue::Boolean(false));
    assert_eq!(one("1 <> r"), PropertyValue::Boolean(true));
    assert_eq!(one("a < 1"), PropertyValue::Null);
    assert_eq!(one("1 > p"), PropertyValue::Null);
    let e = QueryEngine::new()
        .execute("MATCH (a:A)-[r:R]->(b) RETURN a + 1 AS v", &store)
        .unwrap_err()
        .to_string();
    assert!(e.contains("on the left"), "{e}");
    let e = QueryEngine::new()
        .execute("MATCH (a:A)-[r:R]->(b) RETURN 1 - r AS v", &store)
        .unwrap_err()
        .to_string();
    assert!(e.contains("on the right"), "{e}");
}

#[test]
#[ignore = "bug: `n = null` on a node/relationship/path raises a TypeError instead of returning null"]
fn entity_equality_with_null_is_null() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b2 = store.create_node("A");
    store.create_edge(a, b2, "R").unwrap();
    let one = |e: &str| {
        one_on(
            &store,
            &format!("MATCH p = (a:A)-[r:R]->(b:A) RETURN {e} AS v"),
        )
    };
    assert_eq!(one("a = null"), PropertyValue::Null);
    assert_eq!(one("null <> r"), PropertyValue::Null);
    assert_eq!(one("p = null"), PropertyValue::Null);
}

// ---------------------------------------------------------------------------
// indexing and slicing
// ---------------------------------------------------------------------------

#[test]
fn list_and_map_indexing() {
    assert_eq!(i("[1, 2, 3][-1]"), 3);
    null("[1, 2, 3][5]");
    assert_eq!(i("{a: 1}['a']"), 1);
    null("{a: 1}['b']");
    null("null[0]");
    null("[1][null]");
    assert!(err("[1, 2]['x']").contains("list index must be an integer"));
    assert!(err("{a: 1}[0]").contains("map key must be a string"));
    assert!(err("true[0]").contains("cannot index"));
    assert!(err("1['x']").contains("cannot index"));
    assert_eq!(i("date('2024-05-06')['year']"), 2024);
}

#[test]
fn indexing_entity_lists_and_maps() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    store.set_node_property("default", a, "name", "x").unwrap();
    let run = |q: &str| QueryEngine::new().execute(q, &store);
    let batch = run("MATCH (a:A) WITH a, [a, 1] AS l, {k: a} AS m \
                     RETURN l[0].name AS n, l[-1] AS last, l[-5] AS neg, l[9] AS far, \
                     m['k'].name AS mk, m['zz'] AS mz, a['name'] AS an")
    .unwrap();
    let r = &batch.records[0];
    let x = PropertyValue::String("x".into());
    assert_eq!(r.get("n").unwrap().as_property(), Some(&x));
    assert_eq!(
        r.get("last").unwrap().as_property(),
        Some(&PropertyValue::Integer(1))
    );
    assert!(r.get("neg").unwrap().is_null());
    assert!(r.get("far").unwrap().is_null());
    assert_eq!(r.get("mk").unwrap().as_property(), Some(&x));
    assert!(r.get("mz").unwrap().is_null());
    assert_eq!(r.get("an").unwrap().as_property(), Some(&x));
    let e = run("MATCH (a:A) WITH [a, 1] AS l RETURN l['x'] AS v")
        .unwrap_err()
        .to_string();
    assert!(e.contains("list index must be an integer"), "{e}");
    let e = run("MATCH (a:A) WITH {k: a} AS m RETURN m[1] AS v")
        .unwrap_err()
        .to_string();
    assert!(e.contains("map key must be a string"), "{e}");
}

#[test]
fn list_slicing() {
    let ints =
        |v: &[i64]| PropertyValue::Array(v.iter().map(|x| PropertyValue::Integer(*x)).collect());
    assert_eq!(ok("[1, 2, 3][1..]"), ints(&[2, 3]));
    assert_eq!(ok("[1, 2, 3][..2]"), ints(&[1, 2]));
    assert_eq!(ok("[1, 2, 3][-2..]"), ints(&[2, 3]));
    assert_eq!(ok("[1, 2, 3][2..1]"), ints(&[]));
    assert_eq!(ok("[1, 2, 3][5..9]"), ints(&[]));
    null("[1, 2, 3][1..null]");
    null("[1, 2, 3][null..2]");
    null("'abc'[0..1]");
}

// ---------------------------------------------------------------------------
// IN, comprehensions, quantifiers, reduce
// ---------------------------------------------------------------------------

#[test]
fn in_list_three_valued() {
    null("1 IN null");
    null("null IN [null]");
    null("4 IN [1, null, 3]");
    assert!(b("1 IN [1, null]"));
    assert!(!b("null IN []"));
    assert!(!b("[1] IN [[1, null]]"));
    assert!(!b("[1, 2] IN [[null, 'foo']]"));
    null("[1, 2] IN [[1, null]]");
    assert!(b("[1, 2] IN [[1, 2]]"));
    assert!(b("7.0 IN [7, 99]"));
    assert!(b("{a: 1} IN [{a: 1}]"));
    assert!(!b("{a: 1} IN [{b: 1}]"));
    assert!(!b("{a: 1} IN [{a: 2}]"));
    null("{a: null} IN [{a: 2}]");
    assert!(!b("'x' IN [1, 2]"));
}

#[test]
fn list_comprehension_forms() {
    let ints =
        |v: &[i64]| PropertyValue::Array(v.iter().map(|x| PropertyValue::Integer(*x)).collect());
    assert_eq!(ok("[x IN [1, 2, 3] WHERE x > 1 | x * 10]"), ints(&[20, 30]));
    null("[x IN null | x]");
    assert!(err("[x IN 1 | x]").contains("list comprehension needs a list"));
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b2 = store.create_node("A");
    store.create_edge(a, b2, "R").unwrap();
    let batch = QueryEngine::new()
        .execute(
            "MATCH p = (a:A)-[:R]->(b) RETURN [x IN nodes(p) | x] AS ns, [x IN [a, b] WHERE x = a | 1] AS ones",
            &store,
        )
        .unwrap();
    match batch.records[0].get("ns").unwrap() {
        Value::List(items) => assert_eq!(items.len(), 2),
        other => panic!("{other:?}"),
    }
    assert_eq!(
        batch.records[0].get("ones").unwrap().as_property(),
        Some(&ints(&[1]))
    );
}

#[test]
fn quantifier_functions_three_valued() {
    null("any(x IN [0, null] WHERE x = 2)");
    null("all(x IN [2, null] WHERE x = 2)");
    null("single(x IN [2, null] WHERE x = 2)");
    null("none(x IN [0, null] WHERE x = 2)");
    assert!(!b("single(x IN [2, 2, null] WHERE x = 2)"));
    assert!(b("single(x IN [1, 2] WHERE x = 2)"));
    assert!(!b("none(x IN [2] WHERE x = 2)"));
    assert!(b("none(x IN [1] WHERE x = 2)"));
    assert!(!b("all(x IN [1, 2] WHERE x = 2)"));
    assert!(b("all(x IN [] WHERE x = 2)"));
    assert!(!b("any(x IN [] WHERE x = 2)"));
    assert!(!b("any(x IN 5 WHERE x = 2)"));
    // A predicate that is neither boolean nor null counts as false.
    assert!(!b("any(x IN [1] WHERE 5)"));
}

#[test]
fn reduce_over_values_and_entities() {
    assert_eq!(i("reduce(acc = 0, x IN [1, 2, 3] | acc + x)"), 6);
    assert_eq!(i("reduce(acc = 7, x IN 5 | acc + x)"), 7);
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b2 = store.create_node("A");
    store.create_edge(a, b2, "R").unwrap();
    let got = one_on(
        &store,
        "MATCH p = (a:A)-[:R]->(b) RETURN reduce(acc = 0, x IN nodes(p) | acc + 1) AS v",
    );
    assert_eq!(got, PropertyValue::Integer(2));
}

// ---------------------------------------------------------------------------
// eval_function called directly
// ---------------------------------------------------------------------------

fn pv(p: impl Into<PropertyValue>) -> Value {
    Value::Property(p.into())
}

fn call(name: &str, args: &[Value]) -> Result<Value, ExecutionError> {
    eval_function(name, args, None)
}

fn call_on(store: &GraphStore, name: &str, args: &[Value]) -> Result<Value, ExecutionError> {
    eval_function(name, args, Some(store))
}

fn prop(v: Result<Value, ExecutionError>) -> PropertyValue {
    match v {
        Ok(Value::Property(p)) => p,
        Ok(Value::Null) => PropertyValue::Null,
        other => panic!("expected a property, got {other:?}"),
    }
}

fn err_msg(v: Result<Value, ExecutionError>) -> String {
    match v {
        Err(e) => e.to_string(),
        Ok(v) => panic!("expected an error, got {v:?}"),
    }
}

/// A store with `(a:A {name:'x', n:1})-[:R {w: 2}]->(b:A:B)`.
fn small_graph() -> (GraphStore, NodeId, NodeId, crate::graph::EdgeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b2 = store.create_node("A");
    store.add_label_to_node("default", b2, "B").unwrap();
    store.set_node_property("default", a, "name", "x").unwrap();
    store.set_node_property("default", a, "n", 1i64).unwrap();
    let e = store.create_edge(a, b2, "R").unwrap();
    store.set_edge_property(e, "w", 2i64).unwrap();
    (store, a, b2, e)
}

#[test]
fn eval_function_arity_is_checked_before_dispatch() {
    let e = err_msg(call("toUpper", &[]));
    assert!(e.contains("takes at least 1 argument;"), "{e}");
    let e = err_msg(call("atan2", &[pv(1i64)]));
    assert!(e.contains("takes at least 2 arguments"), "{e}");
    assert!(err_msg(call("no_such_function", &[pv(1i64)])).contains("Unknown function"));
}

#[test]
fn eval_function_null_propagates_except_for_tolerant_functions() {
    assert!(matches!(call("toUpper", &[Value::Null]), Ok(Value::Null)));
    assert_eq!(
        prop(call("coalesce", &[Value::Null, pv(3i64)])),
        PropertyValue::Integer(3)
    );
    assert!(matches!(call("coalesce", &[Value::Null]), Ok(Value::Null)));
    assert!(err_msg(call("coalesce", &[])).contains("at least one argument"));
    assert_eq!(
        prop(call("exists", &[Value::Null])),
        PropertyValue::Boolean(false)
    );
    assert_eq!(
        prop(call("exists", &[pv(1i64)])),
        PropertyValue::Boolean(true)
    );
}

#[test]
fn string_functions() {
    assert_eq!(s("toLower('AbC')"), "abc");
    assert_eq!(s("toUpperCase('ab')"), "AB");
    assert_eq!(s("toLowerCase('AB')"), "ab");
    assert_eq!(s("trim('  a  ')"), "a");
    assert_eq!(s("ltrim('  a  ')"), "a  ");
    assert_eq!(s("rtrim('  a  ')"), "  a");
    assert_eq!(s("replace('aXbX', 'X', '-')"), "a-b-");
    assert_eq!(
        ok("split('a,b', ',')"),
        PropertyValue::Array(vec!["a".into(), "b".into()])
    );
    assert_eq!(
        ok("split('ab', '')"),
        PropertyValue::Array(vec!["a".into(), "b".into()])
    );
    assert!(err_msg(call("split", &[pv("a")])).contains("split() requires 2 arguments"));
    assert!(err_msg(call("replace", &[pv("a"), pv("b")])).contains("replace() requires 3"));
    assert!(err_msg(call("toUpper", &[pv(1i64)])).contains("Expected string"));
}

#[test]
fn substring_left_right_and_reverse() {
    assert_eq!(s("substring('hello', 1, 3)"), "ell");
    assert_eq!(s("substring('hello', 2)"), "llo");
    assert_eq!(s("substring('hello', 9)"), "");
    assert!(err("substring('hello', -1)").contains("negative start"));
    assert!(err("substring('hello', 1, -1)").contains("negative length"));
    assert!(err_msg(call("substring", &[pv("a")])).contains("at least 2 arguments"));
    assert!(err_msg(call("substring", &[pv("a"), pv("b")])).contains("Expected integer"));
    assert_eq!(s("left('hello', 2)"), "he");
    assert!(err("left('hello', -1)").contains("negative length"));
    assert_eq!(s("right('hello', 2)"), "lo");
    assert_eq!(s("right('hi', 5)"), "hi");
    assert!(err("right('hello', -1)").contains("negative length"));
    assert_eq!(s("reverse('abc')"), "cba");
    assert_eq!(
        ok("reverse([1, 2])"),
        PropertyValue::Array(vec![2i64.into(), 1i64.into()])
    );
    null("reverse(null)");
    assert!(err_msg(call("reverse", &[pv(true)])).contains("Expected string"));
}

#[test]
fn to_string_of_every_kind() {
    assert_eq!(s("toString('a')"), "a");
    assert_eq!(s("toString(12)"), "12");
    assert_eq!(s("toString(1.0)"), "1.0");
    assert_eq!(s("toString(true)"), "true");
    assert_eq!(s("toString(duration('P1D'))"), "P1D");
    assert_eq!(s("toString(date('2020-01-02'))"), "2020-01-02");
    assert_eq!(s("toString(localtime('10:11'))"), "10:11");
    assert_eq!(
        prop(call("toString", &[pv(PropertyValue::DateTime(0))])),
        PropertyValue::String("1970-01-01T00:00:00+00:00".into())
    );
    assert!(
        err_msg(call("toString", &[pv(PropertyValue::Array(vec![]))]))
            .contains("Cannot convert to string")
    );
}

#[test]
fn numeric_conversions() {
    assert_eq!(i("toInteger(3)"), 3);
    assert_eq!(i("toInteger(3.9)"), 3);
    assert_eq!(i("toInteger(' 42 ')"), 42);
    assert_eq!(i("toInteger('2.9')"), 2);
    null("toInteger('foo')");
    assert!(err_msg(call("toInt", &[pv(true)])).contains("Cannot convert to integer"));
    assert_eq!(f("toFloat(2)"), 2.0);
    assert_eq!(f("toFloat(2.5)"), 2.5);
    assert_eq!(f("toFloat('2.5')"), 2.5);
    null("toFloat('x')");
    assert!(err_msg(call("toFloat", &[pv(true)])).contains("Cannot convert to float"));
    assert!(b("toBoolean('TRUE')"));
    assert!(!b("toBoolean('false')"));
    null("toBoolean('maybe')");
    assert!(b("toBoolean(1)"));
    assert!(b("toBoolean(true)"));
    assert!(err_msg(call("toBoolean", &[pv(1.5f64)])).contains("toBoolean()"));
    assert!(b("toBooleanOrNull('true')"));
    assert!(!b("toBooleanOrNull('false')"));
    assert!(b("toBooleanOrNull(true)"));
    null("toBooleanOrNull('x')");
    null("toBooleanOrNull(1.5)");
    assert_eq!(i("toIntegerOrNull(4)"), 4);
    assert_eq!(i("toIntegerOrNull(4.7)"), 4);
    assert_eq!(i("toIntegerOrNull('5')"), 5);
    null("toIntegerOrNull('x')");
    null("toIntegerOrNull(true)");
    assert_eq!(f("toFloatOrNull(1.5)"), 1.5);
    assert_eq!(f("toFloatOrNull(2)"), 2.0);
    assert_eq!(f("toFloatOrNull('2.5')"), 2.5);
    null("toFloatOrNull('x')");
    null("toFloatOrNull(true)");
    assert_eq!(s("toStringOrNull('a')"), "a");
    assert_eq!(s("toStringOrNull(1)"), "1");
    assert_eq!(s("toStringOrNull(1.5)"), "1.5");
    assert_eq!(s("toStringOrNull(false)"), "false");
    null("toStringOrNull([1])");
}

#[test]
fn size_and_list_accessors() {
    assert_eq!(i("size('abc')"), 3);
    assert_eq!(i("size([1, 2])"), 2);
    assert_eq!(i("size([1.5, 2.5])"), 2);
    assert!(err_msg(call("size", &[pv(1i64)])).contains("size() requires"));
    assert_eq!(
        prop(call("size", &[Value::List(vec![Value::Null, Value::Null])])),
        PropertyValue::Integer(2)
    );
    assert_eq!(
        prop(call(
            "length",
            &[Value::Path {
                nodes: vec![NodeId::new(1), NodeId::new(2)],
                edges: vec![crate::graph::EdgeId::new(1)]
            }]
        )),
        PropertyValue::Integer(1)
    );
    assert_eq!(i("head([1, 2])"), 1);
    null("head([])");
    assert_eq!(i("last([1, 2])"), 2);
    null("last([])");
    assert_eq!(ok("tail([1, 2])"), PropertyValue::Array(vec![2i64.into()]));
    assert!(err_msg(call("head", &[pv(1i64)])).contains("head() requires list"));
    assert!(err_msg(call("last", &[pv(1i64)])).contains("last() requires list"));
    assert!(err_msg(call("tail", &[pv(1i64)])).contains("tail() requires list"));
    let l = Value::List(vec![
        Value::NodeRef(NodeId::new(1)),
        Value::NodeRef(NodeId::new(2)),
    ]);
    assert!(matches!(call("head", &[l.clone()]), Ok(Value::NodeRef(id)) if id == NodeId::new(1)));
    assert!(matches!(call("last", &[l.clone()]), Ok(Value::NodeRef(id)) if id == NodeId::new(2)));
    assert!(matches!(call("tail", &[l]), Ok(Value::List(v)) if v.len() == 1));
    assert!(matches!(
        call("head", &[Value::List(vec![])]),
        Ok(Value::Null)
    ));
    assert!(matches!(
        call("last", &[Value::List(vec![])]),
        Ok(Value::Null)
    ));
}

#[test]
fn path_functions_need_a_path() {
    assert!(err_msg(call("nodes", &[pv(1i64)])).contains("nodes() requires a path"));
    assert!(err_msg(call("relationships", &[pv(1i64)])).contains("relationships() requires a path"));
    let (store, a, b2, e) = small_graph();
    let path = Value::Path {
        nodes: vec![a, b2],
        edges: vec![e, crate::graph::EdgeId::new(999)],
    };
    match call_on(&store, "rels", &[path]).unwrap() {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert!(items[0].is_edge());
            assert!(items[1].is_null(), "a missing relationship reads as null");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn math_functions() {
    assert_eq!(i("abs(-3)"), 3);
    assert_eq!(f("abs(-1.5)"), 1.5);
    assert!(err_msg(call("abs", &[pv("a")])).contains("abs() requires numeric"));
    assert_eq!(i("ceil(1.2)"), 2);
    assert_eq!(i("ceil(4)"), 4);
    assert!(err_msg(call("ceil", &[pv("a")])).contains("ceil()"));
    assert_eq!(i("floor(1.8)"), 1);
    assert_eq!(i("floor(4)"), 4);
    assert!(err_msg(call("floor", &[pv("a")])).contains("floor()"));
    assert_eq!(i("round(1.5)"), 2);
    assert_eq!(i("round(4)"), 4);
    assert_eq!(f("round(3.14159, 2)"), 3.14);
    assert!(err_msg(call("round", &[pv("a")])).contains("round()"));
    assert_eq!(f("sqrt(16)"), 4.0);
    assert_eq!(f("sqrt(2.25)"), 1.5);
    assert!(err_msg(call("sqrt", &[pv("a")])).contains("sqrt()"));
    assert_eq!(i("sign(-4)"), -1);
    assert_eq!(i("sign(2.5)"), 1);
    assert_eq!(i("sign(-2.5)"), -1);
    assert_eq!(i("sign(0.0)"), 0);
    assert!(err_msg(call("sign", &[pv("a")])).contains("sign()"));
    assert_eq!(f("log(1)"), 0.0);
    assert_eq!(f("log(1.0)"), 0.0);
    assert!(err_msg(call("log", &[pv("a")])).contains("log()"));
    assert_eq!(f("exp(0)"), 1.0);
    assert_eq!(f("exp(0.0)"), 1.0);
    assert!(err_msg(call("exp", &[pv("a")])).contains("exp()"));
    assert_eq!(f("log10(100)"), 2.0);
    assert!(b("isNaN(0.0/0.0)"));
    assert!(!b("isNaN(1)"));
    assert!(err_msg(call("isNaN", &[pv("a")])).contains("Expected numeric"));
    assert_eq!(f("e()"), std::f64::consts::E);
    assert_eq!(f("pi()"), std::f64::consts::PI);
    match prop(call("rand", &[])) {
        PropertyValue::Float(r) => assert!((0.0..1.0).contains(&r)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn trigonometric_functions() {
    let close = |e: &str, want: f64| {
        let got = f(e);
        assert!((got - want).abs() < 1e-9, "{e} = {got}, want {want}");
    };
    close("sin(0)", 0.0);
    close("cos(0)", 1.0);
    close("tan(0)", 0.0);
    close("cot(1)", 1.0 / 1f64.tan());
    close("asin(1)", std::f64::consts::FRAC_PI_2);
    close("acos(1)", 0.0);
    close("atan(1)", std::f64::consts::FRAC_PI_4);
    close("atan2(1, 1)", std::f64::consts::FRAC_PI_4);
    close("sinh(0)", 0.0);
    close("cosh(0)", 1.0);
    close("tanh(0)", 0.0);
    close("degrees(pi())", 180.0);
    close("radians(180)", std::f64::consts::PI);
    close("haversin(0)", 0.0);
}

#[test]
fn range_forms() {
    let ints =
        |v: &[i64]| PropertyValue::Array(v.iter().map(|x| PropertyValue::Integer(*x)).collect());
    assert_eq!(ok("range(1, 3)"), ints(&[1, 2, 3]));
    assert_eq!(ok("range(0, 6, 3)"), ints(&[0, 3, 6]));
    assert_eq!(ok("range(3, 1, -1)"), ints(&[3, 2, 1]));
    assert!(err("range(1, 3, 0)").contains("step cannot be 0"));
    assert!(err_msg(call("range", &[pv(1i64)])).contains("at least 2 arguments"));
}

#[test]
fn meta_functions_on_entities() {
    let (store, a, b2, e) = small_graph();
    let node = Value::NodeRef(a);
    let edge = Value::EdgeRef(e, a, b2, EdgeType::new("R"));
    let full_node = Value::Node(a, Box::new(store.get_node(a).unwrap().clone()));
    let full_edge = Value::Edge(e, Box::new(store.get_edge(e).unwrap().clone()));

    assert_eq!(
        prop(call("id", &[edge.clone()])),
        PropertyValue::Integer(e.as_u64() as i64)
    );
    assert!(err_msg(call("id", &[pv(1i64)])).contains("id() requires"));
    assert_eq!(
        prop(call("elementId", &[node.clone()])),
        PropertyValue::String(format!("node:{}", a.as_u64()))
    );
    assert_eq!(
        prop(call("elementId", &[edge.clone()])),
        PropertyValue::String(format!("edge:{}", e.as_u64()))
    );
    assert!(err_msg(call("elementId", &[pv(1i64)])).contains("elementId()"));

    // labels
    assert_eq!(
        prop(call("labels", &[full_node.clone()])),
        PropertyValue::Array(vec!["A".into()])
    );
    assert_eq!(
        prop(call_on(&store, "labels", &[Value::NodeRef(b2)])),
        PropertyValue::Array(vec!["A".into(), "B".into()])
    );
    assert!(err_msg(call("labels", &[node.clone()])).contains("requires store"));
    assert!(err_msg(call_on(
        &store,
        "labels",
        &[Value::NodeRef(NodeId::new(999))]
    ))
    .contains("not found"));
    assert!(err_msg(call("labels", &[pv(1i64)])).contains("labels() requires a node"));

    // type / startNode / endNode
    assert_eq!(
        prop(call("type", &[full_edge.clone()])),
        PropertyValue::String("R".into())
    );
    assert!(err_msg(call("type", &[pv(1i64)])).contains("type() requires"));
    assert!(matches!(call("startNode", &[full_edge.clone()]), Ok(Value::NodeRef(id)) if id == a));
    assert!(matches!(call("endNode", &[full_edge.clone()]), Ok(Value::NodeRef(id)) if id == b2));
    assert!(matches!(call("endNode", &[edge.clone()]), Ok(Value::NodeRef(id)) if id == b2));
    assert!(err_msg(call("startNode", &[pv(1i64)])).contains("startNode()"));
    assert!(err_msg(call("endNode", &[pv(1i64)])).contains("endNode()"));

    // keys
    let keys = PropertyValue::Array(vec!["n".into(), "name".into()]);
    assert_eq!(prop(call_on(&store, "keys", &[full_node.clone()])), keys);
    // Without a store only the node's row-storage map is consulted.
    let mut row_keys: Vec<String> = store
        .get_node(a)
        .unwrap()
        .properties
        .keys()
        .cloned()
        .collect();
    row_keys.sort();
    assert_eq!(
        prop(call("keys", &[full_node.clone()])),
        PropertyValue::Array(row_keys.into_iter().map(PropertyValue::String).collect())
    );
    assert_eq!(prop(call_on(&store, "keys", &[node.clone()])), keys);
    assert!(err_msg(call("keys", &[node.clone()])).contains("requires store"));
    assert!(
        err_msg(call_on(&store, "keys", &[Value::NodeRef(NodeId::new(999))])).contains("not found")
    );
    assert_eq!(
        prop(call("keys", &[full_edge.clone()])),
        PropertyValue::Array(vec!["w".into()])
    );
    assert_eq!(
        prop(call_on(&store, "keys", &[edge.clone()])),
        PropertyValue::Array(vec!["w".into()])
    );
    assert!(err_msg(call("keys", &[edge.clone()])).contains("requires store"));
    let ghost = Value::EdgeRef(crate::graph::EdgeId::new(999), a, b2, EdgeType::new("R"));
    assert!(err_msg(call_on(&store, "keys", &[ghost.clone()])).contains("not found"));
    assert!(err_msg(call("keys", &[pv(1i64)])).contains("keys() requires"));
    assert_eq!(
        ok("keys({b: 1, a: 2})"),
        PropertyValue::Array(vec!["a".into(), "b".into()])
    );

    // properties
    let props = prop(call_on(&store, "properties", &[full_node.clone()]));
    assert_eq!(
        props.as_map().unwrap().get("name"),
        Some(&PropertyValue::String("x".into()))
    );
    let props = prop(call("properties", &[full_node]));
    assert!(props.as_map().is_some());
    let props = prop(call_on(&store, "properties", &[node.clone()]));
    assert_eq!(
        props.as_map().unwrap().get("n"),
        Some(&PropertyValue::Integer(1))
    );
    assert!(err_msg(call("properties", &[node])).contains("requires store"));
    assert!(err_msg(call_on(
        &store,
        "properties",
        &[Value::NodeRef(NodeId::new(999))]
    ))
    .contains("not found"));
    let props = prop(call("properties", &[full_edge]));
    assert_eq!(
        props.as_map().unwrap().get("w"),
        Some(&PropertyValue::Integer(2))
    );
    let props = prop(call_on(&store, "properties", &[edge.clone()]));
    assert_eq!(
        props.as_map().unwrap().get("w"),
        Some(&PropertyValue::Integer(2))
    );
    assert!(err_msg(call("properties", &[edge])).contains("requires store"));
    assert!(err_msg(call_on(&store, "properties", &[ghost])).contains("not found"));
    assert_eq!(ok("properties({a: 1})").as_map().unwrap().len(), 1);
    assert!(err_msg(call("properties", &[pv(1i64)])).contains("properties() requires"));
}

#[test]
fn has_labels_on_nodes_and_relationships() {
    let (store, a, b2, e) = small_graph();
    let labels = |v: &[&str]| {
        pv(PropertyValue::Array(
            v.iter()
                .map(|s| PropertyValue::String(s.to_string()))
                .collect(),
        ))
    };
    let edge = Value::EdgeRef(e, a, b2, EdgeType::new("R"));
    let full_edge = Value::Edge(e, Box::new(store.get_edge(e).unwrap().clone()));
    assert_eq!(
        prop(call("hasLabels", &[edge.clone(), labels(&["R"])])),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        prop(call("hasLabels", &[full_edge, labels(&["R", "S"])])),
        PropertyValue::Boolean(false)
    );
    assert_eq!(
        prop(call_on(
            &store,
            "hasLabels",
            &[Value::NodeRef(b2), labels(&["A", "B"])]
        )),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        prop(call_on(
            &store,
            "hasLabels",
            &[Value::NodeRef(a), labels(&["B"])]
        )),
        PropertyValue::Boolean(false)
    );
    let full = Value::Node(a, Box::new(store.get_node(a).unwrap().clone()));
    assert_eq!(
        prop(call("hasLabels", &[full, labels(&["A"])])),
        PropertyValue::Boolean(true)
    );
    assert!(
        err_msg(call("hasLabels", &[Value::NodeRef(a), labels(&["A"])])).contains("requires store")
    );
    assert_eq!(
        prop(call_on(
            &store,
            "hasLabels",
            &[Value::NodeRef(NodeId::new(999)), labels(&["A"])]
        )),
        PropertyValue::Null
    );
    assert!(err_msg(call("hasLabels", &[pv(1i64), labels(&["A"])])).contains("label test requires"));
    assert!(err_msg(call("hasLabels", &[Value::NodeRef(a), pv(1i64)])).contains("label list"));
}

#[test]
fn value_type_and_is_empty() {
    assert_eq!(s("valueType(1)"), "INTEGER");
    assert_eq!(s("valueType(1.5)"), "FLOAT");
    assert_eq!(s("valueType('a')"), "STRING");
    assert_eq!(s("valueType(true)"), "BOOLEAN");
    assert_eq!(s("valueType([1])"), "LIST");
    assert_eq!(s("valueType({a: 1})"), "MAP");
    assert_eq!(
        prop(call("valueType", &[Value::NodeRef(NodeId::new(1))])),
        PropertyValue::String("NODE".into())
    );
    assert_eq!(
        prop(call(
            "valueType",
            &[Value::EdgeRef(
                crate::graph::EdgeId::new(1),
                NodeId::new(1),
                NodeId::new(2),
                EdgeType::new("R")
            )]
        )),
        PropertyValue::String("RELATIONSHIP".into())
    );
    assert_eq!(
        prop(call(
            "valueType",
            &[Value::Path {
                nodes: vec![],
                edges: vec![]
            }]
        )),
        PropertyValue::String("PATH".into())
    );
    assert_eq!(
        prop(call("valueType", &[Value::List(vec![])])),
        PropertyValue::String("ANY".into())
    );
    assert_eq!(
        prop(call("valueType", &[pv(PropertyValue::Date(0))])),
        PropertyValue::String("ANY".into())
    );
    assert!(b("isEmpty('')"));
    assert!(!b("isEmpty([1])"));
    assert!(b("isEmpty({})"));
    assert!(err_msg(call("isEmpty", &[pv(1i64)])).contains("isEmpty()"));
}

#[test]
fn scalar_fallbacks_for_aggregate_names() {
    assert_eq!(
        prop(call("percentileCont", &[pv(5i64), pv(0.5f64)])),
        PropertyValue::Integer(5)
    );
    assert!(err_msg(call("percentileCont", &[pv(5i64)])).contains("requires 2 arguments"));
    assert_eq!(
        prop(call("percentileDisc", &[pv(5i64), pv(0.5f64)])),
        PropertyValue::Integer(5)
    );
    assert!(err_msg(call("percentileDisc", &[pv(5i64)])).contains("requires 2 arguments"));
    assert_eq!(prop(call("stDev", &[pv(5i64)])), PropertyValue::Float(0.0));
    assert_eq!(prop(call("stDevP", &[pv(5i64)])), PropertyValue::Float(0.0));
}

#[test]
fn random_uuid_and_timestamp() {
    let PropertyValue::String(u) = prop(call("randomUUID", &[])) else {
        panic!("not a string")
    };
    assert_eq!(u.len(), 36, "{u}");
    assert_eq!(u.as_bytes()[14], b'4');
    let PropertyValue::Integer(t) = prop(call("timestamp", &[])) else {
        panic!("not an integer")
    };
    assert!(t > 1_600_000_000_000);
}

#[test]
fn points_and_distances() {
    let geo = ok("point({latitude: 10, longitude: 20})");
    let m = geo.as_map().unwrap();
    assert_eq!(m.get("srid"), Some(&PropertyValue::Integer(4326)));
    assert_eq!(m.get("crs"), Some(&PropertyValue::String("wgs-84".into())));
    let geo3 = ok("point({latitude: 10, longitude: 20, height: 5})");
    assert_eq!(
        geo3.as_map().unwrap().get("srid"),
        Some(&PropertyValue::Integer(4979))
    );
    let cart = ok("point({x: 1, y: 2})");
    assert_eq!(
        cart.as_map().unwrap().get("crs"),
        Some(&PropertyValue::String("cartesian".into()))
    );
    let cart3 = ok("point({x: 1, y: 2, z: 3})");
    assert_eq!(
        cart3.as_map().unwrap().get("srid"),
        Some(&PropertyValue::Integer(9157))
    );
    assert!(err("point({latitude: 10, longitude: 200})").contains("longitude"));
    assert!(err("point({latitude: 100, longitude: 20})").contains("latitude"));
    assert!(err("point({a: 1})").contains("needs either"));
    assert!(err_msg(call("point", &[pv(1i64)])).contains("takes a map"));

    assert_eq!(
        f("point.distance(point({x: 0, y: 0}), point({x: 3, y: 4}))"),
        5.0
    );
    assert_eq!(f("distance(point({x: 0, y: 0}), {x: 3.0, y: 4})"), 5.0);
    let d = f(
        "point.distance(point({latitude: 0, longitude: 0}), point({latitude: 0, longitude: 1}))",
    );
    assert!((d - 111_195.0).abs() < 100.0, "{d}");
    // Geographic by key names alone, and by srid.
    let d2 = f("point.distance({latitude: 0, longitude: 0}, {x: 1, y: 0, srid: 4326})");
    assert!((d2 - d).abs() < 1e-6);
    assert!(
        err("point.distance(point({x: 0, y: 0}), point({latitude: 0, longitude: 0}))")
            .contains("same coordinate system")
    );
    assert!(err("point.distance({x: 0, y: 0, srid: 7203}, {x: 0})").contains("needs `x` and `y`"));
    assert!(
        err("point.distance({crs: 'wgs-84', x: 1}, {latitude: 0, longitude: 0})")
            .contains("latitude")
    );
    assert!(err("point.distance(1, {x: 0, y: 0})").contains("takes points"));

    assert!(b(
        "point.withinBBox(point({x: 1, y: 1}), point({x: 0, y: 0}), point({x: 2, y: 2}))"
    ));
    assert!(!b(
        "point.withinBBox(point({x: 3, y: 1}), point({x: 0, y: 0}), point({x: 2, y: 2}))"
    ));
    assert!(err(
        "point.withinBBox(point({x: 1, y: 1}), point({latitude: 0, longitude: 0}), point({x: 2, y: 2}))"
    )
    .contains("same coordinate"));
}

#[test]
fn hierarchy_functions_without_an_index_refuse() {
    let (store, a, b2, _) = small_graph();
    let e = err_msg(call_on(
        &store,
        "subsumes",
        &[Value::NodeRef(a), Value::NodeRef(b2)],
    ));
    assert!(e.contains("no hierarchy index is declared"), "{e}");
    let e = err_msg(call_on(
        &store,
        "hierarchy_lca",
        &[Value::NodeRef(a), Value::NodeRef(b2)],
    ));
    assert!(e.contains("hierarchy_lca()"), "{e}");
    let e = err_msg(call_on(
        &store,
        "hierarchy_rollup",
        &[Value::NodeRef(a), pv("sum")],
    ));
    assert!(e.contains("hierarchy_rollup()"), "{e}");
    // Argument checking.
    assert!(
        err_msg(call("subsumes", &[Value::NodeRef(a), Value::NodeRef(b2)]))
            .contains("requires graph context")
    );
    assert!(
        err_msg(call("hierarchy_rollup", &[Value::NodeRef(a), pv("sum")]))
            .contains("requires graph context")
    );
    assert!(err_msg(call(
        "hierarchy_lca",
        &[Value::NodeRef(a), Value::NodeRef(b2)]
    ))
    .contains("requires graph context"));
    assert!(err_msg(call_on(&store, "subsumes", &[Value::NodeRef(a)])).contains("2 or 3 arguments"));
    assert!(
        err_msg(call_on(&store, "hierarchy_rollup", &[Value::NodeRef(a)]))
            .contains("2 or 3 arguments")
    );
    assert!(
        err_msg(call_on(&store, "hierarchy_lca", &[Value::NodeRef(a)]))
            .contains("2 or 3 arguments")
    );
    assert!(
        err_msg(call_on(&store, "subsumes", &[pv(1i64), Value::NodeRef(b2)]))
            .contains("first argument must be a node")
    );
    assert!(
        err_msg(call_on(&store, "subsumes", &[Value::NodeRef(a), pv(1i64)]))
            .contains("second argument must be a node")
    );
    assert!(err_msg(call_on(
        &store,
        "hierarchy_lca",
        &[pv(1i64), Value::NodeRef(b2)]
    ))
    .contains("first argument"));
    assert!(err_msg(call_on(
        &store,
        "hierarchy_lca",
        &[Value::NodeRef(a), pv(1i64)]
    ))
    .contains("second argument"));
    assert!(
        err_msg(call_on(&store, "hierarchy_rollup", &[pv(1i64), pv("sum")]))
            .contains("first argument")
    );
    assert!(err_msg(call_on(
        &store,
        "hierarchy_rollup",
        &[Value::NodeRef(a), pv("median")]
    ))
    .contains("unsupported aggregate"));
    assert!(err_msg(call_on(
        &store,
        "subsumes",
        &[Value::NodeRef(a), Value::NodeRef(b2), pv("nope")]
    ))
    .contains("no usable hierarchy index named 'nope'"));
    assert!(err_msg(call_on(
        &store,
        "hierarchy_rollup",
        &[Value::NodeRef(a), pv("sum"), pv("nope")]
    ))
    .contains("named 'nope'"));
    assert!(err_msg(call_on(
        &store,
        "hierarchy_lca",
        &[Value::NodeRef(a), Value::NodeRef(b2), pv("nope")]
    ))
    .contains("named 'nope'"));
}

fn hierarchy_store() -> GraphStore {
    let mut store = GraphStore::new();
    for (code, units) in [("root", 1), ("left", 2), ("right", 3), ("l1", 4)] {
        run_mut(
            &mut store,
            &format!("CREATE (:Term {{code: '{code}', units: {units}}})"),
        )
        .unwrap();
    }
    run_mut(&mut store, "CREATE (:Loose {code: 'loose'})").unwrap();
    for (child, parent) in [("left", "root"), ("right", "root"), ("l1", "left")] {
        run_mut(
            &mut store,
            &format!("MATCH (x:Term {{code:'{child}'}}), (y:Term {{code:'{parent}'}}) CREATE (x)-[:BROADER]->(y)"),
        )
        .unwrap();
    }
    run_mut(
        &mut store,
        "CREATE HIERARCHY INDEX t ON ()-[:BROADER]->() MEASURE units AGGREGATE sum, count",
    )
    .unwrap();
    store
}

#[test]
fn hierarchy_functions_with_an_index() {
    let store = hierarchy_store();
    let q = |e: &str| {
        one_on(&store, &format!("MATCH (a:Term {{code:'l1'}}), (b:Term {{code:'root'}}), (c:Term {{code:'right'}}), (z:Loose) RETURN {e} AS v"))
    };
    assert_eq!(q("subsumes(a, b)"), PropertyValue::Boolean(true));
    assert_eq!(q("subsumes(b, a)"), PropertyValue::Boolean(false));
    assert_eq!(q("subsumes(a, b, 't')"), PropertyValue::Boolean(true));
    // A node outside every hierarchy: false / null / [] rather than an error.
    assert_eq!(q("subsumes(z, z)"), PropertyValue::Boolean(false));
    assert_eq!(q("hierarchy_rollup(z, 'sum')"), PropertyValue::Null);
    assert_eq!(q("hierarchy_lca(z, z)"), PropertyValue::Array(vec![]));
    assert_eq!(q("hierarchy_rollup(b, 'sum')"), PropertyValue::Integer(10));
    assert_eq!(
        q("hierarchy_rollup(b, 'sum', 't')"),
        PropertyValue::Integer(10)
    );
    let root_id = one_on(&store, "MATCH (b:Term {code:'root'}) RETURN id(b) AS v");
    assert_eq!(
        q("hierarchy_lca(a, c)"),
        PropertyValue::Array(vec![root_id.clone()])
    );
    assert_eq!(
        q("hierarchy_lca(a, c, 't')"),
        PropertyValue::Array(vec![root_id])
    );
}
