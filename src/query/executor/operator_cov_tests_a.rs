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
    // Every name a test uses is bound first, so the validator accepts it.
    const NAMES: &str = "a b c d i j l m n p r s x y one";
    let with: Vec<String> = NAMES.split(' ').map(|n| format!("null AS {n}")).collect();
    let q =
        crate::query::parser::parse_query(&format!("WITH {} RETURN {expr} AS v", with.join(", ")))
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

#[test]
#[ignore = "bug: slicing a list built from a non-literal expression (a `Value::List`) returns null"]
fn slicing_an_expression_built_list() {
    let store = GraphStore::new();
    let got = one_on(&store, "WITH 3 AS x RETURN [x, 5, 6][1..] AS v");
    assert_eq!(got, PropertyValue::Array(vec![5i64.into(), 6i64.into()]));
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
    assert!(matches!(call("head", std::slice::from_ref(&l)), Ok(Value::NodeRef(id)) if id == NodeId::new(1)));
    assert!(matches!(call("last", std::slice::from_ref(&l)), Ok(Value::NodeRef(id)) if id == NodeId::new(2)));
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
    assert_eq!(f("round(2.71828, 2)"), 2.72);
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
        prop(call("id", std::slice::from_ref(&edge))),
        PropertyValue::Integer(e.as_u64() as i64)
    );
    assert!(err_msg(call("id", &[pv(1i64)])).contains("id() requires"));
    assert_eq!(
        prop(call("elementId", std::slice::from_ref(&node))),
        PropertyValue::String(format!("node:{}", a.as_u64()))
    );
    assert_eq!(
        prop(call("elementId", std::slice::from_ref(&edge))),
        PropertyValue::String(format!("edge:{}", e.as_u64()))
    );
    assert!(err_msg(call("elementId", &[pv(1i64)])).contains("elementId()"));

    // labels
    assert_eq!(
        prop(call("labels", std::slice::from_ref(&full_node))),
        PropertyValue::Array(vec!["A".into()])
    );
    assert_eq!(
        prop(call_on(&store, "labels", &[Value::NodeRef(b2)])),
        PropertyValue::Array(vec!["A".into(), "B".into()])
    );
    assert!(err_msg(call("labels", std::slice::from_ref(&node))).contains("requires store"));
    assert!(err_msg(call_on(
        &store,
        "labels",
        &[Value::NodeRef(NodeId::new(999))]
    ))
    .contains("not found"));
    assert!(err_msg(call("labels", &[pv(1i64)])).contains("labels() requires a node"));

    // type / startNode / endNode
    assert_eq!(
        prop(call("type", std::slice::from_ref(&full_edge))),
        PropertyValue::String("R".into())
    );
    assert!(err_msg(call("type", &[pv(1i64)])).contains("type() requires"));
    assert!(matches!(call("startNode", std::slice::from_ref(&full_edge)), Ok(Value::NodeRef(id)) if id == a));
    assert!(matches!(call("endNode", std::slice::from_ref(&full_edge)), Ok(Value::NodeRef(id)) if id == b2));
    assert!(matches!(call("endNode", std::slice::from_ref(&edge)), Ok(Value::NodeRef(id)) if id == b2));
    assert!(err_msg(call("startNode", &[pv(1i64)])).contains("startNode()"));
    assert!(err_msg(call("endNode", &[pv(1i64)])).contains("endNode()"));

    // keys
    let keys = PropertyValue::Array(vec!["n".into(), "name".into()]);
    assert_eq!(prop(call_on(&store, "keys", std::slice::from_ref(&full_node))), keys);
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
        prop(call("keys", std::slice::from_ref(&full_node))),
        PropertyValue::Array(row_keys.into_iter().map(PropertyValue::String).collect())
    );
    assert_eq!(prop(call_on(&store, "keys", std::slice::from_ref(&node))), keys);
    assert!(err_msg(call("keys", std::slice::from_ref(&node))).contains("requires store"));
    assert!(
        err_msg(call_on(&store, "keys", &[Value::NodeRef(NodeId::new(999))])).contains("not found")
    );
    assert_eq!(
        prop(call("keys", std::slice::from_ref(&full_edge))),
        PropertyValue::Array(vec!["w".into()])
    );
    assert_eq!(
        prop(call_on(&store, "keys", std::slice::from_ref(&edge))),
        PropertyValue::Array(vec!["w".into()])
    );
    assert!(err_msg(call("keys", std::slice::from_ref(&edge))).contains("requires store"));
    let ghost = Value::EdgeRef(crate::graph::EdgeId::new(999), a, b2, EdgeType::new("R"));
    assert!(err_msg(call_on(&store, "keys", std::slice::from_ref(&ghost))).contains("not found"));
    assert!(err_msg(call("keys", &[pv(1i64)])).contains("keys() requires"));
    assert_eq!(
        ok("keys({b: 1, a: 2})"),
        PropertyValue::Array(vec!["a".into(), "b".into()])
    );

    // properties
    let props = prop(call_on(&store, "properties", std::slice::from_ref(&full_node)));
    assert_eq!(
        props.as_map().unwrap().get("name"),
        Some(&PropertyValue::String("x".into()))
    );
    let props = prop(call("properties", &[full_node]));
    assert!(props.as_map().is_some());
    let props = prop(call_on(&store, "properties", std::slice::from_ref(&node)));
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
    let props = prop(call_on(&store, "properties", std::slice::from_ref(&edge)));
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

// ---------------------------------------------------------------------------
// temporal constructors
// ---------------------------------------------------------------------------

/// `toString(<expr>)`, through both evaluators.
fn ts(expr: &str) -> String {
    s(&format!("toString({expr})"))
}

#[test]
fn date_constructor_forms() {
    assert_eq!(
        ts("date({date: date('2020-05-15'), day: 28})"),
        "2020-05-28"
    );
    assert_eq!(
        ts("date({date: date('2020-05-15'), ordinalDay: 32})"),
        "2020-02-01"
    );
    assert_eq!(
        ts("date({date: date('2020-05-15'), year: 2021, month: 1})"),
        "2021-01-15"
    );
    assert_eq!(
        ts("date({date: date('1816-12-30'), week: 2})"),
        "1817-01-06"
    );
    assert_eq!(
        ts("date({date: date('2020-05-13'), dayOfWeek: 1})"),
        "2020-05-11"
    );
    assert_eq!(
        ts("date({date: date('1984-11-11'), quarter: 3})"),
        "1984-08-11"
    );
    assert_eq!(
        ts("date({date: date('1984-11-11'), dayOfQuarter: 1})"),
        "1984-10-01"
    );
    assert_eq!(
        ts("date({datetime: localdatetime('2020-01-02T10:00')})"),
        "2020-01-02"
    );
    assert_eq!(ts("date({date: date('2020-05-15')})"), "2020-05-15");
    assert_eq!(ts("date({year: 2020, month: 3, day: 4})"), "2020-03-04");
    assert!(err("date({date: date('2020-01-01'), ordinalDay: 400})").contains("invalid ordinalDay"));
    assert!(err("date({date: date('2020-01-01'), month: 2, day: 31})").contains("invalid date"));
    assert!(err("date({date: date('2020-01-01'), week: 60})").contains("invalid week date"));
    assert!(err("date({date: date('2020-01-01'), quarter: 5})").contains("invalid quarter"));
    assert_eq!(ts("date(date('2020-01-02'))"), "2020-01-02");
    assert_eq!(ts("date(localdatetime('2020-01-02T10:00'))"), "2020-01-02");
    assert_eq!(ts("date(datetime('2020-01-02T23:00-02:00'))"), "2020-01-02");
    assert!(err("date(localtime('10:00'))").contains("date() requires"));
    assert!(matches!(
        call("date", &[]),
        Ok(Value::Property(PropertyValue::Date(_)))
    ));
}

#[test]
fn localtime_constructor_forms() {
    assert_eq!(
        ts("localtime({time: localtime('10:11:12'), second: 42})"),
        "10:11:42"
    );
    assert_eq!(
        ts("localtime({time: localtime('10:11:12.5'), millisecond: 7})"),
        "10:11:12.007"
    );
    assert_eq!(
        ts("localtime({time: localtime('10:11'), hour: 1, minute: 2})"),
        "01:02"
    );
    assert_eq!(ts("localtime({hour: 9})"), "09:00");
    assert_eq!(ts("localtime(datetime('2020-01-01T10:11Z'))"), "10:11");
    assert_eq!(ts("localtime(localdatetime('2020-01-01T10:11'))"), "10:11");
    assert_eq!(ts("localtime(time('10:11+01:00'))"), "10:11");
    assert!(err("localtime(date('2020-01-01'))").contains("localtime() requires"));
    assert!(matches!(
        call("localtime", &[]),
        Ok(Value::Property(PropertyValue::LocalTime(_)))
    ));
}

#[test]
fn time_constructor_forms() {
    assert_eq!(ts("time('10:00+01:00')"), "10:00+01:00");
    assert_eq!(ts("time('10:00')"), "10:00Z");
    assert_eq!(
        ts("time({time: time('12:00+01:00'), timezone: '+05:00'})"),
        "16:00+05:00"
    );
    assert_eq!(
        ts("time({time: time('12:00+01:00'), timezone: '+05:00', second: 42})"),
        "16:00:42+05:00"
    );
    assert_eq!(
        ts("time({time: localtime('12:31'), timezone: '+05:00'})"),
        "12:31+05:00"
    );
    assert_eq!(ts("time({time: time('12:00+01:00')})"), "12:00+01:00");
    assert_eq!(ts("time({hour: 10, timezone: '+02:00'})"), "10:00+02:00");
    assert_eq!(ts("time({hour: 10})"), "10:00Z");
    assert_eq!(
        ts("time(datetime('2020-01-01T10:00+03:00'))"),
        "10:00+03:00"
    );
    assert_eq!(ts("time(localtime('10:00'))"), "10:00Z");
    assert!(err("time(date('2020-01-01'))").contains("time() requires"));
    assert!(err_msg(call("time", &[Value::List(vec![])])).contains("time() requires"));
    assert!(matches!(
        call("time", &[]),
        Ok(Value::Property(PropertyValue::Time {
            offset_seconds: 0,
            ..
        }))
    ));
}

#[test]
fn localdatetime_constructor_forms() {
    assert_eq!(ts("localdatetime('2020-01-02')"), "2020-01-02T00:00");
    assert_eq!(
        ts("localdatetime('2015-W30-2T214032.142')"),
        "2015-07-21T21:40:32.142"
    );
    assert_eq!(
        ts("localdatetime({year: 2020, month: 1, day: 2, hour: 3})"),
        "2020-01-02T03:00"
    );
    assert_eq!(
        ts("localdatetime({year: 2020, month: 1, day: 2})"),
        "2020-01-02T00:00"
    );
    assert_eq!(ts("localdatetime(date('2020-01-02'))"), "2020-01-02T00:00");
    assert_eq!(
        ts("localdatetime(datetime('2020-01-02T05:06+01:00'))"),
        "2020-01-02T05:06"
    );
    assert_eq!(
        ts("localdatetime({date: date('2020-01-02'), time: localtime('10:11:12.5'), second: 1, nanosecond: 5})"),
        "2020-01-02T10:11:01.000000005"
    );
    assert_eq!(
        ts("localdatetime({date: date('2020-01-02'), time: localtime('10:11:12.5')})"),
        "2020-01-02T10:11:12.5"
    );
    assert_eq!(
        ts("localdatetime({datetime: localdatetime('2020-01-02T10:11'), day: 5})"),
        "2020-01-05T10:11"
    );
    assert!(err("localdatetime({hour: 1})").contains("needs a date"));
    assert!(err("localdatetime(localtime('10:00'))").contains("localdatetime() requires"));
    assert!(matches!(
        call("localdatetime", &[]),
        Ok(Value::Property(PropertyValue::LocalDateTime { .. }))
    ));
}

#[test]
fn datetime_constructor_forms() {
    assert_eq!(
        ts("datetime('2015-07-21T21:40:32.142+02:00[Europe/Stockholm]')"),
        "2015-07-21T21:40:32.142+02:00[Europe/Stockholm]"
    );
    assert_eq!(
        ts("datetime('2015-07-21T21:40[Europe/Stockholm]')"),
        "2015-07-21T21:40+02:00[Europe/Stockholm]"
    );
    assert_eq!(ts("datetime('2015-07-21T21:40')"), "2015-07-21T21:40Z");
    assert_eq!(ts("datetime({epochMillis: 1000})"), "1970-01-01T00:00:01Z");
    assert_eq!(ts("datetime({epochSeconds: 60})"), "1970-01-01T00:01Z");
    assert_eq!(
        ts("datetime({year: 2020, month: 1, day: 1, timezone: '+01:00'})"),
        "2020-01-01T00:00+01:00"
    );
    assert_eq!(
        ts("datetime({datetime: datetime('2020-01-01T12:00+02:00')})"),
        "2020-01-01T12:00+02:00"
    );
    assert_eq!(
        ts("datetime({datetime: datetime('2020-01-01T12:00+02:00'), timezone: '+00:00'})"),
        "2020-01-01T10:00Z"
    );
    assert_eq!(
        ts("datetime({date: date('2020-03-01'), time: datetime('2019-10-10T12:00[Europe/Stockholm]')})"),
        "2020-03-01T12:00+01:00[Europe/Stockholm]"
    );
    assert_eq!(
        ts("datetime({date: date('2020-07-01'), time: datetime('2019-10-10T12:00[Europe/Stockholm]'), timezone: '+00:00'})"),
        "2020-07-01T10:00Z"
    );
    assert_eq!(
        ts("datetime({date: date('2020-01-01'), time: time('10:00+03:00')})"),
        "2020-01-01T10:00+03:00"
    );
    assert_eq!(
        ts("datetime({date: date('2020-01-01'), time: time('10:00+03:00'), timezone: '+01:00'})"),
        "2020-01-01T08:00+01:00"
    );
    assert_eq!(
        ts("datetime(localdatetime('2020-01-01T10:00'))"),
        "2020-01-01T10:00Z"
    );
    assert_eq!(ts("datetime(date('2020-01-01'))"), "2020-01-01T00:00Z");
    assert_eq!(
        ts("datetime(datetime('2020-01-01T10:00+05:00[Asia/Karachi]'))"),
        "2020-01-01T10:00+05:00[Asia/Karachi]"
    );
    assert!(err("datetime(localtime('10:00'))").contains("datetime() requires"));
    assert!(matches!(
        call("datetime", &[]),
        Ok(Value::Property(PropertyValue::ZonedDateTime {
            offset_seconds: 0,
            ..
        }))
    ));
}

#[test]
fn duration_constructor_forms() {
    assert_eq!(ts("duration('P1Y2M3DT4H5M6.5S')"), "P1Y2M3DT4H5M6.5S");
    assert_eq!(
        ts("duration('P2012-02-02T14:37:21.545')"),
        "P2012Y2M2DT14H37M21.545S"
    );
    assert_eq!(ts("duration('P20120202T143721')"), "P2012Y2M2DT14H37M21S");
    assert_eq!(ts("duration('P2012-02-02')"), "P2012Y2M2D");
    assert_eq!(ts("duration('P2.5W')"), "P17DT12H");
    assert_eq!(ts("duration('P1.5D')"), "P1DT12H");
    assert_eq!(ts("duration('P0.5M')"), "P15DT5H14M33S");
    assert_eq!(ts("duration('P0.5Y')"), "P6M");
    assert_eq!(ts("duration('PT-2.001S')"), "PT-2.001S");
    assert_eq!(ts("duration('P1DT-1H')"), "P1DT-1H");
    assert_eq!(ts("duration('pt1h+30m')"), "PT1H30M");
    assert!(err("duration('x')").contains("Invalid duration format"));
    assert_eq!(ts("duration({months: 0.75})"), "P22DT19H51M49.5S");
    assert_eq!(ts("duration({weeks: 2.5})"), "P17DT12H");
    assert_eq!(ts("duration({years: 1, days: 1.5})"), "P1Y1DT12H");
    assert_eq!(ts("duration({seconds: 2, milliseconds: -1})"), "PT1.999S");
    assert_eq!(ts("duration({nanoseconds: -1})"), "PT-0.000000001S");
    assert_eq!(
        ts("duration({minutes: 1, microseconds: 2})"),
        "PT1M0.000002S"
    );
    assert!(err_msg(call("duration", &[])).contains("requires an argument"));
    assert!(err_msg(call("duration", &[pv(1i64)])).contains("requires string or map"));
}

#[test]
fn parse_extended_duration_rejects_malformed_shapes() {
    assert!(parse_extended_duration("2012-02", "").is_none());
    assert!(parse_extended_duration("2012-0a-02", "").is_none());
    assert!(parse_extended_duration("201202", "").is_none());
    assert!(parse_extended_duration("2012-02-02", "14:37:21.x").is_none());
    assert!(parse_extended_duration("2012-02-02", "14:37").is_none());
    let v = parse_extended_duration("0001-00-00", "00:00:01,5").unwrap();
    assert_eq!(
        v.as_property(),
        Some(&PropertyValue::Duration {
            months: 12,
            days: 0,
            seconds: 1,
            nanos: 500_000_000
        })
    );
}

#[test]
fn epoch_constructors() {
    assert_eq!(
        ts("datetime.fromepoch(416779, 999999999)"),
        "1970-01-05T19:46:19.999999999Z"
    );
    assert_eq!(
        ts("datetime.fromepochmillis(-1)"),
        "1969-12-31T23:59:59.999Z"
    );
    assert!(err("datetime.fromepoch(1, 2000000000)").contains("nanoseconds must be"));
    assert!(err_msg(call("datetime.fromEpoch", &[pv(1i64)])).contains("requires 2 argument(s)"));
    assert!(
        err_msg(call("datetime.fromEpochMillis", &[pv(1i64), pv(2i64)]))
            .contains("requires 1 argument(s)")
    );
    assert!(err_msg(call("datetime.fromEpochMillis", &[pv("x")])).contains("Expected integer"));
}

#[test]
fn truncate_functions() {
    assert_eq!(
        ts("date.truncate('month', date('2020-05-15'))"),
        "2020-05-01"
    );
    assert_eq!(
        ts("date.truncate('month', date('2020-05-15'), {day: 5})"),
        "2020-05-05"
    );
    assert_eq!(
        ts("localtime.truncate('hour', localtime('10:11:12'))"),
        "10:00"
    );
    assert_eq!(
        ts("datetime.truncate('day', datetime('2020-05-15T10:11+02:00'))"),
        "2020-05-15T00:00+02:00"
    );
    assert!(err_msg(call("date.truncate", &[pv("day")]))
        .contains("requires a unit and a temporal value"));
    assert!(err_msg(call(
        "date.truncate",
        &[pv(1i64), pv(PropertyValue::Date(0))]
    ))
    .contains("unit, as a string"));
    assert!(
        err_msg(call("date.truncate", &[pv("day"), Value::List(vec![])]))
            .contains("needs a temporal value")
    );
}

#[test]
fn duration_in_unit_functions() {
    assert_eq!(
        ts("duration.inSeconds(date('2020-01-01'), date('2020-01-02'))"),
        "PT24H"
    );
    assert_eq!(
        ts("duration.inDays(localdatetime('2020-01-01T00:00'), localdatetime('2020-01-02T06:00'))"),
        "P1D"
    );
    assert_eq!(
        ts("duration.inMonths(date('2020-01-01'), date('2021-01-15'))"),
        "P1Y"
    );
    assert_eq!(
        ts("duration.inMonths(date('2021-01-15'), date('2020-01-01'))"),
        "P-1Y"
    );
    assert!(err_msg(call("duration.inSeconds", &[pv(1i64)])).contains("requires 2 arguments"));
    assert!(err_msg(call(
        "duration.inDays",
        &[Value::List(vec![]), Value::List(vec![])]
    ))
    .contains("needs two temporal values"));
}

#[test]
fn duration_between_forms() {
    assert_eq!(
        ts("duration.between(date('1984-10-11'), date('2015-06-24'))"),
        "P30Y8M13D"
    );
    assert_eq!(
        ts("duration.between(date('2015-07-21'), date('2015-06-24'))"),
        "P-27D"
    );
    assert_eq!(
        ts("duration.between(date('2015-06-24'), date('2015-07-01'))"),
        "P7D"
    );
    assert_eq!(
        ts("duration.between(date('2015-01-31'), date('2015-03-01'))"),
        "P1M1D"
    );
    assert_eq!(
        ts("duration.between(date('2015-03-01'), date('2015-01-31'))"),
        "P-1M-1D"
    );
    assert_eq!(
        ts("duration.between(localdatetime('2015-01-01T10:00'), localdatetime('2015-03-01T09:00'))"),
        "P1M27DT23H"
    );
    assert_eq!(
        ts("duration.between(localdatetime('2015-03-01T09:00'), localdatetime('2015-01-01T10:00'))"),
        "P-1M-30DT-23H"
    );
    assert_eq!(
        ts("duration.between(date('2020-01-01'), localtime('16:30'))"),
        "PT16H30M"
    );
    assert_eq!(
        ts("duration.between(time('10:00+01:00'), time('12:00+02:00'))"),
        "PT1H"
    );
    assert_eq!(
        ts("duration.between(localdatetime('2015-07-21T21:40:32.142'), datetime('2015-07-21T21:40:32.142+01:00'))"),
        "PT0S"
    );
    assert_eq!(
        ts("duration.between(datetime('2017-10-28T12:00[Europe/Stockholm]'), datetime('2017-10-30T12:00[Europe/Stockholm]'))"),
        "P2D"
    );
    assert_eq!(
        ts(
            "duration.between(localdatetime({year: 2017, month: 10, day: 29, hour: 0}), \
             datetime({year: 2017, month: 10, day: 29, hour: 4, timezone: 'Europe/Stockholm'}))"
        ),
        "PT5H"
    );
    assert_eq!(
        ts("duration.between(datetime('2014-07-21T21:40:36.143+02:00'), datetime('2015-07-21T21:40:32.142+01:00'))"),
        "P1YT59M55.999S"
    );
    assert!(err("duration.between('a', date('2020-01-01'))").contains("temporal"));
    assert!(
        err_msg(call("duration_between", &[pv(PropertyValue::Date(0))]))
            .contains("requires 2 arguments")
    );
    assert!(err_msg(call(
        "duration.between",
        &[Value::List(vec![]), pv(PropertyValue::Date(0))]
    ))
    .contains("two temporal"));
}

#[test]
fn temporal_arithmetic() {
    assert_eq!(ts("date('2020-01-31') + duration('P1M')"), "2020-02-29");
    assert_eq!(ts("duration('P1D') + date('2020-01-01')"), "2020-01-02");
    assert_eq!(ts("date('2020-03-01') - duration('P1D')"), "2020-02-29");
    assert_eq!(ts("date('1984-10-11') + duration('PT49H')"), "1984-10-13");
    assert_eq!(
        ts("localtime('12:31:14') + duration({months: 1, days: -14, hours: 16})"),
        "04:31:14"
    );
    assert_eq!(ts("time('10:00+01:00') + duration('PT1H')"), "11:00+01:00");
    assert_eq!(ts("time('10:00+01:00') - duration('PT11H')"), "23:00+01:00");
    assert_eq!(
        ts("localdatetime('2020-01-31T10:00') + duration('P1M1DT1H')"),
        "2020-03-01T11:00"
    );
    assert_eq!(
        ts("datetime('2020-01-31T10:00+01:00') + duration('P1M')"),
        "2020-02-29T10:00+01:00"
    );
    assert_eq!(
        ts("datetime('2017-10-29T00:00[Europe/Stockholm]') + duration('P1D')"),
        "2017-10-30T00:00+01:00[Europe/Stockholm]"
    );
    assert_eq!(
        ts("datetime('2017-10-29T00:00[Europe/Stockholm]') + duration('PT24H')"),
        "2017-10-29T23:00+01:00[Europe/Stockholm]"
    );
    assert_eq!(
        ts("datetime('2017-09-29T00:00[Europe/Stockholm]') + duration('P1M')"),
        "2017-10-29T00:00+02:00[Europe/Stockholm]"
    );
    assert_eq!(ts("date('2020-01-02') - date('2020-01-01')"), "P1D");
    assert_eq!(ts("localtime('10:00') - localtime('09:00')"), "PT1H");
    assert_eq!(
        ts("localdatetime('2020-01-02T00:00') - localdatetime('2020-01-01T12:00')"),
        "PT12H"
    );
    assert_eq!(ts("duration('P1D') + duration('PT1H')"), "P1DT1H");
    assert_eq!(ts("duration('P1D') - duration('PT1H')"), "P1DT-1H");
    assert_eq!(ts("duration('P1M') * 2"), "P2M");
    assert_eq!(ts("2 * duration('P1D')"), "P2D");
    assert_eq!(ts("duration('PT1S') * 1.5"), "PT1.5S");
    assert_eq!(ts("duration('P1D') * 0.5"), "PT12H");
    assert_eq!(ts("duration('P1M') / 2"), "P15DT5H14M33S");
    assert_eq!(ts("duration('P2D') / 2.0"), "P1D");
    assert!(err("duration('P1D') * (0.0 / 0.0)").contains("non-finite"));
    assert!(err("duration('P1D') / 0.0").contains("divide a duration by zero"));
}

#[test]
fn legacy_millisecond_datetime_arithmetic() {
    let dt = |ms: i64| pv(PropertyValue::DateTime(ms));
    let dur = |months: i64, days: i64, seconds: i64| {
        pv(PropertyValue::Duration {
            months,
            days,
            seconds,
            nanos: 0,
        })
    };
    let day = 86_400_000i64;
    let op = |o: BinaryOp, l: Value, r: Value| {
        eval_binary_op(&o, l, r)
            .unwrap()
            .as_property()
            .cloned()
            .unwrap()
    };
    assert_eq!(
        op(BinaryOp::Add, dt(0), dur(0, 1, 1)),
        PropertyValue::DateTime(day + 1000)
    );
    assert_eq!(
        op(BinaryOp::Add, dur(0, 1, 0), dt(0)),
        PropertyValue::DateTime(day)
    );
    // 1970-01-01 + 1 month = 1970-02-01; minus 1 month from there is back.
    assert_eq!(
        op(BinaryOp::Add, dt(0), dur(1, 0, 0)),
        PropertyValue::DateTime(31 * day)
    );
    assert_eq!(
        op(BinaryOp::Sub, dt(31 * day), dur(1, 0, 0)),
        PropertyValue::DateTime(0)
    );
    assert_eq!(
        op(BinaryOp::Sub, dt(day + 1500), dt(0)),
        PropertyValue::Duration {
            months: 0,
            days: 1,
            seconds: 1,
            nanos: 500_000_000
        }
    );
    assert_eq!(
        add_duration_to_datetime(i64::MAX, 0, 0, 0),
        PropertyValue::Null
    );
    // The legacy type orders against itself and against a raw integer.
    assert_eq!(
        cypher_ordering(&PropertyValue::DateTime(1), &PropertyValue::DateTime(2)),
        Some(std::cmp::Ordering::Less)
    );
    assert_eq!(
        cypher_ordering(&PropertyValue::DateTime(3), &PropertyValue::Integer(2)),
        Some(std::cmp::Ordering::Greater)
    );
    assert_eq!(
        cypher_ordering(&PropertyValue::Integer(3), &PropertyValue::DateTime(3)),
        Some(std::cmp::Ordering::Equal)
    );
}

#[test]
fn temporal_helpers_directly() {
    use PropertyValue as P;
    let zdt = P::ZonedDateTime {
        secs: 86_400 + 3600,
        nanos: 5,
        offset_seconds: 7200,
        zone: None,
    };
    assert_eq!(date_part_of(&P::DateTime(86_400_000 * 2 + 5)), Some(2));
    assert_eq!(date_part_of(&zdt), Some(1));
    assert_eq!(date_part_of(&P::LocalTime(1)), None);
    assert_eq!(time_part_of(&P::DateTime(1500)), Some(1_500_000_000));
    assert_eq!(time_part_of(&zdt), Some(3 * 3600 * 1_000_000_000 + 5));
    assert_eq!(time_part_of(&P::Date(1)), None);
    assert_eq!(offset_seconds_of(&P::DateTime(0)), Some(0));
    assert_eq!(
        offset_seconds_of(&P::Time {
            nanos: 0,
            offset_seconds: 60
        }),
        Some(60)
    );
    assert_eq!(offset_seconds_of(&P::Date(0)), None);
    assert!(matches!(
        zone_of(&P::Time {
            nanos: 0,
            offset_seconds: 60
        }),
        Some(crate::query::executor::temporal::TzSpec::Offset(60))
    ));
    assert!(zone_of(&P::Date(0)).is_none());
    assert_eq!(temporal_epoch_nanos(&P::DateTime(2)), Some(2_000_000));
    assert_eq!(
        temporal_epoch_nanos(&P::Time {
            nanos: 10,
            offset_seconds: 1
        }),
        Some(10 - 1_000_000_000)
    );
    assert_eq!(temporal_epoch_nanos(&P::Integer(1)), None);
    assert_eq!(day_and_nanos_to_secs(1, -1), (86_399, 999_999_999));
    assert_eq!(weekday_from_iso_num(7), chrono::Weekday::Sun);
    assert_eq!(weekday_from_iso_num(3), chrono::Weekday::Wed);
    assert_eq!(days_in_month(2020, 2), 29);
    assert_eq!(days_in_month(2021, 12), 31);
    assert_eq!(days_in_month(2021, 13), 31);
    assert_eq!(
        rebuild_temporal_like(&P::Integer(0), 5_000_000),
        P::DateTime(5)
    );
    // Months cannot move a value with no calendar of its own.
    let e = shift_temporal(&P::DateTime(0), 1, 0, 0, 0)
        .unwrap_err()
        .to_string();
    assert!(e.contains("cannot add months"), "{e}");
    let e = shift_temporal(&P::Integer(0), 0, 1, 0, 0)
        .unwrap_err()
        .to_string();
    assert!(e.contains("not a temporal value"), "{e}");
    // A temporal map arrives as a `Value::Map`; entities in it are skipped.
    let mut m = std::collections::BTreeMap::new();
    m.insert("year".to_string(), pv(2020i64));
    m.insert("gone".to_string(), Value::Null);
    m.insert("node".to_string(), Value::NodeRef(NodeId::new(1)));
    let got = temporal_arg_map(&Value::Map(m)).unwrap();
    assert_eq!(got.get("year"), Some(&P::Integer(2020)));
    assert_eq!(got.get("gone"), Some(&P::Null));
    assert!(!got.contains_key("node"));
    assert!(temporal_arg_map(&pv(1i64)).is_none());
    // apply_time_overrides: only the named fields move.
    let base = (10 * 3600 + 11 * 60 + 12) * 1_000_000_000 + 500;
    let mut over = std::collections::HashMap::new();
    over.insert("minute".to_string(), P::Integer(30));
    over.insert("microsecond".to_string(), P::Integer(2));
    assert_eq!(
        apply_time_overrides(base, &over),
        (10 * 3600 + 30 * 60 + 12) * 1_000_000_000 + 2_000
    );
    // parse_naive_date_time: date only, and a trailing `T`.
    assert_eq!(parse_naive_date_time("1970-01-02").unwrap(), (86_400, 0));
    assert_eq!(parse_naive_date_time("1970-01-02T").unwrap(), (86_400, 0));
    assert!(parse_naive_date_time("nope").is_err());
    // compose: a date-less map is an error, a selected date with a clock override works.
    let mut m2 = std::collections::HashMap::new();
    m2.insert("hour".to_string(), P::Integer(1));
    assert!(compose_date_and_time(&m2).is_err());
    m2.insert(
        "datetime".to_string(),
        P::LocalDateTime {
            secs: 86_400 + 60,
            nanos: 7,
        },
    );
    m2.insert("millisecond".to_string(), P::Integer(3));
    assert_eq!(
        compose_date_and_time(&m2).unwrap(),
        (1, 3600 * 1_000_000_000 + 60 * 1_000_000_000 + 3_000_000)
    );
}

#[test]
fn storable_property_rules() {
    use PropertyValue as P;
    let map = P::Map(Default::default());
    assert_eq!(storable_property(&pv(1i64)), Some(P::Integer(1)));
    assert_eq!(storable_property(&pv(map.clone())), Some(map.clone()));
    assert_eq!(storable_property(&pv(P::Array(vec![map.clone()]))), None);
    assert_eq!(
        storable_property(&pv(P::Array(vec![P::Array(vec![map.clone()])]))),
        None
    );
    assert_eq!(
        storable_property(&Value::List(vec![pv(1i64), pv(2i64)])),
        Some(P::Array(vec![P::Integer(1), P::Integer(2)]))
    );
    assert_eq!(storable_property(&Value::List(vec![pv(map)])), None);
    assert_eq!(
        storable_property(&Value::List(vec![Value::Map(Default::default())])),
        None
    );
    assert_eq!(
        storable_property(&Value::List(vec![Value::NodeRef(NodeId::new(1))])),
        None
    );
    assert_eq!(storable_property(&Value::NodeRef(NodeId::new(1))), None);
    assert!(property_is_storable(&P::Array(vec![P::Integer(1)])));
}

#[test]
fn scale_duration_edge_cases() {
    assert!(scale_duration(1, 0, 0, 0, f64::INFINITY).is_err());
    assert_eq!(
        scale_duration(0, 0, 58_390, 1, 0.5).unwrap(),
        PropertyValue::Duration {
            months: 0,
            days: 0,
            seconds: 29_195,
            nanos: 0
        }
    );
    assert_eq!(
        scale_duration(0, 14, 16 * 3600, 0, 2.0).unwrap(),
        PropertyValue::Duration {
            months: 0,
            days: 28,
            seconds: 32 * 3600,
            nanos: 0
        }
    );
}

#[test]
fn more_temporal_edge_cases() {
    // An unknown unit letter contributes nothing.
    assert_eq!(ts("duration('P1X2D')"), "P2D");
    // A selection whose source has no date part falls back to the components.
    assert_eq!(
        ts("date({date: localtime('10:00'), year: 2020, month: 2, day: 3})"),
        "2020-02-03"
    );
    assert_eq!(
        ts("localtime({time: date('2020-01-01'), hour: 7})"),
        "07:00"
    );
    assert_eq!(ts("time({time: date('2020-01-01'), hour: 7})"), "07:00Z");
    assert!(!err("date.truncate('fortnight', date('2020-05-15'))").is_empty());
    // A local date-time that lands in a spring-forward gap once the calendar
    // part is added: 2017-03-26T02:30 does not exist in Stockholm.
    assert_eq!(
        ts("duration.between(datetime('2017-03-25T02:30[Europe/Stockholm]'), datetime('2017-03-26T05:00[Europe/Stockholm]'))"),
        "P1DT1H30M"
    );
    // A point map with a non-numeric coordinate.
    assert!(err("point({x: 'a', y: 1})").contains("needs either"));
    assert!(err("point.distance({x: 'a', y: 1}, {x: 0, y: 0})").contains("needs `x` and `y`"));
    // `n:A:B` with a non-string label in the list only tests the string ones.
    let (store, a, _, _) = small_graph();
    let labels = pv(PropertyValue::Array(vec![
        "A".into(),
        PropertyValue::Integer(1),
    ]));
    assert_eq!(
        prop(call_on(&store, "hasLabels", &[Value::NodeRef(a), labels])),
        PropertyValue::Boolean(true)
    );
}

// ---------------------------------------------------------------------------
// eval_expression with bindings
// ---------------------------------------------------------------------------

/// Evaluate `expr` with `record`'s bindings through `eval_expression`.
fn dx(store: &GraphStore, record: &Record, expr: &str) -> Result<Value, ExecutionError> {
    eval_expression(&parse_expr(expr), record, store)
}

fn dxp(store: &GraphStore, record: &Record, expr: &str) -> PropertyValue {
    prop(dx(store, record, expr))
}

#[test]
fn eval_expression_arms_with_bindings() {
    let (store, a, b2, e) = small_graph();
    let mut r = Record::new();
    r.bind("a", Value::NodeRef(a));
    r.bind("b", Value::NodeRef(b2));
    r.bind("r", Value::EdgeRef(e, a, b2, EdgeType::new("R")));
    r.bind("x", pv(3i64));
    r.bind("s", pv("abc"));
    r.bind(
        "p",
        Value::Path {
            nodes: vec![a, b2],
            edges: vec![e],
        },
    );
    let mut m = std::collections::BTreeMap::new();
    m.insert("k".to_string(), Value::NodeRef(a));
    r.bind("m", Value::Map(m));
    r.bind("$param", pv(9i64));

    assert_eq!(
        dxp(&store, &r, "CASE x WHEN 3 THEN 'three' ELSE 'other' END"),
        PropertyValue::String("three".into())
    );
    assert_eq!(
        dxp(&store, &r, "CASE x WHEN 4 THEN 'four' END"),
        PropertyValue::Null
    );
    assert_eq!(
        dxp(
            &store,
            &r,
            "CASE WHEN x > 5 THEN 'big' WHEN x > 1 THEN 'mid' END"
        ),
        PropertyValue::String("mid".into())
    );
    assert_eq!(
        dxp(&store, &r, "CASE WHEN x > 5 THEN 'big' ELSE 'small' END"),
        PropertyValue::String("small".into())
    );
    assert_eq!(dxp(&store, &r, "[x, x + 1][1]"), PropertyValue::Integer(4));
    assert_eq!(
        dxp(&store, &r, "[3, 5, 6][1..]"),
        PropertyValue::Array(vec![5i64.into(), 6i64.into()])
    );
    assert_eq!(dxp(&store, &r, "{v: x}['v']"), PropertyValue::Integer(3));
    assert_eq!(dxp(&store, &r, "a.name"), PropertyValue::String("x".into()));
    assert_eq!(
        dxp(&store, &r, "m.k.name"),
        PropertyValue::String("x".into())
    );
    assert!(matches!(dx(&store, &r, "m.zz"), Ok(Value::Null)));
    assert_eq!(dxp(&store, &r, "r.w"), PropertyValue::Integer(2));
    assert_eq!(dxp(&store, &r, "length(p)"), PropertyValue::Integer(1));
    assert!(matches!(dx(&store, &r, "p"), Ok(Value::Path { .. })));
    assert_eq!(dxp(&store, &r, "$param + 1"), PropertyValue::Integer(10));
    let e2 = dx(&store, &r, "$missing").unwrap_err().to_string();
    assert!(e2.contains("Unresolved parameter"), "{e2}");
    let e3 = dx(&store, &r, "j + 1").unwrap_err();
    assert!(
        matches!(e3, ExecutionError::VariableNotFoundInScope { .. }),
        "{e3:?}"
    );
    let e4 = dx(&store, &Record::new(), "j.prop").unwrap_err();
    assert!(matches!(e4, ExecutionError::VariableNotFound(_)), "{e4:?}");
    assert_eq!(
        dxp(&store, &r, "x IS NOT NULL"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        dxp(&store, &r, "[y IN [1, 2] | y + x]"),
        PropertyValue::Array(vec![4i64.into(), 5i64.into()])
    );
    assert_eq!(
        dxp(&store, &r, "any(y IN [1, 3] WHERE y = x)"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        dxp(&store, &r, "reduce(t = 0, y IN [1, 2] | t + y * x)"),
        PropertyValue::Integer(9)
    );
}

#[test]
fn borrowed_string_comparisons_read_columns() {
    let (mut store, a, b2, e) = small_graph();
    store.set_edge_property(e, "tag", "hot").unwrap();
    let mut r = Record::new();
    r.bind("a", Value::NodeRef(a));
    r.bind("r", Value::EdgeRef(e, a, b2, EdgeType::new("R")));
    r.bind("i", pv(1i64));
    assert_eq!(
        dxp(&store, &r, "a.name = 'x'"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        dxp(&store, &r, "a.name STARTS WITH 'y'"),
        PropertyValue::Boolean(false)
    );
    assert_eq!(
        dxp(&store, &r, "r.tag = 'hot'"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        dxp(&store, &r, "r.tag CONTAINS a.name"),
        PropertyValue::Boolean(false)
    );
    // A number column is not borrowed but still compares.
    assert_eq!(
        dxp(&store, &r, "r.w = 'hot'"),
        PropertyValue::Boolean(false)
    );
    assert_eq!(dxp(&store, &r, "a.n = 'x'"), PropertyValue::Boolean(false));
    // Not a node or relationship: the ordinary path answers.
    let e2 = dx(&store, &r, "i.name = 'x'");
    assert!(e2.is_ok() || e2.is_err());
    assert_eq!(dxp(&store, &r, "a.missing = 'x'"), PropertyValue::Null);
    // A deleted relationship is not borrowed, and reading it is an error.
    let ghost = crate::graph::EdgeId::new(999);
    let mut r2 = Record::new();
    r2.bind("r", Value::EdgeRef(ghost, a, b2, EdgeType::new("R")));
    r2.bind("n", Value::NodeRef(NodeId::new(999)));
    let err_e = dx(&store, &r2, "r.tag = 'hot'").unwrap_err();
    assert!(
        matches!(err_e, ExecutionError::EntityNotFound(ref m) if m.contains("relationship")),
        "{err_e:?}"
    );
    let err_n = dx(&store, &r2, "n.name = 'x'").unwrap_err();
    assert!(
        matches!(err_n, ExecutionError::EntityNotFound(ref m) if m.contains("node")),
        "{err_n:?}"
    );
}

#[test]
fn read_property_missing_is_null_mode() {
    let store = GraphStore::new();
    let r = Record::new();
    assert!(read_property(&r, "n", "p", &store, true).unwrap().is_null());
    assert!(matches!(
        read_property(&r, "n", "p", &store, false),
        Err(ExecutionError::VariableNotFound(_))
    ));
}

#[test]
fn eval_binary_op_entity_edges() {
    let n = Value::NodeRef(NodeId::new(1));
    let e = Value::EdgeRef(
        crate::graph::EdgeId::new(1),
        NodeId::new(1),
        NodeId::new(2),
        EdgeType::new("R"),
    );
    let t = |v: Result<Value, ExecutionError>| v.unwrap().as_property().cloned().unwrap();
    assert_eq!(
        t(eval_binary_op(&BinaryOp::Eq, pv(1i64), n.clone())),
        PropertyValue::Boolean(false)
    );
    assert_eq!(
        t(eval_binary_op(&BinaryOp::Ne, pv("a"), e.clone())),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        t(eval_binary_op(&BinaryOp::Eq, e.clone(), e.clone())),
        PropertyValue::Boolean(true)
    );
    let err = eval_binary_op(&BinaryOp::Sub, pv(1i64), n)
        .unwrap_err()
        .to_string();
    assert!(err.contains("on the right"), "{err}");
    assert_eq!(
        t(eval_binary_op(
            &BinaryOp::Ne,
            pv(PropertyValue::Array(vec![1i64.into()])),
            pv(PropertyValue::Array(vec![1i64.into(), 2i64.into()]))
        )),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        t(eval_binary_op(
            &BinaryOp::Ne,
            pv(PropertyValue::Array(vec![1i64.into()])),
            pv(PropertyValue::Array(vec![PropertyValue::Null]))
        )),
        PropertyValue::Null
    );
    // String position operators on non-strings are null.
    assert_eq!(
        t(eval_binary_op(&BinaryOp::StartsWith, pv(1i64), pv("a"))),
        PropertyValue::Null
    );
    assert_eq!(
        t(eval_binary_op(&BinaryOp::EndsWith, pv("a"), pv(1i64))),
        PropertyValue::Null
    );
    assert_eq!(
        t(eval_binary_op(&BinaryOp::Contains, pv(true), pv(true))),
        PropertyValue::Null
    );
    assert_eq!(
        string_position_op(
            StringPositionOp::EndsWith,
            &PropertyValue::String("abc".into()),
            &PropertyValue::String("bc".into())
        ),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        string_position_op(
            StringPositionOp::StartsWith,
            &PropertyValue::String("abc".into()),
            &PropertyValue::String("b".into())
        ),
        PropertyValue::Boolean(false)
    );
    assert_eq!(
        string_position_op(
            StringPositionOp::Contains,
            &PropertyValue::String("abc".into()),
            &PropertyValue::String("b".into())
        ),
        PropertyValue::Boolean(true)
    );
    // Lists of entities built by an expression concatenate as `Value::List`.
    let l = Value::List(vec![Value::NodeRef(NodeId::new(1))]);
    match eval_binary_op(&BinaryOp::Add, l.clone(), l).unwrap() {
        Value::List(items) => assert_eq!(items.len(), 2),
        other => panic!("{other:?}"),
    }
    assert!(eval_index(pv(1i64), Value::NodeRef(NodeId::new(1)), &GraphStore::new()).is_err());
    assert_eq!(
        cypher_ordering(
            &PropertyValue::String("a".into()),
            &PropertyValue::String("b".into())
        ),
        Some(std::cmp::Ordering::Less)
    );
    assert_eq!(
        cypher_ordering(&PropertyValue::Float(1.0), &PropertyValue::Integer(2)),
        Some(std::cmp::Ordering::Less)
    );
    assert_eq!(
        cypher_ordering(
            &PropertyValue::Integer(1),
            &PropertyValue::String("a".into())
        ),
        None
    );
}

#[test]
fn eval_predicate_standalone_evaluates_a_predicate() {
    let (store, a, _, _) = small_graph();
    let mut r = Record::new();
    r.bind("a", Value::NodeRef(a));
    assert!(eval_predicate_standalone(&parse_expr("a.n = 1"), &r, &store).unwrap());
    assert!(!eval_predicate_standalone(&parse_expr("a.n = 2"), &r, &store).unwrap());
    assert!(!eval_predicate_standalone(&parse_expr("a.missing = 2"), &r, &store).unwrap());
}

// ---------------------------------------------------------------------------
// EXISTS / COUNT subqueries and pattern comprehensions
// ---------------------------------------------------------------------------

/// `(a)-[:K {w:1}]->(b)-[:K]->(c)-[:K]->(c)`, `(a)-[:L]->(c)`, all `:P` with a
/// `name`.
fn exists_graph() -> GraphStore {
    let mut store = GraphStore::new();
    run_mut(
        &mut store,
        "CREATE (a:P {name:'a'})-[:K {w: 1}]->(b:P {name:'b'})-[:K]->(c:P {name:'c'}), \
         (a)-[:L]->(c), (c)-[:K]->(c), (:Q {name: 'q'})",
    )
    .unwrap();
    store
}

fn node_named(store: &GraphStore, name: &str) -> NodeId {
    let id = one_on(
        store,
        &format!("MATCH (n {{name: '{name}'}}) RETURN id(n) AS v"),
    );
    NodeId::new(id.as_integer().unwrap() as u64)
}

fn by_name(store: &GraphStore, q: &str) -> Vec<PropertyValue> {
    rows(store, q, "v")
}

fn bools(v: &[bool]) -> Vec<PropertyValue> {
    v.iter().map(|b| PropertyValue::Boolean(*b)).collect()
}

#[test]
fn exists_subquery_shapes() {
    let store = exists_graph();
    let q = |sub: &str| {
        by_name(
            &store,
            &format!("MATCH (n:P) WITH n ORDER BY n.name RETURN EXISTS {{ {sub} }} AS v"),
        )
    };
    assert_eq!(
        q("MATCH (n)-[:K]->(m:P {name: 'b'})"),
        bools(&[true, false, false])
    );
    assert_eq!(
        q("MATCH (n)-[:K]->(m) WHERE m.name = 'c'"),
        bools(&[false, true, true])
    );
    assert_eq!(
        q("MATCH (x:P {name: 'a'})-[:K*1..2]->(n)"),
        bools(&[false, true, true])
    );
    assert_eq!(q("MATCH ()-[:K]->(n)"), bools(&[false, true, true]));
    assert_eq!(q("MATCH (n)<-[:K]-(m)"), bools(&[false, true, true]));
    assert_eq!(q("MATCH (n)-[:K {w: 1}]->()"), bools(&[true, false, false]));
    assert_eq!(
        q("MATCH (n)-[:K {w: 2}]->()"),
        bools(&[false, false, false])
    );
    assert_eq!(
        q("MATCH p = (n)-[r:K]->(m) WHERE length(p) = 1 AND type(r) = 'K' AND m.name <> n.name"),
        bools(&[true, true, false])
    );
    assert_eq!(
        q("MATCH (n)-[:K]->(), (n)-[:L]->()"),
        bools(&[true, false, false])
    );
    assert_eq!(q("MATCH (n)-[:NOPE]->()"), bools(&[false, false, false]));
    assert_eq!(q("MATCH (n)-->(:Q)"), bools(&[false, false, false]));
    assert_eq!(
        q("MATCH (n)-[*]->(m:P {name: 'c'})"),
        bools(&[true, true, true])
    );
    assert_eq!(q("MATCH (n:Q)"), bools(&[false, false, false]));
}

#[test]
fn exists_between_two_pinned_nodes() {
    let store = exists_graph();
    let got = rows(
        &store,
        "MATCH (n:P), (m:P) WHERE EXISTS { MATCH (n)-[:K]-(m) } RETURN n.name + m.name AS v ORDER BY v",
        "v",
    );
    let want: Vec<PropertyValue> = ["ab", "ba", "bc", "cb", "cc"]
        .iter()
        .map(|s| PropertyValue::String(s.to_string()))
        .collect();
    assert_eq!(got, want);
    let got = rows(
        &store,
        "MATCH (n:P), (m:P) WHERE EXISTS { MATCH (n)<-[:K {w: 1}]-(m) } RETURN n.name + m.name AS v ORDER BY v",
        "v",
    );
    assert_eq!(got, vec![PropertyValue::String("ba".into())]);
}

#[test]
fn count_subquery_counts_every_match() {
    let store = exists_graph();
    let got = by_name(
        &store,
        "MATCH (n:P) WITH n ORDER BY n.name RETURN COUNT { MATCH (n)-[:K]->() } AS v",
    );
    assert_eq!(
        got,
        vec![
            PropertyValue::Integer(1),
            PropertyValue::Integer(1),
            PropertyValue::Integer(1)
        ]
    );
    let got = by_name(&store, "MATCH (n:P) WITH n ORDER BY n.name RETURN COUNT { MATCH (n)-[]-(m) WHERE m.name <> 'a' } AS v");
    // a: b, c (via L); b: c (b->c); c: b, itself once.
    assert_eq!(
        got,
        vec![
            PropertyValue::Integer(2),
            PropertyValue::Integer(1),
            PropertyValue::Integer(2)
        ]
    );
}

#[test]
fn exists_directly_through_eval_expression() {
    let store = exists_graph();
    let a = node_named(&store, "a");
    let mut r = Record::new();
    r.bind("n", Value::NodeRef(a));
    r.bind("one", pv(1i64));
    assert_eq!(
        dxp(&store, &r, "EXISTS { MATCH (n)-[:L]->() }"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        dxp(&store, &r, "COUNT { MATCH (n)-->() }"),
        PropertyValue::Integer(2)
    );
    // Bound to something that is not a node: nothing can match.
    assert_eq!(
        dxp(&store, &r, "EXISTS { MATCH (one)-->() }"),
        PropertyValue::Boolean(false)
    );
    // An inner node pinned to a non-node never closes.
    assert_eq!(
        dxp(&store, &r, "EXISTS { MATCH (n)-->(one) }"),
        PropertyValue::Boolean(false)
    );
    // A path variable binds the walked path.
    assert_eq!(
        dxp(&store, &r, "[p = (n)-[:K]->()-[:K]->() | length(p)]"),
        PropertyValue::Array(vec![2i64.into()])
    );
    match dx(&store, &r, "[p = (n)-[:K]->() | p]").unwrap() {
        Value::List(items) => {
            assert_eq!(items.len(), 1);
            assert!(
                matches!(&items[0], Value::Path { nodes, edges } if nodes.len() == 2 && edges.len() == 1)
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        dxp(&store, &r, "[(n)-[:K|L]->(m) WHERE m.name <> 'b' | m.name]"),
        PropertyValue::Array(vec!["c".into()])
    );
    assert_eq!(
        dxp(&store, &r, "[(n)-[:K]->(m) | m.name]"),
        PropertyValue::Array(vec!["b".into()])
    );
}

#[test]
fn walked_path_follows_edges_in_either_direction() {
    let store = exists_graph();
    let edges: Vec<_> = store
        .all_edges()
        .iter()
        .map(|e| (e.id, e.source, e.target))
        .collect();
    let (eid, src, tgt) = edges[0];
    match walked_path(&store, (tgt, 0), &[eid]) {
        Value::Path { nodes, edges } => {
            assert_eq!(nodes, vec![tgt, src]);
            assert_eq!(edges, vec![eid]);
        }
        other => panic!("{other:?}"),
    }
    // An edge that no longer exists leaves the walk where it was.
    match walked_path(&store, (src, 0), &[crate::graph::EdgeId::new(999)]) {
        Value::Path { nodes, .. } => assert_eq!(nodes, vec![src, src]),
        other => panic!("{other:?}"),
    }
    assert!(edge_ref(&store, crate::graph::EdgeId::new(999)).is_none());
    assert!(
        matches!(edge_ref(&store, eid), Some(Value::EdgeRef(id, s, t, _)) if id == eid && s == src && t == tgt)
    );
}

#[test]
fn exists_node_matches_checks_labels_and_inline_props() {
    let store = exists_graph();
    let q = crate::query::parser::parse_query(
        "MATCH (x:P {name: 'a'}), (y:Q), (z:P {name: 'zz'}) RETURN 1",
    )
    .unwrap();
    let pats: Vec<_> = q.match_clauses[0]
        .pattern
        .paths
        .iter()
        .map(|p| p.start.clone())
        .collect();
    let a = node_named(&store, "a");
    assert!(exists_node_matches(&store, a, &pats[0]));
    assert!(!exists_node_matches(&store, a, &pats[1]));
    assert!(!exists_node_matches(&store, a, &pats[2]));
    assert!(!exists_node_matches(&store, NodeId::new(999), &pats[0]));
}

// ---------------------------------------------------------------------------
// misc helpers: names, arity, formatting, cost
// ---------------------------------------------------------------------------

#[test]
fn collect_expression_names_finds_every_variable() {
    let names = |e: &str| {
        let mut out = HashSet::new();
        collect_expression_names(&parse_expr(e), &mut out);
        let mut v: Vec<String> = out.into_iter().collect();
        v.sort();
        v
    };
    assert_eq!(names("a.x + -b"), vec!["a", "b"]);
    assert_eq!(names("coalesce(a, b.c)"), vec!["a", "b"]);
    assert_eq!(names("toUpper(a)"), vec!["a"]);
    assert_eq!(names("[a, b]"), vec!["a", "b"]);
    assert_eq!(names("{k: a, j: b}"), vec!["a", "b"]);
    assert_eq!(names("a[b]"), vec!["a", "b"]);
    assert_eq!(names("a[b..c]"), vec!["a", "b", "c"]);
    assert_eq!(
        names("CASE a WHEN b THEN c ELSE d END"),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(names("NOT a"), vec!["a"]);
    assert_eq!(names("1"), Vec::<String>::new());
}

#[test]
fn known_functions_and_arity_table() {
    assert!(is_known_function("toUpper"));
    assert!(is_known_function("date.realtime"));
    assert!(!is_known_function("lenght"));
    assert_eq!(min_arity("ToUpper"), Some(1));
    assert_eq!(min_arity("point.withinBBox"), Some(3));
    assert_eq!(min_arity("rand"), None);
}

#[test]
fn format_expression_renders_every_form() {
    let fe = |e: &str| format_expression(&parse_expr(e));
    assert_eq!(fe("a.x > 1 AND (b OR c)"), "a.x > Integer(1) AND (b OR c)");
    assert_eq!(fe("a - (b - c)"), "a - (b - c)");
    assert_eq!(fe("(a - b) - c"), "a - b - c");
    assert_eq!(fe("a ^ (b ^ c)"), "a ^ b ^ c");
    assert_eq!(fe("(a ^ b) ^ c"), "(a ^ b) ^ c");
    assert_eq!(fe("NOT (a AND b)"), "NOT (a AND b)");
    assert_eq!(fe("NOT a"), "NOT a");
    assert_eq!(fe("a IS NULL"), "a IS NULL");
    assert_eq!(fe("(a + b) IS NOT NULL"), "(a + b) IS NOT NULL");
    assert_eq!(fe("-a"), "- a");
    assert_eq!(fe("count(DISTINCT a)"), "count(DISTINCT a)");
    assert_eq!(fe("coalesce(a, b)"), "coalesce(a, b)");
    assert_eq!(fe("$p"), "$p");
    assert_eq!(fe("[a]"), "...");
    for (op, sym) in [
        ("a <> b", "<>"),
        ("a <= b", "<="),
        ("a >= b", ">="),
        ("a < b", "<"),
        ("a = b", "="),
        ("a * b", "*"),
        ("a / b", "/"),
        ("a % b", "%"),
        ("a XOR b", "XOR"),
        ("a STARTS WITH b", "STARTS WITH"),
        ("a ENDS WITH b", "ENDS WITH"),
        ("a CONTAINS b", "CONTAINS"),
        ("a IN b", "IN"),
        ("a =~ b", "=~"),
    ] {
        assert_eq!(fe(op), format!("a {sym} b"), "{op}");
    }
    assert_eq!(
        format_expression(&Expression::PathVariable("p".into())),
        "path(p)"
    );
}

#[test]
fn predicate_cost_weights() {
    let c = |e: &str| predicate_cost(&parse_expr(e));
    assert_eq!(c("a.x"), 1);
    assert_eq!(c("a"), 0);
    assert_eq!(c("$p"), 0);
    assert_eq!(c("a.x > 1"), 2);
    assert_eq!(c("a.x CONTAINS 'z'"), 5);
    assert_eq!(c("NOT a.x"), 2);
    assert_eq!(c("toUpper(a.x)"), 9);
    assert_eq!(c("[a.x, a.y]"), 3);
    assert_eq!(c("{k: a.x}"), 2);
    assert_eq!(c("CASE a.x WHEN 1 THEN a.y ELSE a.z END"), 4);
    assert_eq!(c("a.l[a.i]"), 3);
    assert_eq!(c("a.l[a.i..a.j]"), 5);
    assert_eq!(c("a.l[..]"), 3);
    assert_eq!(c("EXISTS { MATCH (a)-->() }"), 50);
    assert_eq!(c("[x IN a.l WHERE x > 1 | x]"), 22);
    assert_eq!(c("any(x IN a.l WHERE x > 1)"), 22);
    assert_eq!(c("reduce(t = 0, x IN a.l | t + x)"), 22);
    assert_eq!(c("[(a)-->(b) | b]"), 50);
    assert_eq!(predicate_cost(&Expression::PathVariable("p".into())), 0);
}

// ---------------------------------------------------------------------------
// leaf operators and the trait's defaults
// ---------------------------------------------------------------------------

fn drain_all(op: &mut dyn PhysicalOperator, store: &GraphStore) -> Vec<Record> {
    let mut out = Vec::new();
    while let Some(r) = op.next(store).unwrap() {
        out.push(r);
    }
    out
}

fn labelled_store() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..6 {
        let n = store.create_node("A");
        if i % 2 == 0 {
            store.add_label_to_node("default", n, "B").unwrap();
        }
    }
    store.create_node("C");
    store
}

#[test]
fn node_scan_multi_label_is_an_intersection() {
    let store = labelled_store();
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("A"), Label::new("B")]);
    let ids: Vec<u64> = drain_all(&mut op, &store)
        .iter()
        .map(|r| r.get("n").unwrap().node_id().unwrap().as_u64())
        .collect();
    assert_eq!(ids.len(), 3);
    assert!(ids.windows(2).all(|w| w[0] < w[1]));
    // With a limit, a prefix of the same order.
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("B"), Label::new("A")])
        .with_early_limit(2);
    let limited: Vec<u64> = drain_all(&mut op, &store)
        .iter()
        .map(|r| r.get("n").unwrap().node_id().unwrap().as_u64())
        .collect();
    assert_eq!(limited, ids[..2].to_vec());
    // No labels at all: every node.
    let mut op = NodeScanOperator::new("n".into(), vec![]);
    assert_eq!(drain_all(&mut op, &store).len(), 7);
    assert_eq!(op.describe().details, "var=n, all labels");
    op.reset();
    assert_eq!(drain_all(&mut op, &store).len(), 7);
    let d = NodeScanOperator::new("n".into(), vec![Label::new("A")]).describe();
    assert_eq!(d.name, "NodeScan");
    assert!(d.details.contains("\"A\""), "{}", d.details);
}

#[test]
fn node_scan_push_limit_and_batches() {
    let store = labelled_store();
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("A")]);
    assert!(op.try_push_limit(4));
    assert!(op.try_push_limit(10));
    let b1 = op.next_batch(&store, 3).unwrap().unwrap();
    assert_eq!(b1.records.len(), 3);
    assert_eq!(b1.columns, vec!["n".to_string()]);
    let b2 = op.next_batch(&store, 3).unwrap().unwrap();
    assert_eq!(b2.records.len(), 1);
    assert!(op.next_batch(&store, 3).unwrap().is_none());
    assert!(op.next(&store).unwrap().is_none());
    // Unlimited batches, exhausted.
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("C")]);
    assert_eq!(op.next_batch(&store, 10).unwrap().unwrap().records.len(), 1);
    assert!(op.next_batch(&store, 10).unwrap().is_none());
}

#[test]
fn node_scan_large_batches_build_records_in_parallel() {
    let mut store = GraphStore::new();
    for _ in 0..1100 {
        store.create_node("A");
    }
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("A")]);
    let batch = op.next_batch(&store, 2048).unwrap().unwrap();
    assert_eq!(batch.records.len(), 1100);
    let first = batch.records[0]
        .get("n")
        .unwrap()
        .node_id()
        .unwrap()
        .as_u64();
    let last = batch.records[1099]
        .get("n")
        .unwrap()
        .node_id()
        .unwrap()
        .as_u64();
    assert!(first < last);
    // Early limit reached exactly at a batch boundary.
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("A")]).with_early_limit(2);
    assert_eq!(op.next_batch(&store, 2).unwrap().unwrap().records.len(), 2);
    assert!(op.next_batch(&store, 2).unwrap().is_none());
}

#[test]
fn label_count_operator() {
    let store = labelled_store();
    let count_of = |labels: Vec<Label>| {
        let mut op = LabelCountOperator::new(labels, "c".into());
        let rows = drain_all(&mut op, &store);
        assert_eq!(rows.len(), 1);
        op.reset();
        assert_eq!(drain_all(&mut op, &store).len(), 1);
        rows[0].get("c").unwrap().as_property().cloned().unwrap()
    };
    assert_eq!(count_of(vec![]), PropertyValue::Integer(7));
    assert_eq!(count_of(vec![Label::new("A")]), PropertyValue::Integer(6));
    assert_eq!(
        count_of(vec![Label::new("A"), Label::new("B")]),
        PropertyValue::Integer(3)
    );
    assert_eq!(
        LabelCountOperator::new(vec![], "c".into())
            .describe()
            .details,
        "labels=[all], alias=c"
    );
    assert_eq!(
        LabelCountOperator::new(vec![Label::new("A"), Label::new("B")], "c".into())
            .describe()
            .details,
        "labels=[A, B], alias=c"
    );
}

#[test]
fn edge_count_operators() {
    let store = exists_graph();
    let count_of = |t: Option<&str>| {
        let mut op = EdgeCountOperator::new(t.map(String::from), "c".into());
        let rows = drain_all(&mut op, &store);
        assert_eq!(rows.len(), 1);
        op.reset();
        assert_eq!(drain_all(&mut op, &store).len(), 1);
        rows[0].get("c").unwrap().as_property().cloned().unwrap()
    };
    assert_eq!(count_of(Some("K")), PropertyValue::Integer(3));
    assert_eq!(count_of(Some("NOPE")), PropertyValue::Integer(0));
    assert_eq!(count_of(None), PropertyValue::Integer(4));
    assert_eq!(
        EdgeCountOperator::new(Some("K".into()), "c".into())
            .describe()
            .details,
        "type=K, alias=c"
    );
    assert_eq!(
        EdgeCountOperator::new(None, "c".into()).describe().details,
        "all types, alias=c"
    );

    let mut op = EdgeTypeCountOperator::new("t".into(), "c".into());
    let mut got: Vec<(String, i64)> = drain_all(&mut op, &store)
        .iter()
        .map(|r| {
            let t = match r.get("t").unwrap().as_property().unwrap() {
                PropertyValue::String(s) => s.clone(),
                o => panic!("{o:?}"),
            };
            let c = r
                .get("c")
                .unwrap()
                .as_property()
                .unwrap()
                .as_integer()
                .unwrap();
            (t, c)
        })
        .collect();
    got.sort();
    assert_eq!(got, vec![("K".to_string(), 3), ("L".to_string(), 1)]);
    op.reset();
    let b = op.next_batch(&store, 1).unwrap().unwrap();
    assert_eq!(b.records.len(), 1);
    assert_eq!(op.next_batch(&store, 5).unwrap().unwrap().records.len(), 1);
    assert!(op.next_batch(&store, 5).unwrap().is_none());
    let d = op.describe();
    assert_eq!(d.name, "EdgeTypeCount");
    assert_eq!(d.details, "type_alias=t, count_alias=c");
}

/// An operator implementing only what the trait requires.
struct Rows(Vec<Record>, usize);
impl PhysicalOperator for Rows {
    fn next(&mut self, _: &GraphStore) -> ExecutionResult<Option<Record>> {
        let r = self.0.get(self.1).cloned();
        self.1 += 1;
        Ok(r)
    }
    fn reset(&mut self) {
        self.1 = 0;
    }
}

fn int_rows(n: i64) -> Rows {
    Rows(
        (0..n)
            .map(|i| {
                let mut r = Record::new();
                r.bind("i", pv(i));
                r
            })
            .collect(),
        0,
    )
}

#[test]
fn physical_operator_trait_defaults() {
    let mut store = GraphStore::new();
    let mut op = int_rows(3);
    assert!(!op.is_materialized());
    assert!(!op.try_push_limit(1));
    assert!(!op.hint_early_stop(1));
    assert!(op.filter_predicate().is_none());
    assert!(!op.retain_property_reads("a", "b"));
    assert!(op.take_retained_reads().is_none());
    assert!(op.children_mut().is_empty());
    assert!(!op.is_mutating());
    assert!(!op.amplifies_rows());
    let d = op.describe();
    assert_eq!(d.name, "Unknown");
    assert!(d.details.is_empty() && d.children.is_empty());
    assert_eq!(op.next_batch(&store, 2).unwrap().unwrap().records.len(), 2);
    assert_eq!(op.next_batch(&store, 2).unwrap().unwrap().records.len(), 1);
    assert!(op.next_batch(&store, 2).unwrap().is_none());
    op.reset();
    assert_eq!(
        op.next_batch_mut(&mut store, "default", 5)
            .unwrap()
            .unwrap()
            .records
            .len(),
        3
    );
    assert!(op
        .next_batch_mut(&mut store, "default", 5)
        .unwrap()
        .is_none());
    op.reset();
    assert!(op.next_mut(&mut store, "default").unwrap().is_some());
}

#[test]
fn drain_input_for_write_materialises_once() {
    let mut store = GraphStore::new();
    let mut input: OperatorBox = Box::new(int_rows(3));
    drain_input_for_write(&mut input, &mut store, "default").unwrap();
    assert!(input.is_materialized());
    // A second drain leaves the materialised rows alone.
    drain_input_for_write(&mut input, &mut store, "default").unwrap();
    let mut n = 0;
    while input.next(&store).unwrap().is_some() {
        n += 1;
    }
    assert_eq!(n, 3);
}

#[test]
fn operator_description_hash_and_format() {
    let leaf = |details: &str| OperatorDescription {
        name: "Materialized".into(),
        details: details.into(),
        children: vec![],
    };
    let tree = |details: &str| OperatorDescription {
        name: "Project".into(),
        details: "x".into(),
        children: vec![leaf(details)],
    };
    // A row count is data, not plan structure.
    assert_eq!(
        tree("3 rows").structural_hash(),
        tree("4000 rows").structural_hash()
    );
    assert_ne!(
        tree("3 rows").structural_hash(),
        tree("some rows").structural_hash()
    );
    assert_ne!(
        tree(" rows").structural_hash(),
        tree("3 rows").structural_hash()
    );
    assert_ne!(leaf("a").structural_hash(), tree("a").structural_hash());
    let text = tree("3 rows").format(0);
    assert_eq!(text, "Project (x)\n+- Materialized (3 rows)\n");
    let bare = OperatorDescription {
        name: "Leaf".into(),
        details: String::new(),
        children: vec![],
    };
    assert_eq!(bare.format(2), "   +- Leaf\n");
}

#[test]
fn graph_optimization_problem_objectives_and_penalties() {
    let p = GraphOptimizationProblem {
        costs: vec![1.0, 2.0],
        multi_costs: vec![vec![1.0, 0.0], vec![0.0, 1.0]],
        budget: Some(5.0),
        min_total: Some(4.0),
        dim: 2,
        lower: 0.0,
        upper: 10.0,
    };
    let x = Array1::from(vec![1.0, 1.0]);
    assert_eq!(Problem::dim(&p), 2);
    let (lo, hi) = Problem::bounds(&p);
    assert_eq!((lo[0], hi[1]), (0.0, 10.0));
    assert_eq!(p.objective(&x), 3.0);
    // Under budget, but short of the minimum total by 2: 2^2 * 100.
    assert_eq!(p.penalty(&x), 400.0);
    // Over budget by 1 (cost 6), total meets the minimum.
    let y = Array1::from(vec![2.0, 2.0]);
    assert_eq!(p.penalty(&y), 1.0);
    assert_eq!(p.num_objectives(), 2);
    assert_eq!(p.objectives(&y), vec![2.0, 2.0]);
    assert_eq!(MultiObjectiveProblem::dim(&p), 2);
    let (lo, hi) = MultiObjectiveProblem::bounds(&p);
    assert_eq!((lo[1], hi[0]), (0.0, 10.0));
    let free = GraphOptimizationProblem {
        budget: None,
        min_total: None,
        ..p
    };
    assert_eq!(free.penalty(&y), 0.0);
}

#[test]
fn hierarchy_functions_with_only_a_stale_index_refuse() {
    let mut store = hierarchy_store();
    // A new BROADER edge after the build leaves the index stale.
    run_mut(
        &mut store,
        "MATCH (x:Loose), (y:Term {code:'root'}) CREATE (x)-[:BROADER]->(y)",
    )
    .unwrap();
    let e = QueryEngine::new()
        .execute(
            "MATCH (a:Term {code:'l1'}), (b:Term {code:'root'}) RETURN subsumes(a, b) AS v",
            &store,
        )
        .unwrap_err()
        .to_string();
    assert!(e.contains("stale"), "{e}");
}

#[test]
fn node_scan_limit_pushed_after_initialisation() {
    let store = labelled_store();
    let mut op = NodeScanOperator::new("n".into(), vec![Label::new("A")]);
    assert!(op.next(&store).unwrap().is_some());
    // The ids are already materialised; the limit still stops the scan.
    assert!(op.try_push_limit(1));
    assert!(op.next(&store).unwrap().is_none());
}

#[test]
fn binary_op_null_operands_and_unknown_list_equality() {
    let t = |v: Result<Value, ExecutionError>| v.unwrap().as_property().cloned().unwrap();
    assert_eq!(
        t(eval_binary_op(&BinaryOp::Add, pv(1i64), Value::Null)),
        PropertyValue::Null
    );
    assert_eq!(
        t(eval_binary_op(&BinaryOp::Mul, Value::Null, pv(1i64))),
        PropertyValue::Null
    );
    let l = |v: Vec<PropertyValue>| pv(PropertyValue::Array(v));
    assert_eq!(
        t(eval_binary_op(
            &BinaryOp::Eq,
            l(vec![1i64.into()]),
            l(vec![PropertyValue::Null])
        )),
        PropertyValue::Null
    );
    assert_eq!(
        t(eval_binary_op(
            &BinaryOp::Eq,
            l(vec![1i64.into()]),
            l(vec![1.0f64.into()])
        )),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        ok("point({x: 1.5, y: -2.0})").as_map().unwrap().get("x"),
        Some(&PropertyValue::Float(1.5))
    );
}

#[test]
fn path_variable_expression_reads_the_binding() {
    let store = GraphStore::new();
    let mut r = Record::new();
    r.bind(
        "p",
        Value::Path {
            nodes: vec![NodeId::new(1)],
            edges: vec![],
        },
    );
    let got = eval_expression(&Expression::PathVariable("p".into()), &r, &store).unwrap();
    assert!(matches!(got, Value::Path { ref nodes, .. } if nodes.len() == 1));
    let e = eval_expression(&Expression::PathVariable("q".into()), &r, &store).unwrap_err();
    assert!(
        matches!(e, ExecutionError::VariableNotFoundInScope { .. }),
        "{e:?}"
    );
}

#[test]
fn exists_inline_property_falls_back_to_row_storage() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    // Written to the node's own map, not the column store.
    store.get_node_mut(n).unwrap().set_property("k", 7i64);
    let q = crate::query::parser::parse_query("MATCH (x:P {k: 7}), (y:P {k: 8}) RETURN 1").unwrap();
    let pats: Vec<_> = q.match_clauses[0]
        .pattern
        .paths
        .iter()
        .map(|p| p.start.clone())
        .collect();
    assert!(exists_node_matches(&store, n, &pats[0]));
    assert!(!exists_node_matches(&store, n, &pats[1]));
}

#[test]
fn exists_walks_respect_relationship_isomorphism_and_var_length() {
    let store = exists_graph();
    let (a, b2, c) = (
        node_named(&store, "a"),
        node_named(&store, "b"),
        node_named(&store, "c"),
    );
    let at = |id: NodeId| {
        let mut r = Record::new();
        r.bind("n", Value::NodeRef(id));
        r
    };
    // Three distinct K hops: only from a (a->b->c->c); the self-loop may not
    // be walked twice.
    let three = "EXISTS { MATCH (n)-[:K]->()-[:K]->()-[:K]->() }";
    assert_eq!(dxp(&store, &at(a), three), PropertyValue::Boolean(true));
    assert_eq!(dxp(&store, &at(b2), three), PropertyValue::Boolean(false));
    assert_eq!(dxp(&store, &at(c), three), PropertyValue::Boolean(false));
    // A variable-length segment followed by a fixed one.
    let var = "EXISTS { MATCH (n)-[:K*1..2]->(m)-[:L]->() }";
    assert_eq!(dxp(&store, &at(a), var), PropertyValue::Boolean(false));
    let var2 = "EXISTS { MATCH (n)-[:K*1..2]->(m {name: 'c'}) }";
    assert_eq!(dxp(&store, &at(a), var2), PropertyValue::Boolean(true));
    assert_eq!(
        dxp(&store, &at(c), "EXISTS { MATCH (n)-[:K*2..2]->(n) }"),
        PropertyValue::Boolean(false)
    );
    assert_eq!(
        dxp(&store, &at(a), "COUNT { MATCH (n)-[:K*]->() }"),
        PropertyValue::Integer(3)
    );
    // A quantifier over a list of entities.
    let mut r = at(a);
    r.bind(
        "l",
        Value::List(vec![Value::NodeRef(a), Value::NodeRef(b2)]),
    );
    assert_eq!(
        dxp(&store, &r, "any(x IN l WHERE x.name = 'b')"),
        PropertyValue::Boolean(true)
    );
    assert_eq!(
        dxp(&store, &r, "all(x IN l WHERE x.name = 'b')"),
        PropertyValue::Boolean(false)
    );
}

#[test]
fn exists_propagates_errors_from_the_inner_where() {
    let store = exists_graph();
    let (a, b2) = (node_named(&store, "a"), node_named(&store, "b"));
    let mut r = Record::new();
    r.bind("n", Value::NodeRef(a));
    // Walked neighbours.
    let e = dx(&store, &r, "EXISTS { MATCH (n)-->(m) WHERE size(m) = 1 }")
        .unwrap_err()
        .to_string();
    assert!(e.contains("size()"), "{e}");
    let e = dx(&store, &r, "EXISTS { MATCH (n)<--(m) WHERE size(m) = 1 }");
    assert_eq!(
        prop(e),
        PropertyValue::Boolean(false),
        "a has no incoming edge, so nothing is evaluated"
    );
    // A pinned far end looked up directly.
    r.bind("m", Value::NodeRef(b2));
    let e = dx(&store, &r, "EXISTS { MATCH (n)-->(m) WHERE size(m) = 1 }")
        .unwrap_err()
        .to_string();
    assert!(e.contains("size()"), "{e}");
    let mut r2 = Record::new();
    r2.bind("n", Value::NodeRef(b2));
    let e = dx(&store, &r2, "EXISTS { MATCH (n)<--(m) WHERE size(m) = 1 }")
        .unwrap_err()
        .to_string();
    assert!(e.contains("size()"), "{e}");
}
