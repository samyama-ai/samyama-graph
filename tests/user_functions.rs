//! User-defined functions and procedures registered from Rust (LANG-10,
//! #1674; ADR-044).
//!
//! The registry is the process's and these tests run in parallel, so every
//! test registers names of its own.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{Record, Value};
use samyama::query::QueryEngine;
use samyama::udf::{self, RegistrationError};

fn store(create: &str) -> GraphStore {
    let mut store = GraphStore::new();
    if !create.is_empty() {
        QueryEngine::new()
            .execute_mut(create, &mut store, "default")
            .expect("fixture");
    }
    store
}

fn rows(store: &GraphStore, query: &str) -> Vec<Record> {
    QueryEngine::new()
        .execute(query, store)
        .unwrap_or_else(|e| panic!("`{query}`: {e}"))
        .records
}

fn one(store: &GraphStore, query: &str, column: &str) -> Value {
    let r = rows(store, query);
    assert_eq!(r.len(), 1, "`{query}`");
    r[0].get(column)
        .cloned()
        .unwrap_or_else(|| panic!("`{query}` has no `{column}`"))
}

fn refused(store: &GraphStore, query: &str) -> String {
    match QueryEngine::new().execute(query, store) {
        Ok(r) => panic!(
            "`{query}` returned {} rows; it should be refused",
            r.records.len()
        ),
        Err(e) => e.to_string(),
    }
}

fn int(n: i64) -> Value {
    Value::Property(PropertyValue::Integer(n))
}

fn plus_one(args: &[PropertyValue]) -> Result<PropertyValue, String> {
    match &args[0] {
        PropertyValue::Integer(n) => Ok(PropertyValue::Integer(n + 1)),
        PropertyValue::Null => Ok(PropertyValue::Null),
        other => Err(format!("expected an integer, got {other}")),
    }
}

// ---------- functions ----------

#[test]
fn a_registered_function_is_callable_wherever_an_expression_goes() {
    udf::register_function("t1.plusOne", 1..=1, plus_one).unwrap();
    let g = store("CREATE (:N {v: 1}), (:N {v: 5})");
    assert_eq!(one(&g, "RETURN t1.plusOne(1) AS x", "x"), int(2));
    assert_eq!(
        one(&g, "RETURN T1.PLUSONE(1) AS x", "x"),
        int(2),
        "names match regardless of case"
    );
    let r = rows(
        &g,
        "MATCH (n:N) WHERE t1.plusOne(n.v) > 3 RETURN t1.plusOne(n.v) AS x",
    );
    assert_eq!(r.len(), 1);
    assert_eq!(r[0].get("x"), Some(&int(6)));
    assert_eq!(
        one(&g, "RETURN t1.plusOne(null) AS x", "x"),
        Value::Null,
        "null in, null out, as the function decided"
    );
    assert_eq!(one(&g, "WITH t1.plusOne(41) AS y RETURN y", "y"), int(42));

    let mut g = g;
    QueryEngine::new()
        .execute_mut("MATCH (n:N) SET n.w = t1.plusOne(n.v)", &mut g, "default")
        .expect("in SET");
    let r = rows(&g, "MATCH (n:N) RETURN n.w AS w ORDER BY w");
    assert_eq!(
        r.iter().map(|r| r.get("w").cloned()).collect::<Vec<_>>(),
        [Some(int(2)), Some(int(6))]
    );
}

#[test]
fn an_undotted_name_works_too() {
    // CH-UDF's own probe names: `RETURN myfn(1)` and `CALL myfn()`.
    udf::register_function("t2fn", 1..=1, plus_one).unwrap();
    let g = store("");
    assert_eq!(one(&g, "RETURN t2fn(1) AS x", "x"), int(2));
}

#[test]
fn an_unregistered_name_is_still_refused_at_compile_time() {
    // Over an empty graph, so a run-time check would never fire.
    let g = store("");
    let err = refused(&g, "MATCH (n) RETURN t3nothing(n.v)");
    assert!(err.contains("UnknownFunction"), "{err}");

    udf::register_function("t3fn", 0..=0, |_| Ok(PropertyValue::Integer(7))).unwrap();
    assert_eq!(one(&g, "RETURN t3fn() AS x", "x"), int(7));
    assert!(udf::unregister_function("t3fn"));
    assert!(
        refused(&g, "MATCH (n) RETURN t3fn()").contains("UnknownFunction"),
        "removed means unknown again"
    );
    assert!(!udf::unregister_function("t3fn"));
}

#[test]
fn lists_and_maps_go_in_and_come_out() {
    udf::register_function("t4.sum", 1..=1, |args| match &args[0] {
        PropertyValue::Array(xs) => Ok(PropertyValue::Integer(
            xs.iter()
                .map(|x| match x {
                    PropertyValue::Integer(n) => *n,
                    _ => 0,
                })
                .sum(),
        )),
        other => Err(format!("expected a list, got {other}")),
    })
    .unwrap();
    udf::register_function("t4.keys", 1..=1, |args| match &args[0] {
        PropertyValue::Map(m) => {
            let mut k: Vec<String> = m.keys().cloned().collect();
            k.sort();
            Ok(PropertyValue::Array(
                k.into_iter().map(PropertyValue::String).collect(),
            ))
        }
        other => Err(format!("expected a map, got {other}")),
    })
    .unwrap();
    let g = store("CREATE (:N {a: 1, b: 2})");
    assert_eq!(one(&g, "RETURN t4.sum([1, 2, 3]) AS x", "x"), int(6));
    let keys = one(&g, "MATCH (n:N) RETURN t4.keys(properties(n)) AS x", "x");
    assert_eq!(
        keys,
        Value::Property(PropertyValue::Array(vec![
            PropertyValue::String("a".into()),
            PropertyValue::String("b".into())
        ]))
    );
}

#[test]
fn a_node_is_not_a_value() {
    udf::register_function("t5.id", 1..=1, |args| Ok(args[0].clone())).unwrap();
    let g = store("CREATE (:N {v: 1})");
    let err = refused(&g, "MATCH (n:N) RETURN t5.id(n)");
    assert!(err.contains("argument 1 is a node"), "{err}");
    assert!(
        err.contains("`properties(n)`"),
        "says what to pass instead: {err}"
    );
}

#[test]
fn the_wrong_number_of_arguments_is_refused() {
    udf::register_function("t6.pair", 2..=3, |_| Ok(PropertyValue::Null)).unwrap();
    let g = store("");
    let err = refused(&g, "RETURN t6.pair(1) AS x");
    assert!(
        err.contains("`t6.pair()` takes 2 to 3 arguments; got 1"),
        "{err}"
    );
    assert!(
        err.contains("Samyama.ClientError.Statement.ArgumentError"),
        "{err}"
    );
}

#[test]
fn an_error_or_a_panic_fails_the_query_not_the_process() {
    udf::register_function(
        "t7.fails",
        0..=0,
        |_| Err("the tax table is missing".into()),
    )
    .unwrap();
    udf::register_function("t7.panics", 0..=0, |_| panic!("index out of range")).unwrap();
    let g = store("");
    let err = refused(&g, "RETURN t7.fails() AS x");
    assert!(
        err.contains("`t7.fails()` failed: the tax table is missing"),
        "{err}"
    );
    let err = refused(&g, "RETURN t7.panics() AS x");
    assert!(
        err.contains("`t7.panics()` panicked: index out of range"),
        "{err}"
    );
    assert_eq!(
        one(&g, "RETURN 1 AS x", "x"),
        int(1),
        "and the engine carries on"
    );
}

// ---------- procedures ----------

fn register_people_by_label() {
    udf::register_procedure(
        "t9.countLabel",
        1..=1,
        &["label", "count"],
        |store, args| {
            let PropertyValue::String(label) = &args[0] else {
                return Err("a label is a string".into());
            };
            let n = store
                .get_nodes_by_label(&samyama::graph::Label::new(label.as_str()))
                .len();
            Ok(vec![vec![
                PropertyValue::String(label.clone()),
                PropertyValue::Integer(n as i64),
            ]])
        },
    )
    .unwrap();
}

#[test]
fn a_registered_procedure_reads_the_graph_and_yields_its_columns() {
    register_people_by_label();
    let g = store("CREATE (:P), (:P), (:Q)");
    assert_eq!(
        one(
            &g,
            "CALL t9.countLabel('P') YIELD count RETURN count",
            "count"
        ),
        int(2)
    );
    assert_eq!(
        one(&g, "CALL t9.countLabel('Q') YIELD count AS n RETURN n", "n"),
        int(1),
        "YIELD can alias"
    );
    let all = rows(&g, "CALL t9.countLabel('P')");
    assert_eq!(all.len(), 1);
    assert!(
        all[0].get("label").is_some() && all[0].get("count").is_some(),
        "no YIELD: every column"
    );

    let err = refused(&g, "CALL t9.countLabel('P') YIELD total RETURN total");
    assert!(
        err.contains("`t9.countLabel()` yields label, count; there is no `total`"),
        "{err}"
    );
}

#[test]
fn a_procedure_with_several_rows_composes_with_the_rest_of_the_query() {
    udf::register_procedure("t10.range", 1..=1, &["i"], |_, args| {
        let PropertyValue::Integer(n) = args[0] else {
            return Err("n is an integer".into());
        };
        Ok((0..n).map(|i| vec![PropertyValue::Integer(i)]).collect())
    })
    .unwrap();
    let g = store("");
    assert_eq!(
        one(
            &g,
            "CALL t10.range(5) YIELD i WHERE i > 1 RETURN sum(i) AS s",
            "s"
        ),
        int(9)
    );
    let mut params = std::collections::HashMap::new();
    params.insert("n".to_string(), PropertyValue::Integer(3));
    let r = QueryEngine::new()
        .execute_with_params("CALL t10.range($n) YIELD i RETURN i", &g, &params)
        .unwrap();
    assert_eq!(r.records.len(), 3, "arguments from parameters");
}

#[test]
fn a_procedure_is_a_read() {
    register_people_by_label();
    assert!(!QueryEngine::new()
        .statement_is_write("CALL t9.countLabel('P') YIELD count RETURN count")
        .unwrap());
}

#[test]
fn a_row_that_does_not_match_the_columns_is_refused() {
    udf::register_procedure("t11.ragged", 0..=0, &["a", "b"], |_, _| {
        Ok(vec![vec![PropertyValue::Integer(1)]])
    })
    .unwrap();
    let g = store("");
    let err = refused(&g, "CALL t11.ragged() YIELD a RETURN a");
    assert!(
        err.contains("returned a row of 1 values; it declared 2 columns (a, b)"),
        "{err}"
    );
}

#[test]
fn an_unregistered_procedure_is_still_not_found() {
    let g = store("");
    let err = refused(&g, "CALL t12.nothing()");
    assert!(
        err.contains("Samyama.ClientError.Procedure.NotFound"),
        "{err}"
    );
}

// ---------- names ----------

#[test]
fn a_name_the_engine_uses_is_refused_at_registration() {
    let f = |_: &[PropertyValue]| Ok(PropertyValue::Null);
    for (name, says) in [
        ("toUpper", "a built-in function"),
        ("COUNT", "a built-in aggregate"),
        ("date.mine", "the built-in `date` namespace"),
        ("vector.mine", "the built-in `vector` namespace"),
    ] {
        match udf::register_function(name, 0..=0, f) {
            Err(e @ RegistrationError::Reserved { .. }) => {
                assert!(e.to_string().contains(says), "{name}: {e}")
            }
            other => panic!("{name}: {other:?}"),
        }
    }
    let p = |_: &GraphStore, _: &[PropertyValue]| Ok(vec![]);
    for (name, says) in [
        ("db.labels", "the built-in `db` namespace"),
        ("algo.mine", "the built-in `algo` namespace"),
        ("pageRank", "a built-in algorithm"),
        ("ai.rag.retrieve", "the built-in `ai` namespace"),
    ] {
        match udf::register_procedure(name, 0..=0, &["x"], p) {
            Err(e @ RegistrationError::Reserved { .. }) => {
                assert!(e.to_string().contains(says), "{name}: {e}")
            }
            other => panic!("{name}: {other:?}"),
        }
    }
}

#[test]
fn a_malformed_registration_is_refused() {
    let f = |_: &[PropertyValue]| Ok(PropertyValue::Null);
    for name in ["", "1abc", "a..b", "a.", "my-fn", "my fn"] {
        assert_eq!(
            udf::register_function(name, 0..=0, f),
            Err(RegistrationError::InvalidName(name.to_string())),
            "{name:?}"
        );
    }
    #[allow(clippy::reversed_empty_ranges)]
    let empty = 2..=1;
    assert!(matches!(
        udf::register_function("t13.f", empty, f),
        Err(RegistrationError::InvalidArity { min: 2, max: 1 })
    ));
    let p = |_: &GraphStore, _: &[PropertyValue]| Ok(vec![]);
    assert!(matches!(
        udf::register_procedure("t13.p", 0..=0, &[], p),
        Err(RegistrationError::InvalidColumns { .. })
    ));
    assert!(matches!(
        udf::register_procedure("t13.p", 0..=0, &["a", "a"], p),
        Err(RegistrationError::InvalidColumns { .. })
    ));
    assert!(matches!(
        udf::register_procedure("t13.p", 0..=0, &["a.b"], p),
        Err(RegistrationError::InvalidColumns { .. })
    ));
}

#[test]
fn the_registrations_can_be_listed() {
    udf::register_function("t14.listed", 0..=0, |_| Ok(PropertyValue::Null)).unwrap();
    udf::register_procedure("t14.listedProc", 0..=0, &["x"], |_, _| Ok(vec![])).unwrap();
    assert!(udf::function_names().contains(&"t14.listed".to_string()));
    assert!(udf::procedure_names().contains(&"t14.listedProc".to_string()));
}
