//! Additional unit tests for the executors: error codes, `CALL {}` subqueries
//! and parameter substitution in every clause that can hold one.

use super::*;
use crate::graph::PropertyValue;
use crate::query::error_code as codes;
use crate::query::{BoundParams, QueryEngine};

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    for i in 1..=3i64 {
        let n = s.create_node("P");
        s.set_column_property(n, "i", PropertyValue::Integer(i));
    }
    s
}

fn params(pairs: &[(&str, PropertyValue)]) -> BoundParams {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn int(i: i64) -> PropertyValue {
    PropertyValue::Integer(i)
}

fn list(xs: &[i64]) -> PropertyValue {
    PropertyValue::Array(xs.iter().map(|x| int(*x)).collect())
}

/// One column of a result, as property values.
fn col(batch: &RecordBatch, name: &str) -> Vec<PropertyValue> {
    batch
        .records
        .iter()
        .map(|r| match r.get(name) {
            Some(Value::Property(p)) => p.clone(),
            Some(Value::Null) | None => PropertyValue::Null,
            Some(other) => panic!("column {name}: not a property: {other:?}"),
        })
        .collect()
}

fn read(q: &str, p: &BoundParams) -> RecordBatch {
    let s = store();
    QueryEngine::new()
        .execute_with_params(q, &s, p)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn read_err(q: &str, p: &BoundParams) -> String {
    let s = store();
    match QueryEngine::new().execute_with_params(q, &s, p) {
        Ok(b) => panic!("{q}: expected an error, got {} rows", b.records.len()),
        Err(e) => e.to_string(),
    }
}

fn write(s: &mut GraphStore, q: &str, p: &BoundParams) -> RecordBatch {
    QueryEngine::new()
        .execute_mut_with_params(q, s, "default", p)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn sorted(mut v: Vec<PropertyValue>) -> Vec<PropertyValue> {
    v.sort_by(crate::graph::property::cypher_order);
    v
}

// ---------------------------------------------------------------------------
// ExecutionError
// ---------------------------------------------------------------------------

#[test]
fn every_execution_error_has_a_code() {
    let cases: Vec<(ExecutionError, &str)> = vec![
        (ExecutionError::GraphError("g".into()), codes::GRAPH_ACCESS),
        (ExecutionError::PlanningError("p".into()), codes::PLANNING),
        (ExecutionError::RuntimeError("r".into()), codes::RUNTIME),
        (ExecutionError::TypeError("t".into()), codes::TYPE_MISMATCH),
        (
            ExecutionError::VariableNotFound("v".into()),
            codes::VARIABLE_NOT_BOUND,
        ),
        (
            ExecutionError::VariableNotFoundInScope {
                name: "v".into(),
                in_scope: "a, b".into(),
            },
            codes::VARIABLE_NOT_BOUND,
        ),
        (
            ExecutionError::EntityNotFound("Node(1)".into()),
            codes::ENTITY_DELETED,
        ),
        (
            ExecutionError::ConstraintVerificationFailed("c".into()),
            codes::CONSTRAINT,
        ),
        (
            ExecutionError::unknown_function("f"),
            codes::UNKNOWN_FUNCTION,
        ),
        (
            ExecutionError::unknown_procedure("p"),
            codes::UNKNOWN_PROCEDURE,
        ),
        (
            ExecutionError::unknown_algorithm("a"),
            codes::UNKNOWN_ALGORITHM,
        ),
        (ExecutionError::bad_argument("b"), codes::BAD_ARGUMENT),
        (
            ExecutionError::aggregate_misuse("m"),
            codes::AGGREGATE_MISUSE,
        ),
        (ExecutionError::write_in_read("w"), codes::WRITE_IN_READ),
    ];
    for (e, code) in cases {
        assert_eq!(e.code(), code, "{e:?}");
        assert!(e.to_string().starts_with(&format!("[{code}]")), "{e}");
    }
    let scoped = ExecutionError::VariableNotFoundInScope {
        name: "v".into(),
        in_scope: "a, b".into(),
    };
    assert!(scoped.to_string().contains("v (in scope: a, b)"));
    assert!(ExecutionError::EntityNotFound("Node(1)".into())
        .to_string()
        .contains("Node(1) was deleted in this query"));
}

// ---------------------------------------------------------------------------
// CALL {} subqueries
// ---------------------------------------------------------------------------

#[test]
fn a_call_subquery_alone_returns_its_own_rows() {
    let b = read("CALL { MATCH (n:P) RETURN n.i AS i }", &BoundParams::new());
    assert_eq!(sorted(col(&b, "i")), vec![int(1), int(2), int(3)]);
}

#[test]
fn a_call_subquery_is_projected_and_filtered_by_the_outer_query() {
    let b = read(
        "CALL { MATCH (n:P) RETURN n.i AS i } WHERE i > 1 RETURN i",
        &BoundParams::new(),
    );
    assert_eq!(sorted(col(&b, "i")), vec![int(2), int(3)]);
}

#[test]
fn outer_projection_names_default_from_the_expression() {
    let b = read(
        "CALL { MATCH (n:P) RETURN n AS n, n.i AS i } RETURN n.i, i",
        &BoundParams::new(),
    );
    assert_eq!(b.columns, vec!["n.i".to_string(), "i".to_string()]);
    assert_eq!(sorted(col(&b, "n.i")), vec![int(1), int(2), int(3)]);
}

#[test]
fn outer_distinct_deduplicates_subquery_rows() {
    let b = read(
        "CALL { UNWIND [1, 1, 2] AS x RETURN x } RETURN DISTINCT x",
        &BoundParams::new(),
    );
    assert_eq!(sorted(col(&b, "x")), vec![int(1), int(2)]);
}

#[test]
fn a_call_subquery_followed_by_match_is_refused() {
    let msg = read_err(
        "CALL { RETURN 1 AS x } MATCH (n:P) RETURN n",
        &BoundParams::new(),
    );
    assert!(msg.contains("followed by MATCH is not supported"), "{msg}");
}

#[test]
fn writes_inside_a_call_subquery_are_refused() {
    let msg = read_err(
        "CALL { CREATE (n:P) RETURN n } RETURN n",
        &BoundParams::new(),
    );
    assert!(msg.contains("writes inside a CALL {} subquery"), "{msg}");
}

#[test]
fn the_write_executor_runs_a_read_only_call_subquery() {
    let mut s = store();
    let b = write(
        &mut s,
        "CALL { MATCH (n:P) RETURN n.i AS i } RETURN i",
        &BoundParams::new(),
    );
    assert_eq!(sorted(col(&b, "i")), vec![int(1), int(2), int(3)]);
}

#[test]
fn a_call_subquery_streams_through_the_sink() {
    let s = store();
    let engine = QueryEngine::new();
    let mut rows = 0;
    let out = engine
        .execute_streaming_with_params(
            "CALL { MATCH (n:P) RETURN n.i AS i } RETURN i",
            &s,
            &BoundParams::new(),
            10,
            &mut |_, recs| {
                rows += recs.len();
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(out.rows, 3);
    assert_eq!(rows, 3);
}

// ---------------------------------------------------------------------------
// Parameter substitution
// ---------------------------------------------------------------------------

#[test]
fn a_missing_parameter_is_named() {
    let msg = read_err("RETURN $nope AS x", &BoundParams::new());
    assert!(msg.contains("Unresolved parameter: $nope"), "{msg}");
}

#[test]
fn parameters_inside_expressions_of_every_shape() {
    let p = params(&[
        ("a", int(2)),
        ("xs", list(&[1, 2, 3])),
        ("k", PropertyValue::String("x".into())),
    ]);
    let b = read(
        "RETURN CASE $a WHEN 2 THEN $a + 1 ELSE 0 END AS c, \
                CASE WHEN $a > 1 THEN 'big' END AS c2, \
                $xs[$a] AS idx, \
                $xs[0..$a] AS sl, \
                [x IN $xs WHERE x > $a | x * $a] AS comp, \
                all(x IN $xs WHERE x > 0) AS allpos, \
                reduce(s = $a, x IN $xs | s + x) AS red, \
                -$a AS neg, \
                {k: $k} AS m, \
                [$a, $a] AS l",
        &p,
    );
    let r = &b.records[0];
    let get = |n: &str| match r.get(n) {
        Some(Value::List(items)) => PropertyValue::Array(
            items
                .iter()
                .map(|v| v.as_property().cloned().unwrap_or(PropertyValue::Null))
                .collect(),
        ),
        Some(Value::Map(m)) => PropertyValue::Map(
            m.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_property().cloned().unwrap_or(PropertyValue::Null),
                    )
                })
                .collect(),
        ),
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{n}: {other:?}"),
    };
    assert_eq!(get("c"), int(3));
    assert_eq!(get("c2"), PropertyValue::String("big".into()));
    assert_eq!(get("idx"), int(3));
    assert_eq!(get("sl"), list(&[1, 2]));
    assert_eq!(get("comp"), list(&[6]));
    assert_eq!(get("allpos"), PropertyValue::Boolean(true));
    assert_eq!(get("red"), int(8));
    assert_eq!(get("neg"), int(-2));
    assert_eq!(
        r.get("l"),
        Some(&Value::List(vec![
            Value::Property(int(2)),
            Value::Property(int(2))
        ]))
    );
    match get("m") {
        PropertyValue::Map(m) => assert_eq!(m.get("k"), Some(&PropertyValue::String("x".into()))),
        other => panic!("{other:?}"),
    }
}

#[test]
fn parameters_in_patterns_where_with_and_order() {
    let p = params(&[("i", int(2)), ("lim", int(1)), ("min", int(1))]);
    let b = read(
        "MATCH (n:P {i: $i}) WITH n.i AS v WHERE v >= $min \
         RETURN v ORDER BY v + $min SKIP 0 LIMIT $lim",
        &p,
    );
    assert_eq!(col(&b, "v"), vec![int(2)]);
}

#[test]
fn parameters_in_with_row_counts_and_ordering() {
    let p = params(&[("s", int(1)), ("l", int(1)), ("m", int(0))]);
    let b = read(
        "MATCH (n:P) WITH n ORDER BY n.i + $m SKIP $s LIMIT $l RETURN n.i AS i",
        &p,
    );
    assert_eq!(col(&b, "i"), vec![int(2)]);
}

#[test]
fn a_bad_deferred_row_count_is_a_type_error() {
    let p = params(&[("l", int(-1))]);
    let msg = read_err("MATCH (n:P) RETURN n LIMIT $l", &p);
    assert!(msg.contains("non-negative whole number"), "{msg}");
    let p = params(&[("l", PropertyValue::Float(2.0))]);
    let b = read("MATCH (n:P) RETURN n.i AS i LIMIT $l", &p);
    assert_eq!(b.records.len(), 2);
}

#[test]
fn parameters_in_unwind_and_exists_and_pattern_comprehension() {
    let p = params(&[("rows", list(&[1, 2, 3])), ("i", int(1))]);
    let b = read("UNWIND $rows AS r RETURN sum(r) AS s", &p);
    assert_eq!(col(&b, "s"), vec![int(6)]);

    let mut s = store();
    write(
        &mut s,
        "MATCH (a:P {i: 1}), (b:P {i: 2}) CREATE (a)-[:T {w: 5}]->(b)",
        &BoundParams::new(),
    );
    let b = QueryEngine::new()
        .execute_with_params(
            "MATCH (a:P) WHERE EXISTS { MATCH (a)-[:T {w: $i}]->(b) WHERE b.i > $i } \
             RETURN a.i AS i",
            &s,
            &params(&[("i", int(5))]),
        )
        .unwrap();
    assert!(b.records.is_empty(), "b.i is 2, not above 5");
    let b = QueryEngine::new()
        .execute_with_params(
            "MATCH (a:P {i: $i}) RETURN [(a)-[:T {w: $w}]->(b) WHERE b.i > $i | b.i + $i] AS xs",
            &s,
            &params(&[("i", int(1)), ("w", int(5))]),
        )
        .unwrap();
    assert_eq!(col(&b, "xs"), vec![list(&[3])]);
}

#[test]
fn parameters_in_union_branches() {
    let p = params(&[("a", int(1)), ("b", int(2))]);
    let b = read("RETURN $a AS x UNION ALL RETURN $b AS x", &p);
    assert_eq!(sorted(col(&b, "x")), vec![int(1), int(2)]);
}

#[test]
fn parameters_in_create_set_and_delete() {
    let mut s = GraphStore::new();
    let p = params(&[("v", int(7)), ("w", int(9))]);
    write(
        &mut s,
        "CREATE (n:Q {v: $v})-[:R {w: $w}]->(m:Q {v: $w})",
        &p,
    );
    let b = write(
        &mut s,
        "MATCH (n:Q {v: $v}) SET n.v = $v + 1 RETURN n.v AS v",
        &p,
    );
    assert_eq!(col(&b, "v"), vec![int(8)]);
    let b = write(
        &mut s,
        "MATCH (n:Q) SET n += {tag: 7} RETURN n.tag AS t",
        &p,
    );
    assert_eq!(col(&b, "t"), vec![int(7), int(7)]);
    write(
        &mut s,
        "MATCH (n:Q) WITH collect(n) AS ns DETACH DELETE ns[$i]",
        &params(&[("i", int(0))]),
    );
    let b = write(
        &mut s,
        "MATCH (n:Q) RETURN count(n) AS c",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "c"), vec![int(1)]);
}

#[test]
fn parameters_in_merge_actions() {
    let mut s = GraphStore::new();
    let p = params(&[("k", int(1)), ("c", int(10)), ("m", int(20))]);
    let q = "MERGE (n:M {k: $k}) ON CREATE SET n.v = $c ON MATCH SET n.v = $m RETURN n.v AS v";
    assert_eq!(col(&write(&mut s, q, &p), "v"), vec![int(10)]);
    assert_eq!(col(&write(&mut s, q, &p), "v"), vec![int(20)]);
}

#[test]
#[ignore = "bug: SET n += {k: <non-literal>} is refused with 'expects a map ... got Map(...)': a map expression evaluates to Value::Map, which the entity SET does not accept"]
fn set_plus_equals_a_map_holding_a_parameter() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:Q)", &BoundParams::new());
    let b = write(
        &mut s,
        "MATCH (n:Q) SET n += {tag: $v} RETURN n.tag AS t",
        &params(&[("v", int(7))]),
    );
    assert_eq!(col(&b, "t"), vec![int(7)]);
}

#[test]
#[ignore = "bug: MERGE ... ON MATCH SET n += {extra: 10} silently sets nothing: RETURN n.extra is null (literal map as well as a parameterised one)"]
fn merge_on_match_plus_equals_a_map() {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:M {k: 1})", &BoundParams::new());
    let b = write(
        &mut s,
        "MERGE (n:M {k: 1}) ON MATCH SET n += {extra: 10} RETURN n.extra AS e",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "e"), vec![int(10)]);
    let b = write(
        &mut s,
        "MATCH (n:M) RETURN n.extra AS e",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "e"), vec![int(10)]);
}

#[test]
fn parameters_in_foreach_bodies() {
    let mut s = GraphStore::new();
    let p = params(&[("xs", list(&[1, 2])), ("tag", int(5))]);
    write(
        &mut s,
        "FOREACH (x IN $xs | CREATE (:F {v: x, tag: $tag}) \
         FOREACH (y IN [$tag] | MERGE (:G {v: y})))",
        &p,
    );
    let b = write(
        &mut s,
        "MATCH (f:F) RETURN sum(f.v) AS s, sum(f.tag) AS t",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "s"), vec![int(3)]);
    assert_eq!(col(&b, "t"), vec![int(10)]);
    let b = write(
        &mut s,
        "MATCH (g:G) RETURN count(g) AS c",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "c"), vec![int(1)]);
    write(
        &mut s,
        "MATCH (f:F) WITH collect(f) AS fs FOREACH (n IN fs | SET n.v = $tag REMOVE n.tag)",
        &p,
    );
    let b = write(
        &mut s,
        "MATCH (f:F) RETURN sum(f.v) AS s, count(f.tag) AS t",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "s"), vec![int(10)]);
    assert_eq!(col(&b, "t"), vec![int(0)]);
}

#[test]
fn parameters_in_the_clause_pipeline() {
    let mut s = GraphStore::new();
    let p = params(&[("a", int(1)), ("b", int(2)), ("xs", list(&[3]))]);
    let b = write(
        &mut s,
        "CREATE (a:Z {v: $a}) WITH a MERGE (m:Z {v: $b}) WITH a, m \
         UNWIND $xs AS x MATCH (z:Z) WHERE z.v <= $b SET z.w = x + $a RETURN z.v AS v, z.w AS w",
        &p,
    );
    assert_eq!(sorted(col(&b, "v")), vec![int(1), int(2)]);
    assert_eq!(col(&b, "w"), vec![int(4), int(4)]);
    let b = write(
        &mut s,
        "CREATE (t:Z {v: $a}) WITH t MATCH (z:Z {v: $a}) DELETE z",
        &p,
    );
    assert!(b.records.is_empty());
    let b = write(
        &mut s,
        "MATCH (z:Z) RETURN count(z) AS c",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "c"), vec![int(1)]);
}

#[test]
fn parameters_as_procedure_arguments() {
    let s = store();
    let b = QueryEngine::new()
        .execute_with_params(
            "CALL db.labels() YIELD label WHERE label = $l RETURN label",
            &s,
            &params(&[("l", PropertyValue::String("P".into()))]),
        )
        .unwrap();
    assert_eq!(col(&b, "label"), vec![PropertyValue::String("P".into())]);
}

#[test]
fn a_deadline_already_past_times_the_query_out() {
    let s = store();
    let q = crate::query::parse_query("UNWIND range(1, 100) AS x RETURN x").unwrap();
    let err = QueryExecutor::new(&s)
        .with_deadline(std::time::Instant::now())
        .execute(&q)
        .unwrap_err();
    assert!(err.to_string().contains("timed out"), "{err}");
}

#[test]
fn parameters_after_a_with_and_across_extra_stages() {
    let p = params(&[("a", int(1)), ("xs", list(&[10, 20]))]);
    let b = read(
        "MATCH (n:P) WITH n MATCH (m:P) WHERE m.i = $a RETURN m.i AS i",
        &p,
    );
    assert_eq!(sorted(col(&b, "i")), vec![int(1), int(1), int(1)]);
    let b = read(
        "MATCH (n:P) WITH n WHERE n.i >= $a UNWIND $xs AS x \
         MATCH (m:P {i: $a}) WHERE m.i = $a WITH n, x, m RETURN count(*) AS c",
        &p,
    );
    assert_eq!(col(&b, "c"), vec![int(6)]);
}

#[test]
fn parameters_reach_load_csv_and_procedure_arguments_before_they_run() {
    let p = params(&[
        ("src", PropertyValue::String("file:///x.csv".into())),
        ("x", int(1)),
    ]);
    // Substituted first: the failure is about the clause, not the parameter.
    let msg = read_err("LOAD CSV FROM $src AS row RETURN row", &p);
    assert!(!msg.contains("Unresolved parameter"), "{msg}");
    assert!(msg.contains("LOAD CSV"), "{msg}");
    let msg = read_err("CALL nosuch.proc($x) YIELD y RETURN y", &p);
    assert!(!msg.contains("Unresolved parameter"), "{msg}");
    let mut s = store();
    let err = QueryEngine::new()
        .execute_mut_with_params(
            "CREATE (a) WITH a CALL nosuch.proc($x) YIELD y RETURN y",
            &mut s,
            "default",
            &p,
        )
        .unwrap_err()
        .to_string();
    assert!(!err.contains("Unresolved parameter"), "{err}");
}

#[test]
fn parameters_in_a_call_subquery_and_a_pipeline_remove() {
    let b = read(
        "CALL { RETURN $a AS x } RETURN x",
        &params(&[("a", int(4))]),
    );
    assert_eq!(col(&b, "x"), vec![int(4)]);
    let mut s = GraphStore::new();
    let b = write(
        &mut s,
        "CREATE (a:Z {v: $a}) WITH a REMOVE a.v RETURN a.v AS v",
        &params(&[("a", int(1))]),
    );
    assert_eq!(col(&b, "v"), vec![PropertyValue::Null]);
}

fn foreach_delete_leaves(list_expr: &str, p: &BoundParams) -> PropertyValue {
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:D), (:D), (:D)", &BoundParams::new());
    write(
        &mut s,
        &format!("MATCH (n:D) WITH collect(n) AS ns FOREACH (x IN {list_expr} | DETACH DELETE x)"),
        p,
    );
    let b = write(
        &mut s,
        "MATCH (n:D) RETURN count(n) AS c",
        &BoundParams::new(),
    );
    col(&b, "c").remove(0)
}

#[test]
fn a_foreach_body_may_delete_every_node_of_a_list() {
    assert_eq!(foreach_delete_leaves("ns", &BoundParams::new()), int(0));
    // With parameters bound, the body's DELETE goes through substitution too.
    let mut s = GraphStore::new();
    write(&mut s, "CREATE (:D), (:D)", &BoundParams::new());
    write(
        &mut s,
        "MATCH (n:D) WITH collect(n) AS ns FOREACH (x IN ns | SET x.v = $k DETACH DELETE x)",
        &params(&[("k", int(2))]),
    );
    let b = write(
        &mut s,
        "MATCH (n:D) RETURN count(n) AS c",
        &BoundParams::new(),
    );
    assert_eq!(col(&b, "c"), vec![int(0)]);
}

#[test]
#[ignore = "bug: FOREACH (x IN ns[0..2] | DETACH DELETE x) over a slice of a collected node list deletes nothing, while the same body over `ns` deletes every node"]
fn a_foreach_over_a_slice_of_nodes_deletes_them() {
    assert_eq!(
        foreach_delete_leaves("ns[0..2]", &BoundParams::new()),
        int(1)
    );
    assert_eq!(
        foreach_delete_leaves("ns[0..$k]", &params(&[("k", int(2))])),
        int(1)
    );
}
