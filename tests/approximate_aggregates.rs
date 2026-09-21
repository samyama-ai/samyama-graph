//! `approx.countDistinct` and `approx.percentile` through Cypher (NDS-10).
//!
//! Every test compares the approximate answer against the **exact** one
//! computed by the engine's own `count(DISTINCT …)` and `percentileCont(…)`.
//! An approximate aggregate checked against a hardcoded expectation is
//! checked against whatever it happened to return the day it was written; the
//! exact aggregate is the oracle, and it is already here.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

fn engine() -> QueryEngine {
    QueryEngine::new()
}

fn write(store: &mut GraphStore, q: &str) {
    engine()
        .execute_mut(q, store, "default")
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn scalar(store: &GraphStore, q: &str) -> PropertyValue {
    let batch = engine()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let v = batch
        .records
        .first()
        .and_then(|r| r.values().next().cloned())
        .unwrap_or_else(|| panic!("{q}: no rows"));
    match v {
        samyama::query::executor::record::Value::Property(p) => p,
        samyama::query::executor::record::Value::Null => PropertyValue::Null,
        other => panic!("{q}: expected a scalar, got {other:?}"),
    }
}

fn as_f64(v: &PropertyValue) -> f64 {
    match v {
        PropertyValue::Integer(i) => *i as f64,
        PropertyValue::Float(f) => *f,
        other => panic!("not a number: {other:?}"),
    }
}

/// `n` nodes whose `x` cycles through `distinct` values.
fn store_with(n: i64, distinct: i64) -> GraphStore {
    let mut g = GraphStore::new();
    for i in 0..n {
        write(
            &mut g,
            &format!("CREATE (:N {{x: {}, v: {}}})", i % distinct, i),
        );
    }
    g
}

#[test]
fn the_approximate_distinct_count_tracks_the_exact_one() {
    // Small cardinalities must be near-exact or the feature is unusable: a
    // distinct count of 12,000 on a table with 3 rows is noticed immediately
    // and then distrusted forever.
    for (n, distinct) in [(10, 3), (200, 50), (1000, 400)] {
        let g = store_with(n, distinct);
        let exact = as_f64(&scalar(&g, "MATCH (n:N) RETURN count(DISTINCT n.x) AS c"));
        let approx = as_f64(&scalar(
            &g,
            "MATCH (n:N) RETURN approx.countDistinct(n.x) AS c",
        ));
        assert_eq!(
            exact, distinct as f64,
            "the oracle itself is wrong, so this test proves nothing"
        );
        let err = (approx - exact).abs() / exact;
        assert!(
            err <= 0.02,
            "n={n} distinct={distinct}: exact {exact}, approx {approx}, error {:.2}%",
            err * 100.0
        );
    }
}

#[test]
fn it_is_an_estimate_and_not_the_exact_count_by_another_name() {
    // The counterpart. If `approx.countDistinct` just collected a set it would
    // pass every accuracy test above and defeat the point — the whole reason
    // for the sketch is bounded memory. 16,384 one-byte registers cannot
    // represent 30,000 distinct values exactly, so at this size the estimate
    // must differ from the truth at least sometimes.
    let g = store_with(30_000, 30_000);
    let exact = as_f64(&scalar(&g, "MATCH (n:N) RETURN count(DISTINCT n.x) AS c"));
    let approx = as_f64(&scalar(
        &g,
        "MATCH (n:N) RETURN approx.countDistinct(n.x) AS c",
    ));
    assert_eq!(exact, 30_000.0);
    assert_ne!(
        approx, exact,
        "an exact answer at this cardinality means this is not a sketch"
    );
    let err = (approx - exact).abs() / exact;
    assert!(
        err < 0.03,
        "exact {exact}, approx {approx}, error {:.2}%",
        err * 100.0
    );
}

#[test]
fn nulls_are_not_counted_by_either_form() {
    let mut g = GraphStore::new();
    write(
        &mut g,
        "CREATE (:N {x: 1}) CREATE (:N {x: 2}) CREATE (:N {y: 9})",
    );
    let exact = scalar(&g, "MATCH (n:N) RETURN count(DISTINCT n.x) AS c");
    let approx = scalar(&g, "MATCH (n:N) RETURN approx.countDistinct(n.x) AS c");
    assert_eq!(as_f64(&exact), 2.0);
    assert_eq!(
        as_f64(&approx),
        2.0,
        "a missing property must not become a third distinct value"
    );
}

#[test]
fn the_approximate_percentile_tracks_the_exact_one() {
    let mut g = GraphStore::new();
    for i in 1..=2000 {
        write(&mut g, &format!("CREATE (:N {{v: {i}}})"));
    }
    for q in ["0.5", "0.9", "0.95", "0.99"] {
        let exact = as_f64(&scalar(
            &g,
            &format!("MATCH (n:N) RETURN percentileCont(n.v, {q}) AS p"),
        ));
        let approx = as_f64(&scalar(
            &g,
            &format!("MATCH (n:N) RETURN approx.percentile(n.v, {q}) AS p"),
        ));
        let err = (approx - exact).abs() / exact;
        assert!(
            err < 0.02,
            "q={q}: exact {exact}, approx {approx}, error {:.3}%",
            err * 100.0
        );
    }
}

#[test]
fn the_percentile_argument_is_read_and_not_defaulted_to_the_median() {
    // #871: `percentileCont` dropped its second argument and returned the
    // median for every call. The approximate form shares that argument's
    // validation path precisely so it cannot inherit a second copy of the bug.
    let mut g = GraphStore::new();
    for i in 1..=1000 {
        write(&mut g, &format!("CREATE (:N {{v: {i}}})"));
    }
    let p50 = as_f64(&scalar(
        &g,
        "MATCH (n:N) RETURN approx.percentile(n.v, 0.5) AS p",
    ));
    let p99 = as_f64(&scalar(
        &g,
        "MATCH (n:N) RETURN approx.percentile(n.v, 0.99) AS p",
    ));
    assert!(
        p99 > p50 * 1.5,
        "p50={p50} p99={p99}: the percentile argument is being ignored"
    );
}

#[test]
fn an_out_of_range_percentile_is_an_error_not_a_clamped_answer() {
    let g = store_with(10, 10);
    let err = engine()
        .execute("MATCH (n:N) RETURN approx.percentile(n.v, 1.5) AS p", &g)
        .expect_err("1.5 is not a percentile");
    let msg = format!("{err}");
    assert!(
        msg.contains("between 0.0 and 1.0"),
        "the error must say the range; got: {msg}"
    );
}

#[test]
fn no_rows_gives_null_rather_than_zero() {
    // Zero is a value the data might have had.
    let g = GraphStore::new();
    assert_eq!(
        scalar(&g, "MATCH (n:Nope) RETURN approx.percentile(n.v, 0.5) AS p"),
        PropertyValue::Null
    );
    assert_eq!(
        as_f64(&scalar(
            &g,
            "MATCH (n:Nope) RETURN approx.countDistinct(n.x) AS c"
        )),
        0.0,
        "no distinct values is a count of zero, unlike a percentile of nothing"
    );
}

#[test]
fn both_work_grouped_by_a_key() {
    // An aggregate that only worked ungrouped would pass every test above.
    let mut g = GraphStore::new();
    for i in 0..300 {
        write(&mut g, &format!("CREATE (:N {{grp: {}, x: {}}})", i % 3, i));
    }
    let batch = engine()
        .execute(
            "MATCH (n:N) RETURN n.grp AS g, approx.countDistinct(n.x) AS c \
             ORDER BY g",
            &g,
        )
        .unwrap();
    assert_eq!(batch.records.len(), 3, "one row per group");
    for r in &batch.records {
        let c = match r.get("c") {
            Some(samyama::query::executor::record::Value::Property(p)) => as_f64(p),
            other => panic!("{other:?}"),
        };
        assert!(
            (c - 100.0).abs() <= 2.0,
            "each group holds 100 distinct values, got {c}"
        );
    }
}
