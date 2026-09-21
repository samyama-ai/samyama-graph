//! `percentileCont(x, q)` answers the same question in `WITH` as in `RETURN`.
//!
//! It did not. `WITH percentileCont(n.v, 0.9) AS p RETURN p` returned the
//! **median** — the state's initial 0.5 — while the same call in a `RETURN`
//! returned the ninetieth percentile. One function, two clauses, two answers,
//! and no error.
//!
//! This is #871 in the aggregation path that fix did not reach.
//! `AggregateOperator` has four loops that build aggregator states and all
//! four read the percentile argument; `WithBarrierOperator` has one that did
//! not.
//!
//! The tests assert the **two clauses agree**, not that each returns a
//! particular number. A test pinning `RETURN` to 269.1 and `WITH` to 269.1
//! would pass if both drifted together, and the defect here was precisely that
//! two code paths answered one question differently.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

fn engine() -> QueryEngine {
    QueryEngine::new()
}

/// `v` running 0..n-1, so the exact percentile is arithmetic.
fn ramp(n: i64) -> GraphStore {
    let mut g = GraphStore::new();
    let e = engine();
    for i in 0..n {
        e.execute_mut(
            &format!("CREATE (:N {{v: {i}, k: {}}})", i % 3),
            &mut g,
            "default",
        )
        .unwrap();
    }
    g
}

fn number(store: &GraphStore, q: &str) -> f64 {
    let batch = engine()
        .execute(q, store)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
    let v = batch
        .records
        .first()
        .and_then(|r| r.values().next().cloned())
        .unwrap_or_else(|| panic!("{q}: no rows"));
    match v {
        samyama::query::executor::record::Value::Property(PropertyValue::Float(f)) => f,
        samyama::query::executor::record::Value::Property(PropertyValue::Integer(i)) => i as f64,
        other => panic!("{q}: expected a number, got {other:?}"),
    }
}

#[test]
fn the_two_clauses_agree_for_every_percentile() {
    let g = ramp(300);
    for q in ["0.0", "0.25", "0.5", "0.75", "0.9", "0.99", "1.0"] {
        let in_return = number(
            &g,
            &format!("MATCH (n:N) RETURN percentileCont(n.v, {q}) AS p"),
        );
        let in_with = number(
            &g,
            &format!("MATCH (n:N) WITH percentileCont(n.v, {q}) AS p RETURN p"),
        );
        assert_eq!(
            in_return, in_with,
            "q={q}: RETURN said {in_return}, WITH said {in_with}"
        );
    }
}

#[test]
fn the_with_form_is_not_always_the_median() {
    // The specific symptom, pinned on its own: every `WITH percentileCont`
    // returned the median. If the fix is reverted, `p90` and `p50` become
    // equal and the agreement test above also fails — but this one says why.
    let g = ramp(300);
    let p50 = number(
        &g,
        "MATCH (n:N) WITH percentileCont(n.v, 0.5) AS p RETURN p",
    );
    let p90 = number(
        &g,
        "MATCH (n:N) WITH percentileCont(n.v, 0.9) AS p RETURN p",
    );
    assert!(
        p90 > p50 * 1.5,
        "WITH p50={p50} p90={p90}: the percentile argument is being ignored"
    );
}

#[test]
fn percentile_disc_agrees_across_the_two_clauses_too() {
    // `percentileDisc` shares the state and the loop, so it had the same bug
    // and would have kept it if only `percentileCont` were tested.
    let g = ramp(300);
    for q in ["0.1", "0.5", "0.95"] {
        let in_return = number(
            &g,
            &format!("MATCH (n:N) RETURN percentileDisc(n.v, {q}) AS p"),
        );
        let in_with = number(
            &g,
            &format!("MATCH (n:N) WITH percentileDisc(n.v, {q}) AS p RETURN p"),
        );
        assert_eq!(in_return, in_with, "q={q}");
    }
}

#[test]
fn the_two_clauses_agree_when_grouped() {
    // A different loop again: grouped aggregation inside a WITH. The states
    // are built per group, so a fix applied only to the ungrouped path would
    // leave this one answering the median.
    let g = ramp(300);
    let grouped_with = engine()
        .execute(
            "MATCH (n:N) WITH n.k AS k, percentileCont(n.v, 0.9) AS p RETURN k, p ORDER BY k",
            &g,
        )
        .unwrap();
    let grouped_return = engine()
        .execute(
            "MATCH (n:N) RETURN n.k AS k, percentileCont(n.v, 0.9) AS p ORDER BY k",
            &g,
        )
        .unwrap();

    assert_eq!(grouped_with.records.len(), 3);
    assert_eq!(grouped_return.records.len(), 3);
    for (a, b) in grouped_with.records.iter().zip(&grouped_return.records) {
        assert_eq!(
            format!("{:?}", a.get("p")),
            format!("{:?}", b.get("p")),
            "grouped WITH and RETURN disagree"
        );
    }
}

#[test]
fn an_out_of_range_percentile_is_still_refused_in_a_with() {
    // The validation lives in the same call that was missing, so before the
    // fix a WITH accepted 1.5 and quietly answered the median.
    let g = ramp(10);
    let err = engine()
        .execute(
            "MATCH (n:N) WITH percentileCont(n.v, 1.5) AS p RETURN p",
            &g,
        )
        .expect_err("1.5 is not a percentile");
    assert!(
        format!("{err}").contains("between 0.0 and 1.0"),
        "got: {err}"
    );
}
