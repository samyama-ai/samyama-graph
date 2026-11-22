//! Selectable constraint handling for the optimization solvers (OPT-10, #2266).
//!
//! ```cypher
//! CALL algo.or.solve({label: 'Item', property: 'qty', costProperty: 'cost',
//!                     minTotal: 6, budget: 10, constraintHandling: 'repair'})
//! ```
//!
//! `penalty` (objective plus penalty), `repair` (the problem's own repair
//! before evaluation) and `feasibility` (Deb's rules) can each be chosen, on
//! `algo.or.solve` and on `/optimize/solve`. Before, the penalty was the only
//! scheme, nothing selected it, and the multi-objective `or.solve` solvers ran
//! with no constraint at all. These tests check what a caller can see: the
//! three give different answers where they should, `repair` and `feasibility`
//! return feasible answers where `penalty` can trade a violation for a better
//! objective, and the written-back values obey the constraints the result
//! says they obey.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::Record;
use samyama::query::QueryEngine;
use serde_json::{json, Value as Json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tower::ServiceExt;

const T: &str = "default";

/// Four items costing 1, 2, 3 and 4 per unit, with a risk per unit for the
/// multi-objective runs.
fn items() -> (GraphStore, QueryEngine) {
    let mut s = GraphStore::new();
    for (cost, risk) in [(1.0, 4.0), (2.0, 3.0), (3.0, 2.0), (4.0, 1.0)] {
        let n = s.create_node_with_labels([Label::new("Item")]);
        s.set_node_property(T, n, "cost", PropertyValue::Float(cost))
            .unwrap();
        s.set_node_property(T, n, "risk", PropertyValue::Float(risk))
            .unwrap();
    }
    (s, QueryEngine::new())
}

fn solve(s: &mut GraphStore, engine: &QueryEngine, config: &str) -> Result<Record, String> {
    let q = format!(
        "CALL algo.or.solve({{label: 'Item', property: 'qty', lowerBound: 0, upperBound: 10, \
         {config}}}) YIELD feasible, objective RETURN feasible, objective"
    );
    let r = engine.execute_mut(&q, s, T).map_err(|e| e.to_string())?;
    Ok(r.records.into_iter().next().expect("one result row"))
}

fn feasible(r: &Record) -> bool {
    match r.get("feasible").and_then(|v| v.as_property()) {
        Some(PropertyValue::Boolean(b)) => *b,
        other => panic!("feasible was {other:?}"),
    }
}

/// `(sum qty, sum qty * cost)` of what was written back.
fn written(s: &GraphStore, engine: &QueryEngine) -> (f64, f64) {
    let r = engine
        .execute(
            "MATCH (i:Item) RETURN sum(i.qty) AS total, sum(i.qty * i.cost) AS spend",
            s,
        )
        .unwrap();
    let f = |k: &str| match r.records[0].get(k).and_then(|v| v.as_property()) {
        Some(PropertyValue::Float(x)) => *x,
        Some(PropertyValue::Integer(i)) => *i as f64,
        other => panic!("{k} was {other:?}"),
    };
    (f("total"), f("spend"))
}

const SINGLE: &str = "costProperty: 'cost', algorithm: 'Jaya', populationSize: 30, \
                      maxIterations: 150";

#[test]
fn penalty_trades_a_small_violation_for_objective_and_the_other_two_do_not() {
    // Minimise spend with at least 6 units. The penalty for a shortfall d is
    // 100 d², and the saving is d per unit of the cheapest item, so the
    // penalised optimum sits just inside the infeasible side (d = 0.005): an
    // inexact penalty, by construction. Repair and feasibility rules may not
    // return such a point.
    let mut penalty_infeasible = 0;
    for seed in 1..=5 {
        let (mut s, engine) = items();
        let cfg = format!("{SINGLE}, minTotal: 6, seed: {seed}");
        let p = solve(
            &mut s,
            &engine,
            &format!("{cfg}, constraintHandling: 'penalty'"),
        )
        .unwrap();
        if !feasible(&p) {
            penalty_infeasible += 1;
        }
        for strategy in ["repair", "feasibility"] {
            let (mut s, engine) = items();
            let r = solve(
                &mut s,
                &engine,
                &format!("{cfg}, constraintHandling: '{strategy}'"),
            )
            .unwrap();
            assert!(
                feasible(&r),
                "{strategy}, seed {seed}: returned an infeasible answer"
            );
            let (total, _) = written(&s, &engine);
            assert!(
                total >= 6.0,
                "{strategy}, seed {seed}: wrote a total of {total} < 6"
            );
        }
    }
    assert!(
        penalty_infeasible > 0,
        "the penalty was feasible on every seed, so this test no longer shows the strategies differ"
    );
}

#[test]
fn the_default_is_still_the_penalty() {
    // An absent `constraintHandling` keeps what a single-objective solve did
    // before: same seed, same answer as an explicit `penalty`.
    let cfg = format!("{SINGLE}, minTotal: 6, seed: 3");
    let (mut a, engine) = items();
    let implicit = solve(&mut a, &engine, &cfg).unwrap();
    let (mut b, engine) = items();
    let explicit = solve(
        &mut b,
        &engine,
        &format!("{cfg}, constraintHandling: 'penalty'"),
    )
    .unwrap();
    assert_eq!(implicit.get("objective"), explicit.get("objective"));
    assert_eq!(feasible(&implicit), feasible(&explicit));
}

#[test]
fn repair_is_feasible_even_from_a_tiny_search() {
    // One generation of four candidates: nothing has had time to converge,
    // so only the repair can make the answer feasible.
    for seed in 0..20 {
        let (mut s, engine) = items();
        let r = solve(
            &mut s,
            &engine,
            &format!(
                "costProperty: 'cost', algorithm: 'Jaya', populationSize: 4, maxIterations: 1, \
                 minTotal: 6, budget: 10, seed: {seed}, constraintHandling: 'repair'"
            ),
        )
        .unwrap();
        assert!(feasible(&r), "seed {seed}");
        let (total, spend) = written(&s, &engine);
        assert!(
            total >= 6.0 && spend <= 10.0,
            "seed {seed}: total {total}, spend {spend}"
        );
    }
}

#[test]
fn repair_reports_infeasible_when_no_feasible_point_exists() {
    // 6 units at the cheapest price cost 6, over a budget of 5: the repair
    // does what it can and the answer says it is not feasible.
    let (mut s, engine) = items();
    let r = solve(
        &mut s,
        &engine,
        &format!("{SINGLE}, minTotal: 6, budget: 5, seed: 1, constraintHandling: 'repair'"),
    )
    .unwrap();
    assert!(!feasible(&r));
}

#[test]
fn multi_objective_solvers_now_see_the_budget() {
    // Two objectives (spend, risk), at least 6 units, spend at most 12. Before,
    // the multi-objective solvers ran with no constraint: the budget was only
    // checked afterwards, to report `feasible`.
    for strategy in [
        "",
        ", constraintHandling: 'feasibility'",
        ", constraintHandling: 'repair'",
    ] {
        for seed in 1..=3 {
            let (mut s, engine) = items();
            let r = solve(
                &mut s,
                &engine,
                &format!(
                    "costProperties: ['cost', 'risk'], algorithm: 'NSGA2', populationSize: 30, \
                     maxIterations: 60, minTotal: 6, budget: 12, seed: {seed}{strategy}"
                ),
            )
            .unwrap();
            assert!(
                feasible(&r),
                "NSGA2{strategy}, seed {seed}: infeasible point written back"
            );
            let (total, spend) = written(&s, &engine);
            assert!(
                total >= 6.0 && spend <= 12.0,
                "NSGA2{strategy}, seed {seed}: total {total}, spend {spend}"
            );
        }
    }
}

#[test]
fn a_multi_objective_penalty_reports_the_raw_objectives() {
    // Under `penalty` the solver's objectives carry the penalty; the
    // `objective` column must be the plan's own spend and risk.
    let (mut s, engine) = items();
    let r = solve(
        &mut s,
        &engine,
        "costProperties: ['cost', 'risk'], algorithm: 'NSGA2', populationSize: 20, \
         maxIterations: 30, minTotal: 6, budget: 12, seed: 2, constraintHandling: 'penalty'",
    )
    .unwrap();
    let objective = match r.get("objective").and_then(|v| v.as_property()) {
        Some(PropertyValue::Array(a)) => a.clone(),
        other => panic!("objective was {other:?}"),
    };
    let (_, spend) = written(&s, &engine);
    match &objective[0] {
        PropertyValue::Float(f) => {
            assert!((f - spend).abs() < 1e-9, "objective {f} vs spend {spend}")
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_whole_number_limit_is_a_limit() {
    // `{budget: 10}` was read with `as_float`, which is `None` for an integer,
    // so the solve ran with no budget at all; `{budget: 10.0}` worked. Both
    // must now constrain the answer, identically.
    let mut answers = Vec::new();
    for budget in ["10", "10.0"] {
        let (mut s, engine) = items();
        let r = solve(
            &mut s,
            &engine,
            &format!(
                "costProperty: 'cost', algorithm: 'Jaya', populationSize: 10, maxIterations: 5, \
                 minTotal: 6, budget: {budget}, seed: 4, constraintHandling: 'repair'"
            ),
        )
        .unwrap();
        assert!(feasible(&r), "budget: {budget}");
        let (total, spend) = written(&s, &engine);
        assert!(
            total >= 6.0 && spend <= 10.0,
            "budget: {budget}: total {total}, spend {spend}"
        );
        answers.push(r.get("objective").cloned());
    }
    assert_eq!(
        answers[0], answers[1],
        "an integer and a float limit gave different answers"
    );

    let (mut s, engine) = items();
    let err = solve(&mut s, &engine, &format!("{SINGLE}, budget: 'ten'")).expect_err("refused");
    assert!(err.contains("`budget` must be a number"), "{err}");
}

#[test]
fn an_unknown_strategy_is_refused_with_the_choices() {
    let (mut s, engine) = items();
    for (value, expect) in [
        ("'death'", "unknown constraintHandling `death`"),
        ("3", "must be one of penalty, repair, feasibility"),
    ] {
        let err = solve(
            &mut s,
            &engine,
            &format!("{SINGLE}, constraintHandling: {value}"),
        )
        .expect_err(value);
        assert!(err.contains(expect), "{value}: {err}");
    }
}

// ---- /optimize/solve -------------------------------------------------------

fn router() -> axum::Router {
    samyama::http::build_router_for_tests(Arc::new(RwLock::new(GraphStore::new())))
}

async fn start(app: &axum::Router, body: Json) -> (StatusCode, Json) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/optimize/solve")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Json::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, v)
}

/// The `done` (or `error`) event of a solve, as `(event, data)`.
async fn finish(app: &axum::Router, body: Json) -> (String, Json) {
    let (status, v) = start(app, body).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let job = v["job_id"].as_str().unwrap().to_string();
    let res = tokio::time::timeout(Duration::from_secs(60), async {
        app.clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/optimize/solve/{job}/stream"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    })
    .await
    .expect("stream timed out");
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if let Some(event) = line.strip_prefix("event: ") {
            if event == "done" || event == "error" {
                let data = lines.next().unwrap().trim_start_matches("data: ");
                return (
                    event.to_string(),
                    serde_json::from_str(data).unwrap_or(Json::Null),
                );
            }
        }
    }
    panic!("no done or error event in: {text}");
}

#[tokio::test]
async fn uc2_repair_never_returns_an_unsafe_plan_even_from_a_tiny_search() {
    // The measurement in #2266: at population 4 x 1 generation, 14 of 30 runs
    // returned a front with an unsafe plan in it (before #2020), because about
    // 90% of random plans give both contraindicated drugs. With repair, every
    // evaluated plan is a repaired one.
    let app = router();
    for seed in 0..15u64 {
        let (event, done) = finish(
            &app,
            json!({"algorithm": "nsga2", "benchmark": "uc2_dosing", "population_size": 4,
                   "iterations": 1, "seed": seed, "constraint_handling": "repair"}),
        )
        .await;
        assert_eq!(event, "done", "{done}");
        assert_eq!(done["feasible"], json!(true), "seed {seed}: {done}");
    }
}

#[tokio::test]
async fn an_unknown_strategy_is_a_400_and_repair_needs_a_repair() {
    let app = router();
    let (status, v) = start(
        &app,
        json!({"algorithm": "nsga2", "benchmark": "uc2_dosing", "constraint_handling": "death"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        v.to_string()
            .contains("unknown constraint_handling `death`"),
        "{v}"
    );

    // `sphere` has no constraint and no repair, so `repair` is refused rather
    // than quietly run as a penalty.
    let (event, data) = finish(
        &app,
        json!({"algorithm": "jaya", "benchmark": "sphere", "population_size": 4,
               "iterations": 2, "constraint_handling": "repair"}),
    )
    .await;
    assert_eq!(event, "error", "{data}");
    assert!(data.to_string().contains("no repair operator"), "{data}");
}
