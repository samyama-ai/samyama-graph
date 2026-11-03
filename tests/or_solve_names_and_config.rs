//! `algo.or.solve` must run what it was asked for, or say it cannot (#1341).
//!
//! The dispatch ended in `_ => JayaSolver`, and the result record bound the
//! *requested* name. So `{algorithm: 'PSO'}` ran Jaya and came back saying
//! `algorithm: "PSO"`. Three runs of Jaya labelled PSO, DE and Jaya compare
//! identically and claim to compare three algorithms — the wrong-answer shape,
//! with the result actively asserting the wrong thing rather than merely
//! omitting it.
//!
//! The config half is #1316's defect in the one function that never got the
//! guard: every other algorithm call here takes camelCase, this one read
//! snake_case, so `{maxIterations: 5}` ran a hundred iterations and reported
//! success.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn store_with_vars() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:Var {x: 0.0, c: 1.0}), (:Var {x: 0.0, c: 2.0}), (:Var {x: 0.0, c: 3.0})",
            &mut store,
            "default",
        )
        .unwrap();
    (store, engine)
}

fn solve(engine: &QueryEngine, store: &mut GraphStore, cfg: &str) -> Result<String, String> {
    let q = format!(
        "CALL algo.or.solve({{label:'Var', property:'x', costProperty:'c', {cfg}}}) \
         YIELD algorithm, fitness RETURN algorithm"
    );
    engine
        .execute_mut(&q, store, "default")
        .map(|b| format!("{} row(s)", b.records.len()))
        .map_err(|e| e.to_string())
}

#[test]
fn an_unknown_algorithm_is_refused_and_the_message_lists_the_real_ones() {
    let (mut store, engine) = store_with_vars();
    let err = solve(&engine, &mut store, "algorithm:'NoSuchSolver', maxIterations:3")
        .expect_err("an unknown solver name must not silently become Jaya");
    assert!(err.contains("NoSuchSolver"), "the message must name what was asked: {err}");
    assert!(err.contains("Jaya") && err.contains("PSO"),
            "and list what is available: {err}");
}

#[test]
fn the_solvers_that_shipped_unreachable_now_run() {
    // Each of these is implemented in samyama-optimization and was absent from
    // the dispatch, so asking for it ran Jaya. If any regresses to unknown,
    // this fails rather than quietly answering with a different algorithm.
    let (mut store, engine) = store_with_vars();
    for name in [
        "PSO", "DE", "BMR", "BWR", "BMWR", "QOJaya", "SAMPJaya", "EHRJaya", "ITLBO",
        "GOTLBO", "QORao", "SAPHR",
    ] {
        let cfg = format!("algorithm:'{name}', maxIterations:3, populationSize:6");
        assert!(
            solve(&engine, &mut store, &cfg).is_ok(),
            "{name} is implemented in the crate and must be reachable"
        );
    }
}

#[test]
fn the_names_that_always_worked_still_work() {
    let (mut store, engine) = store_with_vars();
    for name in ["Jaya", "Rao1", "TLBO", "GA", "SA", "GWO", "FPA"] {
        let cfg = format!("algorithm:'{name}', maxIterations:3, populationSize:6");
        assert!(solve(&engine, &mut store, &cfg).is_ok(), "{name} regressed");
    }
}

#[test]
fn an_unknown_config_key_is_refused() {
    let (mut store, engine) = store_with_vars();
    let err = solve(&engine, &mut store, "algorithm:'Jaya', maxIterationz:3")
        .expect_err("a misspelt key must not be ignored");
    assert!(err.contains("maxIterationz"), "the message must name the key: {err}");
}

#[test]
fn both_spellings_of_the_config_keys_are_accepted() {
    // snake_case is what this function always read; camelCase is what every
    // other algorithm call here takes. Breaking the first would break existing
    // callers, and refusing the second is what made `{maxIterations: 5}` run a
    // hundred iterations.
    let (mut store, engine) = store_with_vars();
    assert!(solve(&engine, &mut store,
                  "algorithm:'Jaya', max_iterations:3, population_size:6").is_ok());
    assert!(solve(&engine, &mut store,
                  "algorithm:'Jaya', maxIterations:3, populationSize:6").is_ok());
}
