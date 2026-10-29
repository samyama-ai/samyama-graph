//! L-SHADE answers through `algo.or.solve` (OPT-13, #1803).
//!
//! OPT-13 counts the solvers a caller can reach by calling them, so a solver
//! that ships in `samyama-optimization` without a dispatch arm does not count
//! (#1341). This calls it the way a caller would, over Cypher, and checks that
//! the run is L-SHADE's and not some other solver's under its name.
//!
//! How well it searches is tested in the crate, seeded (`lshade.rs`):
//! `or.solve` takes no seed, so comparing two of its runs would be comparing
//! two draws.

use samyama::graph::GraphStore;
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;

const TENANT: &str = "default";
const ITERATIONS: usize = 40;
const POPULATION: usize = 20;

fn store() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE (:Var {x: 0.0, c: 1.0}), (:Var {x: 0.0, c: 2.0}), (:Var {x: 0.0, c: 3.0})",
            &mut store,
            TENANT,
        )
        .unwrap();
    (store, engine)
}

/// `algorithm` and `fitness` from one `or.solve` call, or its error.
fn solve(algorithm: &str) -> Result<(String, f64), String> {
    let (mut store, engine) = store();
    let q = format!(
        "CALL algo.or.solve({{label:'Var', property:'x', costProperty:'c', \
         algorithm:'{algorithm}', maxIterations:{ITERATIONS}, populationSize:{POPULATION}}}) \
         YIELD algorithm, fitness RETURN algorithm, fitness"
    );
    let batch = engine
        .execute_mut(&q, &mut store, TENANT)
        .map_err(|e| e.to_string())?;
    let row = batch.records.first().ok_or("no row")?;
    let name = match row.get("algorithm") {
        Some(Value::Property(p)) => p.as_string().map(str::to_string).unwrap_or_default(),
        other => return Err(format!("algorithm was {other:?}")),
    };
    let fitness = match row.get("fitness") {
        Some(Value::Property(p)) => p.as_float().ok_or("fitness is not a number")?,
        other => return Err(format!("fitness was {other:?}")),
    };
    Ok((name, fitness))
}

#[test]
fn lshade_runs_through_or_solve_under_its_own_name() {
    let (name, fitness) = solve("LSHADE").expect("LSHADE is reachable");
    assert_eq!(name, "LSHADE");
    assert!(fitness.is_finite(), "fitness {fitness}");
}

#[test]
fn lshade_is_listed_where_an_unknown_name_is_refused() {
    let err = solve("NoSuchSolver").expect_err("an unknown name is refused");
    assert!(
        err.contains("LSHADE"),
        "the list of solvers names it: {err}"
    );
}
