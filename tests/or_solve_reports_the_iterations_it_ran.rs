//! `YIELD iterations` is what the solver ran, not what the caller asked for (#1443).
//!
//! # What was wrong
//!
//! ```rust,ignore
//! record.bind("iterations", Integer(max_iter as i64))   // the request, echoed
//! ```
//!
//! `max_iter` is `cfg_i("maxIterations", "max_iterations").unwrap_or(100)` — the
//! configuration the caller passed in. Ask for 50 and you got 50, whether the
//! solver ran 50 iterations or stopped at 12.
//!
//! # The issue says five solvers stop early. Measured: none do.
//!
//! `fpa`, `motlbo`, `nsga2`, `tlbo` and `mo_bmwr_family` all contain `break`,
//! but every one of those breaks is an RNG retry picking a distinct index
//! (`if j != i { break; }`) or the non-dominated sorting loop over ranks. There
//! is no convergence criterion in the suite. Swept all 31 advertised algorithm
//! names at `max_iterations: 500`: every one that runs reports
//! `history.len() == 500`.
//!
//! **So this is a correctness-by-construction fix, not a visible wrong number.**
//! `iterations` and the cap are equal today for every solver, and the value
//! reported happens to be right. It is derived from the wrong thing, and the
//! day a solver gains a stopping rule it goes wrong silently with nothing to
//! catch it.
//!
//! That is why the tests below assert the **invariant** rather than a
//! difference. A test asserting `iterations < maxIterations` cannot currently
//! fail, which makes it worth nothing.
//!
//! # The two surfaces disagreed, and the other one was right
//!
//! `/optimize/solve` has always reported `history.len()` in its `done` event
//! (`src/http/optimize.rs:470`). `OptimizationResult::history` carries one entry
//! per iteration performed. Only the Cypher surface echoed the cap.
//!
//! # What these tests pin
//!
//! `iterations` tracks `history`, never the cap, and the cap is still available
//! under its own name. Overloading one column would leave a caller unable to
//! tell "ran 50 of 50" from "ran 50 of 200".

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parse_query;

const T: &str = "default";

/// Three resources with costs, the fixture the existing or.solve tests use.
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    for (name, cost) in [("A", 10.0), ("B", 20.0), ("C", 15.0)] {
        let n = s.create_node("Resource");
        s.set_node_property(T, n, "name", name).unwrap();
        s.set_node_property(T, n, "cost", PropertyValue::Float(cost)).unwrap();
    }
    s
}

fn solve(cap: usize, algorithm: &str, yields: &str) -> Vec<(String, i64)> {
    let mut s = store();
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'Resource', property: 'allocation', \
         cost_property: 'cost', algorithm: '{algorithm}', budget: 30.0, \
         population_size: 20, max_iterations: {cap}}}) YIELD {yields}"
    ))
    .expect("parse");
    let mut ex = MutQueryExecutor::new(&mut s, T.to_string());
    let r = ex.execute(&q).expect("execute");
    assert!(!r.records.is_empty(), "or.solve returned no rows");
    yields
        .split(", ")
        .filter_map(|k| {
            r.records[0]
                .get(k)
                .and_then(|v| v.as_property())
                .and_then(|p| p.as_integer())
                .map(|i| (k.to_string(), i))
        })
        .collect()
}

#[test]
fn iterations_equals_the_length_of_the_history_it_reports() {
    // The invariant, stated against the column the caller can also read. If
    // these two ever disagree, one of them is derived from something else.
    let mut s = store();
    let q = parse_query(
        "CALL algo.or.solve({label: 'Resource', property: 'allocation', \
         cost_property: 'cost', algorithm: 'Jaya', budget: 30.0, \
         population_size: 20, max_iterations: 40}) YIELD iterations, history",
    )
    .expect("parse");
    let mut ex = MutQueryExecutor::new(&mut s, T.to_string());
    let r = ex.execute(&q).expect("execute");

    let iterations = r.records[0]
        .get("iterations").expect("iterations column")
        .as_property().expect("property").as_integer().expect("integer");
    let history_len = match r.records[0].get("history").expect("history column").as_property() {
        Some(PropertyValue::Array(v)) => v.len() as i64,
        other => panic!("history was not an array: {other:?}"),
    };
    assert_eq!(
        iterations, history_len,
        "iterations must be the number of entries history carries"
    );
}

#[test]
fn iterations_never_exceeds_the_cap() {
    for cap in [5usize, 25, 60] {
        let v = solve(cap, "Jaya", "iterations, max_iterations");
        let it = v.iter().find(|(n, _)| n == "iterations").unwrap().1;
        assert!(
            it <= cap as i64,
            "ran {it} iterations under a cap of {cap}"
        );
        assert!(it > 0, "a solve that produced a result ran no iterations");
    }
}

#[test]
fn the_cap_is_still_reportable_under_its_own_name() {
    // Reporting the run count is not a reason to lose the cap: "ran 12" and
    // "ran 12 of 200" are different answers to a caller comparing solvers.
    let v = solve(137, "Jaya", "max_iterations");
    assert_eq!(v, vec![("max_iterations".to_string(), 137)]);
}
