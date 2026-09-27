//! No name in `or.solve`'s own "Available:" list may panic or report a
//! dispatch bug (#1499).
//!
//! # What was wrong
//!
//! ```rust,ignore
//! let mut multi_costs = vec![Vec::new(); cost_props.len()];
//! ...
//! if cost_props.len() == 1 { single_costs.push(cost); }        // multi_costs stays EMPTY
//! else if !cost_props.is_empty() { ... multi_costs[i].push(cost); }
//! ...
//! if algorithm == "NSGA2" || algorithm == "MOTLBO" || cost_props.len() > 1 {
//! ```
//!
//! With exactly one cost property, `multi_costs` was allocated with one row and
//! that row was never filled. `dim` is the node count. Four multi-objective
//! algorithms, two routes, two different wrong outcomes:
//!
//! - `NSGA2` and `MOTLBO` were force-routed to the Pareto branch by name
//!   whatever the cost count, reached `MultiObjectiveProblem::objectives`, and
//!   indexed `costs[i]` for `i in 0..dim` into the empty row — **a panic on an
//!   unauthenticated query**, which with #1328 needs only reachability.
//! - `MOBMWR` and `MORaoDE` were *not* in that hardcoded pair, so they fell to
//!   the single-objective branch, which has no arm for them, and hit the #1341
//!   guard: "listed as available but has no implementation wired to it; this is
//!   a bug in the dispatch". They are implemented at `operator.rs:17369-17370`.
//!   They were unroutable, not unimplemented, and the message said otherwise.
//!
//! # What these tests pin
//!
//! Every advertised name, run through the same config. The sweep is the test:
//! a per-name test would have been written for the names somebody thought of,
//! and these four were exactly the ones nobody did.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parse_query;

const T: &str = "default";

/// Every name `or.solve` prints in its own refusal message.
const ADVERTISED: &[&str] = &[
    "Rao1", "Rao2", "Rao3", "QORao", "TLBO", "ITLBO", "GOTLBO", "MOTLBO",
    "Jaya", "QOJaya", "SAMPJaya", "EHRJaya",
    "BMR", "BWR", "BMWR", "MOBMWR",
    "PSO", "DE", "GA", "SA", "ABC", "GSA", "HS", "FPA",
    "Firefly", "Cuckoo", "GWO", "Bat",
    "NSGA2", "MORaoDE", "SAPHR",
];

fn store() -> GraphStore {
    let mut s = GraphStore::new();
    for (name, cost, value) in [("A", 10.0, 3.0), ("B", 20.0, 7.0), ("C", 15.0, 5.0)] {
        let n = s.create_node("Resource");
        s.set_node_property(T, n, "name", name).unwrap();
        s.set_node_property(T, n, "cost", PropertyValue::Float(cost)).unwrap();
        s.set_node_property(T, n, "value", PropertyValue::Float(value)).unwrap();
    }
    s
}

/// `Ok(rows)` if the solve ran, `Err(message)` if it was refused, and a panic
/// is caught and reported as such rather than taking the test binary with it.
fn solve(algorithm: &str, cost_clause: &str) -> Result<usize, String> {
    let mut s = store();
    let q = parse_query(&format!(
        "CALL algo.or.solve({{label: 'Resource', property: 'allocation', \
         {cost_clause}, algorithm: '{algorithm}', budget: 30.0, \
         population_size: 12, max_iterations: 20}}) YIELD fitness"
    ))
    .map_err(|e| format!("parse: {e}"))?;
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut ex = MutQueryExecutor::new(&mut s, T.to_string());
        ex.execute(&q)
    }));
    match out {
        Ok(Ok(r)) => Ok(r.records.len()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("PANIC".to_string()),
    }
}

#[test]
fn no_advertised_algorithm_panics_on_a_single_objective_config() {
    // `cost_property`, singular — the spelling every single-objective example
    // in the codebase uses, and the one that panicked.
    let panicked: Vec<&str> = ADVERTISED
        .iter()
        .filter(|a| solve(a, "cost_property: 'cost'") == Err("PANIC".to_string()))
        .copied()
        .collect();
    assert!(
        panicked.is_empty(),
        "these advertised algorithms panicked the executor: {panicked:?}"
    );
}

#[test]
fn no_advertised_algorithm_reports_a_dispatch_bug() {
    // The #1341 guard exists to catch a name in the list with no match arm. It
    // must never fire for a name that has one, because then it is reporting a
    // bug that is not there and hiding the one that is.
    let mut misreported = Vec::new();
    for a in ADVERTISED {
        for clause in ["cost_property: 'cost'", "cost_properties: ['cost', 'value']"] {
            if let Err(e) = solve(a, clause) {
                if e.contains("no implementation wired") {
                    misreported.push(format!("{a} with {clause}"));
                }
            }
        }
    }
    assert!(
        misreported.is_empty(),
        "these reported a dispatch bug they do not have: {misreported:?}"
    );
}

#[test]
fn every_advertised_algorithm_returns_a_row_with_one_cost_property() {
    let mut bad = Vec::new();
    for a in ADVERTISED {
        match solve(a, "cost_property: 'cost'") {
            Ok(n) if n > 0 => {}
            Ok(_) => bad.push(format!("{a}: no rows")),
            Err(e) => bad.push(format!("{a}: {e}")),
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

#[test]
fn every_advertised_algorithm_returns_a_row_with_two_cost_properties() {
    let mut bad = Vec::new();
    for a in ADVERTISED {
        match solve(a, "cost_properties: ['cost', 'value']") {
            Ok(n) if n > 0 => {}
            Ok(_) => bad.push(format!("{a}: no rows")),
            Err(e) => bad.push(format!("{a}: {e}")),
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

#[test]
fn the_advertised_list_and_the_dispatch_agree() {
    // `SOLVERS` is what the refusal message prints. If this test's copy drifts
    // from it, the sweep above stops covering what callers are told exists.
    let refusal = solve("NotAnAlgorithm", "cost_property: 'cost'")
        .expect_err("an unknown name must be refused");
    for a in ADVERTISED {
        assert!(
            refusal.contains(a),
            "`{a}` is swept here but is not in the Available list the engine prints"
        );
    }
}
