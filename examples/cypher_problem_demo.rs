//! End-to-end demo: solve a Cypher-grounded optimization problem with a
//! Rao-family solver and print cache-hit statistics.
//!
//! Problem: linear-regression-style fit. The graph holds (x, y) observations;
//! we tune two coefficients (a, b) so that the Cypher-computed sum of squared
//! errors `sum((y - a*x - b)^2)` is minimized.
//!
//! This is a clean continuous problem that exercises the full CypherProblem
//! path (graph access, parameter substitution, scalar return, memoization)
//! and has a known minimum at the OLS solution.

use ndarray::Array1;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::optimization::CypherProblem;
use samyama::query::QueryEngine;
use samyama_optimization::algorithms::BMWRSolver;
use samyama_optimization::common::{Problem, SolverConfig};
use std::sync::{Arc, RwLock};
use std::time::Instant;

fn main() {
    // Synthetic data: y = 2*x + 1 + tiny noise
    let xs = [-3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    let ys = [-5.0, -3.0, -1.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0];
    let mut g = GraphStore::new();
    for i in 0..xs.len() {
        let n = g.create_node(Label::new("Obs"));
        let nm = g.get_node_mut(n).unwrap();
        nm.set_property("x", PropertyValue::Float(xs[i]));
        nm.set_property("y", PropertyValue::Float(ys[i]));
    }

    let graph = Arc::new(RwLock::new(g));
    let engine = Arc::new(QueryEngine::new());

    // Objective: sum_i (y_i - a*x_i - b)^2
    // Decision variables: $x0 = a, $x1 = b
    let objective = "MATCH (o:Obs) \
        WITH o.y - $x0 * o.x - $x1 AS e \
        RETURN sum(e * e) AS sse";

    let problem = CypherProblem::new(
        2,
        Array1::from(vec![-10.0, -10.0]),
        Array1::from(vec![10.0, 10.0]),
        objective,
        graph,
        engine,
    );

    // Sanity check: at the true optimum (a=2, b=1), SSE should be ~0.
    let v = problem.objective(&Array1::from(vec![2.0, 1.0]));
    println!("sanity SSE at (a=2, b=1): {:.6}", v);

    let cfg = SolverConfig { population_size: 30, max_iterations: 200 };
    let t0 = Instant::now();
    let r = BMWRSolver::new(cfg).solve(&problem);
    let wall = t0.elapsed();

    let stats = problem.stats();
    println!("=== Cypher-grounded BMWR result ===");
    println!("best a, b                     : ({:.6}, {:.6})", r.best_variables[0], r.best_variables[1]);
    println!("best SSE                      : {:.6}", r.best_fitness);
    println!("wall clock                    : {:?}", wall);
    println!("cache hits / misses           : {} / {}", stats.hits, stats.misses);
    println!("cache hit rate                : {:.1}%",
        100.0 * stats.hits as f64 / (stats.hits + stats.misses).max(1) as f64);
    println!("cypher eval total time        : {} ms", stats.total_eval_ms);
    println!("avg per cypher eval           : {:.3} ms",
        stats.total_eval_ms as f64 / stats.misses.max(1) as f64);
    println!("cache size                    : {}", problem.cache_size());
}
