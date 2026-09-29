# Samyama Optimization Engine (Rust)

A high-performance library implementing **metaphor-less** and **nature-inspired** metaheuristic optimization algorithms.

This engine allows you to solve complex resource allocation, scheduling, and engineering problems by defining an objective function and constraints.

## Reproducibility

Every solver accepts an optional seed. Without one, behaviour is unchanged (drawn from entropy);
with one, the run can be re-derived exactly — same best fitness, same solution vector, same
convergence history.

```rust
let result = DESolver::new(config).with_seed(4242).solve(&problem);
```

This matters for published results: a reported optimum that cannot be regenerated can only be
re-sampled, which is not the same claim.

The parallel solvers are the part worth understanding. Seeding only the top-level RNG and letting
rayon workers draw from `thread_rng()` would give runs that *look* reproducible in a
single-threaded test and are not — the seed would be recorded next to results it cannot
regenerate. Each element's stream is instead derived from `(seed, iteration, index)`, so the
answer does not depend on how work is scheduled. `tests/test_reproducibility.rs` asserts this
directly: the same seed under a 1-thread pool and an 8-thread pool must give bit-identical results.

## Algorithms Supported

We support 15+ algorithms across various families:

### Metaphor-less
- **Jaya**: Parameter-less optimization (Toward best, away from worst).
- **Rao (1, 2, 3)**: Algorithms using best, worst, and mean solutions with varying interaction levels.
- **TLBO**: Teaching-Learning-Based Optimization.
- **ITLBO**: Improved TLBO with Elitism.
- **BMR / BWR**: Best-Mean-Random and Best-Worst-Random strategies.
- **QOJaya**: Quasi-Oppositional Jaya (using Opposition-Based Learning).

### Nature-Inspired
- **GWO**: Grey Wolf Optimizer (Alpha, Beta, Delta hierarchy).
- **PSO**: Particle Swarm Optimization.
- **DE**: Differential Evolution.
- **Firefly**: Firefly Algorithm (Light intensity based attraction).
- **Cuckoo**: Cuckoo Search (Levy flights and nest abandonment).
- **Bat**: Bat Algorithm (Echolocation).
- **ABC**: Artificial Bee Colony.
- **FPA**: Flower Pollination Algorithm.
- **GA**: Genetic Algorithm (Tournament selection, Uniform crossover).

### Stochastic / Physics
- **SA**: Simulated Annealing.
- **HS**: Harmony Search.
- **GSA**: Gravitational Search Algorithm.

### Multi-Objective (Pareto)
- **NSGA-II**: Non-dominated Sorting Genetic Algorithm II.
- **MOTLBO**: Multi-Objective TLBO.

## Features
- **Parallel Evaluation**: Automatic multi-threaded fitness calculation via `rayon`.
- **Zero-Copy**: Minimal overhead when operating on large vectors.
- **Constraints**: Support for penalty-based constraint handling (`min_total`, `budget`).
- **History Tracking**: Solvers yield convergence history for visualization.

## What a problem is here, and why there is no MPS/LP export

The problem model is a **black box**. `Problem` (`src/common.rs`) is an objective
function from a variable vector to a scalar, an optional scalar penalty, a dimension, and
box bounds. That is all a solver sees. Every solver in this crate is a population
metaheuristic that samples that function. Every implementor has the same shape, including
the engine's `GraphOptimizationProblem` (`src/query/executor/operator.rs`) and
`CypherProblem` (`src/optimization/cypher_problem.rs`), whose objective runs a Cypher query.

MPS and LP are file formats for a linear or mixed-integer program. They hold an objective
coefficient row, a constraint matrix, constraint senses, right-hand sides and integrality
markers. **None of that exists in this crate, and none of it can be recovered from a
function.** So the missing part of MPS/LP export (requirement OPT-14) is not a serializer.
It is a **structured linear/MILP problem representation**, and this crate does not have
one. An MPS or LP writer depends on that representation. Writing one over the black-box
trait would mean inventing a linear model that is not the problem being solved, so the
exported file would lead to a different optimum.

What that work would involve, none of which exists today:

- a linear model type (objective coefficients, constraint matrix, senses, right-hand
  sides, bounds, integrality) alongside the black-box `Problem` trait;
- a way for callers, including the Cypher `algo.or.solve` path, to build one (today
  constraints become a penalty term);
- MPS/LP writers over that type, and importing results back;
- a decision about problems with no linear form. That is most of the problems here: the
  objectives call the database, which is why this crate uses metaheuristics at all.

Until that representation exists, OPT-14 is blocked on the model, not on a file writer.

## Usage

### Single-Objective (Rust)
```rust
use samyama_optimization::algorithms::*;
use samyama_optimization::common::*;
use ndarray::array;

let problem = SimpleProblem {
    objective_func: |x| x.iter().map(|&v| v * v).sum(), // Sphere function
    dim: 2,
    lower: array![-10.0, -10.0],
    upper: array![10.0, 10.0],
};

// Use Grey Wolf Optimizer
let config = SolverConfig { population_size: 50, max_iterations: 100 };
let solver = GWOSolver::new(config);
let result = solver.solve(&problem);

println!("Best: {:?}", result.best_variables);
println!("Fitness: {}", result.best_fitness);
```

### Multi-Objective (Rust)
```rust
use samyama_optimization::algorithms::NSGA2Solver;

// Define a struct impl MultiObjectiveProblem...
// Then:
let solver = NSGA2Solver::new(config);
let result = solver.solve(&mo_problem);

for ind in result.pareto_front {
    println!("Pareto Solution: {:?} -> Fitness: {:?}", ind.variables, ind.fitness);
}
```

## Integration with Samyama DB
You can access these solvers via Cypher!

```cypher
CALL algo.or.solve({
  algorithm: 'GWO',
  label: 'Factory',
  property: 'production',
  min: 0.0, max: 100.0,
  cost_property: 'cost',
  budget: 50000.0
})
```

## License
Apache-2.0