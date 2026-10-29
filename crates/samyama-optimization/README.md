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
- **L-SHADE**: Differential Evolution that adapts its own F and CR from a success history and shrinks its population linearly (Tanabe & Fukunaga, CEC 2014). Not parallel: one generation runs sequentially.
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

### What a solve returns

Every row reports how good the answer is, not only the answer (OPT-09):

| Column | Meaning |
|---|---|
| `fitness` | Objective plus constraint penalty: what the solver minimised. A list for a multi-objective solve. |
| `objective` | The objective alone, without the penalty. Equal to `fitness` when the answer is feasible. |
| `feasible` | `true` when the answer meets every constraint (`budget`, `minTotal`). Strict: any violation is `false`. |
| `gap` | `objective - bestKnown`, in the objective's units; negative means the answer beat it. `null` unless you pass `bestKnown`, because no solver here can prove a bound. |
| `iterations` / `max_iterations` | Iterations run, and the cap you set. |
| `total_time_ms` | Wall-clock time of the solve alone, in milliseconds. |
| `algorithm`, `history` | The solver, and its best fitness per iteration. Multi-objective solves return `front_size` in place of `history`. |

`bestKnown` (or `best_known`) must be a finite number, and applies to
single-objective solves only: a Pareto front has no single best known value,
so passing it there is an error rather than a `gap` that is silently `null`.

```cypher
CALL algo.or.solve({label: 'Factory', property: 'production', cost_property: 'cost',
                    budget: 50000.0, bestKnown: 41000.0})
YIELD objective, feasible, gap, total_time_ms
```

`/optimize/solve` reports `feasible` and `gap` in its `done` event as well:
`gap` against the benchmark's known optimum, and both `null` for a
multi-objective benchmark (see #1795 for why feasibility cannot be read there yet).

### Measuring a multi-objective front

A multi-objective solve returns a front of trade-offs, not one answer, so its
quality is measured against the problem's known Pareto front (OPT-03).
`moo::quality_metrics(front, reference_front)` computes all five:

| Measure | Function | Better | What it says |
|---|---|---|---|
| Hypervolume | `moo::hypervolume` | larger | Volume the front dominates, up to a reference point past the known front's worst values. Exact, any number of objectives. |
| IGD | `moo::igd` | smaller | Mean distance from each known-front point to the nearest front point. |
| IGD+ | `moo::igd_plus` | smaller | Like IGD, but a point is never charged for beating the known front. |
| Spread | `moo::spread` | smaller | Generalised Δ: how evenly the front is spaced, and whether it reaches each extreme. 0 is perfectly even. |
| Coverage | `moo::coverage(known, front)` | smaller | Share of the front's points the known front weakly dominates, so that are provably not optimal. |

The hypervolume reference point is the known front's worst value in each
objective, pushed out by `moo::HV_REFERENCE_MARGIN` (10%) of its range.

**Known fronts** come from the problems themselves: `ZDT::reference_front` and
`DTLZ::reference_front` evaluate the problem's own objectives on a grid of its
position variables, with its distance variables at the value where it is
optimal, and keep the non-dominated points. There is no copied table to drift
from the formula. The tests check every point against each front's equation
(ZDT1/4: f2 = 1 − √f1; ZDT2/6: f2 = 1 − f1²; DTLZ1: Σf = ½; DTLZ2–6: Σf² = 1).

`/optimize/solve` reports them in its `done` event as `quality`, for every
benchmark with a known front: ZDT1–4, ZDT6 and DTLZ1–7. It is `null` for
`uc2_dosing`, which has no known front, and for single-objective runs. ZDT5 is
binary-coded and is not offered. The measures are computed after the solve's
clock stops, so `total_time_ms` is still the solve alone.

## License
Apache-2.0