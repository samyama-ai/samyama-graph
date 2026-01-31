# Samyama Optimization Engine (Rust)

A high-performance library implementing **metaphor-less** and **nature-inspired** metaheuristic optimization algorithms.

This engine allows you to solve complex resource allocation, scheduling, and engineering problems by defining an objective function and constraints.

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