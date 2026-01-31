# Samyama Optimization Engine (Rust)

A high-performance library implementing metaphor-less metaheuristic optimization algorithms.

## Algorithms
- **Jaya**: Parameter-less optimization (Toward best, away from worst).
- **Rao (1, 2, 3)**: Metaphor-less algorithms using best, worst, and mean solutions.
- **TLBO / ITLBO**: Teaching-Learning-Based Optimization (with Elitism in ITLBO).
- **QOJaya**: Quasi-Oppositional Jaya (using Opposition-Based Learning).
- **BMR / BWR**: Best-Mean-Random and Best-Worst-Random strategies.
- **PSO**: Particle Swarm Optimization.
- **DE**: Differential Evolution.
- **GA**: Genetic Algorithm (Tournament selection, Uniform crossover).
- **GWO**: Grey Wolf Optimizer (Alpha, Beta, Delta hierarchy).
- **Firefly**: Firefly Algorithm (Light intensity based attraction).
- **Cuckoo**: Cuckoo Search (Levy flights and nest abandonment).
- **SA**: Simulated Annealing.
- **Bat**: Bat Algorithm (Echolocation-based search).
- **ABC**: Artificial Bee Colony.

## Features
- **Parallel Evaluation**: Automatic multi-threaded fitness calculation via `rayon`.
- **Zero-Copy**: Minimal overhead when operating on large vectors.
- **Python Bindings**: Easy integration with Python data science stacks.

## Usage (Rust)
```rust
use samyama_optimization::algorithms::*;
use samyama_optimization::common::*;
use ndarray::array;

let problem = SimpleProblem {
    objective_func: |x| x.iter().map(|&v| v * v).sum(),
    dim: 2,
    lower: array![-10.0, -10.0],
    upper: array![10.0, 10.0],
};

let solver = JayaSolver::new(SolverConfig::default());
let result = solver.solve(&problem);
println!("Best: {:?}", result.best_variables);
```

## Usage (Python)
```python
import samyama_optimization as so
import numpy as np

def sphere(x):
    return np.sum(x**2)

res = so.solve_jaya(sphere, np.array([-10, -10]), np.array([10, 10]))
print(f"Best fitness: {res.best_fitness}")
```
