use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct GWOSolver {
    pub config: SolverConfig,
}

impl GWOSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // 1. Initialize Population (Wolves)
        let mut population: Vec<Individual> = (0..self.config.population_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        // Initialize Alpha, Beta, Delta
        let mut alpha = population[0].clone();
        let mut beta = population[0].clone();
        let mut delta = population[0].clone();
        
        // Reset fitness to infinity (minimization)
        alpha.fitness = f64::INFINITY;
        beta.fitness = f64::INFINITY;
        delta.fitness = f64::INFINITY;

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            // Update Alpha, Beta, Delta
            for ind in &population {
                if ind.fitness < alpha.fitness {
                    // Shift down
                    delta = beta.clone();
                    beta = alpha.clone();
                    alpha = ind.clone();
                } else if ind.fitness < beta.fitness && ind.fitness > alpha.fitness {
                    delta = beta.clone();
                    beta = ind.clone();
                } else if ind.fitness < delta.fitness && ind.fitness > beta.fitness {
                    delta = ind.clone();
                }
            }

            history.push(alpha.fitness);

            let a = 2.0 - 2.0 * (iter as f64 / self.config.max_iterations as f64); // linearly decreases from 2 to 0

            // Update positions of Omegas
            for i in 0..self.config.population_size {
                let mut new_vars = Array1::zeros(dim);

                for j in 0..dim {
                    // Hunting equations
                    let r1: f64 = rng.gen();
                    let r2: f64 = rng.gen();
                    let a1 = 2.0 * a * r1 - a;
                    let c1 = 2.0 * r2;
                    let d_alpha = (c1 * alpha.variables[j] - population[i].variables[j]).abs();
                    let x1 = alpha.variables[j] - a1 * d_alpha;

                    let r1: f64 = rng.gen();
                    let r2: f64 = rng.gen();
                    let a2 = 2.0 * a * r1 - a;
                    let c2 = 2.0 * r2;
                    let d_beta = (c2 * beta.variables[j] - population[i].variables[j]).abs();
                    let x2 = beta.variables[j] - a2 * d_beta;

                    let r1: f64 = rng.gen();
                    let r2: f64 = rng.gen();
                    let a3 = 2.0 * a * r1 - a;
                    let c3 = 2.0 * r2;
                    let d_delta = (c3 * delta.variables[j] - population[i].variables[j]).abs();
                    let x3 = delta.variables[j] - a3 * d_delta;

                    new_vars[j] = ((x1 + x2 + x3) / 3.0).clamp(lower[j], upper[j]);
                }

                population[i].variables = new_vars;
                population[i].fitness = problem.fitness(&population[i].variables);
            }
        }

        OptimizationResult {
            best_variables: alpha.variables,
            best_fitness: alpha.fitness,
            history,
        }
    }
}
