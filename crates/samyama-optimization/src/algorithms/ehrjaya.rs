//! EHR-Jaya — Self-adaptive Classification-Learning Hybrid Jaya + Rao-1
//! (Wang et al. 2022, EAAI).
//!
//! Each iteration: rank population, classify into "high-performing" (top half)
//! and "low-performing" (bottom half).
//!   - High-performing individuals use Rao-1 update (pure best-vs-worst pull):
//!       V' = V + r1·(V_best − V_worst)
//!   - Low-performing individuals use Jaya update (best-pull + worst-push):
//!       V' = V + r1·(V_best − |V|) − r2·(V_worst − |V|)
//! Greedy acceptance.

use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct EHRJayaSolver {
    pub config: SolverConfig,
}

impl EHRJayaSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;

        let mut population: Vec<Individual> = (0..pop_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!(
                    "EHR-Jaya Solver: Iteration {}/{}",
                    iter, self.config.max_iterations
                );
            }
            // Rank — index 0 = best, last = worst
            population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
            let best_vars = population[0].variables.clone();
            let worst_vars = population[pop_size - 1].variables.clone();
            history.push(population[0].fitness);

            let half = pop_size / 2;
            population = population
                .into_par_iter()
                .enumerate()
                .map(|(rank, mut ind)| {
                    let mut local_rng = thread_rng();
                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();
                    let mut new_vars = Array1::zeros(dim);

                    if rank < half {
                        // High-performing: Rao-1 update
                        for j in 0..dim {
                            let val =
                                ind.variables[j] + r1 * (best_vars[j] - worst_vars[j]);
                            new_vars[j] = val.clamp(lower[j], upper[j]);
                        }
                    } else {
                        // Low-performing: Jaya update
                        for j in 0..dim {
                            let val = ind.variables[j]
                                + r1 * (best_vars[j] - ind.variables[j].abs())
                                - r2 * (worst_vars[j] - ind.variables[j].abs());
                            new_vars[j] = val.clamp(lower[j], upper[j]);
                        }
                    }

                    let new_fitness = problem.fitness(&new_vars);
                    if new_fitness < ind.fitness {
                        ind.variables = new_vars;
                        ind.fitness = new_fitness;
                    }
                    ind
                })
                .collect();
        }

        population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
        let best = &population[0];
        OptimizationResult {
            best_variables: best.variables.clone(),
            best_fitness: best.fitness,
            history,
        }
    }
}
