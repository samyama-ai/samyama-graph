//! SAPHR — Self-Adaptive Population Hybrid Rao
//! (Water Resources Management 2025, doi:10.1007/s11269-025-04186-7).
//!
//! Combines the three Rao variants (Rao-1, Rao-2, Rao-3) with a
//! self-adaptive selection strategy. For each individual, the Rao variant that
//! produced the most improvement so far is preferred, with epsilon-exploration.
//! Single-objective; supports constraint penalty.

use crate::algorithms::rao::RaoVariant;
use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct SAPHRSolver {
    pub config: SolverConfig,
    pub epsilon: f64,
}

impl SAPHRSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config, epsilon: 0.2 }
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

        // Per-variant success counters across the whole population.
        let mut success: [f64; 3] = [1.0, 1.0, 1.0]; // Laplace-smoothed
        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!(
                    "SAPHR Solver: Iteration {}/{}",
                    iter, self.config.max_iterations
                );
            }
            let (best_idx, worst_idx) = find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            history.push(population[best_idx].fitness);

            // Sample variant by softmax over success counts (probability table).
            let total: f64 = success.iter().sum();
            let probs = [
                success[0] / total,
                (success[0] + success[1]) / total,
            ];

            // Updates per individual + count successes per variant.
            let updates: Vec<(Individual, usize, bool)> = population
                .par_iter()
                .map(|ind| {
                    let mut local_rng = thread_rng();
                    let pick: f64 = local_rng.gen();
                    let chosen = if local_rng.gen::<f64>() < self.epsilon {
                        local_rng.gen_range(0..3)
                    } else if pick < probs[0] {
                        0
                    } else if pick < probs[1] {
                        1
                    } else {
                        2
                    };
                    let variant = match chosen {
                        0 => RaoVariant::Rao1,
                        1 => RaoVariant::Rao2,
                        _ => RaoVariant::Rao3,
                    };

                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();
                    let mut rand_vars = Array1::zeros(dim);
                    let need_rand = matches!(variant, RaoVariant::Rao2 | RaoVariant::Rao3);
                    if need_rand {
                        for j in 0..dim {
                            rand_vars[j] = local_rng.gen_range(lower[j]..upper[j]);
                        }
                    }
                    let rand_fit = if need_rand {
                        problem.fitness(&rand_vars)
                    } else {
                        0.0
                    };

                    let mut new_vars = Array1::zeros(dim);
                    for j in 0..dim {
                        let term1 = best_vars[j] - worst_vars[j];
                        let delta = match variant {
                            RaoVariant::Rao1 => r1 * term1,
                            RaoVariant::Rao2 => {
                                let term2 = if ind.fitness < rand_fit {
                                    ind.variables[j] - rand_vars[j]
                                } else {
                                    rand_vars[j] - ind.variables[j]
                                };
                                r1 * term1 + r2 * term2
                            }
                            RaoVariant::Rao3 => {
                                let term1_abs = best_vars[j] - worst_vars[j].abs();
                                let term2_abs = if ind.fitness < rand_fit {
                                    ind.variables[j] - rand_vars[j]
                                } else {
                                    rand_vars[j] - ind.variables[j]
                                };
                                r1 * term1_abs + r2 * term2_abs
                            }
                        };
                        new_vars[j] = (ind.variables[j] + delta).clamp(lower[j], upper[j]);
                    }
                    let new_fit = problem.fitness(&new_vars);
                    let improved = new_fit < ind.fitness;
                    let updated = if improved {
                        Individual::new(new_vars, new_fit)
                    } else {
                        ind.clone()
                    };
                    (updated, chosen, improved)
                })
                .collect();

            population = updates
                .iter()
                .map(|(ind, _, _)| ind.clone())
                .collect();
            for (_, c, ok) in &updates {
                if *ok {
                    success[*c] += 1.0;
                }
            }
        }

        let (best_idx, _) = find_best_worst(&population);
        let best = &population[best_idx];
        OptimizationResult {
            best_variables: best.variables.clone(),
            best_fitness: best.fitness,
            history,
        }
    }
}

fn find_best_worst(population: &[Individual]) -> (usize, usize) {
    let mut best_idx = 0;
    let mut worst_idx = 0;
    for (i, ind) in population.iter().enumerate() {
        if ind.fitness < population[best_idx].fitness {
            best_idx = i;
        }
        if ind.fitness > population[worst_idx].fitness {
            worst_idx = i;
        }
    }
    (best_idx, worst_idx)
}
