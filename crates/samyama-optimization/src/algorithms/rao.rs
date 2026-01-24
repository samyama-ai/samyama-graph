use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

#[derive(Debug, Clone, Copy)]
pub enum RaoVariant {
    Rao1,
    Rao2,
    Rao3,
}

pub struct RaoSolver {
    pub config: SolverConfig,
    pub variant: RaoVariant,
}

impl RaoSolver {
    pub fn new(config: SolverConfig, variant: RaoVariant) -> Self {
        Self { config, variant }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // Initialize population
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

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!("Rao Solver: Iteration {}/{}", iter, self.config.max_iterations);
            }
            let (best_idx, worst_idx) = self.find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            let best_fitness = population[best_idx].fitness;

            history.push(best_fitness);

            // Update population
            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    let mut new_vars = Array1::zeros(dim);

                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();

                    // For Rao-2 and Rao-3, we need a random individual.
                    // In parallel map, we approximate by generating a random point in bounds.
                    // This is "Best-Worst-Random" style.
                    let mut rand_vars = Array1::zeros(dim);
                    if matches!(self.variant, RaoVariant::Rao2 | RaoVariant::Rao3) {
                        for j in 0..dim {
                            rand_vars[j] = local_rng.gen_range(lower[j]..upper[j]);
                        }
                    }
                    // We assume fitness of random point is worse? Or compare?
                    // Standard Rao compares fitness. We'll compute it if needed.
                    let rand_fitness = if matches!(self.variant, RaoVariant::Rao2 | RaoVariant::Rao3) {
                        problem.fitness(&rand_vars)
                    } else {
                        0.0
                    };

                    for j in 0..dim {
                        let term1 = best_vars[j] - worst_vars[j];
                        
                        let delta = match self.variant {
                            RaoVariant::Rao1 => {
                                r1 * term1
                            },
                            RaoVariant::Rao2 => {
                                let term2 = if ind.fitness < rand_fitness {
                                    ind.variables[j] - rand_vars[j]
                                } else {
                                    rand_vars[j] - ind.variables[j]
                                };
                                r1 * term1 + r2 * term2
                            },
                            RaoVariant::Rao3 => {
                                // Rao-3 often uses |worst| and |best|
                                let term1_abs = best_vars[j] - worst_vars[j].abs();
                                let term2_abs = if ind.fitness < rand_fitness {
                                    ind.variables[j] - rand_vars[j]
                                } else {
                                    rand_vars[j] - ind.variables[j]
                                };
                                r1 * term1_abs + r2 * term2_abs
                            }
                        };

                        new_vars[j] = (ind.variables[j] + delta).clamp(lower[j], upper[j]);
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

        let (final_best_idx, _) = self.find_best_worst(&population);
        let final_best = &population[final_best_idx];

        OptimizationResult {
            best_variables: final_best.variables.clone(),
            best_fitness: final_best.fitness,
            history,
        }
    }

    fn find_best_worst(&self, population: &[Individual]) -> (usize, usize) {
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
}
