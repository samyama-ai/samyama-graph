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
            let (best_idx, worst_idx) = self.find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            let mean_vars = self.calculate_mean(&population, dim);
            let best_fitness = population[best_idx].fitness;

            history.push(best_fitness);

            let phase = 1.0 - (iter as f64 / self.config.max_iterations as f64);

            // Update population
            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    let mut new_vars = Array1::zeros(dim);

                    let r: f64 = local_rng.gen(); // Scalar r per individual

                    for j in 0..dim {
                        let delta = match self.variant {
                            RaoVariant::Rao1 => {
                                if r < 0.5 {
                                    r * (best_vars[j] - ind.variables[j].abs())
                                } else {
                                    r * (best_vars[j] - mean_vars[j])
                                }
                            },
                            RaoVariant::Rao2 => {
                                if r < 0.5 {
                                    r * (best_vars[j] - ind.variables[j].abs()) - r * (worst_vars[j] - ind.variables[j].abs())
                                } else {
                                    r * (best_vars[j] - mean_vars[j]) - r * (worst_vars[j] - ind.variables[j].abs())
                                }
                            },
                            RaoVariant::Rao3 => {
                                r * phase * (best_vars[j] - ind.variables[j].abs())
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

    fn calculate_mean(&self, population: &[Individual], dim: usize) -> Array1<f64> {
        let mut mean = Array1::zeros(dim);
        for ind in population {
            mean += &ind.variables;
        }
        mean / (population.len() as f64)
    }
}