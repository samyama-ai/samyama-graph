use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct JayaSolver {
    pub config: SolverConfig,
}

impl JayaSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

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

        for _ in 0..self.config.max_iterations {
            let (best_idx, worst_idx) = self.find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            let best_fitness = population[best_idx].fitness;

            history.push(best_fitness);

            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    let mut new_vars = Array1::zeros(dim);

                    // Generate r1, r2 once per individual to match Python's vector op
                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();

                    for j in 0..dim {
                        let val = ind.variables[j] 
                            + r1 * (best_vars[j] - ind.variables[j].abs()) 
                            - r2 * (worst_vars[j] - ind.variables[j].abs());
                        
                        new_vars[j] = val.clamp(lower[j], upper[j]);
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