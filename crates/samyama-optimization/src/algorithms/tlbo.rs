use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct TLBOSolver {
    pub config: SolverConfig,
}

impl TLBOSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
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

        for _ in 0..self.config.max_iterations {
            let best_idx = self.find_best(&population);
            let teacher_vars = population[best_idx].variables.clone();
            let best_fitness = population[best_idx].fitness;
            let mean_vars = self.calculate_mean(&population, dim);

            history.push(best_fitness);

            // 1. Teacher Phase
            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    let tf: f64 = local_rng.gen_range(1..3) as f64; // Teaching Factor (1 or 2)
                    let mut new_vars = Array1::zeros(dim);

                    for j in 0..dim {
                        let r: f64 = local_rng.gen();
                        let delta = r * (teacher_vars[j] - tf * mean_vars[j]);
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

            // 2. Learner Phase
            let pop_len = population.len();
            for i in 0..pop_len {
                let mut learner_j_idx;
                loop {
                    learner_j_idx = rng.gen_range(0..pop_len);
                    if learner_j_idx != i { break; }
                }

                let ind_i = &population[i];
                let ind_j = &population[learner_j_idx];
                
                let mut new_vars = Array1::zeros(dim);
                for k in 0..dim {
                    let r: f64 = rng.gen();
                    let delta = if ind_i.fitness < ind_j.fitness {
                        r * (&ind_i.variables[k] - &ind_j.variables[k])
                    } else {
                        r * (&ind_j.variables[k] - &ind_i.variables[k])
                    };
                    new_vars[k] = (ind_i.variables[k] + delta).clamp(lower[k], upper[k]);
                }

                let new_fitness = problem.fitness(&new_vars);
                if new_fitness < population[i].fitness {
                    population[i].variables = new_vars;
                    population[i].fitness = new_fitness;
                }
            }
        }

        let final_best_idx = self.find_best(&population);
        let final_best = &population[final_best_idx];

        OptimizationResult {
            best_variables: final_best.variables.clone(),
            best_fitness: final_best.fitness,
            history,
        }
    }

    fn find_best(&self, population: &[Individual]) -> usize {
        let mut best_idx = 0;
        for (i, ind) in population.iter().enumerate() {
            if ind.fitness < population[best_idx].fitness {
                best_idx = i;
            }
        }
        best_idx
    }

    fn calculate_mean(&self, population: &[Individual], dim: usize) -> Array1<f64> {
        let mut mean = Array1::zeros(dim);
        for ind in population {
            mean += &ind.variables;
        }
        mean / (population.len() as f64)
    }
}
