use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct ITLBOSolver {
    pub config: SolverConfig,
    pub elite_size: usize,
}

impl ITLBOSolver {
    pub fn new(config: SolverConfig) -> Self {
        let elite_size = std::cmp::max(1, config.population_size / 10); // 10% elite
        Self { config, elite_size }
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
                println!("ITLBO Solver: Iteration {}/{}", iter, self.config.max_iterations);
            }
            // Sort to find elites
            population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
            
            // Save elites
            let elites: Vec<Individual> = population.iter().take(self.elite_size).cloned().collect();
            
            let best_fitness = population[0].fitness;
            let teacher_vars = population[0].variables.clone();
            let mean_vars = self.calculate_mean(&population, dim);

            history.push(best_fitness);

            // 1. Teacher Phase
            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    // Adaptive TF: Usually between 1 and 2. 
                    let tf: f64 = local_rng.gen_range(1.0..2.0); 
                    
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

            // 2. Learner Phase (Enhanced)
            let pop_len = population.len();
            
            // Note: Parallel learner phase needs safe random access. 
            // We'll clone the "old" population for reading to allow parallel updates.
            let old_population = population.clone();
            
            population = population
                .into_par_iter()
                .enumerate()
                .map(|(i, mut ind)| {
                    let mut local_rng = thread_rng();
                    
                    let mut learner_j_idx;
                    loop {
                        learner_j_idx = local_rng.gen_range(0..pop_len);
                        if learner_j_idx != i { break; }
                    }
                    let ind_j = &old_population[learner_j_idx];

                    let mut new_vars = Array1::zeros(dim);
                    for k in 0..dim {
                        let r: f64 = local_rng.gen();
                        let delta = if ind.fitness < ind_j.fitness {
                            r * (&ind.variables[k] - &ind_j.variables[k])
                        } else {
                            r * (&ind_j.variables[k] - &ind.variables[k])
                        };
                        new_vars[k] = (ind.variables[k] + delta).clamp(lower[k], upper[k]);
                    }

                    let new_fitness = problem.fitness(&new_vars);
                    if new_fitness < ind.fitness {
                        ind.variables = new_vars;
                        ind.fitness = new_fitness;
                    }
                    ind
                })
                .collect();

            // 3. Elitism: Replace worst individuals with preserved elites
            // We need to sort again to find the worst
            population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
            
            let len = population.len();
            for k in 0..self.elite_size {
                // If the elite is better than the worst
                if elites[k].fitness < population[len - 1 - k].fitness {
                    population[len - 1 - k] = elites[k].clone();
                }
            }
        }

        let best_idx = 0; // Sorted
        let final_best = &population[best_idx];

        OptimizationResult {
            best_variables: final_best.variables.clone(),
            best_fitness: final_best.fitness,
            history,
        }
    }

    fn calculate_mean(&self, population: &[Individual], dim: usize) -> Array1<f64> {
        let mut mean = Array1::zeros(dim);
        for ind in population {
            mean += &ind.variables;
        }
        mean / (population.len() as f64)
    }
}