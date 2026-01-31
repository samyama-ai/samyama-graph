use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct BatSolver {
    pub config: SolverConfig,
    pub f_min: f64,
    pub f_max: f64,
    pub alpha: f64, // Constant for loudness update (0.9)
    pub gamma: f64, // Constant for emission rate update (0.9)
}

impl BatSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self {
            config,
            f_min: 0.0,
            f_max: 2.0,
            alpha: 0.9,
            gamma: 0.9,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // 1. Initialize Bats
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

        let mut velocities: Vec<Array1<f64>> = (0..self.config.population_size)
            .map(|_| Array1::zeros(dim))
            .collect();

        let mut frequencies = vec![0.0; self.config.population_size];
        let mut loudnesses = vec![1.0; self.config.population_size];
        let mut emission_rates = vec![0.5; self.config.population_size];
        let r0 = 0.5; // initial emission rate

        // Initial best
        let mut best_idx = 0;
        for i in 1..population.len() {
            if population[i].fitness < population[best_idx].fitness {
                best_idx = i;
            }
        }
        let mut best_vars = population[best_idx].variables.clone();
        let mut best_fitness = population[best_idx].fitness;

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            history.push(best_fitness);

            for i in 0..self.config.population_size {
                // Update frequency
                let beta: f64 = rng.gen();
                frequencies[i] = self.f_min + (self.f_max - self.f_min) * beta;

                // Update velocity and position
                for j in 0..dim {
                    velocities[i][j] += (population[i].variables[j] - best_vars[j]) * frequencies[i];
                    population[i].variables[j] = (population[i].variables[j] + velocities[i][j]).clamp(lower[j], upper[j]);
                }

                // Local search around best
                if rng.gen::<f64>() > emission_rates[i] {
                    let mut temp_vars = best_vars.clone();
                    let avg_loudness: f64 = loudnesses.iter().sum::<f64>() / (self.config.population_size as f64);
                    for j in 0..dim {
                        let epsilon = (rng.gen::<f64>() - 0.5) * 2.0;
                        temp_vars[j] = (temp_vars[j] + epsilon * avg_loudness).clamp(lower[j], upper[j]);
                    }
                    
                    let temp_fitness = problem.fitness(&temp_vars);
                    
                    // Accept new solution
                    if temp_fitness < population[i].fitness && rng.gen::<f64>() < loudnesses[i] {
                        population[i].variables = temp_vars;
                        population[i].fitness = temp_fitness;
                        
                        // Update loudness and emission rate
                        loudnesses[i] *= self.alpha;
                        emission_rates[i] = r0 * (1.0 - (-self.gamma * (iter as f64)).exp());
                    }
                } else {
                    population[i].fitness = problem.fitness(&population[i].variables);
                }

                // Update global best
                if population[i].fitness < best_fitness {
                    best_vars = population[i].variables.clone();
                    best_fitness = population[i].fitness;
                }
            }
        }

        OptimizationResult {
            best_variables: best_vars,
            best_fitness,
            history,
        }
    }
}
