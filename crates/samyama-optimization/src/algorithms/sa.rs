use crate::common::{OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct SASolver {
    pub config: SolverConfig,
    pub initial_temp: f64,
    pub cooling_rate: f64,
}

impl SASolver {
    pub fn new(config: SolverConfig) -> Self {
        Self {
            config,
            initial_temp: 1000.0,
            cooling_rate: 0.95,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // 1. Initial Solution
        let mut current_vars = Array1::zeros(dim);
        for i in 0..dim {
            current_vars[i] = rng.gen_range(lower[i]..upper[i]);
        }
        let mut current_fitness = problem.fitness(&current_vars);

        let mut best_vars = current_vars.clone();
        let mut best_fitness = current_fitness;

        let mut history = Vec::with_capacity(self.config.max_iterations);
        let mut temp = self.initial_temp;

        // Since SA is typically a single-point search, we'll interpret 'population_size' 
        // as number of trials per temperature step if we want to utilize the config.
        // Or we just run for max_iterations total.
        // To be consistent with other solvers, let's treat population_size * max_iterations 
        // as the total number of evaluations budget.
        
        let total_steps = self.config.max_iterations;

        for _step in 0..total_steps {
            history.push(best_fitness);

            // Generate neighbor
            let mut next_vars = current_vars.clone();
            for i in 0..dim {
                // Perturb slightly (Gaussian or random walk)
                let range = upper[i] - lower[i];
                let delta = (rng.gen::<f64>() - 0.5) * range * 0.1;
                next_vars[i] = (next_vars[i] + delta).clamp(lower[i], upper[i]);
            }

            let next_fitness = problem.fitness(&next_vars);

            // Acceptance probability
            let delta_e = next_fitness - current_fitness;
            
            if delta_e < 0.0 || rng.gen::<f64>() < (-delta_e / temp).exp() {
                current_vars = next_vars;
                current_fitness = next_fitness;

                if current_fitness < best_fitness {
                    best_vars = current_vars.clone();
                    best_fitness = current_fitness;
                }
            }

            // Cool down
            temp *= self.cooling_rate;
        }

        OptimizationResult {
            best_variables: best_vars,
            best_fitness,
            history,
        }
    }
}
