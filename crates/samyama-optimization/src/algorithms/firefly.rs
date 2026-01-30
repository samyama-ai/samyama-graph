use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct FireflySolver {
    pub config: SolverConfig,
    pub alpha: f64,      // Randomization parameter (0.2)
    pub beta0: f64,      // Attractiveness at r=0 (1.0)
    pub gamma: f64,      // Light absorption coefficient (1.0)
}

impl FireflySolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            alpha: 0.2,
            beta0: 1.0,
            gamma: 1.0,
        }
    }

    pub fn with_params(config: SolverConfig, alpha: f64, beta0: f64, gamma: f64) -> Self {
        Self { config, alpha, beta0, gamma }
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
        let mut best_idx = 0;
        
        // Find initial best
        for (i, ind) in population.iter().enumerate() {
            if ind.fitness < population[best_idx].fitness {
                best_idx = i;
            }
        }

        for _iter in 0..self.config.max_iterations {
            history.push(population[best_idx].fitness);

            // Firefly algorithm loop: move i towards j if j is brighter (better fitness)
            // Note: Parallelizing this double loop is tricky due to mutable updates.
            // We'll calculate new positions and then update batch-wise.
            
            let old_population = population.clone();
            let pop_size = self.config.population_size;

            // We can parallelize the outer loop (i)
            let new_positions: Vec<Option<Array1<f64>>> = (0..pop_size).into_par_iter().map(|i| {
                let mut rng = thread_rng();
                let mut moved = false;
                let mut new_vars = old_population[i].variables.clone();
                let fitness_i = old_population[i].fitness;

                for j in 0..pop_size {
                    if i == j { continue; }

                    let fitness_j = old_population[j].fitness;

                    // For minimization, "brighter" means lower fitness value
                    if fitness_j < fitness_i {
                        moved = true;
                        let vars_j = &old_population[j].variables;
                        
                        // Calculate distance r
                        let mut r_sq = 0.0;
                        for k in 0..dim {
                            let diff = new_vars[k] - vars_j[k];
                            r_sq += diff * diff;
                        }
                        // Avoid sqrt if possible, or just use r_sq in exp if formula allows
                        // Standard: exp(-gamma * r^2)
                        
                        let beta = self.beta0 * (-self.gamma * r_sq).exp();

                        for k in 0..dim {
                            let random_step = self.alpha * (rng.gen::<f64>() - 0.5) * (upper[k] - lower[k]);
                            let move_step = beta * (vars_j[k] - new_vars[k]);
                            
                            new_vars[k] = (new_vars[k] + move_step + random_step).clamp(lower[k], upper[k]);
                        }
                    }
                }

                if moved {
                    Some(new_vars)
                } else {
                    None
                }
            }).collect();

            // Apply updates
            for (i, new_pos) in new_positions.into_iter().enumerate() {
                if let Some(vars) = new_pos {
                    let new_fitness = problem.fitness(&vars);
                    // Selection: greedy acceptance? Standard FA moves anyway.
                    // We'll accept if better or just move. 
                    // Standard FA just moves. But ensuring elitism is good.
                    // Let's adopt standard: simple update.
                    // But we keep track of global best.
                    population[i].variables = vars;
                    population[i].fitness = new_fitness;
                }
            }

            // Update best
            for (i, ind) in population.iter().enumerate() {
                if ind.fitness < population[best_idx].fitness {
                    best_idx = i;
                }
            }
        }

        let best_ind = &population[best_idx];

        OptimizationResult {
            best_variables: best_ind.variables.clone(),
            best_fitness: best_ind.fitness,
            history,
        }
    }
}
