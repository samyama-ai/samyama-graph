use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct ABCSolver {
    pub config: SolverConfig,
    pub limit: usize, // Abandonment limit for scout bees
}

impl ABCSolver {
    pub fn new(config: SolverConfig) -> Self {
        // Default limit is usually pop_size * dim / 2
        Self { 
            config,
            limit: 100, // Placeholder, will be adjusted in solve
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;
        let limit = if self.limit == 100 { pop_size * dim / 2 } else { self.limit };

        // 1. Initialize Food Sources (Population)
        let mut foods: Vec<Individual> = (0..pop_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        let mut trial_counters = vec![0; pop_size];
        
        let mut best_idx = 0;
        for i in 1..pop_size {
            if foods[i].fitness < foods[best_idx].fitness {
                best_idx = i;
            }
        }
        let mut best_vars = foods[best_idx].variables.clone();
        let mut best_fitness = foods[best_idx].fitness;

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for _iter in 0..self.config.max_iterations {
            history.push(best_fitness);

            // 2. Employed Bees Phase
            for i in 0..pop_size {
                let mut new_vars = foods[i].variables.clone();
                let j = rng.gen_range(0..dim);
                let mut k;
                loop {
                    k = rng.gen_range(0..pop_size);
                    if k != i { break; }
                }

                let phi: f64 = rng.gen_range(-1.0..1.0);
                new_vars[j] = (foods[i].variables[j] + phi * (foods[i].variables[j] - foods[k].variables[j])).clamp(lower[j], upper[j]);

                let new_fitness = problem.fitness(&new_vars);
                if new_fitness < foods[i].fitness {
                    foods[i] = Individual::new(new_vars, new_fitness);
                    trial_counters[i] = 0;
                } else {
                    trial_counters[i] += 1;
                }
            }

            // 3. Onlooker Bees Phase
            // Calculate probabilities based on fitness
            // Fitness in ABC is often 1/(1+f) if f>=0 else 1+|f|
            let mut probs = vec![0.0; pop_size];
            let mut total_f = 0.0;
            for i in 0..pop_size {
                let f_val = if foods[i].fitness >= 0.0 {
                    1.0 / (1.0 + foods[i].fitness)
                } else {
                    1.0 + foods[i].fitness.abs()
                };
                probs[i] = f_val;
                total_f += f_val;
            }
            for i in 0..pop_size {
                probs[i] /= total_f;
            }

            let mut m = 0;
            let mut t = 0;
            while m < pop_size {
                if rng.gen::<f64>() < probs[t] {
                    m += 1;
                    let i = t;
                    let mut new_vars = foods[i].variables.clone();
                    let j = rng.gen_range(0..dim);
                    let mut k;
                    loop {
                        k = rng.gen_range(0..pop_size);
                        if k != i { break; }
                    }

                    let phi: f64 = rng.gen_range(-1.0..1.0);
                    new_vars[j] = (foods[i].variables[j] + phi * (foods[i].variables[j] - foods[k].variables[j])).clamp(lower[j], upper[j]);

                    let new_fitness = problem.fitness(&new_vars);
                    if new_fitness < foods[i].fitness {
                        foods[i] = Individual::new(new_vars, new_fitness);
                        trial_counters[i] = 0;
                    } else {
                        trial_counters[i] += 1;
                    }
                }
                t = (t + 1) % pop_size;
            }

            // 4. Scout Bees Phase
            for i in 0..pop_size {
                if trial_counters[i] > limit {
                    // Reset
                    let mut vars = Array1::zeros(dim);
                    for j in 0..dim {
                        vars[j] = rng.gen_range(lower[j]..upper[j]);
                    }
                    let fitness = problem.fitness(&vars);
                    foods[i] = Individual::new(vars, fitness);
                    trial_counters[i] = 0;
                }
            }

            // Update best
            for i in 0..pop_size {
                if foods[i].fitness < best_fitness {
                    best_fitness = foods[i].fitness;
                    best_vars = foods[i].variables.clone();
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
