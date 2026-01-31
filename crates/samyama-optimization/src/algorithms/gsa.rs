use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct GSASolver {
    pub config: SolverConfig,
    pub g0: f64,    // Initial gravitational constant (100.0)
    pub alpha: f64, // Damping ratio (20.0)
}

impl GSASolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            g0: 100.0,
            alpha: 20.0,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let n = self.config.population_size;

        // 1. Initialize Agents
        let mut population: Vec<Individual> = (0..n)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        let mut velocities: Vec<Array1<f64>> = (0..n)
            .map(|_| Array1::zeros(dim))
            .collect();

        let mut history = Vec::with_capacity(self.config.max_iterations);
        
        let mut best_vars = population[0].variables.clone();
        let mut best_fitness = population[0].fitness;

        for iter in 0..self.config.max_iterations {
            // Update fitness and find best/worst
            let mut worst_fitness = population[0].fitness;
            for ind in &population {
                if ind.fitness < best_fitness {
                    best_fitness = ind.fitness;
                    best_vars = ind.variables.clone();
                }
                if ind.fitness > worst_fitness {
                    worst_fitness = ind.fitness;
                }
            }
            history.push(best_fitness);

            // 2. Calculate Masses
            let mut masses = vec![0.0; n];
            let mut total_m = 0.0;
            
            // Normalize fitness to calculate mass
            if (worst_fitness - best_fitness).abs() > 1e-9 {
                for i in 0..n {
                    masses[i] = (population[i].fitness - worst_fitness) / (best_fitness - worst_fitness);
                    total_m += masses[i];
                }
                for i in 0..n {
                    masses[i] /= total_m;
                }
            } else {
                for i in 0..n {
                    masses[i] = 1.0 / (n as f64);
                }
            }

            // 3. Calculate Force and Acceleration
            let g = self.g0 * (-(self.alpha * (iter as f64) / (self.config.max_iterations as f64))).exp();
            let mut accelerations: Vec<Array1<f64>> = (0..n).map(|_| Array1::zeros(dim)).collect();

            for i in 0..n {
                for j in 0..n {
                    if i == j { continue; }
                    
                    let mut dist_sq = 0.0;
                    for k in 0..dim {
                        let diff = population[j].variables[k] - population[i].variables[k];
                        dist_sq += diff * diff;
                    }
                    let dist = dist_sq.sqrt() + 1e-6; // avoid zero

                    for k in 0..dim {
                        let force = g * (masses[i] * masses[j] / dist) * (population[j].variables[k] - population[i].variables[k]);
                        accelerations[i][k] += rng.gen::<f64>() * force;
                    }
                }
            }

            // 4. Update Velocity and Position
            for i in 0..n {
                for k in 0..dim {
                    velocities[i][k] = rng.gen::<f64>() * velocities[i][k] + accelerations[i][k];
                    population[i].variables[k] = (population[i].variables[k] + velocities[i][k]).clamp(lower[k], upper[k]);
                }
                population[i].fitness = problem.fitness(&population[i].variables);
            }
        }

        OptimizationResult {
            best_variables: best_vars,
            best_fitness,
            history,
        }
    }
}
