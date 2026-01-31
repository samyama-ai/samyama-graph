use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rand_distr::Distribution;
use std::f64::consts::PI;

pub struct FPASolver {
    pub config: SolverConfig,
    pub p: f64, // Switch probability (0.8)
}

impl FPASolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            p: 0.8,
        }
    }

    /// Levy flight random walk
    fn levy_flight(&self, dim: usize) -> Array1<f64> {
        let beta = 1.5;
        let sigma_u = ((gamma(1.0 + beta) * (PI * beta / 2.0).sin()) / 
                      (gamma((1.0 + beta) / 2.0) * beta * 2.0f64.powf((beta - 1.0) / 2.0)))
                      .powf(1.0 / beta);
        let sigma_v = 1.0;

        let mut step = Array1::zeros(dim);
        let mut rng = thread_rng();

        for i in 0..dim {
            let u_n: f64 = rand_distr::Normal::new(0.0, sigma_u).unwrap().sample(&mut rng);
            let v_n: f64 = rand_distr::Normal::new(0.0, sigma_v).unwrap().sample(&mut rng);
            let s = u_n / v_n.abs().powf(1.0 / beta);
            step[i] = s;
        }
        step
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;

        // 1. Initialize Population
        let mut population: Vec<Individual> = (0..pop_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        // Find initial best
        let mut best_idx = 0;
        for i in 1..pop_size {
            if population[i].fitness < population[best_idx].fitness {
                best_idx = i;
            }
        }
        let mut best_vars = population[best_idx].variables.clone();
        let mut best_fitness = population[best_idx].fitness;

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for _iter in 0..self.config.max_iterations {
            history.push(best_fitness);

            for i in 0..pop_size {
                let mut new_vars = population[i].variables.clone();

                if rng.gen::<f64>() < self.p {
                    // Global Pollination (Levy Flight)
                    let levy = self.levy_flight(dim);
                    for j in 0..dim {
                        let step = levy[j] * (population[i].variables[j] - best_vars[j]);
                        new_vars[j] = (population[i].variables[j] + step).clamp(lower[j], upper[j]);
                    }
                } else {
                    // Local Pollination
                    let mut j_idx;
                    let mut k_idx;
                    loop {
                        j_idx = rng.gen_range(0..pop_size);
                        k_idx = rng.gen_range(0..pop_size);
                        if j_idx != k_idx { break; }
                    }
                    
                    let epsilon: f64 = rng.gen();
                    for j in 0..dim {
                        let step = epsilon * (population[j_idx].variables[j] - population[k_idx].variables[j]);
                        new_vars[j] = (population[i].variables[j] + step).clamp(lower[j], upper[j]);
                    }
                }

                let new_fitness = problem.fitness(&new_vars);
                if new_fitness < population[i].fitness {
                    population[i] = Individual::new(new_vars, new_fitness);
                    if new_fitness < best_fitness {
                        best_fitness = new_fitness;
                        best_vars = population[i].variables.clone();
                    }
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

// Reuse gamma function from cuckoo or move to common
fn gamma(x: f64) -> f64 {
    if (x - 1.5).abs() < 1e-6 { return 0.886227; }
    if (x - 2.5).abs() < 1e-6 { return 1.32934; }
    let term1 = (2.0 * PI / x).sqrt();
    let term2 = (x / std::f64::consts::E).powf(x);
    term1 * term2
}
