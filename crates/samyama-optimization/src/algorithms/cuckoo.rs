use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rand_distr::Distribution;
use std::f64::consts::PI;

pub struct CuckooSolver {
    pub config: SolverConfig,
    pub pa: f64, // Probability of discovering an alien egg (abandonment rate)
}

impl CuckooSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { 
            config,
            pa: 0.25,
        }
    }

    pub fn with_pa(config: SolverConfig, pa: f64) -> Self {
        Self { config, pa }
    }

    /// Levy flight random walk
    fn levy_flight(&self, dim: usize) -> Array1<f64> {
        // Mantegna's algorithm for Levy flights
        let beta = 1.5;
        let sigma_u = ((gamma(1.0 + beta) * (PI * beta / 2.0).sin()) / 
                      (gamma((1.0 + beta) / 2.0) * beta * 2.0f64.powf((beta - 1.0) / 2.0)))
                      .powf(1.0 / beta);
        let sigma_v = 1.0;

        let mut step = Array1::zeros(dim);
        let mut rng = thread_rng();

        for i in 0..dim {
            let u: f64 = rng.gen_range(0.0..1.0) * sigma_u; // Standard normal * sigma_u? No, usually Gaussian(0, sigma_u^2)
            // Simulating Normal distribution
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

        // 1. Initialize Population (Nests)
        let mut nests: Vec<Individual> = (0..self.config.population_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.fitness(&vars);
                Individual::new(vars, fitness)
            })
            .collect();

        // Find current best
        let mut best_idx = 0;
        for i in 1..self.config.population_size {
            if nests[i].fitness < nests[best_idx].fitness {
                best_idx = i;
            }
        }
        let mut best_ind = nests[best_idx].clone();

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for _iter in 0..self.config.max_iterations {
            history.push(best_ind.fitness);

            // 2. Generate new solutions via Levy Flights
            for i in 0..self.config.population_size {
                let step_size = 0.01; // Step scale
                let levy = self.levy_flight(dim);
                let current_vars = &nests[i].variables;
                
                let mut new_vars = Array1::zeros(dim);
                for j in 0..dim {
                    // X_new = X + alpha * Levy * (X - X_best) ??
                    // Standard Cuckoo: X_new = X + alpha * Levy
                    // Sometimes uses difference from best. 
                    // Let's use simple: X_new = X + alpha * Levy * (X - X_best)
                    let diff = current_vars[j] - best_ind.variables[j];
                    let delta = step_size * levy[j] * (if diff.abs() > 1e-6 { diff } else { 1.0 }); // avoid zero mult?
                    // Actually standard CS: step = alpha * L(s, lambda)
                    // Let's stick to X_new = X + alpha * Levy (+)
                    
                    let delta_simple = step_size * levy[j] * (upper[j] - lower[j]);
                    
                    new_vars[j] = (current_vars[j] + delta_simple).clamp(lower[j], upper[j]);
                }

                let new_fitness = problem.fitness(&new_vars);
                
                // Random selection of nest to replace?
                // Standard: Pick a random nest j, replace if new is better.
                let j = rng.gen_range(0..self.config.population_size);
                if new_fitness < nests[j].fitness {
                    nests[j] = Individual::new(new_vars, new_fitness);
                    if nests[j].fitness < best_ind.fitness {
                        best_ind = nests[j].clone();
                    }
                }
            }

            // 3. Abandon worst nests (Alien eggs discovery)
            // Sort to find worst? Or just random pairwise?
            // Standard: Sort nests by fitness
            nests.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
            
            // Keep best (elitism), replace fraction pa of the rest (the worst ones)
            let num_abandon = (self.config.population_size as f64 * self.pa) as usize;
            let start_abandon_idx = self.config.population_size - num_abandon;

            for i in start_abandon_idx..self.config.population_size {
                // Generate new solution
                // Usually via preferential random walk or simple random
                let mut vars = Array1::zeros(dim);
                
                // Pick two random nests for mixing
                let r1 = rng.gen_range(0..self.config.population_size);
                let r2 = rng.gen_range(0..self.config.population_size);
                
                for j in 0..dim {
                    let step = rng.gen::<f64>() * (nests[r1].variables[j] - nests[r2].variables[j]);
                    vars[j] = (nests[i].variables[j] + step).clamp(lower[j], upper[j]);
                }
                
                let fitness = problem.fitness(&vars);
                nests[i] = Individual::new(vars, fitness);
                
                if fitness < best_ind.fitness {
                    best_ind = nests[i].clone();
                }
            }
        }

        OptimizationResult {
            best_variables: best_ind.variables,
            best_fitness: best_ind.fitness,
            history,
        }
    }
}

// Simple gamma function approximation (Lanczos)
fn gamma(x: f64) -> f64 {
    // For x ~ 1.5, simple approximation or crate `statrs`
    // Since we don't have `statrs` in dependencies yet, let's use a hardcoded value for beta=1.5
    // Gamma(1.5) = sqrt(PI)/2 ~= 0.886227
    // Gamma(2.5) = 1.5 * Gamma(1.5) ~= 1.32934
    
    if (x - 1.5).abs() < 1e-6 { return 0.886227; }
    if (x - 2.5).abs() < 1e-6 { return 1.32934; }
    
    // Fallback: Stirling's approximation
    let term1 = (2.0 * PI / x).sqrt();
    let term2 = (x / std::f64::consts::E).powf(x);
    term1 * term2
}
