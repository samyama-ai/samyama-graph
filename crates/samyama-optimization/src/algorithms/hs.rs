use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;

pub struct HSSolver {
    pub config: SolverConfig,
    pub hmcr: f64, // Harmony Memory Consideration Rate (0.7-0.95)
    pub par: f64,  // Pitch Adjustment Rate (0.1-0.5)
    pub bw: f64,   // Bandwidth (step size)
}

impl HSSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self {
            config,
            hmcr: 0.9,
            par: 0.3,
            bw: 0.01,
        }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();

        // 1. Initialize Harmony Memory (HM)
        let mut hm: Vec<Individual> = (0..self.config.population_size)
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

        for _iter in 0..self.config.max_iterations {
            // Find current best for history
            let mut best_idx = 0;
            let mut worst_idx = 0;
            for i in 1..hm.len() {
                if hm[i].fitness < hm[best_idx].fitness {
                    best_idx = i;
                }
                if hm[i].fitness > hm[worst_idx].fitness {
                    worst_idx = i;
                }
            }
            history.push(hm[best_idx].fitness);

            // 2. Improvise a New Harmony
            let mut new_vars = Array1::zeros(dim);
            for j in 0..dim {
                if rng.gen::<f64>() < self.hmcr {
                    // Memory Consideration: pick from HM
                    let r_idx = rng.gen_range(0..self.config.population_size);
                    new_vars[j] = hm[r_idx].variables[j];

                    // Pitch Adjustment
                    if rng.gen::<f64>() < self.par {
                        let range = upper[j] - lower[j];
                        let delta = (rng.gen::<f64>() - 0.5) * 2.0 * self.bw * range;
                        new_vars[j] = (new_vars[j] + delta).clamp(lower[j], upper[j]);
                    }
                } else {
                    // Random selection
                    new_vars[j] = rng.gen_range(lower[j]..upper[j]);
                }
            }

            let new_fitness = problem.fitness(&new_vars);

            // 3. Update Harmony Memory
            if new_fitness < hm[worst_idx].fitness {
                hm[worst_idx] = Individual::new(new_vars, new_fitness);
            }
        }

        // Final sort to find best
        hm.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
        let best = &hm[0];

        OptimizationResult {
            best_variables: best.variables.clone(),
            best_fitness: best.fitness,
            history,
        }
    }
}
