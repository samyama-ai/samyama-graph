//! SAMP-Jaya — Self-Adaptive Multi-Population Jaya (Rao & Saroj 2017).
//!
//! Partitions population into `m` sub-populations. Each sub-population evolves
//! independently using the standard Jaya update against its own best/worst.
//! After every iteration, `m` is adapted:
//!   - if global best improved this iteration → increase m by 1 (more diversity)
//!   - else → decrease m by 1 (more exploitation, larger sub-pops)
//! Bounds: m ∈ [1, m_max] where m_max defaults to floor(pop/4).

use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct SAMPJayaSolver {
    pub config: SolverConfig,
    pub m_max: Option<usize>,
}

impl SAMPJayaSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config, m_max: None }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;
        let m_max = self.m_max.unwrap_or((pop_size / 4).max(2));

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

        let mut history = Vec::with_capacity(self.config.max_iterations);
        let mut m: usize = 2;
        let mut last_global_best = population
            .iter()
            .map(|ind| ind.fitness)
            .fold(f64::INFINITY, f64::min);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!(
                    "SAMP-Jaya Solver: Iteration {}/{} (m={})",
                    iter, self.config.max_iterations, m
                );
            }

            // Sort by fitness then partition into m contiguous sub-populations.
            population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
            let chunk = (pop_size + m - 1) / m;
            let mut new_pop: Vec<Individual> = Vec::with_capacity(pop_size);

            for sub in population.chunks(chunk) {
                let (sb, sw) = find_best_worst(sub);
                let sb_vars = sub[sb].variables.clone();
                let sw_vars = sub[sw].variables.clone();
                let updated: Vec<Individual> = sub
                    .par_iter()
                    .map(|ind| {
                        let mut local_rng = thread_rng();
                        let r1: f64 = local_rng.gen();
                        let r2: f64 = local_rng.gen();
                        let mut new_vars = Array1::zeros(dim);
                        for j in 0..dim {
                            let val = ind.variables[j]
                                + r1 * (sb_vars[j] - ind.variables[j].abs())
                                - r2 * (sw_vars[j] - ind.variables[j].abs());
                            new_vars[j] = val.clamp(lower[j], upper[j]);
                        }
                        let new_fitness = problem.fitness(&new_vars);
                        if new_fitness < ind.fitness {
                            Individual::new(new_vars, new_fitness)
                        } else {
                            ind.clone()
                        }
                    })
                    .collect();
                new_pop.extend(updated);
            }
            population = new_pop;

            let global_best = population
                .iter()
                .map(|ind| ind.fitness)
                .fold(f64::INFINITY, f64::min);
            history.push(global_best);

            // Self-adaptation
            if global_best < last_global_best {
                m = (m + 1).min(m_max);
            } else {
                m = m.saturating_sub(1).max(1);
            }
            last_global_best = global_best;
        }

        let (best_idx, _) = find_best_worst(&population);
        let best = &population[best_idx];
        OptimizationResult {
            best_variables: best.variables.clone(),
            best_fitness: best.fitness,
            history,
        }
    }
}

fn find_best_worst(population: &[Individual]) -> (usize, usize) {
    let mut best_idx = 0;
    let mut worst_idx = 0;
    for (i, ind) in population.iter().enumerate() {
        if ind.fitness < population[best_idx].fitness {
            best_idx = i;
        }
        if ind.fitness > population[worst_idx].fitness {
            worst_idx = i;
        }
    }
    (best_idx, worst_idx)
}
