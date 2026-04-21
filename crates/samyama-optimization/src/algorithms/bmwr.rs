//! BMWR — Best-Mean-Worst-Random algorithm (Rao 2025).
//!
//! Parameter-free, metaphor-free metaheuristic combining BMR's best-vs-mean
//! attraction with BWR's worst-repulsion. Introduced by Rao in the Metals 2025
//! paper (mdpi.com/2075-4701/15/9/1057) alongside BMR/BWR.
//!
//! Update rule (per variable j, candidate k, iteration i):
//!
//! if r4 > 0.5:
//!   V'_{j,k} = V_{j,k}
//!            + r1·(V_{j,best} − T·V_{j,mean})       (BMR-style attraction to best vs mean)
//!            + r2·(V_{j,best} − V_{j,random})       (best-vs-random pull)
//!            − r5·(V_{j,worst} − V_{j,random})      (BWR-style repulsion from worst)
//! else:
//!   V'_{j,k} = U_j − (U_j − L_j)·r3                  (random restart)
//!
//! Greedy acceptance: keep V' iff fitness improves.

use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct BMWRSolver {
    pub config: SolverConfig,
}

impl BMWRSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config }
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;

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

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!("BMWR Solver: Iteration {}/{}", iter, self.config.max_iterations);
            }
            let (best_idx, worst_idx) = find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            let mean_vars = mean_vec(&population, dim);
            let best_fitness = population[best_idx].fitness;
            history.push(best_fitness);

            // Snapshot variables for population-based random pick (paper's intent).
            let snapshot: Vec<Array1<f64>> =
                population.iter().map(|ind| ind.variables.clone()).collect();

            population = population
                .into_par_iter()
                .enumerate()
                .map(|(k, mut ind)| {
                    let mut local_rng = thread_rng();
                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();
                    let r3: f64 = local_rng.gen();
                    let r4: f64 = local_rng.gen();
                    let r5: f64 = local_rng.gen();
                    let t: f64 = local_rng.gen_range(1..3) as f64;

                    let mut new_vars = Array1::zeros(dim);
                    if r4 > 0.5 {
                        // Pick a random candidate that is not k itself.
                        let mut rand_k = local_rng.gen_range(0..pop_size);
                        if rand_k == k && pop_size > 1 {
                            rand_k = (rand_k + 1) % pop_size;
                        }
                        let rand_vars = &snapshot[rand_k];
                        for j in 0..dim {
                            let delta = r1 * (best_vars[j] - t * mean_vars[j])
                                + r2 * (best_vars[j] - rand_vars[j])
                                - r5 * (worst_vars[j] - rand_vars[j]);
                            new_vars[j] = (ind.variables[j] + delta).clamp(lower[j], upper[j]);
                        }
                    } else {
                        for j in 0..dim {
                            new_vars[j] =
                                (upper[j] - (upper[j] - lower[j]) * r3).clamp(lower[j], upper[j]);
                        }
                    }

                    let new_fitness = problem.fitness(&new_vars);
                    if new_fitness < ind.fitness {
                        ind.variables = new_vars;
                        ind.fitness = new_fitness;
                    }
                    ind
                })
                .collect();
        }

        let (final_best_idx, _) = find_best_worst(&population);
        let final_best = &population[final_best_idx];
        OptimizationResult {
            best_variables: final_best.variables.clone(),
            best_fitness: final_best.fitness,
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

fn mean_vec(population: &[Individual], dim: usize) -> Array1<f64> {
    let mut mean = Array1::zeros(dim);
    for ind in population {
        mean += &ind.variables;
    }
    mean / (population.len() as f64)
}
