//! MO-BMR / MO-BWR / MO-BMWR — multi-objective extensions of the BMR/BWR/BMWR
//! family (Rao 2025/2026, MDPI Metals 15/9/1057, MDPI Energies 19/1/34, MDPI
//! JMMP 9/8/249).
//!
//! Wraps the single-objective base update with five features per the JMMP
//! 2025 paper:
//!   1. Elite seeding — preserve top-rank Pareto solutions across iterations.
//!   2. Fast non-dominated sorting (Deb-style; via `crate::moo`).
//!   3. Constraint repairing → penalty fallback.
//!   4. Local exploration — Gaussian perturbation around elites.
//!   5. Edge boosting — extend the Pareto front by perturbing extremes.
//!
//! Total complexity: O(I·(M·c² + c·(m + tf + tp))) per the paper.

use crate::common::{
    MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult, SolverConfig,
};
use crate::moo::{evaluate_population, hypervolume_2d};
use ndarray::Array1;
use rand::prelude::*;
use rand_distr::{Distribution, Normal};

#[derive(Debug, Clone, Copy)]
pub enum MOBMWRVariant {
    MOBMR,
    MOBWR,
    MOBMWR,
}

pub struct MOBMWRSolver {
    pub config: SolverConfig,
    pub variant: MOBMWRVariant,
    /// Local-exploration step size as a fraction of the bound range.
    pub local_step: f64,
    /// Probability of edge boosting per iteration.
    pub edge_boost_prob: f64,
}

impl MOBMWRSolver {
    pub fn new(config: SolverConfig, variant: MOBMWRVariant) -> Self {
        Self {
            config,
            variant,
            local_step: 0.05,
            edge_boost_prob: 0.2,
        }
    }

    pub fn solve<P: MultiObjectiveProblem>(&self, problem: &P) -> MultiObjectiveResult {
        let mut rng = thread_rng();
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let pop_size = self.config.population_size;

        let mut population: Vec<MultiObjectiveIndividual> = (0..pop_size)
            .map(|_| {
                let mut vars = Array1::zeros(dim);
                for i in 0..dim {
                    vars[i] = rng.gen_range(lower[i]..upper[i]);
                }
                let fitness = problem.objectives(&vars);
                let viol: f64 = problem.penalties(&vars).iter().sum();
                MultiObjectiveIndividual::new(vars, fitness, viol)
            })
            .collect();
        evaluate_population(&mut population);

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!(
                    "MO-{:?} Solver: Iteration {}/{}",
                    self.variant, iter, self.config.max_iterations
                );
            }

            // --- Elite seeding: pick best rank-0 individual as the "best" reference.
            let elites: Vec<&MultiObjectiveIndividual> =
                population.iter().filter(|ind| ind.rank == 0).collect();
            if elites.is_empty() {
                break;
            }
            let best_ref = &elites[rng.gen_range(0..elites.len())].variables;
            // For MO, "worst" is taken from the highest rank.
            let worst_rank = population.iter().map(|i| i.rank).max().unwrap_or(0);
            let worst_pool: Vec<&MultiObjectiveIndividual> = population
                .iter()
                .filter(|ind| ind.rank == worst_rank)
                .collect();
            let worst_ref = &worst_pool[rng.gen_range(0..worst_pool.len())].variables;

            // Mean across population.
            let mut mean_vars = Array1::<f64>::zeros(dim);
            for ind in &population {
                mean_vars += &ind.variables;
            }
            mean_vars /= pop_size as f64;

            let snapshot: Vec<Array1<f64>> =
                population.iter().map(|i| i.variables.clone()).collect();

            // --- Generate offspring via base update.
            let mut offspring: Vec<MultiObjectiveIndividual> = Vec::with_capacity(pop_size);
            for k in 0..pop_size {
                let mut local_rng = thread_rng();
                let r1: f64 = local_rng.gen();
                let r2: f64 = local_rng.gen();
                let r3: f64 = local_rng.gen();
                let r4: f64 = local_rng.gen();
                let r5: f64 = local_rng.gen();
                let t: f64 = local_rng.gen_range(1..3) as f64;

                let mut new_vars = Array1::zeros(dim);
                if r4 > 0.5 {
                    let mut rk = local_rng.gen_range(0..pop_size);
                    if rk == k && pop_size > 1 {
                        rk = (rk + 1) % pop_size;
                    }
                    let rand_vars = &snapshot[rk];
                    let v = &population[k].variables;
                    for j in 0..dim {
                        let delta = match self.variant {
                            MOBMWRVariant::MOBMR => {
                                r1 * (best_ref[j] - t * mean_vars[j])
                                    + r2 * (best_ref[j] - rand_vars[j])
                            }
                            MOBMWRVariant::MOBWR => {
                                r1 * (best_ref[j] - t * rand_vars[j])
                                    - r2 * (worst_ref[j] - rand_vars[j])
                            }
                            MOBMWRVariant::MOBMWR => {
                                r1 * (best_ref[j] - t * mean_vars[j])
                                    + r2 * (best_ref[j] - rand_vars[j])
                                    - r5 * (worst_ref[j] - rand_vars[j])
                            }
                        };
                        new_vars[j] = (v[j] + delta).clamp(lower[j], upper[j]);
                    }
                } else {
                    for j in 0..dim {
                        new_vars[j] =
                            (upper[j] - (upper[j] - lower[j]) * r3).clamp(lower[j], upper[j]);
                    }
                }

                // Constraint repair: clip to bounds (already done) — for box constraints
                // this is full repair. For inequality penalties, fall through to penalty.
                let fit = problem.objectives(&new_vars);
                let viol: f64 = problem.penalties(&new_vars).iter().sum();
                offspring.push(MultiObjectiveIndividual::new(new_vars, fit, viol));
            }

            // --- Local exploration: Gaussian perturbation around random elite.
            let normal = Normal::new(0.0, 1.0).unwrap();
            for _ in 0..(pop_size / 10).max(1) {
                let elite = &elites[rng.gen_range(0..elites.len())].variables;
                let mut new_vars = elite.clone();
                for j in 0..dim {
                    let sigma = self.local_step * (upper[j] - lower[j]);
                    new_vars[j] = (new_vars[j] + sigma * normal.sample(&mut rng))
                        .clamp(lower[j], upper[j]);
                }
                let fit = problem.objectives(&new_vars);
                let viol: f64 = problem.penalties(&new_vars).iter().sum();
                offspring.push(MultiObjectiveIndividual::new(new_vars, fit, viol));
            }

            // --- Edge boosting: occasionally push extreme objectives further.
            if rng.gen::<f64>() < self.edge_boost_prob {
                let m = problem.num_objectives();
                for obj in 0..m {
                    // Find current extreme on objective `obj`.
                    let extreme_idx = elites
                        .iter()
                        .enumerate()
                        .min_by(|(_, a), (_, b)| {
                            a.fitness[obj].partial_cmp(&b.fitness[obj]).unwrap()
                        })
                        .map(|(i, _)| i)
                        .unwrap_or(0);
                    let mut new_vars = elites[extreme_idx].variables.clone();
                    for j in 0..dim {
                        let sigma = 0.5 * self.local_step * (upper[j] - lower[j]);
                        new_vars[j] = (new_vars[j] + sigma * normal.sample(&mut rng))
                            .clamp(lower[j], upper[j]);
                    }
                    let fit = problem.objectives(&new_vars);
                    let viol: f64 = problem.penalties(&new_vars).iter().sum();
                    offspring.push(MultiObjectiveIndividual::new(new_vars, fit, viol));
                }
            }

            // --- Combine and select via FNDS + crowding.
            population.extend(offspring);
            evaluate_population(&mut population);
            population.sort_by(|a, b| {
                if a.rank != b.rank {
                    a.rank.cmp(&b.rank)
                } else {
                    b.crowding_distance
                        .partial_cmp(&a.crowding_distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
            });
            population.truncate(pop_size);

            // History: hypervolume if 2-objective, else first objective of best.
            let hist_val = if problem.num_objectives() == 2 {
                let front: Vec<Vec<f64>> = population
                    .iter()
                    .filter(|i| i.rank == 0)
                    .map(|i| i.fitness.clone())
                    .collect();
                let max0 = front
                    .iter()
                    .map(|p| p[0])
                    .fold(f64::NEG_INFINITY, f64::max)
                    .max(1.0);
                let max1 = front
                    .iter()
                    .map(|p| p[1])
                    .fold(f64::NEG_INFINITY, f64::max)
                    .max(1.0);
                hypervolume_2d(&front, [max0 + 1.0, max1 + 1.0])
            } else {
                population
                    .iter()
                    .filter(|i| i.rank == 0)
                    .map(|i| i.fitness[0])
                    .fold(f64::INFINITY, f64::min)
            };
            history.push(hist_val);
        }

        MultiObjectiveResult {
            pareto_front: population.into_iter().filter(|i| i.rank == 0).collect(),
            history,
        }
    }
}
