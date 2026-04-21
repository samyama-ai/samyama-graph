//! MO-Rao+DE — multi-objective hybrid Rao + Differential Evolution
//! (Engineering Optimization 2025, doi:10.1080/0305215X.2025.2463976).
//!
//! Each iteration: with probability p_de, generate offspring via DE/rand/1/bin
//! mutation + binomial crossover; otherwise via Rao-1 update against rank-0
//! best and worst-rank worst. Selection by NSGA-II-style FNDS + crowding.

use crate::common::{
    MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult, SolverConfig,
};
use crate::moo::evaluate_population;
use ndarray::Array1;
use rand::prelude::*;

pub struct MORaoDESolver {
    pub config: SolverConfig,
    /// Probability of using DE branch per offspring.
    pub p_de: f64,
    /// DE scale factor F.
    pub f: f64,
    /// DE crossover rate CR.
    pub cr: f64,
}

impl MORaoDESolver {
    pub fn new(config: SolverConfig) -> Self {
        Self {
            config,
            p_de: 0.5,
            f: 0.5,
            cr: 0.9,
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
                    "MO-Rao+DE Solver: Iteration {}/{}",
                    iter, self.config.max_iterations
                );
            }
            let elites: Vec<usize> = population
                .iter()
                .enumerate()
                .filter(|(_, i)| i.rank == 0)
                .map(|(i, _)| i)
                .collect();
            let worst_rank = population.iter().map(|i| i.rank).max().unwrap_or(0);
            let worst_pool: Vec<usize> = population
                .iter()
                .enumerate()
                .filter(|(_, i)| i.rank == worst_rank)
                .map(|(i, _)| i)
                .collect();

            let mut offspring: Vec<MultiObjectiveIndividual> = Vec::with_capacity(pop_size);
            for k in 0..pop_size {
                let mut new_vars = Array1::zeros(dim);
                if rng.gen::<f64>() < self.p_de {
                    // DE/rand/1/bin
                    let mut idxs: Vec<usize> = (0..pop_size).filter(|&i| i != k).collect();
                    idxs.shuffle(&mut rng);
                    let (a, b, c) = (idxs[0], idxs[1], idxs[2]);
                    let j_rand = rng.gen_range(0..dim);
                    for j in 0..dim {
                        let mutant = population[a].variables[j]
                            + self.f
                                * (population[b].variables[j] - population[c].variables[j]);
                        let pick = rng.gen::<f64>() < self.cr || j == j_rand;
                        new_vars[j] = if pick {
                            mutant.clamp(lower[j], upper[j])
                        } else {
                            population[k].variables[j]
                        };
                    }
                } else {
                    // Rao-1 against random elite/worst pair
                    let best = &population[elites[rng.gen_range(0..elites.len())]].variables;
                    let worst =
                        &population[worst_pool[rng.gen_range(0..worst_pool.len())]].variables;
                    let r1: f64 = rng.gen();
                    for j in 0..dim {
                        let val = population[k].variables[j] + r1 * (best[j] - worst[j]);
                        new_vars[j] = val.clamp(lower[j], upper[j]);
                    }
                }
                let fit = problem.objectives(&new_vars);
                let viol: f64 = problem.penalties(&new_vars).iter().sum();
                offspring.push(MultiObjectiveIndividual::new(new_vars, fit, viol));
            }

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

            history.push(
                population
                    .iter()
                    .filter(|i| i.rank == 0)
                    .map(|i| i.fitness[0])
                    .fold(f64::INFINITY, f64::min),
            );
        }

        MultiObjectiveResult {
            pareto_front: population.into_iter().filter(|i| i.rank == 0).collect(),
            history,
        }
    }
}
