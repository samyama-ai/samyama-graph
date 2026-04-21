//! QO-Rao — Quasi-Oppositional Rao algorithm.
//!
//! Combines the Rao update (variant-selectable) with quasi-opposition-based
//! learning (QOBL): for every iteration, after the Rao update, generate the
//! quasi-opposite of each individual and keep whichever is better.
//!
//! Quasi-opposite point of x in [a, b]:
//!   c   = (a + b) / 2
//!   xo  = a + b − x        (opposite)
//!   xqo = uniform(min(c,xo), max(c,xo))
//!
//! Reference: Rao & Saroj (2020) "Quasi-oppositional-based Rao algorithms for
//! multi-objective design optimization of selected heat sinks" (JCDE).

use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use super::rao::RaoVariant;
use ndarray::Array1;
use rand::prelude::*;
use rayon::prelude::*;

pub struct QORaoSolver {
    pub config: SolverConfig,
    pub variant: RaoVariant,
}

impl QORaoSolver {
    pub fn new(config: SolverConfig, variant: RaoVariant) -> Self {
        Self { config, variant }
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

        // Initial QOBL seeding: combine population + quasi-opposite, keep best N.
        let qo_init: Vec<Individual> = population
            .par_iter()
            .map(|ind| {
                let mut local_rng = thread_rng();
                let qo_vars = quasi_oppose(&ind.variables, &lower, &upper, &mut local_rng);
                let fitness = problem.fitness(&qo_vars);
                Individual::new(qo_vars, fitness)
            })
            .collect();
        population.extend(qo_init);
        population.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
        population.truncate(pop_size);

        let mut history = Vec::with_capacity(self.config.max_iterations);

        for iter in 0..self.config.max_iterations {
            if iter % 10 == 0 {
                println!(
                    "QO-Rao Solver: Iteration {}/{}",
                    iter, self.config.max_iterations
                );
            }
            let (best_idx, worst_idx) = find_best_worst(&population);
            let best_vars = population[best_idx].variables.clone();
            let worst_vars = population[worst_idx].variables.clone();
            history.push(population[best_idx].fitness);

            population = population
                .into_par_iter()
                .map(|mut ind| {
                    let mut local_rng = thread_rng();
                    let r1: f64 = local_rng.gen();
                    let r2: f64 = local_rng.gen();

                    // Random vector for Rao-2/Rao-3 (sampled from bounds; matches existing rao.rs).
                    let mut rand_vars = Array1::zeros(dim);
                    let need_rand =
                        matches!(self.variant, RaoVariant::Rao2 | RaoVariant::Rao3);
                    if need_rand {
                        for j in 0..dim {
                            rand_vars[j] = local_rng.gen_range(lower[j]..upper[j]);
                        }
                    }
                    let rand_fitness = if need_rand {
                        problem.fitness(&rand_vars)
                    } else {
                        0.0
                    };

                    // 1. Rao update (mirrors src/algorithms/rao.rs)
                    let mut new_vars = Array1::zeros(dim);
                    for j in 0..dim {
                        let term1 = best_vars[j] - worst_vars[j];
                        let delta = match self.variant {
                            RaoVariant::Rao1 => r1 * term1,
                            RaoVariant::Rao2 => {
                                let term2 = if ind.fitness < rand_fitness {
                                    ind.variables[j] - rand_vars[j]
                                } else {
                                    rand_vars[j] - ind.variables[j]
                                };
                                r1 * term1 + r2 * term2
                            }
                            RaoVariant::Rao3 => {
                                let term1_abs = best_vars[j] - worst_vars[j].abs();
                                let term2_abs = if ind.fitness < rand_fitness {
                                    ind.variables[j] - rand_vars[j]
                                } else {
                                    rand_vars[j] - ind.variables[j]
                                };
                                r1 * term1_abs + r2 * term2_abs
                            }
                        };
                        new_vars[j] = (ind.variables[j] + delta).clamp(lower[j], upper[j]);
                    }
                    let rao_fitness = problem.fitness(&new_vars);
                    if rao_fitness < ind.fitness {
                        ind.variables = new_vars;
                        ind.fitness = rao_fitness;
                    }

                    // 2. QOBL on the (possibly updated) individual
                    let qo_vars = quasi_oppose(&ind.variables, &lower, &upper, &mut local_rng);
                    let qo_fitness = problem.fitness(&qo_vars);
                    if qo_fitness < ind.fitness {
                        ind.variables = qo_vars;
                        ind.fitness = qo_fitness;
                    }
                    ind
                })
                .collect();
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

fn quasi_oppose(
    x: &Array1<f64>,
    lower: &Array1<f64>,
    upper: &Array1<f64>,
    rng: &mut impl Rng,
) -> Array1<f64> {
    let dim = x.len();
    let mut out = Array1::zeros(dim);
    for j in 0..dim {
        let c = (lower[j] + upper[j]) / 2.0;
        let xo = lower[j] + upper[j] - x[j];
        let (lo, hi) = if c < xo { (c, xo) } else { (xo, c) };
        out[j] = if (hi - lo).abs() < 1e-12 {
            c
        } else {
            rng.gen_range(lo..hi)
        }
        .clamp(lower[j], upper[j]);
    }
    out
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
