//! L-SHADE: success-history adaptive differential evolution with linear
//! population size reduction (OPT-13, #1803).
//!
//! Tanabe and Fukunaga, "Improving the search performance of SHADE using
//! linear population size reduction", IEEE CEC 2014
//! (https://doi.org/10.1109/CEC.2014.6900380). It won the CEC 2014
//! real-parameter single-objective competition.
//!
//! Plain DE (`DESolver`) uses one fixed `F` and `CR` for the whole run. L-SHADE
//! learns them: each trial draws its `F` and `CR` around values kept in a small
//! memory, and the memory moves toward the values that produced improvements,
//! weighted by how much each one improved. The population shrinks linearly from
//! its starting size to [`MIN_POPULATION`], spending early evaluations on
//! exploration and late ones on refinement.
//!
//! One adaptation from the paper: it schedules the reduction by evaluations
//! spent against a maximum. Every solver here is configured by iterations
//! instead, and reports one `history` entry per iteration, so the reduction is
//! scheduled by generation: generation `g` of `max_iterations` has
//! `round(N_init + (N_min - N_init) * g / max_iterations)` members. The
//! starting size is `config.population_size`, where the paper uses `18 * dim`.

use crate::common::{Individual, OptimizationResult, Problem, SolverConfig};
use ndarray::Array1;
use rand::prelude::*;
use rand_distr::{Cauchy, Normal};

/// Entries in each success-history memory (the paper's `H`).
pub const MEMORY_SIZE: usize = 6;
/// The population never shrinks below this: current-to-pbest/1 needs the
/// target, a pbest and two more distinct members.
pub const MIN_POPULATION: usize = 4;
/// Fraction of the population the pbest is drawn from (the paper's `p`).
pub const PBEST_RATE: f64 = 0.11;
/// Archive capacity as a multiple of the population (the paper's `r_arc`).
pub const ARCHIVE_RATE: f64 = 1.4;
/// Starting value of every memory entry.
const INITIAL_MEMORY: f64 = 0.5;
/// Spread of the distributions `F` and `CR` are drawn from.
const SCALE: f64 = 0.1;

pub struct LSHADESolver {
    pub config: SolverConfig,
    /// Seed for reproducible runs; `None` draws from entropy (#455).
    pub seed: Option<u64>,
}

impl LSHADESolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config, seed: None }
    }

    /// Fix the seed so this solver's run can be re-derived (#455).
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Members in generation `generation`: linear from the starting size to
    /// [`MIN_POPULATION`] over the run.
    fn population_at(&self, start: usize, generation: usize) -> usize {
        let iterations = self.config.max_iterations.max(1) as f64;
        let shrink = (start - MIN_POPULATION) as f64 * generation as f64 / iterations;
        (start as f64 - shrink).round() as usize
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        self.run(problem).0
    }

    /// The solve, and the `CR` and `F` memories it ended with.
    fn run<P: Problem>(&self, problem: &P) -> (OptimizationResult, Memories) {
        let mut rng = crate::common::rng::solver_rng(self.seed);
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let start = self.config.population_size.max(MIN_POPULATION);

        let mut population: Vec<Individual> = (0..start)
            .map(|_| {
                let vars = Array1::from_iter((0..dim).map(|j| rng.gen_range(lower[j]..upper[j])));
                let fitness = crate::common::evaluate(problem, &vars);
                Individual::new(vars, fitness)
            })
            .collect();

        let mut memory_cr = [INITIAL_MEMORY; MEMORY_SIZE];
        let mut memory_f = [INITIAL_MEMORY; MEMORY_SIZE];
        let mut next_slot = 0;
        let mut archive: Vec<Array1<f64>> = Vec::new();
        let mut history = Vec::with_capacity(self.config.max_iterations);

        for generation in 0..self.config.max_iterations {
            population.sort_by(|a, b| a.fitness.total_cmp(&b.fitness));
            history.push(population[0].fitness);

            let n = population.len();
            let pbest_pool = ((PBEST_RATE * n as f64).round() as usize).clamp(2, n);
            let mut successes: Vec<(f64, f64, f64)> = Vec::new(); // (CR, F, improvement)
            let mut next = population.clone();

            for i in 0..n {
                let slot = rng.gen_range(0..MEMORY_SIZE);
                let cr = draw_cr(&mut rng, memory_cr[slot]);
                let f = draw_f(&mut rng, memory_f[slot]);

                let pbest = &population[rng.gen_range(0..pbest_pool)].variables;
                let r1 = pick_other(&mut rng, n, &[i]);
                // r2 comes from the population and the archive together.
                let r2 = loop {
                    let r = rng.gen_range(0..n + archive.len());
                    if r != i && r != r1 {
                        break r;
                    }
                };
                let x2 = if r2 < n {
                    &population[r2].variables
                } else {
                    &archive[r2 - n]
                };

                let target = &population[i].variables;
                let forced = rng.gen_range(0..dim);
                let trial = Array1::from_iter((0..dim).map(|j| {
                    if j == forced || rng.gen::<f64>() < cr {
                        let v = target[j]
                            + f * (pbest[j] - target[j])
                            + f * (population[r1].variables[j] - x2[j]);
                        repair(v, target[j], lower[j], upper[j])
                    } else {
                        target[j]
                    }
                }));

                let trial_fitness = crate::common::evaluate(problem, &trial);
                if trial_fitness <= population[i].fitness {
                    if trial_fitness < population[i].fitness {
                        archive.push(target.clone());
                        successes.push((cr, f, population[i].fitness - trial_fitness));
                    }
                    next[i] = Individual::new(trial, trial_fitness);
                }
            }

            if let Some((cr, f)) = weighted_lehmer_means(&successes) {
                memory_cr[next_slot] = cr;
                memory_f[next_slot] = f;
                next_slot = (next_slot + 1) % MEMORY_SIZE;
            }

            // Linear population size reduction: drop the worst.
            next.sort_by(|a, b| a.fitness.total_cmp(&b.fitness));
            next.truncate(
                self.population_at(start, generation + 1)
                    .max(MIN_POPULATION),
            );
            population = next;

            let archive_cap = (ARCHIVE_RATE * population.len() as f64).round() as usize;
            while archive.len() > archive_cap {
                let r = rng.gen_range(0..archive.len());
                archive.swap_remove(r);
            }
        }

        population.sort_by(|a, b| a.fitness.total_cmp(&b.fitness));
        let best = &population[0];
        let result = OptimizationResult {
            best_variables: best.variables.clone(),
            best_fitness: best.fitness,
            history,
        };
        (result, (memory_cr, memory_f))
    }
}

/// The success-history memories: `CR`'s, then `F`'s.
type Memories = ([f64; MEMORY_SIZE], [f64; MEMORY_SIZE]);

/// `CR` from a normal distribution around the memory value, clipped to [0, 1].
fn draw_cr(rng: &mut impl Rng, mean: f64) -> f64 {
    Normal::new(mean, SCALE)
        .map(|d| d.sample(rng))
        .unwrap_or(mean)
        .clamp(0.0, 1.0)
}

/// `F` from a Cauchy distribution around the memory value: redrawn while not
/// positive, and capped at 1.
fn draw_f(rng: &mut impl Rng, location: f64) -> f64 {
    let Ok(cauchy) = Cauchy::new(location, SCALE) else {
        return location.clamp(f64::MIN_POSITIVE, 1.0);
    };
    loop {
        let f = cauchy.sample(rng);
        if f > 0.0 {
            return f.min(1.0);
        }
    }
}

/// An index in `0..n` that is none of `exclude`.
fn pick_other(rng: &mut impl Rng, n: usize, exclude: &[usize]) -> usize {
    loop {
        let r = rng.gen_range(0..n);
        if !exclude.contains(&r) {
            return r;
        }
    }
}

/// A coordinate pushed out of bounds lands halfway between the bound and where
/// it started, as in the paper, rather than on the bound itself.
///
/// So an optimum that lies exactly on a bound is approached, halving the gap
/// each time, but not landed on: on `or.solve`'s linear-cost problem, whose
/// optimum is the lower bound, 40 generations end about 1.5e-4 away where
/// `DESolver`, which clamps onto the bound, reaches it exactly.
fn repair(value: f64, from: f64, lower: f64, upper: f64) -> f64 {
    if value < lower {
        (lower + from) / 2.0
    } else if value > upper {
        (upper + from) / 2.0
    } else {
        value
    }
}

/// The improvement-weighted Lehmer means of the successful `CR`s and `F`s, or
/// `None` when nothing improved this generation.
fn weighted_lehmer_means(successes: &[(f64, f64, f64)]) -> Option<(f64, f64)> {
    let total: f64 = successes.iter().map(|s| s.2).sum();
    // A NaN total is no weight at all, so it is refused with a zero one.
    if successes.is_empty() || total.is_nan() || total <= 0.0 {
        return None;
    }
    let lehmer = |value: fn(&(f64, f64, f64)) -> f64| {
        let num: f64 = successes
            .iter()
            .map(|s| s.2 / total * value(s).powi(2))
            .sum();
        let den: f64 = successes.iter().map(|s| s.2 / total * value(s)).sum();
        if den > 0.0 {
            num / den
        } else {
            0.0
        }
    };
    Some((lehmer(|s| s.0), lehmer(|s| s.1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SimpleProblem;

    const SEED: u64 = 3;

    fn config(population_size: usize, max_iterations: usize) -> SolverConfig {
        SolverConfig {
            population_size,
            max_iterations,
        }
    }

    fn sphere(dim: usize) -> SimpleProblem<fn(&Array1<f64>) -> f64> {
        SimpleProblem {
            objective_func: |x| x.iter().map(|v| v * v).sum(),
            dim,
            lower: Array1::from_elem(dim, -5.0),
            upper: Array1::from_elem(dim, 5.0),
        }
    }

    #[test]
    fn the_population_shrinks_linearly_to_the_minimum() {
        let s = LSHADESolver::new(config(40, 10));
        assert_eq!(s.population_at(40, 0), 40);
        assert_eq!(s.population_at(40, 5), 22);
        assert_eq!(s.population_at(40, 10), MIN_POPULATION);
    }

    #[test]
    fn it_converges_on_the_sphere() {
        let r = LSHADESolver::new(config(50, 200))
            .with_seed(SEED)
            .solve(&sphere(10));
        assert!(r.best_fitness < 1e-6, "best {}", r.best_fitness);
        assert_eq!(r.history.len(), 200);
    }

    #[test]
    fn the_history_never_gets_worse() {
        let r = LSHADESolver::new(config(30, 80))
            .with_seed(SEED)
            .solve(&sphere(5));
        assert!(
            r.history.windows(2).all(|w| w[1] <= w[0]),
            "{:?}",
            r.history
        );
        assert!(r.best_fitness <= *r.history.last().unwrap());
    }

    #[test]
    fn the_same_seed_gives_the_same_answer() {
        let run = || {
            LSHADESolver::new(config(20, 40))
                .with_seed(SEED)
                .solve(&sphere(4))
                .best_fitness
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn every_answer_stays_inside_the_bounds() {
        let r = LSHADESolver::new(config(20, 30))
            .with_seed(SEED)
            .solve(&sphere(6));
        assert!(r.best_variables.iter().all(|v| (-5.0..=5.0).contains(v)));
    }

    #[test]
    fn a_population_smaller_than_the_minimum_is_raised_to_it() {
        let r = LSHADESolver::new(config(1, 20))
            .with_seed(SEED)
            .solve(&sphere(3));
        assert!(r.best_fitness.is_finite());
        assert_eq!(r.history.len(), 20);
    }

    #[test]
    fn f_is_positive_and_at_most_one_and_cr_is_a_probability() {
        let mut rng = crate::common::rng::solver_rng(Some(SEED));
        for _ in 0..1000 {
            let f = draw_f(&mut rng, INITIAL_MEMORY);
            assert!(f > 0.0 && f <= 1.0, "{f}");
            let cr = draw_cr(&mut rng, INITIAL_MEMORY);
            assert!((0.0..=1.0).contains(&cr), "{cr}");
        }
    }

    #[test]
    fn an_out_of_bounds_step_lands_halfway_back() {
        assert_eq!(repair(-9.0, -1.0, -5.0, 5.0), -3.0);
        assert_eq!(repair(12.0, 3.0, -5.0, 5.0), 4.0);
        assert_eq!(repair(2.0, 0.0, -5.0, 5.0), 2.0);
    }

    #[test]
    fn the_memory_moves_toward_values_that_improved_most() {
        // Lehmer mean of CR {0.2, 0.8} weighted 1:3 is
        // (0.25*0.04 + 0.75*0.64) / (0.25*0.2 + 0.75*0.8) = 0.49 / 0.65.
        let (cr, f) = weighted_lehmer_means(&[(0.2, 0.5, 1.0), (0.8, 0.5, 3.0)]).unwrap();
        assert!((cr - 0.49 / 0.65).abs() < 1e-12, "{cr}");
        assert!((f - 0.5).abs() < 1e-12, "{f}");
        assert_eq!(weighted_lehmer_means(&[]), None);
        assert_eq!(
            weighted_lehmer_means(&[(0.5, 0.5, 0.0)]),
            None,
            "no improvement"
        );
    }

    #[test]
    fn the_memories_learn_from_the_run() {
        // The adaptation is what L-SHADE adds over DE. Without it every trial
        // would draw around 0.5 for the whole run; with it, the memories end
        // somewhere the successful trials pulled them, and stay valid.
        let (_, (memory_cr, memory_f)) = LSHADESolver::new(config(40, 100))
            .with_seed(SEED)
            .run(&sphere(10));
        let moved = |m: &[f64]| m.iter().any(|v| (v - INITIAL_MEMORY).abs() > 1e-9);
        assert!(moved(&memory_cr), "CR memory never moved: {memory_cr:?}");
        assert!(moved(&memory_f), "F memory never moved: {memory_f:?}");
        assert!(
            memory_cr.iter().all(|v| (0.0..=1.0).contains(v)),
            "{memory_cr:?}"
        );
        assert!(
            memory_f.iter().all(|v| *v > 0.0 && *v <= 1.0),
            "{memory_f:?}"
        );
    }

    #[test]
    fn it_beats_plain_de_on_a_harder_function() {
        // Rastrigin, 10 dimensions: the adaptation is what L-SHADE adds over
        // DE's fixed F and CR, so on a multimodal function it should do better
        // with the same population and iterations.
        let rastrigin = SimpleProblem {
            objective_func: |x: &Array1<f64>| {
                10.0 * x.len() as f64
                    + x.iter()
                        .map(|v| v * v - 10.0 * (2.0 * std::f64::consts::PI * v).cos())
                        .sum::<f64>()
            },
            dim: 10,
            lower: Array1::from_elem(10, -5.12),
            upper: Array1::from_elem(10, 5.12),
        };
        let cfg = || config(60, 300);
        let lshade = LSHADESolver::new(cfg()).with_seed(SEED).solve(&rastrigin);
        let de = crate::algorithms::DESolver::new(cfg())
            .with_seed(SEED)
            .solve(&rastrigin);
        assert!(
            lshade.best_fitness < de.best_fitness,
            "L-SHADE {} vs DE {}",
            lshade.best_fitness,
            de.best_fitness
        );
    }
}
