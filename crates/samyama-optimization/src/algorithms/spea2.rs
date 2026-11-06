//! SPEA2: the Strength Pareto Evolutionary Algorithm 2 (OPT-13, #1803).
//!
//! Zitzler, Laumanns and Thiele, "SPEA2: Improving the Strength Pareto
//! Evolutionary Algorithm", TIK-Report 103, ETH Zurich, 2001
//! (https://doi.org/10.3929/ethz-a-004284029).
//!
//! NSGA-II ranks by non-dominated fronts and breaks ties by crowding. SPEA2
//! keeps a separate archive of the best points found and gives every point a
//! single fitness:
//!
//! - its **strength** `S(i)` is how many points it dominates;
//! - its **raw fitness** `R(i)` is the sum of the strengths of the points that
//!   dominate it, so a non-dominated point has `R = 0`;
//! - its **density** `D(i) = 1 / (sigma_k + 2)`, where `sigma_k` is the
//!   distance in objective space to its k-th nearest neighbour, separates
//!   points with equal `R` and is always below 1.
//!
//! `F = R + D`, lower is better, and `F < 1` exactly when a point is
//! non-dominated. The next archive is every non-dominated point; when that is
//! too many, the point closest to its nearest neighbour is removed until it
//! fits (ties go to the second-nearest, and so on), which keeps the front's
//! boundary points; when too few, the best dominated points fill it.
//!
//! Dominance is the crate's constrained dominance
//! ([`crate::moo::constrained_dominates`]): a feasible point dominates every
//! infeasible one, so an infeasible point reaches the returned front only
//! when nothing feasible was found.
//!
//! Variation is simulated binary crossover and polynomial mutation (Deb and
//! Agrawal 1995; Deb and Goyal 1996), the operators SPEA2 is usually compared
//! with. The archive is as large as the population, as in the paper's runs.

use crate::common::{
    MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult, SolverConfig,
};
use crate::moo::constrained_dominates;
use ndarray::Array1;
use rand::prelude::*;

/// Distribution index of simulated binary crossover: larger keeps children
/// closer to their parents.
pub const CROSSOVER_ETA: f64 = 15.0;
/// Distribution index of polynomial mutation.
pub const MUTATION_ETA: f64 = 20.0;
/// Probability that a pair of parents is crossed at all.
pub const CROSSOVER_RATE: f64 = 0.9;
/// Below this, two parent coordinates are treated as equal and not crossed.
const SAME_COORDINATE: f64 = 1e-14;

pub struct SPEA2Solver {
    pub config: SolverConfig,
    /// Seed for reproducible runs; `None` draws from entropy (#455).
    pub seed: Option<u64>,
}

impl SPEA2Solver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config, seed: None }
    }

    /// Fix the seed so this solver's run can be re-derived (#455).
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn solve<P: MultiObjectiveProblem>(&self, problem: &P) -> MultiObjectiveResult {
        let mut rng = crate::common::rng::solver_rng(self.seed);
        let dim = problem.dim();
        let (lower, upper) = problem.bounds();
        let size = self.config.population_size.max(2);
        let evaluate = |vars: Array1<f64>| {
            let fitness = crate::common::evaluate_objectives(problem, &vars);
            let violation = problem.penalties(&vars).iter().sum();
            MultiObjectiveIndividual::new(vars, fitness, violation)
        };

        let mut population: Vec<MultiObjectiveIndividual> = (0..size)
            .map(|_| {
                evaluate(Array1::from_iter(
                    (0..dim).map(|j| rng.gen_range(lower[j]..upper[j])),
                ))
            })
            .collect();
        let mut archive: Vec<MultiObjectiveIndividual> = Vec::new();
        let mut history = Vec::with_capacity(self.config.max_iterations);

        for _ in 0..self.config.max_iterations {
            if problem.budget_exhausted() {
                break;
            }
            let mut union = archive;
            union.append(&mut population);
            let fitness = strength_fitness(&union);
            let archive_fitness;
            (archive, archive_fitness) = environmental_selection(union, &fitness, size);
            history.push(best_first_objective(&archive, &archive_fitness));

            population = (0..size)
                .map(|_| {
                    let a = tournament(&archive_fitness, &mut rng);
                    let b = tournament(&archive_fitness, &mut rng);
                    let mut child = sbx(
                        &archive[a].variables,
                        &archive[b].variables,
                        &lower,
                        &upper,
                        &mut rng,
                    );
                    polynomial_mutation(&mut child, &lower, &upper, &mut rng);
                    evaluate(child)
                })
                .collect();
        }

        // The final generation is offspring the archive has not seen yet.
        let mut union = archive;
        union.append(&mut population);
        let fitness = strength_fitness(&union);
        let (archive, archive_fitness) = environmental_selection(union, &fitness, size);
        MultiObjectiveResult {
            pareto_front: archive
                .into_iter()
                .zip(archive_fitness)
                .filter(|(_, f)| *f < 1.0)
                .map(|(mut ind, _)| {
                    ind.rank = 0;
                    ind
                })
                .collect(),
            history,
        }
    }
}

fn dominates(a: &MultiObjectiveIndividual, b: &MultiObjectiveIndividual) -> bool {
    constrained_dominates(
        &a.fitness,
        a.constraint_violation,
        &b.fitness,
        b.constraint_violation,
    )
}

fn distance(a: &[f64], b: &[f64]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f64>()
        .sqrt()
}

/// `F(i) = R(i) + D(i)` for every point of `union`; see the module comment.
fn strength_fitness(union: &[MultiObjectiveIndividual]) -> Vec<f64> {
    let n = union.len();
    let strength: Vec<usize> = (0..n)
        .map(|i| (0..n).filter(|&j| dominates(&union[i], &union[j])).count())
        .collect();
    // k = sqrt(sample size), as in the paper.
    let k = ((n as f64).sqrt() as usize).clamp(1, n.saturating_sub(1).max(1));
    (0..n)
        .map(|i| {
            let raw: usize = (0..n)
                .filter(|&j| dominates(&union[j], &union[i]))
                .map(|j| strength[j])
                .sum();
            let mut distances: Vec<f64> = (0..n)
                .filter(|&j| j != i)
                .map(|j| distance(&union[i].fitness, &union[j].fitness))
                .collect();
            distances.sort_by(f64::total_cmp);
            let sigma_k = distances.get(k - 1).copied().unwrap_or(0.0);
            // A NaN or infinite distance (a NaN objective was mapped to +inf)
            // makes no density contribution rather than poisoning `F`.
            let density = if sigma_k.is_finite() {
                1.0 / (sigma_k + 2.0)
            } else {
                0.0
            };
            raw as f64 + density
        })
        .collect()
}

/// The next archive of `size` points from `union`, with their fitness.
fn environmental_selection(
    union: Vec<MultiObjectiveIndividual>,
    fitness: &[f64],
    size: usize,
) -> (Vec<MultiObjectiveIndividual>, Vec<f64>) {
    let mut order: Vec<usize> = (0..union.len()).collect();
    order.sort_by(|&a, &b| fitness[a].total_cmp(&fitness[b]));
    let non_dominated = order.iter().take_while(|&&i| fitness[i] < 1.0).count();
    let mut chosen: Vec<usize> = if non_dominated <= size {
        // Every non-dominated point, then the best dominated ones.
        order.into_iter().take(size).collect()
    } else {
        truncate(&union, order[..non_dominated].to_vec(), size)
    };
    chosen.sort_unstable();
    let mut slots: Vec<Option<MultiObjectiveIndividual>> = union.into_iter().map(Some).collect();
    let fitness = chosen.iter().map(|&i| fitness[i]).collect();
    let archive = chosen
        .iter()
        .map(|&i| slots[i].take().expect("chosen once"))
        .collect();
    (archive, fitness)
}

/// Remove from `members` (indices into `union`) the point nearest its
/// neighbours, one at a time, until `size` remain. "Nearest" compares each
/// point's sorted distances to the others: the smallest first, then the
/// second-smallest on a tie, and so on.
fn truncate(
    union: &[MultiObjectiveIndividual],
    mut members: Vec<usize>,
    size: usize,
) -> Vec<usize> {
    while members.len() > size {
        let neighbours = |i: usize| {
            let mut d: Vec<f64> = members
                .iter()
                .filter(|&&j| j != i)
                .map(|&j| distance(&union[i].fitness, &union[j].fitness))
                .collect();
            d.sort_by(f64::total_cmp);
            d
        };
        let crowded = (0..members.len())
            .min_by(|&a, &b| {
                let (da, db) = (neighbours(members[a]), neighbours(members[b]));
                da.iter()
                    .zip(&db)
                    .map(|(x, y)| x.total_cmp(y))
                    .find(|o| o.is_ne())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .expect("more than `size` members");
        members.swap_remove(crowded);
    }
    members
}

/// The best first objective among the archive's non-dominated points.
fn best_first_objective(archive: &[MultiObjectiveIndividual], fitness: &[f64]) -> f64 {
    archive
        .iter()
        .zip(fitness)
        .filter(|(_, f)| **f < 1.0)
        .map(|(ind, _)| ind.fitness[0])
        .fold(f64::INFINITY, f64::min)
}

/// Binary tournament on fitness: the index of the better of two draws.
fn tournament(fitness: &[f64], rng: &mut impl Rng) -> usize {
    let a = rng.gen_range(0..fitness.len());
    let b = rng.gen_range(0..fitness.len());
    if fitness[b] < fitness[a] {
        b
    } else {
        a
    }
}

/// Simulated binary crossover: one child, each coordinate spread around its
/// parents with a polynomial distribution of index [`CROSSOVER_ETA`], then
/// kept in bounds.
fn sbx(
    p1: &Array1<f64>,
    p2: &Array1<f64>,
    lower: &Array1<f64>,
    upper: &Array1<f64>,
    rng: &mut impl Rng,
) -> Array1<f64> {
    let mut child = p1.clone();
    if rng.gen::<f64>() > CROSSOVER_RATE {
        return child;
    }
    for j in 0..child.len() {
        if rng.gen_bool(0.5) || (p1[j] - p2[j]).abs() < SAME_COORDINATE {
            continue;
        }
        let u: f64 = rng.gen();
        let beta = if u <= 0.5 {
            (2.0 * u).powf(1.0 / (CROSSOVER_ETA + 1.0))
        } else {
            (1.0 / (2.0 * (1.0 - u))).powf(1.0 / (CROSSOVER_ETA + 1.0))
        };
        let mid = 0.5 * (p1[j] + p2[j]);
        let half = 0.5 * beta * (p1[j] - p2[j]).abs();
        let value = if rng.gen_bool(0.5) {
            mid - half
        } else {
            mid + half
        };
        child[j] = value.clamp(lower[j], upper[j]);
    }
    child
}

/// Polynomial mutation of each coordinate with probability `1 / dim`, kept
/// in bounds.
fn polynomial_mutation(
    vars: &mut Array1<f64>,
    lower: &Array1<f64>,
    upper: &Array1<f64>,
    rng: &mut impl Rng,
) {
    let rate = 1.0 / vars.len().max(1) as f64;
    for j in 0..vars.len() {
        if rng.gen::<f64>() >= rate {
            continue;
        }
        let u: f64 = rng.gen();
        let delta = if u < 0.5 {
            (2.0 * u).powf(1.0 / (MUTATION_ETA + 1.0)) - 1.0
        } else {
            1.0 - (2.0 * (1.0 - u)).powf(1.0 / (MUTATION_ETA + 1.0))
        };
        vars[j] = (vars[j] + delta * (upper[j] - lower[j])).clamp(lower[j], upper[j]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::moo::igd;

    const SEED: u64 = 13;
    const ZDT_DIM: usize = 10;

    fn config(population_size: usize, max_iterations: usize) -> SolverConfig {
        SolverConfig {
            population_size,
            max_iterations,
        }
    }

    fn point(f: &[f64], violation: f64) -> MultiObjectiveIndividual {
        MultiObjectiveIndividual::new(Array1::zeros(1), f.to_vec(), violation)
    }

    /// ZDT1: its front is `f2 = 1 - sqrt(f1)` where the tail is 0.
    struct Zdt1;

    impl MultiObjectiveProblem for Zdt1 {
        fn dim(&self) -> usize {
            ZDT_DIM
        }
        fn num_objectives(&self) -> usize {
            2
        }
        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            (Array1::zeros(ZDT_DIM), Array1::ones(ZDT_DIM))
        }
        fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
            let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (ZDT_DIM as f64 - 1.0);
            vec![x[0], g * (1.0 - (x[0] / g).sqrt())]
        }
    }

    fn zdt1_front() -> Vec<Vec<f64>> {
        (0..100)
            .map(|i| {
                let f1 = i as f64 / 99.0;
                vec![f1, 1.0 - f1.sqrt()]
            })
            .collect()
    }

    fn igd_of(r: &MultiObjectiveResult) -> f64 {
        let points: Vec<Vec<f64>> = r.pareto_front.iter().map(|i| i.fitness.clone()).collect();
        igd(&points, &zdt1_front())
    }

    #[test]
    fn strength_raw_fitness_and_density_follow_the_paper() {
        // a=(0,1) and b=(1,0) each dominate c=(1,1); nothing dominates them.
        let union = [
            point(&[0.0, 1.0], 0.0),
            point(&[1.0, 0.0], 0.0),
            point(&[1.0, 1.0], 0.0),
        ];
        let f = strength_fitness(&union);
        // Non-dominated: R = 0, so F is the density alone, below 1.
        assert!(f[0] < 1.0 && f[1] < 1.0, "{f:?}");
        // c is dominated by two points of strength 1 each: R = 2.
        assert!((2.0..3.0).contains(&f[2]), "{f:?}");
        // k = floor(sqrt(3)) = 1: a's nearest is c at distance 1.
        assert!((f[0] - 1.0 / 3.0).abs() < 1e-12, "{f:?}");
    }

    #[test]
    fn a_feasible_point_outranks_every_infeasible_one() {
        // The infeasible point has better objectives on both counts.
        let union = [point(&[1.0, 1.0], 0.0), point(&[0.0, 0.0], 0.5)];
        let f = strength_fitness(&union);
        assert!(f[0] < 1.0 && f[1] >= 1.0, "{f:?}");
    }

    #[test]
    fn truncation_removes_the_most_crowded_point_and_keeps_the_ends() {
        let union = [
            point(&[0.0, 1.0], 0.0),
            point(&[0.5, 0.5], 0.0),
            point(&[0.51, 0.49], 0.0), // nearly on top of the one before
            point(&[1.0, 0.0], 0.0),
        ];
        let mut kept = truncate(&union, vec![0, 1, 2, 3], 3);
        kept.sort_unstable();
        assert!(
            kept.contains(&0) && kept.contains(&3),
            "the ends stay: {kept:?}"
        );
        assert_eq!(kept.len(), 3);
        assert!(
            !(kept.contains(&1) && kept.contains(&2)),
            "one of the near pair goes: {kept:?}"
        );
    }

    #[test]
    fn the_archive_fills_with_the_best_dominated_points_when_too_few_lead() {
        let union = vec![
            point(&[0.0, 0.0], 0.0),
            point(&[1.0, 1.0], 0.0),
            point(&[2.0, 2.0], 0.0),
        ];
        let f = strength_fitness(&union);
        let (archive, fitness) = environmental_selection(union, &f, 2);
        assert_eq!(archive.len(), 2);
        let firsts: Vec<f64> = archive.iter().map(|i| i.fitness[0]).collect();
        assert_eq!(firsts, vec![0.0, 1.0], "the leader, then the next best");
        assert!(fitness[0] < 1.0 && fitness[1] >= 1.0);
    }

    #[test]
    fn it_converges_on_zdt1() {
        let few = SPEA2Solver::new(config(40, 2)).with_seed(SEED).solve(&Zdt1);
        let many = SPEA2Solver::new(config(40, 150))
            .with_seed(SEED)
            .solve(&Zdt1);
        let (igd_few, igd_many) = (igd_of(&few), igd_of(&many));
        assert!(
            igd_many < igd_few,
            "150 generations {igd_many} vs 2 {igd_few}"
        );
        assert!(igd_many < 0.1, "IGD to ZDT1's front {igd_many}");
        assert_eq!(many.history.len(), 150);
    }

    #[test]
    fn the_returned_front_is_mutually_non_dominated_and_within_the_archive_size() {
        let r = SPEA2Solver::new(config(30, 60))
            .with_seed(SEED)
            .solve(&Zdt1);
        assert!(!r.pareto_front.is_empty() && r.pareto_front.len() <= 30);
        for a in &r.pareto_front {
            for b in &r.pareto_front {
                assert!(
                    !dominates(a, b),
                    "{:?} dominates {:?}",
                    a.fitness,
                    b.fitness
                );
            }
            assert!(a.variables.iter().all(|v| (0.0..=1.0).contains(v)));
        }
    }

    #[test]
    fn the_same_seed_gives_the_same_front() {
        let run = || {
            SPEA2Solver::new(config(20, 30))
                .with_seed(SEED)
                .solve(&Zdt1)
                .pareto_front
                .into_iter()
                .map(|i| i.fitness)
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn crossover_and_mutation_stay_in_bounds() {
        let mut rng = crate::common::rng::solver_rng(Some(SEED));
        let (lo, hi) = (Array1::from_elem(4, -1.0), Array1::from_elem(4, 1.0));
        let (a, b) = (Array1::from_elem(4, -1.0), Array1::from_elem(4, 1.0));
        for _ in 0..1000 {
            let mut c = sbx(&a, &b, &lo, &hi, &mut rng);
            polynomial_mutation(&mut c, &lo, &hi, &mut rng);
            assert!(c.iter().all(|v| (-1.0..=1.0).contains(v)), "{c:?}");
        }
    }
}
