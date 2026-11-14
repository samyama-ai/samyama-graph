//! MOEA/D: multi-objective evolutionary algorithm based on decomposition
//! (OPT-13, #1803).
//!
//! Zhang and Li, "MOEA/D: A Multiobjective Evolutionary Algorithm Based on
//! Decomposition", IEEE Transactions on Evolutionary Computation 11(6), 2007
//! (https://doi.org/10.1109/TEVC.2007.892759).
//!
//! NSGA-II and SPEA2 rank a whole population by dominance. MOEA/D instead
//! turns the problem into `N` single-objective sub-problems, one per weight
//! vector `lambda`, and solves them together:
//!
//! - each sub-problem minimises the **Tchebycheff** value
//!   `g(x | lambda, z*) = max_m lambda_m * |f_m(x) - z*_m|`, where `z*` is the
//!   best value seen so far for each objective (the ideal point);
//! - each sub-problem has a **neighbourhood**, the `T` sub-problems whose
//!   weights are nearest its own; parents are drawn from it, and a child
//!   replaces every neighbour it does better for.
//!
//! Neighbouring weights have neighbouring optima, so what one sub-problem
//! learns is useful to the next, and the weights spread the population along
//! the front without a crowding measure.
//!
//! Constraints follow the crate's rule for every multi-objective solver: when
//! a child is compared with a neighbour, a feasible point beats an infeasible
//! one, the smaller violation wins between two infeasible ones, and only two
//! feasible points are compared by Tchebycheff value. The returned front is
//! the final population's non-dominated points under constrained dominance.
//!
//! Variation is simulated binary crossover and polynomial mutation, as in the
//! paper's continuous experiments. The weight vectors are an even spread for
//! two objectives; for three or more, a simplex lattice (Das and Dennis 1998)
//! of the largest size that fits, topped up with seeded random points on the
//! simplex, so the population is exactly `population_size`.

use super::variation::{polynomial_mutation, sbx};
use crate::common::{
    MultiObjectiveIndividual, MultiObjectiveProblem, MultiObjectiveResult, SolverConfig,
};
use crate::moo::constrained_dominates;
use ndarray::Array1;
use rand::prelude::*;

/// Neighbourhood size as a fraction of the population: the paper's `T = 20`
/// for `N = 100`; never fewer than [`MIN_NEIGHBOURS`]. A tenth was tried
/// first and spread good solutions too slowly: on 30-D ZDT1 at population 40
/// and 100 generations its mean IGD was 0.42 against 0.13 for a fifth.
pub const NEIGHBOURHOOD_FRACTION: f64 = 0.2;
/// The smallest neighbourhood: a sub-problem and one other, so two parents.
pub const MIN_NEIGHBOURS: usize = 2;

pub struct MOEADSolver {
    pub config: SolverConfig,
    /// Seed for reproducible runs; `None` draws from entropy (#455).
    pub seed: Option<u64>,
}

impl MOEADSolver {
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
        let objectives = problem.num_objectives().max(1);
        let (lower, upper) = problem.bounds();
        let size = self.config.population_size.max(MIN_NEIGHBOURS);
        let evaluate = |vars: Array1<f64>| {
            let fitness = crate::common::evaluate_objectives(problem, &vars);
            let violation = problem.penalties(&vars).iter().sum();
            MultiObjectiveIndividual::new(vars, fitness, violation)
        };

        let weights = weight_vectors(size, objectives, &mut rng);
        let neighbours = neighbourhoods(&weights, neighbourhood_size(size));
        let mut population: Vec<MultiObjectiveIndividual> = (0..size)
            .map(|_| {
                evaluate(Array1::from_iter(
                    (0..dim).map(|j| rng.gen_range(lower[j]..upper[j])),
                ))
            })
            .collect();
        let mut ideal = vec![f64::INFINITY; objectives];
        for ind in &population {
            update_ideal(&mut ideal, &ind.fitness);
        }
        let mut history = Vec::with_capacity(self.config.max_iterations);

        for _ in 0..self.config.max_iterations {
            if problem.budget_exhausted() {
                break;
            }
            history.push(best_first_objective(&population));
            for hood in &neighbours {
                let a = hood[rng.gen_range(0..hood.len())];
                let b = hood[rng.gen_range(0..hood.len())];
                let mut child = sbx(
                    &population[a].variables,
                    &population[b].variables,
                    &lower,
                    &upper,
                    &mut rng,
                );
                polynomial_mutation(&mut child, &lower, &upper, &mut rng);
                let child = evaluate(child);
                update_ideal(&mut ideal, &child.fitness);
                for &j in hood {
                    if replaces(&child, &population[j], &weights[j], &ideal) {
                        population[j] = child.clone();
                    }
                }
            }
        }

        let front = population
            .iter()
            .enumerate()
            .filter(|(i, a)| {
                !population.iter().enumerate().any(|(j, b)| {
                    // Of two identical points, keep the first only.
                    *i != j && (dominates(b, a) || (j < *i && b.fitness == a.fitness))
                })
            })
            .map(|(_, a)| {
                let mut a = a.clone();
                a.rank = 0;
                a
            })
            .collect();
        MultiObjectiveResult {
            pareto_front: front,
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

fn neighbourhood_size(size: usize) -> usize {
    ((NEIGHBOURHOOD_FRACTION * size as f64).round() as usize).clamp(MIN_NEIGHBOURS, size)
}

/// `size` weight vectors on the unit simplex in `objectives` dimensions.
fn weight_vectors(size: usize, objectives: usize, rng: &mut impl Rng) -> Vec<Vec<f64>> {
    if objectives == 1 {
        return vec![vec![1.0]; size];
    }
    if objectives == 2 {
        let steps = (size - 1).max(1) as f64;
        return (0..size)
            .map(|i| {
                let w = i as f64 / steps;
                vec![w, 1.0 - w]
            })
            .collect();
    }
    // The largest simplex lattice with no more than `size` points.
    let mut divisions = 1;
    while lattice_size(divisions + 1, objectives) <= size {
        divisions += 1;
    }
    let mut weights = Vec::new();
    lattice(divisions, objectives, &mut Vec::new(), &mut weights);
    weights.truncate(size);
    // Topped up with uniform points on the simplex: normalised exponentials.
    while weights.len() < size {
        let draws: Vec<f64> = (0..objectives)
            .map(|_| -rng.gen::<f64>().max(f64::MIN_POSITIVE).ln())
            .collect();
        let sum: f64 = draws.iter().sum();
        weights.push(draws.iter().map(|d| d / sum).collect());
    }
    weights
}

/// Points of the simplex lattice with `divisions` steps: `C(divisions +
/// objectives - 1, objectives - 1)`.
fn lattice_size(divisions: usize, objectives: usize) -> usize {
    let (n, k) = (divisions + objectives - 1, objectives - 1);
    (0..k).fold(1usize, |acc, i| acc.saturating_mul(n - i) / (i + 1))
}

fn lattice(divisions: usize, objectives: usize, prefix: &mut Vec<usize>, out: &mut Vec<Vec<f64>>) {
    let used: usize = prefix.iter().sum();
    if prefix.len() + 1 == objectives {
        let mut w: Vec<f64> = prefix
            .iter()
            .map(|&c| c as f64 / divisions as f64)
            .collect();
        w.push((divisions - used) as f64 / divisions as f64);
        out.push(w);
        return;
    }
    for c in 0..=(divisions - used) {
        prefix.push(c);
        lattice(divisions, objectives, prefix, out);
        prefix.pop();
    }
}

/// For each weight, the indices of the `t` nearest weights, itself first.
fn neighbourhoods(weights: &[Vec<f64>], t: usize) -> Vec<Vec<usize>> {
    weights
        .iter()
        .map(|w| {
            let mut order: Vec<(f64, usize)> = weights
                .iter()
                .enumerate()
                .map(|(j, v)| {
                    (
                        w.iter().zip(v).map(|(a, b)| (a - b).powi(2)).sum::<f64>(),
                        j,
                    )
                })
                .collect();
            order.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            order.into_iter().take(t).map(|(_, j)| j).collect()
        })
        .collect()
}

fn update_ideal(ideal: &mut [f64], fitness: &[f64]) {
    for (z, f) in ideal.iter_mut().zip(fitness) {
        if f.is_finite() && *f < *z {
            *z = *f;
        }
    }
}

/// `max_m lambda_m * |f_m - z*_m|`. A weight of 0 still counts its objective
/// a little, so a corner sub-problem is not indifferent to the other one.
fn tchebycheff(fitness: &[f64], weight: &[f64], ideal: &[f64]) -> f64 {
    /// What a zero weight counts as, as is usual for Tchebycheff.
    const ZERO_WEIGHT: f64 = 1e-6;
    fitness
        .iter()
        .zip(weight)
        .zip(ideal)
        .map(|((f, w), z)| w.max(ZERO_WEIGHT) * (f - z).abs())
        .fold(0.0, f64::max)
}

/// Whether `child` should take `current`'s place in the sub-problem `weight`.
fn replaces(
    child: &MultiObjectiveIndividual,
    current: &MultiObjectiveIndividual,
    weight: &[f64],
    ideal: &[f64],
) -> bool {
    let (vc, vx) = (child.constraint_violation, current.constraint_violation);
    if vc > 0.0 || vx > 0.0 {
        return vc < vx;
    }
    tchebycheff(&child.fitness, weight, ideal) <= tchebycheff(&current.fitness, weight, ideal)
}

/// The best first objective in the population, the solvers' common history.
fn best_first_objective(population: &[MultiObjectiveIndividual]) -> f64 {
    population
        .iter()
        .map(|ind| ind.fitness[0])
        .fold(f64::INFINITY, f64::min)
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

    /// DTLZ2 with three objectives: its front is the positive unit sphere.
    struct Dtlz2;

    impl MultiObjectiveProblem for Dtlz2 {
        fn dim(&self) -> usize {
            7
        }
        fn num_objectives(&self) -> usize {
            3
        }
        fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
            (Array1::zeros(7), Array1::ones(7))
        }
        fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
            use std::f64::consts::FRAC_PI_2;
            let g: f64 = x.iter().skip(2).map(|v| (v - 0.5).powi(2)).sum();
            let (a, b) = (x[0] * FRAC_PI_2, x[1] * FRAC_PI_2);
            vec![
                (1.0 + g) * a.cos() * b.cos(),
                (1.0 + g) * a.cos() * b.sin(),
                (1.0 + g) * a.sin(),
            ]
        }
    }

    fn zdt1_igd(r: &MultiObjectiveResult) -> f64 {
        let front: Vec<Vec<f64>> = (0..100)
            .map(|i| {
                let f1 = i as f64 / 99.0;
                vec![f1, 1.0 - f1.sqrt()]
            })
            .collect();
        let points: Vec<Vec<f64>> = r.pareto_front.iter().map(|i| i.fitness.clone()).collect();
        igd(&points, &front)
    }

    #[test]
    fn two_objective_weights_are_an_even_spread_on_the_simplex() {
        let w = weight_vectors(5, 2, &mut crate::common::rng::solver_rng(Some(SEED)));
        assert_eq!(
            w,
            vec![
                vec![0.0, 1.0],
                vec![0.25, 0.75],
                vec![0.5, 0.5],
                vec![0.75, 0.25],
                vec![1.0, 0.0]
            ]
        );
    }

    #[test]
    fn three_objective_weights_are_a_lattice_topped_up_to_the_population() {
        // 10 = C(5, 2) is the lattice with 3 divisions; 12 needs 2 more.
        assert_eq!(lattice_size(3, 3), 10);
        let w = weight_vectors(12, 3, &mut crate::common::rng::solver_rng(Some(SEED)));
        assert_eq!(w.len(), 12);
        for v in &w {
            assert!((v.iter().sum::<f64>() - 1.0).abs() < 1e-12, "{v:?}");
            assert!(v.iter().all(|x| *x >= 0.0), "{v:?}");
        }
        assert!(
            w.contains(&vec![0.0, 0.0, 1.0]) && w.contains(&vec![1.0, 0.0, 0.0]),
            "the corners"
        );
    }

    #[test]
    fn a_neighbourhood_is_the_nearest_weights_itself_first() {
        let w = weight_vectors(5, 2, &mut crate::common::rng::solver_rng(Some(SEED)));
        let n = neighbourhoods(&w, 3);
        assert_eq!(n[2][0], 2);
        let mut middle = n[2].clone();
        middle.sort_unstable();
        assert_eq!(middle, vec![1, 2, 3]);
        assert_eq!(n[0], vec![0, 1, 2], "an end's neighbours are on one side");
    }

    #[test]
    fn tchebycheff_is_the_largest_weighted_gap_to_the_ideal_point() {
        let v = tchebycheff(&[3.0, 2.0], &[0.25, 0.75], &[1.0, 1.0]);
        // max(0.25 * 2, 0.75 * 1) = 0.75
        assert!((v - 0.75).abs() < 1e-12, "{v}");
    }

    #[test]
    fn feasibility_decides_a_replacement_before_the_objectives_do() {
        let (w, z) = ([0.5, 0.5], [0.0, 0.0]);
        let good_but_infeasible = point(&[0.0, 0.0], 0.4);
        let feasible = point(&[5.0, 5.0], 0.0);
        assert!(!replaces(&good_but_infeasible, &feasible, &w, &z));
        assert!(replaces(&feasible, &good_but_infeasible, &w, &z));
        assert!(
            replaces(&point(&[9.0, 9.0], 0.1), &point(&[0.0, 0.0], 0.2), &w, &z),
            "less violation"
        );
    }

    #[test]
    fn it_converges_on_zdt1() {
        let few = MOEADSolver::new(config(40, 2)).with_seed(SEED).solve(&Zdt1);
        let many = MOEADSolver::new(config(40, 150))
            .with_seed(SEED)
            .solve(&Zdt1);
        let (igd_few, igd_many) = (zdt1_igd(&few), zdt1_igd(&many));
        assert!(
            igd_many < igd_few,
            "150 generations {igd_many} vs 2 {igd_few}"
        );
        assert!(igd_many < 0.1, "IGD to ZDT1's front {igd_many}");
        assert_eq!(many.history.len(), 150);
    }

    #[test]
    fn on_three_objectives_it_reaches_the_dtlz2_sphere() {
        let r = MOEADSolver::new(config(36, 150))
            .with_seed(SEED)
            .solve(&Dtlz2);
        assert!(r.pareto_front.len() > 1);
        for p in &r.pareto_front {
            let radius = p.fitness.iter().map(|f| f * f).sum::<f64>().sqrt();
            assert!(
                (radius - 1.0).abs() < 0.1,
                "a front point at radius {radius}"
            );
        }
    }

    #[test]
    fn the_returned_front_is_mutually_non_dominated_and_in_bounds() {
        let r = MOEADSolver::new(config(30, 60))
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
            MOEADSolver::new(config(20, 30))
                .with_seed(SEED)
                .solve(&Zdt1)
                .pareto_front
                .into_iter()
                .map(|i| i.fitness)
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }
}
