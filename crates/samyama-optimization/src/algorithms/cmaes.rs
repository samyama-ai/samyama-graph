//! CMA-ES: the covariance matrix adaptation evolution strategy (OPT-13,
//! #1803).
//!
//! Hansen and Ostermeier, "Completely Derandomized Self-Adaptation in
//! Evolution Strategies", Evolutionary Computation 9(2), 2001
//! (https://doi.org/10.1162/106365601750190398), with the default parameters
//! and update rules of Hansen, "The CMA Evolution Strategy: A Tutorial",
//! 2016 (https://arxiv.org/abs/1604.00772).
//!
//! Each generation samples `lambda` candidates from a multivariate normal
//! distribution `N(m, sigma^2 C)` and moves it toward the best `mu` of them:
//!
//! - the **mean** `m` becomes their weighted average;
//! - the **covariance** `C` learns the directions successful steps took (a
//!   rank-one update from the evolution path `p_c` and a rank-`mu` update from
//!   this generation's steps), so the cloud stretches along a valley and a
//!   badly scaled problem becomes, to the search, a round one;
//! - the **step size** `sigma` grows when successive steps point the same way
//!   and shrinks when they cancel (cumulative step-size adaptation, from the
//!   path `p_sigma`).
//!
//! The search runs in coordinates normalised to `[0, 1]` per variable, so one
//! `sigma` fits every variable whatever its range; a candidate is clamped to
//! the bounds before it is evaluated, and the clamped point is the one the
//! update learns from. `population_size` is `lambda`; the paper's default is
//! `4 + floor(3 ln n)`.
//!
//! Each generation decomposes `C` (an `n x n` symmetric matrix) by the cyclic
//! Jacobi method, which is `O(n^3)`: CMA-ES is meant for tens to a few
//! hundred variables, not thousands.

use crate::common::{OptimizationResult, Problem, SolverConfig};
use ndarray::{Array1, Array2};
use rand::prelude::*;
use rand_distr::StandardNormal;

/// The smallest population: `mu = lambda / 2` must be at least 2.
pub const MIN_POPULATION: usize = 4;
/// The initial step size, as a fraction of each variable's range.
pub const INITIAL_SIGMA: f64 = 0.3;
/// Eigenvalues of `C` are kept at least this, so `C^{-1/2}` stays finite.
const MIN_EIGENVALUE: f64 = 1e-20;

/// The strategy parameters for `n` variables and `lambda` candidates, as in
/// Table 1 of the tutorial.
#[derive(Debug, Clone)]
struct Parameters {
    /// `mu` of them, one per selected candidate, best first.
    weights: Vec<f64>,
    mu_eff: f64,
    c_sigma: f64,
    d_sigma: f64,
    c_c: f64,
    c_1: f64,
    c_mu: f64,
    /// `E||N(0, I)||`, the length of a random step under no selection.
    chi_n: f64,
}

impl Parameters {
    fn new(n: usize, lambda: usize) -> Self {
        let nf = n as f64;
        let mu = lambda / 2;
        let raw: Vec<f64> = (1..=mu)
            .map(|i| ((lambda as f64 + 1.0) / 2.0).ln() - (i as f64).ln())
            .collect();
        let total: f64 = raw.iter().sum();
        let weights: Vec<f64> = raw.iter().map(|w| w / total).collect();
        let mu_eff = 1.0 / weights.iter().map(|w| w * w).sum::<f64>();
        let c_sigma = (mu_eff + 2.0) / (nf + mu_eff + 5.0);
        let d_sigma = 1.0 + 2.0 * (((mu_eff - 1.0) / (nf + 1.0)).sqrt() - 1.0).max(0.0) + c_sigma;
        let c_c = (4.0 + mu_eff / nf) / (nf + 4.0 + 2.0 * mu_eff / nf);
        let c_1 = 2.0 / ((nf + 1.3).powi(2) + mu_eff);
        let c_mu =
            (1.0 - c_1).min(2.0 * (mu_eff - 2.0 + 1.0 / mu_eff) / ((nf + 2.0).powi(2) + mu_eff));
        let chi_n = nf.sqrt() * (1.0 - 1.0 / (4.0 * nf) + 1.0 / (21.0 * nf * nf));
        Self {
            weights,
            mu_eff,
            c_sigma,
            d_sigma,
            c_c,
            c_1,
            c_mu,
            chi_n,
        }
    }
}

pub struct CMAESSolver {
    pub config: SolverConfig,
    /// Seed for reproducible runs; `None` draws from entropy (#455).
    pub seed: Option<u64>,
}

impl CMAESSolver {
    pub fn new(config: SolverConfig) -> Self {
        Self { config, seed: None }
    }

    /// Fix the seed so this solver's run can be re-derived (#455).
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    pub fn solve<P: Problem>(&self, problem: &P) -> OptimizationResult {
        let mut rng = crate::common::rng::solver_rng(self.seed);
        let n = problem.dim();
        let (lower, upper) = problem.bounds();
        let lambda = self.config.population_size.max(MIN_POPULATION);
        let p = Parameters::new(n, lambda);
        let to_problem = |u: &Array1<f64>| {
            Array1::from_iter(
                (0..n).map(|j| lower[j] + u[j].clamp(0.0, 1.0) * (upper[j] - lower[j])),
            )
        };

        let mut mean = Array1::from_iter((0..n).map(|_| rng.gen::<f64>()));
        let mut sigma = INITIAL_SIGMA;
        let mut c = Array2::<f64>::eye(n);
        let mut p_sigma = Array1::<f64>::zeros(n);
        let mut p_c = Array1::<f64>::zeros(n);
        let mut best_vars = to_problem(&mean);
        let mut best_fitness = crate::common::evaluate(problem, &best_vars);
        let mut history = Vec::with_capacity(self.config.max_iterations);

        for generation in 0..self.config.max_iterations {
            if problem.budget_exhausted() {
                break;
            }
            history.push(best_fitness);
            let (b, d) = eigen_symmetric(&c);
            let d_sqrt: Vec<f64> = d.iter().map(|v| v.max(MIN_EIGENVALUE).sqrt()).collect();

            // Sample, clamp to the unit box, evaluate.
            let mut samples: Vec<(f64, Array1<f64>)> = (0..lambda)
                .map(|_| {
                    let z: Array1<f64> =
                        Array1::from_iter((0..n).map(|_| rng.sample::<f64, _>(StandardNormal)));
                    let scaled = Array1::from_iter((0..n).map(|k| d_sqrt[k] * z[k]));
                    let x = (&mean + &(b.dot(&scaled) * sigma)).mapv(|v| v.clamp(0.0, 1.0));
                    let fitness = crate::common::evaluate(problem, &to_problem(&x));
                    (fitness, x)
                })
                .collect();
            samples.sort_by(|a, b| a.0.total_cmp(&b.0));
            if samples[0].0 < best_fitness {
                best_fitness = samples[0].0;
                best_vars = to_problem(&samples[0].1);
            }

            // Mean: the weighted average of the best mu.
            let old_mean = mean.clone();
            mean = Array1::zeros(n);
            for (w, (_, x)) in p.weights.iter().zip(&samples) {
                mean = mean + x * *w;
            }
            let y_w = (&mean - &old_mean) / sigma;

            // Step-size path: the step, whitened by C^{-1/2}.
            let inv_sqrt = Array1::from_iter(d_sqrt.iter().map(|v| 1.0 / v));
            let whitened = b.dot(&(b.t().dot(&y_w) * &inv_sqrt));
            p_sigma = &p_sigma * (1.0 - p.c_sigma)
                + &whitened * (p.c_sigma * (2.0 - p.c_sigma) * p.mu_eff).sqrt();
            let p_sigma_norm = p_sigma.dot(&p_sigma).sqrt();
            let damping = (1.0 - (1.0 - p.c_sigma).powi(2 * (generation as i32 + 1))).sqrt();
            let h_sigma = p_sigma_norm / damping < (1.4 + 2.0 / (n as f64 + 1.0)) * p.chi_n;

            // Covariance path, then the rank-one and rank-mu updates.
            let h = if h_sigma { 1.0 } else { 0.0 };
            p_c = &p_c * (1.0 - p.c_c) + &y_w * (h * (p.c_c * (2.0 - p.c_c) * p.mu_eff).sqrt());
            let rank_one = outer(&p_c, &p_c);
            let mut rank_mu = Array2::<f64>::zeros((n, n));
            for (w, (_, x)) in p.weights.iter().zip(&samples) {
                let y = (x - &old_mean) / sigma;
                rank_mu = rank_mu + outer(&y, &y) * *w;
            }
            let lost = (1.0 - h) * p.c_c * (2.0 - p.c_c);
            c = &c * (1.0 - p.c_1 - p.c_mu) + (rank_one + &c * lost) * p.c_1 + rank_mu * p.c_mu;
            // Keep C exactly symmetric against rounding.
            c = (&c + &c.t()) * 0.5;

            sigma *= ((p.c_sigma / p.d_sigma) * (p_sigma_norm / p.chi_n - 1.0)).exp();
            if !sigma.is_finite() || sigma <= 0.0 {
                break;
            }
        }

        OptimizationResult {
            best_variables: best_vars,
            best_fitness,
            history,
        }
    }
}

fn outer(a: &Array1<f64>, b: &Array1<f64>) -> Array2<f64> {
    Array2::from_shape_fn((a.len(), b.len()), |(i, j)| a[i] * b[j])
}

/// Eigenvectors (as columns) and eigenvalues of a symmetric matrix, by the
/// cyclic Jacobi method: rotate away each off-diagonal entry in turn until
/// they are all negligible.
fn eigen_symmetric(a: &Array2<f64>) -> (Array2<f64>, Vec<f64>) {
    /// Sweeps over every off-diagonal entry before giving up; Jacobi
    /// converges quadratically, so a well-formed matrix needs far fewer.
    const MAX_SWEEPS: usize = 100;
    /// Off-diagonal mass, relative to the whole matrix, counted as zero.
    const TOLERANCE: f64 = 1e-24;
    let n = a.nrows();
    let mut m = a.clone();
    let mut v = Array2::<f64>::eye(n);
    let total: f64 = m.iter().map(|x| x * x).sum::<f64>().max(f64::MIN_POSITIVE);
    for _ in 0..MAX_SWEEPS {
        let off: f64 = (0..n)
            .flat_map(|i| (0..n).filter(move |&j| j != i).map(move |j| (i, j)))
            .map(|(i, j)| m[(i, j)].powi(2))
            .sum();
        if off <= TOLERANCE * total {
            break;
        }
        for p in 0..n {
            for q in (p + 1)..n {
                if m[(p, q)] == 0.0 {
                    continue;
                }
                let theta = (m[(q, q)] - m[(p, p)]) / (2.0 * m[(p, q)]);
                let t = theta.signum() / (theta.abs() + (theta * theta + 1.0).sqrt());
                let t = if theta == 0.0 { 1.0 } else { t };
                let cos = 1.0 / (t * t + 1.0).sqrt();
                let sin = t * cos;
                for k in 0..n {
                    let (mkp, mkq) = (m[(k, p)], m[(k, q)]);
                    m[(k, p)] = cos * mkp - sin * mkq;
                    m[(k, q)] = sin * mkp + cos * mkq;
                }
                for k in 0..n {
                    let (mpk, mqk) = (m[(p, k)], m[(q, k)]);
                    m[(p, k)] = cos * mpk - sin * mqk;
                    m[(q, k)] = sin * mpk + cos * mqk;
                }
                for k in 0..n {
                    let (vkp, vkq) = (v[(k, p)], v[(k, q)]);
                    v[(k, p)] = cos * vkp - sin * vkq;
                    v[(k, q)] = sin * vkp + cos * vkq;
                }
            }
        }
    }
    let values = (0..n).map(|i| m[(i, i)]).collect();
    (v, values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SimpleProblem;

    const SEED: u64 = 7;

    fn config(population_size: usize, max_iterations: usize) -> SolverConfig {
        SolverConfig {
            population_size,
            max_iterations,
        }
    }

    fn problem(dim: usize, f: fn(&Array1<f64>) -> f64) -> SimpleProblem<fn(&Array1<f64>) -> f64> {
        SimpleProblem {
            objective_func: f,
            dim,
            lower: Array1::from_elem(dim, -5.0),
            upper: Array1::from_elem(dim, 5.0),
        }
    }

    fn sphere(x: &Array1<f64>) -> f64 {
        x.iter().map(|v| v * v).sum()
    }

    /// Condition number 1e6: axis `i` is scaled by `10^(6 i / (n - 1))`.
    fn ellipsoid(x: &Array1<f64>) -> f64 {
        let n = x.len() as f64;
        x.iter()
            .enumerate()
            .map(|(i, v)| 10f64.powf(6.0 * i as f64 / (n - 1.0)) * v * v)
            .sum()
    }

    fn rosenbrock(x: &Array1<f64>) -> f64 {
        x.windows(2)
            .into_iter()
            .map(|w| 100.0 * (w[1] - w[0] * w[0]).powi(2) + (1.0 - w[0]).powi(2))
            .sum()
    }

    #[test]
    fn eigen_decomposes_a_known_matrix() {
        let a = ndarray::arr2(&[[2.0, 1.0], [1.0, 2.0]]);
        let (_, mut values) = eigen_symmetric(&a);
        values.sort_by(f64::total_cmp);
        assert!(
            (values[0] - 1.0).abs() < 1e-12 && (values[1] - 3.0).abs() < 1e-12,
            "{values:?}"
        );
    }

    #[test]
    fn eigen_reconstructs_a_symmetric_matrix_with_orthonormal_vectors() {
        let mut rng = crate::common::rng::solver_rng(Some(SEED));
        let n = 6;
        let r = Array2::from_shape_fn((n, n), |_| rng.gen::<f64>() - 0.5);
        let a = r.dot(&r.t()) + Array2::<f64>::eye(n);
        let (v, values) = eigen_symmetric(&a);
        let rebuilt = v.dot(&Array2::from_diag(&Array1::from(values))).dot(&v.t());
        let orthogonality = v.t().dot(&v) - Array2::<f64>::eye(n);
        assert!(
            (&rebuilt - &a).iter().all(|e| e.abs() < 1e-10),
            "A = V D V^T"
        );
        assert!(orthogonality.iter().all(|e| e.abs() < 1e-10), "V^T V = I");
    }

    #[test]
    fn the_strategy_parameters_follow_the_tutorial() {
        let p = Parameters::new(10, 10);
        assert_eq!(p.weights.len(), 5, "mu = lambda / 2");
        assert!((p.weights.iter().sum::<f64>() - 1.0).abs() < 1e-12);
        assert!(
            p.weights.windows(2).all(|w| w[0] > w[1]),
            "better ranks weigh more"
        );
        // mu_eff is between 1 and mu; for lambda = 10 it is about 3.17.
        assert!((p.mu_eff - 3.1672).abs() < 1e-3, "{}", p.mu_eff);
        assert!(p.c_1 + p.c_mu <= 1.0 && p.c_sigma < 1.0 && p.c_c < 1.0);
        // E||N(0, I_10)|| is about 3.084.
        assert!((p.chi_n - 3.0843).abs() < 1e-3, "{}", p.chi_n);
    }

    #[test]
    fn it_converges_on_the_sphere() {
        let r = CMAESSolver::new(config(10, 300))
            .with_seed(SEED)
            .solve(&problem(10, sphere));
        assert!(r.best_fitness < 1e-12, "best {}", r.best_fitness);
        assert_eq!(r.history.len(), 300);
    }

    #[test]
    fn it_solves_an_ellipsoid_a_fixed_cloud_cannot() {
        // Condition 1e6. This is what the covariance update is for: it
        // learns the scaling, and the problem becomes a sphere to the search.
        let r = CMAESSolver::new(config(10, 600))
            .with_seed(SEED)
            .solve(&problem(10, ellipsoid));
        assert!(r.best_fitness < 1e-8, "best {}", r.best_fitness);
    }

    #[test]
    fn it_follows_the_rosenbrock_valley() {
        let r = CMAESSolver::new(config(10, 800))
            .with_seed(SEED)
            .solve(&problem(5, rosenbrock));
        assert!(r.best_fitness < 1e-6, "best {}", r.best_fitness);
        assert!(
            r.best_variables.iter().all(|v| (v - 1.0).abs() < 1e-2),
            "{:?}",
            r.best_variables
        );
    }

    #[test]
    fn the_history_never_gets_worse_and_answers_stay_in_bounds() {
        let r = CMAESSolver::new(config(8, 100))
            .with_seed(SEED)
            .solve(&problem(4, rosenbrock));
        assert!(r.history.windows(2).all(|w| w[1] <= w[0]));
        assert!(r.best_variables.iter().all(|v| (-5.0..=5.0).contains(v)));
    }

    #[test]
    fn the_same_seed_gives_the_same_answer() {
        let run = || {
            CMAESSolver::new(config(8, 60))
                .with_seed(SEED)
                .solve(&problem(4, ellipsoid))
                .best_fitness
        };
        assert_eq!(run(), run());
    }

    #[test]
    fn a_population_below_the_minimum_is_raised_to_it() {
        let r = CMAESSolver::new(config(1, 50))
            .with_seed(SEED)
            .solve(&problem(3, sphere));
        assert!(r.best_fitness.is_finite());
        assert_eq!(r.history.len(), 50);
    }
}
