//! Standard single-objective benchmark functions.
//!
//! All minimized. Documented in Jamil & Yang (2013), "A Literature Survey of
//! Benchmark Functions For Global Optimization Problems," arXiv:1308.4008.

use crate::common::SimpleProblem;
use ndarray::Array1;
use std::f64::consts::{E, PI};

pub struct SOProblemSpec {
    pub name: &'static str,
    pub dim: usize,
    pub lower: f64,
    pub upper: f64,
    pub global_minimum: f64,
    pub func: fn(&Array1<f64>) -> f64,
}

impl SOProblemSpec {
    pub fn to_problem(&self) -> SimpleProblem<fn(&Array1<f64>) -> f64> {
        SimpleProblem {
            objective_func: self.func,
            dim: self.dim,
            lower: Array1::from_elem(self.dim, self.lower),
            upper: Array1::from_elem(self.dim, self.upper),
        }
    }
}

pub fn sphere(x: &Array1<f64>) -> f64 {
    x.iter().map(|&v| v * v).sum()
}

pub fn rastrigin(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    10.0 * n
        + x.iter()
            .map(|&v| v * v - 10.0 * (2.0 * PI * v).cos())
            .sum::<f64>()
}

pub fn ackley(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    let s1: f64 = x.iter().map(|&v| v * v).sum();
    let s2: f64 = x.iter().map(|&v| (2.0 * PI * v).cos()).sum();
    -20.0 * (-0.2 * (s1 / n).sqrt()).exp() - (s2 / n).exp() + 20.0 + E
}

pub fn rosenbrock(x: &Array1<f64>) -> f64 {
    (0..x.len() - 1)
        .map(|i| {
            let a = x[i + 1] - x[i] * x[i];
            let b = 1.0 - x[i];
            100.0 * a * a + b * b
        })
        .sum()
}

pub fn griewank(x: &Array1<f64>) -> f64 {
    let s: f64 = x.iter().map(|&v| v * v).sum::<f64>() / 4000.0;
    let p: f64 = x
        .iter()
        .enumerate()
        .map(|(i, &v)| (v / ((i + 1) as f64).sqrt()).cos())
        .product();
    1.0 + s - p
}

pub fn schwefel(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    418.9828872724337 * n
        - x.iter()
            .map(|&v| v * v.abs().sqrt().sin())
            .sum::<f64>()
}

pub fn levy(x: &Array1<f64>) -> f64 {
    let w: Vec<f64> = x.iter().map(|&v| 1.0 + (v - 1.0) / 4.0).collect();
    let n = w.len();
    let term1 = (PI * w[0]).sin().powi(2);
    let term3 = (w[n - 1] - 1.0).powi(2) * (1.0 + (2.0 * PI * w[n - 1]).sin().powi(2));
    let term2: f64 = (0..n - 1)
        .map(|i| (w[i] - 1.0).powi(2) * (1.0 + 10.0 * (PI * w[i] + 1.0).sin().powi(2)))
        .sum();
    term1 + term2 + term3
}

pub fn zakharov(x: &Array1<f64>) -> f64 {
    let s1: f64 = x.iter().map(|&v| v * v).sum();
    let s2: f64 = x.iter().enumerate().map(|(i, &v)| 0.5 * (i + 1) as f64 * v).sum();
    s1 + s2.powi(2) + s2.powi(4)
}

pub fn dixon_price(x: &Array1<f64>) -> f64 {
    let t1 = (x[0] - 1.0).powi(2);
    let t2: f64 = (1..x.len())
        .map(|i| (i + 1) as f64 * (2.0 * x[i] * x[i] - x[i - 1]).powi(2))
        .sum();
    t1 + t2
}

/// Styblinski-Tang. Global minimum at x_i = -2.903534, value = -39.16599 * n.
pub fn styblinski_tang(x: &Array1<f64>) -> f64 {
    0.5 * x.iter().map(|&v| v.powi(4) - 16.0 * v * v + 5.0 * v).sum::<f64>()
}

/// The suite the survey ranks solvers over: 28 scalable functions with
/// closed-form optima.
///
/// The size is load-bearing rather than incidental. OPT-02's pairwise Wilcoxon
/// test ranks over *functions*, and its exact p-value cannot go below 2/2^n --
/// so at ten functions no pair of solvers could be declared different at any
/// corrected threshold, whatever the data did. See the note above the
/// extension block below.
pub fn so_suite(dim: usize) -> Vec<SOProblemSpec> {
    let st_min = -39.16599 * dim as f64;
    let n2 = (dim * dim) as f64;
    vec![
        SOProblemSpec { name: "sphere",          dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: sphere },
        SOProblemSpec { name: "rastrigin",       dim, lower: -5.12,  upper: 5.12,   global_minimum: 0.0,   func: rastrigin },
        SOProblemSpec { name: "ackley",          dim, lower: -32.768,upper: 32.768, global_minimum: 0.0,   func: ackley },
        SOProblemSpec { name: "rosenbrock",      dim, lower: -5.0,   upper: 10.0,   global_minimum: 0.0,   func: rosenbrock },
        SOProblemSpec { name: "griewank",        dim, lower: -600.0, upper: 600.0,  global_minimum: 0.0,   func: griewank },
        SOProblemSpec { name: "schwefel",        dim, lower: -500.0, upper: 500.0,  global_minimum: 0.0,   func: schwefel },
        SOProblemSpec { name: "levy",            dim, lower: -10.0,  upper: 10.0,   global_minimum: 0.0,   func: levy },
        SOProblemSpec { name: "zakharov",        dim, lower: -5.0,   upper: 10.0,   global_minimum: 0.0,   func: zakharov },
        SOProblemSpec { name: "dixon_price",     dim, lower: -10.0,  upper: 10.0,   global_minimum: 0.0,   func: dixon_price },
        SOProblemSpec { name: "styblinski_tang", dim, lower: -5.0,   upper: 5.0,    global_minimum: st_min,func: styblinski_tang },

        SOProblemSpec { name: "schwefel_1_2",    dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: schwefel_1_2 },
        SOProblemSpec { name: "schwefel_2_21",   dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: schwefel_2_21 },
        SOProblemSpec { name: "schwefel_2_22",   dim, lower: -10.0,  upper: 10.0,   global_minimum: 0.0,   func: schwefel_2_22 },
        SOProblemSpec { name: "step",            dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: step },
        SOProblemSpec { name: "sum_squares",     dim, lower: -10.0,  upper: 10.0,   global_minimum: 0.0,   func: sum_squares },
        SOProblemSpec { name: "sum_of_different_powers", dim, lower: -1.0, upper: 1.0, global_minimum: 0.0, func: sum_of_different_powers },
        SOProblemSpec { name: "trid",            dim, lower: -n2,    upper: n2,     global_minimum: trid_minimum(dim), func: trid },
        SOProblemSpec { name: "alpine1",         dim, lower: -10.0,  upper: 10.0,   global_minimum: 0.0,   func: alpine1 },
        SOProblemSpec { name: "happy_cat",       dim, lower: -2.0,   upper: 2.0,    global_minimum: 0.0,   func: happy_cat },
        SOProblemSpec { name: "hgbat",           dim, lower: -2.0,   upper: 2.0,    global_minimum: 0.0,   func: hgbat },
        SOProblemSpec { name: "bent_cigar",      dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: bent_cigar },
        SOProblemSpec { name: "discus",          dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: discus },
        SOProblemSpec { name: "high_conditioned_elliptic", dim, lower: -100.0, upper: 100.0, global_minimum: 0.0, func: high_conditioned_elliptic },
        SOProblemSpec { name: "salomon",         dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: salomon },
        SOProblemSpec { name: "qing",            dim, lower: -500.0, upper: 500.0,  global_minimum: 0.0,   func: qing },
        SOProblemSpec { name: "chung_reynolds",  dim, lower: -100.0, upper: 100.0,  global_minimum: 0.0,   func: chung_reynolds },
        SOProblemSpec { name: "exponential",     dim, lower: -1.0,   upper: 1.0,    global_minimum: -1.0,  func: exponential },
        SOProblemSpec { name: "periodic",        dim, lower: -10.0,  upper: 10.0,   global_minimum: 0.9,   func: periodic },
    ]
}

// ─────────────────────────────────────────────── corpus extension (OPT-02)
//
// Eighteen more functions, taking the suite from ten to twenty-eight.
//
// The reason is not coverage for its own sake. `optimization_survey` computes
// the Wilcoxon signed-rank test OPT-02 asks for, pairwise, with a Holm
// correction across the family -- and on ten functions that test **cannot
// reject anything**. The exact two-sided p-value on n blocks is a count over
// 2^n sign assignments, so the smallest value obtainable is 2/2^n; at n = 10
// that is 1.95e-3, while Holm's first threshold over 325 comparisons is
// 1.54e-4. At n = 28 the floor is 7.5e-9 and the comparison can answer.
//
// More seeds would not have helped: the seeds are inside each median, and the
// blocks the test ranks over are functions.
//
// Every function here is scalable in the dimension, has a global minimum that
// is known in closed form rather than tabulated, and is evaluated at that
// optimum in `tests/test_benchmark_optima.rs`. Functions whose optimum is only
// known numerically for particular dimensions (Michalewicz) or that are
// stochastic by definition (quartic-with-noise) are deliberately left out: a
// benchmark whose target is a table lookup cannot be checked, and one that
// returns a different value each call cannot be ranked.

/// Schwefel 1.2 (quadric). Non-separable; the partial sums couple every
/// variable to the ones before it.
pub fn schwefel_1_2(x: &Array1<f64>) -> f64 {
    let mut running = 0.0;
    let mut total = 0.0;
    for &v in x.iter() {
        running += v;
        total += running * running;
    }
    total
}

/// Schwefel 2.21: the largest coordinate in absolute value.
pub fn schwefel_2_21(x: &Array1<f64>) -> f64 {
    x.iter().fold(0.0f64, |m, &v| m.max(v.abs()))
}

/// Schwefel 2.22: sum plus product of absolute values.
pub fn schwefel_2_22(x: &Array1<f64>) -> f64 {
    let s: f64 = x.iter().map(|&v| v.abs()).sum();
    let p: f64 = x.iter().map(|&v| v.abs()).product();
    s + p
}

/// Step. Flat on every unit cell, so gradient information is useless and only
/// the sampling matters.
pub fn step(x: &Array1<f64>) -> f64 {
    x.iter().map(|&v| (v + 0.5).floor().powi(2)).sum()
}

/// Weighted sphere: coordinate `i` costs `i` times as much.
pub fn sum_squares(x: &Array1<f64>) -> f64 {
    x.iter().enumerate().map(|(i, &v)| (i + 1) as f64 * v * v).sum()
}

/// Sum of different powers. Ill-conditioned near the optimum.
pub fn sum_of_different_powers(x: &Array1<f64>) -> f64 {
    x.iter()
        .enumerate()
        .map(|(i, &v)| v.abs().powi(i as i32 + 2))
        .sum()
}

/// Trid. The only function in the suite whose optimum is away from a constant
/// vector, and whose minimum is negative and grows with the dimension:
/// `-n(n+4)(n-1)/6` at `x_i = i(n+1-i)`.
pub fn trid(x: &Array1<f64>) -> f64 {
    let a: f64 = x.iter().map(|&v| (v - 1.0).powi(2)).sum();
    let b: f64 = (1..x.len()).map(|i| x[i] * x[i - 1]).sum();
    a - b
}

/// Trid's global minimum in `dim` dimensions.
pub fn trid_minimum(dim: usize) -> f64 {
    let n = dim as f64;
    -n * (n + 4.0) * (n - 1.0) / 6.0
}

/// Alpine 1. Many local minima, all of the same shape.
pub fn alpine1(x: &Array1<f64>) -> f64 {
    x.iter().map(|&v| (v * v.sin() + 0.1 * v).abs()).sum()
}

/// HappyCat. Minimum at `x = -1`, not at the origin, which catches a solver
/// that is biased toward the centre of its bounds.
pub fn happy_cat(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    let sq: f64 = x.iter().map(|&v| v * v).sum();
    let s: f64 = x.iter().sum();
    ((sq - n).powi(2)).powf(0.125) + (0.5 * sq + s) / n + 0.5
}

/// HGBat. Also minimised at `x = -1`.
pub fn hgbat(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    let sq: f64 = x.iter().map(|&v| v * v).sum();
    let s: f64 = x.iter().sum();
    (sq * sq - s * s).abs().sqrt() + (0.5 * sq + s) / n + 0.5
}

/// Bent cigar. One cheap direction and `n-1` directions a million times more
/// expensive.
pub fn bent_cigar(x: &Array1<f64>) -> f64 {
    if x.is_empty() {
        return 0.0;
    }
    x[0] * x[0] + 1e6 * x.iter().skip(1).map(|&v| v * v).sum::<f64>()
}

/// Discus. Bent cigar with the conditioning the other way round.
pub fn discus(x: &Array1<f64>) -> f64 {
    if x.is_empty() {
        return 0.0;
    }
    1e6 * x[0] * x[0] + x.iter().skip(1).map(|&v| v * v).sum::<f64>()
}

/// High-conditioned elliptic: a condition number of 10^6 spread smoothly
/// across the coordinates rather than concentrated in one.
pub fn high_conditioned_elliptic(x: &Array1<f64>) -> f64 {
    let n = x.len();
    if n == 1 {
        return x[0] * x[0];
    }
    x.iter()
        .enumerate()
        .map(|(i, &v)| 1e6f64.powf(i as f64 / (n - 1) as f64) * v * v)
        .sum()
}

/// Salomon. Concentric ridges around the origin.
pub fn salomon(x: &Array1<f64>) -> f64 {
    let r = x.iter().map(|&v| v * v).sum::<f64>().sqrt();
    1.0 - (2.0 * PI * r).cos() + 0.1 * r
}

/// Qing. Minimised at `x_i = sqrt(i)`, so every coordinate has a different
/// target and 2^n symmetric optima exist.
pub fn qing(x: &Array1<f64>) -> f64 {
    x.iter()
        .enumerate()
        .map(|(i, &v)| (v * v - (i + 1) as f64).powi(2))
        .sum()
}

/// Chung-Reynolds: the sphere squared, so the gradient vanishes far from the
/// optimum as well as at it.
pub fn chung_reynolds(x: &Array1<f64>) -> f64 {
    let s: f64 = x.iter().map(|&v| v * v).sum();
    s * s
}

/// Exponential. Bounded below by -1, reached at the origin.
pub fn exponential(x: &Array1<f64>) -> f64 {
    -(-0.5 * x.iter().map(|&v| v * v).sum::<f64>()).exp()
}

/// Periodic. Global minimum 0.9 at the origin, surrounded by local minima
/// at 1.0 -- a shallow basin a solver can miss while looking converged.
pub fn periodic(x: &Array1<f64>) -> f64 {
    let s: f64 = x.iter().map(|&v| v.sin().powi(2)).sum();
    let q: f64 = x.iter().map(|&v| v * v).sum();
    1.0 + s - 0.1 * (-q).exp()
}
