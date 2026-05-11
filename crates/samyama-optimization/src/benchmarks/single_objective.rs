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

/// 10-function suite at dim=30 (CEC-conventional dimensionality).
pub fn so_suite(dim: usize) -> Vec<SOProblemSpec> {
    let st_min = -39.16599 * dim as f64;
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
    ]
}
