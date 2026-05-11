//! ZDT and DTLZ multi-objective test suites.
//!
//! References:
//!   - Zitzler, Deb & Thiele 2000, ZDT functions.
//!   - Deb, Thiele, Laumanns & Zitzler 2002, DTLZ scalable test problems.

use crate::common::MultiObjectiveProblem;
use ndarray::Array1;
use std::f64::consts::PI;

pub struct MOProblemSpec {
    pub name: &'static str,
    pub dim: usize,
    pub num_objectives: usize,
    pub lower: f64,
    pub upper: f64,
    /// Approximate hypervolume reference point (one per objective). Used by
    /// `moo::hypervolume_2d` for 2-objective cases. For 3+ objectives,
    /// consumers must supply their own ref-point + HV computation.
    pub hv_ref: Vec<f64>,
}

pub struct ZDT {
    pub variant: u8, // 1..=6
    pub dim: usize,
}

impl MultiObjectiveProblem for ZDT {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { 2 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        match self.variant {
            5 => unreachable!("ZDT5 is binary-coded; not supported in this continuous suite"),
            _ => (Array1::zeros(self.dim), Array1::ones(self.dim)),
        }
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let n = self.dim as f64;
        match self.variant {
            1 => {
                let f1 = x[0];
                let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
                vec![f1, g * (1.0 - (f1 / g).sqrt())]
            }
            2 => {
                let f1 = x[0];
                let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
                vec![f1, g * (1.0 - (f1 / g).powi(2))]
            }
            3 => {
                let f1 = x[0];
                let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
                let f2 = g * (1.0 - (f1 / g).sqrt() - (f1 / g) * (10.0 * PI * f1).sin());
                vec![f1, f2]
            }
            4 => {
                let f1 = x[0];
                let g = 1.0
                    + 10.0 * (n - 1.0)
                    + x.iter()
                        .skip(1)
                        .map(|&v| v * v - 10.0 * (4.0 * PI * v).cos())
                        .sum::<f64>();
                vec![f1, g * (1.0 - (f1 / g).sqrt())]
            }
            6 => {
                let f1 = 1.0 - (-4.0 * x[0]).exp() * (6.0 * PI * x[0]).sin().powi(6);
                let g = 1.0
                    + 9.0 * (x.iter().skip(1).sum::<f64>() / (n - 1.0)).powf(0.25);
                vec![f1, g * (1.0 - (f1 / g).powi(2))]
            }
            _ => panic!("ZDT variant {} not supported", self.variant),
        }
    }
}

pub struct DTLZ {
    pub variant: u8, // 1..=7
    pub dim: usize,
    pub m: usize,    // number of objectives
}

impl MultiObjectiveProblem for DTLZ {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { self.m }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim), Array1::ones(self.dim))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let m = self.m;
        let k = self.dim - m + 1;
        let xm = &x.as_slice().unwrap()[self.dim - k..];

        match self.variant {
            1 => {
                let g = 100.0 * (k as f64
                    + xm.iter()
                        .map(|&v| (v - 0.5).powi(2) - (20.0 * PI * (v - 0.5)).cos())
                        .sum::<f64>());
                let mut f = vec![0.5 * (1.0 + g); m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= x[j]; }
                    if i > 0 { f[i] *= 1.0 - x[m - 1 - i]; }
                }
                f
            }
            2 | 3 | 4 => {
                // DTLZ2/3/4 share spherical PF; only g and the meta-variable transformation differ.
                let g = match self.variant {
                    2 | 4 => xm.iter().map(|&v| (v - 0.5).powi(2)).sum::<f64>(),
                    3 => 100.0 * (k as f64
                        + xm.iter()
                            .map(|&v| (v - 0.5).powi(2) - (20.0 * PI * (v - 0.5)).cos())
                            .sum::<f64>()),
                    _ => unreachable!(),
                };
                let alpha = if self.variant == 4 { 100.0 } else { 1.0 };
                let theta: Vec<f64> = (0..m - 1)
                    .map(|j| 0.5 * PI * x[j].powf(alpha))
                    .collect();
                let mut f = vec![1.0 + g; m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= theta[j].cos(); }
                    if i > 0 { f[i] *= theta[m - 1 - i].sin(); }
                }
                f
            }
            5 => {
                let g = xm.iter().map(|&v| (v - 0.5).powi(2)).sum::<f64>();
                let mut theta = vec![0.0; m - 1];
                theta[0] = 0.5 * PI * x[0];
                let t = PI / (4.0 * (1.0 + g));
                for j in 1..m - 1 {
                    theta[j] = t * (1.0 + 2.0 * g * x[j]);
                }
                let mut f = vec![1.0 + g; m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= theta[j].cos(); }
                    if i > 0 { f[i] *= theta[m - 1 - i].sin(); }
                }
                f
            }
            6 => {
                let g = xm.iter().map(|&v| v.powf(0.1)).sum::<f64>();
                let mut theta = vec![0.0; m - 1];
                theta[0] = 0.5 * PI * x[0];
                let t = PI / (4.0 * (1.0 + g));
                for j in 1..m - 1 {
                    theta[j] = t * (1.0 + 2.0 * g * x[j]);
                }
                let mut f = vec![1.0 + g; m];
                for i in 0..m {
                    for j in 0..(m - 1 - i) { f[i] *= theta[j].cos(); }
                    if i > 0 { f[i] *= theta[m - 1 - i].sin(); }
                }
                f
            }
            7 => {
                let g = 1.0 + 9.0 / k as f64 * xm.iter().sum::<f64>();
                let mut f = vec![0.0; m];
                for i in 0..m - 1 { f[i] = x[i]; }
                let h: f64 = (0..m - 1)
                    .map(|i| f[i] / (1.0 + g) * (1.0 + (3.0 * PI * f[i]).sin()))
                    .sum();
                f[m - 1] = (1.0 + g) * (m as f64 - h);
                f
            }
            _ => panic!("DTLZ variant {} not supported", self.variant),
        }
    }
}

/// Returns the full MO test suite: ZDT1-4, ZDT6, DTLZ1-7 at m=3.
pub fn moo_suite(zdt_dim: usize, dtlz_dim: usize, dtlz_m: usize) -> Vec<MOProblemSpec> {
    let mut v = Vec::new();
    for variant in [1u8, 2, 3, 4, 6] {
        v.push(MOProblemSpec {
            name: match variant { 1 => "ZDT1", 2 => "ZDT2", 3 => "ZDT3", 4 => "ZDT4", 6 => "ZDT6", _ => "ZDT?" },
            dim: zdt_dim,
            num_objectives: 2,
            lower: 0.0,
            upper: 1.0,
            hv_ref: vec![1.1, 1.1],
        });
    }
    for variant in 1u8..=7 {
        v.push(MOProblemSpec {
            name: match variant {
                1 => "DTLZ1", 2 => "DTLZ2", 3 => "DTLZ3", 4 => "DTLZ4",
                5 => "DTLZ5", 6 => "DTLZ6", 7 => "DTLZ7", _ => "DTLZ?",
            },
            dim: dtlz_dim,
            num_objectives: dtlz_m,
            lower: 0.0,
            upper: 1.0,
            hv_ref: vec![1.1; dtlz_m],
        });
    }
    v
}

/// Constructor matching name -> Box<dyn MultiObjectiveProblem>.
pub fn build_mo_problem(spec: &MOProblemSpec) -> Box<dyn MultiObjectiveProblem> {
    if let Some(rest) = spec.name.strip_prefix("ZDT") {
        let variant: u8 = rest.parse().expect("ZDT variant");
        return Box::new(ZDT { variant, dim: spec.dim });
    }
    if let Some(rest) = spec.name.strip_prefix("DTLZ") {
        let variant: u8 = rest.parse().expect("DTLZ variant");
        return Box::new(DTLZ { variant, dim: spec.dim, m: spec.num_objectives });
    }
    panic!("unknown MO problem {}", spec.name)
}
