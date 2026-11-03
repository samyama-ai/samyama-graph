//! Criterion benches for the Rao algorithm family extensions.
//!
//! Compares the new single-objective solvers (BMWR, SAMP-Jaya, EHR-Jaya,
//! QO-Rao, SAPHR) against existing baselines (BMR, BWR, Jaya, Rao-1) on
//! standard test functions, and runs the multi-objective MO-BMR / MO-BWR /
//! MO-BMWR / MO-Rao+DE solvers on ZDT1, ZDT2, ZDT3 and DTLZ1 (3-objective).

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ndarray::Array1;
use samyama_optimization::algorithms::{
    BMRSolver, BMWRSolver, BWRSolver, EHRJayaSolver, JayaSolver, MOBMWRSolver, MOBMWRVariant,
    MORaoDESolver, QORaoSolver, RaoSolver, RaoVariant, SAMPJayaSolver, SAPHRSolver,
};
use samyama_optimization::common::{
    MultiObjectiveProblem, Problem, SolverConfig, SimpleProblem,
};

// --- Single-objective test functions ---

fn sphere(x: &Array1<f64>) -> f64 {
    x.iter().map(|&v| v * v).sum()
}

fn rastrigin(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    10.0 * n
        + x.iter()
            .map(|&v| v * v - 10.0 * (2.0 * std::f64::consts::PI * v).cos())
            .sum::<f64>()
}

fn ackley(x: &Array1<f64>) -> f64 {
    let n = x.len() as f64;
    let s1: f64 = x.iter().map(|&v| v * v).sum();
    let s2: f64 = x.iter().map(|&v| (2.0 * std::f64::consts::PI * v).cos()).sum();
    -20.0 * (-0.2 * (s1 / n).sqrt()).exp() - (s2 / n).exp() + 20.0 + std::f64::consts::E
}

fn make_problem(f: fn(&Array1<f64>) -> f64, dim: usize, lo: f64, hi: f64) -> impl Problem {
    SimpleProblem {
        objective_func: f,
        dim,
        lower: Array1::from_elem(dim, lo),
        upper: Array1::from_elem(dim, hi),
    }
}

// --- Multi-objective test functions ---

struct ZDT {
    pub variant: u8, // 1, 2, or 3
    pub dim: usize,
}

impl MultiObjectiveProblem for ZDT {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { 2 }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim), Array1::ones(self.dim))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let f1 = x[0];
        let n = self.dim as f64;
        let g = 1.0 + 9.0 * x.iter().skip(1).sum::<f64>() / (n - 1.0);
        let f2 = match self.variant {
            1 => g * (1.0 - (f1 / g).sqrt()),
            2 => g * (1.0 - (f1 / g).powi(2)),
            3 => g * (1.0 - (f1 / g).sqrt() - (f1 / g) * (10.0 * std::f64::consts::PI * f1).sin()),
            _ => g * (1.0 - (f1 / g).sqrt()),
        };
        vec![f1, f2]
    }
}

struct DTLZ1 {
    pub dim: usize,
    pub m: usize, // num objectives
}

impl MultiObjectiveProblem for DTLZ1 {
    fn dim(&self) -> usize { self.dim }
    fn num_objectives(&self) -> usize { self.m }
    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        (Array1::zeros(self.dim), Array1::ones(self.dim))
    }
    fn objectives(&self, x: &Array1<f64>) -> Vec<f64> {
        let k = self.dim - self.m + 1;
        let xm: f64 = x.iter().skip(self.dim - k).map(|&v| {
            (v - 0.5).powi(2) - (20.0 * std::f64::consts::PI * (v - 0.5)).cos()
        }).sum();
        let g = 100.0 * (k as f64 + xm);
        let mut f = vec![0.5 * (1.0 + g); self.m];
        for i in 0..self.m {
            for j in 0..(self.m - 1 - i) {
                f[i] *= x[j];
            }
            if i > 0 {
                f[i] *= 1.0 - x[self.m - 1 - i];
            }
        }
        f
    }
}

// --- Bench groups ---

fn bench_single_obj(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_single_obj_sphere_10d");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 30, max_iterations: 100 };
    let p = make_problem(sphere, 10, -10.0, 10.0);

    group.bench_function("BMR", |b| b.iter(|| black_box(BMRSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("BWR", |b| b.iter(|| black_box(BWRSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("BMWR", |b| b.iter(|| black_box(BMWRSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("Jaya", |b| b.iter(|| black_box(JayaSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("Rao1", |b| b.iter(|| black_box(RaoSolver::new(cfg.clone(), RaoVariant::Rao1).solve(&p))));
    group.bench_function("SAMP-Jaya", |b| b.iter(|| black_box(SAMPJayaSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("EHR-Jaya", |b| b.iter(|| black_box(EHRJayaSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("QO-Rao", |b| b.iter(|| black_box(QORaoSolver::new(cfg.clone(), RaoVariant::Rao1).solve(&p))));
    group.bench_function("SAPHR", |b| b.iter(|| black_box(SAPHRSolver::new(cfg.clone()).solve(&p))));
    group.finish();
}

fn bench_rastrigin(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_rastrigin_10d");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 50, max_iterations: 200 };
    let p = make_problem(rastrigin, 10, -5.12, 5.12);
    group.bench_function("BMWR", |b| b.iter(|| black_box(BMWRSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("EHR-Jaya", |b| b.iter(|| black_box(EHRJayaSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("SAPHR", |b| b.iter(|| black_box(SAPHRSolver::new(cfg.clone()).solve(&p))));
    group.finish();
}

fn bench_ackley(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_ackley_10d");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 50, max_iterations: 200 };
    let p = make_problem(ackley, 10, -32.768, 32.768);
    group.bench_function("BMWR", |b| b.iter(|| black_box(BMWRSolver::new(cfg.clone()).solve(&p))));
    group.bench_function("QO-Rao", |b| b.iter(|| black_box(QORaoSolver::new(cfg.clone(), RaoVariant::Rao1).solve(&p))));
    group.finish();
}

fn bench_mo_zdt1(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_mo_zdt1_30d");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 50, max_iterations: 100 };
    let p = ZDT { variant: 1, dim: 30 };
    group.bench_function("MO-BMR",   |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMR).solve(&p))));
    group.bench_function("MO-BWR",   |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBWR).solve(&p))));
    group.bench_function("MO-BMWR",  |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMWR).solve(&p))));
    group.bench_function("MO-Rao+DE",|b| b.iter(|| black_box(MORaoDESolver::new(cfg.clone()).solve(&p))));
    group.finish();
}

fn bench_mo_zdt2(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_mo_zdt2_30d");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 50, max_iterations: 100 };
    let p = ZDT { variant: 2, dim: 30 };
    group.bench_function("MO-BMWR", |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMWR).solve(&p))));
    group.bench_function("MO-Rao+DE", |b| b.iter(|| black_box(MORaoDESolver::new(cfg.clone()).solve(&p))));
    group.finish();
}

fn bench_mo_zdt3(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_mo_zdt3_30d");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 50, max_iterations: 100 };
    let p = ZDT { variant: 3, dim: 30 };
    group.bench_function("MO-BMWR", |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMWR).solve(&p))));
    group.finish();
}

fn bench_mo_dtlz1(c: &mut Criterion) {
    let mut group = c.benchmark_group("rao_family_mo_dtlz1_3obj");
    group.sample_size(10);
    let cfg = SolverConfig { population_size: 60, max_iterations: 100 };
    let p = DTLZ1 { dim: 7, m: 3 };
    group.bench_function("MO-BMR",  |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMR).solve(&p))));
    group.bench_function("MO-BMWR", |b| b.iter(|| black_box(MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMWR).solve(&p))));
    group.finish();
}

criterion_group!(
    benches,
    bench_single_obj,
    bench_rastrigin,
    bench_ackley,
    bench_mo_zdt1,
    bench_mo_zdt2,
    bench_mo_zdt3,
    bench_mo_dtlz1,
);
criterion_main!(benches);
