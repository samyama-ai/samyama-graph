//! Benchmark runner: drives N solvers × M problems × K seeds and collects results.

use super::multi_objective::{MOProblemSpec, DTLZ, ZDT};
use super::single_objective::SOProblemSpec;
use crate::common::MultiObjectiveProblem;
use crate::algorithms::*;
use crate::common::{MultiObjectiveResult, OptimizationResult, SolverConfig};
use crate::moo::{fast_non_dominated_sort, hypervolume_2d, igd};
use serde::Serialize;
use std::time::Instant;

#[derive(Debug, Clone, Serialize)]
pub struct SORecord {
    pub solver: String,
    pub problem: String,
    pub dim: usize,
    pub seed_index: usize,
    pub best_fitness: f64,
    pub global_minimum: f64,
    pub gap: f64,
    pub iterations: usize,
    pub wall_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct MORecord {
    pub solver: String,
    pub problem: String,
    pub dim: usize,
    pub num_objectives: usize,
    pub seed_index: usize,
    pub pareto_size: usize,
    pub hypervolume: Option<f64>,
    pub igd: Option<f64>,
    pub wall_ms: u128,
}

/// List of SO solvers reported in the paper.
pub fn so_solver_names() -> Vec<&'static str> {
    vec![
        // Rao family + parents
        "Jaya", "Rao-1", "Rao-2", "Rao-3", "TLBO", "ITLBO",
        "BMR", "BWR", "BMWR", "QO-Jaya", "QO-Rao", "SAMP-Jaya", "EHR-Jaya", "SAPHR",
        // Comparison baselines
        "GA", "DE", "PSO", "GWO", "SA", "ABC",
    ]
}

/// List of MO solvers reported in the paper.
pub fn mo_solver_names() -> Vec<&'static str> {
    vec!["NSGA-II", "MOTLBO", "MO-BMWR", "MO-BMR", "MO-BWR", "MO-Rao+DE"]
}

fn solve_so(name: &str, cfg: &SolverConfig, spec: &SOProblemSpec) -> OptimizationResult {
    let p = spec.to_problem();
    match name {
        "Jaya"      => JayaSolver::new(cfg.clone()).solve(&p),
        "Rao-1"     => RaoSolver::new(cfg.clone(), RaoVariant::Rao1).solve(&p),
        "Rao-2"     => RaoSolver::new(cfg.clone(), RaoVariant::Rao2).solve(&p),
        "Rao-3"     => RaoSolver::new(cfg.clone(), RaoVariant::Rao3).solve(&p),
        "TLBO"      => TLBOSolver::new(cfg.clone()).solve(&p),
        "ITLBO"     => ITLBOSolver::new(cfg.clone()).solve(&p),
        "BMR"       => BMRSolver::new(cfg.clone()).solve(&p),
        "BWR"       => BWRSolver::new(cfg.clone()).solve(&p),
        "BMWR"      => BMWRSolver::new(cfg.clone()).solve(&p),
        "QO-Jaya"   => QOJayaSolver::new(cfg.clone()).solve(&p),
        "QO-Rao"    => QORaoSolver::new(cfg.clone(), RaoVariant::Rao1).solve(&p),
        "SAMP-Jaya" => SAMPJayaSolver::new(cfg.clone()).solve(&p),
        "EHR-Jaya"  => EHRJayaSolver::new(cfg.clone()).solve(&p),
        "SAPHR"     => SAPHRSolver::new(cfg.clone()).solve(&p),
        "GA"        => GASolver::new(cfg.clone()).solve(&p),
        "DE"        => DESolver::new(cfg.clone()).solve(&p),
        "PSO"       => PSOSolver::new(cfg.clone()).solve(&p),
        "GWO"       => GWOSolver::new(cfg.clone()).solve(&p),
        "SA"        => SASolver::new(cfg.clone()).solve(&p),
        "ABC"       => ABCSolver::new(cfg.clone()).solve(&p),
        _           => panic!("unknown SO solver: {}", name),
    }
}

fn solve_mo_inner<P: MultiObjectiveProblem>(name: &str, cfg: &SolverConfig, p: &P) -> MultiObjectiveResult {
    match name {
        "NSGA-II"   => NSGA2Solver::new(cfg.clone()).solve(p),
        "MOTLBO"    => MOTLBOSolver::new(cfg.clone()).solve(p),
        "MO-BMWR"   => MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMWR).solve(p),
        "MO-BMR"    => MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBMR).solve(p),
        "MO-BWR"    => MOBMWRSolver::new(cfg.clone(), MOBMWRVariant::MOBWR).solve(p),
        "MO-Rao+DE" => MORaoDESolver::new(cfg.clone()).solve(p),
        _           => panic!("unknown MO solver: {}", name),
    }
}

fn solve_mo(name: &str, cfg: &SolverConfig, spec: &MOProblemSpec) -> MultiObjectiveResult {
    if let Some(rest) = spec.name.strip_prefix("ZDT") {
        let variant: u8 = rest.parse().expect("ZDT variant");
        let p = ZDT { variant, dim: spec.dim };
        return solve_mo_inner(name, cfg, &p);
    }
    if let Some(rest) = spec.name.strip_prefix("DTLZ") {
        let variant: u8 = rest.parse().expect("DTLZ variant");
        let p = DTLZ { variant, dim: spec.dim, m: spec.num_objectives };
        return solve_mo_inner(name, cfg, &p);
    }
    panic!("unknown MO problem {}", spec.name)
}

pub fn run_so_suite(
    solvers: &[&str],
    problems: &[SOProblemSpec],
    cfg: &SolverConfig,
    seeds: usize,
) -> Vec<SORecord> {
    let mut out = Vec::with_capacity(solvers.len() * problems.len() * seeds);
    for solver in solvers {
        for spec in problems {
            for seed in 0..seeds {
                let t0 = Instant::now();
                let r = solve_so(solver, cfg, spec);
                let wall_ms = t0.elapsed().as_millis();
                out.push(SORecord {
                    solver: solver.to_string(),
                    problem: spec.name.to_string(),
                    dim: spec.dim,
                    seed_index: seed,
                    best_fitness: r.best_fitness,
                    global_minimum: spec.global_minimum,
                    gap: r.best_fitness - spec.global_minimum,
                    iterations: r.history.len(),
                    wall_ms,
                });
            }
        }
    }
    out
}

pub fn run_mo_suite(
    solvers: &[&str],
    problems: &[MOProblemSpec],
    cfg: &SolverConfig,
    seeds: usize,
) -> Vec<MORecord> {
    let mut out = Vec::with_capacity(solvers.len() * problems.len() * seeds);
    for solver in solvers {
        for spec in problems {
            for seed in 0..seeds {
                let t0 = Instant::now();
                let mut r = solve_mo(solver, cfg, spec);
                let wall_ms = t0.elapsed().as_millis();
                fast_non_dominated_sort(&mut r.pareto_front);
                let front: Vec<&[f64]> = r.pareto_front.iter()
                    .filter(|i| i.rank == 0)
                    .map(|i| i.fitness.as_slice())
                    .collect();
                let front_owned: Vec<Vec<f64>> = front.iter().map(|f| f.to_vec()).collect();
                let (hv, igd_val) = if spec.num_objectives == 2 && !front_owned.is_empty() {
                    let hv = hypervolume_2d(&front_owned, [spec.hv_ref[0], spec.hv_ref[1]]);
                    let ref_pts: Vec<Vec<f64>> = (0..100)
                        .map(|i| {
                            let f1 = i as f64 / 99.0;
                            vec![f1, 1.0 - f1.sqrt()]
                        })
                        .collect();
                    let igd_val = igd(&front_owned, &ref_pts);
                    (Some(hv), Some(igd_val))
                } else { (None, None) };
                let pareto_size = front_owned.len();
                out.push(MORecord {
                    solver: solver.to_string(),
                    problem: spec.name.to_string(),
                    dim: spec.dim,
                    num_objectives: spec.num_objectives,
                    seed_index: seed,
                    pareto_size,
                    hypervolume: hv,
                    igd: igd_val,
                    wall_ms,
                });
            }
        }
    }
    out
}

/// CSV emitter (no external dep).
pub fn write_so_csv(records: &[SORecord], path: &std::path::Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "solver,problem,dim,seed,best_fitness,global_minimum,gap,iterations,wall_ms")?;
    for r in records {
        writeln!(f, "{},{},{},{},{},{},{},{},{}",
            r.solver, r.problem, r.dim, r.seed_index,
            r.best_fitness, r.global_minimum, r.gap, r.iterations, r.wall_ms)?;
    }
    Ok(())
}

pub fn write_mo_csv(records: &[MORecord], path: &std::path::Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "solver,problem,dim,num_objectives,seed,pareto_size,hypervolume,igd,wall_ms")?;
    for r in records {
        writeln!(f, "{},{},{},{},{},{},{},{},{}",
            r.solver, r.problem, r.dim, r.num_objectives, r.seed_index,
            r.pareto_size,
            r.hypervolume.map(|v| v.to_string()).unwrap_or_default(),
            r.igd.map(|v| v.to_string()).unwrap_or_default(),
            r.wall_ms)?;
    }
    Ok(())
}
