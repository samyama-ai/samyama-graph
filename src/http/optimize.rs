//! HTTP endpoints for the optimization workstream.
//!
//! Wraps the `samyama-optimization` crate behind a small REST + SSE API so
//! the Samyama Insight UI can run solvers without a Python sidecar.
//! Contracts are specified in `samyama-cloud/wiki/decisions/optimization-in-insight.md`.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{sse::{Event, KeepAlive, Sse}, IntoResponse, Json},
    routing::{get, post},
    Router,
};
use futures::stream::Stream;
use ndarray::Array1;
use samyama_optimization::algorithms::{
    BMRSolver, BMWRSolver, BWRSolver, DESolver, EHRJayaSolver, GASolver,
    GOTLBOSolver, ITLBOSolver, JayaSolver, MOBMWRSolver, MOBMWRVariant,
    MORaoDESolver, NSGA2Solver, PSOSolver, QORaoSolver, QOJayaSolver,
    RaoSolver, RaoVariant, SAMPJayaSolver, SAPHRSolver, TLBOSolver,
};
use samyama_optimization::common::{
    MultiObjectiveProblem, Problem, SimpleProblem, SolverConfig,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, pin::Pin, sync::Arc};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;

// ---------- Static metadata ----------

#[derive(Serialize, Clone)]
struct PaperRef {
    title: &'static str,
    url: &'static str,
    year: u16,
}

#[derive(Serialize, Clone)]
struct ParamSpec {
    name: &'static str,
    #[serde(rename = "type")]
    ty: &'static str,
    default: serde_json::Value,
}

#[derive(Serialize, Clone)]
struct AlgorithmInfo {
    id: &'static str,
    name: &'static str,
    family: &'static str,
    variant: Option<&'static str>,
    equation_tex: &'static str,
    paper_refs: Vec<PaperRef>,
    params: Vec<ParamSpec>,
    multi_objective: bool,
}

fn algorithm_catalog() -> Vec<AlgorithmInfo> {
    let rao_bmr_paper = PaperRef {
        title: "BMR and BWR: Two simple metaphor-free optimization algorithms",
        url: "https://arxiv.org/abs/2407.11149",
        year: 2024,
    };
    let rao_casting_paper = PaperRef {
        title: "Optimization of Different Metal Casting Processes Using Three Simple and Efficient Advanced Algorithms",
        url: "https://www.mdpi.com/2075-4701/15/9/1057",
        year: 2025,
    };
    let rao_mo_mfg_paper = PaperRef {
        title: "Single, Multi-, and Many-Objective Optimization of Manufacturing Processes (MO-* Algorithm 2)",
        url: "https://www.mdpi.com/2504-4494/9/8/249",
        year: 2025,
    };
    let rao_energy_paper = PaperRef {
        title: "Multi-objective dispatch with MO-BMR/BWR/BMWR",
        url: "https://www.mdpi.com/1996-1073/19/1/34",
        year: 2026,
    };
    let jaya_paper = PaperRef {
        title: "Jaya: A simple and new optimization algorithm",
        url: "http://growingscience.com/beta/ijiec/2406-jaya-a-simple-and-new-optimization-algorithm-for-solving-constrained-and-unconstrained-optimization-problems.html",
        year: 2016,
    };
    let rao_series_paper = PaperRef {
        title: "Rao algorithms: Three metaphor-less simple algorithms",
        url: "http://growingscience.com/beta/ijiec/3339-rao-algorithms-three-metaphor-less-simple-algorithms.html",
        year: 2020,
    };
    let tlbo_paper = PaperRef {
        title: "Teaching-learning-based optimization",
        url: "https://doi.org/10.1016/j.cad.2010.12.015",
        year: 2011,
    };

    let default_params = || {
        vec![
            ParamSpec { name: "population_size", ty: "int", default: serde_json::json!(50) },
            ParamSpec { name: "iterations",      ty: "int", default: serde_json::json!(200) },
        ]
    };

    vec![
        AlgorithmInfo {
            id: "jaya", name: "Jaya", family: "rao", variant: None,
            equation_tex: r"V' = V + r_1(V_{best} - |V|) - r_2(V_{worst} - |V|)",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "rao1", name: "Rao-1", family: "rao", variant: Some("Rao1"),
            equation_tex: r"V' = V + r_1(V_{best} - V_{worst})",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "rao2", name: "Rao-2", family: "rao", variant: Some("Rao2"),
            equation_tex: r"V' = V + r_1(V_{best}-V_{worst}) + r_2(\text{rand pairwise})",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "rao3", name: "Rao-3", family: "rao", variant: Some("Rao3"),
            equation_tex: r"V' = V + r_1(V_{best} - |V_{worst}|) + r_2(\cdot)",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "tlbo", name: "TLBO", family: "rao", variant: None,
            equation_tex: r"\text{Teacher + Learner phases}",
            paper_refs: vec![tlbo_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "itlbo", name: "ITLBO", family: "rao", variant: None,
            equation_tex: r"\text{TLBO with elitism}",
            paper_refs: vec![tlbo_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "qojaya", name: "QO-Jaya", family: "rao", variant: None,
            equation_tex: r"\text{Jaya + quasi-oppositional learning}",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "gotlbo", name: "GO-TLBO", family: "rao", variant: None,
            equation_tex: r"\text{Generalized-oppositional TLBO}",
            paper_refs: vec![tlbo_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "bmr", name: "BMR (Best-Mean-Random)", family: "rao", variant: None,
            equation_tex: r"V' = V + r_1(V_{best}-TV_{mean}) + r_2(V_{best}-V_{rand})",
            paper_refs: vec![rao_bmr_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "bwr", name: "BWR (Best-Worst-Random)", family: "rao", variant: None,
            equation_tex: r"V' = V + r_1(V_{best}-TV_{rand}) - r_2(V_{worst}-V_{rand})",
            paper_refs: vec![rao_bmr_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "bmwr", name: "BMWR (Best-Mean-Worst-Random)", family: "rao", variant: None,
            equation_tex: r"V' = V + r_1(V_{best}-TV_{mean}) + r_2(V_{best}-V_{rand}) - r_5(V_{worst}-V_{rand})",
            paper_refs: vec![rao_casting_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "samp_jaya", name: "SAMP-Jaya", family: "rao", variant: None,
            equation_tex: r"\text{Self-adaptive multi-population Jaya}",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "qo_rao", name: "QO-Rao", family: "rao", variant: Some("Rao1"),
            equation_tex: r"\text{Rao + quasi-oppositional learning}",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "ehrjaya", name: "EHR-Jaya", family: "rao", variant: None,
            equation_tex: r"\text{Classification-based hybrid Jaya + Rao-1}",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "saphr", name: "SAPHR", family: "rao", variant: None,
            equation_tex: r"\text{Self-adaptive hybrid Rao-1/2/3}",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "mo_bmr", name: "MO-BMR", family: "rao", variant: Some("MOBMR"),
            equation_tex: r"\text{MO extension of BMR}",
            paper_refs: vec![rao_casting_paper.clone(), rao_mo_mfg_paper.clone()],
            params: default_params(), multi_objective: true,
        },
        AlgorithmInfo {
            id: "mo_bwr", name: "MO-BWR", family: "rao", variant: Some("MOBWR"),
            equation_tex: r"\text{MO extension of BWR}",
            paper_refs: vec![rao_casting_paper.clone(), rao_mo_mfg_paper.clone()],
            params: default_params(), multi_objective: true,
        },
        AlgorithmInfo {
            id: "mo_bmwr", name: "MO-BMWR", family: "rao", variant: Some("MOBMWR"),
            equation_tex: r"\text{MO extension of BMWR}",
            paper_refs: vec![rao_energy_paper.clone(), rao_mo_mfg_paper.clone()],
            params: default_params(), multi_objective: true,
        },
        AlgorithmInfo {
            id: "mo_rao_de", name: "MO-Rao+DE", family: "rao", variant: None,
            equation_tex: r"\text{Rao-1 hybridised with DE/rand/1/bin}",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(), multi_objective: true,
        },
        AlgorithmInfo {
            id: "pso", name: "PSO", family: "swarm", variant: None,
            equation_tex: r"v \leftarrow wv + c_1r_1(p_{best}-x) + c_2r_2(g_{best}-x)",
            paper_refs: vec![], params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "de", name: "Differential Evolution", family: "evolutionary", variant: None,
            equation_tex: r"v = x_{r1} + F(x_{r2}-x_{r3})",
            paper_refs: vec![], params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "ga", name: "Genetic Algorithm", family: "evolutionary", variant: None,
            equation_tex: r"\text{Crossover + mutation}",
            paper_refs: vec![], params: default_params(), multi_objective: false,
        },
        AlgorithmInfo {
            id: "nsga2", name: "NSGA-II", family: "multi-objective", variant: None,
            equation_tex: r"\text{FNDS + crowding distance + elitist truncation}",
            paper_refs: vec![], params: default_params(), multi_objective: true,
        },
    ]
}

#[derive(Serialize, Clone)]
struct BenchmarkInfo {
    id: &'static str,
    name: &'static str,
    dim: usize,
    lower: f64,
    upper: f64,
    num_objectives: usize,
    #[serde(rename = "type")]
    ty: &'static str,
    optimum: Option<f64>,
}

fn benchmark_catalog() -> Vec<BenchmarkInfo> {
    vec![
        BenchmarkInfo { id: "sphere",     name: "Sphere",     dim: 10, lower: -10.0,    upper: 10.0,    num_objectives: 1, ty: "single", optimum: Some(0.0) },
        BenchmarkInfo { id: "rastrigin",  name: "Rastrigin",  dim: 10, lower: -5.12,    upper: 5.12,    num_objectives: 1, ty: "single", optimum: Some(0.0) },
        BenchmarkInfo { id: "ackley",     name: "Ackley",     dim: 10, lower: -32.768,  upper: 32.768,  num_objectives: 1, ty: "single", optimum: Some(0.0) },
        BenchmarkInfo { id: "rosenbrock", name: "Rosenbrock", dim: 10, lower: -5.0,     upper: 10.0,    num_objectives: 1, ty: "single", optimum: Some(0.0) },
        BenchmarkInfo { id: "zdt1",       name: "ZDT1",       dim: 30, lower: 0.0,      upper: 1.0,     num_objectives: 2, ty: "multi",  optimum: None },
        BenchmarkInfo { id: "zdt2",       name: "ZDT2",       dim: 30, lower: 0.0,      upper: 1.0,     num_objectives: 2, ty: "multi",  optimum: None },
        BenchmarkInfo { id: "zdt3",       name: "ZDT3",       dim: 30, lower: 0.0,      upper: 1.0,     num_objectives: 2, ty: "multi",  optimum: None },
        BenchmarkInfo { id: "dtlz1",      name: "DTLZ1",      dim: 7,  lower: 0.0,      upper: 1.0,     num_objectives: 3, ty: "multi",  optimum: None },
    ]
}

// ---------- Problems ----------

fn single_obj(name: &str, dim: usize, lower: f64, upper: f64) -> SimpleProblem<fn(&Array1<f64>) -> f64> {
    let f: fn(&Array1<f64>) -> f64 = match name {
        "sphere"     => |x| x.iter().map(|&v| v * v).sum(),
        "rastrigin"  => |x| {
            let n = x.len() as f64;
            10.0 * n + x.iter().map(|&v| v * v - 10.0 * (2.0 * std::f64::consts::PI * v).cos()).sum::<f64>()
        },
        "ackley"     => |x| {
            let n = x.len() as f64;
            let s1: f64 = x.iter().map(|&v| v * v).sum();
            let s2: f64 = x.iter().map(|&v| (2.0 * std::f64::consts::PI * v).cos()).sum();
            -20.0 * (-0.2 * (s1 / n).sqrt()).exp() - (s2 / n).exp() + 20.0 + std::f64::consts::E
        },
        "rosenbrock" => |x| {
            (0..x.len()-1).map(|i| {
                let a = x[i + 1] - x[i] * x[i];
                let b = 1.0 - x[i];
                100.0 * a * a + b * b
            }).sum()
        },
        _ => |x| x.iter().map(|&v| v * v).sum(), // default sphere
    };
    SimpleProblem {
        objective_func: f,
        dim,
        lower: Array1::from_elem(dim, lower),
        upper: Array1::from_elem(dim, upper),
    }
}

struct ZDT { variant: u8, dim: usize }
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

struct DTLZ1 { dim: usize, m: usize }
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

// ---------- Job registry + state ----------

#[derive(Default)]
pub struct OptimizeState {
    jobs: Mutex<HashMap<String, JobHandle>>,
}

struct JobHandle {
    cancel_tx: Option<tokio::sync::oneshot::Sender<()>>,
    cancel_flag: Option<CancelHandle>,
    event_rx: Option<mpsc::Receiver<SseEvent>>,
}

#[derive(Clone)]
struct CancelHandle {
    flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Debug, Clone)]
enum SseEvent {
    Iteration { iter: usize, best_fitness: f64 },
    Done { final_fitness: f64, iterations: usize },
    Error { message: String },
}

pub fn router() -> Router<Arc<OptimizeState>> {
    Router::new()
        .route("/optimize/algorithms", get(list_algorithms))
        .route("/optimize/benchmarks", get(list_benchmarks))
        .route("/optimize/solve", post(start_solve))
        .route("/optimize/solve/:id/stream", get(stream_solve))
        .route("/optimize/solve/:id/cancel", post(cancel_solve))
}

async fn list_algorithms() -> Json<Vec<AlgorithmInfo>> {
    Json(algorithm_catalog())
}

async fn list_benchmarks() -> Json<Vec<BenchmarkInfo>> {
    Json(benchmark_catalog())
}

#[derive(Deserialize)]
struct SolveReq {
    algorithm: String,
    benchmark: String,
    #[serde(default = "default_pop")]
    population_size: usize,
    #[serde(default = "default_iter")]
    iterations: usize,
    #[serde(default)]
    dim: Option<usize>,
    #[serde(default)]
    seed: Option<u64>,
}
fn default_pop() -> usize { 50 }
fn default_iter() -> usize { 200 }

#[derive(Serialize)]
struct SolveResp { job_id: String }

async fn start_solve(
    State(state): State<Arc<OptimizeState>>,
    Json(req): Json<SolveReq>,
) -> Result<Json<SolveResp>, (StatusCode, String)> {
    // Validate benchmark + algorithm exist up-front.
    let bench = benchmark_catalog()
        .into_iter()
        .find(|b| b.id == req.benchmark)
        .ok_or((StatusCode::BAD_REQUEST, format!("unknown benchmark: {}", req.benchmark)))?;
    let algo = algorithm_catalog()
        .into_iter()
        .find(|a| a.id == req.algorithm)
        .ok_or((StatusCode::BAD_REQUEST, format!("unknown algorithm: {}", req.algorithm)))?;

    let job_id = Uuid::new_v4().to_string();
    let (event_tx, event_rx) = mpsc::channel::<SseEvent>(256);
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();

    {
        let mut jobs = state.jobs.lock().await;
        jobs.insert(
            job_id.clone(),
            JobHandle { cancel_tx: Some(cancel_tx), cancel_flag: None, event_rx: Some(event_rx) },
        );
    }

    let cfg = SolverConfig {
        population_size: req.population_size,
        max_iterations: req.iterations,
    };
    let dim = req.dim.unwrap_or(bench.dim);

    // Seed (not currently propagated into solvers, which use thread_rng).
    let _ = req.seed;

    // Run in a blocking task so we don't stall the async runtime.
    // AtomicBool cancel flag — polled between the compute future and emit loop.
    let cancelled_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_state = CancelHandle { flag: cancelled_flag.clone() };

    {
        // Re-lock and swap the JobHandle cancel field to the AtomicBool handle.
        // (Keeping this structure out of the oneshot so the select! below doesn't
        //  fire spuriously when the one-shot sender is Option::None-ed out.)
        let mut jobs = state.jobs.lock().await;
        if let Some(h) = jobs.get_mut(&job_id) {
            h.cancel_tx = None; // drop the oneshot sender — unused in this flow
            h.cancel_flag = Some(cancel_state);
        }
    }
    let _ = cancel_rx; // silence warning; we don't use the oneshot receiver

    tokio::task::spawn(async move {
        let compute = tokio::task::spawn_blocking(move || {
            run_solver(&algo.id, algo.multi_objective, &bench.id, bench.num_objectives, dim, cfg)
        });

        match compute.await {
            Ok(Ok(SolverOutcome { history, final_fitness })) => {
                for (iter, best) in history.iter().enumerate() {
                    if cancelled_flag.load(std::sync::atomic::Ordering::Relaxed) {
                        let _ = event_tx.send(SseEvent::Error { message: "cancelled".into() }).await;
                        return;
                    }
                    if event_tx
                        .send(SseEvent::Iteration { iter, best_fitness: *best })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = event_tx
                    .send(SseEvent::Done { final_fitness, iterations: history.len() })
                    .await;
            }
            Ok(Err(e)) => {
                let _ = event_tx.send(SseEvent::Error { message: e }).await;
            }
            Err(e) => {
                let _ = event_tx.send(SseEvent::Error { message: format!("join: {e}") }).await;
            }
        }
    });

    Ok(Json(SolveResp { job_id }))
}

struct SolverOutcome {
    history: Vec<f64>,
    final_fitness: f64,
}

fn run_solver(
    algo_id: &str,
    multi_objective: bool,
    bench_id: &str,
    _num_obj: usize,
    bench_dim: usize,
    cfg: SolverConfig,
) -> Result<SolverOutcome, String> {
    if multi_objective {
        // MO problems: track first-objective best across iterations.
        let run = |hist: Vec<f64>, final_first: f64| SolverOutcome {
            final_fitness: final_first,
            history: hist,
        };
        let (hist, final_f) = match bench_id {
            "zdt1" | "zdt2" | "zdt3" => {
                let v = bench_id.chars().last().unwrap().to_digit(10).unwrap() as u8;
                let problem = ZDT { variant: v, dim: 30 };
                let r = match algo_id {
                    "mo_bmr"    => MOBMWRSolver::new(cfg, MOBMWRVariant::MOBMR).solve(&problem),
                    "mo_bwr"    => MOBMWRSolver::new(cfg, MOBMWRVariant::MOBWR).solve(&problem),
                    "mo_bmwr"   => MOBMWRSolver::new(cfg, MOBMWRVariant::MOBMWR).solve(&problem),
                    "mo_rao_de" => MORaoDESolver::new(cfg).solve(&problem),
                    "nsga2"     => NSGA2Solver::new(cfg).solve(&problem),
                    _           => return Err(format!("algorithm {} not multi-objective", algo_id)),
                };
                let final_first = r.pareto_front.iter()
                    .map(|ind| ind.fitness[0])
                    .fold(f64::INFINITY, f64::min);
                (r.history, final_first)
            }
            "dtlz1" => {
                let problem = DTLZ1 { dim: 7, m: 3 };
                let r = match algo_id {
                    "mo_bmr"    => MOBMWRSolver::new(cfg, MOBMWRVariant::MOBMR).solve(&problem),
                    "mo_bwr"    => MOBMWRSolver::new(cfg, MOBMWRVariant::MOBWR).solve(&problem),
                    "mo_bmwr"   => MOBMWRSolver::new(cfg, MOBMWRVariant::MOBMWR).solve(&problem),
                    "mo_rao_de" => MORaoDESolver::new(cfg).solve(&problem),
                    "nsga2"     => NSGA2Solver::new(cfg).solve(&problem),
                    _           => return Err(format!("algorithm {} not multi-objective", algo_id)),
                };
                let final_first = r.pareto_front.iter()
                    .map(|ind| ind.fitness[0])
                    .fold(f64::INFINITY, f64::min);
                (r.history, final_first)
            }
            other => return Err(format!("benchmark {} is not multi-objective", other)),
        };
        Ok(run(hist, final_f))
    } else {
        let bench = benchmark_catalog().into_iter().find(|b| b.id == bench_id)
            .ok_or_else(|| format!("unknown benchmark: {bench_id}"))?;
        let dim = if bench_dim == 0 { bench.dim } else { bench_dim };
        let problem = single_obj(bench.id, dim, bench.lower, bench.upper);

        let result = match algo_id {
            "jaya"      => JayaSolver::new(cfg).solve(&problem),
            "rao1"      => RaoSolver::new(cfg, RaoVariant::Rao1).solve(&problem),
            "rao2"      => RaoSolver::new(cfg, RaoVariant::Rao2).solve(&problem),
            "rao3"      => RaoSolver::new(cfg, RaoVariant::Rao3).solve(&problem),
            "tlbo"      => TLBOSolver::new(cfg).solve(&problem),
            "itlbo"     => ITLBOSolver::new(cfg).solve(&problem),
            "qojaya"    => QOJayaSolver::new(cfg).solve(&problem),
            "gotlbo"    => GOTLBOSolver::new(cfg).solve(&problem),
            "bmr"       => BMRSolver::new(cfg).solve(&problem),
            "bwr"       => BWRSolver::new(cfg).solve(&problem),
            "bmwr"      => BMWRSolver::new(cfg).solve(&problem),
            "samp_jaya" => SAMPJayaSolver::new(cfg).solve(&problem),
            "qo_rao"    => QORaoSolver::new(cfg, RaoVariant::Rao1).solve(&problem),
            "ehrjaya"   => EHRJayaSolver::new(cfg).solve(&problem),
            "saphr"     => SAPHRSolver::new(cfg).solve(&problem),
            "pso"       => PSOSolver::new(cfg).solve(&problem),
            "de"        => DESolver::new(cfg).solve(&problem),
            "ga"        => GASolver::new(cfg).solve(&problem),
            other       => return Err(format!("algorithm {} not supported on single-objective benchmarks", other)),
        };
        Ok(SolverOutcome {
            final_fitness: result.best_fitness,
            history: result.history,
        })
    }
}

type SseStream = Pin<Box<dyn Stream<Item = Result<Event, Infallible>> + Send>>;

async fn stream_solve(
    State(state): State<Arc<OptimizeState>>,
    Path(id): Path<String>,
) -> Result<Sse<SseStream>, (StatusCode, String)> {
    let rx = {
        let mut jobs = state.jobs.lock().await;
        match jobs.get_mut(&id) {
            Some(h) => h.event_rx.take(),
            None => return Err((StatusCode::NOT_FOUND, "unknown job".into())),
        }
    };
    let Some(rx) = rx else {
        return Err((StatusCode::CONFLICT, "job already being streamed".into()));
    };

    let stream = ReceiverStream::new(rx).map(|evt| {
        let ev = match evt {
            SseEvent::Iteration { iter, best_fitness } => Event::default()
                .event("iteration")
                .json_data(serde_json::json!({ "iter": iter, "best_fitness": best_fitness }))
                .unwrap(),
            SseEvent::Done { final_fitness, iterations } => Event::default()
                .event("done")
                .json_data(serde_json::json!({
                    "final_fitness": final_fitness,
                    "iterations": iterations,
                    "total_time_ms": 0,
                }))
                .unwrap(),
            SseEvent::Error { message } => Event::default()
                .event("error")
                .json_data(serde_json::json!({ "message": message }))
                .unwrap(),
        };
        Ok::<_, Infallible>(ev)
    });

    Ok(Sse::new(Box::pin(stream) as SseStream).keep_alive(KeepAlive::default()))
}

#[derive(Serialize)]
struct CancelResp { cancelled: bool }

async fn cancel_solve(
    State(state): State<Arc<OptimizeState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut jobs = state.jobs.lock().await;
    match jobs.get_mut(&id) {
        Some(h) => {
            if let Some(handle) = h.cancel_flag.as_ref() {
                handle.flag.store(true, std::sync::atomic::Ordering::Relaxed);
                Json(CancelResp { cancelled: true })
            } else {
                Json(CancelResp { cancelled: false })
            }
        }
        None => Json(CancelResp { cancelled: false }),
    }
}

// Bring StreamExt into scope for `.map(...)` above.
use tokio_stream::StreamExt;
