//! HTTP endpoints for the optimization workstream.
//!
//! Wraps the `samyama-optimization` crate behind a small REST + SSE API so
//! the Samyama Insight UI can run solvers without a Python sidecar.
//! Contracts are specified in `samyama-cloud/wiki/decisions/optimization-in-insight.md`.

use axum::{
    extract::{rejection::JsonRejection, Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Json,
    },
    routing::{get, post},
    Router,
};
use futures::stream::Stream;
use ndarray::Array1;
use samyama_optimization::algorithms::{
    BMRSolver, BMWRSolver, BWRSolver, DESolver, EHRJayaSolver, GASolver, GOTLBOSolver, ITLBOSolver,
    JayaSolver, MOBMWRSolver, MOBMWRVariant, MORaoDESolver, NSGA2Solver, PSOSolver, QOJayaSolver,
    QORaoSolver, RaoSolver, RaoVariant, SAMPJayaSolver, SAPHRSolver, TLBOSolver,
};
use samyama_optimization::benchmarks::{DTLZ, DTLZ_FRONT_SAMPLES_PER_AXIS, ZDT, ZDT_FRONT_SAMPLES};
use samyama_optimization::budget::{Budget, Budgeted, StopReason};
use samyama_optimization::common::{
    duration_ms, gap_to_best_known, is_feasible, MultiObjectiveProblem, Problem, SimpleProblem,
    SolverConfig,
};
use samyama_optimization::moo::{quality_metrics, QualityMetrics};
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
            ParamSpec {
                name: "population_size",
                ty: "int",
                default: serde_json::json!(50),
            },
            ParamSpec {
                name: "iterations",
                ty: "int",
                default: serde_json::json!(200),
            },
            // Optional limits (OPT-11, #1798); null means none.
            ParamSpec {
                name: "time_budget_ms",
                ty: "int",
                default: serde_json::Value::Null,
            },
            ParamSpec {
                name: "max_evaluations",
                ty: "int",
                default: serde_json::Value::Null,
            },
        ]
    };

    vec![
        AlgorithmInfo {
            id: "jaya",
            name: "Jaya",
            family: "rao",
            variant: None,
            equation_tex: r"V' = V + r_1(V_{best} - |V|) - r_2(V_{worst} - |V|)",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "rao1",
            name: "Rao-1",
            family: "rao",
            variant: Some("Rao1"),
            equation_tex: r"V' = V + r_1(V_{best} - V_{worst})",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "rao2",
            name: "Rao-2",
            family: "rao",
            variant: Some("Rao2"),
            equation_tex: r"V' = V + r_1(V_{best}-V_{worst}) + r_2(\text{rand pairwise})",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "rao3",
            name: "Rao-3",
            family: "rao",
            variant: Some("Rao3"),
            equation_tex: r"V' = V + r_1(V_{best} - |V_{worst}|) + r_2(\cdot)",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "tlbo",
            name: "TLBO",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Teacher + Learner phases}",
            paper_refs: vec![tlbo_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "itlbo",
            name: "ITLBO",
            family: "rao",
            variant: None,
            equation_tex: r"\text{TLBO with elitism}",
            paper_refs: vec![tlbo_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "qojaya",
            name: "QO-Jaya",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Jaya + quasi-oppositional learning}",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "gotlbo",
            name: "GO-TLBO",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Generalized-oppositional TLBO}",
            paper_refs: vec![tlbo_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "bmr",
            name: "BMR (Best-Mean-Random)",
            family: "rao",
            variant: None,
            equation_tex: r"V' = V + r_1(V_{best}-TV_{mean}) + r_2(V_{best}-V_{rand})",
            paper_refs: vec![rao_bmr_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "bwr",
            name: "BWR (Best-Worst-Random)",
            family: "rao",
            variant: None,
            equation_tex: r"V' = V + r_1(V_{best}-TV_{rand}) - r_2(V_{worst}-V_{rand})",
            paper_refs: vec![rao_bmr_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "bmwr",
            name: "BMWR (Best-Mean-Worst-Random)",
            family: "rao",
            variant: None,
            equation_tex: r"V' = V + r_1(V_{best}-TV_{mean}) + r_2(V_{best}-V_{rand}) - r_5(V_{worst}-V_{rand})",
            paper_refs: vec![rao_casting_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "samp_jaya",
            name: "SAMP-Jaya",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Self-adaptive multi-population Jaya}",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "qo_rao",
            name: "QO-Rao",
            family: "rao",
            variant: Some("Rao1"),
            equation_tex: r"\text{Rao + quasi-oppositional learning}",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "ehrjaya",
            name: "EHR-Jaya",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Classification-based hybrid Jaya + Rao-1}",
            paper_refs: vec![jaya_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "saphr",
            name: "SAPHR",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Self-adaptive hybrid Rao-1/2/3}",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "mo_bmr",
            name: "MO-BMR",
            family: "rao",
            variant: Some("MOBMR"),
            equation_tex: r"\text{MO extension of BMR}",
            paper_refs: vec![rao_casting_paper.clone(), rao_mo_mfg_paper.clone()],
            params: default_params(),
            multi_objective: true,
        },
        AlgorithmInfo {
            id: "mo_bwr",
            name: "MO-BWR",
            family: "rao",
            variant: Some("MOBWR"),
            equation_tex: r"\text{MO extension of BWR}",
            paper_refs: vec![rao_casting_paper.clone(), rao_mo_mfg_paper.clone()],
            params: default_params(),
            multi_objective: true,
        },
        AlgorithmInfo {
            id: "mo_bmwr",
            name: "MO-BMWR",
            family: "rao",
            variant: Some("MOBMWR"),
            equation_tex: r"\text{MO extension of BMWR}",
            paper_refs: vec![rao_energy_paper.clone(), rao_mo_mfg_paper.clone()],
            params: default_params(),
            multi_objective: true,
        },
        AlgorithmInfo {
            id: "mo_rao_de",
            name: "MO-Rao+DE",
            family: "rao",
            variant: None,
            equation_tex: r"\text{Rao-1 hybridised with DE/rand/1/bin}",
            paper_refs: vec![rao_series_paper.clone()],
            params: default_params(),
            multi_objective: true,
        },
        AlgorithmInfo {
            id: "pso",
            name: "PSO",
            family: "swarm",
            variant: None,
            equation_tex: r"v \leftarrow wv + c_1r_1(p_{best}-x) + c_2r_2(g_{best}-x)",
            paper_refs: vec![],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "de",
            name: "Differential Evolution",
            family: "evolutionary",
            variant: None,
            equation_tex: r"v = x_{r1} + F(x_{r2}-x_{r3})",
            paper_refs: vec![],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "ga",
            name: "Genetic Algorithm",
            family: "evolutionary",
            variant: None,
            equation_tex: r"\text{Crossover + mutation}",
            paper_refs: vec![],
            params: default_params(),
            multi_objective: false,
        },
        AlgorithmInfo {
            id: "nsga2",
            name: "NSGA-II",
            family: "multi-objective",
            variant: None,
            equation_tex: r"\text{FNDS + crowding distance + elitist truncation}",
            paper_refs: vec![],
            params: default_params(),
            multi_objective: true,
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
        BenchmarkInfo {
            id: "sphere",
            name: "Sphere",
            dim: 10,
            lower: -10.0,
            upper: 10.0,
            num_objectives: 1,
            ty: "single",
            optimum: Some(0.0),
        },
        BenchmarkInfo {
            id: "rastrigin",
            name: "Rastrigin",
            dim: 10,
            lower: -5.12,
            upper: 5.12,
            num_objectives: 1,
            ty: "single",
            optimum: Some(0.0),
        },
        BenchmarkInfo {
            id: "ackley",
            name: "Ackley",
            dim: 10,
            lower: -32.768,
            upper: 32.768,
            num_objectives: 1,
            ty: "single",
            optimum: Some(0.0),
        },
        BenchmarkInfo {
            id: "rosenbrock",
            name: "Rosenbrock",
            dim: 10,
            lower: -5.0,
            upper: 10.0,
            num_objectives: 1,
            ty: "single",
            optimum: Some(0.0),
        },
        BenchmarkInfo {
            id: "zdt1",
            name: "ZDT1",
            dim: 30,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 2,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "zdt2",
            name: "ZDT2",
            dim: 30,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 2,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "zdt3",
            name: "ZDT3",
            dim: 30,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 2,
            ty: "multi",
            optimum: None,
        },
        // ZDT5 is binary-coded and cannot be posed to these continuous
        // solvers, so the ZDT suite here is the other five (OPT-03).
        BenchmarkInfo {
            id: "zdt4",
            name: "ZDT4",
            dim: 10,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 2,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "zdt6",
            name: "ZDT6",
            dim: 10,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 2,
            ty: "multi",
            optimum: None,
        },
        // The usual DTLZ sizes at three objectives: k = 5 distance variables
        // for DTLZ1, 10 for DTLZ2-6, 20 for DTLZ7 (Deb et al. 2002).
        BenchmarkInfo {
            id: "dtlz1",
            name: "DTLZ1",
            dim: 7,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "dtlz2",
            name: "DTLZ2",
            dim: 12,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "dtlz3",
            name: "DTLZ3",
            dim: 12,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "dtlz4",
            name: "DTLZ4",
            dim: 12,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "dtlz5",
            name: "DTLZ5",
            dim: 12,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "dtlz6",
            name: "DTLZ6",
            dim: 12,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "dtlz7",
            name: "DTLZ7",
            dim: 22,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "multi",
            optimum: None,
        },
        BenchmarkInfo {
            id: "uc2_dosing",
            name: "UC2 — Drug-combination dosing",
            dim: 6,
            lower: 0.0,
            upper: 1.0,
            num_objectives: 3,
            ty: "usecase",
            optimum: None,
        },
    ]
}

// ---------- Problems ----------

fn single_obj(
    name: &str,
    dim: usize,
    lower: f64,
    upper: f64,
) -> SimpleProblem<fn(&Array1<f64>) -> f64> {
    let f: fn(&Array1<f64>) -> f64 = match name {
        "sphere" => |x| x.iter().map(|&v| v * v).sum(),
        "rastrigin" => |x| {
            let n = x.len() as f64;
            10.0 * n
                + x.iter()
                    .map(|&v| v * v - 10.0 * (2.0 * std::f64::consts::PI * v).cos())
                    .sum::<f64>()
        },
        "ackley" => |x| {
            let n = x.len() as f64;
            let s1: f64 = x.iter().map(|&v| v * v).sum();
            let s2: f64 = x
                .iter()
                .map(|&v| (2.0 * std::f64::consts::PI * v).cos())
                .sum();
            -20.0 * (-0.2 * (s1 / n).sqrt()).exp() - (s2 / n).exp() + 20.0 + std::f64::consts::E
        },
        "rosenbrock" => |x| {
            (0..x.len() - 1)
                .map(|i| {
                    let a = x[i + 1] - x[i] * x[i];
                    let b = 1.0 - x[i];
                    100.0 * a * a + b * b
                })
                .sum()
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

/// A catalogue benchmark with a known Pareto front: a ZDT or DTLZ problem
/// from `samyama-optimization`, at the catalogue's size.
///
/// The crate's definitions, not copies of them: this file used to carry its
/// own ZDT and DTLZ1, and its ZDT quietly ran ZDT1 for any variant it did not
/// know. One definition means the problem a client solves is the one its
/// quality is measured against.
enum FrontProblem {
    Zdt(ZDT),
    Dtlz(DTLZ),
}

impl FrontProblem {
    /// The problem for a catalogue id, or `None` for one that has no known
    /// front (single-objective, or a use case such as `uc2_dosing`).
    fn for_benchmark(bench_id: &str) -> Option<Self> {
        let info = benchmark_catalog()
            .into_iter()
            .find(|b| b.id == bench_id && b.ty == "multi")?;
        if let Some(variant) = bench_id.strip_prefix("zdt") {
            return Some(FrontProblem::Zdt(ZDT {
                variant: variant.parse().ok()?,
                dim: info.dim,
            }));
        }
        let variant = bench_id.strip_prefix("dtlz")?.parse().ok()?;
        Some(FrontProblem::Dtlz(DTLZ {
            variant,
            dim: info.dim,
            m: info.num_objectives,
        }))
    }

    /// The known front, sampled; see `ZDT::reference_front` and
    /// `DTLZ::reference_front`.
    fn reference_front(&self) -> Vec<Vec<f64>> {
        match self {
            FrontProblem::Zdt(p) => p.reference_front(ZDT_FRONT_SAMPLES),
            FrontProblem::Dtlz(p) => p.reference_front(DTLZ_FRONT_SAMPLES_PER_AXIS),
        }
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
    Iteration {
        iter: usize,
        best_fitness: f64,
        pareto_front: Option<Vec<Vec<f64>>>,
    },
    Done {
        final_fitness: f64,
        iterations: usize,
        final_pareto: Option<Vec<Vec<f64>>>,
        seed: Option<u64>,
        /// Wall-clock time of the solve itself, in milliseconds: from the moment
        /// the solver starts to the moment it returns. Queueing before the
        /// blocking task runs and streaming the iteration events afterwards are
        /// not part of it. Fractional, because a small solve finishes in well
        /// under a millisecond and rounding it to 0 would read as "not measured"
        /// (#1798, where the field was the constant 0).
        total_time_ms: f64,
        /// Whether the best solution satisfies its problem's constraints
        /// (OPT-09, #1776); see [`SolverOutcome::feasible`] for when it is null.
        feasible: Option<bool>,
        /// `final_fitness` minus the benchmark's known optimum (OPT-09, #1776);
        /// null when the benchmark has no single known optimum.
        gap: Option<f64>,
        /// The five front-quality measures (OPT-03, #1753); see
        /// [`SolverOutcome::quality`] for when it is null.
        quality: Option<QualityMetrics>,
        /// Objective evaluations the solve made (OPT-11, #1798).
        evaluations: u64,
        /// Whether the solve ran all its iterations or a budget stopped it.
        stopped_by: StopReason,
        /// The budget it ran within, echoed so the result says what it cost.
        budget: Budget,
    },
    Error {
        message: String,
    },
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

/// A solve request.
///
/// `deny_unknown_fields` is load-bearing, not tidiness (#1478). Without it a
/// mistyped or unsupported field — `n_var`, `num_variables`, `constraints` —
/// was dropped and the caller still got a job id, so nothing on this surface
/// could tell an **accepted** field from an **honoured** one: a nonsense field
/// returned 200 exactly as a real one did. An unknown field is now a 400 that
/// names it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Stop once this much wall-clock time has passed (OPT-11, #1798).
    #[serde(default)]
    time_budget_ms: Option<u64>,
    /// Stop once the objective has been evaluated this many times (OPT-11).
    #[serde(default)]
    max_evaluations: Option<u64>,
}

impl SolveReq {
    /// The request's budget. A limit of 0 is refused rather than read as "no
    /// limit" or "stop at once": either reading would be a guess.
    fn budget(&self) -> Result<Budget, String> {
        for (field, value) in [
            ("time_budget_ms", self.time_budget_ms),
            ("max_evaluations", self.max_evaluations),
        ] {
            if value == Some(0) {
                return Err(format!(
                    "{field} must be at least 1; leave it out for no limit"
                ));
            }
        }
        Ok(Budget {
            time: self.time_budget_ms.map(std::time::Duration::from_millis),
            evaluations: self.max_evaluations,
        })
    }
}

fn default_pop() -> usize {
    50
}
fn default_iter() -> usize {
    200
}

#[derive(Serialize)]
struct SolveResp {
    job_id: String,
}

/// The innermost message of a JSON rejection.
///
/// `JsonRejection`'s own `Display` is the generic wrapper ("Failed to
/// deserialize the JSON body into the target type"); the serde error that
/// names the field is one level down in the source chain.
fn rejection_detail(e: &JsonRejection) -> String {
    use std::error::Error;
    let mut src: Option<&(dyn Error + 'static)> = e.source();
    let mut deepest: Option<String> = None;
    while let Some(s) = src {
        deepest = Some(s.to_string());
        src = s.source();
    }
    deepest.unwrap_or_else(|| e.body_text())
}

async fn start_solve(
    State(state): State<Arc<OptimizeState>>,
    body: Result<Json<SolveReq>, JsonRejection>,
) -> Result<Json<SolveResp>, (StatusCode, String)> {
    // Take the rejection by hand rather than letting axum render it: the
    // default is a 422, and this surface answers a bad body with 400 (see the
    // unknown-algorithm and unknown-benchmark arms below). The serde message is
    // passed through verbatim because it is the part that names the offending
    // field — "unknown field `n_var`, expected one of ...". A 400 that does not
    // say which field is barely better than the silence it replaces.
    let Json(req) = body.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            format!("invalid solve request: {}", rejection_detail(&e)),
        )
    })?;

    // Validate benchmark + algorithm exist up-front.
    let bench = benchmark_catalog()
        .into_iter()
        .find(|b| b.id == req.benchmark)
        .ok_or((
            StatusCode::BAD_REQUEST,
            format!("unknown benchmark: {}", req.benchmark),
        ))?;
    let algo = algorithm_catalog()
        .into_iter()
        .find(|a| a.id == req.algorithm)
        .ok_or((
            StatusCode::BAD_REQUEST,
            format!("unknown algorithm: {}", req.algorithm),
        ))?;
    let budget = req.budget().map_err(|e| (StatusCode::BAD_REQUEST, e))?;

    let job_id = Uuid::new_v4().to_string();
    let (event_tx, event_rx) = mpsc::channel::<SseEvent>(256);
    let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel::<()>();

    {
        let mut jobs = state.jobs.lock().await;
        jobs.insert(
            job_id.clone(),
            JobHandle {
                cancel_tx: Some(cancel_tx),
                cancel_flag: None,
                event_rx: Some(event_rx),
            },
        );
    }

    let cfg = SolverConfig {
        population_size: req.population_size,
        max_iterations: req.iterations,
    };
    let dim = req.dim.unwrap_or(bench.dim);

    // Seed: threaded into the solver's RNG (#1478). `None` keeps the previous
    // behaviour (entropy) — an absent seed and a supplied one are different
    // requests, so there is no default here.
    let seed = req.seed;

    // Run in a blocking task so we don't stall the async runtime.
    // AtomicBool cancel flag — polled between the compute future and emit loop.
    let cancelled_flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_state = CancelHandle {
        flag: cancelled_flag.clone(),
    };

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
        // The clock starts inside the blocking task, so time spent waiting for
        // a blocking thread is not billed to the solver.
        let compute = tokio::task::spawn_blocking(move || {
            let started = std::time::Instant::now();
            let outcome = run_solver_within(
                &algo.id,
                algo.multi_objective,
                &bench.id,
                bench.num_objectives,
                dim,
                cfg,
                seed,
                budget,
            );
            let elapsed = started.elapsed();
            // Measured after the clock stops: `total_time_ms` is the solve.
            (outcome.map(|o| with_quality(o, bench.id)), elapsed)
        });

        match compute.await {
            Ok((
                Ok(SolverOutcome {
                    history,
                    final_fitness,
                    final_pareto,
                    feasible,
                    gap,
                    quality,
                    evaluations,
                    stopped_by,
                }),
                elapsed,
            )) => {
                let total_time_ms = duration_ms(elapsed);
                let iterations = history.len();
                let last_iter = iterations.saturating_sub(1);
                for (iter, best) in history.iter().enumerate() {
                    if cancelled_flag.load(std::sync::atomic::Ordering::Relaxed) {
                        let _ = event_tx
                            .send(SseEvent::Error {
                                message: "cancelled".into(),
                            })
                            .await;
                        return;
                    }
                    // Stamp the final Pareto front onto the LAST iteration event
                    // so the samyama-insight hook (which reads pareto_front from
                    // iteration events, not from done) renders the chart.
                    let pareto_for_event = if iter == last_iter {
                        final_pareto.clone()
                    } else {
                        None
                    };
                    if event_tx
                        .send(SseEvent::Iteration {
                            iter,
                            best_fitness: *best,
                            pareto_front: pareto_for_event,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = event_tx
                    .send(SseEvent::Done {
                        final_fitness,
                        iterations,
                        final_pareto,
                        seed,
                        total_time_ms,
                        feasible,
                        gap,
                        quality,
                        evaluations,
                        stopped_by,
                        budget,
                    })
                    .await;
            }
            Ok((Err(e), _)) => {
                let _ = event_tx.send(SseEvent::Error { message: e }).await;
            }
            Err(e) => {
                let _ = event_tx
                    .send(SseEvent::Error {
                        message: format!("join: {e}"),
                    })
                    .await;
            }
        }
    });

    Ok(Json(SolveResp { job_id }))
}

struct SolverOutcome {
    history: Vec<f64>,
    final_fitness: f64,
    /// Final Pareto front, one entry per non-dominated individual, each a vector
    /// of objective values. None for single-objective runs.
    final_pareto: Option<Vec<Vec<f64>>>,
    /// Whether the best solution satisfies the problem's constraints
    /// (OPT-09, #1776), read from `Problem::penalty`.
    ///
    /// None for multi-objective runs, deliberately. UC2 adds its
    /// contraindication penalty to every objective instead of reporting it
    /// through `MultiObjectiveProblem::penalties`, so each front point's
    /// `constraint_violation` is 0 whether or not it is penalised (#1795).
    /// Reading that field would report a penalised point as feasible; null
    /// says the surface cannot tell, which is true.
    feasible: Option<bool>,
    /// The best solution's objective minus the benchmark's known optimum
    /// (OPT-09, #1776). None when the catalog lists no optimum, which is every
    /// multi-objective benchmark: a front has no single optimum to subtract.
    gap: Option<f64>,
    /// Hypervolume, IGD, IGD+, spread and coverage of the final front against
    /// the benchmark's known front (OPT-03, #1753). None for a single-objective
    /// run and for a benchmark with no known front (`uc2_dosing`). Filled in
    /// by [`with_quality`] after the solve's clock has stopped, so measuring
    /// the answer is not billed to `total_time_ms`.
    quality: Option<QualityMetrics>,
    /// Objective evaluations the solve made, counted by [`Budgeted`].
    evaluations: u64,
    /// Whether the solve ran all its iterations or a budget stopped it.
    stopped_by: StopReason,
}

/// Measure `outcome`'s final front against `bench_id`'s known front, if both
/// exist. Runs after the solve is timed.
fn with_quality(mut outcome: SolverOutcome, bench_id: &str) -> SolverOutcome {
    if let (Some(front), Some(problem)) =
        (&outcome.final_pareto, FrontProblem::for_benchmark(bench_id))
    {
        outcome.quality = quality_metrics(front, &problem.reference_front());
    }
    outcome
}

/// Apply the request's seed to a solver, if one was given.
///
/// Each solver has its own type and its own inherent `with_seed`, so there is
/// no trait to be generic over; the macro is the cheapest way to say "seed it
/// if we have a seed, otherwise leave it on entropy" at each construction
/// site without repeating the `match` 23 times.
macro_rules! seeded {
    ($ctor:expr, $seed:expr) => {{
        let solver = $ctor;
        match $seed {
            Some(s) => solver.with_seed(s),
            None => solver,
        }
    }};
}

/// What one multi-objective run hands back: its convergence history, the best
/// first objective on its final front, and the front's finite points.
type MultiObjectiveRun = (Vec<f64>, f64, Vec<Vec<f64>>);

/// Run one multi-objective solver on `problem`: its convergence history, the
/// best first objective on the final front, and the front's finite points.
///
/// One function for every multi-objective benchmark; each used to repeat the
/// same solver `match` and the same post-processing.
fn solve_multi_objective<P: MultiObjectiveProblem>(
    algo_id: &str,
    cfg: SolverConfig,
    seed: Option<u64>,
    budget: Budget,
    problem: P,
) -> Result<(MultiObjectiveRun, u64, StopReason), String> {
    let problem = &Budgeted::new(problem, budget);
    let r = match algo_id {
        "mo_bmr" => seeded!(MOBMWRSolver::new(cfg, MOBMWRVariant::MOBMR), seed).solve(problem),
        "mo_bwr" => seeded!(MOBMWRSolver::new(cfg, MOBMWRVariant::MOBWR), seed).solve(problem),
        "mo_bmwr" => seeded!(MOBMWRSolver::new(cfg, MOBMWRVariant::MOBMWR), seed).solve(problem),
        "mo_rao_de" => seeded!(MORaoDESolver::new(cfg), seed).solve(problem),
        "nsga2" => seeded!(NSGA2Solver::new(cfg), seed).solve(problem),
        _ => return Err(format!("algorithm {} not multi-objective", algo_id)),
    };
    let final_first = r
        .pareto_front
        .iter()
        .map(|ind| ind.fitness[0])
        .fold(f64::INFINITY, f64::min);
    // Drop individuals with non-finite objective values — serde_json
    // serializes NaN/Inf as null, which breaks downstream consumers
    // (e.g. the samyama-insight Pareto chart).
    let pareto: Vec<Vec<f64>> = r
        .pareto_front
        .iter()
        .filter(|ind| ind.fitness.iter().all(|v| v.is_finite()))
        .map(|ind| ind.fitness.clone())
        .collect();
    Ok((
        (r.history, final_first, pareto),
        problem.evaluations(),
        problem.stop_reason(),
    ))
}

/// [`run_solver_within`] with no budget.
#[cfg(test)]
fn run_solver(
    algo_id: &str,
    multi_objective: bool,
    bench_id: &str,
    num_obj: usize,
    bench_dim: usize,
    cfg: SolverConfig,
    seed: Option<u64>,
) -> Result<SolverOutcome, String> {
    run_solver_within(
        algo_id,
        multi_objective,
        bench_id,
        num_obj,
        bench_dim,
        cfg,
        seed,
        Budget::default(),
    )
}

/// Run one solve, stopping early if `budget` is spent first.
#[allow(clippy::too_many_arguments)]
fn run_solver_within(
    algo_id: &str,
    multi_objective: bool,
    bench_id: &str,
    _num_obj: usize,
    bench_dim: usize,
    cfg: SolverConfig,
    seed: Option<u64>,
    budget: Budget,
) -> Result<SolverOutcome, String> {
    if multi_objective {
        let ((hist, final_f, pareto), evaluations, stopped_by) = if bench_id == "uc2_dosing" {
            solve_multi_objective(
                algo_id,
                cfg,
                seed,
                budget,
                super::uc_problems::UC2DosingProblem::new(),
            )?
        } else {
            match FrontProblem::for_benchmark(bench_id) {
                Some(FrontProblem::Zdt(problem)) => {
                    solve_multi_objective(algo_id, cfg, seed, budget, problem)?
                }
                Some(FrontProblem::Dtlz(problem)) => {
                    solve_multi_objective(algo_id, cfg, seed, budget, problem)?
                }
                None => return Err(format!("benchmark {} is not multi-objective", bench_id)),
            }
        };
        Ok(SolverOutcome {
            history: hist,
            final_fitness: final_f,
            final_pareto: Some(pareto),
            feasible: None,
            gap: None,
            quality: None,
            evaluations,
            stopped_by,
        })
    } else {
        let bench = benchmark_catalog()
            .into_iter()
            .find(|b| b.id == bench_id)
            .ok_or_else(|| format!("unknown benchmark: {bench_id}"))?;
        let dim = if bench_dim == 0 { bench.dim } else { bench_dim };
        let budgeted = Budgeted::new(single_obj(bench.id, dim, bench.lower, bench.upper), budget);
        let problem = &budgeted;

        let result = match algo_id {
            "jaya" => seeded!(JayaSolver::new(cfg), seed).solve(problem),
            "rao1" => seeded!(RaoSolver::new(cfg, RaoVariant::Rao1), seed).solve(problem),
            "rao2" => seeded!(RaoSolver::new(cfg, RaoVariant::Rao2), seed).solve(problem),
            "rao3" => seeded!(RaoSolver::new(cfg, RaoVariant::Rao3), seed).solve(problem),
            "tlbo" => seeded!(TLBOSolver::new(cfg), seed).solve(problem),
            "itlbo" => seeded!(ITLBOSolver::new(cfg), seed).solve(problem),
            "qojaya" => seeded!(QOJayaSolver::new(cfg), seed).solve(problem),
            "gotlbo" => seeded!(GOTLBOSolver::new(cfg), seed).solve(problem),
            "bmr" => seeded!(BMRSolver::new(cfg), seed).solve(problem),
            "bwr" => seeded!(BWRSolver::new(cfg), seed).solve(problem),
            "bmwr" => seeded!(BMWRSolver::new(cfg), seed).solve(problem),
            "samp_jaya" => seeded!(SAMPJayaSolver::new(cfg), seed).solve(problem),
            "qo_rao" => seeded!(QORaoSolver::new(cfg, RaoVariant::Rao1), seed).solve(problem),
            "ehrjaya" => seeded!(EHRJayaSolver::new(cfg), seed).solve(problem),
            "saphr" => seeded!(SAPHRSolver::new(cfg), seed).solve(problem),
            "pso" => seeded!(PSOSolver::new(cfg), seed).solve(problem),
            "de" => seeded!(DESolver::new(cfg), seed).solve(problem),
            "ga" => seeded!(GASolver::new(cfg), seed).solve(problem),
            other => {
                return Err(format!(
                    "algorithm {} not supported on single-objective benchmarks",
                    other
                ))
            }
        };
        // Measured on the bare problem: checking the answer is not part of
        // the solve, so it does not count against the budget.
        let problem = budgeted.inner();
        let objective = problem.objective(&result.best_variables);
        Ok(SolverOutcome {
            final_fitness: result.best_fitness,
            history: result.history,
            final_pareto: None,
            feasible: Some(is_feasible(problem.penalty(&result.best_variables))),
            gap: bench
                .optimum
                .map(|optimum| gap_to_best_known(objective, optimum)),
            quality: None,
            evaluations: budgeted.evaluations(),
            stopped_by: budgeted.stop_reason(),
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
            SseEvent::Iteration {
                iter,
                best_fitness,
                pareto_front,
            } => Event::default()
                .event("iteration")
                .json_data(serde_json::json!({
                    "iter": iter,
                    "best_fitness": best_fitness,
                    "pareto_front": pareto_front,
                }))
                .unwrap(),
            SseEvent::Done {
                final_fitness,
                iterations,
                final_pareto,
                seed,
                total_time_ms,
                feasible,
                gap,
                quality,
                evaluations,
                stopped_by,
                budget,
            } => Event::default()
                .event("done")
                .json_data(serde_json::json!({
                    "final_fitness": final_fitness,
                    "iterations": iterations,
                    "total_time_ms": total_time_ms,
                    "final_pareto": final_pareto,
                    // Echo the seed that produced this result: a published number
                    // should carry what is needed to re-derive it (#1478).
                    "seed": seed,
                    // The rest of the result envelope (OPT-09, #1776).
                    "feasible": feasible,
                    "gap": gap,
                    // How good the front is against the known one (OPT-03).
                    "quality": quality,
                    // What the solve cost and why it stopped (OPT-11, #1798).
                    "evaluations": evaluations,
                    "stopped_by": stopped_by,
                    "time_budget_ms": budget.time.map(|t| t.as_millis() as u64),
                    "max_evaluations": budget.evaluations,
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
struct CancelResp {
    cancelled: bool,
}

async fn cancel_solve(
    State(state): State<Arc<OptimizeState>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut jobs = state.jobs.lock().await;
    match jobs.get_mut(&id) {
        Some(h) => {
            if let Some(handle) = h.cancel_flag.as_ref() {
                handle
                    .flag
                    .store(true, std::sync::atomic::Ordering::Relaxed);
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

#[cfg(test)]
#[path = "optimize_cov_tests.rs"]
mod cov_tests;
