//! Independent benchmark suite for the Rao algorithm family and baselines.
//!
//! Built for Paper 8 (graph-grounded optimization). Provides:
//!   - [`single_objective`] — standard SO test functions (sphere, rastrigin,
//!     ackley, rosenbrock, griewank, schwefel, levy, zakharov, dixon-price,
//!     styblinski-tang) with documented global optima.
//!   - [`multi_objective`] — ZDT1-6 and DTLZ1-7.
//!   - [`runner`] — drives a set of solvers across a set of problems for K
//!     seeds and emits a record per (solver, problem, seed) cell.
//!
//! CSV emission and statistical analysis (Wilcoxon, HV, IGD) are wired in
//! `examples/run_baseline_suite.rs`. Non-determinism note: solvers currently
//! use `rand::thread_rng()` internally; replicate variance is reported in
//! mean/std/min/max but a fixed-seed mode is a planned follow-up.
//!
//! CEC2017/CEC2022 shifted+rotated functions require data files distributed
//! with the official CEC test packages. Loader stubs live in
//! [`cec_data`]; populate `data/cec/` from the official source to activate.

pub mod cec_data;
pub mod multi_objective;
pub mod runner;
pub mod single_objective;

pub use multi_objective::{moo_suite, MOProblemSpec, DTLZ, ZDT};
pub use runner::{run_mo_suite, run_so_suite, MORecord, SORecord};
pub use single_objective::{so_suite, SOProblemSpec};
