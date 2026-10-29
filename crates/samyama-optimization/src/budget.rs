//! A time or evaluation budget for a solve (OPT-11, #1798).
//!
//! A solver runs `max_iterations` iterations; nothing else could stop it. A
//! caller with a deadline, or a cap on how many times an expensive objective
//! may be called, had to guess an iteration count and hope.
//!
//! [`Budgeted`] wraps a problem, counts every evaluation and watches the
//! clock. Every solver asks [`Problem::budget_exhausted`] (or its
//! multi-objective twin) at the top of each iteration and stops there, so a
//! budgeted run ends exactly as a run with fewer iterations would, and its
//! result is the best found so far.
//!
//! The check is between iterations, so a run can overshoot by at most one
//! iteration's evaluations. The evaluations a solver makes before its first
//! iteration (its initial population) always happen.

use crate::common::{MultiObjectiveProblem, Problem};
use ndarray::Array1;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// The limits a run may not pass. `None` in a field means no limit of that
/// kind; both `None` is a budget that never runs out.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Budget {
    /// Wall-clock time from the first evaluation.
    pub time: Option<Duration>,
    /// Objective evaluations.
    pub evaluations: Option<u64>,
}

/// Why a run stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// It ran every iteration it was configured for.
    Iterations,
    /// The time budget ran out.
    TimeBudget,
    /// The evaluation budget ran out.
    EvaluationBudget,
}

/// `StopReason` packed into an `AtomicU8`; 0 means "not stopped by a budget".
const NOT_STOPPED: u8 = 0;
const STOPPED_BY_TIME: u8 = 1;
const STOPPED_BY_EVALUATIONS: u8 = 2;

/// A problem with a [`Budget`]: it counts its evaluations and tells the solver
/// when the budget is spent.
///
/// Evaluations are counted atomically, so a solver that evaluates in parallel
/// is counted correctly.
pub struct Budgeted<P> {
    inner: P,
    budget: Budget,
    evaluations: AtomicU64,
    /// Set by the first evaluation, so time spent building the solver is not
    /// billed to the budget.
    started: OnceLock<Instant>,
    stopped: AtomicU8,
}

impl<P> Budgeted<P> {
    pub fn new(inner: P, budget: Budget) -> Self {
        Self {
            inner,
            budget,
            evaluations: AtomicU64::new(0),
            started: OnceLock::new(),
            stopped: AtomicU8::new(NOT_STOPPED),
        }
    }

    /// The wrapped problem.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// How many times the objective has been evaluated.
    pub fn evaluations(&self) -> u64 {
        self.evaluations.load(Ordering::Relaxed)
    }

    /// Why the run stopped: a budget, if the solver stopped because one was
    /// spent, otherwise its iteration count.
    pub fn stop_reason(&self) -> StopReason {
        match self.stopped.load(Ordering::Relaxed) {
            STOPPED_BY_TIME => StopReason::TimeBudget,
            STOPPED_BY_EVALUATIONS => StopReason::EvaluationBudget,
            _ => StopReason::Iterations,
        }
    }

    fn count(&self) {
        self.started.get_or_init(Instant::now);
        self.evaluations.fetch_add(1, Ordering::Relaxed);
    }

    /// Whether the budget is spent, recording which limit was hit. Answering
    /// true is what stops the solver, so the reason is recorded only then.
    fn exhausted(&self) -> bool {
        let reason = if self
            .budget
            .evaluations
            .is_some_and(|max| self.evaluations() >= max)
        {
            STOPPED_BY_EVALUATIONS
        } else if self
            .budget
            .time
            .zip(self.started.get())
            .is_some_and(|(limit, started)| started.elapsed() >= limit)
        {
            STOPPED_BY_TIME
        } else {
            return false;
        };
        self.stopped.store(reason, Ordering::Relaxed);
        true
    }
}

impl<P: Problem> Problem for Budgeted<P> {
    fn objective(&self, variables: &Array1<f64>) -> f64 {
        self.count();
        self.inner.objective(variables)
    }

    fn penalty(&self, variables: &Array1<f64>) -> f64 {
        self.inner.penalty(variables)
    }

    /// Counted once, and delegated whole, so a problem that overrides
    /// `fitness` keeps its own.
    fn fitness(&self, variables: &Array1<f64>) -> f64 {
        self.count();
        self.inner.fitness(variables)
    }

    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        self.inner.bounds()
    }

    fn budget_exhausted(&self) -> bool {
        self.exhausted() || self.inner.budget_exhausted()
    }
}

impl<P: MultiObjectiveProblem> MultiObjectiveProblem for Budgeted<P> {
    fn objectives(&self, variables: &Array1<f64>) -> Vec<f64> {
        self.count();
        self.inner.objectives(variables)
    }

    fn penalties(&self, variables: &Array1<f64>) -> Vec<f64> {
        self.inner.penalties(variables)
    }

    fn dim(&self) -> usize {
        self.inner.dim()
    }

    fn bounds(&self) -> (Array1<f64>, Array1<f64>) {
        self.inner.bounds()
    }

    fn num_objectives(&self) -> usize {
        self.inner.num_objectives()
    }

    fn budget_exhausted(&self) -> bool {
        self.exhausted() || self.inner.budget_exhausted()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SimpleProblem;

    const DIM: usize = 3;

    fn sphere() -> SimpleProblem<fn(&Array1<f64>) -> f64> {
        SimpleProblem {
            objective_func: |x| x.iter().map(|v| v * v).sum(),
            dim: DIM,
            lower: Array1::from_elem(DIM, -1.0),
            upper: Array1::from_elem(DIM, 1.0),
        }
    }

    fn point() -> Array1<f64> {
        Array1::zeros(DIM)
    }

    #[test]
    fn no_budget_never_runs_out() {
        let p = Budgeted::new(sphere(), Budget::default());
        for _ in 0..1000 {
            p.objective(&point());
        }
        assert!(!p.budget_exhausted());
        assert_eq!(p.stop_reason(), StopReason::Iterations);
        assert_eq!(p.evaluations(), 1000);
    }

    #[test]
    fn the_evaluation_budget_runs_out_at_its_count() {
        let max = 5;
        let p = Budgeted::new(
            sphere(),
            Budget {
                evaluations: Some(max),
                ..Budget::default()
            },
        );
        for _ in 0..max - 1 {
            p.objective(&point());
        }
        assert!(!p.budget_exhausted(), "one evaluation left");
        assert_eq!(p.stop_reason(), StopReason::Iterations, "not stopped yet");
        p.fitness(&point());
        assert!(p.budget_exhausted());
        assert_eq!(p.stop_reason(), StopReason::EvaluationBudget);
    }

    #[test]
    fn fitness_and_objective_each_count_once() {
        let p = Budgeted::new(sphere(), Budget::default());
        p.fitness(&point());
        p.objective(&point());
        p.penalty(&point());
        assert_eq!(p.evaluations(), 2, "penalty is not an evaluation");
    }

    #[test]
    fn the_time_budget_starts_at_the_first_evaluation() {
        let limit = Duration::from_millis(20);
        let p = Budgeted::new(
            sphere(),
            Budget {
                time: Some(limit),
                ..Budget::default()
            },
        );
        std::thread::sleep(limit * 2);
        assert!(
            !p.budget_exhausted(),
            "nothing evaluated: the clock has not started"
        );
        p.objective(&point());
        assert!(!p.budget_exhausted());
        std::thread::sleep(limit * 2);
        assert!(p.budget_exhausted());
        assert_eq!(p.stop_reason(), StopReason::TimeBudget);
    }

    #[test]
    fn the_wrapper_answers_with_the_inner_problem() {
        let p = Budgeted::new(sphere(), Budget::default());
        let x = Array1::from_elem(DIM, 0.5);
        assert_eq!(p.objective(&x), sphere().objective(&x));
        assert_eq!(p.dim(), DIM);
        assert_eq!(p.bounds(), sphere().bounds());
        assert_eq!(p.inner().dim(), DIM);
    }
}
