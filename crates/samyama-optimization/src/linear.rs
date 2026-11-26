//! Exact linear and mixed-integer programming (OPT-05, #1766).
//!
//! Every other solver in this crate is a population metaheuristic over a
//! black-box [`crate::common::Problem`]: it samples the objective, keeps the
//! best point it saw, and can say nothing about how far that point is from the
//! optimum. When the problem is linear, an exact solver does better: it
//! returns the optimum and *proves* it, or proves there is none.
//!
//! This module is that path. [`LinearModel`] is a structured linear program
//! (variables with bounds and an integrality flag, a linear objective, linear
//! constraints), the representation the crate README says MPS/LP export
//! (OPT-14) is blocked on. [`solve`] hands it to HiGHS, an open-source LP/MIP
//! solver (MIT), and reports its status as is: `optimal`, `infeasible`,
//! `unbounded`, or a limit it stopped at. It never reports a best-found point
//! as an optimum.
//!
//! [`LinearProgramSpec`] is the user-facing form, the same for the Cypher
//! procedure `algo.or.lp` and the HTTP route `POST /optimize/lp`:
//!
//! ```json
//! { "sense": "max",
//!   "objective": {"x": 3, "y": 2},
//!   "constraints": [{"terms": {"x": 1, "y": 1}, "op": "<=", "rhs": 4}],
//!   "bounds": {"y": [0, 3]},
//!   "integer": ["x"] }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

/// Most variables a model may have. A model is built from a request, so a
/// limit keeps one call from exhausting memory; it is far above what a call
/// written by hand or generated from a graph needs.
pub const MAX_VARIABLES: usize = 1_000_000;

/// Most nonzero constraint coefficients a model may have.
pub const MAX_NONZEROS: usize = 10_000_000;

/// Minimise or maximise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Sense {
    Min,
    Max,
}

impl Sense {
    /// `min`/`minimize`/`minimise` or `max`/`maximize`/`maximise`, any case.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "min" | "minimize" | "minimise" => Some(Sense::Min),
            "max" | "maximize" | "maximise" => Some(Sense::Max),
            _ => None,
        }
    }
}

/// How a constraint's left side relates to its right side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ConstraintOp {
    #[serde(rename = "<=")]
    Le,
    #[serde(rename = ">=")]
    Ge,
    #[serde(rename = "=")]
    Eq,
}

impl ConstraintOp {
    /// `<=`, `>=`, `=` or `==`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim() {
            "<=" => Some(ConstraintOp::Le),
            ">=" => Some(ConstraintOp::Ge),
            "=" | "==" => Some(ConstraintOp::Eq),
            _ => None,
        }
    }
}

/// One decision variable.
#[derive(Debug, Clone, PartialEq)]
pub struct Variable {
    pub name: String,
    /// `f64::NEG_INFINITY` for no lower bound.
    pub lower: f64,
    /// `f64::INFINITY` for no upper bound.
    pub upper: f64,
    pub integer: bool,
}

/// `sum(coefficient * variable) op rhs`.
#[derive(Debug, Clone, PartialEq)]
pub struct LinearConstraint {
    pub name: Option<String>,
    /// `(variable index, coefficient)`, each variable at most once.
    pub terms: Vec<(usize, f64)>,
    pub op: ConstraintOp,
    pub rhs: f64,
}

/// A linear or mixed-integer program.
#[derive(Debug, Clone, PartialEq)]
pub struct LinearModel {
    pub sense: Sense,
    pub variables: Vec<Variable>,
    /// One objective coefficient per variable, in `variables` order.
    pub objective: Vec<f64>,
    pub constraints: Vec<LinearConstraint>,
}

/// Why a model was refused before it was solved.
#[derive(Debug, Clone, PartialEq)]
pub struct LinearModelError(pub String);

impl std::fmt::Display for LinearModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LinearModelError {}

fn refuse<T>(msg: impl Into<String>) -> Result<T, LinearModelError> {
    Err(LinearModelError(msg.into()))
}

impl LinearModel {
    /// Whether any variable must be integral (a MILP rather than an LP).
    pub fn is_mip(&self) -> bool {
        self.variables.iter().any(|v| v.integer)
    }

    /// Refuse a model that cannot mean what it says: a NaN or infinite
    /// coefficient, an empty or inverted bound, a term naming no variable, a
    /// variable twice in one constraint. Each is refused by name rather than
    /// passed to the solver, which would either reject it with a code or
    /// quietly solve something else.
    pub fn validate(&self) -> Result<(), LinearModelError> {
        if self.variables.is_empty() {
            return refuse("the model has no variables: name at least one in `objective`, a constraint, `bounds` or `integer`");
        }
        if self.variables.len() > MAX_VARIABLES {
            return refuse(format!(
                "the model has {} variables; the limit is {MAX_VARIABLES}",
                self.variables.len()
            ));
        }
        if self.objective.len() != self.variables.len() {
            return refuse(format!(
                "{} objective coefficients for {} variables",
                self.objective.len(),
                self.variables.len()
            ));
        }
        let mut names = BTreeSet::new();
        for (v, c) in self.variables.iter().zip(&self.objective) {
            if v.name.is_empty() {
                return refuse("a variable has an empty name");
            }
            if !names.insert(v.name.as_str()) {
                return refuse(format!("variable `{}` is declared twice", v.name));
            }
            if !c.is_finite() {
                return refuse(format!(
                    "the objective coefficient of `{}` is {c}, not a finite number",
                    v.name
                ));
            }
            if v.lower.is_nan() || v.upper.is_nan() {
                return refuse(format!("a bound of `{}` is not a number", v.name));
            }
            if v.lower == f64::INFINITY || v.upper == f64::NEG_INFINITY {
                return refuse(format!(
                    "`{}` has a bound no value can meet: [{}, {}]",
                    v.name, v.lower, v.upper
                ));
            }
            if v.lower > v.upper {
                return refuse(format!(
                    "`{}` has lower bound {} above its upper bound {}",
                    v.name, v.lower, v.upper
                ));
            }
        }
        let mut nonzeros = 0usize;
        for (i, c) in self.constraints.iter().enumerate() {
            let label = c.name.clone().unwrap_or_else(|| format!("constraint {i}"));
            if !c.rhs.is_finite() {
                return refuse(format!(
                    "{label}: the right-hand side is {}, not a finite number",
                    c.rhs
                ));
            }
            let mut seen = BTreeSet::new();
            for &(var, coef) in &c.terms {
                if var >= self.variables.len() {
                    return refuse(format!(
                        "{label}: a term names variable index {var}, which does not exist"
                    ));
                }
                if !seen.insert(var) {
                    return refuse(format!(
                        "{label}: `{}` appears twice",
                        self.variables[var].name
                    ));
                }
                if !coef.is_finite() {
                    return refuse(format!(
                        "{label}: the coefficient of `{}` is {coef}, not a finite number",
                        self.variables[var].name
                    ));
                }
            }
            nonzeros += c.terms.len();
        }
        if nonzeros > MAX_NONZEROS {
            return refuse(format!(
                "the constraints have {nonzeros} coefficients; the limit is {MAX_NONZEROS}"
            ));
        }
        Ok(())
    }
}

/// How a solve ended, as HiGHS reported it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LinearStatus {
    /// Solved to proven optimality.
    Optimal,
    /// No point satisfies the constraints and bounds.
    Infeasible,
    /// The objective improves without limit.
    Unbounded,
    /// One of the two, and HiGHS did not establish which.
    InfeasibleOrUnbounded,
    /// The time limit stopped it. `values` holds the best point found, if any;
    /// it is not proven optimal.
    TimeLimit,
    /// Any other end: an iteration or memory limit, an interrupt, a solver
    /// error. Nothing is claimed about the answer.
    NotSolved,
}

impl LinearStatus {
    /// The status as its wire name: `optimal`, `infeasible`, `unbounded`,
    /// `infeasible_or_unbounded`, `time_limit` or `not_solved`.
    pub fn as_str(self) -> &'static str {
        match self {
            LinearStatus::Optimal => "optimal",
            LinearStatus::Infeasible => "infeasible",
            LinearStatus::Unbounded => "unbounded",
            LinearStatus::InfeasibleOrUnbounded => "infeasible_or_unbounded",
            LinearStatus::TimeLimit => "time_limit",
            LinearStatus::NotSolved => "not_solved",
        }
    }
}

/// The result of a solve.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct LinearSolution {
    pub status: LinearStatus,
    /// The objective at `values`; `None` when there is no point to report.
    pub objective: Option<f64>,
    /// One value per variable, in model order; `None` when there is no point.
    pub values: Option<Vec<f64>>,
    /// Whether `values` satisfies every constraint and bound: `true` when
    /// there is a point, `false` when the model is proven infeasible, `None`
    /// when that is unknown.
    pub feasible: Option<bool>,
    /// `0` exactly when `status` is `optimal`, which is proven; `None`
    /// otherwise. The result envelope's convention (OPT-09).
    pub gap: Option<f64>,
    /// For a MILP stopped early with a point: HiGHS's proven relative gap
    /// between that point and the best bound. `None` otherwise.
    pub mip_gap: Option<f64>,
    /// Simplex iterations.
    pub iterations: i64,
    /// Branch-and-bound nodes (a MILP), else 0.
    pub nodes: i64,
    pub total_time_ms: f64,
}

/// Limits on a solve.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SolveOptions {
    /// Stop after this long, with the best point found so far.
    pub time_limit: Option<Duration>,
}

/// Solve `model` with HiGHS.
///
/// The model is validated first and a bad one is refused with the reason.
/// HiGHS runs single-threaded and silent: a query does not take every core
/// or write to the server's stdout.
pub fn solve(
    model: &LinearModel,
    options: SolveOptions,
) -> Result<LinearSolution, LinearModelError> {
    use highs::{HighsModelStatus as H, RowProblem, Sense as HSense};

    model.validate()?;
    let started = Instant::now();

    let mut problem = RowProblem::default();
    let cols: Vec<highs::Col> = model
        .variables
        .iter()
        .zip(&model.objective)
        .map(|(v, &c)| problem.add_column_with_integrality(c, v.lower..=v.upper, v.integer))
        .collect();
    for c in &model.constraints {
        let row = c.terms.iter().map(|&(i, coef)| (cols[i], coef));
        match c.op {
            ConstraintOp::Le => problem.add_row(..=c.rhs, row),
            ConstraintOp::Ge => problem.add_row(c.rhs.., row),
            ConstraintOp::Eq => problem.add_row(c.rhs..=c.rhs, row),
        }
    }

    let sense = match model.sense {
        Sense::Min => HSense::Minimise,
        Sense::Max => HSense::Maximise,
    };
    let mut highs = problem
        .try_optimise(sense)
        .map_err(|s| LinearModelError(format!("HiGHS refused the model: {s:?}")))?;
    highs.make_quiet();
    highs.set_option("threads", 1);
    if let Some(limit) = options.time_limit {
        highs.set_option("time_limit", limit.as_secs_f64());
    }
    let solved = highs
        .try_solve()
        .map_err(|s| LinearModelError(format!("HiGHS failed to solve the model: {s:?}")))?;

    let has_point = matches!(
        solved.primal_solution_status(),
        highs::HighsSolutionStatus::Feasible
    );
    let status = match solved.status() {
        H::Optimal => LinearStatus::Optimal,
        H::Infeasible => LinearStatus::Infeasible,
        H::Unbounded => LinearStatus::Unbounded,
        H::UnboundedOrInfeasible => LinearStatus::InfeasibleOrUnbounded,
        H::ReachedTimeLimit => LinearStatus::TimeLimit,
        _ => LinearStatus::NotSolved,
    };
    // A point is reported only where it means something: at the optimum, or
    // as the best found when a limit stopped the search. An unbounded model's
    // last iterate is not an answer.
    let report_point =
        has_point && matches!(status, LinearStatus::Optimal | LinearStatus::TimeLimit);
    let values = report_point.then(|| solved.get_solution().columns().to_vec());
    let objective = values.as_ref().map(|x| {
        x.iter()
            .zip(&model.objective)
            .map(|(v, c)| v * c)
            .sum::<f64>()
    });
    let feasible = match status {
        LinearStatus::Optimal => Some(true),
        LinearStatus::Infeasible => Some(false),
        // Unbounded means feasible points exist, none of them optimal.
        LinearStatus::Unbounded => Some(true),
        LinearStatus::TimeLimit if has_point => Some(true),
        _ => None,
    };
    let mip_gap = (model.is_mip() && status == LinearStatus::TimeLimit && has_point)
        .then(|| solved.mip_gap())
        .filter(|g| g.is_finite());
    Ok(LinearSolution {
        status,
        objective,
        values,
        feasible,
        gap: (status == LinearStatus::Optimal).then_some(0.0),
        mip_gap,
        iterations: solved.simplex_iteration_count(),
        nodes: if model.is_mip() {
            solved.int_info_value(c"mip_node_count").unwrap_or(0)
        } else {
            0
        },
        total_time_ms: crate::common::duration_ms(started.elapsed()),
    })
}

// ---------- The user-facing form ----------

/// A linear program as a user writes it: variables by name.
///
/// Every name that appears in `objective`, a constraint's `terms`, `bounds` or
/// `integer` is a variable. A variable with no entry in `bounds` is
/// **non-negative**, `[0, +inf)`, the usual LP convention; `null` on either
/// side of a bound removes that side.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LinearProgramSpec {
    /// `min` (default) or `max`.
    #[serde(default)]
    pub sense: Option<String>,
    /// Variable name to objective coefficient. A variable left out has 0.
    #[serde(default)]
    pub objective: BTreeMap<String, f64>,
    #[serde(default)]
    pub constraints: Vec<ConstraintSpec>,
    /// Variable name to `[lower, upper]`; `null` is unbounded on that side.
    #[serde(default)]
    pub bounds: BTreeMap<String, (Option<f64>, Option<f64>)>,
    /// Names of the variables that must take integer values.
    #[serde(default)]
    pub integer: Vec<String>,
    /// Stop after this many milliseconds with the best point found.
    #[serde(default, alias = "time_limit_ms")]
    pub time_limit_ms: Option<u64>,
}

/// One constraint as a user writes it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConstraintSpec {
    #[serde(default)]
    pub name: Option<String>,
    /// Variable name to coefficient.
    pub terms: BTreeMap<String, f64>,
    /// `<=`, `>=` or `=`.
    pub op: String,
    pub rhs: f64,
}

impl LinearProgramSpec {
    /// The model this spec describes, with variables in name order, and the
    /// solve options it asks for. A spec that cannot be read is refused.
    pub fn to_model(&self) -> Result<(LinearModel, SolveOptions), LinearModelError> {
        let sense = match &self.sense {
            None => Sense::Min,
            Some(s) => Sense::parse(s).ok_or_else(|| {
                LinearModelError(format!("`sense` must be `min` or `max`, got `{s}`"))
            })?,
        };
        let mut names: BTreeSet<&str> = BTreeSet::new();
        names.extend(self.objective.keys().map(String::as_str));
        for c in &self.constraints {
            names.extend(c.terms.keys().map(String::as_str));
        }
        names.extend(self.bounds.keys().map(String::as_str));
        names.extend(self.integer.iter().map(String::as_str));
        let index: BTreeMap<&str, usize> = names.iter().enumerate().map(|(i, n)| (*n, i)).collect();
        let integer: BTreeSet<&str> = self.integer.iter().map(String::as_str).collect();

        let variables: Vec<Variable> = names
            .iter()
            .map(|&name| {
                let (lower, upper) = match self.bounds.get(name) {
                    None => (0.0, f64::INFINITY),
                    Some((lo, hi)) => {
                        (lo.unwrap_or(f64::NEG_INFINITY), hi.unwrap_or(f64::INFINITY))
                    }
                };
                Variable {
                    name: name.to_string(),
                    lower,
                    upper,
                    integer: integer.contains(name),
                }
            })
            .collect();
        let objective = names
            .iter()
            .map(|n| self.objective.get(*n).copied().unwrap_or(0.0))
            .collect();
        let constraints = self
            .constraints
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let op = ConstraintOp::parse(&c.op).ok_or_else(|| {
                    LinearModelError(format!(
                        "{}: `op` must be `<=`, `>=` or `=`, got `{}`",
                        c.name.clone().unwrap_or_else(|| format!("constraint {i}")),
                        c.op
                    ))
                })?;
                Ok(LinearConstraint {
                    name: c.name.clone(),
                    terms: c
                        .terms
                        .iter()
                        .map(|(n, &coef)| (index[n.as_str()], coef))
                        .collect(),
                    op,
                    rhs: c.rhs,
                })
            })
            .collect::<Result<Vec<_>, LinearModelError>>()?;
        let options = SolveOptions {
            time_limit: match self.time_limit_ms {
                Some(0) => {
                    return refuse("`timeLimitMs` must be at least 1; leave it out for no limit")
                }
                ms => ms.map(Duration::from_millis),
            },
        };
        let model = LinearModel {
            sense,
            variables,
            objective,
            constraints,
        };
        model.validate()?;
        Ok((model, options))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(json: serde_json::Value) -> LinearProgramSpec {
        serde_json::from_value(json).unwrap()
    }

    fn run(json: serde_json::Value) -> (LinearModel, LinearSolution) {
        let (m, o) = spec(json).to_model().unwrap();
        let s = solve(&m, o).unwrap();
        (m, s)
    }

    fn value(m: &LinearModel, s: &LinearSolution, name: &str) -> f64 {
        let i = m.variables.iter().position(|v| v.name == name).unwrap();
        s.values.as_ref().unwrap()[i]
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-6
    }

    /// max 3x + 2y s.t. x + y <= 4, x + 3y <= 6, x, y >= 0. Optimum 12 at
    /// (4, 0): every vertex of the feasible region checked by hand,
    /// (0,0)=0, (4,0)=12, (3,1)=11, (0,2)=4.
    #[test]
    fn a_two_variable_lp_returns_its_proven_optimum() {
        let (m, s) = run(serde_json::json!({
            "sense": "max",
            "objective": {"x": 3, "y": 2},
            "constraints": [
                {"terms": {"x": 1, "y": 1}, "op": "<=", "rhs": 4},
                {"terms": {"x": 1, "y": 3}, "op": "<=", "rhs": 6}
            ]
        }));
        assert_eq!(s.status, LinearStatus::Optimal);
        assert!(close(s.objective.unwrap(), 12.0), "{s:?}");
        assert!(close(value(&m, &s, "x"), 4.0) && close(value(&m, &s, "y"), 0.0));
        assert_eq!((s.gap, s.feasible, s.nodes), (Some(0.0), Some(true), 0));
    }

    /// A 0/1 knapsack whose LP relaxation is fractional: 19.2, at a = 1,
    /// b = 0.8. The integer optimum, 18 (b and c), is checked against all 8
    /// choices enumerated below.
    #[test]
    fn a_milp_returns_the_integer_optimum_not_the_relaxation() {
        let weights = [6.0, 5.0, 5.0];
        let values = [12.0, 9.0, 9.0];
        let cap = 10.0;
        let mut best = 0.0f64;
        for mask in 0..8u32 {
            let (w, v) = (0..3)
                .filter(|i| mask >> i & 1 == 1)
                .fold((0.0, 0.0), |(w, v), i| (w + weights[i], v + values[i]));
            if w <= cap {
                best = best.max(v);
            }
        }
        let (m, s) = run(serde_json::json!({
            "sense": "max",
            "objective": {"a": 12, "b": 9, "c": 9},
            "constraints": [{"name": "capacity", "terms": {"a": 6, "b": 5, "c": 5}, "op": "<=", "rhs": 10}],
            "bounds": {"a": [0, 1], "b": [0, 1], "c": [0, 1]},
            "integer": ["a", "b", "c"]
        }));
        assert_eq!(s.status, LinearStatus::Optimal);
        assert!(close(best, 18.0));
        assert!(close(s.objective.unwrap(), best), "{s:?} vs {best}");
        for v in s.values.as_ref().unwrap() {
            assert!(close(*v, v.round()), "integral: {v}");
        }
        assert!(m.is_mip());

        // The same model without integrality solves to the relaxation, which
        // is strictly better: so integrality was enforced above.
        let (_, relaxed) = run(serde_json::json!({
            "sense": "max",
            "objective": {"a": 12, "b": 9, "c": 9},
            "constraints": [{"terms": {"a": 6, "b": 5, "c": 5}, "op": "<=", "rhs": 10}],
            "bounds": {"a": [0, 1], "b": [0, 1], "c": [0, 1]}
        }));
        assert!(close(relaxed.objective.unwrap(), 19.2), "{relaxed:?}");
    }

    #[test]
    fn infeasible_is_reported_with_no_point() {
        let (_, s) = run(serde_json::json!({
            "objective": {"x": 1},
            "constraints": [
                {"terms": {"x": 1}, "op": ">=", "rhs": 5},
                {"terms": {"x": 1}, "op": "<=", "rhs": 3}
            ]
        }));
        assert!(
            matches!(
                s.status,
                LinearStatus::Infeasible | LinearStatus::InfeasibleOrUnbounded
            ),
            "{s:?}"
        );
        assert_eq!((s.objective, s.values.as_ref(), s.gap), (None, None, None));
        assert_ne!(s.feasible, Some(true));
    }

    #[test]
    fn unbounded_is_reported_with_no_point() {
        let (_, s) = run(serde_json::json!({
            "sense": "max",
            "objective": {"x": 1},
            "constraints": [{"terms": {"x": 1, "y": -1}, "op": "<=", "rhs": 1}]
        }));
        assert!(
            matches!(
                s.status,
                LinearStatus::Unbounded | LinearStatus::InfeasibleOrUnbounded
            ),
            "{s:?}"
        );
        assert_eq!((s.objective, s.values.as_ref(), s.gap), (None, None, None));
    }

    #[test]
    fn equality_ge_and_free_bounds_are_honoured() {
        // min x - y, x + y = 10, x >= 2 (as a constraint), y <= 7, y free below.
        // x = 10 - y, objective 10 - 2y, smallest at y = 7: x = 3, objective -4.
        let (m, s) = run(serde_json::json!({
            "objective": {"x": 1, "y": -1},
            "constraints": [
                {"terms": {"x": 1, "y": 1}, "op": "=", "rhs": 10},
                {"terms": {"x": 1}, "op": ">=", "rhs": 2}
            ],
            "bounds": {"y": [null, 7]}
        }));
        assert_eq!(s.status, LinearStatus::Optimal);
        assert!(close(s.objective.unwrap(), -4.0), "{s:?}");
        assert!(close(value(&m, &s, "x"), 3.0) && close(value(&m, &s, "y"), 7.0));
    }

    #[test]
    fn a_variable_left_out_of_bounds_is_non_negative() {
        let (_, s) = run(serde_json::json!({"objective": {"x": 1}}));
        assert_eq!(s.status, LinearStatus::Optimal);
        assert!(
            close(s.objective.unwrap(), 0.0),
            "min x with x >= 0 is 0: {s:?}"
        );
    }

    #[test]
    fn a_bad_spec_is_refused_by_name() {
        for (json, says) in [
            (serde_json::json!({}), "no variables"),
            (
                serde_json::json!({"sense": "up", "objective": {"x": 1}}),
                "`sense`",
            ),
            (
                serde_json::json!({"objective": {"x": 1}, "constraints": [{"terms": {"x": 1}, "op": "<", "rhs": 1}]}),
                "`op`",
            ),
            (
                serde_json::json!({"objective": {"x": 1}, "bounds": {"x": [5, 1]}}),
                "above its upper bound",
            ),
            (
                serde_json::json!({"objective": {"x": 1}, "timeLimitMs": 0}),
                "timeLimitMs",
            ),
        ] {
            let err = spec(json.clone()).to_model().expect_err(&json.to_string());
            assert!(err.0.contains(says), "{json}: {err}");
        }
        // An unknown key is a deserialisation error, not ignored.
        assert!(serde_json::from_value::<LinearProgramSpec>(
            serde_json::json!({"objectiv": {"x": 1}})
        )
        .is_err());
    }

    #[test]
    fn validate_refuses_what_the_spec_cannot_express() {
        let base = LinearModel {
            sense: Sense::Min,
            variables: vec![Variable {
                name: "x".into(),
                lower: 0.0,
                upper: 1.0,
                integer: false,
            }],
            objective: vec![1.0],
            constraints: vec![],
        };
        let mut m = base.clone();
        m.objective = vec![f64::NAN];
        assert!(m.validate().unwrap_err().0.contains("finite"));
        let mut m = base.clone();
        m.constraints = vec![LinearConstraint {
            name: None,
            terms: vec![(0, 1.0), (0, 2.0)],
            op: ConstraintOp::Le,
            rhs: 1.0,
        }];
        assert!(m.validate().unwrap_err().0.contains("twice"));
        let mut m = base.clone();
        m.constraints = vec![LinearConstraint {
            name: Some("c".into()),
            terms: vec![(3, 1.0)],
            op: ConstraintOp::Le,
            rhs: 1.0,
        }];
        assert!(m.validate().unwrap_err().0.contains("does not exist"));
        let mut m = base;
        m.variables[0].lower = f64::INFINITY;
        assert!(m.validate().unwrap_err().0.contains("no value can meet"));
    }

    /// A MILP too hard to finish in a millisecond stops at the time limit and
    /// says so; it does not claim the point it has is optimal.
    #[test]
    fn a_time_limit_is_reported_and_never_called_optimal() {
        // 30 items, a knapsack with awkward weights: hard enough that 1 ms is
        // not enough on any machine, small enough to build quickly.
        let n = 30;
        let mut objective = serde_json::Map::new();
        let mut terms = serde_json::Map::new();
        let mut bounds = serde_json::Map::new();
        let mut integer = Vec::new();
        for i in 0..n {
            let name = format!("x{i:02}");
            objective.insert(name.clone(), serde_json::json!(1000 + (i * 7919) % 997));
            terms.insert(name.clone(), serde_json::json!(1000 + (i * 104_729) % 991));
            bounds.insert(name.clone(), serde_json::json!([0, 1]));
            integer.push(name);
        }
        let (_, s) = run(serde_json::json!({
            "sense": "max",
            "objective": objective,
            "constraints": [{"terms": terms, "op": "<=", "rhs": 15_000.5}],
            "bounds": bounds,
            "integer": integer,
            "timeLimitMs": 1
        }));
        assert_ne!(s.gap, Some(0.0), "{s:?}");
        if s.status != LinearStatus::Optimal {
            assert_eq!(s.status, LinearStatus::TimeLimit, "{s:?}");
            assert_eq!(s.gap, None);
        }
    }
}
