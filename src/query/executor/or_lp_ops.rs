//! `algo.or.lp`: an exact linear or mixed-integer program, solved by HiGHS
//! (OPT-05, #1766).
//!
//! Every other `algo.or.*` procedure answers with a metaheuristic or a
//! fixed-shape template. This one takes the program itself, as one config map
//! in the form `samyama_optimization::linear::LinearProgramSpec` reads, and
//! returns HiGHS's proven answer:
//!
//! ```cypher
//! CALL algo.or.lp({
//!   sense: 'max',
//!   objective: {x: 3, y: 2},
//!   constraints: [{terms: {x: 1, y: 1}, op: '<=', rhs: 4},
//!                 {terms: {x: 1, y: 3}, op: '<=', rhs: 6}],
//!   integer: ['x']
//! }) YIELD status, objective, values, gap
//! ```
//!
//! It reads no graph and writes nothing. One row always comes back, so an
//! infeasible or unbounded program is an answer (`status`), not an empty
//! result a caller could mistake for a failed call.

use super::*;
use samyama_optimization::linear::{solve as solve_linear, LinearProgramSpec};

impl AlgorithmOperator {
    pub(super) fn execute_or_lp(&mut self) -> ExecutionResult<()> {
        let call = "algo.or.lp";
        let spec = match self.args.as_slice() {
            [Expression::Literal(m @ PropertyValue::Map(_))] => m.to_json(),
            _ => {
                return Err(ExecutionError::bad_argument(format!(
                    "{call} takes one config map: {{sense, objective, constraints, bounds, \
                     integer, timeLimitMs}}, for example \
                     {{sense: 'max', objective: {{x: 3, y: 2}}, \
                     constraints: [{{terms: {{x: 1, y: 1}}, op: '<=', rhs: 4}}]}}"
                )))
            }
        };
        let spec: LinearProgramSpec = serde_json::from_value(spec)
            .map_err(|e| ExecutionError::bad_argument(format!("{call}: {e}")))?;
        let (model, options) = spec
            .to_model()
            .map_err(|e| ExecutionError::bad_argument(format!("{call}: {e}")))?;
        let solution = solve_linear(&model, options)
            .map_err(|e| ExecutionError::RuntimeError(format!("{call}: {e}")))?;

        let float = |v: Option<f64>| match v {
            Some(x) => Value::Property(PropertyValue::Float(x)),
            None => Value::Null,
        };
        let mut record = Record::new();
        record.bind(
            "status".to_string(),
            Value::Property(PropertyValue::String(solution.status.as_str().to_string())),
        );
        // Variable name to value, the names the caller wrote.
        let values = match &solution.values {
            Some(xs) => Value::Property(PropertyValue::Map(
                model
                    .variables
                    .iter()
                    .zip(xs)
                    .map(|(v, x)| (v.name.clone(), PropertyValue::Float(*x)))
                    .collect(),
            )),
            None => Value::Null,
        };
        record.bind("values".to_string(), values);
        // The result envelope (OPT-09): `gap` is 0 only for a proven optimum.
        Self::bind_result_envelope(
            &mut record,
            float(solution.objective),
            match solution.feasible {
                Some(b) => Value::Property(PropertyValue::Boolean(b)),
                None => Value::Null,
            },
            float(solution.gap),
            solution.total_time_ms,
        );
        record.bind("mip_gap".to_string(), float(solution.mip_gap));
        record.bind(
            "iterations".to_string(),
            Value::Property(PropertyValue::Integer(solution.iterations)),
        );
        record.bind(
            "nodes".to_string(),
            Value::Property(PropertyValue::Integer(solution.nodes)),
        );
        record.bind(
            "solver".to_string(),
            Value::Property(PropertyValue::String("HiGHS".to_string())),
        );
        self.results.push(record);
        Ok(())
    }
}
