//! `CALL` of a procedure registered from Rust (LANG-10, #1674). The registry
//! itself is [`crate::udf`].

use std::sync::Arc;

use crate::graph::{GraphStore, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use crate::udf::Procedure;

/// Runs a registered procedure once and streams its rows.
pub struct UserProcedureOperator {
    procedure: Arc<Procedure>,
    args: Vec<PropertyValue>,
    /// `(column index, variable)` for each yielded column.
    yields: Vec<(usize, String)>,
    rows: Option<std::vec::IntoIter<Vec<PropertyValue>>>,
}

impl UserProcedureOperator {
    /// Arguments are evaluated once, here, as the built-in procedures'
    /// are. `yields` are `(name, alias)`; none yields every column.
    pub fn new(
        procedure: Arc<Procedure>,
        args: &[Expression],
        yields: &[(String, Option<String>)],
        store: &GraphStore,
    ) -> ExecutionResult<Self> {
        let name = procedure.name.clone();
        let args = args
            .iter()
            .enumerate()
            .map(
                |(i, expr)| match eval_expression(expr, &Record::new(), store)? {
                    Value::Property(p) => Ok(p),
                    Value::Null => Ok(PropertyValue::Null),
                    _ => Err(ExecutionError::bad_argument(format!(
                        "`{name}()` takes values, and argument {} is not one. Pass a \
                     property (`n.name`) or `properties(n)` instead.",
                        i + 1
                    ))),
                },
            )
            .collect::<ExecutionResult<Vec<_>>>()?;
        let yields = if yields.is_empty() {
            procedure.columns.iter().cloned().enumerate().collect()
        } else {
            yields
                .iter()
                .map(
                    |(column, alias)| match procedure.columns.iter().position(|c| c == column) {
                        Some(i) => Ok((i, alias.clone().unwrap_or_else(|| column.clone()))),
                        None => Err(ExecutionError::bad_argument(format!(
                            "`{name}()` yields {}; there is no `{column}`",
                            procedure.columns.join(", ")
                        ))),
                    },
                )
                .collect::<ExecutionResult<Vec<_>>>()?
        };
        Ok(Self {
            procedure,
            args,
            yields,
            rows: None,
        })
    }
}

impl PhysicalOperator for UserProcedureOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(self.procedure.call(store, &self.args)?.into_iter());
        }
        let Some(row) = self.rows.as_mut().and_then(Iterator::next) else {
            return Ok(None);
        };
        let mut record = Record::new();
        for (i, var) in &self.yields {
            let value = match &row[*i] {
                PropertyValue::Null => Value::Null,
                v => Value::Property(v.clone()),
            };
            record.bind(var.clone(), value);
        }
        Ok(Some(record))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "UserProcedure".to_string(),
            details: self.procedure.name.clone(),
            children: Vec::new(),
        }
    }
}
