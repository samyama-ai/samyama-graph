//! The `db.retention.*` procedures (#2217): per-label bounds on how long
//! nodes are kept and how many. What a rule removes is in
//! [`crate::graph::retention`].
//!
//! | Call | Yields | Writes |
//! |---|---|---|
//! | `db.retention.set(label, {maxAge, maxRecords})` | `label, maxAgeMs, maxRecords, replaced` | the rules |
//! | `db.retention.drop(label)` | `label, dropped` | the rules |
//! | `db.retention.list()` | `label, maxAgeMs, maxRecords` | no |
//!
//! `maxAge` is a duration string (`'90s'`, `'15m'`, `'1h'`, `'7d'`) or an
//! integer number of milliseconds; `maxRecords` a positive integer. At least
//! one is required.

use std::collections::HashMap;

use crate::graph::retention::{parse_duration, RetentionRule};
use crate::graph::{GraphStore, PropertyValue};
use crate::query::ast::Expression;
use crate::query::executor::operator::{eval_expression, OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};

/// The procedures, by the name a caller writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionProcedure {
    Set,
    Drop,
    List,
}

impl RetentionProcedure {
    /// Which procedure `name` calls, case-insensitively, or `None`.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_lowercase().as_str() {
            "db.retention.set" => Some(Self::Set),
            "db.retention.drop" => Some(Self::Drop),
            "db.retention.list" => Some(Self::List),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Set => "db.retention.set",
            Self::Drop => "db.retention.drop",
            Self::List => "db.retention.list",
        }
    }

    /// Whether it changes the rules, and so needs the write path.
    pub fn writes(self) -> bool {
        !matches!(self, Self::List)
    }

    pub fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Set => &["label", "maxAgeMs", "maxRecords", "replaced"],
            Self::Drop => &["label", "dropped"],
            Self::List => &["label", "maxAgeMs", "maxRecords"],
        }
    }
}

/// Does this `CALL` change the retention rules? `Query::is_write()` asks.
pub fn call_writes(name: &str) -> bool {
    RetentionProcedure::from_name(name).is_some_and(RetentionProcedure::writes)
}

/// One `db.retention.*` call as a physical operator.
pub struct RetentionOperator {
    procedure: RetentionProcedure,
    args: Vec<Expression>,
    /// Output variable per column, after `YIELD ... AS`.
    columns: Vec<(&'static str, String)>,
    rows: Option<std::vec::IntoIter<Record>>,
}

fn bad(call: &str, msg: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::bad_argument(format!("{call}: {msg}"))
}

fn int_or_null(n: Option<u64>) -> Value {
    Value::Property(match n {
        Some(n) => PropertyValue::Integer(i64::try_from(n).unwrap_or(i64::MAX)),
        None => PropertyValue::Null,
    })
}

impl RetentionOperator {
    /// Built by the planner. `yields` are `(column, alias)` pairs; an empty
    /// list yields every column under its own name.
    pub fn new(
        procedure: RetentionProcedure,
        args: Vec<Expression>,
        yields: &[(String, Option<String>)],
    ) -> ExecutionResult<Self> {
        let all = procedure.columns();
        let columns = if yields.is_empty() {
            all.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            let mut columns = Vec::with_capacity(yields.len());
            for (name, alias) in yields {
                let Some(column) = all.iter().find(|c| **c == name.as_str()) else {
                    return Err(ExecutionError::PlanningError(format!(
                        "{} yields {}; it has no `{name}` column",
                        procedure.name(),
                        all.join(", ")
                    )));
                };
                columns.push((*column, alias.clone().unwrap_or_else(|| name.clone())));
            }
            columns
        };
        Ok(Self {
            procedure,
            args,
            columns,
            rows: None,
        })
    }

    fn row(&self, cells: &[(&str, Value)]) -> Record {
        let mut record = Record::new();
        for (column, alias) in &self.columns {
            if let Some((_, v)) = cells.iter().find(|(c, _)| c == column) {
                record.bind(alias.clone(), v.clone());
            }
        }
        record
    }

    fn arity(&self, expected: usize) -> ExecutionResult<()> {
        if self.args.len() == expected {
            return Ok(());
        }
        let shape = match self.procedure {
            RetentionProcedure::Set => "(label, {maxAge, maxRecords})",
            RetentionProcedure::Drop => "(label)",
            RetentionProcedure::List => "no arguments",
        };
        Err(bad(
            self.procedure.name(),
            format!("takes {shape}; got {} argument(s)", self.args.len()),
        ))
    }

    fn label(&self, store: &GraphStore) -> ExecutionResult<String> {
        match eval_expression(&self.args[0], &Record::new(), store)? {
            Value::Property(PropertyValue::String(s)) if !s.trim().is_empty() => Ok(s),
            other => Err(bad(
                self.procedure.name(),
                format!("the label must be a non-empty string, got {other:?}"),
            )),
        }
    }

    fn rule(&self, store: &GraphStore) -> ExecutionResult<RetentionRule> {
        let call = self.procedure.name();
        let config: HashMap<String, PropertyValue> =
            match eval_expression(&self.args[1], &Record::new(), store)? {
                Value::Property(PropertyValue::Map(m)) => m,
                other => {
                    return Err(bad(
                        call,
                        format!("the second argument must be a map, got {other:?}"),
                    ))
                }
            };
        if let Some(unknown) = config.keys().find(|k| *k != "maxAge" && *k != "maxRecords") {
            return Err(bad(
                call,
                format!("unknown key `{unknown}`. It reads: maxAge, maxRecords"),
            ));
        }
        let max_age_ms = match config.get("maxAge") {
            None | Some(PropertyValue::Null) => None,
            Some(PropertyValue::String(s)) => Some(parse_duration(s).map_err(|e| bad(call, format!("maxAge: {e}")))?),
            Some(PropertyValue::Integer(n)) if *n > 0 => Some(*n as u64),
            Some(other) => {
                return Err(bad(
                    call,
                    format!("maxAge must be a duration such as '1h' or a positive number of milliseconds, got {other:?}"),
                ))
            }
        };
        let max_records = match config.get("maxRecords") {
            None | Some(PropertyValue::Null) => None,
            Some(PropertyValue::Integer(n)) if *n > 0 => Some(*n as u64),
            Some(other) => {
                return Err(bad(
                    call,
                    format!("maxRecords must be a positive integer, got {other:?}"),
                ))
            }
        };
        if max_age_ms.is_none() && max_records.is_none() {
            return Err(bad(
                call,
                "give maxAge, maxRecords or both; to remove a rule, call db.retention.drop",
            ));
        }
        Ok(RetentionRule {
            max_age_ms,
            max_records,
        })
    }

    fn list(&self, store: &GraphStore) -> ExecutionResult<Vec<Record>> {
        self.arity(0)?;
        Ok(store
            .retention_rules()
            .iter()
            .map(|(label, rule)| {
                self.row(&[
                    (
                        "label",
                        Value::Property(PropertyValue::String(label.clone())),
                    ),
                    ("maxAgeMs", int_or_null(rule.max_age_ms)),
                    ("maxRecords", int_or_null(rule.max_records)),
                ])
            })
            .collect())
    }

    fn compute_mut(&self, store: &mut GraphStore) -> ExecutionResult<Vec<Record>> {
        match self.procedure {
            RetentionProcedure::List => self.list(store),
            RetentionProcedure::Set => {
                self.arity(2)?;
                let label = self.label(store)?;
                let rule = self.rule(store)?;
                let replaced = store.set_retention(&label, rule).is_some();
                Ok(vec![self.row(&[
                    ("label", Value::Property(PropertyValue::String(label))),
                    ("maxAgeMs", int_or_null(rule.max_age_ms)),
                    ("maxRecords", int_or_null(rule.max_records)),
                    (
                        "replaced",
                        Value::Property(PropertyValue::Boolean(replaced)),
                    ),
                ])])
            }
            RetentionProcedure::Drop => {
                self.arity(1)?;
                let label = self.label(store)?;
                let dropped = store.drop_retention(&label).is_some();
                Ok(vec![self.row(&[
                    ("label", Value::Property(PropertyValue::String(label))),
                    ("dropped", Value::Property(PropertyValue::Boolean(dropped))),
                ])])
            }
        }
    }
}

impl PhysicalOperator for RetentionOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.procedure.writes() {
            // `Query::is_write` sends these to the write executor; reaching
            // here means a read executor was built for one directly.
            return Err(ExecutionError::write_in_read(format!(
                "{} changes what later sweeps delete, so it needs the write path",
                self.procedure.name()
            )));
        }
        if self.rows.is_none() {
            self.rows = Some(self.list(store)?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn next_mut(
        &mut self,
        store: &mut GraphStore,
        _tenant_id: &str,
    ) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(self.compute_mut(store)?.into_iter());
        }
        Ok(self.rows.as_mut().and_then(Iterator::next))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn is_mutating(&self) -> bool {
        self.procedure.writes()
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Retention".to_string(),
            details: self.procedure.name().to_string(),
            children: Vec::new(),
        }
    }
}
