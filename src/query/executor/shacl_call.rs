//! `CALL rdf.shacl.validate(shapes [, baseIri]) YIELD focus_node, result_path,
//! severity, source_constraint, message` (#2100, part 3).
//!
//! Validates the graph, read as RDF where it is, against SHACL shapes given as
//! Turtle; see [`crate::rdf::shacl`] for what is checked. One row per
//! violation, so no rows means the graph conforms. Writes nothing.

use std::collections::VecDeque;

use crate::graph::{GraphStore, PropertyValue};
use crate::query::executor::operator::{OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use crate::rdf::{shacl, GraphAsRdf, MappingConfig};

/// The base IRI when the call names none: `samyama convert`'s and the SPARQL
/// endpoint's, so shapes written against an export validate the live graph.
pub const DEFAULT_BASE_IRI: &str = "https://samyama.ai/rdf/";

/// The columns, in the order a bare `CALL` binds them.
pub const COLUMNS: [&str; 5] = ["focus_node", "result_path", "severity", "source_constraint", "message"];

pub struct ShaclValidateOperator {
    shapes: String,
    base: String,
    /// (column, the variable it binds), for the columns asked for.
    bind: Vec<(&'static str, String)>,
    rows: Option<VecDeque<Record>>,
}

impl ShaclValidateOperator {
    /// `yields` is the `YIELD` list as (name, alias); empty binds every column.
    pub fn new(shapes: String, base: Option<String>, yields: &[(String, Option<String>)]) -> ExecutionResult<Self> {
        let bind = if yields.is_empty() {
            COLUMNS.iter().map(|c| (*c, c.to_string())).collect()
        } else {
            yields
                .iter()
                .map(|(name, alias)| match COLUMNS.iter().find(|c| **c == name.as_str()) {
                    Some(c) => Ok((*c, alias.clone().unwrap_or_else(|| name.clone()))),
                    None => Err(ExecutionError::PlanningError(format!(
                        "rdf.shacl.validate yields {}, not `{name}`",
                        COLUMNS.join(", ")
                    ))),
                })
                .collect::<ExecutionResult<_>>()?
        };
        Ok(Self { shapes, base: base.unwrap_or_else(|| DEFAULT_BASE_IRI.to_string()), bind, rows: None })
    }

    fn run(&self, store: &GraphStore) -> ExecutionResult<VecDeque<Record>> {
        let data = GraphAsRdf::new(store, MappingConfig::new(self.base.clone()));
        // Refused, never answered with no rows: no rows reads as "conforms".
        let results = shacl::validate(&self.shapes, &data)
            .map_err(|e| ExecutionError::RuntimeError(format!("rdf.shacl.validate: {e}")))?;
        Ok(results
            .into_iter()
            .map(|r| {
                let mut record = Record::new();
                for (column, var) in &self.bind {
                    let s = |v: &String| Value::Property(PropertyValue::String(v.clone()));
                    let value = match *column {
                        "focus_node" => s(&r.focus_node),
                        "result_path" => r.result_path.as_ref().map(s).unwrap_or(Value::Null),
                        "severity" => s(&r.severity),
                        "source_constraint" => s(&r.source_constraint),
                        _ => s(&r.message),
                    };
                    record.bind(var.clone(), value);
                }
                record
            })
            .collect())
    }
}

impl PhysicalOperator for ShaclValidateOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        if self.rows.is_none() {
            self.rows = Some(self.run(store)?);
        }
        Ok(self.rows.as_mut().and_then(VecDeque::pop_front))
    }

    fn reset(&mut self) {
        self.rows = None;
    }

    fn describe(&self) -> OperatorDescription {
        OperatorDescription { name: "RdfShaclValidate".to_string(), details: format!("base={}", self.base), children: Vec::new() }
    }
}
