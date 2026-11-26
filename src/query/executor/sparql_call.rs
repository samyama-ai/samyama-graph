//! `CALL rdf.sparql(query [, base]) YIELD row` (#2100, part 3b).
//!
//! SPARQL from inside Cypher, for clients that reach the engine over RESP, an
//! SDK or MCP rather than HTTP. The query reads the graph through
//! [`GraphAsRdf`](crate::rdf::GraphAsRdf), exactly as `POST /api/sparql`
//! does, and writes nothing.
//!
//! Each SPARQL solution is one Cypher row with one column, `row`: a map from
//! variable name to value. One column whatever the query asks, so `YIELD row`
//! is always the same shape and `RETURN row.name` reads a variable. Values are
//! Cypher values, not RDF terms: a typed literal becomes the value it encodes
//! (the export's own `value_for`, so `30` is the integer 30), an IRI or blank
//! node becomes its string. A variable left unbound is absent from the map,
//! so `row.x` reads `null`. ASK yields one row, `{result: true|false}`.

use std::collections::{HashMap, VecDeque};

use crate::graph::{GraphStore, PropertyValue};
use crate::query::executor::operator::{OperatorDescription, PhysicalOperator};
use crate::query::executor::{ExecutionError, ExecutionResult, Record, Value};
use crate::rdf::{MappingConfig, RdfTerm};
use crate::sparql::{SparqlEngine, SparqlResults};

/// The base IRI used when the call names none: `POST /api/sparql`'s, which is
/// `samyama convert`'s.
pub const DEFAULT_BASE_IRI: &str = "https://samyama.ai/rdf/";

/// Runs one SPARQL query on first pull and yields its solutions.
pub struct SparqlCallOperator {
    query: String,
    base: String,
    rows: Option<VecDeque<Record>>,
}

impl SparqlCallOperator {
    pub fn new(query: String, base: Option<String>) -> Self {
        Self { query, base: base.unwrap_or_else(|| DEFAULT_BASE_IRI.to_string()), rows: None }
    }

    fn run(&self, store: &GraphStore) -> ExecutionResult<VecDeque<Record>> {
        let results = SparqlEngine::over_graph(store, MappingConfig::new(self.base.clone()))
            .query(&self.query)
            // Refused, never answered with no rows: an empty result to a query
            // the engine did not understand reads as "no matches".
            .map_err(|e| ExecutionError::RuntimeError(format!("rdf.sparql: {e}")))?;
        let row = |m: HashMap<String, PropertyValue>| {
            let mut r = Record::new();
            r.bind("row".to_string(), Value::Property(PropertyValue::Map(m)));
            r
        };
        match results {
            SparqlResults::Bindings { variables, solutions } => Ok(solutions
                .iter()
                .map(|s| {
                    row(variables
                        .iter()
                        .filter_map(|v| s.get(v).map(|t| (v.clone(), cypher_value(t))))
                        .collect())
                })
                .collect()),
            SparqlResults::Boolean(b) => {
                Ok(VecDeque::from([row(HashMap::from([("result".to_string(), PropertyValue::Boolean(b))]))]))
            }
            SparqlResults::Graph(_) => Err(ExecutionError::RuntimeError(
                "rdf.sparql: CONSTRUCT and DESCRIBE return a graph, which is not evaluated yet".into(),
            )),
        }
    }
}

/// An RDF term as the Cypher value a client reads.
fn cypher_value(t: &RdfTerm) -> PropertyValue {
    match t {
        RdfTerm::NamedNode(n) => PropertyValue::String(n.as_str().to_string()),
        RdfTerm::BlankNode(b) => PropertyValue::String(format!("_:{}", b.as_str())),
        RdfTerm::Literal(l) => crate::rdf::value_for(l),
    }
}

impl PhysicalOperator for SparqlCallOperator {
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
        OperatorDescription {
            name: "RdfSparql".to_string(),
            details: format!("base={}", self.base),
            children: Vec::new(),
        }
    }
}
