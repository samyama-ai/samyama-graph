//! SPARQL 1.1 query language support
//!
//! This module implements SPARQL query parsing and execution.
//!
//! # Requirements Coverage
//!
//! - REQ-SPARQL-001: SPARQL 1.1 query language
//! - REQ-SPARQL-002: SPARQL HTTP protocol
//! - REQ-SPARQL-003: Query forms (SELECT, CONSTRUCT, ASK, DESCRIBE)
//! - REQ-SPARQL-004: SPARQL UPDATE operations
//! - REQ-SPARQL-005: Filtering and constraints
//! - REQ-SPARQL-006: Aggregates
//! - REQ-SPARQL-007: Federation (SERVICE keyword)
//! - REQ-SPARQL-008: Query optimization
//!
//! # Example
//!
//! ```rust,ignore
//! use samyama::sparql::{SparqlEngine, SparqlResults};
//! use samyama::rdf::RdfStore;
//!
//! let store = RdfStore::new();
//! let engine = SparqlEngine::new(store);
//!
//! let query = r#"
//!     PREFIX foaf: <http://xmlns.com/foaf/0.1/>
//!     SELECT ?name WHERE {
//!         ?person foaf:name ?name .
//!     }
//! "#;
//!
//! let results = engine.query(query).unwrap();
//! ```

mod parser;
mod executor;
mod algebra;
mod optimizer;
mod results;
mod http;

pub use parser::{SparqlParser, ParseError as SparqlParseError};
pub use executor::{SparqlExecutor, ExecutionError};
pub use results::{SparqlResults, ResultFormat, QuerySolution};
pub use http::{SparqlHttpEndpoint, HttpError};

use crate::rdf::RdfStore;
use thiserror::Error;

/// SPARQL errors
#[derive(Error, Debug)]
pub enum SparqlError {
    /// Parse error
    #[error("Parse error: {0}")]
    Parse(String),

    /// Execution error
    #[error("Execution error: {0}")]
    Execution(String),

    /// Type error
    #[error("Type error: {0}")]
    Type(String),

    /// HTTP error
    #[error("HTTP error: {0}")]
    Http(String),
}

pub type SparqlResult<T> = Result<T, SparqlError>;

/// SPARQL query engine
pub struct SparqlEngine {
    store: RdfStore,
    executor: SparqlExecutor,
}

impl SparqlEngine {
    /// Create a new SPARQL engine
    pub fn new(store: RdfStore) -> Self {
        Self {
            store: store.clone(),
            executor: SparqlExecutor::new(store),
        }
    }

    /// Execute a SPARQL query. SELECT and ASK are evaluated; a form or
    /// feature that is not evaluated yet is an error, never an empty result.
    pub fn query(&self, query_str: &str) -> SparqlResult<SparqlResults> {
        use spargebra::Query;
        let query = SparqlParser::parse(query_str).map_err(|e| SparqlError::Parse(e.to_string()))?;
        let run = |p| self.executor.evaluate(p).map_err(|e| SparqlError::Execution(e.to_string()));
        match &query {
            Query::Select { pattern, .. } => {
                let solutions = run(pattern)?;
                Ok(SparqlResults::Bindings { variables: projected(pattern), solutions })
            }
            Query::Ask { pattern, .. } => Ok(SparqlResults::Boolean(!run(pattern)?.is_empty())),
            Query::Construct { .. } | Query::Describe { .. } => Err(SparqlError::Execution(
                "CONSTRUCT and DESCRIBE are not evaluated yet".into(),
            )),
        }
    }

    /// Execute a SPARQL UPDATE operation. Not implemented: refused rather
    /// than reported as done, since a caller would read `Ok` as "written".
    pub fn update(&mut self, _update_str: &str) -> SparqlResult<()> {
        Err(SparqlError::Execution("SPARQL UPDATE is not supported yet".into()))
    }
}

/// The variables a SELECT returns, in the order it names them.
fn projected(pattern: &spargebra::algebra::GraphPattern) -> Vec<String> {
    use spargebra::algebra::GraphPattern as P;
    match pattern {
        P::Project { variables, .. } => variables.iter().map(|v| v.as_str().to_string()).collect(),
        P::Distinct { inner } | P::Reduced { inner } | P::Slice { inner, .. } | P::OrderBy { inner, .. } => {
            projected(inner)
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_creation() {
        let store = RdfStore::new();
        let _engine = SparqlEngine::new(store);
    }

    #[test]
    fn test_query_stub() {
        let store = RdfStore::new();
        let engine = SparqlEngine::new(store);

        let result = engine.query("SELECT * WHERE { ?s ?p ?o }");
        assert!(result.is_ok());
    }

    #[test]
    fn update_is_refused_rather_than_reported_as_written() {
        let mut engine = SparqlEngine::new(RdfStore::new());
        assert!(engine.update("INSERT DATA { <a:s> <a:p> <a:o> }").is_err());
        assert_eq!(engine.store.len(), 0);
    }

    #[test]
    fn stub_parser_accepts_update_text() {
        assert!(SparqlParser::parse_update("CLEAR ALL").is_ok());
    }

    #[tokio::test]
    async fn stub_http_endpoint_start_returns_immediately() {
        assert!(SparqlHttpEndpoint::new().start(0).await.is_ok());
        assert!(SparqlHttpEndpoint::default().start(1).await.is_ok());
    }

    #[test]
    fn stub_results_serialize_to_empty_text_in_every_format() {
        let r = SparqlResults::Boolean(true);
        for f in [ResultFormat::Json, ResultFormat::Xml, ResultFormat::Csv, ResultFormat::Tsv] {
            assert_eq!(r.serialize(f).unwrap(), "");
        }
        let g = SparqlResults::Graph(vec![]);
        assert!(matches!(g.clone(), SparqlResults::Graph(t) if t.is_empty()));
    }

    #[test]
    fn error_messages_name_their_kind() {
        assert_eq!(SparqlError::Parse("a".into()).to_string(), "Parse error: a");
        assert_eq!(SparqlError::Execution("b".into()).to_string(), "Execution error: b");
        assert_eq!(SparqlError::Type("c".into()).to_string(), "Type error: c");
        assert_eq!(SparqlError::Http("d".into()).to_string(), "HTTP error: d");
        assert_eq!(SparqlParseError::Syntax("e".into()).to_string(), "Syntax error: e");
        assert_eq!(SparqlParseError::Unsupported("f".into()).to_string(), "Unsupported feature: f");
        assert_eq!(ExecutionError::Query("g".into()).to_string(), "Query error: g");
        assert_eq!(ExecutionError::TypeMismatch("h".into()).to_string(), "Type mismatch: h");
        assert_eq!(ExecutionError::Unsupported("k".into()).to_string(), "Unsupported SPARQL feature: k");
        assert_eq!(HttpError::Server("i".into()).to_string(), "Server error: i");
        assert_eq!(HttpError::InvalidRequest("j".into()).to_string(), "Invalid request: j");
    }
}
