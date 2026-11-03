//! SPARQL query results

use crate::rdf::{Triple, RdfTerm};
use std::collections::HashMap;

/// SPARQL result format
#[derive(Debug, Clone, Copy)]
pub enum ResultFormat {
    /// JSON results
    Json,
    /// XML results
    Xml,
    /// CSV results
    Csv,
    /// TSV results
    Tsv,
}

/// Query solution (variable bindings)
#[derive(Debug, Clone)]
pub struct QuerySolution {
    /// Variable name → RDF term bindings
    pub bindings: HashMap<String, RdfTerm>,
}

impl QuerySolution {
    /// Create a new query solution
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
        }
    }

    /// Get a binding
    pub fn get(&self, variable: &str) -> Option<&RdfTerm> {
        self.bindings.get(variable)
    }

    /// Add a binding
    pub fn bind(&mut self, variable: String, term: RdfTerm) {
        self.bindings.insert(variable, term);
    }
}

impl Default for QuerySolution {
    fn default() -> Self {
        Self::new()
    }
}

/// SPARQL query results
#[derive(Debug, Clone)]
pub enum SparqlResults {
    /// Bindings from SELECT query
    Bindings {
        /// Variables
        variables: Vec<String>,
        /// Solutions
        solutions: Vec<QuerySolution>,
    },

    /// Boolean result from ASK query
    Boolean(bool),

    /// Graph from CONSTRUCT/DESCRIBE query
    Graph(Vec<Triple>),
}

impl SparqlResults {
    /// Create empty bindings result
    pub fn empty() -> Self {
        SparqlResults::Bindings {
            variables: Vec::new(),
            solutions: Vec::new(),
        }
    }

    /// Serialize results to string
    ///
    /// TODO: Implement using sparesults library
    pub fn serialize(&self, _format: ResultFormat) -> Result<String, String> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_solution() {
        let mut solution = QuerySolution::new();
        assert!(solution.bindings.is_empty());

        // TODO: Add more tests once RdfTerm conversion is complete
    }

    #[test]
    fn test_empty_results() {
        let results = SparqlResults::empty();
        match results {
            SparqlResults::Bindings { variables, solutions } => {
                assert!(variables.is_empty());
                assert!(solutions.is_empty());
            }
            _ => panic!("Expected bindings"),
        }
    }

    // ========== Batch 6: Additional SPARQL Results Tests ==========

    #[test]
    fn test_query_solution_bind_and_get() {
        use crate::rdf::{NamedNode, Literal};

        let mut sol = QuerySolution::new();
        let term = RdfTerm::NamedNode(NamedNode::new("http://example.org/x").unwrap());
        sol.bind("x".to_string(), term);

        assert!(sol.get("x").is_some());
        assert!(sol.get("y").is_none());
    }

    #[test]
    fn test_query_solution_default() {
        let sol = QuerySolution::default();
        assert!(sol.bindings.is_empty());
    }

    #[test]
    fn test_sparql_results_boolean() {
        let result = SparqlResults::Boolean(true);
        match result {
            SparqlResults::Boolean(val) => assert!(val),
            _ => panic!("Expected Boolean"),
        }
    }

    #[test]
    fn test_sparql_results_serialize() {
        let result = SparqlResults::empty();
        let output = result.serialize(ResultFormat::Json);
        assert!(output.is_ok());
    }
}
