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

    /// Serialize results to string.
    ///
    /// JSON is the W3C SPARQL 1.1 Query Results JSON Format. The others are
    /// refused rather than returned empty: this used to answer every format
    /// with `Ok("")`, which a caller reads as "no results" (#2100).
    pub fn serialize(&self, format: ResultFormat) -> Result<String, String> {
        match format {
            ResultFormat::Json => Ok(self.to_json()?.to_string()),
            other => Err(format!("SPARQL results as {other:?} are not supported yet; use JSON")),
        }
    }

    /// The W3C SPARQL 1.1 Query Results JSON document
    /// (<https://www.w3.org/TR/sparql11-results-json/>).
    ///
    /// SELECT: `{"head":{"vars":[..]},"results":{"bindings":[..]}}`, a variable
    /// left unbound in a row is absent from that row's object, as the format
    /// requires. ASK: `{"head":{},"boolean":b}`. CONSTRUCT/DESCRIBE produce a
    /// graph, which this format cannot carry, and are refused.
    pub fn to_json(&self) -> Result<serde_json::Value, String> {
        use serde_json::{json, Map, Value};
        match self {
            SparqlResults::Bindings { variables, solutions } => {
                let bindings: Vec<Value> = solutions
                    .iter()
                    .map(|s| {
                        let mut row = Map::new();
                        for v in variables {
                            if let Some(t) = s.get(v) {
                                row.insert(v.clone(), term_json(t));
                            }
                        }
                        Value::Object(row)
                    })
                    .collect();
                Ok(json!({ "head": { "vars": variables }, "results": { "bindings": bindings } }))
            }
            SparqlResults::Boolean(b) => Ok(json!({ "head": {}, "boolean": b })),
            SparqlResults::Graph(_) => {
                Err("a CONSTRUCT or DESCRIBE result is a graph, not a JSON results document".into())
            }
        }
    }
}

const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

/// One RDF term in the JSON results format. A plain string carries no
/// datatype and a language-tagged one carries `xml:lang` instead of one, as
/// the format writes them; every other literal names its datatype, so a
/// client can tell `30` the integer from `"30"` the string.
fn term_json(t: &RdfTerm) -> serde_json::Value {
    use serde_json::json;
    match t {
        RdfTerm::NamedNode(n) => json!({ "type": "uri", "value": n.as_str() }),
        RdfTerm::BlankNode(b) => json!({ "type": "bnode", "value": b.as_str() }),
        RdfTerm::Literal(l) => {
            let mut o = json!({ "type": "literal", "value": l.value() });
            if let Some(lang) = l.language() {
                o["xml:lang"] = json!(lang);
            } else {
                let dt = l.datatype();
                if dt.as_str() != XSD_STRING && dt.as_str() != RDF_LANG_STRING {
                    o["datatype"] = json!(dt.as_str());
                }
            }
            o
        }
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
