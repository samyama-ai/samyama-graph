//! SPARQL parser using spargebra library

use thiserror::Error;

/// Parse errors
#[derive(Error, Debug)]
pub enum ParseError {
    /// Syntax error
    #[error("Syntax error: {0}")]
    Syntax(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    Unsupported(String),
}

/// SPARQL parser
pub struct SparqlParser;

impl SparqlParser {
    /// Parse a SPARQL query string
    ///
    /// TODO: Implement using spargebra::Query::parse
    pub fn parse(_query: &str) -> Result<(), ParseError> {
        // TODO: Implement parsing
        Ok(())
    }

    /// Parse a SPARQL UPDATE string
    ///
    /// TODO: Implement using spargebra::Update::parse
    pub fn parse_update(_update: &str) -> Result<(), ParseError> {
        // TODO: Implement update parsing
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_stub() {
        let result = SparqlParser::parse("SELECT * WHERE { ?s ?p ?o }");
        assert!(result.is_ok());
    }
}
