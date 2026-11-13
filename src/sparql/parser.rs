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
    /// Parse a SPARQL 1.1 query into its algebra.
    pub fn parse(query: &str) -> Result<spargebra::Query, ParseError> {
        spargebra::Query::parse(query, None).map_err(|e| ParseError::Syntax(e.to_string()))
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
    fn a_query_parses_and_bad_text_is_a_syntax_error() {
        assert!(matches!(
            SparqlParser::parse("SELECT * WHERE { ?s ?p ?o }"),
            Ok(spargebra::Query::Select { .. })
        ));
        assert!(matches!(
            SparqlParser::parse("SELEC ?s"),
            Err(ParseError::Syntax(_))
        ));
    }
}
