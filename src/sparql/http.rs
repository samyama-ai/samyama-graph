//! SPARQL HTTP protocol endpoint

use thiserror::Error;

/// HTTP errors
#[derive(Error, Debug)]
pub enum HttpError {
    /// Server error
    #[error("Server error: {0}")]
    Server(String),

    /// Invalid request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
}

/// SPARQL HTTP endpoint
///
/// TODO: Implement using axum web framework
/// - POST /sparql for queries
/// - Content negotiation
/// - Result format handling
pub struct SparqlHttpEndpoint;

impl SparqlHttpEndpoint {
    /// Create a new HTTP endpoint
    pub fn new() -> Self {
        Self
    }

    /// Start the HTTP server
    ///
    /// TODO: Implement using axum
    pub async fn start(&self, _port: u16) -> Result<(), HttpError> {
        Ok(())
    }
}

impl Default for SparqlHttpEndpoint {
    fn default() -> Self {
        Self::new()
    }
}
