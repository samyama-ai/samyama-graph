//! Error types for the Samyama SDK

use thiserror::Error;

/// Errors that can occur when using the Samyama SDK
#[derive(Error, Debug)]
pub enum SamyamaError {
    /// Query parsing or execution error
    #[error("Query error: {0}")]
    QueryError(String),

    /// Connection error (remote mode)
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// RESP protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// The request was sent and no answer arrived in time (#2175).
    ///
    /// Kept apart from [`HttpError`](Self::HttpError) because it means
    /// something a refused connection does not: the server may have received
    /// the request and applied it. After a timed-out write the outcome is
    /// unknown, so repeat it only if the statement is safe to run twice
    /// (`MERGE` on a key, a write under a unique constraint, `SET` to a fixed
    /// value). A refused connection never reached the server and is safe to
    /// retry.
    #[error("Timeout: {0}")]
    Timeout(String),

    /// HTTP transport error
    #[error("HTTP error: {0}")]
    HttpError(#[source] reqwest::Error),

    /// JSON serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Vector search error
    #[error("Vector error: {0}")]
    VectorError(String),

    /// Graph algorithm error
    #[error("Algorithm error: {0}")]
    AlgorithmError(String),

    /// Persistence error
    #[error("Persistence error: {0}")]
    PersistenceError(String),
}

impl From<reqwest::Error> for SamyamaError {
    /// A timeout becomes [`SamyamaError::Timeout`]; everything else stays an
    /// [`SamyamaError::HttpError`]. Rust and Python used to report both as
    /// `HTTP error: error sending request`, so a caller could not tell a write
    /// that may have happened from one that certainly did not (#2175).
    fn from(e: reqwest::Error) -> Self {
        // A connect timeout is a connection failure: nothing was sent.
        if e.is_timeout() && !e.is_connect() {
            SamyamaError::Timeout(format!(
                "no response from the server in time ({e}); \
                 if this was a write, it may or may not have been applied"
            ))
        } else {
            SamyamaError::HttpError(e)
        }
    }
}

pub type SamyamaResult<T> = Result<T, SamyamaError>;
