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

    /// HTTP transport error
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    /// JSON serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

pub type SamyamaResult<T> = Result<T, SamyamaError>;
