//! Samyama SDK — Client library for the Samyama Graph Database
//!
//! Provides two client implementations:
//!
//! - **`EmbeddedClient`** — In-process, no network. Uses `GraphStore` and `QueryEngine`
//!   directly. Ideal for tests, examples, and embedded applications.
//!
//! - **`RemoteClient`** — Connects to a running Samyama server via HTTP.
//!   For production client applications.
//!
//! Both implement the `SamyamaClient` trait for a unified API.
//!
//! # Quick Start
//!
//! ```rust
//! use samyama_sdk::{EmbeddedClient, SamyamaClient};
//!
//! #[tokio::main]
//! async fn main() {
//!     let client = EmbeddedClient::new();
//!
//!     // Create data
//!     client.query("default", r#"CREATE (n:Person {name: "Alice"})"#)
//!         .await.unwrap();
//!
//!     // Query data
//!     let result = client.query_readonly("default", "MATCH (n:Person) RETURN n.name")
//!         .await.unwrap();
//!     println!("Found {} records", result.len());
//! }
//! ```

pub mod client;
pub mod embedded;
pub mod error;
pub mod models;
pub mod remote;

// Re-export main types
pub use client::SamyamaClient;
pub use embedded::EmbeddedClient;
pub use remote::RemoteClient;
pub use error::{SamyamaError, SamyamaResult};
pub use models::{QueryResult, SdkNode, SdkEdge, ServerStatus, StorageStats};

// Re-export graph types for convenience when using EmbeddedClient
pub use samyama::graph::{GraphStore, NodeId, EdgeId, Label, PropertyValue};
pub use samyama::query::QueryEngine;
