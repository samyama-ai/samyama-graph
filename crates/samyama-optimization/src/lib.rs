pub mod algorithms;
pub mod common;
pub mod moo;

/// Re-export common types
pub use common::*;

/// Initialize the optimization engine
pub fn init() {
    tracing::info!("Samyama Optimization Engine Initialized");
}
