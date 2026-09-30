pub mod algorithms;
pub mod benchmarks;
pub mod common;
pub mod moo;
pub mod stats;

/// Re-export common types
pub use common::*;

/// Initialize the optimization engine
pub fn init() {
    tracing::info!("Samyama Optimization Engine Initialized");
}

#[cfg(test)]
#[path = "lib_cov_tests.rs"]
mod cov_tests;
