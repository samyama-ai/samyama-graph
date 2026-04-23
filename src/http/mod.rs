//! HTTP module for Web UI and REST API

pub mod server;
pub mod handler;
pub mod optimize;
pub mod uc_problems;

pub use server::HttpServer;

/// Build an axum Router wired with the optimize endpoints, for integration
/// tests that don't need the full visualizer / graph routes.
pub fn build_router_for_tests(_store: std::sync::Arc<tokio::sync::RwLock<crate::graph::GraphStore>>) -> axum::Router {
    use std::sync::Arc;
    let state = Arc::new(optimize::OptimizeState::default());
    optimize::router().with_state(state)
}
