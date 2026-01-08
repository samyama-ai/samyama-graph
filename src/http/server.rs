//! HTTP server implementation for the Visualizer

use axum::{
    routing::{get, post},
    Router,
    response::{Html, IntoResponse},
};
use crate::graph::GraphStore;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::info;
use super::handler::{query_handler, status_handler};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "src/http/static/"]
struct Assets;

async fn static_handler() -> impl IntoResponse {
    match Assets::get("index.html") {
        Some(content) => {
            let html = std::str::from_utf8(content.data.as_ref()).unwrap_or("Error: Invalid UTF-8 in index.html");
            Html(html.to_string())
        },
        None => Html("<h1>Error: index.html not found</h1><p>Ensure src/http/static/index.html exists and was compiled.</p>".to_string()),
    }
}

/// HTTP server managing the Visualizer API and static assets
pub struct HttpServer {
    store: Arc<RwLock<GraphStore>>,
    port: u16,
}

impl HttpServer {
    /// Create a new HTTP server
    pub fn new(store: Arc<RwLock<GraphStore>>, port: u16) -> Self {
        Self { store, port }
    }

    /// Start the HTTP server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let app = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .layer(CorsLayer::permissive())
            .with_state(Arc::clone(&self.store));

        let addr = format!("0.0.0.0:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;
        
        info!("Visualizer available at http://localhost:{}", self.port);
        
        axum::serve(listener, app).await?;
        
        Ok(())
    }
}
