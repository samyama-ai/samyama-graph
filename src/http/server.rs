//! HTTP server implementation for the Visualizer

use axum::{
    routing::{get, post},
    Router,
    response::{Html, IntoResponse},
};
use crate::graph::GraphStore;
use crate::query::QueryEngine;
use std::sync::Arc;
use tokio::sync::RwLock;
use axum::extract::DefaultBodyLimit;
use tower_http::cors::CorsLayer;
use tracing::info;
use super::handler::{
    query_handler, status_handler, schema_handler, sample_handler,
    import_csv_handler, import_json_handler,
    export_snapshot_handler, restore_snapshot_handler,
};
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

/// Shared application state for HTTP routes
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<RwLock<GraphStore>>,
    pub engine: Arc<QueryEngine>,
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
        let state = AppState {
            store: Arc::clone(&self.store),
            engine: Arc::new(QueryEngine::new()),
        };

        let app = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .route("/api/schema", get(schema_handler))
            .route("/api/sample", post(sample_handler))
            .route("/api/import/csv", post(import_csv_handler))
            .route("/api/import/json", post(import_json_handler))
            .route("/api/snapshot/export", post(export_snapshot_handler))
            .route("/api/snapshot/import", post(restore_snapshot_handler)
                .layer(DefaultBodyLimit::max(2 * 1024 * 1024 * 1024))) // 2 GB
            .layer(CorsLayer::permissive())
            .with_state(state);

        let addr = format!("0.0.0.0:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        info!("Visualizer available at http://localhost:{}", self.port);

        axum::serve(listener, app).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::QueryEngine;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    #[test]
    fn test_http_server_new() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let server = HttpServer::new(Arc::clone(&store), 9090);

        assert_eq!(server.port, 9090);
        // The store Arc should have 2 strong refs (original + server)
        assert_eq!(Arc::strong_count(&store), 2);
    }

    #[test]
    fn test_http_server_new_different_ports() {
        let store = Arc::new(RwLock::new(GraphStore::new()));
        let s1 = HttpServer::new(Arc::clone(&store), 8080);
        let s2 = HttpServer::new(Arc::clone(&store), 8081);

        assert_eq!(s1.port, 8080);
        assert_eq!(s2.port, 8081);
        // 3 strong refs: original + s1 + s2
        assert_eq!(Arc::strong_count(&store), 3);
    }

    #[test]
    fn test_app_state_clone() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };

        let cloned = state.clone();

        // Both should point to the same underlying store and engine
        assert!(Arc::ptr_eq(&state.store, &cloned.store));
        assert!(Arc::ptr_eq(&state.engine, &cloned.engine));
    }

    #[test]
    fn test_app_state_store_is_shared() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };

        let cloned = state.clone();

        // After clone, Arc strong_count should be 2
        assert_eq!(Arc::strong_count(&state.store), 2);
        assert_eq!(Arc::strong_count(&state.engine), 2);

        drop(cloned);

        // After dropping clone, strong_count back to 1
        assert_eq!(Arc::strong_count(&state.store), 1);
        assert_eq!(Arc::strong_count(&state.engine), 1);
    }

    #[test]
    fn test_app_state_multiple_clones() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };

        let c1 = state.clone();
        let c2 = state.clone();
        let c3 = c1.clone();

        assert_eq!(Arc::strong_count(&state.store), 4);
        assert_eq!(Arc::strong_count(&state.engine), 4);

        assert!(Arc::ptr_eq(&state.store, &c2.store));
        assert!(Arc::ptr_eq(&c1.store, &c3.store));
    }

    #[tokio::test]
    async fn test_app_state_store_read_write() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };

        // Write through the state
        {
            let mut store = state.store.write().await;
            let n = store.create_node("Test");
            store.get_node_mut(n).unwrap().set_property("key", "value");
        }

        // Read through a clone
        let cloned = state.clone();
        {
            let store = cloned.store.read().await;
            assert_eq!(store.node_count(), 1);
        }
    }

    #[test]
    fn test_static_handler_returns_html() {
        // Assets::get("index.html") should return Some for the embedded file
        let asset = Assets::get("index.html");
        assert!(asset.is_some(), "index.html should be embedded via RustEmbed");
        let content = asset.unwrap();
        let html = std::str::from_utf8(content.data.as_ref()).unwrap();
        assert!(html.contains("<html") || html.contains("<!DOCTYPE") || html.contains("<body"),
            "Embedded file should contain HTML content");
    }

    #[tokio::test]
    async fn test_router_construction() {
        // Verify that the Router can be built without panicking
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };

        let _app: Router = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .layer(CorsLayer::permissive())
            .with_state(state);
    }

    #[tokio::test]
    async fn test_static_handler_response() {
        let state = AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
        };

        let app = Router::new()
            .route("/", get(static_handler))
            .with_state(state);

        let req: axum::http::Request<Body> = axum::http::Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();
        let response = app.oneshot(req).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let html = std::str::from_utf8(&bytes).unwrap();
        assert!(html.contains("<html") || html.contains("<!DOCTYPE") || html.contains("<body"),
            "Static handler should return HTML content");
    }
}
