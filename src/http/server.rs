//! HTTP server implementation for the Visualizer

use axum::{
    extract::DefaultBodyLimit,
    response::{Html, IntoResponse},
    routing::{get, post},
    Router,
};
use crate::embed::EmbedPipeline;
use crate::graph::GraphStore;
use crate::persistence::TenantManager;
use crate::query::QueryEngine;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tracing::info;
use super::handler::{
    query_handler, status_handler, schema_handler, sample_handler,
    import_csv_handler, import_json_handler,
    export_snapshot_handler, restore_snapshot_handler,
};
use super::vector::{list_indexes_handler, create_index_handler, search_handler};

/// HA-09: Build the tenant CRUD sub-router backed by the shared `TenantManager`.
/// Exposed at the crate level so integration tests can mount it in isolation.
pub fn build_tenant_router(tenants: Arc<TenantManager>) -> axum::Router {
    let cache = Arc::new(RwLock::new(HashMap::<String, Arc<EmbedPipeline>>::new()));
    super::tenants::router(tenants, cache)
}
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
    /// Data directory for persisting snapshots (HA-08)
    pub data_path: Option<String>,
    /// Tenant manager for multi-tenancy support
    pub tenant_manager: Option<Arc<TenantManager>>,
    /// Global embed pipeline (fallback when tenant has no embed_config)
    pub embed_pipeline: Option<Arc<EmbedPipeline>>,
    /// Per-tenant EmbedPipeline cache; invalidated on PATCH /api/tenants/:id
    pub embed_cache: Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>>,
}

/// HTTP server managing the Visualizer API and static assets
pub struct HttpServer {
    store: Arc<RwLock<GraphStore>>,
    port: u16,
    data_path: Option<String>,
    tenants: Option<Arc<TenantManager>>,
    embed_pipeline: Option<Arc<EmbedPipeline>>,
}

impl HttpServer {
    /// Create a new HTTP server
    pub fn new(store: Arc<RwLock<GraphStore>>, port: u16) -> Self {
        Self { store, port, data_path: None, tenants: None, embed_pipeline: None }
    }

    /// Set the data directory for snapshot persistence (HA-08)
    pub fn with_data_path(mut self, path: Option<String>) -> Self {
        self.data_path = path;
        self
    }

    /// HA-09: share a `TenantManager` with the RESP command handler so
    /// tenants created via HTTP are immediately visible to `GRAPH.LIST`.
    pub fn with_tenant_manager(mut self, tenants: Arc<TenantManager>) -> Self {
        self.tenants = Some(tenants);
        self
    }

    /// Set a global embed pipeline used as fallback for all tenants that have
    /// no per-tenant embed_config configured.
    pub fn with_embed_pipeline(mut self, pipeline: Arc<EmbedPipeline>) -> Self {
        self.embed_pipeline = Some(pipeline);
        self
    }

    /// Start the HTTP server
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let embed_cache: Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let state = AppState {
            store: Arc::clone(&self.store),
            engine: Arc::new(QueryEngine::new()),
            data_path: self.data_path.clone(),
            tenant_manager: self.tenants.clone(),
            embed_pipeline: self.embed_pipeline.clone(),
            embed_cache: Arc::clone(&embed_cache),
        };

        let optimize_state = Arc::new(super::optimize::OptimizeState::default());

        let main_router = Router::new()
            .route("/", get(static_handler))
            .route("/api/query", post(query_handler))
            .route("/api/status", get(status_handler))
            .route("/api/schema", get(schema_handler))
            .route("/api/sample", post(sample_handler))
            .route("/api/import/csv", post(import_csv_handler))
            .route("/api/import/json", post(import_json_handler))
            .route("/api/vector/indexes", get(list_indexes_handler))
            .route("/api/vector/indexes", post(create_index_handler))
            .route("/api/vector-search", post(search_handler))
            .route("/api/snapshot/export", post(export_snapshot_handler))
            .route("/api/snapshot/import", post(restore_snapshot_handler)
                // 64 GB cap. PubMed-v2 (11 GB) and trifecta-pubmed (12 GB) need
                // headroom; 64 GB lets per-source snapshots up to ~50 GB through.
                // Body is buffered in memory by the multipart extractor — see #197
                // follow-up for streaming-to-disk to drop the RAM ceiling.
                .layer(DefaultBodyLimit::max(64 * 1024 * 1024 * 1024)))
            .with_state(state);

        let mut app = main_router
            .merge(super::optimize::router().with_state(optimize_state));

        if let Some(tm) = self.tenants.as_ref() {
            app = app.merge(super::tenants::router(Arc::clone(tm), Arc::clone(&embed_cache)));
        }

        let app = app.layer(CorsLayer::permissive());

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
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
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
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
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
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
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
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
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
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
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
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
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
