//! HTTP handlers for vector index management and similarity search

use axum::{extract::State, response::IntoResponse, Json};
use crate::embed::EmbedPipeline;
use crate::http::server::AppState;
use crate::vector::DistanceMetric;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

/// GET /api/vector/indexes — list all registered vector indexes
pub async fn list_indexes_handler(State(state): State<AppState>) -> impl IntoResponse {
    let store = state.store.read().await;
    let keys = store.vector_index.list_indices();
    let indexes: Vec<_> = keys
        .iter()
        .map(|k| {
            json!({
                "label": k.label,
                "property_key": k.property_key,
            })
        })
        .collect();
    Json(json!({ "indexes": indexes, "count": indexes.len() }))
}

/// Request body for POST /api/vector/indexes
#[derive(Deserialize)]
pub struct CreateIndexRequest {
    pub label: String,
    pub property_key: String,
    pub dimensions: usize,
    /// "cosine" (default), "l2", or "inner_product"
    #[serde(default = "default_metric")]
    pub metric: String,
}

fn default_metric() -> String {
    "cosine".to_string()
}

fn parse_metric(s: &str) -> Option<DistanceMetric> {
    match s.to_lowercase().as_str() {
        "cosine" => Some(DistanceMetric::Cosine),
        "l2" => Some(DistanceMetric::L2),
        "inner_product" | "dot" => Some(DistanceMetric::InnerProduct),
        _ => None,
    }
}

fn canonical_metric(m: &DistanceMetric) -> &'static str {
    match m {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::L2 => "l2",
        DistanceMetric::InnerProduct => "inner_product",
    }
}

/// POST /api/vector/indexes — create a new vector index
pub async fn create_index_handler(
    State(state): State<AppState>,
    Json(payload): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    if payload.label.is_empty() || payload.property_key.is_empty() {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "label and property_key are required" })),
        )
            .into_response();
    }
    if payload.dimensions == 0 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "dimensions must be > 0" })),
        )
            .into_response();
    }

    let metric = match parse_metric(&payload.metric) {
        Some(m) => m,
        None => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!(
                    "unknown metric '{}'; expected cosine, l2, or inner_product",
                    payload.metric
                ) })),
            )
                .into_response();
        }
    };

    let canonical = canonical_metric(&metric);
    // write lock: create_vector_index mutates the index registry
    let store = state.store.write().await;
    match store.create_vector_index(&payload.label, &payload.property_key, payload.dimensions, metric) {
        Ok(_) => Json(json!({
            "status": "ok",
            "label": payload.label,
            "property_key": payload.property_key,
            "dimensions": payload.dimensions,
            "metric": canonical,
        }))
        .into_response(),
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

fn default_graph() -> String {
    "default".to_string()
}

/// Request body for POST /api/vector-search
#[derive(Deserialize)]
pub struct VectorSearchRequest {
    /// Natural language query to convert to a vector via the embed pipeline
    pub query_text: Option<String>,
    /// Raw query vector (alternative to query_text)
    pub query_vector: Option<Vec<f32>>,
    /// Label of nodes to search within (defaults to "Paper")
    pub label: Option<String>,
    /// Property key that holds the vector embedding (defaults to "embedding")
    pub property_key: Option<String>,
    /// Number of nearest neighbors to return (default: 10)
    pub k: Option<usize>,
    /// Tenant/graph to search. Defaults to "default".
    #[serde(default = "default_graph")]
    pub graph: String,
    /// If true, include the raw embedding vector in each result node's properties.
    #[serde(default)]
    pub include_vectors: bool,
}

/// Resolve an `EmbedPipeline` for the given tenant, consulting the AppState cache.
/// Build order: per-tenant cache → tenant embed_config → global fallback pipeline.
async fn resolve_embed_pipeline(state: &AppState, tenant_id: &str) -> Option<Arc<EmbedPipeline>> {
    // 1. Warm-cache hit
    {
        let cache = state.embed_cache.read().await;
        if let Some(p) = cache.get(tenant_id) {
            return Some(Arc::clone(p));
        }
    }

    // 2. Build from tenant embed_config and populate the cache
    if let Some(ref tm) = state.tenant_manager {
        if let Ok(tenant) = tm.get_tenant(tenant_id) {
            if let Some(embed_config) = tenant.embed_config {
                if let Ok(pipeline) = EmbedPipeline::new(embed_config) {
                    let p = Arc::new(pipeline);
                    state.embed_cache.write().await.insert(tenant_id.to_string(), Arc::clone(&p));
                    return Some(p);
                }
            }
        }
    }

    // 3. Global fallback
    state.embed_pipeline.clone()
}

/// POST /api/vector-search — k-nearest-neighbor vector search.
/// Accepts query_text (converted to embedding via EmbedPipeline) or a raw query_vector.
pub async fn search_handler(
    State(state): State<AppState>,
    Json(payload): Json<VectorSearchRequest>,
) -> impl IntoResponse {
    let start = Instant::now();

    let k = payload.k.unwrap_or(10);
    if k == 0 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "k must be > 0" })),
        )
            .into_response();
    }

    let tenant_id = &payload.graph;
    let property_key = payload.property_key.as_deref().unwrap_or("embedding");

    // Resolve query vector from query_text or query_vector
    let (query_vector, mode) = if let Some(text) = &payload.query_text {
        match resolve_embed_pipeline(&state, tenant_id).await {
            Some(pipeline) => match pipeline.process_text(text).await {
                Ok(chunks) if !chunks.is_empty() => (chunks[0].embedding.clone(), "text"),
                Ok(_) => {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(json!({ "error": "Failed to generate embedding: empty result" })),
                    )
                        .into_response()
                }
                Err(e) => {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(json!({ "error": format!("Embedding generation failed: {}", e) })),
                    )
                        .into_response()
                }
            },
            None => {
                return (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "error": "Embedding pipeline not configured. Provide query_vector directly or configure embed_config on the tenant." })),
                )
                    .into_response()
            }
        }
    } else if let Some(vec) = payload.query_vector {
        if vec.is_empty() {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({ "error": "query_vector must not be empty" })),
            )
                .into_response();
        }
        (vec, "vector")
    } else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Either query_text or query_vector must be provided" })),
        )
            .into_response();
    };

    let store = state.store.read().await;
    let label = payload.label.as_deref().unwrap_or("Paper");

    match store.vector_search(label, property_key, &query_vector, k) {
        Ok(results) => {
            let search_results: Vec<_> = results
                .iter()
                .map(|(node_id, distance)| {
                    let node_info = store
                        .get_node(*node_id)
                        .map(|n| {
                            let mut properties = serde_json::Map::new();
                            for (prop_key, v) in &n.properties {
                                // Use the caller-supplied property_key, not the literal "embedding",
                                // so nodes indexed under a different key are correctly filtered.
                                if prop_key != property_key || payload.include_vectors {
                                    properties.insert(prop_key.clone(), v.to_json());
                                }
                            }
                            json!({
                                "id": node_id.as_u64(),
                                "labels": n.labels.iter().map(|l| l.as_str()).collect::<Vec<_>>(),
                                "properties": properties,
                            })
                        })
                        .unwrap_or_else(|| json!({ "id": node_id.as_u64() }));

                    // score = 1/(1+distance) is a monotonic similarity proxy.
                    // Accurate for L2; an approximation for cosine and inner-product.
                    let score = 1.0_f32 / (1.0_f32 + distance);
                    json!({ "node": node_info, "distance": distance, "score": score })
                })
                .collect();

            let elapsed = start.elapsed().as_secs_f64() * 1000.0;

            Json(json!({
                "results": search_results,
                "mode": mode,
                "query_text": if mode == "text" { payload.query_text.as_deref() } else { None },
                "k": k,
                "execution_time_ms": elapsed,
            }))
            .into_response()
        }
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!(
                    "Vector search failed: {}. Index may not be built for label '{}' property '{}'.",
                    e, label, property_key
                )
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request, routing::{get, post}, Router};
    use crate::graph::GraphStore;
    use crate::http::server::AppState;
    use crate::query::QueryEngine;
    use http_body_util::BodyExt;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    fn test_state() -> AppState {
        AppState {
            store: Arc::new(RwLock::new(GraphStore::new())),
            engine: Arc::new(QueryEngine::new()),
            data_path: None,
            tenant_manager: None,
            embed_pipeline: None,
            embed_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn test_app(state: AppState) -> Router {
        Router::new()
            .route("/api/vector/indexes", get(list_indexes_handler).post(create_index_handler))
            .route("/api/vector-search", post(search_handler))
            .with_state(state)
    }

    async fn post_json(
        app: Router,
        uri: &str,
        body: serde_json::Value,
    ) -> (axum::http::StatusCode, serde_json::Value) {
        let req = Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn test_search_missing_both_query_fields() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector-search",
            json!({ "label": "Node", "property_key": "embedding", "k": 5 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("query_text or query_vector"));
    }

    #[tokio::test]
    async fn test_search_empty_query_vector() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector-search",
            json!({ "query_vector": [], "label": "Node", "property_key": "embedding" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("must not be empty"));
    }

    #[tokio::test]
    async fn test_search_query_text_no_pipeline() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector-search",
            json!({ "query_text": "find something", "label": "Node", "k": 3 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(body["error"].as_str().unwrap().contains("Embedding pipeline not configured"));
    }

    #[tokio::test]
    async fn test_search_k_zero() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector-search",
            json!({ "query_vector": [0.1_f32, 0.2_f32], "k": 0 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("k must be > 0"));
    }

    #[tokio::test]
    async fn test_create_index_unknown_metric() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector/indexes",
            json!({ "label": "Node", "property_key": "vec", "dimensions": 128, "metric": "manhattan" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("unknown metric"));
    }

    #[tokio::test]
    async fn test_create_index_zero_dimensions() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector/indexes",
            json!({ "label": "Node", "property_key": "vec", "dimensions": 0 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("dimensions must be > 0"));
    }

    #[tokio::test]
    async fn test_create_index_empty_label() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector/indexes",
            json!({ "label": "", "property_key": "vec", "dimensions": 64 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("label and property_key are required"));
    }

    #[tokio::test]
    async fn test_create_index_echoes_canonical_metric() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector/indexes",
            json!({ "label": "Doc", "property_key": "emb", "dimensions": 64, "metric": "inner_product" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK);
        assert_eq!(body["metric"].as_str().unwrap(), "inner_product");
    }

    #[tokio::test]
    async fn test_create_index_dot_alias_unknown() {
        // "dot" is a recognised alias for inner_product in parse_metric
        let (status, _) = post_json(
            test_app(test_state()),
            "/api/vector/indexes",
            json!({ "label": "Doc", "property_key": "emb", "dimensions": 64, "metric": "dot" }),
        )
        .await;
        // dot is accepted
        assert_eq!(status, axum::http::StatusCode::OK);
    }
}
