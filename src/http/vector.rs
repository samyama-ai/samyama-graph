//! HTTP handlers for vector index management and similarity search

use axum::{extract::State, response::IntoResponse, Json};
use crate::http::server::AppState;
use crate::vector::DistanceMetric;
use crate::embed::EmbedPipeline;
use serde::Deserialize;
use serde_json::json;
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

    let metric = match payload.metric.to_lowercase().as_str() {
        "l2" => DistanceMetric::L2,
        "inner_product" | "dot" => DistanceMetric::InnerProduct,
        _ => DistanceMetric::Cosine,
    };

    let store = state.store.read().await;
    match store.create_vector_index(&payload.label, &payload.property_key, payload.dimensions, metric) {
        Ok(_) => Json(json!({
            "status": "ok",
            "label": payload.label,
            "property_key": payload.property_key,
            "dimensions": payload.dimensions,
            "metric": payload.metric,
        }))
        .into_response(),
        Err(e) => (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
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
    #[serde(default)]
    pub graph: String,
    /// If true, include the raw embedding vector in each result node's properties.
    #[serde(default)]
    pub include_vectors: bool,
}

/// POST /api/vector-search — enterprise-style k-nearest-neighbor vector search.
/// Supports query_text → embedding conversion via per-tenant or global EmbedPipeline.
pub async fn search_handler(
    State(state): State<AppState>,
    Json(payload): Json<VectorSearchRequest>,
) -> impl IntoResponse {
    let start = Instant::now();

    let k = payload.k.unwrap_or(10);
    let tenant_id = if payload.graph.is_empty() {
        "default".to_string()
    } else {
        payload.graph.clone()
    };

    // Build per-tenant embed pipeline if the tenant has embed_config configured.
    let tenant_embed_pipeline: Option<EmbedPipeline> =
        if let Some(ref tm) = state.tenant_manager {
            if let Ok(tenant) = tm.get_tenant(&tenant_id) {
                if let Some(embed_config) = tenant.embed_config {
                    EmbedPipeline::new(embed_config).ok()
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

    // Resolve query vector from query_text or query_vector
    let query_vector = if let Some(text) = &payload.query_text {
        let pipeline_ref: Option<&EmbedPipeline> = tenant_embed_pipeline
            .as_ref()
            .or(state.embed_pipeline.as_ref().map(|p| p.as_ref()));

        match pipeline_ref {
            Some(pipeline) => match pipeline.process_text(text).await {
                Ok(chunks) if !chunks.is_empty() => chunks[0].embedding.clone(),
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
        vec
    } else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Either query_text or query_vector must be provided" })),
        )
            .into_response();
    };

    if k == 0 {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "k must be > 0" })),
        )
            .into_response();
    }

    let store = state.store.read().await;
    let label = payload.label.as_deref().unwrap_or("Paper");
    let property_key = payload.property_key.as_deref().unwrap_or("embedding");

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
                                if prop_key != "embedding" || payload.include_vectors {
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

                    let score = 1.0_f32 / (1.0_f32 + distance);
                    json!({ "node": node_info, "distance": distance, "score": score })
                })
                .collect();

            let elapsed = start.elapsed().as_secs_f64() * 1000.0;

            Json(json!({
                "results": search_results,
                "query_text": payload.query_text,
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

