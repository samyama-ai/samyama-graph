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
            let (dimensions, metric) = match store.vector_index.get_index(&k.label, &k.property_key) {
                Some(idx) => {
                    let idx = idx.read().unwrap();
                    (Some(idx.dimensions()), Some(canonical_metric(&idx.metric())))
                }
                None => (None, None),
            };
            json!({
                "label": k.label,
                "property_key": k.property_key,
                "dimensions": dimensions,
                "metric": metric,
                // The embedding model that built the index; null when unknown (#275).
                "model_id": store.vector_index.model_id(&k.label, &k.property_key),
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
    /// The embedding model whose vectors this index holds (#275). Optional:
    /// without it the model is unknown and text queries are not checked.
    #[serde(default)]
    pub model_id: Option<String>,
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
        Ok(_) => {
            if let Some(model_id) = payload.model_id.as_deref() {
                store.vector_index.set_model_id(&payload.label, &payload.property_key, model_id);
            }
            Json(json!({
                "status": "ok",
                "label": payload.label,
                "property_key": payload.property_key,
                "dimensions": payload.dimensions,
                "metric": canonical,
                "model_id": store.vector_index.model_id(&payload.label, &payload.property_key),
            }))
            .into_response()
        }
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

    // Refuse a graph this build cannot serve, the same way `/api/query` and
    // `/api/query/export` do. Without this the argument was accepted, ignored,
    // and the default graph searched for any value — so a tenant that exists
    // read another tenant's data, with a 200 and no notification (#1476).
    if let Some(refusal) = crate::http::handler::reject_foreign_graph(&payload.graph) {
        return refusal;
    }

    let tenant_id = &payload.graph;

    // Which property holds the vectors? Ask the index, do not guess.
    //
    // This defaulted to the literal "embedding". A vector index on any other
    // property — `CREATE VECTOR INDEX vx FOR (n:V) ON (n.emb)` is perfectly
    // ordinary — made this endpoint search a property that does not exist and
    // return `200` with an empty result set, which reads as "nothing is
    // similar" rather than "wrong property" (#1481). The manager knew the
    // right answer the whole time; nobody asked it.
    //
    // An explicit `property_key` still wins, so a caller who names it keeps
    // today's behaviour. Only the unnamed case changes, and only from a guess
    // to either the index's own property or an error.
    let resolved_property: String = match payload.property_key.as_deref() {
        Some(p) => p.to_string(),
        None => {
            let label = payload.label.as_deref();
            let candidates: Vec<String> = {
                let store = state.store.read().await;
                store
                    .vector_index
                    .list_indices()
                    .into_iter()
                    .filter(|k| label.is_none_or(|l| k.label == l))
                    .map(|k| k.property_key)
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            match candidates.len() {
                // No index to consult. Keep the historical default rather than
                // refusing: `search_all` and the no-label path still work, and
                // an empty graph should not become an error.
                0 => "embedding".to_string(),
                1 => candidates.into_iter().next().unwrap(),
                _ => {
                    // Ambiguous, and a guess here is how the original defect
                    // read as an empty result. Name the choices instead.
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "label {:?} has vector indexes on more than one property ({}); \
                                 pass `property_key` to say which one to search",
                                label.unwrap_or("<any>"),
                                candidates.join(", ")
                            )
                        })),
                    )
                        .into_response();
                }
            }
        }
    };
    let property_key = resolved_property.as_str();

    // Resolve query vector from query_text or query_vector. A text query also
    // carries the model that embedded it, so it can be checked against the
    // model that built the index (#275); a raw vector carries none.
    let (query_vector, mode, query_model) = if let Some(text) = &payload.query_text {
        match resolve_embed_pipeline(&state, tenant_id).await {
            Some(pipeline) => match pipeline.process_text(text).await {
                Ok(chunks) if !chunks.is_empty() => {
                    (chunks[0].embedding.clone(), "text", Some(pipeline.model_id().to_string()))
                }
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
        (vec, "vector", None)
    } else {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Either query_text or query_vector must be provided" })),
        )
            .into_response();
    };

    let store = state.store.read().await;

    if let Some(query_model) = query_model.as_deref() {
        if let Some(refusal) = incompatible_index(
            &store.vector_index,
            payload.label.as_deref(),
            property_key,
            query_model,
            query_vector.len(),
        ) {
            return refusal;
        }
    }

    // When the caller pins a `label`, search just that index. When no `label`
    // is given, fan out across EVERY index (all labels + properties) and merge to
    // a global top-k — so the default matches against all nodes rather than a
    // single hardcoded label.
    let search_result = match payload.label.as_deref() {
        Some(label) => store.vector_search(label, property_key, &query_vector, k),
        None => store.vector_search_all(&query_vector, k),
    };

    match search_result {
        Ok(results) => {
            let search_results: Vec<_> = results
                .iter()
                .map(|(node_id, distance)| {
                    let node_info = store
                        .get_node(*node_id)
                        .map(|n| {
                            // Resolve the FULL property set (inline HashMap +
                            // ColumnStore). Iterating `n.properties` alone misses
                            // ColumnStore-backed scalars (name/title/ids), leaving
                            // an empty map for stub/bulk/v2-snapshot-loaded nodes.
                            let mut properties = serde_json::Map::new();
                            for (prop_key, v) in store.node_properties_full(*node_id) {
                                // Use the caller-supplied property_key, not the literal "embedding",
                                // so nodes indexed under a different key are correctly filtered.
                                if prop_key != property_key || payload.include_vectors {
                                    properties.insert(prop_key, v.to_json());
                                }
                            }
                            json!({
                                "id": node_id.as_u64(),
                                // Sorted, as in the query handler: the hash
                                // order of a label set is not a contract (#1353).
                                "labels": crate::http::handler::sorted_label_strs(&n.labels),
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
        Err(e) => {
            let scope = match payload.label.as_deref() {
                Some(l) => format!("label '{}' property '{}'", l, property_key),
                None => "any indexed label".to_string(),
            };
            (
                axum::http::StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("Vector search failed: {}. Index may not be built for {}.", e, scope)
                })),
            )
                .into_response()
        }
    }
}

/// Refuse a text query whose embedding model differs from the model that
/// built an index it would search (#275).
///
/// Vectors from different models live in different spaces even at the same
/// length, so searching one with the other returns confident, wrong
/// neighbours and a 200. The indexes checked are exactly the ones the search
/// would read: the pinned `(label, property_key)` index, or -- with no label --
/// every index whose dimension matches the query, which is what `search_all`
/// fans out over. An index with no recorded model is not checked.
fn incompatible_index(
    manager: &crate::vector::VectorIndexManager,
    label: Option<&str>,
    property_key: &str,
    query_model: &str,
    query_dimensions: usize,
) -> Option<axum::response::Response> {
    let mut keys: Vec<(String, String)> = match label {
        Some(l) => vec![(l.to_string(), property_key.to_string())],
        None => manager
            .list_indices()
            .into_iter()
            .map(|k| (k.label, k.property_key))
            .collect(),
    };
    keys.sort();
    for (l, p) in keys {
        let Some(index) = manager.get_index(&l, &p) else { continue };
        let index_dimensions = index.read().unwrap().dimensions();
        if label.is_none() && index_dimensions != query_dimensions {
            continue;
        }
        let Some(index_model) = manager.model_id(&l, &p) else { continue };
        if index_model == query_model.trim() {
            continue;
        }
        return Some(
            (
                axum::http::StatusCode::CONFLICT,
                Json(json!({
                    "error": format!(
                        "incompatible index: {}.{} was built with embedding model '{}' ({} dims), \
                         but query_text was embedded with '{}' ({} dims); vectors from different \
                         models are not comparable. Re-embed and reindex with one model, or pass \
                         query_vector from the index's model.",
                        l, p, index_model, index_dimensions, query_model, query_dimensions
                    ),
                    "code": "incompatible_index",
                    "label": l,
                    "property_key": p,
                    "index_model_id": index_model,
                    "index_dimensions": index_dimensions,
                    "query_model_id": query_model,
                    "query_dimensions": query_dimensions,
                })),
            )
                .into_response(),
        );
    }
    None
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
            persistence: None,
            snapshot_key: None,
            transactions: Default::default(),
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

    // ---- #275: an index records the model that built it; a text query
    // embedded by a different model is refused instead of searched. ----

    fn mock_pipeline(model: &str) -> Arc<EmbedPipeline> {
        let config = crate::persistence::tenant::AutoEmbedConfig {
            provider: crate::persistence::tenant::LLMProvider::Mock,
            embedding_model: model.to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 1000,
            chunk_overlap: 0,
            // The mock provider always produces 64-dimensional vectors.
            vector_dimension: 64,
            embedding_policies: HashMap::new(),
            embedding_property: "embedding".to_string(),
        };
        Arc::new(EmbedPipeline::new(config).unwrap())
    }

    fn state_with_model(model: Option<&str>) -> AppState {
        let mut state = test_state();
        state.embed_pipeline = model.map(mock_pipeline);
        state
    }

    /// A 64-dim Doc index on `embedding` holding two vectors, created over HTTP
    /// with `model_id` (or without one when `None`).
    async fn seed_doc_index(state: &AppState, model_id: Option<&str>) {
        let mut body = json!({ "label": "Doc", "property_key": "embedding", "dimensions": 64 });
        if let Some(m) = model_id {
            body["model_id"] = json!(m);
        }
        let (status, _) = post_json(test_app(state.clone()), "/api/vector/indexes", body).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let mut store = state.store.write().await;
        for i in 0..2u8 {
            let id = store.create_node("Doc");
            let mut v = vec![0.1_f32; 64];
            v[0] = i as f32 / 10.0;
            store
                .vector_index
                .add_vector("Doc", "embedding", id, &v)
                .unwrap();
        }
    }

    async fn text_search(state: &AppState, label: Option<&str>) -> (axum::http::StatusCode, serde_json::Value) {
        let mut body = json!({ "query_text": "graph databases", "k": 2 });
        if let Some(l) = label {
            body["label"] = json!(l);
        }
        post_json(test_app(state.clone()), "/api/vector-search", body).await
    }

    #[tokio::test]
    async fn test_text_search_with_other_model_is_refused() {
        let state = state_with_model(Some("model-b"));
        seed_doc_index(&state, Some("model-a")).await;

        for label in [Some("Doc"), None] {
            let (status, body) = text_search(&state, label).await;
            assert_eq!(status, axum::http::StatusCode::CONFLICT, "label {:?}: {}", label, body);
            assert_eq!(body["code"], "incompatible_index", "{}", body);
            assert_eq!(body["query_model_id"], "model-b", "{}", body);
            assert_eq!(body["index_model_id"], "model-a", "{}", body);
            assert_eq!(body["label"], "Doc", "{}", body);
            let err = body["error"].as_str().unwrap();
            assert!(err.contains("model-a") && err.contains("model-b"), "{}", err);
        }
    }

    #[tokio::test]
    async fn test_text_search_with_same_model_searches() {
        let state = state_with_model(Some("model-a"));
        seed_doc_index(&state, Some("model-a")).await;
        for label in [Some("Doc"), None] {
            let (status, body) = text_search(&state, label).await;
            assert_eq!(status, axum::http::StatusCode::OK, "{}", body);
            assert_eq!(body["results"].as_array().unwrap().len(), 2, "{}", body);
        }
    }

    #[tokio::test]
    async fn test_raw_vector_search_ignores_model_binding() {
        let state = state_with_model(Some("model-b"));
        seed_doc_index(&state, Some("model-a")).await;
        let (status, body) = post_json(
            test_app(state.clone()),
            "/api/vector-search",
            json!({ "query_vector": vec![0.1_f32; 64], "label": "Doc", "k": 2 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{}", body);
        assert_eq!(body["results"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_unbound_index_is_searchable_by_any_model() {
        let state = state_with_model(Some("model-b"));
        seed_doc_index(&state, None).await;
        let (status, body) = text_search(&state, Some("Doc")).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{}", body);
        assert_eq!(body["results"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_list_indexes_reports_model_id() {
        let state = state_with_model(None);
        seed_doc_index(&state, Some("model-a")).await;
        let req = Request::builder().uri("/api/vector/indexes").body(Body::empty()).unwrap();
        let resp = test_app(state).oneshot(req).await.unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["indexes"][0]["model_id"], "model-a", "{}", body);
        assert_eq!(body["indexes"][0]["dimensions"], 64, "{}", body);
    }

    /// The binding survives `metadata.json`, and metadata written before the
    /// field existed still loads and searches (model unknown, no check).
    #[tokio::test]
    async fn test_model_id_persists_and_legacy_metadata_loads() {
        let dir = tempfile::tempdir().unwrap();
        let src = state_with_model(None);
        seed_doc_index(&src, Some("model-a")).await;
        src.store.read().await.vector_index.dump_all(dir.path()).unwrap();

        let meta_path = dir.path().join("metadata.json");
        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        assert_eq!(meta[0]["model_id"], "model-a", "{}", meta);

        // Round trip: the reloaded index still refuses model B.
        let reloaded = state_with_model(Some("model-b"));
        reloaded.store.read().await.vector_index.load_all(dir.path()).unwrap();
        let (status, body) = text_search(&reloaded, Some("Doc")).await;
        assert_eq!(status, axum::http::StatusCode::CONFLICT, "{}", body);

        // Legacy: strip `model_id`, as a pre-#275 server would have written it.
        let mut legacy = meta.clone();
        legacy[0].as_object_mut().unwrap().remove("model_id");
        std::fs::write(&meta_path, serde_json::to_vec(&legacy).unwrap()).unwrap();
        let old = state_with_model(Some("model-b"));
        old.store.read().await.vector_index.load_all(dir.path()).unwrap();
        // The graph the index was dumped beside: search returns only nodes
        // that exist and carry the label (#1605).
        {
            let mut store = old.store.write().await;
            for _ in 0..2 {
                store.create_node("Doc");
            }
        }
        assert_eq!(old.store.read().await.vector_index.model_id("Doc", "embedding"), None);
        let (status, body) = text_search(&old, Some("Doc")).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{}", body);
        assert_eq!(body["results"].as_array().unwrap().len(), 2, "{}", body);
    }

    async fn list(state: AppState) -> serde_json::Value {
        let req = Request::builder()
            .uri("/api/vector/indexes")
            .body(Body::empty())
            .unwrap();
        let resp = test_app(state).oneshot(req).await.unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn listed_indexes_report_their_canonical_metric() {
        let state = test_state();
        for (label, metric) in [("A", "L2"), ("B", "dot"), ("C", "Cosine")] {
            let (status, _) = post_json(
                test_app(state.clone()),
                "/api/vector/indexes",
                json!({ "label": label, "property_key": "v", "dimensions": 4, "metric": metric }),
            )
            .await;
            assert_eq!(status, axum::http::StatusCode::OK);
        }
        let body = list(state).await;
        assert_eq!(body["count"], 3);
        let mut metrics: Vec<(String, String)> = body["indexes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| {
                (
                    i["label"].as_str().unwrap().into(),
                    i["metric"].as_str().unwrap().into(),
                )
            })
            .collect();
        metrics.sort();
        assert_eq!(
            metrics,
            vec![
                ("A".into(), "l2".into()),
                ("B".into(), "inner_product".into()),
                ("C".into(), "cosine".into())
            ]
        );
        assert!(body["indexes"][0]["model_id"].is_null());
    }

    #[tokio::test]
    async fn a_search_of_a_graph_this_build_does_not_serve_is_refused() {
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector-search",
            json!({ "query_vector": [0.1], "graph": "tenant-b" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(body["graph"], "tenant-b");
    }

    #[tokio::test]
    async fn a_label_with_indexes_on_two_properties_needs_a_property_key() {
        let state = test_state();
        for p in ["emb_a", "emb_b"] {
            state
                .store
                .read()
                .await
                .create_vector_index("Doc", p, 2, DistanceMetric::L2)
                .unwrap();
        }
        let (status, body) = post_json(
            test_app(state),
            "/api/vector-search",
            json!({ "query_vector": [0.1, 0.2], "label": "Doc" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        let e = body["error"].as_str().unwrap();
        assert!(e.contains("emb_a, emb_b") && e.contains("\"Doc\""), "{e}");
    }

    #[tokio::test]
    async fn the_only_indexed_property_is_searched_and_hidden_unless_asked_for() {
        let state = test_state();
        {
            let mut store = state.store.write().await;
            store
                .create_vector_index("Doc", "emb", 2, DistanceMetric::L2)
                .unwrap();
            let id = store.create_node("Doc");
            let _ = store.set_node_property(
                "default",
                id,
                "title".to_string(),
                crate::graph::PropertyValue::String("t".into()),
            );
            store
                .vector_index
                .add_vector("Doc", "emb", id, &vec![1.0_f32, 0.0])
                .unwrap();
            let _ = store.set_node_property(
                "default",
                id,
                "emb".to_string(),
                crate::graph::PropertyValue::Vector(vec![1.0, 0.0]),
            );
        }
        let (status, body) = post_json(
            test_app(state.clone()),
            "/api/vector-search",
            json!({ "query_vector": [1.0, 0.0], "label": "Doc", "k": 1 }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        assert_eq!(body["mode"], "vector");
        assert!(body["query_text"].is_null());
        let node = &body["results"][0]["node"];
        assert_eq!(node["labels"], json!(["Doc"]));
        assert_eq!(node["properties"]["title"], "t");
        assert!(
            node["properties"].get("emb").is_none(),
            "the vector is hidden: {node}"
        );
        assert_eq!(body["results"][0]["score"], 1.0);

        let (_, body) = post_json(
            test_app(state),
            "/api/vector-search",
            json!({ "query_vector": [1.0, 0.0], "label": "Doc", "k": 1, "include_vectors": true }),
        )
        .await;
        assert!(
            body["results"][0]["node"]["properties"]["emb"].is_array(),
            "{body}"
        );
    }

    #[tokio::test]
    async fn a_label_with_no_index_is_a_400_naming_the_index() {
        // Not an empty `results`: an index that does not exist used to answer
        // exactly like one in which nothing is similar (#1660).
        let (status, body) = post_json(
            test_app(test_state()),
            "/api/vector-search",
            json!({ "query_vector": [0.1, 0.2], "label": "Nothing", "property_key": "vec" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
        assert!(body.to_string().contains("no vector index on :Nothing(vec)"), "{body}");
    }

    #[tokio::test]
    async fn a_query_of_the_wrong_dimension_is_a_400_naming_the_index() {
        let state = test_state();
        state
            .store
            .read()
            .await
            .create_vector_index("Doc", "vec", 2, DistanceMetric::L2)
            .unwrap();
        let (status, body) = post_json(
            test_app(state),
            "/api/vector-search",
            json!({ "query_vector": [0.1, 0.2, 0.3], "label": "Doc", "property_key": "vec" }),
        )
        .await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST, "{body}");
        let e = body["error"].as_str().unwrap();
        assert!(e.starts_with("Vector search failed"), "{e}");
        assert!(e.contains("label 'Doc' property 'vec'"), "{e}");
    }

    fn tenant_config(
        provider: crate::persistence::tenant::LLMProvider,
        model: &str,
    ) -> crate::persistence::tenant::AutoEmbedConfig {
        crate::persistence::tenant::AutoEmbedConfig {
            provider,
            embedding_model: model.to_string(),
            api_key: None,
            api_base_url: None,
            chunk_size: 1000,
            chunk_overlap: 0,
            vector_dimension: 64,
            embedding_policies: HashMap::new(),
            embedding_property: "embedding".to_string(),
        }
    }

    #[tokio::test]
    async fn a_tenant_embed_config_builds_and_caches_a_pipeline() {
        let tm = Arc::new(crate::persistence::TenantManager::new());
        tm.update_embed_config(
            "default",
            Some(tenant_config(
                crate::persistence::tenant::LLMProvider::Mock,
                "tenant-model",
            )),
        )
        .unwrap();
        let mut state = test_state();
        state.tenant_manager = Some(tm);
        // A global pipeline exists too; the tenant's own config wins.
        state.embed_pipeline = Some(mock_pipeline("global-model"));
        seed_doc_index(&state, Some("tenant-model")).await;

        for _ in 0..2 {
            let (status, body) = text_search(&state, Some("Doc")).await;
            assert_eq!(status, axum::http::StatusCode::OK, "{body}");
            assert_eq!(body["mode"], "text");
            assert_eq!(body["query_text"], "graph databases");
        }
        let cache = state.embed_cache.read().await;
        assert_eq!(cache.get("default").unwrap().model_id(), "tenant-model");
    }

    #[tokio::test]
    async fn a_tenant_config_that_cannot_build_falls_back_to_the_global_pipeline() {
        // Azure needs a base URL, so this config cannot become a pipeline.
        let tm = Arc::new(crate::persistence::TenantManager::new());
        tm.update_embed_config(
            "default",
            Some(tenant_config(
                crate::persistence::tenant::LLMProvider::AzureOpenAI,
                "azure-model",
            )),
        )
        .unwrap();
        let mut state = state_with_model(Some("model-a"));
        state.tenant_manager = Some(tm);
        seed_doc_index(&state, Some("model-a")).await;
        let (status, body) = text_search(&state, Some("Doc")).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
        assert!(
            state.embed_cache.read().await.is_empty(),
            "nothing was built to cache"
        );
    }

    #[tokio::test]
    async fn an_embedding_failure_is_a_400_naming_it() {
        let tm = Arc::new(crate::persistence::TenantManager::new());
        tm.update_embed_config(
            "default",
            Some(tenant_config(
                crate::persistence::tenant::LLMProvider::Anthropic,
                "some-model",
            )),
        )
        .unwrap();
        let mut state = test_state();
        state.tenant_manager = Some(tm);
        let (status, body) = text_search(&state, Some("Doc")).await;
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .starts_with("Embedding generation failed"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn an_index_of_another_dimension_is_not_checked_for_its_model() {
        // No label: the search fans out over indexes of the query's dimension
        // only, so a 32-dim index built by another model is not in the way.
        let state = state_with_model(Some("model-b"));
        {
            let store = state.store.read().await;
            store
                .create_vector_index("Small", "embedding", 32, DistanceMetric::Cosine)
                .unwrap();
            store
                .vector_index
                .set_model_id("Small", "embedding", "model-a");
        }
        let (status, body) = text_search(&state, None).await;
        assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    }
}
