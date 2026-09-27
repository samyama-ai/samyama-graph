//! HA-09: HTTP endpoints for tenant CRUD, backed by the shared `TenantManager`.
//!
//! Routes:
//! - `POST   /api/tenants`       — create a tenant
//! - `GET    /api/tenants`       — list all tenants
//! - `GET    /api/tenants/:id`   — get one tenant
//! - `DELETE /api/tenants/:id`   — delete a tenant
//!
//! A tenant created here is immediately visible to the RESP `GRAPH.LIST`
//! command because the same `Arc<TenantManager>` backs both paths.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, patch, post},
    Json, Router,
};
use crate::embed::EmbedPipeline;
use crate::persistence::{AutoEmbedConfig, ResourceQuotas, TenantError, TenantManager};
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TenantState {
    pub tenants: Arc<TenantManager>,
    /// Shared with AppState; cleared on PATCH to avoid serving stale pipelines.
    pub embed_cache: Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>>,
}

#[derive(Deserialize)]
pub struct CreateTenantBody {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub quotas: Option<ResourceQuotas>,
}

fn tenant_to_json(t: &crate::persistence::Tenant) -> serde_json::Value {
    json!({
        "id": t.id,
        "name": t.name,
        "enabled": t.enabled,
        "embed_config": t.embed_config.as_ref().map(embed_config_to_json),
    })
}

/// The tenant's embedding configuration, with the key redacted.
///
/// This view did not exist (#1494). `embed_config` could be written over
/// `PATCH` and never read back, so a PATCH that erased it looked exactly like
/// one that set it: 200, same body. Whether the config is there is the thing a
/// caller needs to be able to check, and it could not.
///
/// `api_key` is a secret and is never returned; the boolean says whether one is
/// stored, which is what a caller debugging a silent embedding failure actually
/// needs to know.
fn embed_config_to_json(c: &AutoEmbedConfig) -> serde_json::Value {
    json!({
        "provider": c.provider,
        "embedding_model": c.embedding_model,
        "api_key_set": c.api_key.is_some(),
        "api_base_url": c.api_base_url,
        "chunk_size": c.chunk_size,
        "chunk_overlap": c.chunk_overlap,
        "vector_dimension": c.vector_dimension,
        "embedding_policies": c.embedding_policies,
        "embedding_property": c.embedding_property,
    })
}

pub async fn create_tenant(
    State(state): State<TenantState>,
    Json(body): Json<CreateTenantBody>,
) -> impl IntoResponse {
    match state.tenants.create_tenant(body.id.clone(), body.name.clone(), body.quotas) {
        Ok(()) => match state.tenants.get_tenant(&body.id) {
            Ok(t) => (StatusCode::CREATED, Json(tenant_to_json(&t))).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response(),
        },
        Err(TenantError::AlreadyExists(id)) => (
            StatusCode::CONFLICT,
            Json(json!({ "error": format!("Tenant '{}' already exists", id) })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn list_tenants(State(state): State<TenantState>) -> impl IntoResponse {
    let mut tenants = state.tenants.list_tenants();
    tenants.sort_by(|a, b| a.id.cmp(&b.id));
    let body = json!({
        "tenants": tenants.iter().map(tenant_to_json).collect::<Vec<_>>(),
    });
    (StatusCode::OK, Json(body)).into_response()
}

pub async fn get_tenant(
    State(state): State<TenantState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.tenants.get_tenant(&id) {
        Ok(t) => (StatusCode::OK, Json(tenant_to_json(&t))).into_response(),
        Err(TenantError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Tenant '{}' not found", id) })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

pub async fn delete_tenant(
    State(state): State<TenantState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.tenants.delete_tenant(&id) {
        Ok(()) => (StatusCode::NO_CONTENT, ()).into_response(),
        Err(TenantError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Tenant '{}' not found", id) })),
        )
            .into_response(),
        Err(TenantError::PermissionDenied(msg)) => (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": msg })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Request body for PATCH /api/tenants/:id — update embed_config.
///
/// `Option<Option<_>>` is load-bearing, not a typo (#1494). A PATCH says what
/// to change; a field it does not mention must be left alone. With a plain
/// `Option`, an absent `embed_config` and an explicit `"embed_config": null`
/// both deserialize to `None`, and the handler passed that straight to
/// `update_embed_config`, which writes it. So `PATCH {}` erased the tenant's
/// embedding configuration and answered 200 — and with no `deny_unknown_fields`
/// so did `PATCH {"embedconfig": {...}}`, a single missing underscore. The
/// config is write-only over this API (`tenant_to_json` does not return it), so
/// the 200 was the only thing the caller ever saw. The first sign was
/// embeddings silently no longer being produced.
///
/// The three cases are now distinct:
///   absent          -> `None`             -> leave it as it is
///   `null`          -> `Some(None)`       -> clear it, deliberately
///   an object       -> `Some(Some(cfg))`  -> set it
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchTenantBody {
    #[serde(default, deserialize_with = "deserialize_some")]
    pub embed_config: Option<Option<AutoEmbedConfig>>,
}

/// Deserialize a present field into `Some(_)`, so that `null` is distinguishable
/// from absent. Without it `#[serde(default)]` collapses the two.
fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    T::deserialize(deserializer).map(Some)
}

pub async fn patch_tenant(
    State(state): State<TenantState>,
    Path(id): Path<String>,
    body: Result<Json<PatchTenantBody>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    // Taking the rejection by hand turns axum's default 422 into the 400 the
    // rest of this handler answers a bad request with, and lets the body name
    // the field serde refused — which is the whole value of
    // `deny_unknown_fields` to a caller who has just mistyped one.
    let body = match body {
        Ok(Json(b)) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("invalid tenant patch: {}", e.body_text()) })),
            )
                .into_response()
        }
    };

    // Absent means "not mentioned", so there is nothing to do and nothing to
    // invalidate. Answering with the tenant unchanged is the honest 200.
    let Some(embed_config) = body.embed_config else {
        return match state.tenants.get_tenant(&id) {
            Ok(t) => (StatusCode::OK, Json(tenant_to_json(&t))).into_response(),
            Err(TenantError::NotFound(_)) => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": format!("Tenant '{}' not found", id) })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": e.to_string() })),
            )
                .into_response(),
        };
    };

    match state.tenants.update_embed_config(&id, embed_config) {
        Ok(()) => {
            // Invalidate cached pipeline so the next search rebuilds from the new config.
            state.embed_cache.write().await.remove(&id);
            match state.tenants.get_tenant(&id) {
                Ok(t) => (StatusCode::OK, Json(tenant_to_json(&t))).into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": e.to_string() })),
                )
                    .into_response(),
            }
        }
        Err(TenantError::NotFound(_)) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("Tenant '{}' not found", id) })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// Build the tenant CRUD router, parameterised on the shared `TenantManager`.
pub fn router(
    tenants: Arc<TenantManager>,
    embed_cache: Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>>,
) -> Router {
    let state = TenantState { tenants, embed_cache };
    Router::new()
        .route("/api/tenants", post(create_tenant).get(list_tenants))
        .route(
            "/api/tenants/:id",
            get(get_tenant).delete(delete_tenant).patch(patch_tenant),
        )
        .with_state(state)
}
