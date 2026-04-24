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
    routing::{delete, get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::persistence::{ResourceQuotas, TenantError, TenantManager};

#[derive(Clone)]
pub struct TenantState {
    pub tenants: Arc<TenantManager>,
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

/// Build the tenant CRUD router, parameterised on the shared `TenantManager`.
pub fn router(tenants: Arc<TenantManager>) -> Router {
    let state = TenantState { tenants };
    Router::new()
        .route("/api/tenants", post(create_tenant).get(list_tenants))
        .route(
            "/api/tenants/:id",
            get(get_tenant).delete(delete_tenant),
        )
        .with_state(state)
}
