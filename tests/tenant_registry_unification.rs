//! HA-09: Unify HTTP + RESP tenant registries.
//!
//! A tenant created via HTTP must be visible to the RESP `GRAPH.LIST`
//! command, and vice versa — both paths share a single `TenantManager`.

use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::persistence::TenantManager;
use samyama::protocol::command::CommandHandler;
use samyama::protocol::resp::RespValue;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

fn http_router(tm: Arc<TenantManager>) -> Router {
    samyama::http::server::build_tenant_router(tm)
}

async fn post_json(app: Router, path: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn get_json(app: Router, path: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

fn resp_graph_list(handler: &CommandHandler, store: &Arc<RwLock<GraphStore>>) -> Vec<String> {
    let rt = tokio::runtime::Handle::current();
    let cmd = RespValue::Array(vec![
        RespValue::BulkString(Some(b"GRAPH.LIST".to_vec())),
    ]);
    let result = tokio::task::block_in_place(|| {
        rt.block_on(handler.handle_command(&cmd, store))
    });
    match result {
        RespValue::Array(items) => items
            .into_iter()
            .filter_map(|v| match v {
                RespValue::BulkString(Some(b)) => Some(String::from_utf8_lossy(&b).to_string()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_created_via_http_visible_to_resp() {
    let tm = Arc::new(TenantManager::new());
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let handler = CommandHandler::new_with_tenants(None, Arc::clone(&tm));

    // Precondition: only "default" exists.
    assert_eq!(resp_graph_list(&handler, &store), vec!["default".to_string()]);

    // Create via HTTP.
    let (status, _body) = post_json(
        http_router(Arc::clone(&tm)),
        "/api/tenants",
        json!({ "id": "alpha", "name": "Alpha Tenant" }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);

    // RESP now sees both.
    let mut listed = resp_graph_list(&handler, &store);
    listed.sort();
    assert_eq!(listed, vec!["alpha".to_string(), "default".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_created_via_tenant_manager_visible_to_http() {
    let tm = Arc::new(TenantManager::new());
    tm.create_tenant("beta".to_string(), "Beta".to_string(), None)
        .unwrap();

    let (status, body) = get_json(http_router(Arc::clone(&tm)), "/api/tenants").await;
    assert_eq!(status, StatusCode::OK);

    let ids: Vec<String> = body
        .get("tenants")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .filter_map(|t| t.get("id").and_then(|v| v.as_str()).map(String::from))
        .collect();
    assert!(ids.contains(&"default".to_string()));
    assert!(ids.contains(&"beta".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_tenant_creation_returns_conflict() {
    let tm = Arc::new(TenantManager::new());
    let (status1, _) = post_json(
        http_router(Arc::clone(&tm)),
        "/api/tenants",
        json!({ "id": "dup", "name": "First" }),
    )
    .await;
    assert_eq!(status1, StatusCode::CREATED);

    let (status2, _) = post_json(
        http_router(Arc::clone(&tm)),
        "/api/tenants",
        json!({ "id": "dup", "name": "Second" }),
    )
    .await;
    assert_eq!(status2, StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_tenant_removes_from_both_views() {
    let tm = Arc::new(TenantManager::new());
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let handler = CommandHandler::new_with_tenants(None, Arc::clone(&tm));

    post_json(
        http_router(Arc::clone(&tm)),
        "/api/tenants",
        json!({ "id": "ephemeral", "name": "E" }),
    )
    .await;
    assert!(resp_graph_list(&handler, &store).contains(&"ephemeral".to_string()));

    // Delete via HTTP.
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/tenants/ephemeral")
        .body(Body::empty())
        .unwrap();
    let resp = http_router(Arc::clone(&tm)).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Gone from RESP too.
    assert!(!resp_graph_list(&handler, &store).contains(&"ephemeral".to_string()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cannot_delete_default_tenant() {
    let tm = Arc::new(TenantManager::new());
    let req = Request::builder()
        .method("DELETE")
        .uri("/api/tenants/default")
        .body(Body::empty())
        .unwrap();
    let resp = http_router(Arc::clone(&tm)).oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}
