//! `PATCH /api/tenants/:id` changes what it is told to change, and nothing else (#1494).
//!
//! # What was wrong
//!
//! ```rust,ignore
//! pub struct PatchTenantBody { pub embed_config: Option<AutoEmbedConfig> }
//! ...
//! state.tenants.update_embed_config(&id, body.embed_config)
//! ```
//!
//! A plain `Option` cannot tell an absent field from an explicit `null`: both
//! deserialize to `None`, and `None` was written. So
//!
//! - `PATCH {}` erased the tenant's embedding configuration and answered 200,
//! - `PATCH {"embedconfig": {...}}` — one missing underscore — did the same,
//!   because the struct had no `deny_unknown_fields`.
//!
//! Neither was observable. `tenant_to_json` returned `id`, `name` and
//! `enabled`; `embed_config` was write-only over the whole API, so a caller
//! got the identical 200 body whether they had set the config or destroyed it.
//! The first symptom was embeddings quietly no longer being produced, at a
//! remove from the request that caused it.
//!
//! # What these tests pin
//!
//! Absent, `null` and a value are three different instructions. An unknown
//! field is a 400 that names it. And the config can be read back — redacted,
//! because `api_key` is a secret — so that every one of these assertions is
//! something a caller could make too.

use samyama::persistence::tenant::{AutoEmbedConfig, LLMProvider, TenantManager};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

const T: &str = "acme";

fn config() -> AutoEmbedConfig {
    AutoEmbedConfig {
        provider: LLMProvider::Ollama,
        embedding_model: "nomic-embed-text".to_string(),
        api_key: Some("sk-not-a-real-key".to_string()),
        api_base_url: Some("http://127.0.0.1:11434".to_string()),
        chunk_size: 512,
        chunk_overlap: 64,
        vector_dimension: 768,
        embedding_policies: HashMap::from([(
            "Doc".to_string(),
            vec!["body".to_string()],
        )]),
        embedding_property: "embedding".to_string(),
    }
}

/// A tenants router over one tenant that already has an embedding config.
fn app_with_configured_tenant() -> axum::Router {
    let tenants = Arc::new(TenantManager::new());
    tenants
        .create_tenant(T.to_string(), "Acme".to_string(), Default::default())
        .expect("create");
    tenants
        .update_embed_config(T, Some(config()))
        .expect("seed the config");
    samyama::http::tenants::router(tenants, Arc::new(tokio::sync::RwLock::new(HashMap::new())))
}

async fn send(app: &axum::Router, method: &str, uri: &str, body: Option<Value>) -> (StatusCode, Value) {
    let req = Request::builder().method(method).uri(uri);
    let req = match body {
        Some(b) => req
            .header("content-type", "application/json")
            .body(Body::from(b.to_string())),
        None => req.body(Body::empty()),
    }
    .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let v: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| json!({}));
    (status, v)
}

async fn config_of(app: &axum::Router) -> Value {
    let (status, body) = send(app, "GET", &format!("/api/tenants/{T}"), None).await;
    assert_eq!(status, StatusCode::OK, "GET tenant: {body}");
    body["embed_config"].clone()
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_config_can_be_read_back_at_all() {
    // Everything below depends on this: with `embed_config` write-only there is
    // no assertion a caller could make, which is why the defect survived.
    let app = app_with_configured_tenant();
    let c = config_of(&app).await;
    assert_eq!(c["embedding_model"], "nomic-embed-text");
    assert_eq!(c["vector_dimension"], 768);
}

#[tokio::test]
async fn the_key_itself_is_never_returned() {
    let app = app_with_configured_tenant();
    let c = config_of(&app).await;
    assert_eq!(c["api_key_set"], json!(true), "a caller needs to know one is set");
    assert!(
        !c.to_string().contains("sk-not-a-real-key"),
        "the key must not be in the response: {c}"
    );
}

#[tokio::test]
async fn an_empty_patch_leaves_the_config_alone() {
    // The data loss. Before the fix this returned 200 and cleared the config.
    let app = app_with_configured_tenant();
    let (status, _) = send(&app, "PATCH", &format!("/api/tenants/{T}"), Some(json!({}))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        config_of(&app).await["embedding_model"],
        "nomic-embed-text",
        "a field the patch did not mention must survive it"
    );
}

#[tokio::test]
async fn a_misspelled_field_is_a_400_that_names_it_and_changes_nothing() {
    let app = app_with_configured_tenant();
    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/tenants/{T}"),
        Some(json!({"embedconfig": {"provider": "Ollama"}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("embedconfig"),
        "the error must name the field that was refused: {body}"
    );
    assert_eq!(
        config_of(&app).await["embedding_model"],
        "nomic-embed-text",
        "a refused patch must not have written anything"
    );
}

#[tokio::test]
async fn an_explicit_null_still_clears_the_config() {
    // Clearing must remain possible — the fix distinguishes the two cases, it
    // does not remove one of them.
    let app = app_with_configured_tenant();
    let (status, _) = send(
        &app,
        "PATCH",
        &format!("/api/tenants/{T}"),
        Some(json!({"embed_config": null})),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        config_of(&app).await,
        Value::Null,
        "an explicit null is an instruction to clear"
    );
}

#[tokio::test]
async fn a_patch_with_a_config_sets_it() {
    let app = app_with_configured_tenant();
    let mut c = config();
    c.embedding_model = "all-minilm".to_string();
    c.vector_dimension = 384;
    let (status, body) = send(
        &app,
        "PATCH",
        &format!("/api/tenants/{T}"),
        Some(json!({"embed_config": c})),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let back = config_of(&app).await;
    assert_eq!(back["embedding_model"], "all-minilm");
    assert_eq!(back["vector_dimension"], 384);
}

#[tokio::test]
async fn an_empty_patch_against_a_tenant_that_does_not_exist_is_404() {
    // The short-circuit for "nothing to do" must not turn a missing tenant
    // into a 200.
    let app = app_with_configured_tenant();
    let (status, _) = send(&app, "PATCH", "/api/tenants/nobody", Some(json!({}))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
