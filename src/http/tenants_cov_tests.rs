//! Tenant CRUD over HTTP, driven through the router in-process.

use super::*;
use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use tower::util::ServiceExt;

type Cache = Arc<RwLock<HashMap<String, Arc<EmbedPipeline>>>>;

fn app() -> (Router, Arc<TenantManager>, Cache) {
    let tm = Arc::new(TenantManager::new());
    let cache: Cache = Arc::new(RwLock::new(HashMap::new()));
    (router(Arc::clone(&tm), Arc::clone(&cache)), tm, cache)
}

async fn call(
    app: Router,
    method: &str,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let resp = app
        .oneshot(
            req.body(Body::from(body.unwrap_or("").to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
        })
    };
    (status, json)
}

fn mock_config_json() -> serde_json::Value {
    json!({
        "provider": "Mock",
        "embedding_model": "mock",
        "api_key": "sk-secret",
        "api_base_url": null,
        "chunk_size": 100,
        "chunk_overlap": 10,
        "vector_dimension": 64,
        "embedding_policies": { "Doc": ["body"] }
    })
}

#[tokio::test]
async fn create_then_get_returns_the_tenant_without_an_embed_config() {
    let (app, tm, _) = app();
    let (status, body) = call(
        app.clone(),
        "POST",
        "/api/tenants",
        Some(r#"{"id":"acme","name":"Acme Corp"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["id"], "acme");
    assert_eq!(body["name"], "Acme Corp");
    assert_eq!(body["enabled"], true);
    assert!(body["embed_config"].is_null());
    assert!(tm.get_tenant("acme").is_ok());

    let (status, body) = call(app, "GET", "/api/tenants/acme", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["name"], "Acme Corp");
}

#[tokio::test]
async fn creating_a_tenant_twice_is_a_conflict() {
    let (app, _, _) = app();
    let body = r#"{"id":"dup","name":"Dup"}"#;
    assert_eq!(
        call(app.clone(), "POST", "/api/tenants", Some(body))
            .await
            .0,
        StatusCode::CREATED
    );
    let (status, err) = call(app, "POST", "/api/tenants", Some(body)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(err["error"], "Tenant 'dup' already exists");
}

#[tokio::test]
async fn a_tenant_can_be_created_with_quotas() {
    let (app, tm, _) = app();
    let quotas = serde_json::to_value(ResourceQuotas::default()).unwrap();
    let body = json!({ "id": "q", "name": "Quota", "quotas": quotas }).to_string();
    let (status, json) = call(app, "POST", "/api/tenants", Some(&body)).await;
    assert_eq!(status, StatusCode::CREATED, "{json}");
    assert_eq!(tm.get_tenant("q").unwrap().name, "Quota");
}

#[tokio::test]
async fn list_is_sorted_by_id_and_includes_the_default_tenant() {
    let (app, _, _) = app();
    for id in ["zulu", "alpha"] {
        let b = json!({ "id": id, "name": id }).to_string();
        call(app.clone(), "POST", "/api/tenants", Some(&b)).await;
    }
    let (status, body) = call(app, "GET", "/api/tenants", None).await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<&str> = body["tenants"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["alpha", "default", "zulu"]);
}

#[tokio::test]
async fn get_and_delete_of_an_unknown_tenant_are_404() {
    let (app, _, _) = app();
    let (status, body) = call(app.clone(), "GET", "/api/tenants/nope", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "Tenant 'nope' not found");
    let (status, body) = call(app, "DELETE", "/api/tenants/nope", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "Tenant 'nope' not found");
}

#[tokio::test]
async fn the_default_tenant_cannot_be_deleted() {
    let (app, tm, _) = app();
    let (status, body) = call(app, "DELETE", "/api/tenants/default", None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"], "Cannot delete default tenant");
    assert!(tm.get_tenant("default").is_ok());
}

#[tokio::test]
async fn delete_removes_the_tenant_with_no_content() {
    let (app, tm, _) = app();
    call(
        app.clone(),
        "POST",
        "/api/tenants",
        Some(r#"{"id":"gone","name":"Gone"}"#),
    )
    .await;
    let (status, body) = call(app.clone(), "DELETE", "/api/tenants/gone", None).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_null(), "a 204 has no body: {body}");
    assert!(tm.get_tenant("gone").is_err());
    assert_eq!(
        call(app, "GET", "/api/tenants/gone", None).await.0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn patch_sets_the_embed_config_and_redacts_the_key() {
    let (app, tm, cache) = app();
    call(
        app.clone(),
        "POST",
        "/api/tenants",
        Some(r#"{"id":"t","name":"T"}"#),
    )
    .await;
    // A stale pipeline for this tenant must be dropped by the PATCH.
    let stale: AutoEmbedConfig = serde_json::from_value(mock_config_json()).unwrap();
    cache
        .write()
        .await
        .insert("t".into(), Arc::new(EmbedPipeline::new(stale).unwrap()));

    let body = json!({ "embed_config": mock_config_json() }).to_string();
    let (status, json) = call(app, "PATCH", "/api/tenants/t", Some(&body)).await;
    assert_eq!(status, StatusCode::OK, "{json}");
    let cfg = &json["embed_config"];
    assert_eq!(cfg["provider"], "Mock");
    assert_eq!(cfg["embedding_model"], "mock");
    assert_eq!(cfg["api_key_set"], true);
    assert_eq!(cfg["vector_dimension"], 64);
    assert_eq!(cfg["chunk_size"], 100);
    assert_eq!(cfg["chunk_overlap"], 10);
    assert_eq!(cfg["embedding_property"], "embedding");
    assert_eq!(cfg["embedding_policies"]["Doc"][0], "body");
    assert!(
        !json.to_string().contains("sk-secret"),
        "the key leaked: {json}"
    );
    assert!(tm.get_tenant("t").unwrap().embed_config.is_some());
    assert!(
        cache.read().await.get("t").is_none(),
        "the cached pipeline survived"
    );
}

#[tokio::test]
async fn patch_without_the_field_leaves_the_config_alone() {
    let (app, tm, _) = app();
    call(
        app.clone(),
        "POST",
        "/api/tenants",
        Some(r#"{"id":"t","name":"T"}"#),
    )
    .await;
    let set = json!({ "embed_config": mock_config_json() }).to_string();
    call(app.clone(), "PATCH", "/api/tenants/t", Some(&set)).await;

    let (status, json) = call(app, "PATCH", "/api/tenants/t", Some("{}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["embed_config"]["embedding_model"], "mock");
    assert!(tm.get_tenant("t").unwrap().embed_config.is_some());
}

#[tokio::test]
async fn patch_with_null_clears_the_config() {
    let (app, tm, _) = app();
    call(
        app.clone(),
        "POST",
        "/api/tenants",
        Some(r#"{"id":"t","name":"T"}"#),
    )
    .await;
    let set = json!({ "embed_config": mock_config_json() }).to_string();
    call(app.clone(), "PATCH", "/api/tenants/t", Some(&set)).await;

    let (status, json) = call(
        app,
        "PATCH",
        "/api/tenants/t",
        Some(r#"{"embed_config":null}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["embed_config"].is_null());
    assert!(tm.get_tenant("t").unwrap().embed_config.is_none());
}

#[tokio::test]
async fn patch_with_a_misspelled_field_is_a_400_naming_it() {
    let (app, _, _) = app();
    let (status, json) = call(
        app,
        "PATCH",
        "/api/tenants/default",
        Some(r#"{"embedconfig":{}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let e = json["error"].as_str().unwrap();
    assert!(e.starts_with("invalid tenant patch:"), "{e}");
    assert!(e.contains("embedconfig"), "{e}");
}

#[tokio::test]
async fn patch_of_an_unknown_tenant_is_404_either_way() {
    let (app, _, _) = app();
    let (status, json) = call(app.clone(), "PATCH", "/api/tenants/ghost", Some("{}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json["error"], "Tenant 'ghost' not found");
    let set = json!({ "embed_config": mock_config_json() }).to_string();
    let (status, json) = call(app, "PATCH", "/api/tenants/ghost", Some(&set)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json["error"], "Tenant 'ghost' not found");
}

#[tokio::test]
async fn patch_with_an_unusable_config_is_a_400() {
    let (app, tm, _) = app();
    let mut cfg = mock_config_json();
    cfg["vector_dimension"] = json!(0);
    let body = json!({ "embed_config": cfg }).to_string();
    let (status, json) = call(app, "PATCH", "/api/tenants/default", Some(&body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        json["error"].as_str().unwrap().contains("vector_dimension"),
        "{json}"
    );
    assert!(tm.get_tenant("default").unwrap().embed_config.is_none());
}
