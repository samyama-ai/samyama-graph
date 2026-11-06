//! `/api/import/json` keeps list properties, embeddings among them (#2047).
//!
//! Node properties went through a converter that knew only strings, numbers
//! and booleans and answered `None` for anything else, so an `embedding` list
//! was left off every imported node, the vector index never saw it, and the
//! import answered `200 ok` with the row count. These tests go through the
//! handler: what changed is its property loop.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::vector::DistanceMetric;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

fn import_app(store: GraphStore) -> (axum::Router, Arc<tokio::sync::RwLock<GraphStore>>) {
    let store = Arc::new(tokio::sync::RwLock::new(store));
    let state = samyama::http::server::AppState {
        store: store.clone(),
        engine: Arc::new(samyama::query::QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        persistence: None,
        snapshot_key: None,
        transactions: Default::default(),
    };
    let app = axum::Router::new()
        .route(
            "/api/import/json",
            axum::routing::post(samyama::http::handler::import_json_handler),
        )
        .with_state(state);
    (app, store)
}

async fn post_json(app: &axum::Router, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/import/json")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
}

fn indexed_store() -> GraphStore {
    let s = GraphStore::new();
    s.create_vector_index("Doc", "embedding", 3, DistanceMetric::Cosine)
        .expect("index");
    s
}

#[tokio::test]
async fn an_imported_embedding_reaches_the_vector_index() {
    let (app, store) = import_app(indexed_store());
    let (status, body) = post_json(
        &app,
        json!({
            "label": "Doc",
            "nodes": [
                {"name": "x", "embedding": [1.0, 0.0, 0.0]},
                {"name": "y", "embedding": [0.0, 1.0, 0.0]},
                {"name": "z", "embedding": [0.0, 0.0, 1.0]}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "ok", "{body}");
    assert_eq!(body["nodes_created"], 3, "{body}");

    let s = store.read().await;
    let hits = s
        .vector_index
        .search("Doc", "embedding", &[0.0, 0.9, 0.1], 1)
        .expect("search");
    assert_eq!(hits.len(), 1, "the vectors are on the nodes and not in the index");
    let name = s.node_properties_full(hits[0].0).get("name").cloned();
    assert_eq!(name, Some(PropertyValue::String("y".into())));
}

#[tokio::test]
async fn an_imported_list_is_stored_on_the_node_without_an_index() {
    let (app, store) = import_app(GraphStore::new());
    let (status, body) = post_json(
        &app,
        json!({"label": "Doc", "nodes": [{"embedding": [0.5, 1, 2.5], "tags": ["a", "b"]}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let s = store.read().await;
    let id = s.get_nodes_by_label(&Label::new("Doc"))[0].id;
    let p = s.node_properties_full(id);
    assert_eq!(
        p.get("embedding").and_then(|v| v.to_vector()),
        Some(vec![0.5, 1.0, 2.5]),
        "the list was dropped: {p:?}"
    );
    assert_eq!(
        p.get("tags"),
        Some(&PropertyValue::Array(vec![
            PropertyValue::String("a".into()),
            PropertyValue::String("b".into()),
        ]))
    );
}

#[tokio::test]
async fn a_vector_of_the_wrong_dimensions_refuses_the_import() {
    // The store would keep the vector on the node and leave it out of the
    // index without a word. Refused before the first node, so nothing is
    // half-imported.
    let (app, store) = import_app(indexed_store());
    let (status, body) = post_json(
        &app,
        json!({
            "label": "Doc",
            "nodes": [
                {"embedding": [1.0, 0.0, 0.0]},
                {"embedding": [1.0, 0.0]}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let e = body["error"].as_str().unwrap_or_default();
    assert!(e.contains("node 1") && e.contains("3 dimensions") && e.contains("has 2"), "{e}");
    assert_eq!(store.read().await.node_count(), 0, "nothing is written");
}

#[tokio::test]
async fn a_value_no_property_can_hold_is_refused_and_named() {
    let (app, store) = import_app(GraphStore::new());
    for (bad, says) in [
        (json!({"m": {"a": 1}}), "a map is not a property value"),
        (json!({"m": [1, {"a": 1}]}), "element 1 of the list is a map"),
        (json!({"m": [[1, 2], [3]]}), "element 0 of the list is a list"),
        (json!({"m": [1, null]}), "element 1 of the list is null"),
    ] {
        let (status, body) = post_json(&app, json!({"label": "Doc", "nodes": [{"ok": 1}, bad]})).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        let e = body["error"].as_str().unwrap_or_default();
        assert!(e.contains("node 1, property `m`") && e.contains(says), "{e}");
    }
    assert_eq!(store.read().await.node_count(), 0, "nothing is written");
}

#[tokio::test]
async fn a_property_the_store_refuses_is_reported_not_discarded() {
    // A uniqueness violation is found by the store, after nodes exist. It was
    // `let _ =`; the response now says which property was not written.
    let mut s = GraphStore::new();
    s.create_unique_constraint(&Label::new("Doc"), "id").expect("constraint");
    let (app, _store) = import_app(s);
    let (status, body) = post_json(
        &app,
        json!({"label": "Doc", "nodes": [{"id": 1}, {"id": 1}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "partial", "{body}");
    assert_eq!(body["properties_not_written"], 1, "{body}");
    let e = body["errors"][0].as_str().unwrap_or_default();
    assert!(e.contains("node 1, property `id`"), "{e}");
}

#[tokio::test]
async fn an_edge_list_property_is_kept_and_a_bad_one_refused() {
    let (app, store) = import_app(GraphStore::new());
    let (status, body) = post_json(
        &app,
        json!({"label": "P", "nodes": [{"k": "a"}, {"k": "b"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let edge = |props: serde_json::Value| {
        let mut e = json!({"from": "a", "to": "b"});
        e.as_object_mut().unwrap().extend(props.as_object().unwrap().clone());
        json!({
            "import_mode": "edge",
            "edge_type": "R",
            "source_label": "P", "source_key_col": "from", "source_key_prop": "k",
            "target_label": "P", "target_key_col": "to", "target_key_prop": "k",
            "edges": [e]
        })
    };
    let (status, body) = post_json(&app, edge(json!({"w": {"a": 1}}))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["error"].as_str().unwrap_or_default().contains("edge 0, property `w`"),
        "{body}"
    );
    assert_eq!(store.read().await.edge_count(), 0);

    let (status, body) = post_json(&app, edge(json!({"w": [1, 2]}))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(store.read().await.edge_count(), 1, "{body}");
}
