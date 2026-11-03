//! `/api/vector-search` refuses a `graph` it cannot honour (#1476).
//!
//! It took a `graph` argument, used it only to select an embedding pipeline,
//! and then searched the one store. So a tenant that **exists** read another
//! tenant's data, and a tenant id that does not exist read it too:
//!
//! ```text
//! POST /api/vector-search {"label":"Doc","graph":"ta", ...}
//!   -> 200, {"name": "default-tenant-secret"}
//! POST /api/vector-search {"label":"Doc","graph":"no-such-tenant-at-all", ...}
//!   -> 200, {"name": "default-tenant-secret"}
//! ```
//!
//! `/api/query` and `/api/query/export` already refused the identical argument.
//! The engine knew how to say this; the vector path did not ask.
//!
//! In a single-graph build nothing is technically crossed — but the shape is
//! the dangerous part: an argument accepted, ignored, and answered `200`. The
//! same handler in a multi-graph build serves the wrong graph with no signal at
//! the call site.
//!
//! The guard is now `handler::reject_foreign_graph`, one implementation for the
//! four handlers that take a `graph`, so they cannot drift apart again.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::http::server::HttpServer;
use samyama::vector::DistanceMetric;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn store_with_a_secret() -> Arc<RwLock<GraphStore>> {
    let mut store = GraphStore::new();
    store
        .vector_index
        .create_index("Doc", "embedding", 3, DistanceMetric::L2)
        .expect("index");
    let id = store.create_node("Doc");
    store
        .set_node_property("default", id, "name", "default-tenant-secret")
        .unwrap();
    store
        .set_node_property(
            "default",
            id,
            "embedding",
            PropertyValue::Array(vec![
                PropertyValue::Float(1.0),
                PropertyValue::Float(0.0),
                PropertyValue::Float(0.0),
            ]),
        )
        .unwrap();
    Arc::new(RwLock::new(store))
}

async fn search(graph: Option<&str>) -> (StatusCode, serde_json::Value) {
    let app = HttpServer::new(store_with_a_secret().await, 0).router();
    let mut body = serde_json::json!({
        "query_vector": [1.0, 0.0, 0.0], "k": 3, "label": "Doc"
    });
    if let Some(g) = graph {
        body["graph"] = serde_json::json!(g);
    }
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/vector-search")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::json!({})))
}

use tower::ServiceExt;

#[tokio::test]
async fn a_foreign_graph_is_refused_rather_than_ignored() {
    for g in ["ta", "tb", "no-such-tenant-at-all"] {
        let (status, body) = search(Some(g)).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "graph {g:?} was not refused; body: {body}"
        );
        let err = body["error"].as_str().unwrap_or_default();
        assert!(
            err.contains(g),
            "the refusal must name the graph that was asked for, got: {err}"
        );
    }
}

/// The half that matters for not breaking anything.
#[tokio::test]
async fn the_served_graph_and_an_omitted_argument_still_work() {
    let (status, body) = search(Some("default")).await;
    assert_eq!(status, StatusCode::OK, "explicit default must work: {body}");
    assert_eq!(body["results"].as_array().map(|a| a.len()), Some(1));

    let (status, body) = search(None).await;
    assert_eq!(status, StatusCode::OK, "omitted graph must work: {body}");
    assert_eq!(body["results"].as_array().map(|a| a.len()), Some(1));
}
