//! A node imported over HTTP is in every index a directly-written node is
//! in (#1505).
//!
//! # What was wrong
//!
//! `/api/import/csv` and `/api/import/json` set each property on the `&mut Node`
//! that `get_node_mut` hands out:
//!
//! ```rust,ignore
//! if let Some(node) = store_guard.get_node_mut(node_id) { ... node.set_property(...) }
//! ```
//!
//! That writes the value and maintains nothing. The full-text index is updated
//! from `apply_property_set_readback`, which only `set_node_property` runs.
//! Measured on the store directly:
//!
//! ```text
//! create_node + set_node_property                  -> search returns 1
//! create_node + get_node_mut(id).set_property(..)  -> search returns 0
//! ```
//!
//! So a corpus imported through either endpoint was findable by `MATCH` and
//! invisible to `db.index.fulltext.queryNodes`, with no error and no warning at
//! any point. The import reported `"status": "ok"` and the row count.
//!
//! # Not the same defect as #1472 or #1477
//!
//! Full-text is maintained correctly from the writing thread whenever the
//! setter is used, with or without `index_sender` armed. This is a third path
//! that skipped the setter, not a recurrence of either.
//!
//! # Why the tests go through the store rather than the router
//!
//! The endpoints' own property loops are what changed, and the assertion that
//! matters is "the index saw it". Both spellings are exercised here against the
//! same index so the two cannot drift apart again without a failure.

use samyama::graph::{GraphStore, PropertyValue};

const T: &str = "default";

fn store_with_index() -> GraphStore {
    let mut s = GraphStore::new();
    s.create_fulltext_index("ft", "Doc", "body");
    s
}

fn hits(s: &GraphStore, term: &str) -> usize {
    s.fulltext.search("ft", term, 10).map(|v| v.len()).unwrap_or(0)
}

#[test]
fn the_setter_maintains_the_full_text_index() {
    // The control: the path that always worked.
    let mut s = store_with_index();
    let n = s.create_node("Doc");
    s.set_node_property(T, n, "body", PropertyValue::String("quick brown fox".into()))
        .expect("set");
    assert_eq!(hits(&s, "quick"), 1);
}

#[test]
fn setting_through_get_node_mut_does_not_maintain_it() {
    // Pinning the asymmetry itself, so the reason the import handlers had to
    // change is recorded rather than implied. If `get_node_mut` is ever made to
    // maintain the registries, this test is the one that should fail and be
    // deleted deliberately.
    let mut s = store_with_index();
    let n = s.create_node("Doc");
    if let Some(node) = s.get_node_mut(n) {
        node.set_property("body".to_string(), PropertyValue::String("quick brown fox".into()));
    }
    assert_eq!(
        hits(&s, "quick"),
        0,
        "get_node_mut now maintains full-text; the import handlers can be simplified"
    );
}

#[test]
fn a_node_written_the_way_the_importers_write_is_searchable() {
    // The shape both handlers now use: collect, then write through the setter.
    let mut s = store_with_index();
    let n = s.create_node("Doc");
    let props = vec![
        ("body".to_string(), PropertyValue::String("quick brown fox".into())),
        ("title".to_string(), PropertyValue::String("untouched".into())),
    ];
    for (k, v) in props {
        s.set_node_property(T, n, k, v).expect("set");
    }
    assert_eq!(hits(&s, "quick"), 1);
    assert_eq!(hits(&s, "fox"), 1);
    assert_eq!(hits(&s, "untouched"), 0, "only the indexed property is indexed");
}

#[test]
fn many_imported_rows_are_all_searchable() {
    // One row working is consistent with the index being rebuilt by something
    // else on the way past; a corpus is not.
    let mut s = store_with_index();
    for i in 0..200u64 {
        let n = s.create_node("Doc");
        s.set_node_property(T, n, "body", PropertyValue::String(format!("document {i} about graphs")))
            .expect("set");
    }
    assert_eq!(hits(&s, "graphs"), 10, "search caps at the limit asked for");
    assert_eq!(hits(&s, "nonexistentterm"), 0);
}

#[test]
fn an_index_created_after_the_rows_still_finds_them() {
    // The other order, which the endpoints allow: import first, declare after.
    let mut s = GraphStore::new();
    let n = s.create_node("Doc");
    s.set_node_property(T, n, "body", PropertyValue::String("quick brown fox".into()))
        .expect("set");
    s.create_fulltext_index("ft", "Doc", "body");
    assert_eq!(hits(&s, "quick"), 1, "declaring the index did not backfill it");
}

// ---------------------------------------------------------------------------
// Through the endpoints themselves.
//
// Everything above tests the store. None of it can fail for the change this
// file is about, because the change is in the handlers: they used to write
// through `get_node_mut` and now write through the setter. These are the tests
// that go red without it.
// ---------------------------------------------------------------------------

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;
use std::sync::Arc;
use tower::ServiceExt;

fn import_app(
    store: GraphStore,
) -> (axum::Router, Arc<tokio::sync::RwLock<GraphStore>>) {
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

async fn post_json(app: &axum::Router, body: serde_json::Value) -> StatusCode {
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
    let _ = res.into_body().collect().await.unwrap().to_bytes();
    status
}

#[tokio::test]
async fn a_corpus_imported_over_http_is_searchable() {
    let (app, store) = import_app(store_with_index());

    let status = post_json(
        &app,
        json!({
            "label": "Doc",
            "nodes": [
                {"body": "quick brown fox"},
                {"body": "lazy dog sleeping"},
                {"body": "the quick end"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let s = store.read().await;
    assert_eq!(s.node_count(), 3, "the rows landed");
    assert_eq!(
        hits(&s, "quick"),
        2,
        "the documents are in the graph and not in the index: this is #1505"
    );
    assert_eq!(hits(&s, "dog"), 1);
}

#[tokio::test]
async fn an_http_import_into_a_store_with_no_index_still_works() {
    // The change must not make an import depend on an index existing.
    let (app, store) = import_app(GraphStore::new());
    let status = post_json(&app, json!({"label": "Doc", "nodes": [{"body": "no index here"}]})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(store.read().await.node_count(), 1);
}

#[tokio::test]
async fn an_http_import_still_writes_every_property_it_used_to() {
    // The handler now collects and re-writes rather than setting in place. The
    // properties themselves must be unchanged, including the non-string types.
    let (app, store) = import_app(GraphStore::new());
    let status = post_json(
        &app,
        json!({"label": "Doc", "nodes": [{"s": "text", "i": 42, "f": 1.5, "b": true, "skipped": null}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let s = store.read().await;
    let id = s.get_nodes_by_label(&samyama::graph::Label::new("Doc"))[0].id;
    let p = s.node_properties_full(id);
    assert_eq!(p.get("s"), Some(&PropertyValue::String("text".into())));
    assert_eq!(p.get("i"), Some(&PropertyValue::Integer(42)));
    assert_eq!(p.get("f"), Some(&PropertyValue::Float(1.5)));
    assert_eq!(p.get("b"), Some(&PropertyValue::Boolean(true)));
    assert!(p.get("skipped").is_none(), "a null is still skipped, as before");
}
