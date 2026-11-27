//! A graph's node style travels in the `.sgsnap` header and is served over
//! HTTP (`samyama.icons/1`, #2029).
//!
//! - Export writes the graph's style into the header; a graph without one
//!   writes a header with no `icons` key, exactly as before.
//! - Import loads the header's style, replacing the graph's, and clears it
//!   when the header has none. A bad entry is skipped with a warning and never
//!   fails the import.
//! - `GET /api/icons` serves it; `PUT /api/icons` replaces it, refusing a body
//!   with any mistake.

use std::io::{BufRead, BufReader};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::query::QueryEngine;
use samyama::snapshot::icons::{IconStyle, FORMAT, MAX_STYLE_BYTES};
use serde_json::{json, Value};
use tower::ServiceExt;

fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    QueryEngine::new()
        .execute_mut(
            "CREATE (c:Country {name: 'India'})-[:IN_REGION]->(r:Region {code: 'SEAR'}), \
                    (:Disease {name: 'Measles'})",
            &mut s,
            "default",
        )
        .unwrap();
    s
}

fn style(v: Value) -> IconStyle {
    let s = IconStyle::read(&v);
    assert!(s.warnings.is_empty(), "{:?}", s.warnings);
    s
}

fn country_style() -> IconStyle {
    style(json!({
        "format": FORMAT,
        "nodes": {
            "Country": {"color": "#2563EB", "icon": "lucide:Flag", "caption": "name"},
            "Region": {"color": "#7C3AED"}
        },
        "edges": {"IN_REGION": {"color": "#94A3B8"}}
    }))
}

fn export(store: &GraphStore) -> Vec<u8> {
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(store, &mut buf).unwrap();
    buf
}

fn header(snap: &[u8]) -> Value {
    let mut line = String::new();
    BufReader::new(flate2::read::GzDecoder::new(snap))
        .read_line(&mut line)
        .unwrap();
    serde_json::from_str(&line).unwrap()
}

/// The snapshot with its header's `icons` set to `icons` (or removed).
fn with_icons(snap: &[u8], icons: Option<Value>) -> Vec<u8> {
    let mut out = Vec::new();
    samyama::snapshot::edit_header(snap, &mut out, |h| {
        match icons {
            Some(v) => h.insert("icons".into(), v),
            None => h.remove("icons"),
        };
        Ok(())
    })
    .unwrap();
    out
}

#[test]
fn a_graph_without_a_style_writes_no_icons_key() {
    let h = header(&export(&graph()));
    assert!(h.get("icons").is_none(), "{h}");
    assert_eq!(h["version"], 2);
}

#[test]
fn the_style_round_trips_through_the_header() {
    let mut src = graph();
    src.set_icon_style(Some(country_style()), "api");
    let snap = export(&src);
    assert_eq!(
        header(&snap)["icons"]["nodes"]["Country"]["icon"],
        "lucide:Flag"
    );

    let mut dst = GraphStore::new();
    let stats = samyama::snapshot::import_tenant(&mut dst, &snap[..]).unwrap();
    assert_eq!(dst.icon_style(), Some(&country_style()));
    assert_eq!(dst.icon_style_source(), "header");
    assert_eq!(stats.icons.source, "header");
    assert_eq!(stats.icons.styled, 2);
    assert_eq!(stats.icons.unstyled, ["Disease"]);
    assert!(stats.icons.warnings.is_empty());
}

/// An older snapshot has no style: nothing fails, and the import clears the
/// style the graph had, so it cannot draw the new snapshot's labels.
#[test]
fn a_snapshot_without_a_style_clears_the_graph_s() {
    let old = export(&graph());
    let mut dst = GraphStore::new();
    dst.set_icon_style(Some(country_style()), "api");
    let stats = samyama::snapshot::import_tenant(&mut dst, &old[..]).unwrap();
    assert_eq!(dst.icon_style(), None);
    assert_eq!(dst.icon_style_source(), "none");
    assert_eq!(stats.icons.source, "none");
    assert_eq!(stats.icons.styled, 0);
    assert_eq!(stats.icons.unstyled, ["Country", "Disease", "Region"]);
}

/// Re-importing replaces the style rather than merging into it.
#[test]
fn a_reimport_replaces_the_style() {
    let mut dst = GraphStore::new();
    dst.set_icon_style(Some(country_style()), "api");
    let other = with_icons(
        &export(&graph()),
        Some(json!({"format": FORMAT, "nodes": {"Disease": {"color": "#DC2626"}}})),
    );
    samyama::snapshot::import_tenant(&mut dst, &other[..]).unwrap();
    let s = dst.icon_style().unwrap();
    assert_eq!(s.nodes.keys().collect::<Vec<_>>(), ["Disease"]);
    assert!(s.edges.is_empty());
}

#[test]
fn bad_entries_are_skipped_with_warnings_and_the_import_succeeds() {
    let snap = with_icons(
        &export(&graph()),
        Some(json!({
            "format": FORMAT,
            "nodes": {
                "Country": {"color": "blue", "icon": "lucide:Flag"},
                "Region": {"color": "#7C3AED"},
                "Ghost": {"color": "#000000"},
                "Disease": "red"
            },
            "edges": {"IN_REGION": {"color": "#94A3B8"}, "OLD": {"color": "#000000"}}
        })),
    );
    let mut dst = GraphStore::new();
    let stats = samyama::snapshot::import_tenant(&mut dst, &snap[..]).unwrap();
    assert_eq!(stats.node_count, 3);
    let s = dst.icon_style().unwrap();
    // The bad colour is dropped, the entry's icon kept.
    assert_eq!(s.nodes["Country"].color, None);
    assert_eq!(s.nodes["Country"].icon.as_deref(), Some("lucide:Flag"));
    // Labels the schema lacks are listed and not loaded.
    assert!(!s.nodes.contains_key("Ghost"));
    assert!(!s.edges.contains_key("OLD"));
    assert_eq!(stats.icons.unknown_labels, ["Ghost"]);
    assert_eq!(stats.icons.unknown_edge_types, ["OLD"]);
    assert_eq!(stats.icons.unstyled, ["Disease"]);
    assert_eq!(stats.icons.warnings.len(), 2, "{:?}", stats.icons.warnings);
}

#[test]
fn an_unknown_format_loads_no_style_and_says_why() {
    let snap = with_icons(
        &export(&graph()),
        Some(json!({"format": "samyama.icons/2", "nodes": {"Country": {"color": "#2563EB"}}})),
    );
    let mut dst = GraphStore::new();
    let stats = samyama::snapshot::import_tenant(&mut dst, &snap[..]).unwrap();
    assert_eq!(dst.icon_style(), None);
    assert_eq!(stats.icons.source, "none");
    assert_eq!(stats.icons.warnings.len(), 1);
    assert!(stats.icons.warnings[0].contains("samyama.icons/2"));
}

/// A header whose `icons` is not even an object still imports.
#[test]
fn a_malformed_style_never_fails_the_import() {
    let snap = with_icons(&export(&graph()), Some(json!([1, 2, 3])));
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &snap[..]).unwrap();
    assert_eq!(dst.icon_style(), None);
}

// ---- HTTP ------------------------------------------------------------------

fn app(store: GraphStore) -> (axum::Router, Arc<tokio::sync::RwLock<GraphStore>>) {
    let store = Arc::new(tokio::sync::RwLock::new(store));
    let state = samyama::http::server::AppState {
        store: store.clone(),
        engine: Arc::new(QueryEngine::new()),
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
            "/api/icons",
            axum::routing::get(samyama::http::icons::get_icons_handler)
                .put(samyama::http::icons::put_icons_handler),
        )
        .route(
            "/api/snapshot/import",
            axum::routing::post(samyama::http::handler::restore_snapshot_handler),
        )
        .with_state(state);
    (app, store)
}

async fn call(app: &axum::Router, method: &str, uri: &str, body: Vec<u8>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if !body.is_empty() {
        req = req.header("content-type", "application/json");
    }
    let res = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn post_snapshot(app: &axum::Router, bytes: &[u8]) -> (StatusCode, Value) {
    let boundary = "icons-test-boundary";
    let mut body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
         filename=\"g.sgsnap\"\r\nContent-Type: application/octet-stream\r\n\r\n"
    )
    .into_bytes();
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/snapshot/import")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn get_without_a_style_answers_empty_maps() {
    let (app, _) = app(graph());
    let (status, body) = call(&app, "GET", "/api/icons", vec![]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body,
        json!({"format": FORMAT, "nodes": {}, "edges": {}, "source": "none"})
    );
}

#[tokio::test]
async fn put_replaces_the_style_and_get_serves_it() {
    let (app, store) = app(graph());
    let put = json!({
        "nodes": {"Country": {"color": "#2563EB", "icon": "lucide:Flag"}, "Later": {"color": "#000000"}},
        "edges": {"IN_REGION": {"color": "#94A3B8"}}
    });
    let (status, body) = call(&app, "PUT", "/api/icons", put.to_string().into_bytes()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["source"], "api");
    assert_eq!(body["icons"]["styled"], 1);
    assert_eq!(body["icons"]["unstyled"], json!(["Disease", "Region"]));
    // A label the graph does not have yet is kept, and said to be unknown.
    assert_eq!(body["icons"]["unknown_labels"], json!(["Later"]));

    let (_, got) = call(&app, "GET", "/api/icons", vec![]).await;
    assert_eq!(got["nodes"]["Country"]["icon"], "lucide:Flag");
    assert_eq!(got["nodes"]["Later"]["color"], "#000000");
    assert_eq!(got["edges"]["IN_REGION"]["color"], "#94A3B8");

    // The next export carries it.
    let snap = export(&*store.read().await);
    assert_eq!(
        header(&snap)["icons"]["nodes"]["Country"]["color"],
        "#2563EB"
    );

    // An empty style clears it.
    let (status, body) = call(&app, "PUT", "/api/icons", b"{}".to_vec()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["source"], "none");
    assert_eq!(store.read().await.icon_style(), None);
}

#[tokio::test]
async fn put_refuses_a_style_with_any_mistake_and_changes_nothing() {
    let mut g = graph();
    g.set_icon_style(Some(country_style()), "header");
    let (app, store) = app(g);
    for bad in [
        json!({"nodes": {"Country": {"color": "blue"}}}),
        json!({"nodes": {"Country": {"icon": "<svg onload=x>"}}}),
        json!({"nodes": {"Country": {"colour": "#000000"}}}),
        json!({"format": "samyama.icons/2"}),
    ] {
        let (status, body) = call(&app, "PUT", "/api/icons", bad.to_string().into_bytes()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}: {body}");
        assert_eq!(body["code"], "invalid_style");
        assert!(!body["problems"].as_array().unwrap().is_empty(), "{body}");
    }
    let (status, body) = call(&app, "PUT", "/api/icons", b"{not json".to_vec()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "invalid_style");
    assert_eq!(store.read().await.icon_style(), Some(&country_style()));
}

#[tokio::test]
async fn put_refuses_a_style_over_the_size_limit() {
    let (app, _) = app(graph());
    let caption = "x".repeat(100);
    let nodes: serde_json::Map<String, Value> = (0..1000)
        .map(|i| (format!("L{i}"), json!({"caption": caption})))
        .collect();
    let body = json!({"nodes": nodes}).to_string().into_bytes();
    assert!(body.len() > MAX_STYLE_BYTES);
    let (status, out) = call(&app, "PUT", "/api/icons", body).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{out}");
    assert_eq!(out["code"], "style_too_large");
}

/// This build serves one graph; asking for another is refused, never answered
/// with the default graph's style.
#[tokio::test]
async fn another_graph_is_refused() {
    let mut g = graph();
    g.set_icon_style(Some(country_style()), "header");
    let (app, _) = app(g);
    let (status, _) = call(&app, "GET", "/api/icons?graph=other", vec![]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = call(&app, "PUT", "/api/icons?graph=other", b"{}".to_vec()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_snapshot_import_loads_and_reports_the_header_style() {
    let (app, _) = app(GraphStore::new());
    let mut src = graph();
    src.set_icon_style(Some(country_style()), "api");
    let (status, body) = post_snapshot(&app, &export(&src)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["icons"],
        json!({"source": "header", "styled": 2, "unstyled": ["Disease"],
               "unknown_labels": [], "unknown_edge_types": [], "warnings": []})
    );
    let (_, got) = call(&app, "GET", "/api/icons", vec![]).await;
    assert_eq!(got["source"], "header");
    assert_eq!(got["nodes"]["Region"]["color"], "#7C3AED");

    // An older snapshot clears it.
    let (_, body) = post_snapshot(&app, &export(&graph())).await;
    assert_eq!(body["icons"]["source"], "none");
    let (_, got) = call(&app, "GET", "/api/icons", vec![]).await;
    assert_eq!(got["nodes"], json!({}));
}
