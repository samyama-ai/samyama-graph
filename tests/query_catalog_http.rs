//! A graph's query catalog over HTTP (#2026).
//!
//! Every call goes through the real router (`HttpServer::router`): load a
//! catalog, list and search it, read one entry, run it with bound values, and
//! each refusal with the code and fields a client marks its form from. The
//! snapshot import's side -- a linked sidecar checked against the header, and
//! the old catalog dropped -- is at the end.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, Label, PropertyMap, PropertyValue};
use samyama::http::server::HttpServer;
use samyama::snapshot::verify::{build_catalog, queries_sha256, QuerySpec};
use serde_json::{json, Value as J};
use tokio::sync::RwLock;
use tower::ServiceExt;

/// Twenty `:C` nodes: `iso` cycles through four countries, `n` is 0..20.
fn graph() -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..20i64 {
        let mut p = PropertyMap::new();
        p.insert("n".into(), PropertyValue::Integer(i));
        p.insert(
            "iso".into(),
            PropertyValue::String(["NG", "KE", "GH", "IN"][(i % 4) as usize].into()),
        );
        s.create_node_with_properties("default", vec![Label::new("C")], p);
    }
    s
}

fn specs() -> Vec<QuerySpec> {
    serde_json::from_value(json!([
        {"id": "count", "question": "How many countries are recorded?",
         "difficulty": "easy", "cypher": "MATCH (c:C) RETURN count(c) AS n"},
        {"id": "by_iso", "question": "Cases in a country",
         "paraphrases": ["outbreaks by region"], "difficulty": "medium",
         "cypher": "MATCH (c:C) WHERE c.iso = $iso RETURN c.n AS n ORDER BY n",
         "params": [{"name": "iso", "type": "string", "sample": "NG",
                     "enum_values": ["NG", "KE", "GH", "IN"]}]},
        {"id": "above", "question": "Cases above a threshold", "difficulty": "hard",
         "cypher": "MATCH (c:C) WHERE c.n > $min RETURN c.n AS n ORDER BY n",
         "params": [{"name": "min", "type": "int", "sample": 15}]}
    ]))
    .unwrap()
}

/// The catalog built against `graph()`, as the bytes a client uploads.
fn catalog_bytes() -> Vec<u8> {
    serde_json::to_vec(&build_catalog(&graph(), &specs(), &[]).unwrap()).unwrap()
}

fn router() -> axum::Router {
    HttpServer::new(Arc::new(RwLock::new(graph())), 0).router()
}

async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    ct: Option<String>,
    body: Vec<u8>,
) -> (StatusCode, J) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = ct {
        req = req.header("content-type", ct);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(J::Null))
}

/// A multipart body of `(field name, bytes)` parts.
fn multipart(parts: &[(&str, &[u8])]) -> (String, Vec<u8>) {
    let mut body = Vec::new();
    for (name, bytes) in parts {
        body.extend_from_slice(
            format!(
                "--B\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{name}.bin\"\r\n\
                 Content-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(b"--B--\r\n");
    ("multipart/form-data; boundary=B".into(), body)
}

async fn import(app: &axum::Router, bytes: &[u8]) -> (StatusCode, J) {
    let (ct, body) = multipart(&[("file", bytes)]);
    send(app, Method::POST, "/api/queries/import", Some(ct), body).await
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, J) {
    send(app, Method::GET, uri, None, Vec::new()).await
}

async fn run(app: &axum::Router, id: &str, body: J) -> (StatusCode, J) {
    send(
        app,
        Method::POST,
        &format!("/api/queries/{id}/run"),
        Some("application/json".into()),
        serde_json::to_vec(&body).unwrap(),
    )
    .await
}

async fn loaded() -> axum::Router {
    let app = router();
    let (status, body) = import(&app, &catalog_bytes()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    app
}

#[tokio::test]
async fn import_reports_what_it_loaded() {
    let app = router();
    let bytes = catalog_bytes();
    let (status, body) = import(&app, &bytes).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["loaded"], 3);
    assert_eq!(body["replaced"], false);
    assert_eq!(body["sha256"], queries_sha256(&bytes));
    assert_eq!(
        body["by_difficulty"],
        json!({"easy": 1, "medium": 1, "hard": 1})
    );
    assert_eq!(body["with_params"], 2);
    let (_, again) = import(&app, &bytes).await;
    assert_eq!(again["replaced"], true);
}

#[tokio::test]
async fn before_a_catalog_is_loaded_every_read_is_no_catalog() {
    let app = router();
    for uri in ["/api/queries", "/api/queries/count"] {
        let (status, body) = get(&app, uri).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["code"], "no_catalog", "{uri}");
    }
    let (status, body) = run(&app, "count", json!({})).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "no_catalog");
}

#[tokio::test]
async fn the_list_carries_params_and_searches_every_word() {
    let app = loaded().await;
    let (status, body) = get(&app, "/api/queries").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 3);
    let by_iso = &body["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["id"] == "by_iso")
        .unwrap();
    assert_eq!(by_iso["params"][0]["name"], "iso");
    assert_eq!(by_iso["params"][0]["type"], "string");
    assert_eq!(
        by_iso["params"][0]["enum_values"],
        json!(["NG", "KE", "GH", "IN"])
    );
    assert!(
        by_iso.get("cypher").is_none(),
        "the list leaves the query to GET :id"
    );

    let ids = |b: &J| -> Vec<String> {
        b["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["id"].as_str().unwrap().to_string())
            .collect()
    };
    // A paraphrase matches; every word must appear.
    assert_eq!(
        ids(&get(&app, "/api/queries?q=outbreaks%20REGION").await.1),
        ["by_iso"]
    );
    assert!(ids(&get(&app, "/api/queries?q=outbreaks%20threshold").await.1).is_empty());
    assert_eq!(
        ids(&get(&app, "/api/queries?difficulty=hard").await.1),
        ["above"]
    );
}

#[tokio::test]
async fn one_entry_carries_its_query_and_ceiling() {
    let app = loaded().await;
    let (status, body) = get(&app, "/api/queries/above").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["cypher"].as_str().unwrap().contains("$min"));
    assert!(body["work"].as_u64().is_some());
    assert!(body["work_ceiling"].as_u64().unwrap() >= body["work"].as_u64().unwrap());
    let (status, body) = get(&app, "/api/queries/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "query_not_found");
}

#[tokio::test]
async fn a_run_binds_the_values_given() {
    let app = loaded().await;
    let (status, body) = run(&app, "by_iso", json!({"params": {"iso": "KE"}})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["columns"], json!(["n"]));
    assert_eq!(body["rows"], json!([[1], [5], [9], [13], [17]]));
    assert_eq!(body["row_count"], 5);
    assert_eq!(body["truncated"], false);
    assert_eq!(body["source"], "computed");

    let (status, body) = run(&app, "count", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"], json!([[20]]));
}

/// The CLI fills an omitted value from the sample; the API refuses it, since
/// the sample answers a question the caller did not ask.
#[tokio::test]
async fn an_omitted_value_is_param_missing_not_the_sample() {
    let app = loaded().await;
    let (status, body) = run(&app, "by_iso", json!({"params": {}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "param_missing");
    assert_eq!(body["param"], "iso");
}

#[tokio::test]
async fn each_parameter_refusal_names_its_field() {
    let app = loaded().await;

    let (status, body) = run(&app, "by_iso", json!({"params": {"iso": "KE", "limit": 3}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "param_unknown");
    assert_eq!(body["param"], "limit");
    assert_eq!(body["declared"], json!(["iso"]));

    let (status, body) = run(&app, "above", json!({"params": {"min": "15"}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "param_type");
    assert_eq!(body["param"], "min");
    assert_eq!(body["expected"], "int");
    assert_eq!(body["got"], "string");
    // The message is about the caller's value, not the sample (which is an int).
    let msg = body["error"].as_str().unwrap();
    assert!(
        msg.contains("\"15\"") && msg.contains("value given"),
        "{msg}"
    );

    let (status, body) = run(&app, "by_iso", json!({"params": {"iso": "FR"}})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "param_not_allowed");
    assert_eq!(body["param"], "iso");
    assert_eq!(body["allowed"], json!(["NG", "KE", "GH", "IN"]));
}

#[tokio::test]
async fn a_run_past_the_work_ceiling_is_422() {
    // `work` recorded as 0 against a template that scans: the floor still
    // admits 10,000 rows, so the graph has to be bigger than that.
    let mut s = GraphStore::new();
    for i in 0..12_000i64 {
        let mut p = PropertyMap::new();
        p.insert("n".into(), PropertyValue::Integer(i));
        s.create_node_with_properties("default", vec![Label::new("C")], p);
    }
    let app = HttpServer::new(Arc::new(RwLock::new(s)), 0).router();
    let catalog = json!({
        "format": "samyama.queries/1", "generated_by": "test", "provenance": "authored",
        "entries": [{"id": "scan", "question": "All above", "rows": 1, "hash": "h", "work": 0,
                     "cypher": "MATCH (c:C) WHERE c.n > $min RETURN c.n AS n",
                     "params": [{"name": "min", "type": "int", "sample": 11999}]}]
    });
    let (status, body) = import(&app, &serde_json::to_vec(&catalog).unwrap()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = run(&app, "scan", json!({"params": {"min": -1}})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["code"], "work_ceiling");
    assert_eq!(body["limit"], 10_000);
    assert_eq!(body["params"], json!({"min": -1}));
}

#[tokio::test]
async fn a_catalog_with_a_writing_entry_is_refused_whole() {
    let app = loaded().await;
    let mut catalog: J = serde_json::from_slice(&catalog_bytes()).unwrap();
    catalog["entries"].as_array_mut().unwrap().push(json!({
        "id": "wipe", "question": "Wipe", "rows": 0, "hash": "h", "work": 1,
        "cypher": "MATCH (c:C) DETACH DELETE c"
    }));
    let (status, body) = import(&app, &serde_json::to_vec(&catalog).unwrap()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["code"], "invalid_catalog");
    assert!(body["error"].as_str().unwrap().contains("wipe"), "{body}");
    // The catalog already loaded is untouched.
    assert_eq!(get(&app, "/api/queries").await.1["total"], 3);
}

#[tokio::test]
async fn another_graph_is_refused_as_api_query_refuses_it() {
    let app = loaded().await;
    let (status, _) = get(&app, "/api/queries?graph=other").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = run(&app, "count", json!({"graph": "other"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

/// A snapshot of `graph()` whose header links `catalog` by its SHA-256.
fn linked_snapshot(catalog: &[u8]) -> Vec<u8> {
    let mut plain = Vec::new();
    samyama::snapshot::export_tenant(&graph(), &mut plain).unwrap();
    let mut out = Vec::new();
    let link = json!({"file": "graph.sgqueries", "sha256": queries_sha256(catalog)});
    samyama::snapshot::edit_header(&plain[..], &mut out, |h| {
        h.insert("queries".into(), link);
        Ok(())
    })
    .unwrap();
    out
}

async fn import_snapshot(app: &axum::Router, parts: &[(&str, &[u8])]) -> (StatusCode, J) {
    let (ct, body) = multipart(parts);
    send(app, Method::POST, "/api/snapshot/import", Some(ct), body).await
}

#[tokio::test]
async fn a_snapshot_import_loads_the_linked_catalog() {
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router();
    let catalog = catalog_bytes();
    let snap = linked_snapshot(&catalog);
    let (status, body) = import_snapshot(&app, &[("file", &snap), ("queries", &catalog)]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["queries"],
        json!({"linked": true, "expected_file": "graph.sgqueries",
               "sha256": queries_sha256(&catalog), "loaded": true})
    );
    assert_eq!(get(&app, "/api/queries").await.1["total"], 3);
}

#[tokio::test]
async fn a_sidecar_that_is_not_the_linked_catalog_is_409_and_writes_nothing() {
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router();
    let catalog = catalog_bytes();
    let snap = linked_snapshot(&catalog);
    let mut other = catalog.clone();
    other.push(b' ');
    let (status, body) = import_snapshot(&app, &[("file", &snap), ("queries", &other)]).await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["code"], "catalog_mismatch");
    assert_eq!(body["expected"], queries_sha256(&catalog));
    assert_eq!(body["got"], queries_sha256(&other));
    let (_, q) = send(
        &app,
        Method::POST,
        "/api/query",
        Some("application/json".into()),
        br#"{"query": "MATCH (n) RETURN count(n) AS c"}"#.to_vec(),
    )
    .await;
    assert_eq!(q["records"][0][0], 0, "{q}");
}

/// Importing replaces the graph's data, so questions about the old graph go.
/// Without the sidecar, the response says the client should ask for it.
#[tokio::test]
async fn a_snapshot_import_drops_the_previous_catalog() {
    let app = loaded().await;
    let snap = linked_snapshot(&catalog_bytes());
    let (status, body) = import_snapshot(&app, &[("file", &snap)]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["queries"]["linked"], true);
    assert_eq!(body["queries"]["loaded"], false);
    assert_eq!(get(&app, "/api/queries").await.1["code"], "no_catalog");
}
