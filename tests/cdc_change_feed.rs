//! The change feed (#2025): `GET /api/cdc/events` and what writes it.
//!
//! Every write here is made the way a client makes it, through
//! `HttpServer::router()` or through `apply_mutations` as the RESP path does,
//! and every read goes through the route, so what is asserted is what a
//! consumer would see.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tower::ServiceExt;

struct Server {
    pm: Arc<PersistenceManager>,
    app: axum::Router,
}

fn server(dir: &std::path::Path, retention: u64) -> Server {
    let pm = Arc::new(PersistenceManager::new(dir).unwrap());
    pm.set_cdc_retention(retention);
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
        .with_persistence(Arc::clone(&pm))
        .router();
    Server { pm, app }
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn post(app: &axum::Router, uri: &str, body: Value) -> Value {
    let req = Request::post(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let (status, b) = send(app, req).await;
    assert_eq!(status, StatusCode::OK, "{uri}: {b}");
    b
}

async fn query(app: &axum::Router, q: &str) {
    post(app, "/api/query", json!({ "query": q })).await;
}

async fn events_at(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    send(app, Request::get(uri).body(Body::empty()).unwrap()).await
}

/// Every event after `after`, read a page at a time.
async fn all_after(app: &axum::Router, after: u64) -> Vec<Value> {
    let mut out = Vec::new();
    let mut next = after;
    loop {
        let (status, b) = events_at(app, &format!("/api/cdc/events?after={next}&limit=2")).await;
        assert_eq!(status, StatusCode::OK, "{b}");
        let page = b["events"].as_array().unwrap().clone();
        next = b["next"].as_u64().unwrap();
        let more = b["more"].as_bool().unwrap();
        out.extend(page);
        if !more {
            return out;
        }
    }
}

fn ops(events: &[Value]) -> Vec<&str> {
    events.iter().map(|e| e["op"].as_str().unwrap()).collect()
}

#[tokio::test]
async fn each_kind_of_change_is_an_event_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 1000);

    query(&s.app, "CREATE (a:Person {name: 'ada', age: 36})").await;
    query(&s.app, "MATCH (a:Person) SET a.age = 37, a.city = 'london'").await;
    query(&s.app, "MATCH (a:Person) REMOVE a.city").await;
    query(&s.app, "MATCH (a:Person) SET a:Engineer").await;
    query(&s.app, "MATCH (a:Person) REMOVE a:Engineer").await;
    query(
        &s.app,
        "MATCH (a:Person) CREATE (a)-[:KNOWS {since: 1843}]->(:Person {name: 'charles'})",
    )
    .await;
    query(&s.app, "MATCH ()-[r:KNOWS]->() SET r.since = 1842").await;
    query(&s.app, "MATCH ()-[r:KNOWS]->() DELETE r").await;
    query(&s.app, "MATCH (c:Person {name: 'charles'}) DELETE c").await;

    let ev = all_after(&s.app, 0).await;
    assert_eq!(
        ops(&ev),
        [
            "node_created",
            "property_set",
            "property_set",
            "property_removed",
            "label_added",
            "label_removed",
            "node_created",
            "edge_created",
            "property_set",
            "edge_deleted",
            "node_deleted",
        ],
        "{ev:#?}"
    );
    // Offsets count from 1 with no gaps.
    let offsets: Vec<u64> = ev.iter().map(|e| e["offset"].as_u64().unwrap()).collect();
    assert_eq!(offsets, (1..=ev.len() as u64).collect::<Vec<_>>());

    assert_eq!(ev[0]["labels"], json!(["Person"]));
    assert_eq!(ev[0]["properties"]["name"], "ada");
    assert_eq!(
        (ev[1]["key"].as_str(), &ev[1]["old"], &ev[1]["value"]),
        (Some("age"), &json!(36), &json!(37))
    );
    assert_eq!(
        (ev[2]["key"].as_str(), &ev[2]["old"]),
        (Some("city"), &Value::Null)
    );
    assert_eq!(ev[3]["old"], "london");
    assert_eq!(ev[4]["label"], "Engineer");
    assert_eq!(ev[7]["type"], "KNOWS");
    assert_eq!(ev[8]["entity"], "edge");
    assert_eq!(
        ev[10]["properties"]["name"], "charles",
        "a delete carries what was deleted"
    );

    // One statement is one transaction, and its last event says so.
    assert_eq!(ev[1]["tx"], ev[2]["tx"]);
    assert_ne!(ev[2]["tx"], ev[3]["tx"]);
    assert_eq!(ev[1]["commit"], Value::Null);
    assert_eq!(ev[2]["commit"], true);
    assert_eq!(ev[6]["tx"], ev[7]["tx"], "the node and edge of one CREATE");
    for e in &ev {
        assert_eq!(e["tenant"], "default");
        assert!(e["ts"].as_i64().unwrap() > 0);
    }
}

#[tokio::test]
async fn a_rolled_back_transaction_emits_nothing_and_a_committed_one_emits_once() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 1000);
    query(&s.app, "CREATE (:Seed)").await;
    let before = all_after(&s.app, 0).await.len();

    let tx = post(&s.app, "/api/tx/begin", json!({})).await["tx"]
        .as_str()
        .unwrap()
        .to_string();
    post(
        &s.app,
        "/api/query",
        json!({ "query": "CREATE (:Ghost {n: 1})", "tx": tx }),
    )
    .await;
    post(
        &s.app,
        "/api/query",
        json!({ "query": "MATCH (s:Seed) SET s.x = 1", "tx": tx }),
    )
    .await;
    post(&s.app, &format!("/api/tx/{tx}/rollback"), json!({})).await;
    assert_eq!(
        all_after(&s.app, 0).await.len(),
        before,
        "a rollback reached the feed"
    );

    let tx = post(&s.app, "/api/tx/begin", json!({})).await["tx"]
        .as_str()
        .unwrap()
        .to_string();
    post(
        &s.app,
        "/api/query",
        json!({ "query": "CREATE (:Kept)", "tx": tx }),
    )
    .await;
    post(
        &s.app,
        "/api/query",
        json!({ "query": "MATCH (s:Seed) SET s.x = 2", "tx": tx }),
    )
    .await;
    post(&s.app, &format!("/api/tx/{tx}/commit"), json!({})).await;
    let ev = all_after(&s.app, before as u64).await;
    assert_eq!(ops(&ev), ["property_set", "node_created"], "{ev:#?}");
    assert_eq!(
        ev[0]["tx"], ev[1]["tx"],
        "a transaction's events share its number"
    );
}

#[tokio::test]
async fn tenants_have_their_own_feeds_and_a_bound_credential_reads_only_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 1000);
    query(&s.app, "CREATE (:InDefault)").await;

    // Another tenant's write, persisted the way GRAPH.QUERY persists it.
    let mut other = GraphStore::new();
    other.enable_write_log();
    QueryEngine::new()
        .execute_mut("CREATE (:InAcme)-[:R]->(:InAcme)", &mut other, "acme")
        .unwrap();
    let log = other.take_write_log();
    s.pm.apply_mutations("acme", &other, &log).unwrap();

    let (_, d) = events_at(&s.app, "/api/cdc/events").await;
    assert_eq!(ops(d["events"].as_array().unwrap()), ["node_created"]);
    let (_, a) = events_at(&s.app, "/api/cdc/events?graph=acme").await;
    let acme = a["events"].as_array().unwrap();
    assert_eq!(ops(acme), ["node_created", "node_created", "edge_created"]);
    assert_eq!(acme[0]["offset"], 1, "each tenant counts its own offsets");
    assert!(acme.iter().all(|e| e["tenant"] == "acme"));

    // A credential bound to `acme` reads acme and nothing else.
    let token = "acme-reader-token";
    let digest: String = {
        use sha2::{Digest, Sha256};
        Sha256::digest(token.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    let cred = samyama::auth::Credential::parse(&format!("bob:{digest}:tenant=acme:roles=read"))
        .unwrap()
        .unwrap();
    let app = HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
        .with_persistence(Arc::clone(&s.pm))
        .with_credentials(vec![cred])
        .router();
    let get = |uri: &str| {
        Request::get(uri)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };
    let (status, _) = send(&app, get("/api/cdc/events")).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "the default tenant's feed");
    let (status, b) = send(&app, get("/api/cdc/events?graph=acme")).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    assert_eq!(b["events"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn offsets_survive_a_restart_and_carry_on_from_where_they_were() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = server(dir.path(), 1000);
        query(&s.app, "CREATE (:A {n: 1})").await;
        query(&s.app, "CREATE (:A {n: 2})").await;
        s.pm.checkpoint().unwrap();
    }
    let s = server(dir.path(), 1000);
    let ev = all_after(&s.app, 0).await;
    assert_eq!(
        ev.len(),
        2,
        "the events did not survive the restart: {ev:#?}"
    );
    assert_eq!(ev[1]["properties"]["n"], 2);

    query(&s.app, "CREATE (:A {n: 3})").await;
    let ev = all_after(&s.app, 2).await;
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0]["offset"], 3, "the offset restarted");
    assert_eq!(ev[0]["tx"], 3, "the transaction number restarted");
}

#[tokio::test]
async fn a_trimmed_offset_is_reported_with_the_oldest_still_held() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 3);
    for n in 0..5 {
        query(&s.app, &format!("CREATE (:A {{n: {n}}})")).await;
    }
    let (status, b) = events_at(&s.app, "/api/cdc/events?after=0").await;
    assert_eq!(status, StatusCode::GONE, "{b}");
    assert_eq!(b["oldest"], 3);
    assert_eq!(b["latest"], 5);
    assert!(b["error"].as_str().unwrap().contains("trimmed"));

    // From just before the oldest held, the rest is all there.
    let (status, b) = events_at(&s.app, "/api/cdc/events?after=2").await;
    assert_eq!(status, StatusCode::OK, "{b}");
    let offsets: Vec<u64> = b["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["offset"].as_u64().unwrap())
        .collect();
    assert_eq!(offsets, [3, 4, 5]);
}

#[tokio::test]
async fn the_feed_is_off_unless_a_retention_is_set() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 0);
    query(&s.app, "CREATE (:A)").await;
    let (status, b) = events_at(&s.app, "/api/cdc/events").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{b}");
    assert!(b["error"].as_str().unwrap().contains("--cdc-retention"));

    // And a write made while it was off is not in it once it is on.
    s.pm.set_cdc_retention(10);
    let (status, b) = events_at(&s.app, "/api/cdc/events").await;
    assert_eq!(status, StatusCode::OK, "{b}");
    assert!(b["events"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn bad_parameters_are_a_json_400() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 10);
    for uri in [
        "/api/cdc/events?after=x",
        "/api/cdc/events?limit=0",
        "/api/cdc/events?wait=-1",
    ] {
        let (status, b) = events_at(&s.app, uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(b["error"].is_string(), "{uri}: {b}");
    }
}

#[tokio::test]
async fn a_waiting_reader_is_answered_by_the_next_commit() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 10);
    let app = s.app.clone();
    let reader =
        tokio::spawn(async move { events_at(&app, "/api/cdc/events?after=0&wait=30").await });
    // Let the reader reach its wait; the outcome does not depend on it doing so.
    tokio::task::yield_now().await;
    query(&s.app, "CREATE (:Woke)").await;
    let (status, b) = reader.await.unwrap();
    assert_eq!(status, StatusCode::OK, "{b}");
    assert_eq!(ops(b["events"].as_array().unwrap()), ["node_created"]);
}

#[tokio::test]
async fn dropping_a_graph_keeps_its_offsets_and_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let s = server(dir.path(), 100);
    query(&s.app, "CREATE (:A)").await;
    query(&s.app, "CREATE (:A)").await;
    s.pm.drop_graph("default").unwrap();
    let (status, b) = events_at(&s.app, "/api/cdc/events?after=0").await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "the dropped graph's events are gone: {b}"
    );
    let ev = all_after(&s.app, 2).await;
    assert_eq!(ops(&ev), ["graph_dropped"]);
    assert_eq!(ev[0]["offset"], 3);
}
