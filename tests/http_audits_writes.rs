//! Every state-changing request is recorded (REL-08).
//!
//! REL-08 asks for "users, roles, per-graph and per-label permissions, audit
//! log of every write and admin action". The audit log is one of those four;
//! the other three are not here.
//!
//! # Selected by method, not by a list of routes
//!
//! `POST`, `PUT`, `PATCH` and `DELETE` are recorded; `GET`, `HEAD` and
//! `OPTIONS` are not. A list of write routes is a list somebody forgets to
//! extend — the same reason the credential layer exempts nothing — and a new
//! endpoint is audited the day it is added rather than the day someone
//! remembers it exists.
//!
//! The consequence, asserted below rather than hidden: a **read** submitted as
//! `POST /api/query` is recorded too. Telling it apart means parsing Cypher in
//! a middleware, and a log that occasionally over-records is worth more than
//! one that occasionally misses a write.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use samyama::graph::GraphStore;
use samyama::http::server::{read_credentials, AuditLog, Credential, HttpServer};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

const TOKEN: &str = "s3cret-token";

fn digest_of(token: &str) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(token.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

fn tmp(name: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static N: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!("samyama-audit-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir.join(format!("{name}-{}.txt", N.fetch_add(1, Ordering::Relaxed)))
}

fn credentials(name: &str, token: &str) -> Vec<Credential> {
    let path = tmp("creds");
    std::fs::write(&path, format!("{name}:{}\n", digest_of(token))).expect("write");
    read_credentials(&path).expect("parse")
}

struct Fixture {
    app: axum::Router,
    log_path: std::path::PathBuf,
}

fn fixture(creds: Vec<Credential>) -> Fixture {
    let log_path = tmp("audit");
    let log = Arc::new(AuditLog::open(&log_path).expect("open audit log"));
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0)
        .with_credentials(creds)
        .with_audit_log(log)
        .router();
    Fixture { app, log_path }
}

fn entries(path: &std::path::Path) -> Vec<serde_json::Value> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap_or_else(|e| panic!("line is not JSON: {l:?} ({e})")))
        .collect()
}

fn get(path: &str, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method("GET").uri(path);
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::empty()).expect("request")
}

fn post_query(cypher: &str, bearer: Option<&str>) -> Request<Body> {
    let mut b = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header("content-type", "application/json");
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Body::from(
        serde_json::json!({ "query": cypher }).to_string(),
    ))
    .expect("request")
}

#[tokio::test]
async fn a_write_is_recorded_with_the_subject_that_made_it() {
    let f = fixture(credentials("ops", TOKEN));
    let res = f
        .app
        .clone()
        .oneshot(post_query("CREATE (:N {n: 1})", Some(TOKEN)))
        .await
        .expect("response");
    assert!(res.status().is_success(), "the write itself failed: {}", res.status());

    let rows = entries(&f.log_path);
    assert_eq!(rows.len(), 1, "expected one entry, got {rows:?}");
    let e = &rows[0];
    assert_eq!(e["subject"], "ops", "the entry names the credential, not the token");
    assert_eq!(e["method"], "POST");
    assert_eq!(e["path"], "/api/query");
    assert_eq!(e["status"], 200);
    assert!(
        e["at"].as_str().is_some_and(|t| t.contains('T')),
        "an entry needs a timestamp, got {:?}",
        e["at"]
    );
}

#[tokio::test]
async fn the_token_never_appears_in_the_log() {
    // The entry is built from the credential that matched, not from the header
    // that was presented, so there is no path by which a secret reaches the
    // file. Asserted because this is the failure that would be discovered by
    // somebody else reading the log.
    let f = fixture(credentials("ops", TOKEN));
    let _ = f.app.clone().oneshot(post_query("CREATE (:N)", Some(TOKEN))).await;
    let text = std::fs::read_to_string(&f.log_path).unwrap_or_default();
    assert!(!text.contains(TOKEN), "the log contains the bearer token:\n{text}");
    assert!(!text.contains("Bearer"), "the log contains the header:\n{text}");
}

#[tokio::test]
async fn a_read_is_not_recorded() {
    let f = fixture(credentials("ops", TOKEN));
    for path in ["/api/status", "/api/schema", "/metrics"] {
        let _ = f.app.clone().oneshot(get(path, Some(TOKEN))).await;
    }
    assert!(
        entries(&f.log_path).is_empty(),
        "GET requests should not be audited: {:?}",
        entries(&f.log_path)
    );
}

#[tokio::test]
async fn a_refused_request_is_recorded() {
    // A 401 on a write route is exactly the entry an audit log exists for, so
    // the layer sits outside the credential check rather than inside it.
    let f = fixture(credentials("ops", TOKEN));
    let res = f
        .app
        .clone()
        .oneshot(post_query("MATCH (n) DETACH DELETE n", None))
        .await
        .expect("response");
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);

    let rows = entries(&f.log_path);
    assert_eq!(rows.len(), 1, "the refusal should still be recorded");
    assert_eq!(rows[0]["status"], 401);
    assert_eq!(
        rows[0]["subject"], "unauthenticated",
        "nothing authenticated, so there is no subject to name"
    );
}

#[tokio::test]
async fn a_read_submitted_as_a_post_is_recorded_too() {
    // Stated rather than hidden. Telling this apart means parsing Cypher in a
    // middleware; over-recording is the safe direction.
    let f = fixture(credentials("ops", TOKEN));
    let _ = f.app.clone().oneshot(post_query("MATCH (n) RETURN count(n)", Some(TOKEN))).await;
    let rows = entries(&f.log_path);
    assert_eq!(rows.len(), 1, "a POST is recorded whether or not it wrote");
}

#[tokio::test]
async fn entries_append_rather_than_replace() {
    // One line per request, in order. A log that truncated would lose exactly
    // the history it exists to keep.
    let f = fixture(credentials("ops", TOKEN));
    for i in 0..5 {
        let _ = f
            .app
            .clone()
            .oneshot(post_query(&format!("CREATE (:N {{n: {i}}})"), Some(TOKEN)))
            .await;
    }
    let rows = entries(&f.log_path);
    assert_eq!(rows.len(), 5, "expected five entries, got {}", rows.len());
    assert!(rows.iter().all(|e| e["subject"] == "ops"));
}

#[tokio::test]
async fn without_a_log_nothing_is_written_and_the_api_still_works() {
    // The default. An audit log written to a path nobody chose is how a disk
    // fills up on a machine that was working yesterday.
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0).router();
    let res = app
        .oneshot(post_query("CREATE (:N {n: 1})", None))
        .await
        .expect("response");
    assert!(
        res.status().is_success(),
        "an unaudited server must behave exactly as before: {}",
        res.status()
    );
}

#[test]
fn an_unopenable_log_is_an_error() {
    // `--audit-log` pointing somewhere unwritable stops the server rather than
    // running unaudited for an operator who asked to be audited.
    assert!(AuditLog::open("/proc/nonexistent-dir/audit.log").is_err());
}
