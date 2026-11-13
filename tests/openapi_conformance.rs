//! Every operation in `openapi/openapi.yaml` answers as the document says (API-01, #1723).
//!
//! The document is the contract a client is generated from. An answer whose
//! status the operation does not list, or whose Content-Type the listed status
//! does not declare, breaks such a client at exactly the moment something went
//! wrong -- a 400 sent as `text/plain` to a client that parses `{"error": ...}`
//! fails on the error it was meant to report. The CH-API check in the
//! benchmarks repo found 8 such operations; nothing in this repo caught drift.
//!
//! Each operation is called through the real router (`HttpServer::router`)
//! with a well-formed request and with malformed ones, and every answer is held
//! to the document: its status must be listed for that operation, and its
//! Content-Type must be one that status declares. The operations are read from
//! the YAML, so a documented operation with no probe here fails the run rather
//! than being skipped.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::HttpServer;
use samyama::persistence::TenantManager;
use tokio::sync::RwLock;
use tower::ServiceExt;

const DOC: &str = "openapi/openapi.yaml";

/// `(METHOD, path)` -> status -> the media types that status declares.
/// A status with no `content` maps to an empty list: it promises no body type.
type Contract = BTreeMap<(String, String), BTreeMap<String, Vec<String>>>;

fn contract() -> Contract {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(DOC);
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let components = &doc["components"]["responses"];
    let mut out = Contract::new();
    for (p, ops) in doc["paths"].as_mapping().unwrap() {
        let p = p.as_str().unwrap().to_string();
        for (m, op) in ops.as_mapping().unwrap() {
            let m = m.as_str().unwrap().to_ascii_uppercase();
            if !["GET", "POST", "PUT", "PATCH", "DELETE"].contains(&m.as_str()) {
                continue;
            }
            let mut statuses = BTreeMap::new();
            for (status, resp) in op["responses"].as_mapping().unwrap() {
                let status = match status {
                    serde_yaml::Value::Number(n) => n.to_string(),
                    other => other.as_str().unwrap().to_string(),
                };
                // A `$ref` to a shared response is followed.
                let resp = match resp["$ref"].as_str() {
                    Some(r) => &components[r.rsplit('/').next().unwrap()],
                    None => resp,
                };
                let types = resp["content"]
                    .as_mapping()
                    .map(|c| c.keys().map(|k| k.as_str().unwrap().to_string()).collect())
                    .unwrap_or_default();
                statuses.insert(status, types);
            }
            out.insert((m, p.clone()), statuses);
        }
    }
    out
}

/// One request to send: a body and its Content-Type.
struct Probe {
    what: &'static str,
    path: &'static str,
    body: Option<(&'static str, Vec<u8>)>,
}

fn json(what: &'static str, path: &'static str, body: &str) -> Probe {
    Probe {
        what,
        path,
        body: Some(("application/json", body.as_bytes().to_vec())),
    }
}

fn bare(what: &'static str, path: &'static str) -> Probe {
    Probe {
        what,
        path,
        body: None,
    }
}

fn raw(what: &'static str, path: &'static str, ct: &'static str, body: &str) -> Probe {
    Probe {
        what,
        path,
        body: Some((ct, body.as_bytes().to_vec())),
    }
}

/// The requests each documented operation gets: a well-formed one where one
/// can be made without outside services, and the malformed ones a client
/// actually sends -- broken JSON, a missing required field, the wrong
/// Content-Type, an unknown id.
fn probes(method: &str, path: &str) -> Option<Vec<Probe>> {
    const BROKEN: &str = "{";
    let p = match (method, path) {
        ("POST", "/api/query") => vec![
            json("a read", "/api/query", r#"{"query": "RETURN 1 AS x"}"#),
            json("a syntax error", "/api/query", r#"{"query": "RETRUN 1"}"#),
            json("no query field", "/api/query", r#"{}"#),
            json("broken JSON", "/api/query", BROKEN),
            raw("not JSON", "/api/query", "text/plain", "RETURN 1"),
        ],
        ("POST", "/api/tx/begin") => vec![bare("begin", "/api/tx/begin")],
        ("POST", "/api/tx/{id}/commit") => vec![bare("unknown id", "/api/tx/nope/commit")],
        ("POST", "/api/tx/{id}/rollback") => vec![bare("unknown id", "/api/tx/nope/rollback")],
        ("GET", "/api/schema") => vec![bare("schema", "/api/schema")],
        ("GET", "/api/algorithms/catalog") => vec![bare("catalog", "/api/algorithms/catalog")],
        ("POST", "/api/import/csv") => vec![
            raw("not multipart", "/api/import/csv", "application/json", "{}"),
            raw(
                "multipart with no file",
                "/api/import/csv",
                "multipart/form-data; boundary=X",
                "--X--\r\n",
            ),
        ],
        ("POST", "/api/import/parquet") => vec![
            raw(
                "no label",
                "/api/import/parquet",
                "application/octet-stream",
                "PAR1",
            ),
            raw(
                "not parquet",
                "/api/import/parquet?label=Doc",
                "application/octet-stream",
                "nope",
            ),
        ],
        ("POST", "/api/query/export") => vec![
            json(
                "an export",
                "/api/query/export",
                r#"{"query": "RETURN 1 AS x"}"#,
            ),
            json(
                "unknown format",
                "/api/query/export",
                r#"{"query": "RETURN 1", "format": "xls"}"#,
            ),
            json("broken JSON", "/api/query/export", BROKEN),
        ],
        ("POST", "/api/import/json") => vec![
            json(
                "an import",
                "/api/import/json",
                r#"{"label": "P", "nodes": [{"name": "a"}]}"#,
            ),
            json("no nodes", "/api/import/json", r#"{"label": "P"}"#),
            json("broken JSON", "/api/import/json", BROKEN),
        ],
        ("GET", "/") => vec![bare("root", "/")],
        ("GET", "/optimize/algorithms") => vec![bare("list", "/optimize/algorithms")],
        ("GET", "/optimize/benchmarks") => vec![bare("list", "/optimize/benchmarks")],
        ("POST", "/optimize/solve") => vec![
            json(
                "unknown algorithm",
                "/optimize/solve",
                r#"{"algorithm": "nope"}"#,
            ),
            json("broken JSON", "/optimize/solve", BROKEN),
        ],
        ("GET", "/optimize/solve/{id}/stream") => {
            vec![bare("unknown id", "/optimize/solve/nope/stream")]
        }
        ("POST", "/optimize/solve/{id}/cancel") => {
            vec![bare("unknown id", "/optimize/solve/nope/cancel")]
        }
        ("GET", "/api/memory") => vec![bare("memory", "/api/memory")],
        ("GET", "/metrics") => vec![bare("metrics", "/metrics")],
        ("GET", "/api/status") => vec![bare("status", "/api/status")],
        ("GET", "/api/tenants") => vec![bare("list", "/api/tenants")],
        ("POST", "/api/tenants") => vec![
            json(
                "a tenant",
                "/api/tenants",
                r#"{"id": "probe", "name": "Probe"}"#,
            ),
            json(
                "the same tenant again",
                "/api/tenants",
                r#"{"id": "probe", "name": "Probe"}"#,
            ),
            json("broken JSON", "/api/tenants", BROKEN),
        ],
        ("GET", "/api/tenants/{id}") => vec![bare("unknown id", "/api/tenants/nope")],
        ("DELETE", "/api/tenants/{id}") => vec![bare("unknown id", "/api/tenants/nope")],
        ("PATCH", "/api/tenants/{id}") => vec![
            json("unknown id", "/api/tenants/nope", r#"{}"#),
            json("broken JSON", "/api/tenants/nope", BROKEN),
        ],
        ("POST", "/api/sample") => vec![
            json("a sample", "/api/sample", r#"{}"#),
            json("broken JSON", "/api/sample", BROKEN),
        ],
        ("GET", "/api/vector/indexes") => vec![bare("list", "/api/vector/indexes")],
        ("POST", "/api/vector/indexes") => vec![
            json("no fields", "/api/vector/indexes", r#"{}"#),
            json("broken JSON", "/api/vector/indexes", BROKEN),
        ],
        ("POST", "/api/vector-search") => vec![
            json("no fields", "/api/vector-search", r#"{}"#),
            json("broken JSON", "/api/vector-search", BROKEN),
        ],
        ("POST", "/api/snapshot/export") => vec![bare("export", "/api/snapshot/export")],
        ("POST", "/api/snapshot/import") => vec![raw(
            "not multipart",
            "/api/snapshot/import",
            "application/json",
            "{}",
        )],
        ("POST", "/api/enrich/policy") => vec![
            json("no fields", "/api/enrich/policy", r#"{}"#),
            json("broken JSON", "/api/enrich/policy", BROKEN),
        ],
        ("POST", "/api/enrich") => vec![
            json("no fields", "/api/enrich", r#"{}"#),
            json("broken JSON", "/api/enrich", BROKEN),
        ],
        ("POST", "/api/verify") => vec![
            json("no fields", "/api/verify", r#"{}"#),
            json("broken JSON", "/api/verify", BROKEN),
        ],
        ("POST", "/api/nlq") => vec![
            json("no fields", "/api/nlq", r#"{}"#),
            json("broken JSON", "/api/nlq", BROKEN),
        ],
        ("GET", "/api/hierarchies") => vec![bare("list", "/api/hierarchies")],
        ("POST", "/api/hierarchies/{name}/cues") => vec![
            json("unknown hierarchy", "/api/hierarchies/nope/cues", r#"{}"#),
            json("broken JSON", "/api/hierarchies/nope/cues", BROKEN),
        ],
        ("GET", "/api/structure") => vec![bare("get", "/api/structure")],
        ("PUT", "/api/structure") => vec![
            json("no fields", "/api/structure", r#"{}"#),
            json("broken JSON", "/api/structure", BROKEN),
        ],
        ("POST", "/api/structure/cues") => vec![
            json("no fields", "/api/structure/cues", r#"{}"#),
            json("broken JSON", "/api/structure/cues", BROKEN),
        ],
        _ => return None,
    };
    Some(p)
}

fn router() -> axum::Router {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    HttpServer::new(store, 0)
        .with_tenant_manager(Arc::new(TenantManager::new()))
        .router()
}

#[tokio::test]
async fn every_documented_operation_answers_with_a_status_and_type_the_document_lists() {
    let contract = contract();
    assert!(
        contract.len() >= 39,
        "read only {} operations from {DOC}",
        contract.len()
    );
    let mut wrong = Vec::new();
    let mut unprobed = Vec::new();
    let mut sent = 0;
    for ((method, path), statuses) in &contract {
        let Some(probes) = probes(method, path) else {
            unprobed.push(format!("{method} {path}"));
            continue;
        };
        // A fresh server per operation: a transaction `tx/begin` leaves open
        // would otherwise make every later write wait for its timeout.
        let app = router();
        for probe in probes {
            let mut req = Request::builder()
                .method(Method::from_bytes(method.as_bytes()).unwrap())
                .uri(probe.path);
            let body = match probe.body {
                Some((ct, bytes)) => {
                    req = req.header("content-type", ct);
                    Body::from(bytes)
                }
                None => Body::empty(),
            };
            let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
            sent += 1;
            let status = resp.status().as_u16().to_string();
            let ct = resp.headers().get("content-type").map(|v| {
                v.to_str()
                    .unwrap()
                    .split(';')
                    .next()
                    .unwrap()
                    .trim()
                    .to_string()
            });
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            let shown = String::from_utf8_lossy(&body[..body.len().min(120)]).to_string();
            let call = format!("{method} {path} ({}, sent to {})", probe.what, probe.path);
            let Some(types) = statuses.get(&status).or_else(|| statuses.get("default")) else {
                wrong.push(format!(
                    "{call}: status {status} is not documented (documents {:?}); body {shown:?}",
                    statuses.keys().collect::<Vec<_>>()
                ));
                continue;
            };
            match (&ct, types.is_empty()) {
                (_, true) => {}
                (Some(ct), false) if types.iter().any(|t| t == ct) => {}
                _ => wrong.push(format!(
                    "{call}: {status} answered Content-Type {ct:?}, the document declares \
                     {types:?}; body {shown:?}"
                )),
            }
            // Every error the document declares is an `ErrorResponse`: an
            // object with an `error` string.
            if status.starts_with('4') || status.starts_with('5') {
                let error = serde_json::from_slice::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string));
                if error.as_deref().is_none_or(str::is_empty) {
                    wrong.push(format!(
                        "{call}: {status} body is not {{\"error\": \"...\"}}: {shown:?}"
                    ));
                }
            }
        }
    }
    assert!(
        unprobed.is_empty(),
        "documented operations with no probe: {unprobed:?}"
    );
    assert!(
        wrong.is_empty(),
        "{} of {sent} answers break {DOC}:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
}

async fn call(
    app: axum::Router,
    method: &str,
    uri: &str,
    ct: Option<&str>,
    body: Vec<u8>,
) -> (u16, Option<String>, serde_json::Value) {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = ct {
        req = req.header("content-type", ct);
    }
    let resp = app
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let ct = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        ct,
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    )
}

#[tokio::test]
async fn a_request_the_server_cannot_read_is_a_json_400_that_names_the_problem() {
    // Before: 422 text/plain, 400 text/plain and 415 text/plain, one per extractor.
    let cases: [(&str, Option<&str>, &str, &str); 4] = [
        (
            "/api/query",
            Some("application/json"),
            "{}",
            "missing field `query`",
        ),
        (
            "/api/query",
            Some("application/json"),
            "{",
            "EOF while parsing",
        ),
        (
            "/api/query",
            Some("text/plain"),
            "RETURN 1",
            "Content-Type: application/json",
        ),
        (
            "/api/import/parquet",
            Some("application/octet-stream"),
            "PAR1",
            "missing field `label`",
        ),
    ];
    for (uri, ct, body, says) in cases {
        let (status, got_ct, json) = call(router(), "POST", uri, ct, body.into()).await;
        assert_eq!(status, 400, "{uri} {body:?}");
        assert_eq!(
            got_ct.as_deref(),
            Some("application/json"),
            "{uri} {body:?}"
        );
        let error = json["error"].as_str().unwrap_or_default();
        assert!(
            error.contains(says),
            "{uri} {body:?}: {error:?} should say {says:?}"
        );
    }
}

#[tokio::test]
async fn a_body_over_the_limit_keeps_413_and_is_json_too() {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let app = HttpServer::new(store, 0)
        .with_import_body_limit(16)
        .router();
    let (status, ct, json) = call(
        app,
        "POST",
        "/api/import/json",
        Some("application/json"),
        format!(
            r#"{{"label": "P", "nodes": [{{"name": "{}"}}]}}"#,
            "x".repeat(200)
        )
        .into_bytes(),
    )
    .await;
    assert_eq!(status, 413, "a size limit is not a malformed request");
    assert_eq!(ct.as_deref(), Some("application/json"));
    assert!(
        json["error"].as_str().is_some_and(|e| !e.is_empty()),
        "{json}"
    );
}

#[tokio::test]
async fn answers_that_were_already_right_are_left_alone() {
    // A handler's own JSON error keeps its status and its words.
    let (status, _, json) = call(
        router(),
        "POST",
        "/api/query",
        Some("application/json"),
        br#"{"query": "RETRUN 1"}"#.to_vec(),
    )
    .await;
    assert_eq!(status, 400);
    assert!(
        json["error"].as_str().unwrap().contains("SyntaxError"),
        "{json}"
    );
    // `/metrics` is a 200 in Prometheus's text format, and stays one.
    let resp = router()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/plain"));
}
