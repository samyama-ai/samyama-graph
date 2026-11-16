//! `POST /api/sparql` (#2100, part 3): SPARQL over the graph through the
//! shipped router, answered in the W3C SPARQL 1.1 Query Results JSON format.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::server::{read_credentials, HttpServer};
use samyama::sparql::ResultFormat;
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tower::ServiceExt;

const B: &str = "https://samyama.ai/rdf/";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn app() -> axum::Router {
    HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0).router()
}

async fn send(app: &axum::Router, req: Request<Body>) -> (StatusCode, String, Value) {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let ct = res
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    (status, ct, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn sparql(q: &str) -> Request<Body> {
    Request::post("/api/sparql")
        .header("content-type", "application/sparql-query")
        .body(Body::from(q.to_string()))
        .unwrap()
}

fn sparql_json(body: Value) -> Request<Body> {
    Request::post("/api/sparql")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn cypher(app: &axum::Router, q: &str) {
    let req = Request::post("/api/query")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": q }).to_string()))
        .unwrap();
    let (s, _, b) = send(app, req).await;
    assert_eq!(s, StatusCode::OK, "{q}: {b}");
}

/// The rows of a SELECT answer, sorted, so order does not matter.
fn bindings(doc: &Value) -> Vec<Value> {
    let mut rows: Vec<Value> = doc["results"]["bindings"].as_array().expect("bindings").clone();
    rows.sort_by_key(|r| r.to_string());
    rows
}

#[tokio::test]
async fn a_select_is_answered_in_the_w3c_json_format() {
    let app = app();
    cypher(&app, "CREATE (:Person {name: 'Alice', age: 30, score: 2.5, active: true})").await;
    let (status, ct, doc) = send(
        &app,
        sparql(&format!(
            "SELECT ?p ?name ?age ?score ?active WHERE {{ ?p a <{B}class/Person> ; <{B}prop/name> ?name ; \
             <{B}prop/age> ?age ; <{B}prop/score> ?score ; <{B}prop/active> ?active }}"
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{doc}");
    assert_eq!(ct, "application/sparql-results+json");
    assert_eq!(doc["head"]["vars"], json!(["p", "name", "age", "score", "active"]));
    let row = &bindings(&doc)[0];
    assert_eq!(row["p"]["type"], "uri");
    assert!(row["p"]["value"].as_str().unwrap().starts_with(&format!("{B}node/")));
    // A plain string carries no datatype; every other literal names its own.
    assert_eq!(row["name"], json!({ "type": "literal", "value": "Alice" }));
    assert_eq!(row["age"], json!({ "type": "literal", "value": "30", "datatype": format!("{XSD}integer") }));
    assert_eq!(row["score"]["datatype"], format!("{XSD}double"));
    assert_eq!(row["active"], json!({ "type": "literal", "value": "true", "datatype": format!("{XSD}boolean") }));
}

#[tokio::test]
async fn an_unbound_variable_is_absent_from_its_row() {
    let app = app();
    cypher(&app, "CREATE (:P {name: 'with', age: 1}), (:P {name: 'without'})").await;
    let (status, _, doc) = send(
        &app,
        sparql(&format!(
            "SELECT ?n ?a WHERE {{ ?x <{B}prop/name> ?n OPTIONAL {{ ?x <{B}prop/age> ?a }} }}"
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{doc}");
    let rows = bindings(&doc);
    assert_eq!(rows.len(), 2);
    let without = rows.iter().find(|r| r["n"]["value"] == "without").unwrap();
    assert!(without.get("a").is_none(), "unbound is absent, not null: {without}");
    let with = rows.iter().find(|r| r["n"]["value"] == "with").unwrap();
    assert_eq!(with["a"]["value"], "1");
}

#[tokio::test]
async fn ask_answers_a_boolean_document() {
    let app = app();
    cypher(&app, "CREATE (:A)-[:KNOWS]->(:B)").await;
    let (s, _, doc) = send(&app, sparql(&format!("ASK {{ ?x <{B}rel/KNOWS> ?y }}"))).await;
    assert_eq!((s, &doc), (StatusCode::OK, &json!({ "head": {}, "boolean": true })));
    let (s, _, doc) = send(&app, sparql(&format!("ASK {{ ?x <{B}rel/HATES> ?y }}"))).await;
    assert_eq!((s, &doc), (StatusCode::OK, &json!({ "head": {}, "boolean": false })));
}

#[tokio::test]
async fn the_json_body_and_a_base_of_ones_own_are_accepted() {
    let app = app();
    cypher(&app, "CREATE (:Person {name: 'Alice'})").await;
    let other = "http://example.org/g/";
    let (s, _, doc) = send(
        &app,
        sparql_json(json!({
            "query": format!("SELECT ?n WHERE {{ ?p <{other}prop/name> ?n }}"),
            "base": other,
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{doc}");
    assert_eq!(bindings(&doc)[0]["n"]["value"], "Alice");
    // Under the default base the same query names predicates nothing has.
    let (_, _, doc) = send(
        &app,
        sparql_json(json!({ "query": format!("SELECT ?n WHERE {{ ?p <{other}prop/name> ?n }}") })),
    )
    .await;
    assert_eq!(bindings(&doc).len(), 0);
}

/// Each query reads the graph as it is: a write through Cypher is in the next
/// SPARQL answer, and SPARQL itself changes nothing.
#[tokio::test]
async fn a_write_is_in_the_next_answer_and_a_query_changes_nothing() {
    let app = app();
    let count = format!("SELECT ?p WHERE {{ ?p a <{B}class/Person> }}");
    let (_, _, before) = send(&app, sparql(&count)).await;
    assert_eq!(bindings(&before).len(), 0);
    cypher(&app, "CREATE (:Person {name: 'Bob'})").await;
    let (_, _, after) = send(&app, sparql(&count)).await;
    assert_eq!(bindings(&after).len(), 1);
    for _ in 0..3 {
        send(&app, sparql("SELECT * WHERE { ?s ?p ?o }")).await;
    }
    let (_, _, again) = send(&app, sparql(&count)).await;
    assert_eq!(bindings(&again).len(), 1, "reading wrote something");
}

/// A question the engine does not answer is refused, never an empty 200.
#[tokio::test]
async fn what_is_not_answered_is_refused_not_returned_empty() {
    let app = app();
    cypher(&app, "CREATE (:A)").await;
    for (q, why) in [
        ("SELECT ?x WHERE { ?x ", "does not parse"),
        ("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }", "CONSTRUCT is not evaluated yet"),
        ("DESCRIBE <https://samyama.ai/rdf/node/1>", "DESCRIBE is not evaluated yet"),
        ("INSERT DATA { <a:s> <a:p> <a:o> }", "an UPDATE is not a query"),
    ] {
        let (s, _, doc) = send(&app, sparql(q)).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{why}: {q}: {doc}");
        assert!(doc["error"].as_str().is_some_and(|e| !e.is_empty()), "{why}: says why: {doc}");
    }
    let (s, _, _) = send(&app, sparql_json(json!({ "not_query": 1 }))).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "a body with no query");
    let (s, _, doc) = send(&app, sparql_json(json!({ "query": "ASK { ?s ?p ?o }", "graph": "other" }))).await;
    assert_eq!(s, StatusCode::BAD_REQUEST, "{doc}");
    assert!(doc["error"].as_str().unwrap().contains("single graph"));
}

#[test]
fn formats_other_than_json_are_refused_rather_than_empty() {
    let r = samyama::sparql::SparqlResults::Boolean(true);
    assert!(r.serialize(ResultFormat::Json).unwrap().contains("\"boolean\":true"));
    for f in [ResultFormat::Xml, ResultFormat::Csv, ResultFormat::Tsv] {
        assert!(r.serialize(f).is_err(), "{f:?} came back as if it were done");
    }
}

// ── credentials ────────────────────────────────────────────────────────────

fn creds(roles: &str) -> Vec<samyama::http::server::Credential> {
    use sha2::{Digest, Sha256};
    let digest: String = Sha256::digest(b"tok").iter().map(|b| format!("{b:02x}")).collect();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("creds.txt");
    std::fs::write(&path, format!("u:{digest}{roles}\n")).unwrap();
    read_credentials(&path).expect("parse")
}

fn authed(creds: Vec<samyama::http::server::Credential>) -> axum::Router {
    HttpServer::new(Arc::new(RwLock::new(GraphStore::new())), 0)
        .with_credentials(creds)
        .router()
}

#[tokio::test]
async fn a_reader_may_query_and_no_credential_may_not() {
    let app = authed(creds(":roles=read"));
    let (s, _, _) = send(&app, sparql("ASK { ?s ?p ?o }")).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "no credential");
    let mut req = sparql("ASK { ?s ?p ?o }");
    req.headers_mut().insert("authorization", "Bearer tok".parse().unwrap());
    let (s, _, doc) = send(&app, req).await;
    assert_eq!(s, StatusCode::OK, "{doc}");
    assert_eq!(doc["boolean"], false);
}

#[tokio::test]
async fn a_credential_bound_to_another_tenant_is_refused() {
    let app = authed(creds(":roles=read:tenant=elsewhere"));
    let mut req = sparql("ASK { ?s ?p ?o }");
    req.headers_mut().insert("authorization", "Bearer tok".parse().unwrap());
    let (s, _, doc) = send(&app, req).await;
    assert_eq!(s, StatusCode::FORBIDDEN, "{doc}");
}
