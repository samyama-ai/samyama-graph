//! The HTTP handlers' refusals, formats and edge cases, driven in-process
//! through a router built from an `AppState` the test can inspect.

use super::*;
use crate::graph::{GraphStore, NodeId};
use crate::query::QueryEngine;
use axum::{
    body::Body,
    http::Request,
    routing::{get, post},
    Router,
};
use http_body_util::BodyExt;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

/// sha256("test").
const D: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

fn state() -> AppState {
    AppState {
        store: Arc::new(RwLock::new(GraphStore::new())),
        engine: Arc::new(QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(RwLock::new(HashMap::new())),
        persistence: None,
        snapshot_key: None,
        transactions: Default::default(),
    }
}

/// A state backed by a real persistence manager whose default tenant has
/// room for `max_nodes` nodes and `max_edges` edges.
fn state_with_quota(
    max_nodes: usize,
    max_edges: usize,
) -> (
    AppState,
    Arc<crate::persistence::PersistenceManager>,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().unwrap();
    let pm = Arc::new(crate::persistence::PersistenceManager::new(dir.path()).unwrap());
    pm.tenants()
        .update_quotas(
            "default",
            crate::persistence::ResourceQuotas {
                max_nodes: Some(max_nodes),
                max_edges: Some(max_edges),
                ..crate::persistence::ResourceQuotas::unlimited()
            },
        )
        .unwrap();
    let mut s = state();
    s.persistence = Some(Arc::clone(&pm));
    (s, pm, dir)
}

fn routes(state: AppState) -> Router {
    Router::new()
        .route("/api/query", post(query_handler))
        .route("/api/query/export", post(export_handler))
        .route(
            "/api/tx/begin",
            post(crate::http::transactions::begin_handler),
        )
        .route(
            "/api/tx/:id/commit",
            post(crate::http::transactions::commit_handler),
        )
        .route(
            "/api/tx/:id/rollback",
            post(crate::http::transactions::rollback_handler),
        )
        .route("/api/import/parquet", post(import_parquet_handler))
        .route("/api/import/csv", post(import_csv_handler))
        .route("/api/import/json", post(import_json_handler))
        .route("/api/enrich/policy", post(set_enrich_policy_handler))
        .route("/api/enrich", post(enrich_handler))
        .route("/api/verify", post(verify_handler))
        .route("/api/nlq", post(nlq_handler))
        .route("/api/status", get(status_handler))
        .route("/api/memory", get(memory_handler))
        .route("/metrics", get(metrics_handler))
        .route("/api/schema", get(schema_handler))
        .route("/api/sample", post(sample_handler))
        .route("/api/snapshot/export", post(export_snapshot_handler))
        .route("/api/snapshot/import", post(restore_snapshot_handler))
        .with_state(state)
}

/// The same routes, with every request carrying `subject` as the credential
/// layer would have put it there.
fn routes_as(state: AppState, subject: &str) -> Router {
    let cred = crate::auth::Credential::parse(subject).unwrap().unwrap();
    routes(state).layer(axum::Extension(Subject(cred)))
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    bytes: bytes::Bytes,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.bytes)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {}", String::from_utf8_lossy(&self.bytes)))
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes).into_owned()
    }
    fn header(&self, name: &str) -> String {
        self.headers
            .get(name)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default()
    }
}

async fn send(app: Router, req: Request<Body>) -> Reply {
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    Reply {
        status,
        headers,
        bytes,
    }
}

async fn post_json(app: Router, uri: &str, body: serde_json::Value) -> Reply {
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}

async fn get_uri(app: Router, uri: &str) -> Reply {
    send(
        app,
        Request::builder().uri(uri).body(Body::empty()).unwrap(),
    )
    .await
}

/// One multipart part: a text field, or a file when `filename` is set.
struct Part<'a> {
    name: &'a str,
    filename: Option<&'a str>,
    data: Vec<u8>,
}

fn field<'a>(name: &'a str, value: &str) -> Part<'a> {
    Part {
        name,
        filename: None,
        data: value.as_bytes().to_vec(),
    }
}

fn file(data: impl Into<Vec<u8>>) -> Part<'static> {
    Part {
        name: "file",
        filename: Some("upload.bin"),
        data: data.into(),
    }
}

async fn post_multipart(app: Router, uri: &str, parts: Vec<Part<'_>>) -> Reply {
    const B: &str = "COV-BOUNDARY";
    let mut body = Vec::new();
    for p in parts {
        body.extend_from_slice(format!("--{B}\r\n").as_bytes());
        match p.filename {
            Some(f) => body.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"{}\"; filename=\"{f}\"\r\n\r\n",
                    p.name
                )
                .as_bytes(),
            ),
            None => body.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"{}\"\r\n\r\n",
                    p.name
                )
                .as_bytes(),
            ),
        }
        body.extend_from_slice(&p.data);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{B}--\r\n").as_bytes());
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", format!("multipart/form-data; boundary={B}"))
            .body(Body::from(body))
            .unwrap(),
    )
    .await
}

async fn run(state: &AppState, query: &str) -> serde_json::Value {
    let r = post_json(
        routes(state.clone()),
        "/api/query",
        json!({ "query": query }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{query}: {}", r.text());
    r.json()
}

// ---------- pure helpers ----------

#[test]
fn csv_cells_become_the_value_they_most_plausibly_are() {
    assert_eq!(csv_cell_value(""), None);
    assert_eq!(csv_cell_value("   "), None);
    assert_eq!(csv_cell_value(" 42 "), Some(PropertyValue::Integer(42)));
    assert_eq!(csv_cell_value("2.5"), Some(PropertyValue::Float(2.5)));
    assert_eq!(csv_cell_value("TRUE"), Some(PropertyValue::Boolean(true)));
    assert_eq!(csv_cell_value("False"), Some(PropertyValue::Boolean(false)));
    assert_eq!(
        csv_cell_value(" Pune "),
        Some(PropertyValue::String("Pune".into()))
    );
}

#[test]
fn json_scalars_convert_and_everything_else_is_skipped() {
    assert_eq!(
        json_scalar_value(&json!("s")),
        Some(PropertyValue::String("s".into()))
    );
    assert_eq!(
        json_scalar_value(&json!(7)),
        Some(PropertyValue::Integer(7))
    );
    assert_eq!(
        json_scalar_value(&json!(1.5)),
        Some(PropertyValue::Float(1.5))
    );
    assert_eq!(
        json_scalar_value(&json!(u64::MAX)),
        Some(PropertyValue::Float(u64::MAX as f64)),
        "a u64 beyond i64 is kept as a float"
    );
    assert_eq!(
        json_scalar_value(&json!(true)),
        Some(PropertyValue::Boolean(true))
    );
    assert_eq!(json_scalar_value(&json!(null)), None);
    assert_eq!(json_scalar_value(&json!([1])), None);
    assert_eq!(json_scalar_value(&json!({"a": 1})), None);
}

#[test]
fn endpoint_keys_render_scalars_as_text() {
    assert_eq!(
        endpoint_key_text(&PropertyValue::String("a".into())).as_deref(),
        Some("a")
    );
    assert_eq!(
        endpoint_key_text(&PropertyValue::Integer(101)).as_deref(),
        Some("101")
    );
    assert_eq!(
        endpoint_key_text(&PropertyValue::Float(1.5)).as_deref(),
        Some("1.5")
    );
    assert_eq!(
        endpoint_key_text(&PropertyValue::Boolean(true)).as_deref(),
        Some("true")
    );
    assert_eq!(endpoint_key_text(&PropertyValue::Null), None);
}

#[test]
fn bool_fields_are_true_unless_they_say_otherwise() {
    for f in ["false", "0", "no", " OFF "] {
        assert!(!parse_bool_field(f), "{f}");
    }
    for t in ["true", "1", "yes", "anything"] {
        assert!(parse_bool_field(t), "{t}");
    }
}

#[test]
fn import_modes_parse_with_synonyms_and_refuse_the_rest() {
    assert_eq!(parse_import_mode("").unwrap(), ImportMode::Node);
    assert_eq!(parse_import_mode(" Nodes ").unwrap(), ImportMode::Node);
    assert_eq!(parse_import_mode("EDGES").unwrap(), ImportMode::Edge);
    let r = parse_import_mode("triples").unwrap_err();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn accept_negotiation_finds_ndjson_among_several_types() {
    let mut h = axum::http::HeaderMap::new();
    assert!(!accepts_ndjson(&h));
    h.insert("accept", "application/json".parse().unwrap());
    assert!(!accepts_ndjson(&h));
    h.insert(
        "accept",
        "text/html, Application/X-NDJSON;q=0.9".parse().unwrap(),
    );
    assert!(accepts_ndjson(&h));
}

#[test]
fn edge_specs_fall_back_to_the_column_name_for_the_key_property() {
    let mut spec = EdgeImportSpec {
        source_key_col: " src ".into(),
        target_key_col: "dst".into(),
        ..Default::default()
    };
    assert_eq!(spec.source_prop(), "src");
    assert_eq!(spec.target_prop(), "dst");
    spec.source_key_prop = "id".into();
    spec.target_key_prop = " id ".into();
    assert_eq!(spec.source_prop(), "id");
    assert_eq!(spec.target_prop(), "id");
}

#[test]
fn edge_import_notes_are_capped() {
    let mut stats = EdgeImportStats::default();
    for i in 0..(MAX_REPORTED_ERRORS + 5) {
        stats.note(format!("e{i}"));
    }
    assert_eq!(stats.errors.len(), MAX_REPORTED_ERRORS);
    assert_eq!(stats.errors[0], "e0");
}

#[test]
fn render_row_shapes_every_kind_of_value() {
    let mut node = crate::graph::Node::new(NodeId(3), "Person");
    node.add_label("Admin");
    node.set_property("name", "ada");
    let mut edge = crate::graph::Edge::new(crate::graph::EdgeId(9), NodeId(3), NodeId(4), "KNOWS");
    edge.set_property("since", 2020i64);

    let mut record = crate::query::Record::new();
    record.bind("n", Value::Node(NodeId(3), Box::new(node)));
    record.bind("r", Value::NodeRef(NodeId(4)));
    record.bind("e", Value::Edge(crate::graph::EdgeId(9), Box::new(edge)));
    record.bind(
        "er",
        Value::EdgeRef(
            crate::graph::EdgeId(10),
            NodeId(4),
            NodeId(3),
            "LIKES".into(),
        ),
    );
    record.bind(
        "p",
        Value::Path {
            nodes: vec![NodeId(3), NodeId(4)],
            edges: vec![crate::graph::EdgeId(9)],
        },
    );
    record.bind(
        "l",
        Value::List(vec![Value::Property(PropertyValue::Integer(1))]),
    );
    record.bind(
        "m",
        Value::Map(
            [("k".to_string(), Value::Property(PropertyValue::Integer(2)))]
                .into_iter()
                .collect(),
        ),
    );
    record.bind("x", Value::Property(PropertyValue::Float(0.5)));
    let columns: Vec<String> = ["n", "r", "e", "er", "p", "l", "m", "x", "missing"]
        .iter()
        .map(|s| s.to_string())
        .collect();

    let (mut nodes, mut edges) = (HashMap::new(), HashMap::new());
    let row = render_row(&record, &columns, &HashMap::new(), &mut nodes, &mut edges);

    // No merged view for node 3, so its own properties are used.
    assert_eq!(row[0]["id"], "3");
    assert_eq!(
        row[0]["labels"],
        json!(["Admin", "Person"]),
        "labels are sorted"
    );
    assert_eq!(row[0]["properties"]["name"], "ada");
    assert_eq!(row[1], json!({ "id": "4", "labels": [], "properties": {} }));
    assert_eq!(row[2]["type"], "KNOWS");
    assert_eq!(row[2]["source"], "3");
    assert_eq!(row[2]["properties"]["since"], 2020);
    assert_eq!(
        row[3],
        json!({ "id": "10", "source": "4", "target": "3", "type": "LIKES", "properties": {} })
    );
    assert_eq!(
        row[4],
        json!({ "nodes": ["3", "4"], "edges": ["9"], "length": 1 })
    );
    assert_eq!(row[5].as_array().unwrap().len(), 1);
    assert!(row[6]["k"].as_str().unwrap().contains('2'), "{}", row[6]);
    assert_eq!(row[7], json!(0.5));
    assert!(row[8].is_null(), "a missing column renders as null");
    assert_eq!(nodes.len(), 2);
    assert_eq!(edges.len(), 2);

    // With a merged view, it wins over the value's own properties.
    let merged: HashMap<u64, HashMap<String, PropertyValue>> = [(
        3u64,
        [("name".to_string(), PropertyValue::String("merged".into()))]
            .into_iter()
            .collect(),
    )]
    .into_iter()
    .collect();
    let row = render_row(&record, &columns[..1], &merged, &mut nodes, &mut edges);
    assert_eq!(row[0]["properties"]["name"], "merged");
}

// ---------- /api/query ----------

#[tokio::test]
async fn a_credential_bound_to_another_tenant_cannot_query() {
    let app = routes_as(state(), &format!("b:{D}:tenant=acme"));
    let r = post_json(app, "/api/query", json!({ "query": "RETURN 1" })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("bound to tenant 'acme'"));
}

#[tokio::test]
async fn a_parameter_the_query_never_uses_is_a_400() {
    let r = post_json(
        routes(state()),
        "/api/query",
        json!({ "query": "RETURN 1", "params": { "unused": 1 } }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.json()["error"].as_str().unwrap().contains("unused"),
        "{}",
        r.text()
    );
}

#[tokio::test]
async fn a_bound_parameter_is_returned_as_its_value() {
    let r = post_json(
        routes(state()),
        "/api/query",
        json!({ "query": "RETURN $name AS name", "params": { "name": "ada" } }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["records"], json!([["ada"]]));
}

#[tokio::test]
async fn a_cached_read_is_served_from_the_cache_the_second_time() {
    let s = state();
    run(&s, "CREATE (:C {v: 1})").await;
    let body = json!({ "query": "MATCH (c:C) RETURN c.v AS v", "cache": true });
    let first = post_json(routes(s.clone()), "/api/query", body.clone())
        .await
        .json();
    let second = post_json(routes(s.clone()), "/api/query", body)
        .await
        .json();
    assert_eq!(first["cached"], false);
    assert_eq!(second["cached"], true);
    assert_eq!(first["records"], second["records"]);
    let bad = post_json(
        routes(s),
        "/api/query",
        json!({ "query": "MATCH (c RETURN c", "cache": true }),
    )
    .await;
    assert_eq!(bad.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_deleted_node_is_rendered_from_the_value_it_carried() {
    let s = state();
    run(&s, "CREATE (:Gone {name: 'x'})").await;
    let json = run(&s, "MATCH (g:Gone) DELETE g RETURN g").await;
    assert_eq!(json["records"].as_array().unwrap().len(), 1, "{json}");
    assert_eq!(s.store.read().await.node_count(), 0);
}

// ---------- streaming ----------

async fn stream(app: Router, body: serde_json::Value) -> Reply {
    send(
        app,
        Request::builder()
            .method("POST")
            .uri("/api/query")
            .header("content-type", "application/json")
            .header("accept", NDJSON)
            .body(Body::from(body.to_string()))
            .unwrap(),
    )
    .await
}

fn ndjson_lines(r: &Reply) -> Vec<serde_json::Value> {
    r.text()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn a_streamed_read_has_a_header_rows_and_a_done_trailer() {
    let s = state();
    run(&s, "CREATE (:S {i: 1}), (:S {i: 2})").await;
    let r = stream(
        routes(s),
        json!({ "query": "MATCH (n:S) RETURN n.i AS i, n ORDER BY i" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.header("content-type"), NDJSON);
    let lines = ndjson_lines(&r);
    assert_eq!(lines[0]["columns"], json!(["i", "n"]));
    assert!(lines[0]["snapshot_version"].is_u64());
    assert_eq!(lines[1]["row"][0], 1);
    assert_eq!(
        lines[1]["row"][1]["properties"]["i"], 1,
        "nodes are rendered with properties"
    );
    assert_eq!(lines[2]["row"][0], 2);
    let trailer = lines.last().unwrap();
    assert_eq!(trailer["done"], true);
    assert_eq!(trailer["rows"], 2);
    assert_eq!(lines.len(), 4);
}

#[tokio::test]
async fn an_empty_streamed_result_still_has_a_header_and_trailer() {
    let r = stream(
        routes(state()),
        json!({ "query": "MATCH (n:None) RETURN n" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    let lines = ndjson_lines(&r);
    assert_eq!(lines.len(), 2, "{}", r.text());
    assert_eq!(lines[0]["columns"], json!(["n"]));
    assert_eq!(lines[1]["done"], true);
    assert_eq!(lines[1]["rows"], 0);
}

#[tokio::test]
async fn a_stream_that_fails_before_its_first_row_is_a_400() {
    let r = stream(routes(state()), json!({ "query": "MATCH (n RETURN n" })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"].is_string());
}

#[tokio::test]
async fn writes_and_transactions_are_not_streamed() {
    let s = state();
    let r = stream(routes(s.clone()), json!({ "query": "CREATE (:X)" })).await;
    assert_eq!(r.status, StatusCode::NOT_ACCEPTABLE);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .starts_with("A write cannot be streamed"));
    assert_eq!(s.store.read().await.node_count(), 0);
    let r = stream(routes(s), json!({ "query": "RETURN 1", "tx": "t" })).await;
    assert_eq!(r.status, StatusCode::NOT_ACCEPTABLE);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .starts_with("A statement inside a transaction cannot be streamed"));
}

#[tokio::test]
async fn a_stream_spanning_several_chunks_keeps_every_row() {
    let r = stream(
        routes(state()),
        json!({ "query": "UNWIND range(1, 600) AS x RETURN x" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    let lines = ndjson_lines(&r);
    assert_eq!(lines.len(), 602, "header + 600 rows + trailer");
    assert_eq!(lines[600]["row"][0], 600);
    assert_eq!(lines[601]["rows"], 600);
}

// ---------- transactions over /api/query ----------

async fn begin(s: &AppState) -> String {
    let r = post_json(routes(s.clone()), "/api/tx/begin", json!({})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    r.json()["tx"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn statements_in_a_transaction_see_its_writes_and_commit_keeps_them() {
    let s = state();
    let tx = begin(&s).await;
    let w = post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "CREATE (:T {v: 1})", "tx": tx }),
    )
    .await;
    assert_eq!(w.status, StatusCode::OK, "{}", w.text());
    let r = post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "MATCH (t:T) RETURN t.v AS v", "tx": tx }),
    )
    .await;
    assert_eq!(r.json()["records"], json!([[1]]));
    let c = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/commit"),
        json!({}),
    )
    .await;
    assert_eq!(c.status, StatusCode::OK);
    assert_eq!(c.json()["committed"], true);
    assert_eq!(s.store.read().await.node_count(), 1);
}

#[tokio::test]
async fn a_statement_for_an_unknown_transaction_is_404() {
    let r = post_json(
        routes(state()),
        "/api/query",
        json!({ "query": "RETURN 1", "tx": "nope" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("no open transaction 'nope'"));
}

#[tokio::test]
async fn a_transaction_past_its_deadline_is_rolled_back_on_next_use() {
    let s = state();
    let tx = begin(&s).await;
    post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "CREATE (:Late)", "tx": tx }),
    )
    .await;
    s.transactions.lock().await.get_mut(&tx).unwrap().deadline =
        std::time::Instant::now() - std::time::Duration::from_secs(1);

    let r = post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "RETURN 1", "tx": tx }),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("was rolled back"));
    assert_eq!(
        s.store.read().await.node_count(),
        0,
        "the expired write survived"
    );
    // The tombstone answers a late commit the same way.
    let c = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/commit"),
        json!({}),
    )
    .await;
    assert_eq!(c.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_commit_past_the_deadline_rolls_back_instead() {
    let s = state();
    let tx = begin(&s).await;
    post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "CREATE (:Late)", "tx": tx }),
    )
    .await;
    s.transactions.lock().await.get_mut(&tx).unwrap().deadline =
        std::time::Instant::now() - std::time::Duration::from_secs(1);
    let c = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/commit"),
        json!({}),
    )
    .await;
    assert_eq!(c.status, StatusCode::CONFLICT, "{}", c.text());
    assert_eq!(s.store.read().await.node_count(), 0);
}

#[tokio::test]
async fn a_persistent_transaction_reaches_disk_at_commit() {
    let (s, pm, _dir) = state_with_quota(100, 100);
    let tx = begin(&s).await;
    post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "CREATE (:D), (:D)", "tx": tx }),
    )
    .await;
    let c = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/commit"),
        json!({}),
    )
    .await;
    assert_eq!(c.status, StatusCode::OK, "{}", c.text());
    pm.checkpoint().unwrap();
    assert_eq!(pm.recover("default").unwrap().0.len(), 2);
}

#[tokio::test]
async fn a_commit_whose_store_transaction_is_gone_is_a_conflict() {
    let s = state();
    let tx = begin(&s).await;
    // End the store's transaction behind the session's back.
    s.transactions
        .lock()
        .await
        .get_mut(&tx)
        .unwrap()
        .guard
        .rollback_session_transaction()
        .unwrap();
    let c = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/commit"),
        json!({}),
    )
    .await;
    assert_eq!(c.status, StatusCode::CONFLICT, "{}", c.text());
}

#[tokio::test]
async fn begin_on_a_store_already_in_a_transaction_is_a_conflict() {
    let s = state();
    s.store.write().await.begin_session_transaction().unwrap();
    let r = post_json(routes(s.clone()), "/api/tx/begin", json!({})).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    assert!(s.transactions.lock().await.get_mut("anything").is_none());
}

#[tokio::test]
async fn a_rollback_whose_store_transaction_is_gone_still_releases_the_session() {
    let s = state();
    let tx = begin(&s).await;
    s.transactions
        .lock()
        .await
        .get_mut(&tx)
        .unwrap()
        .guard
        .rollback_session_transaction()
        .unwrap();
    let r = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["rolled_back"], true);
    // The lock was released: a plain query runs.
    run(&s, "RETURN 1").await;
}

#[tokio::test]
async fn expire_rolls_back_an_open_transaction_and_leaves_a_tombstone() {
    let s = state();
    let tx = begin(&s).await;
    post_json(
        routes(s.clone()),
        "/api/query",
        json!({ "query": "CREATE (:E)", "tx": tx }),
    )
    .await;
    crate::http::transactions::expire(&s.transactions, &tx, std::time::Duration::from_secs(5))
        .await;
    assert_eq!(s.store.read().await.node_count(), 0);
    let r = post_json(
        routes(s.clone()),
        &format!("/api/tx/{tx}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("longer than 5s"));
    // Expiring it again is a no-op.
    crate::http::transactions::expire(&s.transactions, &tx, std::time::Duration::from_secs(5))
        .await;
}

// ---------- /api/query/export ----------

#[tokio::test]
async fn export_refuses_foreign_graphs_unknown_formats_and_bad_queries() {
    let app = routes(state());
    let r = post_json(
        app.clone(),
        "/api/query/export",
        json!({ "query": "RETURN 1", "graph": "g2" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("'g2' does not exist"));
    let r = post_json(
        app.clone(),
        "/api/query/export",
        json!({ "query": "RETURN 1", "format": "xlsx" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        r.json()["error"],
        "unknown format 'xlsx'; use 'arrow', 'parquet' or 'csv'"
    );
    let r = post_json(
        app,
        "/api/query/export",
        json!({ "query": "MATCH (n RETURN n" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn export_is_held_to_the_credentials_tenant() {
    let app = routes_as(state(), &format!("b:{D}:tenant=acme"));
    let r = post_json(app, "/api/query/export", json!({ "query": "RETURN 1" })).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn export_answers_in_each_format_with_its_content_type() {
    let s = state();
    run(&s, "CREATE (:E {name: 'a', n: 1}), (:E {name: 'b', n: 2})").await;
    let q = "MATCH (e:E) RETURN e.name AS name, e.n AS n ORDER BY n";
    let app = routes(s);

    let csv = post_json(
        app.clone(),
        "/api/query/export",
        json!({ "query": q, "format": "CSV" }),
    )
    .await;
    assert_eq!(csv.status, StatusCode::OK);
    assert!(csv.header("content-type").starts_with("text/csv"));
    assert!(csv.header("content-disposition").contains("result.csv"));
    assert!(!csv.header("x-samyama-export-report").is_empty());
    assert_eq!(csv.text().lines().next().unwrap(), "name,n");

    let pq = post_json(
        app.clone(),
        "/api/query/export",
        json!({ "query": q, "format": "parquet" }),
    )
    .await;
    assert_eq!(pq.status, StatusCode::OK);
    assert_eq!(pq.header("content-type"), "application/vnd.apache.parquet");
    assert_eq!(&pq.bytes[..4], b"PAR1");

    let arrow = post_json(app, "/api/query/export", json!({ "query": q })).await;
    assert_eq!(arrow.status, StatusCode::OK);
    assert_eq!(
        arrow.header("content-type"),
        "application/vnd.apache.arrow.stream"
    );
    assert!(arrow
        .header("content-disposition")
        .contains("result.arrows"));
}

// ---------- /api/import/parquet ----------

async fn parquet_of(query: &str) -> Vec<u8> {
    let s = state();
    run(
        &s,
        "CREATE (:Src {title: 'x', n: 1}), (:Src {title: 'y', n: 2})",
    )
    .await;
    let r = post_json(
        routes(s),
        "/api/query/export",
        json!({ "query": query, "format": "parquet" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    r.bytes.to_vec()
}

#[tokio::test]
async fn a_parquet_file_imports_one_node_per_row() {
    let data = parquet_of("MATCH (s:Src) RETURN s.title AS title, s.n AS n").await;
    let s = state();
    let r = post_multipart(
        routes(s.clone()),
        "/api/import/parquet?label=Doc",
        vec![file(data)],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["status"], "ok");
    let json = run(&s, "MATCH (d:Doc) RETURN d.title AS t ORDER BY t").await;
    assert_eq!(json["records"], json!([["x"], ["y"]]));
}

#[tokio::test]
async fn parquet_import_refusals() {
    let app = routes(state());
    let r = post_multipart(
        app.clone(),
        "/api/import/parquet?label=Doc&graph=g2",
        vec![],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("'g2' does not exist"));
    let r = post_multipart(app.clone(), "/api/import/parquet?label=%20", vec![]).await;
    assert_eq!(r.json()["error"], "label must not be empty");
    let r = post_multipart(
        app.clone(),
        "/api/import/parquet?label=Doc",
        vec![field("other", "x")],
    )
    .await;
    assert_eq!(
        r.json()["error"],
        "no `file` field in the multipart request"
    );
    let r = post_multipart(
        app,
        "/api/import/parquet?label=Doc",
        vec![file(b"not parquet".to_vec())],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_parquet_import_past_the_quota_is_429_and_writes_nothing() {
    let data = parquet_of("MATCH (s:Src) RETURN s.title AS title").await;
    let (s, _pm, _dir) = state_with_quota(1, 0);
    let r = post_multipart(
        routes(s.clone()),
        "/api/import/parquet?label=Doc",
        vec![file(data)],
    )
    .await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS, "{}", r.text());
    assert!(r.json()["stats"].is_null());
    assert_eq!(s.store.read().await.node_count(), 0);
}

// ---------- /api/import/csv ----------

#[tokio::test]
async fn csv_import_refusals_name_what_is_wrong() {
    let app = routes(state());
    let r = post_multipart(
        app.clone(),
        "/api/import/csv",
        vec![field("label", "P"), field("graph", "g2"), file("a\n1\n")],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = post_multipart(
        app.clone(),
        "/api/import/csv",
        vec![
            field("label", "P"),
            field("import_mode", "rows"),
            file("a\n1\n"),
        ],
    )
    .await;
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("unknown import_mode 'rows'"));
    let r = post_multipart(app.clone(), "/api/import/csv", vec![field("label", "P")]).await;
    assert_eq!(r.json()["error"], "No file field in multipart request");
    let r = post_multipart(app.clone(), "/api/import/csv", vec![file("a\n1\n")]).await;
    assert_eq!(r.json()["error"], "Missing 'label' field");
    let r = post_multipart(
        app.clone(),
        "/api/import/csv",
        vec![field("label", "P"), file("")],
    )
    .await;
    assert_eq!(r.json()["error"], "Empty CSV file");
}

#[tokio::test]
async fn csv_import_honours_the_delimiter_and_id_column() {
    let s = state();
    let r = post_multipart(
        routes(s.clone()),
        "/api/import/csv",
        vec![
            field("label", "P"),
            field("delimiter", ";"),
            field("id_column", "id"),
            field("unknown_field", "ignored"),
            file("id;name;score\n1;ada;2.5\n2;grace;\n"),
        ],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert_eq!(j["nodes_created"], 2);
    assert_eq!(j["columns"], json!(["id", "name", "score"]));
    let rows = run(
        &s,
        "MATCH (p:P) RETURN p.name AS n, p.score AS s ORDER BY n",
    )
    .await;
    assert_eq!(
        rows["records"],
        json!([["ada", 2.5], ["grace", null]]),
        "an empty cell is unset"
    );
}

#[tokio::test]
async fn a_csv_node_import_past_the_quota_is_429() {
    let (s, _pm, _dir) = state_with_quota(1, 0);
    let r = post_multipart(
        routes(s.clone()),
        "/api/import/csv",
        vec![field("label", "P"), file("a\n1\n2\n")],
    )
    .await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r.json()["nodes_created"], 0);
    assert_eq!(s.store.read().await.node_count(), 0);
}

fn edge_fields<'a>() -> Vec<Part<'a>> {
    vec![
        field("import_mode", "edge"),
        field("source_label", "P"),
        field("source_key_col", "from"),
        field("source_key_prop", "id"),
        field("target_label", "P"),
        field("target_key_col", "to"),
        field("target_key_prop", "id"),
        field("edge_type", "KNOWS"),
    ]
}

async fn people(s: &AppState, ids: &[i64]) {
    for id in ids {
        run(s, &format!("CREATE (:P {{id: {id}}})")).await;
    }
}

#[tokio::test]
async fn a_csv_edge_import_names_its_missing_fields_and_columns() {
    let app = routes(state());
    let r = post_multipart(
        app.clone(),
        "/api/import/csv",
        vec![
            field("import_mode", "edge"),
            field("edge_type", "R"),
            file("a,b\n1,2\n"),
        ],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        r.json()["error"],
        "edge import is missing: source_label, source_key_col, target_label, target_key_col"
    );
    let mut parts = edge_fields();
    parts.push(file("x,y\n1,2\n"));
    let r = post_multipart(app, "/api/import/csv", parts).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("must name both source_key_col 'from'"));
}

#[tokio::test]
async fn a_csv_edge_import_between_nodes_of_one_label_carries_properties() {
    let s = state();
    people(&s, &[1, 2, 3]).await;
    let mut parts = edge_fields();
    parts.push(file("from,to,since\n1,2,2020\n2,3,\n"));
    let r = post_multipart(routes(s.clone()), "/api/import/csv", parts).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert_eq!(j["created"], 2);
    assert_eq!(j["import_mode"], "edge");
    assert_eq!(j["strict"], true);
    let rows = run(
        &s,
        "MATCH (a:P)-[k:KNOWS]->(b:P) RETURN a.id, b.id, k.since ORDER BY a.id",
    )
    .await;
    assert_eq!(rows["records"], json!([[1, 2, 2020], [2, 3, null]]));
}

#[tokio::test]
async fn a_non_strict_edge_import_skips_and_counts_bad_records() {
    let s = state();
    people(&s, &[1, 2]).await;
    run(&s, "CREATE (:P {id: 2})").await; // node 2 is now ambiguous
    let mut parts = edge_fields();
    parts.push(field("strict", "false"));
    parts.push(file("from,to\n1,9\n1,2\n,1\n"));
    let r = post_multipart(routes(s.clone()), "/api/import/csv", parts).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert_eq!(j["strict"], false);
    assert_eq!(j["processed"], 3);
    assert_eq!(j["created"], 0);
    assert_eq!(j["skipped"], 3);
    assert_eq!(j["missing_targets"], 1);
    assert_eq!(j["ambiguous"], 1);
    assert_eq!(j["missing_sources"], 1);
    assert_eq!(j["errors"].as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn an_edge_import_past_the_edge_quota_is_429() {
    let (s, _pm, _dir) = state_with_quota(100, 0);
    let mut parts = edge_fields();
    parts.push(file("from,to\n1,2\n"));
    let r = post_multipart(routes(s.clone()), "/api/import/csv", parts).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r.json()["created"], 0);

    let r = post_json(
        routes(s),
        "/api/import/json",
        json!({
            "import_mode": "edge", "edges": [{"from": 1, "to": 2}],
            "source_label": "P", "source_key_col": "from", "source_key_prop": "id",
            "target_label": "P", "target_key_col": "to", "target_key_prop": "id",
            "edge_type": "KNOWS"
        }),
    )
    .await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
}

// ---------- /api/import/json ----------

#[tokio::test]
async fn json_import_refusals_and_quota() {
    let r = post_json(
        routes(state()),
        "/api/import/json",
        json!({ "import_mode": "bulk" }),
    )
    .await;
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("unknown import_mode 'bulk'"));
    let r = post_json(
        routes(state()),
        "/api/import/json",
        json!({ "nodes": [{}] }),
    )
    .await;
    assert_eq!(r.json()["error"], "Missing 'label' field");
    let r = post_json(
        routes(state()),
        "/api/import/json",
        json!({ "import_mode": "edge", "edge_type": "R" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .starts_with("edge import is missing"));

    let (s, _pm, _dir) = state_with_quota(1, 0);
    let r = post_json(
        routes(s.clone()),
        "/api/import/json",
        json!({ "label": "N", "nodes": [{}, {}] }),
    )
    .await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(s.store.read().await.node_count(), 0);
}

#[tokio::test]
async fn json_import_keeps_scalars_and_drops_structures() {
    let s = state();
    let r = post_json(
        routes(s.clone()),
        "/api/import/json",
        json!({ "label": "N", "nodes": [{ "a": 1, "b": "x", "c": [1, 2], "d": null }, "not an object"] }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["nodes_created"], 2);
    let rows = run(&s, "MATCH (n:N) WHERE n.a = 1 RETURN keys(n) AS k").await;
    let mut keys: Vec<String> = serde_json::from_value(rows["records"][0][0].clone()).unwrap();
    keys.sort();
    assert_eq!(keys, vec!["a", "b"]);
}

#[tokio::test]
async fn a_json_edge_import_with_a_strict_refusal_writes_nothing() {
    let s = state();
    people(&s, &[1]).await;
    let r = post_json(
        routes(s.clone()),
        "/api/import/json",
        json!({
            "import_mode": "edge", "edges": [{"from": 1, "to": 2, "w": 1.5}],
            "source_label": "P", "source_key_col": "from", "source_key_prop": "id",
            "target_label": "P", "target_key_col": "to", "target_key_prop": "id",
            "edge_type": "KNOWS"
        }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let j = r.json();
    assert_eq!(j["status"], "error");
    assert!(j["error"].as_str().unwrap().contains("1 of 1 records"));
    assert_eq!(s.store.read().await.edge_count(), 0);
}

// ---------- status, metrics, memory, schema, sample ----------

async fn indexed_graph() -> AppState {
    let s = state();
    run(
        &s,
        "CREATE (a:P {name: 'a', score: 1.5, ok: true, id: 1})-[:R {w: 2, tag: 'x', f: 0.5, b: false, l: [1]}]->(b:P {name: 'b', id: 2}), (:Q {title: 'q'}), (:Z {label: 'z'}), (:Y {n: 7})",
    )
    .await;
    run(&s, "CREATE INDEX ON :P(name)").await;
    run(&s, "CREATE CONSTRAINT FOR (n:P) REQUIRE n.id IS UNIQUE").await;
    s
}

#[tokio::test]
async fn metrics_expose_gauges_and_one_line_per_index() {
    let s = indexed_graph().await;
    let r = get_uri(routes(s), "/metrics").await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.header("content-type").starts_with("text/plain"));
    let t = r.text();
    assert!(
        t.contains("# TYPE samyama_nodes gauge\nsamyama_nodes 5\n"),
        "{t}"
    );
    assert!(t.contains("samyama_edges 1\n"));
    assert!(
        t.contains("samyama_index_bytes{label=\"P\",property=\"name\""),
        "{t}"
    );
    assert!(t.contains("samyama_query_cache_hits"));
}

#[tokio::test]
async fn memory_reports_bytes_per_edge_and_the_tenant_count() {
    let mut s = indexed_graph().await;
    s.tenant_manager = Some(Arc::new(crate::persistence::TenantManager::new()));
    let j = get_uri(routes(s), "/api/memory").await.json();
    assert_eq!(j["estimate"], true);
    assert_eq!(j["graph"]["edges"], 1);
    assert!(j["graph"]["bytes_per_edge"].as_f64().unwrap() > 0.0);
    assert_eq!(j["tenants"], 1);
    assert!(j["index_memory_bytes"].as_u64().unwrap() > 0);

    let empty = get_uri(routes(state()), "/api/memory").await.json();
    assert!(
        empty["graph"]["bytes_per_edge"].is_null(),
        "no edges, no ratio"
    );
    assert!(empty["tenants"].is_null());
}

#[tokio::test]
async fn schema_types_properties_and_lists_indexes_and_constraints() {
    let s = indexed_graph().await;
    let j = get_uri(routes(s), "/api/schema").await.json();
    let p = j["node_types"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["label"] == "P")
        .unwrap();
    assert_eq!(p["count"], 2);
    assert_eq!(p["properties"]["score"], "Float");
    assert_eq!(p["properties"]["ok"], "Boolean");
    assert_eq!(p["properties"]["id"], "Integer");
    assert_eq!(p["properties_sampled_from"], 2);
    let r = &j["edge_types"][0];
    assert_eq!(r["type"], "R");
    assert_eq!(r["source_labels"], json!(["P"]));
    assert_eq!(r["target_labels"], json!(["P"]));
    // The index list is not ordered, so compare it as a set.
    let indexed: std::collections::BTreeSet<(String, String, String)> = j["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| {
            (
                i["label"].as_str().unwrap().to_string(),
                i["property"].as_str().unwrap().to_string(),
                i["type"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert!(
        indexed.contains(&("P".into(), "name".into(), "BTREE".into())),
        "{indexed:?}"
    );
    assert_eq!(j["constraints"][0]["label"], "P");
    assert_eq!(j["constraints"][0]["property"], "id");
    assert_eq!(j["constraints"][0]["type"], "UNIQUE");
    assert_eq!(j["statistics"]["total_nodes"], 5);
    assert_eq!(j["statistics"]["avg_out_degree"], 0.2);
}

#[tokio::test]
async fn schema_reports_vector_and_other_property_types() {
    let s = state();
    {
        let mut st = s.store.write().await;
        let id = st.create_node("V");
        let _ = st.set_node_property(
            "default",
            id,
            "emb".to_string(),
            PropertyValue::Vector(vec![0.1, 0.2]),
        );
        let _ = st.set_node_property(
            "default",
            id,
            "tags".to_string(),
            PropertyValue::Array(vec![PropertyValue::String("a".into())]),
        );
    }
    let j = get_uri(routes(s), "/api/schema").await.json();
    let v = &j["node_types"][0];
    assert_eq!(v["properties"]["emb"], "Vector", "{j}");
    assert_eq!(v["properties"]["tags"], "Unknown");
    assert_eq!(j["statistics"]["avg_out_degree"], 0.0);
}

#[tokio::test]
async fn sample_of_an_empty_graph_is_empty() {
    let j = post_json(routes(state()), "/api/sample", json!({}))
        .await
        .json();
    assert_eq!(
        j,
        json!({ "nodes": [], "edges": [], "total_nodes": 0, "total_edges": 0 })
    );
}

#[tokio::test]
async fn sample_names_nodes_and_keeps_edges_between_sampled_ones() {
    let s = indexed_graph().await;
    let j = post_json(routes(s.clone()), "/api/sample", json!({ "max_nodes": 50 }))
        .await
        .json();
    assert_eq!(j["sampled_nodes"], 5);
    let names: std::collections::BTreeSet<String> = j["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains("a") && names.contains("q") && names.contains("z"),
        "{names:?}"
    );
    assert!(
        names.iter().any(|n| n.parse::<u64>().is_ok()),
        "a node with no name uses its id: {names:?}"
    );
    assert_eq!(j["sampled_edges"], 1);
    let e = &j["edges"][0];
    assert_eq!(e["type"], "R");
    assert_eq!(e["properties"]["w"], 2);
    assert_eq!(e["properties"]["tag"], "x");
    assert_eq!(e["properties"]["f"], 0.5);
    assert_eq!(e["properties"]["b"], false);
    assert!(
        e["properties"]["l"].is_string(),
        "a list is rendered as text"
    );

    let only_q = post_json(routes(s), "/api/sample", json!({ "labels": ["Q"] }))
        .await
        .json();
    assert_eq!(only_q["total_nodes"], 1);
    assert_eq!(only_q["nodes"][0]["label"], "Q");
    assert_eq!(only_q["sampled_edges"], 0);
}

#[tokio::test]
async fn sample_strides_across_a_large_label() {
    let s = state();
    run(
        &s,
        "UNWIND range(1, 40) AS i CREATE (:Many {name: toString(i), v: null})",
    )
    .await;
    let j = post_json(routes(s), "/api/sample", json!({ "max_nodes": 10 }))
        .await
        .json();
    assert_eq!(j["total_nodes"], 40);
    assert_eq!(j["sampled_nodes"], 10);
}

#[tokio::test]
async fn sample_renders_null_and_other_property_values() {
    let s = state();
    s.store.write().await.create_node("Raw");
    {
        let mut st = s.store.write().await;
        let id = st.get_nodes_by_label(&"Raw".into())[0].id;
        let _ = st.set_node_property("default", id, "nothing".to_string(), PropertyValue::Null);
        let _ = st.set_node_property(
            "default",
            id,
            "list".to_string(),
            PropertyValue::Array(vec![PropertyValue::Integer(1)]),
        );
        let _ = st.set_node_property("default", id, "b".to_string(), PropertyValue::Boolean(true));
        let _ = st.set_node_property("default", id, "f".to_string(), PropertyValue::Float(1.25));
    }
    let j = post_json(routes(s), "/api/sample", json!({})).await.json();
    let props = &j["nodes"][0]["properties"];
    assert_eq!(props["b"], true);
    assert_eq!(props["f"], 1.25);
    assert!(props["list"].is_string());
}

// ---------- snapshots ----------

async fn snapshot_bytes(s: &AppState) -> Vec<u8> {
    let r = send(
        routes(s.clone()),
        Request::builder()
            .method("POST")
            .uri("/api/snapshot/export")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r
        .header("content-disposition")
        .contains("snapshot.sgsnap\""));
    let dropped: serde_json::Value =
        serde_json::from_str(&r.header("x-samyama-export-dropped")).unwrap();
    // Edges lose their creation time in a snapshot, and the header says so.
    let edges = s.store.read().await.edge_count();
    if edges == 0 {
        assert_eq!(dropped, json!([]));
    } else {
        assert_eq!(dropped[0]["what"], "edge_creation_timestamps");
        assert_eq!(dropped[0]["count"], edges);
    }
    r.bytes.to_vec()
}

#[tokio::test]
async fn a_snapshot_round_trips_and_is_persisted_under_the_data_path() {
    let src = state();
    run(&src, "CREATE (:S {name: 'a'})-[:L]->(:S {name: 'b'})").await;
    let data = snapshot_bytes(&src).await;

    let dir = tempfile::tempdir().unwrap();
    let mut dst = state();
    dst.data_path = Some(dir.path().to_string_lossy().into_owned());
    let r = post_multipart(
        routes(dst.clone()),
        "/api/snapshot/import",
        vec![field("note", "x"), file(data)],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert_eq!(j["nodes_imported"], 2);
    assert_eq!(j["edges_imported"], 1);
    assert_eq!(dst.store.read().await.node_count(), 2);
    assert!(
        dir.path().join("snapshots").exists(),
        "the snapshot was not persisted"
    );
}

#[tokio::test]
async fn a_snapshot_import_deduplicates_on_the_given_keys() {
    let src = state();
    run(&src, "CREATE (:S {name: 'a'}), (:S {name: 'b'})").await;
    let data = snapshot_bytes(&src).await;
    let dst = state();
    run(&dst, "CREATE (:S {name: 'a'})").await;
    let r = post_multipart(
        routes(dst.clone()),
        "/api/snapshot/import?dedup_key=name,%20,",
        vec![file(data)],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["nodes_merged"], 1);
    assert_eq!(dst.store.read().await.node_count(), 2);
}

#[tokio::test]
async fn snapshot_import_refusals() {
    let app = routes(state());
    let r = post_multipart(app.clone(), "/api/snapshot/import", vec![field("x", "y")]).await;
    assert_eq!(r.json()["error"], "No file field in multipart request");
    let r = post_multipart(app, "/api/snapshot/import", vec![file(b"garbage".to_vec())]).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn a_snapshot_past_the_quota_is_refused_whole() {
    let src = state();
    run(&src, "CREATE (:S), (:S), (:S)").await;
    let data = snapshot_bytes(&src).await;
    let (s, _pm, _dir) = state_with_quota(2, 0);
    let r = post_multipart(routes(s.clone()), "/api/snapshot/import", vec![file(data)]).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r.json()["nodes_imported"], 0);
    assert_eq!(s.store.read().await.node_count(), 0);
}

#[tokio::test]
async fn encrypted_snapshots_need_the_key_and_refuse_dedup() {
    let key = Arc::new([3u8; crate::snapshot::encryption::KEY_BYTES]);
    let mut src = state();
    src.snapshot_key = Some(Arc::clone(&key));
    run(&src, "CREATE (:Secret {v: 1})").await;
    let r = send(
        routes(src.clone()),
        Request::builder()
            .method("POST")
            .uri("/api/snapshot/export")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert!(r.header("content-disposition").contains(".enc"));
    let data = r.bytes.to_vec();

    let r = post_multipart(
        routes(state()),
        "/api/snapshot/import",
        vec![file(data.clone())],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("no --snapshot-key"));

    let mut keyed = state();
    keyed.snapshot_key = Some(Arc::clone(&key));
    let r = post_multipart(
        routes(keyed.clone()),
        "/api/snapshot/import?dedup_key=v",
        vec![file(data.clone())],
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["error"]
        .as_str()
        .unwrap()
        .contains("not supported yet"));

    let r = post_multipart(
        routes(keyed.clone()),
        "/api/snapshot/import",
        vec![file(data)],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(keyed.store.read().await.node_count(), 1);
}

// ---------- enrichment and NLQ ----------

#[tokio::test]
async fn enrich_and_verify_report_query_errors_and_empty_work() {
    // The policy is process-global and only these handlers read it; this is
    // the only test that sets it, and it is restored to the default at the end.
    let policy = json!({ "policies": { "Asset": { "vendor": { "trust_floor": 0.5 } } } });
    let r = post_json(routes(state()), "/api/enrich/policy", policy).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["declared_properties"], 1);

    let s = state();
    let r = post_json(
        routes(s.clone()),
        "/api/enrich",
        json!({ "query": "MATCH (n RETURN n" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = post_json(
        routes(s.clone()),
        "/api/verify",
        json!({ "query": "MATCH (n RETURN n" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    run(&s, "CREATE (:Asset {name: 'pump'})").await;
    let r = post_json(
        routes(s.clone()),
        "/api/verify",
        json!({ "query": "MATCH (a:Asset) RETURN a" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["promoted"], 0);

    if std::env::var("NLQ_PROVIDER").is_err() {
        // With no provider configured the fill phase is refused rather than
        // sending the node's values to a default third party.
        let r = post_json(
            routes(s.clone()),
            "/api/enrich",
            json!({ "query": "MATCH (a:Asset) RETURN a" }),
        )
        .await;
        assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR, "{}", r.text());
        assert!(
            r.json()["error"].as_str().unwrap().contains("NLQ_PROVIDER"),
            "{}",
            r.text()
        );
    }

    let r = post_json(routes(state()), "/api/enrich/policy", json!({})).await;
    assert_eq!(r.json()["declared_properties"], 0);
}

#[tokio::test]
async fn nlq_without_a_configured_provider_is_refused() {
    if std::env::var("NLQ_PROVIDER").is_ok() {
        return; // the refusal under test is for an unset provider
    }
    let r = post_json(
        routes(state()),
        "/api/nlq",
        json!({ "question": "who knows ada?" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.json()["error"].as_str().unwrap().contains("NLQ_PROVIDER"),
        "{}",
        r.text()
    );
}

// ---------- truncated uploads, odd storage, empty graphs ----------

/// A multipart body cut off inside the part named `name`, with no closing
/// boundary: what a client that dies mid-upload sends.
async fn post_truncated(app: Router, uri: &str, before: &str, name: &str) -> Reply {
    const B: &str = "TRUNC";
    let body = format!(
        "{before}--{B}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"x\"\r\n\r\nabc,def"
    );
    send(
        app,
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", format!("multipart/form-data; boundary={B}"))
            .body(Body::from(body))
            .unwrap(),
    )
    .await
}

fn label_part() -> String {
    "--TRUNC\r\nContent-Disposition: form-data; name=\"label\"\r\n\r\nP\r\n".to_string()
}

#[tokio::test]
async fn an_upload_cut_off_inside_the_file_is_a_400() {
    let s = state();
    let r = post_truncated(routes(s.clone()), "/api/import/csv", &label_part(), "file").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.json()["error"]
            .as_str()
            .unwrap()
            .starts_with("Failed to read file"),
        "{}",
        r.text()
    );

    let r = post_truncated(routes(s.clone()), "/api/snapshot/import", "", "file").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.json()["error"]
            .as_str()
            .unwrap()
            .starts_with("Failed to read file"),
        "{}",
        r.text()
    );

    let r = post_truncated(
        routes(s.clone()),
        "/api/import/parquet?label=Doc",
        "",
        "file",
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.json()["error"]
            .as_str()
            .unwrap()
            .starts_with("could not read the file"),
        "{}",
        r.text()
    );
    assert_eq!(s.store.read().await.node_count(), 0);
}

#[tokio::test]
async fn an_upload_cut_off_inside_a_text_field_has_no_file() {
    let r = post_truncated(routes(state()), "/api/import/csv", "", "label").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["error"], "No file field in multipart request");
}

#[tokio::test]
async fn a_snapshot_that_cannot_be_persisted_is_still_imported() {
    let src = state();
    run(&src, "CREATE (:S)").await;
    let data = snapshot_bytes(&src).await;
    // The data path is a regular file, so the snapshots directory cannot be made.
    let dir = tempfile::tempdir().unwrap();
    let not_a_dir = dir.path().join("file");
    std::fs::write(&not_a_dir, b"x").unwrap();
    let mut dst = state();
    dst.data_path = Some(not_a_dir.to_string_lossy().into_owned());
    let r = post_multipart(
        routes(dst.clone()),
        "/api/snapshot/import",
        vec![file(data)],
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(dst.store.read().await.node_count(), 1);
}

#[tokio::test]
async fn the_schema_of_an_empty_graph_has_no_degree() {
    let j = get_uri(routes(state()), "/api/schema").await.json();
    assert_eq!(j["node_types"], json!([]));
    assert_eq!(j["edge_types"], json!([]));
    assert_eq!(j["statistics"]["total_nodes"], 0);
    assert_eq!(j["statistics"]["avg_out_degree"], 0.0);
}

#[tokio::test]
async fn a_stream_that_fails_after_its_first_chunk_ends_with_an_error_trailer() {
    // Row 300 divides by zero, after the first 256-row chunk has been sent.
    let r = stream(
        routes(state()),
        json!({ "query": "UNWIND range(1, 300) AS x RETURN 10 / (300 - x) AS q" }),
    )
    .await;
    assert_eq!(
        r.status,
        StatusCode::OK,
        "the status went out with the first chunk"
    );
    let lines = ndjson_lines(&r);
    let last = lines.last().unwrap();
    assert!(
        last.get("done").is_none(),
        "a failed stream must not say done: {last}"
    );
    assert!(last["error"].is_string(), "{last}");
    assert_eq!(
        last["rows"],
        (lines.len() - 2) as u64,
        "rows counts what was sent"
    );
}
