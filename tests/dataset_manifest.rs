//! A dataset is addressable by (snapshot hash, query, seed) and rebuilds
//! bit-identically from its manifest (ML-08, #1747).
//!
//! Before this, `seed` in an export body was dropped with HTTP 200 and the
//! bytes matched the call without it, and `snapshot_version` was a counter
//! that said nothing about the data. These tests drive the HTTP route the way
//! a training pipeline would: export, read the manifest, rebuild from it,
//! compare bytes -- and check that a changed seed or a changed graph is seen.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// 2,000 items: enough rows that a filter batch crosses the parallel
/// threshold (256 rows) when its predicate is expensive.
fn items() -> GraphStore {
    let mut store = GraphStore::new();
    for k in 0..2000i64 {
        let n = store.create_node("Item");
        store.set_node_property("default", n, "k", k).unwrap();
        store
            .set_node_property("default", n, "name", format!("item-{k}"))
            .unwrap();
    }
    store
}

fn router(store: Arc<RwLock<GraphStore>>) -> Router {
    let state = samyama::http::server::AppState {
        store,
        engine: Arc::new(QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
        persistence: None,
        snapshot_key: None,
        transactions: Default::default(),
    };
    Router::new()
        .route(
            "/api/query/export",
            post(samyama::http::handler::export_handler),
        )
        .with_state(state)
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn header(&self, name: &str) -> String {
        self.headers
            .get(name)
            .unwrap_or_else(|| panic!("no {name} header"))
            .to_str()
            .unwrap()
            .to_string()
    }
    fn manifest(&self) -> Json {
        serde_json::from_str(&self.header("x-samyama-dataset-manifest"))
            .expect("the manifest is JSON")
    }
    fn json(&self) -> Json {
        serde_json::from_slice(&self.body).expect("a JSON body")
    }
}

async fn export(store: &Arc<RwLock<GraphStore>>, body: Json) -> Reply {
    let r = router(store.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query/export")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = r.status();
    let headers = r.headers().clone();
    let body = r.into_body().collect().await.unwrap().to_bytes().to_vec();
    Reply {
        status,
        headers,
        body,
    }
}

/// A sample the way datasets are drawn: `rand()` in the filter, which is
/// expensive enough (cost 23 >= 20) to have run on several threads, plus
/// `rand()` and `randomUUID()` in the rows.
const SAMPLE: &str = "MATCH (n:Item) WHERE rand() < 0.5 AND toString(n.k) CONTAINS '1' \
                      RETURN n.k AS k, rand() AS r, randomUUID() AS u ORDER BY k";

fn shared(store: GraphStore) -> Arc<RwLock<GraphStore>> {
    Arc::new(RwLock::new(store))
}

#[tokio::test]
async fn the_same_seed_rebuilds_the_same_bytes_in_every_format() {
    let store = shared(items());
    for format in ["arrow", "parquet", "csv"] {
        let body = json!({ "query": SAMPLE, "seed": 7, "format": format });
        let a = export(&store, body.clone()).await;
        let b = export(&store, body).await;
        assert_eq!(
            a.status,
            StatusCode::OK,
            "{format}: {}",
            String::from_utf8_lossy(&a.body)
        );
        assert!(!a.body.is_empty());
        assert_eq!(
            a.body, b.body,
            "{format}: the same seed gave different bytes"
        );
        assert_eq!(a.manifest(), b.manifest(), "{format}");
    }
}

#[tokio::test]
async fn a_different_seed_draws_a_different_dataset() {
    let store = shared(items());
    let a = export(
        &store,
        json!({ "query": SAMPLE, "seed": 7, "format": "csv" }),
    )
    .await;
    let b = export(
        &store,
        json!({ "query": SAMPLE, "seed": 8, "format": "csv" }),
    )
    .await;
    assert_ne!(a.body, b.body);
    assert_ne!(
        a.header("x-samyama-content-hash"),
        b.header("x-samyama-content-hash")
    );
    // The graph is the same: only the seed moved.
    assert_eq!(
        a.header("x-samyama-snapshot-hash"),
        b.header("x-samyama-snapshot-hash")
    );
}

#[tokio::test]
async fn the_manifest_describes_the_bytes_it_came_with() {
    let store = shared(items());
    let r = export(
        &store,
        json!({ "query": SAMPLE, "seed": 42, "format": "csv" }),
    )
    .await;
    let m = r.manifest();
    let digest: String = Sha256::digest(&r.body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        m["content_hash"],
        json!(format!("sha256:{digest}")),
        "checked against the body itself"
    );
    assert_eq!(m["query"], json!(SAMPLE));
    assert_eq!(m["seed"], json!(42));
    assert_eq!(m["format"], json!("csv"));
    assert_eq!(m["graph"], json!("default"));
    assert_eq!(m["engine_version"], json!(samyama::VERSION));
    // One CSV line per row, plus the header.
    let lines = String::from_utf8(r.body.clone()).unwrap().lines().count();
    assert_eq!(m["rows"], json!(lines - 1));
    let snapshot = m["snapshot_hash"].as_str().unwrap().to_string();
    assert!(
        snapshot.starts_with("sha256:") && snapshot.len() == 71,
        "{snapshot}"
    );
    assert_eq!(r.header("x-samyama-snapshot-hash"), snapshot);
    assert_eq!(
        r.header("x-samyama-content-hash"),
        m["content_hash"].as_str().unwrap()
    );
    assert_eq!(r.header("x-samyama-seed"), "42");
}

#[tokio::test]
async fn rebuilding_from_the_manifest_gives_identical_bytes() {
    let store = shared(items());
    let first = export(
        &store,
        json!({ "query": SAMPLE, "seed": 3, "format": "parquet" }),
    )
    .await;
    let m = first.manifest();
    // Only what the manifest says: the triple, and the format.
    let again = export(
        &store,
        json!({
            "query": m["query"],
            "seed": m["seed"],
            "format": m["format"],
            "snapshot_hash": m["snapshot_hash"],
        }),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.body, first.body);
    assert_eq!(again.manifest(), m);
}

#[tokio::test]
async fn a_rebuild_against_a_changed_graph_is_refused_not_quietly_different() {
    let store = shared(items());
    let first = export(&store, json!({ "query": SAMPLE, "seed": 3 })).await;
    let pinned = first.header("x-samyama-snapshot-hash");
    {
        let mut g = store.write().await;
        let n = g.create_node("Item");
        g.set_node_property("default", n, "k", 99999i64).unwrap();
    }
    let r = export(
        &store,
        json!({ "query": SAMPLE, "seed": 3, "snapshot_hash": pinned }),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    let now = r.json()["snapshot_hash"].as_str().unwrap().to_string();
    assert_ne!(now, pinned, "the 409 names the hash the graph has now");
    assert!(r.json()["error"].as_str().unwrap().contains(&pinned));
    // Without the pin the export runs, and reports the new hash.
    let unpinned = export(&store, json!({ "query": SAMPLE, "seed": 3 })).await;
    assert_eq!(unpinned.status, StatusCode::OK);
    assert_eq!(unpinned.header("x-samyama-snapshot-hash"), now);
}

#[tokio::test]
async fn an_unknown_field_is_refused_and_named_rather_than_dropped() {
    let store = shared(items());
    let r = export(&store, json!({ "query": "RETURN 1 AS x", "sed": 7 })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let e = r.json()["error"].as_str().unwrap().to_string();
    assert!(e.contains("sed"), "{e}");
    let r = export(&store, json!({ "query": "RETURN 1 AS x", "seed": -1 })).await;
    assert_eq!(
        r.status,
        StatusCode::BAD_REQUEST,
        "a seed is a non-negative integer"
    );
}

#[tokio::test]
async fn without_a_seed_the_manifest_says_so() {
    let store = shared(items());
    let r = export(
        &store,
        json!({ "query": "MATCH (n:Item) RETURN n.k AS k ORDER BY k LIMIT 3" }),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.header("x-samyama-seed"), "none");
    assert_eq!(r.manifest()["seed"], Json::Null);
}

#[tokio::test]
async fn a_query_in_any_language_travels_in_the_manifest_header() {
    let store = shared(items());
    let q = "RETURN 'café — 東京 🚀' AS s";
    let r = export(&store, json!({ "query": q, "format": "csv" })).await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.header("x-samyama-dataset-manifest").is_ascii());
    assert_eq!(r.manifest()["query"], json!(q));
}

#[test]
fn a_seeded_statement_repeats_its_draws_and_the_seed_ends_with_it() {
    use samyama::query::executor::operator::statement_rng;
    let store = items();
    let engine = QueryEngine::new();
    let draws = |store: &GraphStore| -> Vec<String> {
        engine
            .execute(
                "UNWIND range(1, 20) AS i RETURN rand() AS r, randomUUID() AS u",
                store,
            )
            .unwrap()
            .records
            .iter()
            .map(|r| format!("{:?}", (r.get("r"), r.get("u"))))
            .collect()
    };
    let a = {
        let _g = statement_rng::seed(11);
        draws(&store)
    };
    let b = {
        let _g = statement_rng::seed(11);
        draws(&store)
    };
    let c = {
        let _g = statement_rng::seed(12);
        draws(&store)
    };
    assert_eq!(a, b);
    assert_ne!(a, c);
    // After the guard is gone, `rand()` is unseeded again.
    assert_ne!(draws(&store), draws(&store));
    assert_ne!(draws(&store), a);
}

#[test]
fn a_predicate_that_draws_random_numbers_never_runs_in_parallel() {
    use samyama::query::ast::{BinaryOp, Expression};
    use samyama::query::executor::operator::FilterOperator;
    let call = |name: &str, args: Vec<Expression>| Expression::Function {
        name: name.to_string(),
        args,
        distinct: false,
    };
    let prop = Expression::Property {
        variable: "n".to_string(),
        property: "k".to_string(),
    };
    let contains = Expression::Binary {
        left: Box::new(call("toString", vec![prop])),
        op: BinaryOp::Contains,
        right: Box::new(Expression::Literal(PropertyValue::String("1".into()))),
    };
    let with = |left: Expression| Expression::Binary {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(contains.clone()),
    };
    let lt = |left: Expression| Expression::Binary {
        left: Box::new(left),
        op: BinaryOp::Lt,
        right: Box::new(Expression::Literal(PropertyValue::Float(0.5))),
    };
    // The same cost either way; only the random draw differs.
    let deterministic = with(lt(call("abs", vec![])));
    let random = with(lt(call("rand", vec![])));
    assert!(
        FilterOperator::predicate_is_parallel(&deterministic),
        "the fixture must cross the threshold"
    );
    assert!(!FilterOperator::predicate_is_parallel(&random));
}
