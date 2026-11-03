//! `GET /api/memory` — where the memory goes (COST-06).
//!
//! COST-06 asks that the engine report memory per graph, index and tenant so a
//! customer can attribute cost. `GraphStore::memory_report` had the
//! per-component walk already and nothing exposed it, which is the same as not
//! having it: the number has to leave the process for anyone outside to act on.
//!
//! These tests are about the two properties that make the number worth
//! reporting — **it responds to the data**, and **it does not overstate what it
//! knows**. A constant would satisfy any check that only asked whether a field
//! is present, and a figure labelled `total` would invite a comparison against
//! RSS that it cannot survive.

use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::HttpServer;
use tokio::sync::RwLock;
use tower::util::ServiceExt;

async fn memory_of(setup: &[&str]) -> serde_json::Value {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let server = HttpServer::new(Arc::clone(&store), 0);
    for q in setup {
        let res = server
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::json!({ "query": q }).to_string()))
                    .unwrap(),
            )
            .await
            .expect("query");
        assert!(res.status().is_success(), "setup failed: {q}");
    }
    let res = server
        .router()
        .oneshot(Request::builder().uri("/api/memory").body(Body::empty()).unwrap())
        .await
        .expect("memory");
    assert!(res.status().is_success());
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

fn u(v: &serde_json::Value, path: &[&str]) -> u64 {
    let mut cur = v;
    for p in path {
        cur = cur.get(p).unwrap_or_else(|| panic!("no {p} in {cur}"));
    }
    cur.as_u64().unwrap_or_else(|| panic!("not a number: {cur}"))
}

#[tokio::test]
async fn the_figure_grows_with_the_graph() {
    // The property that makes it a measurement rather than a field. A constant
    // passes "is the key present"; it does not pass this.
    let small = memory_of(&["UNWIND range(1, 10) AS i CREATE (:P {name: 'n' + toString(i)})"]).await;
    let large =
        memory_of(&["UNWIND range(1, 2000) AS i CREATE (:P {name: 'n' + toString(i)})"]).await;
    let a = u(&small, &["graph", "attributed_bytes"]);
    let b = u(&large, &["graph", "attributed_bytes"]);
    assert!(
        b > a * 10,
        "200x the nodes gave {b} bytes against {a}; the figure does not follow the data"
    );
}

#[tokio::test]
async fn the_components_add_up_to_the_attributed_total() {
    // If they do not, one of them is being counted twice or not at all, and
    // the breakdown is the part a customer uses to decide what to change.
    let v = memory_of(&["UNWIND range(1, 100) AS i CREATE (:P {name: 'x'})-[:R]->(:Q)"]).await;
    let components = v["graph"]["components"].as_object().expect("components");
    let sum: u64 = components.values().map(|x| x.as_u64().unwrap_or(0)).sum();
    assert_eq!(
        sum,
        u(&v, &["graph", "attributed_bytes"]),
        "the components do not sum to the attributed total"
    );
}

#[tokio::test]
async fn an_index_is_reported_with_its_own_cost() {
    let v = memory_of(&[
        "UNWIND range(1, 500) AS i CREATE (:P {name: 'n' + toString(i), age: i})",
        "CREATE INDEX ON :P(name)",
    ])
    .await;
    let indexes = v["indexes"].as_array().expect("indexes");
    assert_eq!(indexes.len(), 1, "expected one index: {indexes:?}");
    assert_eq!(indexes[0]["label"], "P");
    assert_eq!(indexes[0]["property"], "name");
    assert_eq!(indexes[0]["entries"], 500);
    assert!(
        indexes[0]["bytes"].as_u64().unwrap() > 0,
        "an index over 500 nodes costs something"
    );
    assert_eq!(
        u(&v, &["index_memory_bytes"]),
        indexes[0]["bytes"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn a_graph_with_no_index_reports_none_rather_than_a_zero_row() {
    let v = memory_of(&["CREATE (:P {name: 'a'})"]).await;
    assert!(v["indexes"].as_array().expect("indexes").is_empty());
    assert_eq!(u(&v, &["index_memory_bytes"]), 0);
}

#[tokio::test]
async fn the_response_says_it_is_an_estimate() {
    // The honesty that keeps this usable. Without it somebody compares
    // `attributed_bytes` against RSS, finds a gap, and concludes the engine is
    // leaking -- when the gap is the allocator and the indexes this does not
    // walk.
    let v = memory_of(&["CREATE (:P)"]).await;
    assert_eq!(v["estimate"], true);
    let note = v["note"].as_str().expect("note");
    assert!(note.contains("not a reading of the allocator"), "{note}");
    assert!(note.contains("floor"), "{note}");
    assert!(
        v["graph"].get("total_bytes").is_none(),
        "nothing here may be called a total: it is a floor on the graph, not the process"
    );
}

#[tokio::test]
async fn bytes_per_edge_is_null_on_a_graph_with_no_edges() {
    // 0.0 bytes per edge is a claim, and a false one.
    let v = memory_of(&["CREATE (:P)"]).await;
    assert!(v["graph"]["bytes_per_edge"].is_null(), "{}", v["graph"]);
}

#[tokio::test]
async fn bytes_per_edge_is_reported_when_there_are_edges() {
    let v = memory_of(&["UNWIND range(1, 100) AS i CREATE (:P)-[:R]->(:Q)"]).await;
    let b = v["graph"]["bytes_per_edge"].as_f64().expect("a number");
    assert!(b > 0.0, "{b}");
}

// --- /metrics (REL-10) ------------------------------------------------

async fn metrics_of(setup: &[&str]) -> String {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let server = HttpServer::new(Arc::clone(&store), 0);
    for q in setup {
        server
            .router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/query")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::json!({ "query": q }).to_string()))
                    .unwrap(),
            )
            .await
            .expect("query");
    }
    let res = server
        .router()
        .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
        .await
        .expect("metrics");
    assert_eq!(
        res.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/plain; version=0.0.4"),
        "Prometheus refuses a body it cannot recognise by content type"
    );
    String::from_utf8(
        res.into_body().collect().await.unwrap().to_bytes().to_vec(),
    )
    .expect("utf-8")
}

#[tokio::test]
async fn metrics_carry_help_and_type_for_every_series() {
    // A series without them scrapes, and then nobody knows what it means.
    let body = metrics_of(&["CREATE (:P)-[:R]->(:Q)"]).await;
    for name in [
        "samyama_nodes",
        "samyama_edges",
        "samyama_memory_attributed_bytes",
        "samyama_index_memory_bytes",
        "samyama_query_cache_entries",
    ] {
        assert!(body.contains(&format!("# HELP {name} ")), "no HELP for {name}");
        assert!(body.contains(&format!("# TYPE {name} gauge")), "no TYPE for {name}");
        assert!(
            body.lines().any(|l| l.starts_with(&format!("{name} "))),
            "no sample for {name}"
        );
    }
}

#[tokio::test]
async fn metrics_report_the_graph_that_is_there() {
    let body = metrics_of(&["UNWIND range(1, 7) AS i CREATE (:P)-[:R]->(:Q)"]).await;
    assert!(body.contains("samyama_nodes 14"), "{body}");
    assert!(body.contains("samyama_edges 7"), "{body}");
}

#[tokio::test]
async fn each_index_gets_its_own_labelled_series() {
    // "indexes cost 4 GB" and "this one index costs 4 GB" lead to different
    // actions, so the label set has to carry which.
    let body = metrics_of(&[
        "UNWIND range(1, 50) AS i CREATE (:P {name: 'n' + toString(i)})",
        "CREATE INDEX ON :P(name)",
    ])
    .await;
    assert!(
        body.lines().any(|l| l.starts_with("samyama_index_bytes{")
            && l.contains("label=\"P\"")
            && l.contains("property=\"name\"")),
        "{body}"
    );
}
