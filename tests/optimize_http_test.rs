//! Integration tests for the `/optimize/*` HTTP API.
//!
//! TDD-first: these tests drive the design of `src/http/optimize.rs`.
//! Contracts mirror `samyama-cloud/wiki/decisions/optimization-in-insight.md`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::http::build_router_for_tests;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tower::ServiceExt;

fn router() -> axum::Router {
    let store = Arc::new(RwLock::new(GraphStore::new()));
    build_router_for_tests(store)
}

async fn body_to_json(body: Body) -> Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!("body not JSON: {e}: {}", String::from_utf8_lossy(&bytes))
    })
}

#[tokio::test]
async fn algorithms_endpoint_lists_rao_family_and_baselines() {
    let app = router();
    let res = app
        .oneshot(
            Request::builder()
                .uri("/optimize/algorithms")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let v = body_to_json(res.into_body()).await;
    let arr = v.as_array().expect("expected top-level array");
    assert!(arr.len() >= 20, "expected 20+ algorithms, got {}", arr.len());

    // Spot-check shape + membership
    let names: Vec<String> = arr
        .iter()
        .map(|a| a["id"].as_str().unwrap().to_string())
        .collect();
    for required in [
        "jaya", "rao1", "rao2", "rao3", "tlbo", "bmr", "bwr", "bmwr",
        "samp_jaya", "qo_rao", "ehrjaya", "saphr",
        "mo_bmr", "mo_bwr", "mo_bmwr", "mo_rao_de",
        "pso", "de", "ga", "nsga2",
    ] {
        assert!(names.iter().any(|n| n == required), "missing algorithm: {required}");
    }

    // Shape check on the Jaya entry.
    let jaya = arr.iter().find(|a| a["id"] == "jaya").unwrap();
    assert_eq!(jaya["family"], "rao");
    assert!(jaya["equation_tex"].is_string());
    assert!(jaya["paper_refs"].is_array());
    assert!(jaya["params"].is_array());
}

#[tokio::test]
async fn benchmarks_endpoint_lists_standard_functions() {
    let app = router();
    let res = app
        .oneshot(
            Request::builder()
                .uri("/optimize/benchmarks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let v = body_to_json(res.into_body()).await;
    let arr = v.as_array().expect("expected array");
    let ids: Vec<String> = arr
        .iter()
        .map(|b| b["id"].as_str().unwrap().to_string())
        .collect();
    for required in [
        "sphere", "rastrigin", "ackley", "rosenbrock",
        "zdt1", "zdt2", "zdt3", "dtlz1",
    ] {
        assert!(ids.iter().any(|i| i == required), "missing benchmark: {required}");
    }

    let zdt1 = arr.iter().find(|b| b["id"] == "zdt1").unwrap();
    assert_eq!(zdt1["type"], "multi");
    assert_eq!(zdt1["num_objectives"], 2);

    let sphere = arr.iter().find(|b| b["id"] == "sphere").unwrap();
    assert_eq!(sphere["type"], "single");
    assert_eq!(sphere["num_objectives"], 1);
}

#[tokio::test]
async fn solve_then_stream_jaya_sphere_converges() {
    let app = router();

    // Kick off a job.
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/optimize/solve")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "algorithm": "jaya",
                        "benchmark": "sphere",
                        "population_size": 30,
                        "iterations": 100,
                        "dim": 2
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let v = body_to_json(res.into_body()).await;
    let job_id = v["job_id"].as_str().expect("job_id").to_string();

    // Stream until "done" or timeout.
    let stream_res = tokio::time::timeout(Duration::from_secs(10), async {
        app.oneshot(
            Request::builder()
                .uri(format!("/optimize/solve/{}/stream", job_id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
    })
    .await
    .expect("stream request timed out");
    assert_eq!(stream_res.status(), StatusCode::OK);

    // Collect the full SSE body (handlers return once the job completes).
    let bytes = stream_res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes);

    // We expect at least one "iteration" event and one "done" event.
    let iter_events = text.matches("event: iteration").count();
    assert!(iter_events >= 10, "expected >=10 iteration events, got {iter_events}");
    assert!(text.contains("event: done"), "no 'done' event: {text}");

    // Extract the final_fitness from the done event and assert convergence.
    let done_line = text
        .lines()
        .skip_while(|l| !l.starts_with("event: done"))
        .nth(1) // "data: {...}"
        .expect("done data line");
    let data = done_line.trim_start_matches("data: ");
    let done: Value = serde_json::from_str(data).unwrap();
    let final_fitness = done["final_fitness"].as_f64().unwrap();
    assert!(
        final_fitness < 0.1,
        "Jaya on Sphere 2D did not converge: final_fitness = {final_fitness}"
    );
}

#[tokio::test]
async fn cancel_endpoint_returns_ok_even_for_unknown_job() {
    let app = router();
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/optimize/solve/does-not-exist/cancel")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // We idempotently return 200 with {cancelled:false} for unknown jobs.
    assert_eq!(res.status(), StatusCode::OK);
    let v = body_to_json(res.into_body()).await;
    assert_eq!(v["cancelled"], false);
}
