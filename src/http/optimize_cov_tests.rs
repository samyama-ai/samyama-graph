//! The optimisation endpoints: catalogues, solve/stream/cancel, and every
//! solver and problem the dispatcher can reach.

use super::*;
use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use tower::util::ServiceExt;

fn app() -> Router {
    router().with_state(Arc::new(OptimizeState::default()))
}

fn cfg() -> SolverConfig {
    SolverConfig {
        population_size: 6,
        max_iterations: 3,
    }
}

async fn get_json(app: Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let resp = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post(app: Router, uri: &str, body: &str) -> (StatusCode, String) {
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// The events of an SSE body, as `(event, data)` pairs.
async fn stream_events(app: Router, job: &str) -> (StatusCode, Vec<(String, serde_json::Value)>) {
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/optimize/solve/{job}/stream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).into_owned();
    let mut events = Vec::new();
    for block in text.split("\n\n") {
        let mut name = None;
        let mut data = None;
        for line in block.lines() {
            if let Some(n) = line
                .strip_prefix("event: ")
                .or_else(|| line.strip_prefix("event:"))
            {
                name = Some(n.trim().to_string());
            }
            if let Some(d) = line
                .strip_prefix("data: ")
                .or_else(|| line.strip_prefix("data:"))
            {
                data = Some(serde_json::from_str(d.trim()).unwrap_or(serde_json::Value::Null));
            }
        }
        if let (Some(n), Some(d)) = (name, data) {
            events.push((n, d));
        }
    }
    (status, events)
}

#[tokio::test]
async fn the_algorithm_catalogue_lists_every_solver_once() {
    let (status, json) = get_json(app(), "/optimize/algorithms").await;
    assert_eq!(status, StatusCode::OK);
    let algos = json.as_array().unwrap();
    assert_eq!(algos.len(), 23);
    let ids: std::collections::HashSet<&str> =
        algos.iter().map(|a| a["id"].as_str().unwrap()).collect();
    assert_eq!(ids.len(), 23, "duplicate ids");
    let nsga2 = algos.iter().find(|a| a["id"] == "nsga2").unwrap();
    assert_eq!(nsga2["multi_objective"], true);
    assert_eq!(nsga2["family"], "multi-objective");
    let jaya = algos.iter().find(|a| a["id"] == "jaya").unwrap();
    assert_eq!(jaya["multi_objective"], false);
    assert_eq!(jaya["params"][0]["name"], "population_size");
    assert_eq!(jaya["params"][0]["type"], "int");
    assert_eq!(jaya["params"][0]["default"], 50);
    assert_eq!(jaya["paper_refs"][0]["year"], 2016);
    let mo_bmr = algos.iter().find(|a| a["id"] == "mo_bmr").unwrap();
    assert_eq!(mo_bmr["variant"], "MOBMR");
    assert_eq!(mo_bmr["paper_refs"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn the_benchmark_catalogue_describes_each_problem() {
    let (status, json) = get_json(app(), "/optimize/benchmarks").await;
    assert_eq!(status, StatusCode::OK);
    let b = json.as_array().unwrap();
    assert_eq!(b.len(), 9);
    let sphere = b.iter().find(|x| x["id"] == "sphere").unwrap();
    assert_eq!(sphere["type"], "single");
    assert_eq!(sphere["optimum"], 0.0);
    let dtlz1 = b.iter().find(|x| x["id"] == "dtlz1").unwrap();
    assert_eq!(dtlz1["num_objectives"], 3);
    assert!(dtlz1["optimum"].is_null());
    let uc2 = b.iter().find(|x| x["id"] == "uc2_dosing").unwrap();
    assert_eq!(uc2["type"], "usecase");
}

#[tokio::test]
async fn an_unknown_field_benchmark_or_algorithm_is_a_400() {
    let (s, m) = post(
        app(),
        "/optimize/solve",
        r#"{"algorithm":"jaya","benchmark":"sphere","n_var":3}"#,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(m.starts_with("invalid solve request:"), "{m}");
    assert!(m.contains("n_var"), "the field is named: {m}");

    let (s, m) = post(
        app(),
        "/optimize/solve",
        r#"{"algorithm":"jaya","benchmark":"moon"}"#,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(m, "unknown benchmark: moon");

    let (s, m) = post(
        app(),
        "/optimize/solve",
        r#"{"algorithm":"magic","benchmark":"sphere"}"#,
    )
    .await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(m, "unknown algorithm: magic");

    let (s, m) = post(app(), "/optimize/solve", "not json").await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(m.starts_with("invalid solve request:"), "{m}");
}

#[tokio::test]
async fn a_single_objective_solve_streams_iterations_then_done_with_its_seed() {
    let app = app();
    let (s, body) = post(
        app.clone(),
        "/optimize/solve",
        r#"{"algorithm":"jaya","benchmark":"sphere","population_size":6,"iterations":4,"dim":3,"seed":42}"#,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let job = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, events) = stream_events(app.clone(), &job).await;
    assert_eq!(status, StatusCode::OK);
    let (last_name, last) = events.last().expect("some events");
    assert_eq!(last_name, "done", "{events:?}");
    assert_eq!(last["seed"], 42);
    assert!(last["final_pareto"].is_null());
    let iterations: Vec<_> = events.iter().filter(|(n, _)| n == "iteration").collect();
    assert_eq!(
        iterations.len() as u64,
        last["iterations"].as_u64().unwrap()
    );
    assert_eq!(iterations[0].1["iter"], 0);
    assert!(
        last["final_fitness"].as_f64().unwrap() >= 0.0,
        "sphere is non-negative"
    );

    // The receiver has been taken: a second stream of the same job is refused.
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/optimize/solve/{job}/stream"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn a_multi_objective_solve_stamps_the_pareto_front_on_the_last_iteration() {
    let app = app();
    let (s, body) = post(
        app.clone(),
        "/optimize/solve",
        r#"{"algorithm":"nsga2","benchmark":"zdt1","population_size":6,"iterations":2,"seed":7}"#,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{body}");
    let job = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, events) = stream_events(app, &job).await;
    let (name, done) = events.last().unwrap();
    assert_eq!(name, "done");
    let front = done["final_pareto"].as_array().expect("a pareto front");
    assert!(!front.is_empty());
    assert!(
        front.iter().all(|p| p.as_array().unwrap().len() == 2),
        "ZDT1 has two objectives"
    );
    let iters: Vec<_> = events.iter().filter(|(n, _)| n == "iteration").collect();
    assert!(iters.last().unwrap().1["pareto_front"].is_array());
    if iters.len() > 1 {
        assert!(
            iters[0].1["pareto_front"].is_null(),
            "only the last carries the front"
        );
    }
}

#[tokio::test]
async fn a_solver_error_is_streamed_as_an_error_event() {
    // `mo_bmr` on a single-objective benchmark passes validation (both ids
    // exist) and is refused by the dispatcher.
    let app = app();
    let (s, body) = post(
        app.clone(),
        "/optimize/solve",
        r#"{"algorithm":"mo_bmr","benchmark":"sphere","population_size":4,"iterations":1}"#,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let job = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, events) = stream_events(app, &job).await;
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0].0, "error");
    assert_eq!(
        events[0].1["message"],
        "benchmark sphere is not multi-objective"
    );
}

#[tokio::test]
async fn cancel_stops_a_run_before_it_finishes() {
    let app = app();
    // More iterations than the 256-event channel holds, so the emitter blocks
    // until the stream is read and cannot finish before the cancel lands.
    let (s, body) = post(
        app.clone(),
        "/optimize/solve",
        r#"{"algorithm":"jaya","benchmark":"sphere","population_size":4,"iterations":600,"dim":2,"seed":1}"#,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let job = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (s, body) = post(app.clone(), &format!("/optimize/solve/{job}/cancel"), "").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, r#"{"cancelled":true}"#);

    let (_, events) = stream_events(app, &job).await;
    let (name, last) = events.last().unwrap();
    assert_eq!(name, "error", "a cancelled run must not end with done");
    assert_eq!(last["message"], "cancelled");
    assert!(events.iter().all(|(n, _)| n != "done"));
}

#[tokio::test]
async fn cancel_or_stream_of_an_unknown_job_says_so() {
    let (s, body) = post(app(), "/optimize/solve/nope/cancel", "").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body, r#"{"cancelled":false}"#);
    let resp = app()
        .oneshot(
            Request::builder()
                .uri("/optimize/solve/nope/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&bytes[..], b"unknown job");
}

#[tokio::test]
async fn cancel_of_a_job_without_a_cancel_handle_reports_false() {
    let state = Arc::new(OptimizeState::default());
    state.jobs.lock().await.insert(
        "bare".into(),
        JobHandle {
            cancel_tx: None,
            cancel_flag: None,
            event_rx: None,
        },
    );
    let app = router().with_state(Arc::clone(&state));
    let (_, body) = post(app, "/optimize/solve/bare/cancel", "").await;
    assert_eq!(body, r#"{"cancelled":false}"#);
}

#[test]
fn every_single_objective_solver_runs_on_every_single_objective_benchmark() {
    let algos = [
        "jaya",
        "rao1",
        "rao2",
        "rao3",
        "tlbo",
        "itlbo",
        "qojaya",
        "gotlbo",
        "bmr",
        "bwr",
        "bmwr",
        "samp_jaya",
        "qo_rao",
        "ehrjaya",
        "saphr",
        "pso",
        "de",
        "ga",
    ];
    for algo in algos {
        let out = run_solver(algo, false, "sphere", 1, 2, cfg(), Some(3))
            .unwrap_or_else(|e| panic!("{algo}: {e}"));
        assert!(out.final_pareto.is_none(), "{algo}");
        assert!(!out.history.is_empty(), "{algo}");
        assert!(
            out.final_fitness.is_finite() && out.final_fitness >= 0.0,
            "{algo}"
        );
    }
    for bench in ["rastrigin", "ackley", "rosenbrock"] {
        let out = run_solver("jaya", false, bench, 1, 0, cfg(), None).unwrap();
        assert!(out.final_fitness.is_finite(), "{bench}");
    }
}

#[test]
fn every_multi_objective_solver_runs_on_every_multi_objective_benchmark() {
    let algos = ["mo_bmr", "mo_bwr", "mo_bmwr", "mo_rao_de", "nsga2"];
    for bench in ["zdt1", "zdt2", "zdt3", "dtlz1", "uc2_dosing"] {
        for algo in algos {
            let out = run_solver(algo, true, bench, 2, 0, cfg(), Some(5))
                .unwrap_or_else(|e| panic!("{algo}/{bench}: {e}"));
            let front = out.final_pareto.expect("a front");
            assert!(
                front.iter().all(|p| p.iter().all(|v| v.is_finite())),
                "{algo}/{bench}: non-finite points are filtered"
            );
        }
    }
}

#[test]
fn a_single_objective_solver_on_a_multi_objective_benchmark_is_refused() {
    for bench in ["zdt1", "uc2_dosing", "dtlz1"] {
        let e = run_solver("jaya", true, bench, 2, 0, cfg(), None)
            .err()
            .unwrap();
        assert_eq!(e, "algorithm jaya not multi-objective");
    }
    let e = run_solver("nsga2", false, "sphere", 1, 0, cfg(), None)
        .err()
        .unwrap();
    assert_eq!(
        e,
        "algorithm nsga2 not supported on single-objective benchmarks"
    );
    let e = run_solver("jaya", false, "moon", 1, 0, cfg(), None)
        .err()
        .unwrap();
    assert_eq!(e, "unknown benchmark: moon");
}

#[test]
fn the_single_objective_functions_are_zero_at_their_optimum() {
    let origin = Array1::from_elem(4, 0.0);
    let ones = Array1::from_elem(4, 1.0);
    let f = |name: &str, x: &Array1<f64>| (single_obj(name, 4, -1.0, 1.0).objective_func)(x);
    assert_eq!(f("sphere", &origin), 0.0);
    assert!(f("rastrigin", &origin).abs() < 1e-12);
    assert!(f("ackley", &origin).abs() < 1e-12);
    assert_eq!(f("rosenbrock", &ones), 0.0);
    assert_eq!(f("sphere", &ones), 4.0);
    // Unknown names fall back to the sphere.
    assert_eq!(f("unknown", &ones), 4.0);
    let p = single_obj("sphere", 4, -2.0, 3.0);
    assert_eq!(p.dim, 4);
    assert_eq!(p.lower[0], -2.0);
    assert_eq!(p.upper[3], 3.0);
}

#[test]
fn zdt_objectives_follow_their_definitions() {
    let mut x = Array1::from_elem(30, 0.0);
    x[0] = 0.25;
    // g == 1 when the tail is zero.
    for (variant, f2) in [
        (1u8, 1.0 - 0.25f64.sqrt()),
        (2, 1.0 - 0.25f64.powi(2)),
        (
            3,
            1.0 - 0.25f64.sqrt() - 0.25 * (10.0 * std::f64::consts::PI * 0.25).sin(),
        ),
        (9, 1.0 - 0.25f64.sqrt()),
    ] {
        let p = ZDT { variant, dim: 30 };
        assert_eq!(p.dim(), 30);
        assert_eq!(p.num_objectives(), 2);
        let (lo, hi) = p.bounds();
        assert_eq!((lo[0], hi[29]), (0.0, 1.0));
        let o = p.objectives(&x);
        assert_eq!(o[0], 0.25);
        assert!((o[1] - f2).abs() < 1e-12, "variant {variant}: {o:?}");
    }
}

#[test]
fn dtlz1_objectives_sum_to_half_on_the_optimal_front() {
    let p = DTLZ1 { dim: 7, m: 3 };
    assert_eq!(p.dim(), 7);
    assert_eq!(p.num_objectives(), 3);
    let (lo, hi) = p.bounds();
    assert_eq!((lo[6], hi[0]), (0.0, 1.0));
    // With every distance variable at 0.5, g = 0 and the objectives sum to 0.5.
    let x = Array1::from(vec![0.3, 0.6, 0.5, 0.5, 0.5, 0.5, 0.5]);
    let o = p.objectives(&x);
    assert_eq!(o.len(), 3);
    assert!((o.iter().sum::<f64>() - 0.5).abs() < 1e-9, "{o:?}");
}

#[tokio::test]
async fn a_body_without_a_json_content_type_is_a_400_that_says_so() {
    let resp = app()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/optimize/solve")
                .body(Body::from(r#"{"algorithm":"jaya","benchmark":"sphere"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let m = String::from_utf8_lossy(&bytes);
    assert!(m.starts_with("invalid solve request:"), "{m}");
    assert!(m.to_lowercase().contains("content-type"), "{m}");
}

#[tokio::test]
async fn the_test_router_serves_the_optimizer_alone() {
    let store = Arc::new(tokio::sync::RwLock::new(crate::graph::GraphStore::new()));
    let app = crate::http::build_router_for_tests(store);
    let (status, json) = get_json(app.clone(), "/optimize/benchmarks").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json.as_array().unwrap().len(), 9);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/status")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "no graph routes");
}
