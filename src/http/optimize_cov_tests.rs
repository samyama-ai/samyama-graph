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

/// `total_time_ms` on a `done` event is a measurement, not a placeholder.
///
/// It was the constant 0 (#1798). A real solve takes some time, however
/// small, so a measured value is strictly positive; 0 is what the old
/// constant sent and must not come back.
fn assert_solve_was_timed(done: &serde_json::Value) {
    let ms = done["total_time_ms"]
        .as_f64()
        .unwrap_or_else(|| panic!("total_time_ms is a number: {done}"));
    assert!(
        ms.is_finite() && ms > 0.0,
        "total_time_ms is measured, got {ms}"
    );
}

/// Any fixed seed: the envelope tests check relationships, not values.
const ENVELOPE_SEED: u64 = 3;

/// `run_solver`'s `bench_dim` value meaning "use the catalog's dimension".
const CATALOG_DIM: usize = 0;

/// The catalog's known optimum for a benchmark, so a test never restates it.
fn optimum_of(benchmark: &str) -> Option<f64> {
    benchmark_catalog()
        .into_iter()
        .find(|b| b.id == benchmark)
        .unwrap_or_else(|| panic!("{benchmark} is in the catalog"))
        .optimum
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
    assert_eq!(
        b.len(),
        benchmark_catalog().len(),
        "every catalogue entry is served"
    );
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
    assert_solve_was_timed(last);
    // The rest of the result envelope (OPT-09, #1776). Sphere is box-bounded
    // only, so every answer is feasible, and it has a known optimum, so the gap
    // is the objective's distance from it.
    assert_eq!(last["feasible"], true, "{last}");
    let optimum = optimum_of("sphere").expect("sphere has a known optimum");
    assert_eq!(
        last["gap"].as_f64().expect("a gap"),
        last["final_fitness"].as_f64().unwrap() - optimum
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
    assert_solve_was_timed(done);
    // A front has no single optimum, and its feasibility cannot be read on
    // this surface (#1795): both fields are present and null, never guessed.
    assert!(done.get("feasible").is_some_and(|v| v.is_null()), "{done}");
    assert!(done.get("gap").is_some_and(|v| v.is_null()), "{done}");
    let front = done["final_pareto"].as_array().expect("a pareto front");
    assert!(!front.is_empty());
    assert!(
        front.iter().all(|p| p.as_array().unwrap().len() == 2),
        "ZDT1 has two objectives"
    );
    // ZDT1 has a known front, so the done event measures against it (OPT-03).
    let quality = done["quality"].as_object().expect("a quality object");
    for metric in QUALITY_METRICS {
        assert!(quality[metric].is_number(), "`{metric}` in {done}");
    }
    assert_eq!(quality["reference_point"].as_array().unwrap().len(), 2);
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
fn every_single_objective_benchmark_reports_feasibility_and_its_gap() {
    for bench in ["sphere", "rastrigin", "ackley", "rosenbrock"] {
        let out =
            run_solver("jaya", false, bench, 1, CATALOG_DIM, cfg(), Some(ENVELOPE_SEED)).unwrap();
        assert_eq!(out.feasible, Some(true), "{bench} has bounds only, no constraints");
        let optimum = optimum_of(bench).unwrap_or_else(|| panic!("{bench} has an optimum"));
        // These problems add no penalty, so the objective is the fitness.
        assert_eq!(out.gap, Some(out.final_fitness - optimum), "{bench}");
        assert!(
            out.gap.is_some_and(|g| g >= 0.0),
            "{bench}: nothing beats a true optimum"
        );
    }
}

#[test]
fn a_multi_objective_result_reports_no_feasibility_or_gap() {
    for bench in ["zdt1", "dtlz1", "uc2_dosing"] {
        let out =
            run_solver("nsga2", true, bench, 2, CATALOG_DIM, cfg(), Some(ENVELOPE_SEED)).unwrap();
        assert_eq!(out.feasible, None, "{bench}");
        assert_eq!(out.gap, None, "{bench}");
        assert_eq!(optimum_of(bench), None, "{bench}: the catalog lists no optimum");
    }
}

#[test]
fn every_multi_objective_solver_runs_on_every_multi_objective_benchmark() {
    let algos = ["mo_bmr", "mo_bwr", "mo_bmwr", "mo_rao_de", "nsga2"];
    for bench in multi_objective_benchmarks() {
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

/// The five measures OPT-03 asks every multi-objective run to report.
const QUALITY_METRICS: [&str; 5] = ["hypervolume", "igd", "igd_plus", "spread", "coverage"];

/// Every multi-objective benchmark in the catalogue, from the catalogue.
fn multi_objective_benchmarks() -> Vec<&'static str> {
    benchmark_catalog()
        .into_iter()
        .filter(|b| b.num_objectives > 1)
        .map(|b| b.id)
        .collect()
}

#[test]
fn the_catalogue_reaches_five_zdt_and_all_seven_dtlz_problems() {
    let ids = multi_objective_benchmarks();
    for variant in [1, 2, 3, 4, 6] {
        assert!(
            ids.contains(&format!("zdt{variant}").as_str()),
            "zdt{variant}"
        );
    }
    assert!(
        !ids.contains(&"zdt5"),
        "ZDT5 is binary-coded and cannot be posed here"
    );
    for variant in 1..=7 {
        assert!(
            ids.contains(&format!("dtlz{variant}").as_str()),
            "dtlz{variant}"
        );
    }
}

#[test]
fn each_known_front_benchmark_is_the_crate_problem_at_the_catalogue_size() {
    for b in benchmark_catalog().into_iter().filter(|b| b.ty == "multi") {
        let problem = FrontProblem::for_benchmark(b.id).unwrap_or_else(|| panic!("{}", b.id));
        let (dim, objectives) = match &problem {
            FrontProblem::Zdt(p) => (p.dim(), p.num_objectives()),
            FrontProblem::Dtlz(p) => (p.dim(), p.num_objectives()),
        };
        assert_eq!((dim, objectives), (b.dim, b.num_objectives), "{}", b.id);
        assert!(
            !problem.reference_front().is_empty(),
            "{}: a known front",
            b.id
        );
    }
    // No known front: a use case, a single-objective benchmark, an unknown id.
    for id in ["uc2_dosing", "sphere", "zdt5", "dtlz9", "moon"] {
        assert!(FrontProblem::for_benchmark(id).is_none(), "{id}");
    }
}

#[test]
fn every_benchmark_with_a_known_front_reports_all_five_measures() {
    for bench in multi_objective_benchmarks()
        .into_iter()
        .filter(|b| *b != "uc2_dosing")
    {
        let out = with_quality(
            run_solver(
                "nsga2",
                true,
                bench,
                0,
                CATALOG_DIM,
                cfg(),
                Some(ENVELOPE_SEED),
            )
            .unwrap_or_else(|e| panic!("{bench}: {e}")),
            bench,
        );
        let q = out.quality.unwrap_or_else(|| panic!("{bench}: no quality"));
        let objectives = out.final_pareto.unwrap()[0].len();
        assert_eq!(q.reference_point.len(), objectives, "{bench}");
        assert!(
            q.hypervolume >= 0.0 && q.hypervolume.is_finite(),
            "{bench}: {q:?}"
        );
        assert!(
            q.igd.is_finite() && q.igd_plus.is_finite() && q.spread.is_finite(),
            "{bench}: {q:?}"
        );
        assert!((0.0..=1.0).contains(&q.coverage), "{bench}: {q:?}");
        assert!(
            q.igd_plus <= q.igd + 1e-12,
            "{bench}: IGD+ never exceeds IGD"
        );
    }
}

#[test]
fn a_run_with_no_known_front_reports_no_quality() {
    let use_case = run_solver(
        "nsga2",
        true,
        "uc2_dosing",
        0,
        CATALOG_DIM,
        cfg(),
        Some(ENVELOPE_SEED),
    )
    .unwrap();
    assert_eq!(with_quality(use_case, "uc2_dosing").quality, None);
    let single = run_solver(
        "jaya",
        false,
        "sphere",
        1,
        CATALOG_DIM,
        cfg(),
        Some(ENVELOPE_SEED),
    )
    .unwrap();
    assert_eq!(with_quality(single, "sphere").quality, None);
}

#[test]
fn run_solver_leaves_quality_to_be_measured_after_the_clock() {
    // `run_solver` is what is timed; measuring is `with_quality`'s job.
    let out = run_solver(
        "nsga2",
        true,
        "zdt1",
        0,
        CATALOG_DIM,
        cfg(),
        Some(ENVELOPE_SEED),
    )
    .unwrap();
    assert_eq!(out.quality, None);
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
    assert_eq!(json.as_array().unwrap().len(), benchmark_catalog().len());
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

// ---------------------------------------------------------------------------
// Extra verification of #1798 (`total_time_ms` on `done`), beyond the PR's own.
// ---------------------------------------------------------------------------

async fn start_job(app: Router, body: &str) -> String {
    let (s, resp) = post(app, "/optimize/solve", body).await;
    assert_eq!(s, StatusCode::OK, "{resp}");
    serde_json::from_str::<serde_json::Value>(&resp).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn done_of(app: Router, body: &str) -> serde_json::Value {
    let job = start_job(app.clone(), body).await;
    let (_, events) = stream_events(app, &job).await;
    let (name, done) = events.last().expect("some events").clone();
    assert_eq!(name, "done", "{events:?}");
    done
}

#[tokio::test]
async fn total_time_ms_is_a_json_float_not_the_old_integer_zero() {
    let done = done_of(
        app(),
        r#"{"algorithm":"jaya","benchmark":"sphere","population_size":6,"iterations":3,"dim":2,"seed":3}"#,
    )
    .await;
    let v = &done["total_time_ms"];
    assert!(v.is_f64(), "serialised as a float, got {v}");
    assert!(!v.is_u64() && !v.is_i64(), "not an integer, got {v}");
}

#[tokio::test]
async fn total_time_ms_never_exceeds_the_wall_clock_of_the_whole_request() {
    let app = app();
    let t0 = std::time::Instant::now();
    let done = done_of(
        app,
        r#"{"algorithm":"jaya","benchmark":"rastrigin","population_size":40,"iterations":60,"dim":10,"seed":9}"#,
    )
    .await;
    let wall_ms = t0.elapsed().as_secs_f64() * 1_000.0;
    let ms = done["total_time_ms"].as_f64().unwrap();
    assert!(ms > 0.0 && ms <= wall_ms, "0 < {ms} <= wall {wall_ms}");
}

#[tokio::test]
async fn a_heavier_solve_reports_a_longer_time_than_a_tiny_one() {
    let tiny = done_of(
        app(),
        r#"{"algorithm":"jaya","benchmark":"sphere","population_size":4,"iterations":1,"dim":2,"seed":1}"#,
    )
    .await["total_time_ms"]
        .as_f64()
        .unwrap();
    // ~4 orders of magnitude more evaluations than `tiny`.
    let heavy = done_of(
        app(),
        r#"{"algorithm":"jaya","benchmark":"rastrigin","population_size":200,"iterations":300,"dim":30,"seed":1}"#,
    )
    .await["total_time_ms"]
        .as_f64()
        .unwrap();
    assert!(
        heavy > tiny,
        "heavy {heavy} ms should exceed tiny {tiny} ms"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn total_time_ms_excludes_time_spent_waiting_for_the_stream_reader() {
    // 400 iterations > the 256-slot channel, so the emitter blocks until the
    // stream is read. A slow reader must not inflate the solver's time.
    let app = app();
    let job = start_job(
        app.clone(),
        r#"{"algorithm":"jaya","benchmark":"sphere","population_size":4,"iterations":400,"dim":2,"seed":5}"#,
    )
    .await;
    let delay_ms = 1_500.0;
    tokio::time::sleep(std::time::Duration::from_millis(delay_ms as u64)).await;
    let (_, events) = stream_events(app, &job).await;
    let (name, done) = events.last().unwrap();
    assert_eq!(name, "done");
    assert_eq!(events.iter().filter(|(n, _)| n == "iteration").count(), 400);
    let ms = done["total_time_ms"].as_f64().unwrap();
    assert!(
        ms > 0.0 && ms < delay_ms,
        "solver time {ms} ms must not include the {delay_ms} ms the reader waited"
    );
}

#[tokio::test]
async fn error_and_cancel_events_carry_no_time() {
    let app = app();
    let job = start_job(
        app.clone(),
        r#"{"algorithm":"mo_bmr","benchmark":"sphere","population_size":4,"iterations":1}"#,
    )
    .await;
    let (_, events) = stream_events(app.clone(), &job).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "error");
    assert!(
        events[0].1.get("total_time_ms").is_none(),
        "{:?}",
        events[0]
    );

    let job = start_job(
        app.clone(),
        r#"{"algorithm":"jaya","benchmark":"sphere","population_size":4,"iterations":600,"dim":2,"seed":1}"#,
    )
    .await;
    post(app.clone(), &format!("/optimize/solve/{job}/cancel"), "").await;
    let (_, events) = stream_events(app, &job).await;
    assert!(events
        .iter()
        .all(|(n, d)| n != "done" && d.get("total_time_ms").is_none()));
}

#[tokio::test]
async fn every_algorithm_reports_a_measured_time_over_http() {
    let (_, algos) = get_json(app(), "/optimize/algorithms").await;
    let algos = algos.as_array().unwrap();
    assert_eq!(algos.len(), 23);
    for a in algos {
        let id = a["id"].as_str().unwrap();
        let bench = if a["multi_objective"].as_bool().unwrap() {
            "zdt1"
        } else {
            "sphere"
        };
        let body = format!(
            r#"{{"algorithm":"{id}","benchmark":"{bench}","population_size":8,"iterations":3,"seed":11}}"#
        );
        let done = done_of(app(), &body).await;
        let ms = done["total_time_ms"].as_f64().unwrap_or(-1.0);
        assert!(
            ms.is_finite() && ms > 0.0,
            "{id} on {bench}: total_time_ms = {}",
            done["total_time_ms"]
        );
        assert_eq!(done["seed"], 11, "{id}");
    }
}
