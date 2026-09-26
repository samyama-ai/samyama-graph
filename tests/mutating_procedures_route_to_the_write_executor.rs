//! The router and the procedure registry agree about which `CALL`s write (#1468).
//!
//! `Query::is_write()` classified a statement by its clauses and never looked at
//! the procedure a `CALL` named. `AlgorithmOperator` knew perfectly well that
//! `algo.or.solve` writes — its `is_mutating` said so and its read path refused
//! the call — and nothing asked it. So every `CALL algo.or.solve(...)` was routed
//! to the read executor and refused with "requires write access", which made all
//! 31 solvers unreachable over HTTP and RESP (both classify through `is_write()`)
//! while they worked in-process against a `MutQueryExecutor`.
//!
//! # What these tests assert
//!
//! Not a list of solver names. A list here is a second hand-maintained list, and
//! two of those disagreeing is the whole defect. The assertion is **agreement**:
//! for every procedure `AlgorithmOperator::MUTATING_PROCEDURES` marks mutating, a
//! `CALL` to it must be classified a write. A procedure added to that list later
//! is covered the day it is added.
//!
//! The other direction is guarded too. "Route every `CALL` as a write" would pass
//! the first test and be a regression, not a fix: it would take the write lock and
//! the mutation journal for `CALL algo.pageRank()`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::{GraphStore, PropertyValue};
use samyama::http::server::HttpServer;
use samyama::query::executor::operator::AlgorithmOperator;
use samyama::query::QueryEngine;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower::ServiceExt;

/// A syntactically valid `CALL` for a procedure name, with an empty config map.
///
/// The arguments do not have to be ones the procedure accepts: routing happens
/// before the procedure reads them, and the tests below never assert on a result
/// the arguments would shape.
fn call_for(procedure: &str) -> String {
    format!("CALL algo.{procedure}({{}})")
}

/// Every procedure the registry marks mutating is classified as a write.
#[test]
fn the_router_calls_every_registered_mutating_procedure_a_write() {
    assert!(
        !AlgorithmOperator::MUTATING_PROCEDURES.is_empty(),
        "an empty registry would make this test pass without asserting anything"
    );
    let engine = QueryEngine::new();
    for (procedure, _why) in AlgorithmOperator::MUTATING_PROCEDURES {
        let q = call_for(procedure);
        assert!(
            engine.statement_is_write(&q).unwrap_or_else(|e| panic!("{q} did not parse: {e}")),
            "the registry marks `{procedure}` mutating and the router routed it to \
             the read executor: {q}"
        );
    }
}

/// And the classification is load-bearing: the read executor really does refuse
/// each of them, so calling it a read is not a harmless misfiling.
///
/// Without this, the test above would still pass if the read executor had quietly
/// learned to run these — which would make the routing question moot and the
/// assertion above meaningless.
#[test]
fn the_read_executor_refuses_every_registered_mutating_procedure() {
    let engine = QueryEngine::new();
    let store = GraphStore::new();
    for (procedure, _why) in AlgorithmOperator::MUTATING_PROCEDURES {
        let q = call_for(procedure);
        let err = engine
            .execute(&q, &store)
            .err()
            .unwrap_or_else(|| panic!("the read executor accepted a mutating procedure: {q}"));
        assert!(
            err.to_string().contains("requires write access"),
            "{q} failed for another reason, so this asserts nothing: {err}"
        );
    }
}

/// The registry is what the operator itself answers with, not a copy of it.
#[test]
fn the_registry_is_the_operators_own_predicate() {
    for (procedure, _why) in AlgorithmOperator::MUTATING_PROCEDURES {
        assert!(AlgorithmOperator::procedure_is_mutating(procedure));
        // The prefixes and the casing a caller actually types.
        assert!(AlgorithmOperator::procedure_is_mutating(&format!("algo.{procedure}")));
        assert!(AlgorithmOperator::procedure_is_mutating(&format!(
            "samyama.{}",
            procedure.to_ascii_uppercase()
        )));
    }
}

/// The other direction. A read-only `CALL` still routes to the read executor.
///
/// If this fails, the fix was "classify every `CALL` as a write": correct answers,
/// and every algorithm call now takes the write path — the exclusive store lock and
/// the mutation journal — for a query that changes nothing.
const READ_ONLY_CALLS: &[&str] = &[
    "CALL algo.pageRank()",
    "CALL algo.wcc()",
    "CALL algo.triangleCount()",
    "CALL db.labels()",
    "CALL db.propertyKeys()",
];

#[test]
fn a_read_only_call_still_routes_to_the_read_executor() {
    let engine = QueryEngine::new();
    for q in READ_ONLY_CALLS {
        assert!(
            !engine.statement_is_write(q).unwrap_or_else(|e| panic!("{q} did not parse: {e}")),
            "a read-only CALL was routed to the write executor: {q}"
        );
    }
}

/// And a read-only `CALL` runs there rather than being refused, which is what makes
/// the classification above the right one.
#[test]
fn a_read_only_call_runs_on_the_read_executor() {
    let engine = QueryEngine::new();
    let store = GraphStore::new();
    for q in READ_ONLY_CALLS {
        if let Err(err) = engine.execute(q, &store) {
            panic!("a read-only CALL was refused by the read executor: {q} ({err})");
        }
    }
}

/// A write clause next to a read-only `CALL` is still a write, and a `CALL` to a
/// mutating procedure inside a subquery is one too.
#[test]
fn a_mutating_call_is_found_wherever_it_sits() {
    let engine = QueryEngine::new();
    for q in [
        "CALL algo.or.solve({label: 'Item', property: 'alloc', cost_property: 'cost'}) YIELD fitness RETURN fitness",
        "CALL algo.pageRank() YIELD node, score CREATE (:Ranked {score: score})",
    ] {
        assert!(engine.statement_is_write(q).unwrap(), "classified as a read: {q}");
    }
}

// ---------------------------------------------------------------------------
// End to end over HTTP, against the server's own router.
// ---------------------------------------------------------------------------

fn store_with_items() -> Arc<RwLock<GraphStore>> {
    let mut store = GraphStore::new();
    for cost in [5.0_f64, 10.0, 15.0] {
        let n = store.create_node("Item");
        store
            .set_node_property("default", n, "cost", PropertyValue::Float(cost))
            .unwrap();
    }
    Arc::new(RwLock::new(store))
}

/// `POST /api/query` with a solver call returns a solution, not a refusal.
///
/// This is the measurement in #1468: taken through the HTTP surface rather than an
/// in-process `QueryEngine`, which is why the defect had not shown up before.
#[tokio::test]
async fn a_solver_is_reachable_over_http() {
    let app = HttpServer::new(store_with_items(), 0).router();
    let body = serde_json::json!({
        "query": "CALL algo.or.solve({label: 'Item', property: 'alloc', \
                  cost_property: 'cost', algorithm: 'Jaya', population_size: 10, \
                  max_iterations: 20}) YIELD fitness, algorithm RETURN fitness, algorithm"
    });
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/query")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8_lossy(&bytes).to_string();
    assert!(
        !text.contains("requires write access"),
        "the solver was refused for lack of write access over HTTP: {text}"
    );
    assert_eq!(status, StatusCode::OK, "body: {text}");
    let v: serde_json::Value = serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"));
    assert!(
        text.contains("Jaya"),
        "no solution in the response, so nothing was solved: {v}"
    );
}
