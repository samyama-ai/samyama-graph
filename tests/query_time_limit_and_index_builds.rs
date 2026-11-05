//! The query time limit an operator can actually raise, and index builds that
//! are not thrown away once finished (#2046).
//!
//! `CREATE VECTOR INDEX` over 25,000 768-d vectors failed with `Query timed out
//! after 1 rows` after the HNSW build had completed, and the finished index was
//! rolled back, even with `SAMYAMA_QUERY_TIMEOUT=1200`. Three things combined:
//! every tenant carried a shipped `max_query_time_ms` of 60 s, the engine takes
//! the shorter of that and the server's timeout so the environment variable
//! could only lower it, and the write path checked the deadline after the
//! build's one row and failed a statement that had already done its work.
//!
//! Following CLAUDE.md, nothing here measures elapsed time. A limit of zero has
//! already passed when the statement starts, which is the one deadline that can
//! be asserted on deterministically.

use samyama::graph::GraphStore;
use samyama::persistence::tenant::{ResourceQuotas, TenantManager};
use samyama::query::{BoundParams, QueryEngine};
use std::time::Duration;

fn write(
    engine: &QueryEngine,
    store: &mut GraphStore,
    q: &str,
    limit: Option<Duration>,
) -> Result<(), String> {
    engine
        .execute_mut_with_params_within(q, store, "default", &BoundParams::new(), limit)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

fn seeded() -> (QueryEngine, GraphStore) {
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    write(
        &engine,
        &mut store,
        "UNWIND range(1, 20) AS i CREATE (:Doc {i: i, emb: [toFloat(i), 1.0, 0.5]})",
        None,
    )
    .expect("seed");
    (engine, store)
}

// ------------------------------------------------------------ quotas ----

#[test]
fn a_default_tenant_sets_no_query_time_limit_of_its_own() {
    // With a limit here the engine's `min` caps `SAMYAMA_QUERY_TIMEOUT`, so
    // raising the server's timeout could never take effect.
    assert_eq!(ResourceQuotas::default().max_query_time_ms, None);
    let m = TenantManager::new();
    assert_eq!(m.query_time_limit("default"), None);
}

#[test]
fn a_query_time_limit_set_on_the_default_tenant_is_the_one_read_back() {
    // What `--max-query-time-ms` does to the default tenant.
    let m = TenantManager::new();
    let raised = ResourceQuotas { max_query_time_ms: Some(1_200_000), ..ResourceQuotas::default() };
    m.update_quotas("default", raised).expect("default tenant must exist");
    assert_eq!(m.query_time_limit("default"), Some(Duration::from_millis(1_200_000)));
    let t = m.get_tenant("default").unwrap();
    assert_eq!(t.quotas.max_nodes, Some(1_000_000), "a field the flag did not name moved");
}

// ------------------------------------------------------- index builds ----

#[test]
fn a_vector_index_build_already_out_of_time_is_refused_before_it_starts() {
    let (engine, mut store) = seeded();
    let err = write(
        &engine,
        &mut store,
        "CREATE VECTOR INDEX doc_emb FOR (d:Doc) ON (d.emb) OPTIONS {dimensions: 3, similarity: 'cosine'}",
        Some(Duration::ZERO),
    )
    .expect_err("a build past its deadline ran");
    assert!(err.contains("Query timed out"), "not the timeout error: {err}");
    assert!(err.contains("before the index build started"), "the refusal does not say the build never ran: {err}");
    assert!(
        store.vector_index.list_indices().is_empty(),
        "a refused build left an index behind"
    );
}

#[test]
fn a_vector_index_build_within_its_deadline_is_kept_and_searchable() {
    let (engine, mut store) = seeded();
    write(
        &engine,
        &mut store,
        "CREATE VECTOR INDEX doc_emb FOR (d:Doc) ON (d.emb) OPTIONS {dimensions: 3, similarity: 'cosine'}",
        Some(Duration::from_secs(3600)),
    )
    .expect("the build failed");
    assert_eq!(store.vector_index.list_indices().len(), 1);
    let hits = store
        .vector_search("Doc", "emb", &[5.0, 1.0, 0.5], 3)
        .expect("the index the statement reported built is not there");
    assert_eq!(hits.len(), 3, "the built index was not backfilled");
}

#[test]
fn a_property_index_build_already_out_of_time_is_refused_the_same_way() {
    let (engine, mut store) = seeded();
    let err = write(&engine, &mut store, "CREATE INDEX ON :Doc(i)", Some(Duration::ZERO))
        .expect_err("a build past its deadline ran");
    assert!(err.contains("before the index build started"), "not the pre-build refusal: {err}");
}

#[test]
fn a_data_write_past_its_deadline_still_times_out_after_its_first_batch() {
    // The exemption is for index builds only: a write that ran out of time
    // part way is still stopped and undone (#1593).
    let (engine, mut store) = seeded();
    let err = write(
        &engine,
        &mut store,
        "UNWIND range(1, 3000) AS i CREATE (:N {i: i})",
        Some(Duration::ZERO),
    )
    .expect_err("a write past its deadline succeeded");
    assert!(err.contains("Query timed out after"), "not the per-batch timeout: {err}");
}
