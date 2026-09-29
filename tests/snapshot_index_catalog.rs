//! A `.sgsnap` round trip carries the index catalog (#1506).
//!
//! #1477 made index definitions survive a restart. A snapshot round trip was
//! still lossy in the same way: the file had no catalog, so import rebuilt
//! nothing but vector indexes, and those it *rediscovered* from embedding-shaped
//! properties -- losing the index's name and quantization, forcing the metric
//! to cosine, and inventing an index over any float list nobody had indexed.
//!
//! | structure | before | after |
//! |---|---|---|
//! | BTREE property index | dropped | declared and rebuilt |
//! | unique constraint    | dropped | declared and rebuilt |
//! | full-text            | dropped | declared and rebuilt |
//! | vector               | rediscovered (cosine, f32, unnamed) | declared as written |
//!
//! Every comparison is against `index_catalog()` of the source store, which is
//! what `SHOW INDEXES` and the RocksDB catalog both read. Equality in both
//! directions is the point: an index lost and an index invented are the same
//! failure seen from opposite sides.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use samyama::graph::GraphStore;
use samyama::index::catalog::IndexDefinition;
use samyama::query::QueryEngine;
use samyama::vector::index::Quantization;
use samyama::vector::DistanceMetric;
use std::sync::Arc;
use tower::ServiceExt;

const T: &str = "default";

fn run(store: &mut GraphStore, q: &str) {
    QueryEngine::new()
        .execute_mut(q, store, T)
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

/// Rows, then one of each kind of index. The vector index is declared with
/// every parameter away from its default, because a default is exactly what
/// rediscovery produces and would make a lost parameter look preserved.
///
/// `:Q(emb)` is embedding-shaped and deliberately **not** indexed: it is the
/// property rediscovery would have invented an index over.
fn source() -> GraphStore {
    let mut s = GraphStore::new();
    run(
        &mut s,
        "CREATE (:P {id: 1, email: 'a@x', body: 'quick brown fox', emb: [0.1, 0.2, 0.3, 0.4]}) \
         CREATE (:P {id: 2, email: 'b@x', body: 'lazy dog sleeps', emb: [0.9, 0.8, 0.7, 0.6]}) \
         CREATE (:Q {emb: [0.5, 0.5, 0.5, 0.5]})",
    );
    run(&mut s, "CREATE INDEX ON :P(id)");
    run(
        &mut s,
        "CREATE CONSTRAINT ON (n:P) ASSERT n.email IS UNIQUE",
    );
    run(&mut s, "CREATE FULLTEXT INDEX ftidx FOR (d:P) ON (d.body)");
    run(
        &mut s,
        "CREATE VECTOR INDEX vidx FOR (v:P) ON (v.emb) \
         OPTIONS {dimensions: 4, similarity: 'l2', quantization: 'fp16'}",
    );
    s
}

fn export(store: &GraphStore) -> Vec<u8> {
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(store, &mut buf).expect("export");
    buf
}

fn vector_def(store: &GraphStore) -> Vec<IndexDefinition> {
    store
        .index_catalog()
        .definitions
        .into_iter()
        .filter(|d| matches!(d, IndexDefinition::Vector { .. }))
        .collect()
}

#[test]
fn every_declared_index_crosses_the_snapshot_exactly() {
    let src = source();
    let bytes = export(&src);

    let mut dst = GraphStore::new();
    let stats = samyama::snapshot::import_tenant(&mut dst, &bytes[..]).expect("import");

    // Neither lost nor invented: the whole catalog, compared both ways.
    assert_eq!(dst.index_catalog(), src.index_catalog());

    let r = stats
        .indexes
        .expect("a snapshot written now carries its catalog");
    assert_eq!(
        (r.unique, r.fulltext, r.vector, r.failed),
        (1, 1, 1, 0),
        "{r:?}"
    );
    assert_eq!(stats.index_conflicts, 0);

    // The parameters rediscovery could not recover, spelled out.
    assert_eq!(
        vector_def(&dst),
        vec![IndexDefinition::Vector {
            name: Some("vidx".into()),
            label: "P".into(),
            property: "emb".into(),
            dimensions: 4,
            metric: DistanceMetric::L2,
            quantization: Quantization::Fp16,
        }]
    );
}

#[test]
fn the_restored_indexes_are_built_not_just_declared() {
    // A declared-and-empty index answers every search with nothing and no
    // error, which is worse than a missing one. Assert contents.
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &export(&source())[..]).expect("import");

    let by_name = QueryEngine::new()
        .execute(
            "CALL db.index.vector.queryNodes('vidx', 2, [0.1, 0.2, 0.3, 0.4]) \
             YIELD node RETURN node",
            &dst,
        )
        .expect("the vector index resolves by its name after import");
    assert_eq!(by_name.records.len(), 2);

    assert_eq!(
        dst.fulltext.search("ftidx", "quick", 10).map(|v| v.len()),
        Some(1)
    );

    let dup = QueryEngine::new().execute_mut("CREATE (:P {email: 'a@x'})", &mut dst, T);
    assert!(dup.is_err(), "the unique constraint did not come back");
}

#[test]
fn an_import_marks_the_catalog_for_persistence() {
    // `restore_index_catalog` clears the dirty flag, which is right on boot and
    // wrong here: definitions from a file are new to this data directory.
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &export(&source())[..]).expect("import");
    assert!(dst.index_catalog_is_dirty());
}

#[test]
fn a_graph_with_no_indexes_still_carries_an_empty_catalog() {
    // Presence is the signal. An empty catalog means "no indexes", which is
    // what stops the HTTP path rediscovering one over `:Q(emb)`.
    let mut s = GraphStore::new();
    run(&mut s, "CREATE (:Q {emb: [0.5, 0.5, 0.5, 0.5]})");
    let mut dst = GraphStore::new();
    let stats = samyama::snapshot::import_tenant(&mut dst, &export(&s)[..]).expect("import");
    let r = stats.indexes.expect("an empty catalog is still a catalog");
    assert_eq!(r.total(), 0);
    assert!(dst.index_catalog().is_empty());
}

/// A v2 file as written before #1506: header, nodes, no `"t":"i"` line.
fn pre_1506_snapshot() -> Vec<u8> {
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    let header = serde_json::json!({
        "format": "sgsnap", "version": 2, "tenant": "default",
        "node_count": 2, "edge_count": 0, "labels": ["Doc"], "edge_types": [],
        "created_at": "2026-01-01T00:00:00Z", "samyama_version": "1.10.0"
    });
    writeln!(gz, "{header}").unwrap();
    for (id, v) in [(1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])] {
        let node = serde_json::json!({
            "t": "n", "id": id, "labels": ["Doc"],
            "props": {"emb": {"__type": "Vector", "value": v}}
        });
        writeln!(gz, "{node}").unwrap();
    }
    gz.finish().unwrap()
}

#[test]
fn a_snapshot_without_a_catalog_still_imports() {
    let mut dst = GraphStore::new();
    let stats =
        samyama::snapshot::import_tenant(&mut dst, &pre_1506_snapshot()[..]).expect("import");
    assert_eq!(stats.node_count, 2);
    assert!(
        stats.indexes.is_none(),
        "absent is not empty: the caller needs to know the file said nothing"
    );
    assert!(dst.index_catalog().is_empty());
}

#[test]
fn a_different_definition_already_on_the_target_is_kept() {
    // The rows already there were indexed under the target's definition, and
    // replacing it would change answers for data this import did not bring.
    let mut dst = GraphStore::new();
    run(
        &mut dst,
        "CREATE VECTOR INDEX vidx FOR (v:P) ON (v.emb) OPTIONS {dimensions: 4}",
    );
    let before = vector_def(&dst);

    let stats = samyama::snapshot::import_tenant(&mut dst, &export(&source())[..]).expect("import");
    assert_eq!(stats.index_conflicts, 1);
    assert_eq!(
        vector_def(&dst),
        before,
        "the target's cosine f32 index was replaced"
    );
    // The non-colliding definitions still arrive.
    let r = stats.indexes.unwrap();
    assert_eq!((r.unique, r.fulltext, r.vector), (1, 1, 0), "{r:?}");
}

#[test]
fn an_identical_definition_on_the_target_is_not_a_conflict() {
    let src = source();
    let mut dst = source();
    let stats = samyama::snapshot::import_tenant(&mut dst, &export(&src)[..]).expect("import");
    assert_eq!(stats.index_conflicts, 0);
    assert_eq!(dst.index_catalog(), src.index_catalog());
}

// ---- the HTTP path, which is where rediscovery used to run -----------------

fn app() -> (axum::Router, Arc<tokio::sync::RwLock<GraphStore>>) {
    let store = Arc::new(tokio::sync::RwLock::new(GraphStore::new()));
    let state = samyama::http::server::AppState {
        store: store.clone(),
        engine: Arc::new(QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        persistence: None,
        snapshot_key: None,
        transactions: Default::default(),
    };
    let app = axum::Router::new()
        .route(
            "/api/snapshot/import",
            axum::routing::post(samyama::http::handler::restore_snapshot_handler),
        )
        .with_state(state);
    (app, store)
}

async fn post_snapshot(app: &axum::Router, bytes: &[u8]) -> serde_json::Value {
    let boundary = "sgsnap-test-boundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; \
             filename=\"g.sgsnap\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/snapshot/import")
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).expect("json body");
    assert_eq!(status, StatusCode::OK, "{json}");
    json
}

#[tokio::test]
async fn the_http_import_declares_the_catalog_and_invents_nothing() {
    // Before #1506 this endpoint ran `rebuild_vector_index_full`, which gave
    // back `:P(emb)` as an unnamed cosine f32 index, added one over `:Q(emb)`
    // that the source never had, and nothing else.
    let src = source();
    let (app, store) = app();
    let body = post_snapshot(&app, &export(&src)).await;

    assert_eq!(store.read().await.index_catalog(), src.index_catalog());
    assert_eq!(body["vector_indices_rebuilt"], 1, "{body}");
    assert_eq!(body["indexes_restored"]["fulltext"], 1, "{body}");
}

#[tokio::test]
async fn the_http_import_of_an_old_snapshot_still_rediscovers_vectors() {
    // The pre-#1506 behaviour, kept for files that say nothing about their
    // indexes: without it, vector search over an old snapshot finds nothing.
    let (app, store) = app();
    let body = post_snapshot(&app, &pre_1506_snapshot()).await;

    assert_eq!(body["vector_indices_rebuilt"], 1, "{body}");
    assert!(body["indexes_restored"].is_null(), "{body}");
    let s = store.read().await;
    let hits = s
        .vector_search("Doc", "emb", &[1.0, 0.0, 0.0], 2)
        .expect("search");
    assert_eq!(hits.len(), 2);
}
