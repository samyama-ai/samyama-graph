//! A bulk import is admitted or refused whole, before any of it is written (#1495).
//!
//! # What was wrong
//!
//! `#1483`/`#1492` moved the tenant quota to admission control, but it is
//! charged one row at a time, inside `create_edge*` and at four sites in the
//! write executor. A bulk import reaches none of them:
//!
//! - `/api/snapshot/import` takes `state.store.write()` directly instead of
//!   going through `AppState::mutate`, so `set_write_admission` is never
//!   called and the store's `admission` is `None`. **No ceiling of any kind**
//!   applied to that endpoint; the 64 GB request-body cap was the only limit
//!   on how much a caller could add, and nothing authenticates the caller by
//!   default (#1328).
//! - `/api/import/parquet` does go through `mutate`, so the ceiling is
//!   installed — but `parquet_to_nodes` calls `store.create_node`, which
//!   returns a `NodeId` and has nowhere to refuse. The ceiling was set and
//!   never consulted.
//!
//! CSV and JSON import were already covered: both call
//! `AppState::quota_refuses` with the row count up front. Both are node-only,
//! so passing `0` edges is correct for them and is not a third hole.
//!
//! # What these tests pin
//!
//! Both remaining paths ask the whole question once, from the count the file
//! itself declares — a snapshot header's `node_count`/`edge_count`, a Parquet
//! footer's `num_rows`. A refusal writes nothing, which is the property that
//! one-row-at-a-time enforcement cannot give a bulk path: a limit reached on
//! row 900 would otherwise leave 899 rows behind.

use samyama::graph::{GraphStore, WriteAdmission};

const T: &str = "default";

// ---------------------------------------------------------------------------
// GraphStore::admits_bulk — the primitive both paths use
// ---------------------------------------------------------------------------

fn store_admitting(max_nodes: Option<u64>, max_edges: Option<u64>, used: (u64, u64)) -> GraphStore {
    let mut s = GraphStore::new();
    s.set_write_admission(Some(WriteAdmission {
        nodes_used: used.0,
        max_nodes,
        edges_used: used.1,
        max_edges,
    }));
    s
}

#[test]
fn bulk_admission_refuses_a_batch_that_would_cross_the_node_ceiling() {
    let s = store_admitting(Some(1000), None, (900, 0));
    assert!(s.admits_bulk(50, 0).is_ok(), "950 of 1000 fits");
    let e = s.admits_bulk(200, 0).expect_err("1100 of 1000 must not fit");
    assert!(
        e.to_string().contains("1100/1000"),
        "the refusal must say how far over it is, got: {e}"
    );
}

#[test]
fn bulk_admission_refuses_a_batch_that_would_cross_the_edge_ceiling() {
    let s = store_admitting(None, Some(10), (0, 9));
    assert!(s.admits_bulk(1_000_000, 1).is_ok(), "10 of 10 fits, nodes uncapped");
    assert!(s.admits_bulk(0, 5).is_err(), "14 of 10 must not fit");
}

#[test]
fn bulk_admission_admits_anything_when_no_ceiling_is_configured() {
    // A store with no admission at all, which is what an unquotaed server has.
    let s = GraphStore::new();
    assert!(s.admits_bulk(u64::MAX, u64::MAX).is_ok());
    // And one with admission whose ceilings are None.
    let s = store_admitting(None, None, (10, 10));
    assert!(s.admits_bulk(u64::MAX, u64::MAX).is_ok());
}

#[test]
fn bulk_admission_counts_what_this_statement_has_already_created() {
    // `admission_births` is what the single-row path charges against. A bulk
    // check that ignored it would let a statement that already created rows
    // add a batch on top of them.
    let mut s = store_admitting(Some(10), None, (0, 0));
    for _ in 0..8 {
        s.create_node("N");
    }
    assert!(
        s.admits_bulk(5, 0).is_err(),
        "8 already created plus 5 is 13 against a ceiling of 10"
    );
}

// ---------------------------------------------------------------------------
// Parquet
// ---------------------------------------------------------------------------

/// A Parquet file of `rows` single-column rows.
fn parquet_bytes(rows: usize) -> Vec<u8> {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>()))],
    )
    .expect("batch");
    let mut out: Vec<u8> = Vec::new();
    {
        let mut w = ArrowWriter::try_new(&mut out, schema, None).expect("writer");
        w.write(&batch).expect("write");
        w.close().expect("close");
    }
    out
}

#[test]
fn a_parquet_file_over_the_ceiling_is_refused_and_writes_nothing() {
    let bytes = parquet_bytes(100);
    let mut s = store_admitting(Some(50), None, (0, 0));

    let err = samyama::export::import::parquet_to_nodes(&mut s, T, "Row", bytes)
        .expect_err("100 rows against a ceiling of 50 must be refused");

    assert!(
        matches!(err, samyama::export::ExportError::QuotaExceeded(_)),
        "a full graph is a quota refusal, not a malformed file: {err:?}"
    );
    // The property the row-at-a-time ceiling could not give: nothing landed.
    assert_eq!(
        s.node_count(),
        0,
        "a refused import must leave the graph exactly as it was"
    );
}

#[test]
fn a_parquet_file_that_fits_is_imported() {
    let bytes = parquet_bytes(100);
    let mut s = store_admitting(Some(1000), None, (0, 0));
    let stats = samyama::export::import::parquet_to_nodes(&mut s, T, "Row", bytes)
        .expect("100 rows under a ceiling of 1000");
    assert_eq!(stats.nodes_created, 100);
    assert_eq!(s.node_count(), 100);
}

#[test]
fn a_parquet_import_with_no_ceiling_is_unaffected() {
    let bytes = parquet_bytes(64);
    let mut s = GraphStore::new();
    let stats =
        samyama::export::import::parquet_to_nodes(&mut s, T, "Row", bytes).expect("no ceiling");
    assert_eq!(stats.nodes_created, 64);
}

// ---------------------------------------------------------------------------
// Snapshot header
// ---------------------------------------------------------------------------

/// A snapshot of a graph with `nodes` nodes and one edge per adjacent pair.
fn snapshot_bytes(nodes: usize) -> Vec<u8> {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..nodes).map(|_| s.create_node("N")).collect();
    for w in ids.windows(2) {
        s.create_edge(w[0], w[1], "NEXT").expect("edge");
    }
    let mut out: Vec<u8> = Vec::new();
    samyama::snapshot::export_tenant(&s, &mut out).expect("export");
    out
}

#[test]
fn peeking_a_snapshot_reads_the_counts_it_declares() {
    let bytes = snapshot_bytes(20);
    let h = samyama::snapshot::peek_header(std::io::Cursor::new(&bytes)).expect("header");
    assert_eq!(h.format, "sgsnap");
    assert_eq!(h.node_count, 20);
    assert_eq!(h.edge_count, 19);
}

#[test]
fn peeking_a_plaintext_snapshot_through_the_encryption_sniff_reads_the_same_counts() {
    let bytes = snapshot_bytes(7);
    let h = samyama::snapshot::peek_header_maybe_encrypted(std::io::Cursor::new(&bytes), None)
        .expect("plaintext is sniffed as plaintext");
    assert_eq!(h.node_count, 7);
    assert_eq!(h.edge_count, 6);
}

#[test]
fn peeking_refuses_a_file_that_is_not_a_snapshot() {
    assert!(samyama::snapshot::peek_header(std::io::Cursor::new(b"not gzip".to_vec())).is_err());
}

#[test]
fn peeking_does_not_read_the_body() {
    // The point of peeking is that a hundred-million-node file costs one line.
    // Truncating everything after the header must still yield the header — if
    // the peek consumed the body it would fail here.
    let bytes = snapshot_bytes(50);
    let full = samyama::snapshot::peek_header(std::io::Cursor::new(&bytes)).expect("header");
    assert_eq!(full.node_count, 50);

    // A header-only file: re-encode just line 0 of the same snapshot.
    use flate2::read::GzDecoder;
    use flate2::write::GzEncoder;
    use std::io::{BufRead, BufReader, Write};
    let mut line = String::new();
    BufReader::new(GzDecoder::new(std::io::Cursor::new(&bytes)))
        .read_line(&mut line)
        .expect("line 0");
    let mut enc = GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(line.as_bytes()).expect("write");
    let header_only = enc.finish().expect("finish");

    let h = samyama::snapshot::peek_header(std::io::Cursor::new(&header_only))
        .expect("a header with no body is still a readable header");
    assert_eq!(h.node_count, 50);
    assert_eq!(h.edge_count, 49);
}

// ---------------------------------------------------------------------------
// /api/snapshot/import end to end
//
// The endpoint #1495 names. Everything above is the machinery; this is the
// claim: an unauthenticated POST could add an unbounded number of nodes to a
// quotaed server, and now cannot.
// ---------------------------------------------------------------------------

fn multipart(bytes: &[u8]) -> (String, Vec<u8>) {
    let boundary = "----samyamatestboundary";
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"g.sgsnap\"\r\n\r\n",
    );
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    (
        format!("multipart/form-data; boundary={boundary}"),
        body,
    )
}

/// A router serving only `/api/snapshot/import`, over a store whose tenant has
/// the given node ceiling.
fn import_app(
    max_nodes: Option<usize>,
) -> (
    axum::Router,
    std::sync::Arc<tokio::sync::RwLock<GraphStore>>,
    tempfile::TempDir,
) {
    use samyama::persistence::tenant::ResourceQuotas;
    use samyama::persistence::PersistenceManager;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("tempdir");
    let pm = PersistenceManager::new(dir.path()).expect("persistence");
    pm.tenants()
        .update_quotas(
            T,
            ResourceQuotas { max_nodes, ..ResourceQuotas::unlimited() },
        )
        .expect("quotas");

    let store = Arc::new(tokio::sync::RwLock::new(GraphStore::new()));
    let state = samyama::http::server::AppState {
        store: store.clone(),
        engine: Arc::new(samyama::query::QueryEngine::new()),
        data_path: None,
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        persistence: Some(Arc::new(pm)),
        snapshot_key: None,
        transactions: Default::default(),
    };

    let app = axum::Router::new()
        .route(
            "/api/snapshot/import",
            axum::routing::post(samyama::http::handler::restore_snapshot_handler),
        )
        .with_state(state);
    (app, store, dir)
}

async fn post_snapshot(app: axum::Router, bytes: &[u8]) -> (axum::http::StatusCode, String) {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let (ctype, body) = multipart(bytes);
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/snapshot/import")
                .header("content-type", ctype)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let b = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&b).into_owned())
}

#[tokio::test]
async fn a_snapshot_over_the_node_quota_is_refused_and_the_graph_stays_empty() {
    let bytes = snapshot_bytes(100);
    let (app, store, _dir) = import_app(Some(10));

    let (status, body) = post_snapshot(app, &bytes).await;

    assert_eq!(
        status,
        axum::http::StatusCode::TOO_MANY_REQUESTS,
        "100 nodes against a ceiling of 10: {body}"
    );
    assert!(body.contains("quota exceeded"), "the refusal must say why: {body}");
    assert_eq!(
        store.read().await.node_count(),
        0,
        "the file was refused whole; not one node may have landed"
    );
}

#[tokio::test]
async fn a_snapshot_that_fits_the_quota_is_imported() {
    let bytes = snapshot_bytes(10);
    let (app, store, _dir) = import_app(Some(1000));

    let (status, body) = post_snapshot(app, &bytes).await;

    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(store.read().await.node_count(), 10);
}

#[tokio::test]
async fn a_snapshot_import_into_an_unquotaed_server_is_unaffected() {
    let bytes = snapshot_bytes(100);
    let (app, store, _dir) = import_app(None);

    let (status, body) = post_snapshot(app, &bytes).await;

    assert_eq!(status, axum::http::StatusCode::OK, "{body}");
    assert_eq!(store.read().await.node_count(), 100);
}
