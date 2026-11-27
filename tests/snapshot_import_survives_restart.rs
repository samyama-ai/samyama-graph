//! A snapshot import survives a restart (#2348).
//!
//! # What was wrong
//!
//! `/api/snapshot/import` did not go through the mutation journal. It kept the
//! uploaded bytes under `data_path/snapshots` and replayed them at boot, but
//! only when RocksDB recovered nothing. A server that had any ordinary write
//! recovered that write, skipped the side file, and came back without the
//! import. The Snap smoke test found it as "data lost on restart".
//!
//! # What these tests pin
//!
//! With a persistence layer the import is journalled like any other write:
//! what it created is in RocksDB when the reply is sent, a restart recovers it
//! next to the graph's other writes, and later edits to imported nodes land on
//! the same rows. The side file is no longer written when there is somewhere
//! better to keep the data.

use std::sync::Arc;

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::persistence::tenant::ResourceQuotas;
use samyama::persistence::PersistenceManager;

const T: &str = "default";

/// A snapshot of `nodes` `:Imported` nodes, each with its index as `i`, and
/// one `NEXT` edge per adjacent pair.
fn snapshot_bytes(nodes: usize) -> Vec<u8> {
    let mut s = GraphStore::new();
    let ids: Vec<_> = (0..nodes)
        .map(|i| {
            let id = s.create_node("Imported");
            s.set_node_property(T, id, "i", i as i64).expect("property");
            id
        })
        .collect();
    for w in ids.windows(2) {
        s.create_edge(w[0], w[1], "NEXT").expect("edge");
    }
    let mut out = Vec::new();
    samyama::snapshot::export_tenant(&s, &mut out).expect("export");
    out
}

struct Server {
    state: samyama::http::server::AppState,
    pm: Arc<PersistenceManager>,
}

/// A server over the persistence directory `dir`, holding what a restart
/// recovers from it, with `side` as its data path for the old side file.
fn boot(dir: &std::path::Path, side: &std::path::Path, max_nodes: Option<usize>) -> Server {
    let pm = Arc::new(PersistenceManager::new(dir).expect("persistence"));
    if max_nodes.is_some() {
        pm.tenants()
            .update_quotas(T, ResourceQuotas { max_nodes, ..ResourceQuotas::unlimited() })
            .expect("quotas");
    }
    let mut store = GraphStore::new();
    pm.recover_into(T, &mut store).expect("recover");
    let state = samyama::http::server::AppState {
        store: Arc::new(tokio::sync::RwLock::new(store)),
        engine: Arc::new(samyama::query::QueryEngine::new()),
        data_path: Some(side.to_string_lossy().into_owned()),
        tenant_manager: None,
        embed_pipeline: None,
        embed_cache: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        persistence: Some(pm.clone()),
        snapshot_key: None,
        transactions: Default::default(),
    };
    Server { state, pm }
}

/// Stop the server: every handle on RocksDB is dropped, so the next `boot`
/// over the same directory opens it the way a new process would.
fn stop(server: Server) {
    let Server { state, pm } = server;
    pm.flush().expect("flush");
    drop(state);
    assert_eq!(Arc::strong_count(&pm), 1, "something still holds the database");
    drop(pm);
}

async fn import(state: &samyama::http::server::AppState, bytes: &[u8]) -> (u16, String) {
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let boundary = "----samyamatestboundary";
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"g.sgsnap\"\r\n\r\n",
    );
    body.extend_from_slice(bytes);
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

    let app = axum::Router::new()
        .route(
            "/api/snapshot/import",
            axum::routing::post(samyama::http::handler::restore_snapshot_handler),
        )
        .with_state(state.clone());
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/snapshot/import")
                .header("content-type", format!("multipart/form-data; boundary={boundary}"))
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status().as_u16();
    let b = res.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&b).into_owned())
}

fn imported_ids(store: &GraphStore) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = store
        .get_nodes_by_label(&Label::new("Imported"))
        .iter()
        .map(|n| n.id)
        .collect();
    ids.sort_by_key(|id| id.as_u64());
    ids
}

#[tokio::test]
async fn an_import_survives_a_restart_next_to_an_ordinary_write() {
    let dir = tempfile::tempdir().unwrap();
    let side = tempfile::tempdir().unwrap();

    let server = boot(dir.path(), side.path(), None);
    // The case that lost the import: RocksDB holds something, so a restart
    // recovers from it rather than from the side file.
    server.state.mutate(T, |s| { s.create_node("Before"); }).await;
    let (status, body) = import(&server.state, &snapshot_bytes(10)).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(server.state.store.read().await.node_count(), 11);
    stop(server);

    let server = boot(dir.path(), side.path(), None);
    let store = server.state.store.read().await;
    assert_eq!(store.node_count(), 11, "the ordinary write and all ten imported nodes");
    assert_eq!(store.edge_count(), 9, "the imported edges too");
    assert_eq!(store.get_nodes_by_label(&Label::new("Before")).len(), 1);
    let mut is: Vec<_> = imported_ids(&store)
        .into_iter()
        .map(|id| store.node_property(id, "i"))
        .collect();
    is.sort_by_key(|v| format!("{v:?}"));
    assert_eq!(is.len(), 10);
    assert!(
        is.iter().all(|v| matches!(v, Some(PropertyValue::Integer(_)))),
        "imported properties come back with the rows: {is:?}"
    );
    drop(store);
    stop(server);
}

#[tokio::test]
async fn edits_to_imported_nodes_after_the_import_survive_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let side = tempfile::tempdir().unwrap();

    let server = boot(dir.path(), side.path(), None);
    let (status, body) = import(&server.state, &snapshot_bytes(3)).await;
    assert_eq!(status, 200, "{body}");
    let ids = imported_ids(&*server.state.store.read().await);
    let (kept, gone) = (ids[0], ids[2]);
    server
        .state
        .mutate(T, |s| {
            s.set_node_property(T, kept, "edited", true).unwrap();
            s.delete_node(T, gone).unwrap();
        })
        .await;
    stop(server);

    // The edits name the rows the import journalled, so a restart applies
    // them to those rows: nothing is resurrected and nothing is duplicated.
    let server = boot(dir.path(), side.path(), None);
    let store = server.state.store.read().await;
    assert_eq!(imported_ids(&store), vec![ids[0], ids[1]]);
    assert_eq!(
        store.node_property(kept, "edited"),
        Some(PropertyValue::Boolean(true))
    );
    drop(store);
    stop(server);
}

#[tokio::test]
async fn a_refused_import_leaves_nothing_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let side = tempfile::tempdir().unwrap();

    let server = boot(dir.path(), side.path(), Some(5));
    let (status, body) = import(&server.state, &snapshot_bytes(10)).await;
    assert_eq!(status, 429, "{body}");
    stop(server);

    let server = boot(dir.path(), side.path(), Some(5));
    assert_eq!(server.state.store.read().await.node_count(), 0);
    stop(server);
}

#[tokio::test]
async fn with_a_persistence_layer_no_side_file_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let side = tempfile::tempdir().unwrap();

    let server = boot(dir.path(), side.path(), None);
    let (status, body) = import(&server.state, &snapshot_bytes(4)).await;
    assert_eq!(status, 200, "{body}");
    stop(server);

    // The rows are in RocksDB; a second copy of the upload would only be
    // replayed on a boot that recovered nothing, which can no longer happen
    // to a server that has it.
    assert!(
        !side.path().join("snapshots").exists(),
        "the import was kept twice"
    );
}
