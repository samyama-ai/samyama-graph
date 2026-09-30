//! Coverage-focused tests for `PersistenceManager`: admission ceilings, the
//! session-transaction commit and its failure path, edge mutations, the index
//! catalog round trip, graph drops and vector checkpoints.

use super::*;
use crate::graph::event::Mutation;
use crate::graph::{EdgeId, Label, NodeId, PropertyValue};
use crate::persistence::tenant::ResourceQuotas;
use tempfile::TempDir;

fn manager() -> (TempDir, PersistenceManager) {
    let dir = TempDir::new().unwrap();
    let m = PersistenceManager::new(dir.path()).unwrap();
    (dir, m)
}

#[test]
fn write_admission_reflects_the_tenants_quota_and_usage() {
    let (_dir, m) = manager();
    let a = m
        .write_admission("default")
        .expect("default tenant has quotas");
    assert_eq!(a.max_nodes, Some(1_000_000));
    assert_eq!(a.nodes_used, 0);
    m.tenants().set_usage("default", "nodes", 5).unwrap();
    assert_eq!(m.write_admission("default").unwrap().nodes_used, 5);

    m.tenants()
        .create_tenant(
            "free".into(),
            "Free".into(),
            Some(ResourceQuotas::unlimited()),
        )
        .unwrap();
    assert_eq!(
        m.write_admission("free"),
        None,
        "no ceiling admits everything"
    );
    assert_eq!(m.write_admission("nobody"), None);
    assert!(Arc::ptr_eq(&m.tenants_arc(), &m.tenants_arc()));
}

#[test]
fn commit_without_an_open_transaction_is_refused() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    let err = m
        .commit_session_transaction("default", &mut store)
        .unwrap_err();
    assert!(err.contains("not found"), "{err}");
}

#[test]
fn a_committed_session_transaction_reaches_the_disk() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let v = store.begin_session_transaction().unwrap();
    let n = store.create_node("Person");
    store
        .set_node_property("default", n, "name", "Ann")
        .unwrap();
    assert_eq!(m.commit_session_transaction("default", &mut store), Ok(v));
    assert_eq!(store.session_transaction_version(), None);
    let stored = m
        .storage()
        .get_node("default", n.as_u64())
        .unwrap()
        .expect("persisted");
    assert_eq!(
        stored.properties.get("name"),
        Some(&PropertyValue::String("Ann".into()))
    );
}

#[test]
fn a_session_transaction_that_cannot_be_persisted_is_rolled_back() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let kept = store.create_node("Person");
    let log = store.take_write_log();
    m.apply_mutations("default", &store, &log).unwrap();

    store.begin_session_transaction().unwrap();
    let n = store.create_node("Person");
    store
        .set_node_property("default", kept, "name", "changed")
        .unwrap();
    m.fail_next_apply_for_test();
    let err = m
        .commit_session_transaction("default", &mut store)
        .unwrap_err();
    assert!(err.contains("was rolled back"), "{err}");
    assert!(
        store.get_node(n).is_none(),
        "the transaction's node is gone"
    );
    assert_eq!(
        store.node_property(kept, "name"),
        None,
        "the write was undone"
    );
    assert!(m
        .storage()
        .get_node("default", n.as_u64())
        .unwrap()
        .is_none());
    assert!(m
        .storage()
        .get_node("default", kept.as_u64())
        .unwrap()
        .is_some());
}

#[test]
fn edge_mutations_are_written_and_deleted() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    store.set_edge_property(e, "since", 2001i64).unwrap();
    let log = store.take_write_log();
    let written = m.apply_mutations("default", &store, &log).unwrap();
    assert_eq!(written, 3, "two nodes and one edge, each once");
    let stored = m
        .storage()
        .get_edge("default", e.as_u64())
        .unwrap()
        .expect("edge persisted");
    assert_eq!(
        stored.properties.get("since"),
        Some(&PropertyValue::Integer(2001))
    );
    assert_eq!(m.tenants().get_usage("default").unwrap().edge_count, 1);

    store.delete_edge(e).unwrap();
    let log = store.take_write_log();
    assert!(log.contains(&Mutation::EdgeDeleted(e)));
    assert_eq!(m.apply_mutations("default", &store, &log).unwrap(), 1);
    assert!(m
        .storage()
        .get_edge("default", e.as_u64())
        .unwrap()
        .is_none());

    // An upsert of an edge that no longer exists, and a delete of one never
    // stored, both write nothing.
    let ghost = EdgeId::new(500);
    let n = m
        .apply_mutations(
            "default",
            &store,
            &[Mutation::EdgeUpserted(ghost), Mutation::EdgeDeleted(ghost)],
        )
        .unwrap();
    assert_eq!(n, 0);
    assert_eq!(
        m.apply_mutations(
            "default",
            &store,
            &[Mutation::NodeDeleted(NodeId::new(900))]
        )
        .unwrap(),
        0
    );
}

#[test]
fn a_dirty_index_catalog_is_persisted_with_the_next_statement() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.create_property_index(&Label::new("P"), "name");
    assert!(store.index_catalog_is_dirty());
    assert_eq!(m.apply_mutations("default", &store, &[]).unwrap(), 0);
    assert!(!store.index_catalog_is_dirty());
    let catalog = m.load_index_catalog("default").unwrap();
    assert_eq!(catalog.len(), 1);

    store.create_fulltext_index("ft", "P", "bio");
    m.persist_index_catalog("default", &store).unwrap();
    assert!(!store.index_catalog_is_dirty());
    assert_eq!(m.load_index_catalog("default").unwrap().len(), 2);
    assert!(m.load_index_catalog("other").unwrap().is_empty());
}

#[test]
fn recover_into_rebuilds_rows_then_indexes_and_skips_dangling_edges() {
    let (_dir, m) = manager();
    let mut src = GraphStore::new();
    src.enable_write_log();
    let a = src.create_node("P");
    src.set_node_property("default", a, "name", "Ann").unwrap();
    src.create_property_index(&Label::new("P"), "name");
    let log = src.take_write_log();
    m.apply_mutations("default", &src, &log).unwrap();
    // An edge whose endpoints were never stored.
    let dangling =
        crate::graph::Edge::new(EdgeId::new(77), NodeId::new(100), NodeId::new(101), "R");
    m.persist_create_edge("default", &dangling).unwrap();

    let mut dst = GraphStore::new();
    let (nodes, edges, indexes) = m.recover_into("default", &mut dst).unwrap();
    assert_eq!((nodes, edges), (1, 1));
    assert_eq!(dst.edge_count(), 0, "the dangling edge is not inserted");
    assert_eq!(indexes.property, 1);
    let idx = dst
        .property_index
        .get_index(&Label::new("P"), "name")
        .unwrap();
    assert_eq!(
        idx.read()
            .unwrap()
            .get(&PropertyValue::String("Ann".into())),
        vec![a]
    );
}

#[test]
fn drop_graph_removes_the_rows_and_zeroes_usage() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let a = store.create_node("P");
    let b = store.create_node("P");
    store.create_edge(a, b, "R").unwrap();
    let log = store.take_write_log();
    m.apply_mutations("default", &store, &log).unwrap();
    assert!(m
        .list_persisted_tenants()
        .unwrap()
        .contains(&"default".to_string()));

    m.drop_graph("default").unwrap();
    let (nodes, edges) = m.recover("default").unwrap();
    assert!(nodes.is_empty() && edges.is_empty());
    let usage = m.tenants().get_usage("default").unwrap();
    assert_eq!((usage.node_count, usage.edge_count), (0, 0));
}

#[test]
fn checkpoints_flush_and_vectors_round_trip() {
    let (_dir, m) = manager();
    m.set_wal_sync_for_test(true);
    let mut store = GraphStore::new();
    store.enable_write_log();
    store.create_node("P");
    let log = store.take_write_log();
    m.apply_mutations("default", &store, &log).unwrap();
    m.checkpoint().unwrap();
    m.flush().unwrap();

    let vi = crate::vector::VectorIndexManager::new();
    vi.create_index("P", "emb", 2, crate::vector::DistanceMetric::Cosine)
        .unwrap();
    vi.add_vector("P", "emb", NodeId::new(1), &vec![1.0, 0.0])
        .unwrap();
    m.checkpoint_vectors(&vi).unwrap();
    let back = crate::vector::VectorIndexManager::new();
    m.recover_vectors(&back).unwrap();
    assert_eq!(
        back.search("P", "emb", &[1.0, 0.0], 1).unwrap()[0].0,
        NodeId::new(1)
    );
}

#[tokio::test]
async fn start_indexer_indexes_vectors_from_the_channel() {
    let (_dir, m) = manager();
    let (mut inner, rx) = GraphStore::with_async_indexing();
    inner
        .create_vector_index("Doc", "v", 2, crate::vector::DistanceMetric::Cosine)
        .unwrap();
    let vi = Arc::clone(&inner.vector_index);
    let n = inner.create_node("Doc");
    inner
        .set_node_property("default", n, "v", PropertyValue::Vector(vec![0.0, 1.0]))
        .unwrap();
    let store = Arc::new(tokio::sync::RwLock::new(inner));
    m.start_indexer(Arc::clone(&store), rx);

    let mut found = false;
    for _ in 0..500 {
        if vi
            .get_index("Doc", "v")
            .map(|i| i.read().unwrap().len())
            .unwrap_or(0)
            == 1
        {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert!(found, "the background indexer added the vector");
    assert_eq!(vi.search("Doc", "v", &[0.0, 1.0], 1).unwrap()[0].0, n);
}
