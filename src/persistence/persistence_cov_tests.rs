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

#[test]
fn a_node_deleted_after_it_was_persisted_is_removed_from_disk() {
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let n = store.create_node("P");
    let log = store.take_write_log();
    m.apply_mutations("default", &store, &log).unwrap();
    assert_eq!(m.tenants().get_usage("default").unwrap().node_count, 1);

    store.delete_node("default", n).unwrap();
    let log = store.take_write_log();
    assert_eq!(m.apply_mutations("default", &store, &log).unwrap(), 1);
    assert!(m
        .storage()
        .get_node("default", n.as_u64())
        .unwrap()
        .is_none());
    assert_eq!(m.tenants().get_usage("default").unwrap().node_count, 0);
}

/// What one `apply_mutations` call cost, counted rather than timed (#1109).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ApplyOps {
    row_reads: u64,
    quota_checks: u64,
    wal_locks: u64,
    wal_appends: u64,
}

fn ops_now() -> ApplyOps {
    ApplyOps {
        row_reads: storage::ROW_READS.with(|c| c.get()),
        quota_checks: tenant::QUOTA_CHECKS.with(|c| c.get()),
        wal_locks: wal::WAL_LOCKS.with(|c| c.get()),
        wal_appends: wal::WAL_APPENDS.with(|c| c.get()),
    }
}

/// Run `apply_mutations` and return what it wrote and what it cost.
fn apply_counted(
    m: &PersistenceManager,
    tenant: &str,
    store: &GraphStore,
    log: &[Mutation],
) -> (Result<usize, PersistenceError>, ApplyOps) {
    let before = ops_now();
    let result = m.apply_mutations(tenant, store, log);
    let after = ops_now();
    let ops = ApplyOps {
        row_reads: after.row_reads - before.row_reads,
        quota_checks: after.quota_checks - before.quota_checks,
        wal_locks: after.wal_locks - before.wal_locks,
        wal_appends: after.wal_appends - before.wal_appends,
    };
    (result, ops)
}

/// A batch pays for its existence probe, its quota check and its WAL lock
/// once, not once per row (#1109).
///
/// Measured on the per-row code this replaced: 1,000 creates cost 1,000
/// probes, 1,000 quota checks and 1,000 lock acquisitions. The WAL still gets
/// one record per row — its format is unchanged — so `wal_appends` stays at
/// the row count by design.
#[test]
fn a_batch_costs_one_probe_one_quota_check_and_one_wal_lock() {
    const N: usize = 1_000;
    let (_dir, m) = manager();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let ids: Vec<NodeId> = (0..N)
        .map(|i| {
            let n = store.create_node("P");
            store
                .set_node_property("default", n, "i", i as i64)
                .unwrap();
            n
        })
        .collect();
    let log = store.take_write_log();
    let (written, ops) = apply_counted(&m, "default", &store, &log);
    eprintln!("#1109 {N} creates: {ops:?}");
    assert_eq!(written.unwrap(), N);
    assert_eq!(
        ops,
        ApplyOps {
            row_reads: 1,
            quota_checks: 1,
            wal_locks: 1,
            wal_appends: N as u64
        },
        "per batch, not per row"
    );
    assert_eq!(m.tenants().get_usage("default").unwrap().node_count, N);

    // Mixed: every existing node updated, N new nodes and N new edges between
    // them. Still one of each, and the counter moves by the new ids only.
    for &n in &ids {
        store.set_node_property("default", n, "seen", true).unwrap();
    }
    for &a in &ids {
        let b = store.create_node("Q");
        store.create_edge(a, b, "R").unwrap();
    }
    let log = store.take_write_log();
    let (written, ops) = apply_counted(&m, "default", &store, &log);
    eprintln!("#1109 {N} updates + {N} node creates + {N} edge creates: {ops:?}");
    assert_eq!(written.unwrap(), 3 * N);
    assert_eq!(
        ops,
        ApplyOps {
            row_reads: 2,
            quota_checks: 1,
            wal_locks: 1,
            wal_appends: 3 * N as u64
        },
        "one probe per kind of row, one quota check, one lock"
    );
    let usage = m.tenants().get_usage("default").unwrap();
    assert_eq!((usage.node_count, usage.edge_count), (2 * N, N));
    let stored = m
        .storage()
        .get_node("default", ids[7].as_u64())
        .unwrap()
        .unwrap();
    assert_eq!(stored.properties.get("i"), Some(&PropertyValue::Integer(7)));
    assert_eq!(
        stored.properties.get("seen"),
        Some(&PropertyValue::Boolean(true))
    );
}

/// A mixed batch — creates, updates, deletes, nodes and edges — survives a
/// restart exactly as it was written (#1109).
#[test]
fn a_mixed_batch_reads_back_identically_after_a_restart() {
    let dir = TempDir::new().unwrap();
    let mut store = GraphStore::new();
    store.enable_write_log();
    let (usage_before, nodes_before, edges_before) = {
        let m = PersistenceManager::new(dir.path()).unwrap();
        let a = store.create_node("P");
        let b = store.create_node("P");
        let gone = store.create_node("P");
        let e = store.create_edge(a, b, "R").unwrap();
        let log = store.take_write_log();
        m.apply_mutations("default", &store, &log).unwrap();

        // One batch: update a, create c and an edge to it, delete `gone`,
        // re-type nothing, set an edge property, and create-then-delete d.
        store
            .set_node_property("default", a, "name", "Ann")
            .unwrap();
        let c = store.create_node("Q");
        store.set_node_property("default", c, "n", 3i64).unwrap();
        store.create_edge(b, c, "S").unwrap();
        store.set_edge_property(e, "w", 1.5f64).unwrap();
        store.delete_node("default", gone).unwrap();
        let d = store.create_node("Tmp");
        store.delete_node("default", d).unwrap();
        let log = store.take_write_log();
        assert_eq!(m.apply_mutations("default", &store, &log).unwrap(), 5);
        m.flush().unwrap();

        let usage = m.tenants().get_usage("default").unwrap();
        let (mut nodes, mut edges) = m.recover("default").unwrap();
        nodes.sort_by_key(|n| n.id);
        edges.sort_by_key(|e| e.id);
        ((usage.node_count, usage.edge_count), nodes, edges)
    };
    assert_eq!(usage_before, (3, 2));

    let m = PersistenceManager::new(dir.path()).unwrap();
    let mut back = GraphStore::new();
    let (n, e, _) = m.recover_into("default", &mut back).unwrap();
    assert_eq!((n, e), (store.node_count(), store.edge_count()));
    let usage = m.tenants().get_usage("default").unwrap();
    assert_eq!((usage.node_count, usage.edge_count), usage_before);
    let (mut nodes, mut edges) = m.recover("default").unwrap();
    nodes.sort_by_key(|n| n.id);
    edges.sort_by_key(|e| e.id);
    let key = |n: &Node| (n.id, n.labels.clone(), n.properties.clone());
    assert_eq!(
        nodes.iter().map(key).collect::<Vec<_>>(),
        nodes_before.iter().map(key).collect::<Vec<_>>()
    );
    let ekey = |e: &Edge| {
        (
            e.id,
            e.source,
            e.target,
            e.edge_type.clone(),
            e.properties.clone(),
        )
    };
    assert_eq!(
        edges.iter().map(ekey).collect::<Vec<_>>(),
        edges_before.iter().map(ekey).collect::<Vec<_>>()
    );
    // And the disk agrees with the store it came from.
    for node in &nodes {
        let live = store.node_materialized(node.id).expect("in the store");
        assert_eq!(node.properties, live.properties);
    }
}

/// A batch that crosses the quota at persist time stops at it: the rows
/// before the ceiling are written and counted, the one that hit it and every
/// row after it are not on disk at all (#1274, #1109).
///
/// This is the per-row code's behaviour and it is kept: the check moved from
/// every row to once per batch, and the batch falls back to checking each new
/// id only when the one check says it will not all fit.
#[test]
fn a_batch_that_crosses_the_quota_at_persist_time_writes_nothing_past_it() {
    let (_dir, m) = manager();
    let quotas = ResourceQuotas {
        max_nodes: Some(5),
        ..ResourceQuotas::unlimited()
    };
    m.tenants().update_quotas("default", quotas).unwrap();

    let mut store = GraphStore::new();
    store.enable_write_log();
    let first = store.create_node("P");
    let log = store.take_write_log();
    m.apply_mutations("default", &store, &log).unwrap();

    // An update to `first` and eight new nodes against four free slots.
    store
        .set_node_property("default", first, "x", 1i64)
        .unwrap();
    let new: Vec<NodeId> = (0..8).map(|_| store.create_node("P")).collect();
    let log = store.take_write_log();
    let (result, ops) = apply_counted(&m, "default", &store, &log);
    let err = result.unwrap_err();
    assert!(err.to_string().contains("nodes (5/5)"), "{err}");

    let on_disk = |id: NodeId| m.storage().get_node("default", id.as_u64()).unwrap();
    assert_eq!(
        on_disk(first).unwrap().properties.get("x"),
        Some(&PropertyValue::Integer(1)),
        "the update ahead of the ceiling was written"
    );
    for (i, &n) in new.iter().enumerate() {
        assert_eq!(on_disk(n).is_some(), i < 4, "new node {i}");
    }
    assert_eq!(m.tenants().get_usage("default").unwrap().node_count, 5);
    assert_eq!(
        ops.wal_appends, 5,
        "one WAL record each for the update and the four admitted nodes, none for the refused"
    );

    // A delete earlier in a batch frees the slot a later create uses, as it
    // did when each row was checked on its own. `new[5]` is one of the refused
    // nodes: still in the store, not on disk.
    store.delete_node("default", new[0]).unwrap();
    store.take_write_log();
    let log = [
        Mutation::NodeDeleted(new[0]),
        Mutation::NodeUpserted(new[5]),
    ];
    assert_eq!(m.apply_mutations("default", &store, &log).unwrap(), 2);
    assert!(on_disk(new[0]).is_none());
    assert!(on_disk(new[5]).is_some());
    assert_eq!(m.tenants().get_usage("default").unwrap().node_count, 5);
}

/// Usage collected over a batch and applied once lands where applying each
/// change in turn would have, including a decrement that stopped at zero
/// part way through (#1109).
#[test]
fn a_usage_run_applied_once_matches_applying_each_step() {
    let (_dir, m) = manager();
    let t = m.tenants();
    let walks: [&[i64]; 5] = [
        &[1, 1, -1],
        &[-1, 1],
        &[-1, -1, 1, 1, 1],
        &[1, -1, -1, -1, 1],
        &[],
    ];
    for start in [0usize, 1, 3] {
        for walk in walks {
            t.set_usage("default", "nodes", start).unwrap();
            for &s in walk {
                if s > 0 {
                    t.increment_usage("default", "nodes", 1).unwrap();
                } else {
                    t.decrement_usage("default", "nodes", 1).unwrap();
                }
            }
            let stepwise = t.get_usage("default").unwrap().node_count;

            t.set_usage("default", "nodes", start).unwrap();
            let mut run = UsageRun::default();
            walk.iter().for_each(|&s| run.step(s));
            t.apply_usage_run("default", "nodes", run.net, run.low)
                .unwrap();
            assert_eq!(
                t.get_usage("default").unwrap().node_count,
                stepwise,
                "start {start}, walk {walk:?}"
            );
        }
    }
}
