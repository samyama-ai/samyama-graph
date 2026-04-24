//! HA-08: Snapshot import persistence (survive restart).
//!
//! These tests assert that after a snapshot is imported and persisted,
//! the data is recoverable on a fresh server boot without re-uploading.

use samyama::graph::GraphStore;
use samyama::snapshot::{export_tenant, persist};

/// Build a small graph and return an exported .sgsnap byte blob.
fn build_sample_snapshot() -> Vec<u8> {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    store.get_node_mut(a).unwrap().set_property("name", "Alice");
    let b = store.create_node("Person");
    store.get_node_mut(b).unwrap().set_property("name", "Bob");
    store.create_edge(a, b, "KNOWS").unwrap();

    let mut buf = Vec::new();
    export_tenant(&store, &mut buf).expect("export");
    buf
}

#[test]
fn persist_then_restore_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().to_string_lossy().to_string();
    let bytes = build_sample_snapshot();

    // Persist.
    persist::persist_snapshot(&data_path, &bytes).expect("persist");

    // Simulate restart: fresh empty store, restore from the same dir.
    let mut restored = GraphStore::new();
    let stats = persist::restore_persisted_snapshots(&data_path, &mut restored)
        .expect("restore")
        .expect("some snapshot found");

    assert_eq!(stats.node_count, 2);
    assert_eq!(stats.edge_count, 1);
    assert_eq!(restored.node_count(), 2);
    assert_eq!(restored.edge_count(), 1);
}

#[test]
fn restore_is_noop_when_no_snapshot_dir() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().to_string_lossy().to_string();
    let mut store = GraphStore::new();

    let result = persist::restore_persisted_snapshots(&data_path, &mut store).expect("ok");
    assert!(result.is_none(), "no snapshot should yield None");
    assert_eq!(store.node_count(), 0);
}

#[test]
fn restore_skips_partial_write_without_marker() {
    // Simulate a crash mid-flush: .sgsnap exists but marker file does not.
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().to_string_lossy().to_string();
    let snap_dir = std::path::PathBuf::from(&data_path).join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();

    // Write a malformed-but-present snapshot file, no marker.
    std::fs::write(snap_dir.join("default.sgsnap"), b"partial junk").unwrap();

    let mut store = GraphStore::new();
    let result = persist::restore_persisted_snapshots(&data_path, &mut store).expect("ok");
    assert!(result.is_none(), "partial snapshot without marker must be ignored");
    assert_eq!(store.node_count(), 0);
}

#[test]
fn persist_is_atomic_no_partial_file() {
    // After persist_snapshot returns Ok, both the .sgsnap and marker exist.
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().to_string_lossy().to_string();
    let bytes = build_sample_snapshot();

    persist::persist_snapshot(&data_path, &bytes).expect("persist");

    let snap = std::path::PathBuf::from(&data_path)
        .join("snapshots")
        .join("default.sgsnap");
    let marker = std::path::PathBuf::from(&data_path)
        .join("snapshots")
        .join("default.sgsnap.committed");
    assert!(snap.exists(), ".sgsnap should exist");
    assert!(marker.exists(), "committed marker should exist");
    // No leftover temp file.
    let tmp_file = std::path::PathBuf::from(&data_path)
        .join("snapshots")
        .join("default.sgsnap.tmp");
    assert!(!tmp_file.exists(), "temp file should be renamed away");
}

#[test]
fn persist_overwrites_previous_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().to_string_lossy().to_string();

    // First import: 2 nodes.
    persist::persist_snapshot(&data_path, &build_sample_snapshot()).unwrap();

    // Second import: different graph, 3 nodes.
    let bigger = {
        let mut s = GraphStore::new();
        for i in 0..3 {
            let n = s.create_node("T");
            s.get_node_mut(n).unwrap().set_property("i", i as i64);
        }
        let mut buf = Vec::new();
        export_tenant(&s, &mut buf).unwrap();
        buf
    };
    persist::persist_snapshot(&data_path, &bigger).unwrap();

    let mut restored = GraphStore::new();
    let stats = persist::restore_persisted_snapshots(&data_path, &mut restored)
        .unwrap()
        .unwrap();
    assert_eq!(stats.node_count, 3, "latest snapshot wins");
    assert_eq!(restored.node_count(), 3);
}
