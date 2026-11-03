//! A restart recovers from RocksDB alone; the logical WAL is not part of it
//! (#1592).
//!
//! Samyama writes two logs: its own logical WAL under `wal/`, and RocksDB's
//! physical one inside `data/`. The module comment said writes went through
//! the logical WAL first and that it was replayed on start-up. Neither was
//! true: recovery scans RocksDB, which replays its own log when it opens. The
//! decision recorded in ADR-023's amendment is to make that the stated design,
//! and these tests pin it -- the logical WAL can be deleted, or overwritten with
//! garbage, and a restart finds exactly what was persisted.
//!
//! What this does not cover is RocksDB's own log after a crash of the process;
//! that is the SIGKILL test (`docs/FAILURE-MODES.md` row 34).

use samyama::graph::{GraphStore, Label};
use samyama::persistence::PersistenceManager;
use samyama::query::QueryEngine;
use std::path::Path;

const T: &str = "default";

/// Persist a small graph the way the server does, then close the manager
/// without a checkpoint, so nothing but the ordinary write path has run.
fn persist_a_graph(dir: &Path) {
    let pm = PersistenceManager::new(dir).unwrap();
    let engine = QueryEngine::new();
    let mut store = GraphStore::new();
    store.enable_write_log();
    for q in [
        "CREATE (:Person {name: 'ada', born: 1815})",
        "CREATE (:Person {name: 'charles', born: 1791})",
        "CREATE (:Person {name: 'gone'})",
        "MATCH (a:Person {name: 'ada'}), (c:Person {name: 'charles'}) CREATE (a)-[:KNEW {since: 1833}]->(c)",
        "MATCH (a:Person {name: 'ada'}) SET a.title = 'countess'",
        "MATCH (g:Person {name: 'gone'}) DELETE g",
    ] {
        engine.execute_mut(q, &mut store, T).expect(q);
        let muts = store.take_write_log();
        pm.apply_mutations(T, &store, &muts).expect(q);
    }
}

/// What a restart on `dir` finds, as the server's start-up builds it.
fn restart(dir: &Path) -> GraphStore {
    let pm = PersistenceManager::new(dir).unwrap();
    let mut store = GraphStore::new();
    pm.recover_into(T, &mut store).unwrap();
    store
}

fn assert_the_graph_is_whole(store: &GraphStore) {
    assert_eq!(store.get_nodes_by_label(&Label::new("Person")).len(), 2, "a node was lost or a deleted one came back");
    assert_eq!(store.edge_count(), 1, "the relationship was lost");
    let r = QueryEngine::new()
        .execute(
            "MATCH (a:Person {name: 'ada'})-[k:KNEW]->(c:Person) RETURN a.title AS t, a.born AS b, k.since AS s, c.name AS c",
            store,
        )
        .unwrap();
    assert_eq!(r.records.len(), 1, "the pattern did not survive the restart");
    let row = &r.records[0];
    let get = |k: &str| row.get(k).and_then(|v| v.as_property()).map(|p| p.to_string()).unwrap_or_default();
    assert!(get("t").contains("countess"), "the SET was lost: {}", get("t"));
    assert_eq!(get("b"), "1815");
    assert_eq!(get("s"), "1833");
    assert!(get("c").contains("charles"));
}

fn wal_logs(dir: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect()
}

#[test]
fn a_restart_with_the_logical_wal_intact_finds_the_graph() {
    // The control: without it, the two tests below could pass on a write path
    // that persisted nothing and a check that looked at nothing.
    let dir = tempfile::tempdir().unwrap();
    persist_a_graph(dir.path());
    assert!(!wal_logs(dir.path()).is_empty(), "the write path no longer writes the logical WAL");
    assert_the_graph_is_whole(&restart(dir.path()));
}

#[test]
fn a_restart_with_the_logical_wal_deleted_loses_nothing() {
    let dir = tempfile::tempdir().unwrap();
    persist_a_graph(dir.path());
    std::fs::remove_dir_all(dir.path().join("wal")).unwrap();
    assert_the_graph_is_whole(&restart(dir.path()));
}

#[test]
fn a_restart_with_the_logical_wal_overwritten_loses_nothing() {
    // Garbage rather than absence: a recovery that read the log and tolerated a
    // missing one would pass the test above and fail this one.
    let dir = tempfile::tempdir().unwrap();
    persist_a_graph(dir.path());
    let logs = wal_logs(dir.path());
    assert!(!logs.is_empty());
    for log in logs {
        let len = std::fs::metadata(&log).unwrap().len() as usize;
        std::fs::write(&log, vec![0xA5u8; len.max(64)]).unwrap();
    }
    assert_the_graph_is_whole(&restart(dir.path()));
}
