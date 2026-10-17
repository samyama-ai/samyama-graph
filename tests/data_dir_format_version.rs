//! A data directory says which format wrote it, and is refused when this
//! engine cannot read it (#1657, REL-13).
//!
//! 0 of the 8 files a fresh directory held carried a Samyama format version, and
//! a directory the engine could not open was served as an empty database: the
//! process printed the error and kept running with persistence off. These tests
//! pin the half of that which lives in the library -- the version is written,
//! read and enforced, and a refused directory is left as it was. The server's
//! half, exiting instead of serving, is `refuse_to_start` in `main.rs`.

use samyama::persistence::format::{FORMAT_FILE, FORMAT_VERSION};
use samyama::persistence::PersistenceManager;

/// One node written through the persistence path the server uses.
fn write_one_node(pm: &PersistenceManager) {
    let mut store = samyama::graph::GraphStore::new();
    store.enable_write_log();
    let id = store.create_node("Person");
    store.set_node_property("default", id, "name", "Ada").expect("set");
    let mutations = store.take_write_log();
    pm.apply_mutations("default", &store, &mutations).expect("write");
}

fn entries(dir: &std::path::Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[test]
fn a_new_directory_is_stamped_with_this_format() {
    let dir = tempfile::tempdir().unwrap();
    drop(PersistenceManager::new(dir.path()).expect("open"));
    let text = std::fs::read_to_string(dir.path().join(FORMAT_FILE)).expect("stamped");
    assert!(text.contains(&format!("format = {FORMAT_VERSION}")), "{text}");
}

#[test]
fn a_directory_from_a_newer_format_is_refused_and_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let newer = format!("format = {}\nwritten_by = 99.0.0\n", FORMAT_VERSION + 1);
    std::fs::write(dir.path().join(FORMAT_FILE), &newer).unwrap();
    let before = entries(dir.path());

    let err = match PersistenceManager::new(dir.path()) {
        Ok(_) => panic!("a newer format was opened"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&format!("format {}", FORMAT_VERSION + 1)), "{err}");

    // Nothing created, nothing rewritten: the engine that wrote it can still
    // open it exactly as it left it.
    assert_eq!(entries(dir.path()), before);
    assert_eq!(std::fs::read_to_string(dir.path().join(FORMAT_FILE)).unwrap(), newer);
}

#[test]
fn a_directory_from_before_versioning_opens_with_its_data_and_is_stamped() {
    let dir = tempfile::tempdir().unwrap();
    {
        let pm = PersistenceManager::new(dir.path()).expect("open");
        write_one_node(&pm);
    }
    // What every release before this one left behind: the data, no file.
    std::fs::remove_file(dir.path().join(FORMAT_FILE)).unwrap();

    let pm = PersistenceManager::new(dir.path()).expect("an unversioned directory must open");
    let (nodes, _) = pm.recover("default").expect("recover");
    assert_eq!(nodes.len(), 1, "the data written before versioning is still there");
    assert!(dir.path().join(FORMAT_FILE).exists(), "the directory was adopted");
}

#[test]
fn a_format_file_that_names_no_format_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(FORMAT_FILE), "format = soon\n").unwrap();
    assert!(PersistenceManager::new(dir.path()).is_err());
    assert!(!dir.path().join("data").exists(), "a refused directory was written to");
}
