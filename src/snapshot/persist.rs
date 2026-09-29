//! HA-08: Durable snapshot persistence.
//!
//! Pattern: Redis RDB. Bulk imports bypass the WAL; durability is handled by
//! writing the received .sgsnap bytes to disk atomically (tmp → fsync → rename),
//! then dropping a `<file>.committed` marker. On boot the server scans the
//! snapshot directory and only replays snapshots whose marker is present, so a
//! crash mid-flush leaves no partial state in play.

use std::fs::{self, File};
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};

use crate::graph::store::GraphStore;
use crate::snapshot::format::ImportStats;

/// Filename used for the single-tenant default snapshot.
const DEFAULT_SNAPSHOT_NAME: &str = "default.sgsnap";
const COMMITTED_SUFFIX: &str = ".committed";
const TMP_SUFFIX: &str = ".tmp";

fn snapshot_dir(data_path: &str) -> PathBuf {
    Path::new(data_path).join("snapshots")
}

/// Atomically persist `bytes` as `<data_path>/snapshots/default.sgsnap`.
///
/// Sequence:
/// 1. Create `snapshots/` if missing.
/// 2. Write bytes to `default.sgsnap.tmp`, fsync.
/// 3. Rename tmp → `default.sgsnap`.
/// 4. Drop empty marker file `default.sgsnap.committed`, fsync.
///
/// The marker is never removed. Because the rename is atomic,
/// `default.sgsnap` is always a whole file — the previous snapshot or the new
/// one — so a marker left in place from an earlier persist still describes a
/// whole file. A failure or crash anywhere before the rename leaves the
/// previous committed snapshot exactly as it was (#1520); a crash between the
/// rename and the marker on a first persist leaves no marker (ignored on boot).
pub fn persist_snapshot(data_path: &str, bytes: &[u8]) -> std::io::Result<()> {
    let dir = snapshot_dir(data_path);
    fs::create_dir_all(&dir)?;

    let final_path = dir.join(DEFAULT_SNAPSHOT_NAME);
    let tmp_path = dir.join(format!("{}{}", DEFAULT_SNAPSHOT_NAME, TMP_SUFFIX));
    let marker_path = dir.join(format!("{}{}", DEFAULT_SNAPSHOT_NAME, COMMITTED_SUFFIX));

    let written = (|| {
        let mut f = File::create(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()
    })();
    if let Err(e) = written {
        // Best effort: don't leave a partial tmp file behind. The committed
        // snapshot, if any, is untouched.
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }
    fs::rename(&tmp_path, &final_path)?;

    // Drop the committed marker last and fsync it.
    {
        let f = File::create(&marker_path)?;
        f.sync_all()?;
    }
    // Make the rename and the marker's directory entry durable.
    File::open(&dir)?.sync_all()?;

    Ok(())
}

/// If a committed snapshot exists under `<data_path>/snapshots/`, import it
/// into `store` and return its stats. Returns `Ok(None)` if no committed
/// snapshot is present (fresh install or crash-before-commit).
pub fn restore_persisted_snapshots(
    data_path: &str,
    store: &mut GraphStore,
) -> Result<Option<ImportStats>, Box<dyn std::error::Error>> {
    let dir = snapshot_dir(data_path);
    if !dir.exists() {
        return Ok(None);
    }

    let snap_path = dir.join(DEFAULT_SNAPSHOT_NAME);
    let marker_path = dir.join(format!("{}{}", DEFAULT_SNAPSHOT_NAME, COMMITTED_SUFFIX));
    if !snap_path.exists() || !marker_path.exists() {
        return Ok(None);
    }

    let bytes = fs::read(&snap_path)?;
    let cursor = Cursor::new(bytes);
    let stats = crate::snapshot::import_tenant_with_dedup(store, cursor, &[])?;
    Ok(Some(stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_none_when_dir_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let mut store = GraphStore::new();
        let res = restore_persisted_snapshots(&tmp.path().to_string_lossy(), &mut store).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn persist_writes_marker_last() {
        let tmp = tempfile::tempdir().unwrap();
        persist_snapshot(&tmp.path().to_string_lossy(), b"not-a-real-snap").unwrap();
        let dir = tmp.path().join("snapshots");
        assert!(dir.join("default.sgsnap").exists());
        assert!(dir.join("default.sgsnap.committed").exists());
        assert!(!dir.join("default.sgsnap.tmp").exists());
    }

    /// #1520: a persist that fails before the rename must leave the previous
    /// committed snapshot restorable. It used to remove the marker first, so
    /// the server booted empty from a directory holding a complete snapshot.
    #[test]
    fn a_failed_persist_keeps_the_last_good_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let data_path = tmp.path().to_string_lossy().to_string();

        let mut src = GraphStore::new();
        let a = src.create_node("Person");
        let b = src.create_node("Person");
        src.create_edge(a, b, "KNOWS").unwrap();
        let mut good = Vec::new();
        crate::snapshot::export_tenant(&src, &mut good).unwrap();
        persist_snapshot(&data_path, &good).unwrap();

        // A directory where the tmp file goes makes File::create fail with
        // EISDIR — a write failure that also fires when the tests run as root,
        // where a read-only file would not.
        let dir = tmp.path().join("snapshots");
        fs::create_dir(dir.join("default.sgsnap.tmp")).unwrap();
        assert!(persist_snapshot(&data_path, b"never-written").is_err());

        assert!(dir.join("default.sgsnap.committed").exists());
        assert_eq!(fs::read(dir.join("default.sgsnap")).unwrap(), good);
        let mut restored = GraphStore::new();
        let stats = restore_persisted_snapshots(&data_path, &mut restored)
            .unwrap()
            .expect("the previous committed snapshot must still restore");
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
    }

    #[test]
    fn a_second_persist_replaces_the_first() {
        let tmp = tempfile::tempdir().unwrap();
        let data_path = tmp.path().to_string_lossy().to_string();
        persist_snapshot(&data_path, b"first").unwrap();
        persist_snapshot(&data_path, b"second").unwrap();
        let dir = tmp.path().join("snapshots");
        assert_eq!(fs::read(dir.join("default.sgsnap")).unwrap(), b"second");
        assert!(dir.join("default.sgsnap.committed").exists());
        assert!(!dir.join("default.sgsnap.tmp").exists());
    }
}
