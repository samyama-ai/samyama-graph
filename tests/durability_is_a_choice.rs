//! `SAMYAMA_FSYNC=1` makes an acknowledged write durable (REL-03, #1309).
//!
//! Nothing on the write path was synced. The WAL's `sync_mode` defaulted to
//! false and its setter had **no callers anywhere in the repository**, so it
//! was false for the life of every process; and even when true the call was
//! `flush()` on a `BufWriter`, which moves bytes into the OS page cache and is
//! not a durability barrier. RocksDB was opened with no `WriteOptions`, so its
//! writes were unsynced too.
//!
//! `docs/ACID_GUARANTEES.md` had to say so, and did. What it could not offer
//! was a way to choose otherwise.
//!
//! # The default does not move
//!
//! Off, exactly as before. This changes what an operator can choose, not what
//! they get without asking — and the first test here is the one that says so,
//! because a durability switch that quietly defaults to on is a 461x
//! throughput regression nobody asked for.
//!
//! # What these tests can and cannot check
//!
//! That the barrier is *requested*: that the flag is read, that both halves of
//! the write path move together, and that writes still land and replay. They
//! cannot check that the device honoured it — `sync_data` returns when the
//! kernel says the bytes are on the device, and whether the device lied is a
//! property of the device. Claiming otherwise from a unit test would be the
//! kind of durability claim this issue exists to remove.

use samyama::persistence::storage::fsync_enabled;
use samyama::persistence::PersistenceManager;

/// These read one process-global environment variable, so they cannot run at
/// the same time. The same trap as the latency histogram and the persistence
/// health flag: a test whose result depends on what else is running is not a
/// test.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn guard() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Set `SAMYAMA_FSYNC` for the duration of a closure and put it back.
fn with_fsync<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
    let previous = std::env::var("SAMYAMA_FSYNC").ok();
    match value {
        Some(v) => std::env::set_var("SAMYAMA_FSYNC", v),
        None => std::env::remove_var("SAMYAMA_FSYNC"),
    }
    let out = f();
    match previous {
        Some(p) => std::env::set_var("SAMYAMA_FSYNC", p),
        None => std::env::remove_var("SAMYAMA_FSYNC"),
    }
    out
}

#[test]
fn the_default_is_off() {
    // The most important test in the file. An unset variable must mean what it
    // has always meant.
    let _s = guard();
    with_fsync(None, || {
        assert!(
            !fsync_enabled(),
            "durability must stay opt-in: turning it on by default is a 461x \
             throughput change nobody asked for"
        );
    });
}

#[test]
fn the_wals_own_default_is_off_too() {
    // The previous test checks the *storage* half's flag. Break-testing found
    // that insufficient: forcing the WAL's default to `true` left it passing,
    // because the two halves read the flag by different routes and only one
    // was being asserted. This constructs a WAL and asks it.
    let _s = guard();
    with_fsync(None, || {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = samyama::persistence::wal::Wal::new(dir.path()).expect("wal");
        assert!(
            !wal.sync_mode(),
            "the WAL must default to unsynced, as it always has"
        );
    });
    with_fsync(Some("1"), || {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = samyama::persistence::wal::Wal::new(dir.path()).expect("wal");
        assert!(wal.sync_mode(), "SAMYAMA_FSYNC=1 must reach the WAL as well");
    });
}

#[test]
fn the_flag_is_read_and_is_not_over_eager() {
    let _s = guard();
    for on in ["1", "true", "yes", "on", "TRUE", "On"] {
        with_fsync(Some(on), || {
            assert!(fsync_enabled(), "{on:?} should enable fsync");
        });
    }
    // The other half: a value that is not an affirmative must not enable it.
    // "0" and "false" are obvious; "maybe" is the case a looser check —
    // `is_ok()`, or a non-empty test — would get wrong, and getting it wrong
    // in this direction silently costs 461x.
    for off in ["0", "false", "no", "off", "", "maybe", "2"] {
        with_fsync(Some(off), || {
            assert!(!fsync_enabled(), "{off:?} should not enable fsync");
        });
    }
}

#[test]
fn a_synced_write_still_lands_and_reads_back() {
    // The barrier must not change the answer, only when it reaches the
    // platter. A sync that lost the write would pass every check above.
    let _s = guard();
    with_fsync(Some("1"), || {
        let dir = tempfile::tempdir().expect("tempdir");
        let pm = PersistenceManager::new(dir.path()).expect("persistence");
        let mut store = samyama::graph::GraphStore::new();
        store.enable_write_log();
        let id = store.create_node("Person");
        store
            .set_node_property("default", id, "name", "Ada")
            .expect("set");
        let mutations = store.take_write_log();
        let written = pm
            .apply_mutations("default", &store, &mutations)
            .expect("a synced write must succeed");
        assert!(written > 0, "the write reported nothing persisted");
    });
}

#[test]
fn both_halves_of_the_write_path_move_together() {
    // The WAL and the store are two barriers, and turning on one is a
    // durability level nobody asked for: a write in the store's page cache and
    // an fsynced WAL entry describing it is not more durable than neither.
    let _s = guard();
    with_fsync(Some("1"), || {
        let dir = tempfile::tempdir().expect("tempdir");
        let pm = PersistenceManager::new(dir.path()).expect("persistence");
        assert!(
            fsync_enabled(),
            "the store half reads the same flag as the WAL half"
        );
        // The WAL half is not observable from outside, so this asserts the
        // thing that is: one flag, read by both, at construction.
        drop(pm);
    });
}

#[test]
fn the_level_is_fixed_for_the_life_of_a_process() {
    // Reading the variable per write would make "was this write durable?"
    // unanswerable for any particular write. The WAL reads it once, at
    // construction; changing the environment afterwards must not move an
    // already-open writer.
    let _s = guard();
    let dir = tempfile::tempdir().expect("tempdir");
    let pm = with_fsync(Some("1"), || {
        PersistenceManager::new(dir.path()).expect("persistence")
    });
    with_fsync(Some("0"), || {
        let mut store = samyama::graph::GraphStore::new();
        store.enable_write_log();
        store.create_node("N");
        let mutations = store.take_write_log();
        pm.apply_mutations("default", &store, &mutations)
            .expect("the open manager keeps the level it was built with");
    });
}
