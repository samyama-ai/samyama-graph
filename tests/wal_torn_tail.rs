//! A WAL whose last record is torn replays up to the last complete one (#1311).
//!
//! REL-15 asks for failure scenarios with *observed* behaviour. "Corrupt or
//! truncated WAL" was one of four the survey could not write a row for:
//! `wal.rs` handled `UnexpectedEof` during replay and nothing ever produced
//! one, so what the code did on a torn record had never been watched.
//!
//! What it did, once watched: a truncated **length prefix** stopped replay
//! cleanly, and a truncated **record body** returned an error that abandoned
//! the whole replay -- taking every complete record before it down with the
//! torn one. A process killed between writing a length and writing the bytes
//! it promised is the ordinary shape of a crash, and it made the WAL
//! unreplayable rather than replayable up to the last good record.
//!
//! The two truncation points are tested separately because they took different
//! code paths and only one of them was right.

use samyama::persistence::wal::{WalEntry, Wal};
use std::fs::OpenOptions;

fn entry(id: u64) -> WalEntry {
    WalEntry::CreateNode {
        tenant: "default".to_string(),
        node_id: id,
        labels: vec!["N".to_string()],
        properties: Vec::new(),
    }
}

/// Write `n` records, then cut `bytes_off` bytes from the end of the file.
fn wal_with_a_cut_tail(dir: &std::path::Path, n: u64, bytes_off: u64) -> u64 {
    {
        let mut w = Wal::new(dir).expect("wal");
        for i in 1..=n {
            w.append(entry(i)).expect("append");
        }
        w.flush().expect("flush");
    }
    let file = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_file())
        .expect("a wal file");
    let len = std::fs::metadata(&file).unwrap().len();
    let cut = len.saturating_sub(bytes_off);
    OpenOptions::new()
        .write(true)
        .open(&file)
        .unwrap()
        .set_len(cut)
        .unwrap();
    cut
}

fn replay_count(dir: &std::path::Path) -> Result<u64, String> {
    let w = Wal::new(dir).expect("wal");
    let mut seen = 0u64;
    w.replay(0, |_| {
        seen += 1;
        Ok(())
    })
    .map(|_| seen)
    .map_err(|e| e.to_string())
}

#[test]
fn a_record_cut_in_its_body_does_not_discard_the_records_before_it() {
    let dir = tempfile::tempdir().unwrap();
    // Cut a few bytes off the last record's body: the length prefix is intact
    // and promises more bytes than the file holds. This is the path that used
    // to fail the whole replay.
    wal_with_a_cut_tail(dir.path(), 5, 6);

    let seen = replay_count(dir.path()).expect("a torn tail is not a replay failure");
    assert_eq!(
        seen, 4,
        "the four complete records must survive the fifth being torn"
    );
}

#[test]
fn a_record_cut_in_its_length_prefix_also_replays_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    // Cut so that fewer than four bytes of the final length prefix remain.
    // This path was already correct; the test keeps it that way, and makes the
    // pair of behaviours one fact rather than two accidents.
    let mut w = Wal::new(dir.path()).expect("wal");
    for i in 1..=4 {
        w.append(entry(i)).expect("append");
    }
    w.flush().expect("flush");
    drop(w);

    let file = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_file())
        .unwrap();
    let len = std::fs::metadata(&file).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&file)
        .unwrap()
        .set_len(len + 2)
        .unwrap(); // two stray bytes: a length prefix that cannot be completed

    let seen = replay_count(dir.path()).expect("a stray partial prefix is not a failure");
    assert_eq!(seen, 4, "all four complete records replay");
}

#[test]
fn an_untouched_wal_replays_everything() {
    // The control. Without it, a replay that returned zero for every input
    // would pass both tests above.
    let dir = tempfile::tempdir().unwrap();
    {
        let mut w = Wal::new(dir.path()).expect("wal");
        for i in 1..=5 {
            w.append(entry(i)).expect("append");
        }
        w.flush().expect("flush");
    }
    assert_eq!(replay_count(dir.path()).expect("clean replay"), 5);
}
