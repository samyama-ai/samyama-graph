//! A WAL record damaged after being written in full is refused by name (#1311).
//!
//! `tests/wal_torn_tail.rs` covers a record that was never finished. This file
//! covers the other case: every byte was written, and then one changed. Replay
//! must stop with `WalError::Corruption` rather than apply the damaged record.
//!
//! The record "checksum" used to be an XOR of the entry's bytes. It kept only
//! 8 bits, did not cover the sequence number, and could not see the same bit
//! flipped in two bytes. The first two tests below are damage that checksum
//! let through -- they fail against it and pass against the CRC-32 that
//! replaced it. The last two keep WALs written in the old format replayable.

use samyama::persistence::wal::{Wal, WalEntry, WalError};
use serde::Serialize;
use std::path::{Path, PathBuf};

fn entry(id: u64) -> WalEntry {
    WalEntry::CreateNode {
        tenant: "default".to_string(),
        node_id: id,
        labels: vec!["N".to_string()],
        properties: Vec::new(),
    }
}

fn the_wal_file(dir: &Path) -> PathBuf {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.is_file())
        .expect("a wal file")
}

/// `(offset of the length word, offset of the body, body length)` per record.
fn frames(bytes: &[u8]) -> Vec<(usize, usize, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 4 <= bytes.len() {
        let word = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let len = (word & 0x7FFF_FFFF) as usize;
        out.push((at, at + 4, len));
        at += 4 + len;
    }
    out
}

fn write_five(dir: &Path) {
    let mut w = Wal::new(dir).expect("wal");
    for i in 1..=5 {
        w.append(entry(i)).expect("append");
    }
    w.flush().expect("flush");
}

/// Replay, returning the node ids seen before replay stopped, and its result.
fn replay(dir: &Path) -> (Vec<u64>, Result<u64, WalError>) {
    let w = Wal::new(dir).expect("wal");
    let mut seen = Vec::new();
    let result = w.replay(0, |e| {
        if let WalEntry::CreateNode { node_id, .. } = e {
            seen.push(*node_id);
        }
        Ok(())
    });
    (seen, result)
}

fn damage(file: &Path, f: impl FnOnce(&mut Vec<u8>)) {
    let mut bytes = std::fs::read(file).unwrap();
    f(&mut bytes);
    std::fs::write(file, bytes).unwrap();
}

fn find(hay: &[u8], needle: &[u8]) -> usize {
    hay.windows(needle.len())
        .position(|w| w == needle)
        .expect("needle present")
}

#[test]
fn the_same_bit_flipped_in_two_bytes_of_a_record_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    write_five(dir.path());
    let file = the_wal_file(dir.path());

    let mut third_at = 0;
    damage(&file, |bytes| {
        let (at, body, len) = frames(bytes)[2];
        third_at = at;
        // "default" -> "edfault": bit 0 flipped in 'd' and in 'e'. Still valid
        // UTF-8, still a well-formed entry, and invisible to an XOR of bytes.
        let i = body + find(&bytes[body..body + len], b"default");
        bytes[i] ^= 0x01;
        bytes[i + 1] ^= 0x01;
    });

    let (seen, result) = replay(dir.path());
    match result {
        Err(WalError::Corruption(offset)) => assert_eq!(
            offset, third_at as u64,
            "the error names the damaged record's offset"
        ),
        other => panic!("expected WalError::Corruption, got {other:?} after {seen:?}"),
    }
    assert_eq!(
        seen,
        vec![1, 2],
        "the records before the damage were replayed, the damaged one was not"
    );
}

#[test]
fn a_flipped_bit_in_the_sequence_number_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    write_five(dir.path());
    let file = the_wal_file(dir.path());

    let mut third_at = 0;
    damage(&file, |bytes| {
        let (at, body, _) = frames(bytes)[2];
        third_at = at;
        // Format 1 body: [format u8][sequence u64 LE]... -- flip the second
        // byte of the sequence (3 -> 259). The old checksum did not cover the
        // sequence at all.
        bytes[body + 2] ^= 0x01;
    });

    let (seen, result) = replay(dir.path());
    assert!(
        matches!(result, Err(WalError::Corruption(o)) if o == third_at as u64),
        "expected WalError::Corruption({third_at}), got {result:?}"
    );
    assert_eq!(seen, vec![1, 2]);
}

/// A record exactly as the pre-#1311 code wrote it: bincode of this struct,
/// behind a plain little-endian length word.
#[derive(Serialize)]
struct LegacyRecord {
    sequence: u64,
    entry: WalEntry,
    checksum: u32,
}

fn legacy_bytes(sequence: u64, e: WalEntry) -> Vec<u8> {
    let payload = bincode::serialize(&e).unwrap();
    let checksum = payload.iter().fold(0u32, |acc, &b| acc ^ (b as u32));
    let body = bincode::serialize(&LegacyRecord {
        sequence,
        entry: e,
        checksum,
    })
    .unwrap();
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

fn write_legacy_wal(dir: &Path, ids: std::ops::RangeInclusive<u64>) -> PathBuf {
    let file = dir.join(format!("wal-{:016x}.log", 0));
    let mut bytes = Vec::new();
    for i in ids {
        bytes.extend(legacy_bytes(i, entry(i)));
    }
    std::fs::write(&file, bytes).unwrap();
    file
}

#[test]
fn a_wal_written_in_the_old_format_still_replays_and_takes_new_records() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_wal(dir.path(), 1..=3);

    let (seen, result) = replay(dir.path());
    result.expect("a legacy WAL replays");
    assert_eq!(seen, vec![1, 2, 3]);

    // A restarted process appends format-1 records to the same file. Both
    // formats must replay from it, in order.
    {
        let mut w = Wal::new(dir.path()).expect("wal");
        w.append(entry(4)).unwrap();
        w.append(entry(5)).unwrap();
        w.flush().unwrap();
    }
    let (seen, result) = replay(dir.path());
    result.expect("a mixed-format WAL replays");
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
}

#[test]
fn a_legacy_record_that_fails_its_old_checksum_is_still_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let file = write_legacy_wal(dir.path(), 1..=3);

    let mut second_at = 0;
    damage(&file, |bytes| {
        let (at, body, len) = frames(bytes)[1];
        second_at = at;
        // One flipped byte: the kind of damage the XOR does see.
        let i = body + find(&bytes[body..body + len], b"default");
        bytes[i] ^= 0x01;
    });

    let (seen, result) = replay(dir.path());
    assert!(
        matches!(result, Err(WalError::Corruption(o)) if o == second_at as u64),
        "expected WalError::Corruption({second_at}), got {result:?}"
    );
    assert_eq!(seen, vec![1]);
}
