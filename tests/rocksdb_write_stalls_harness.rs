//! Checks the measurement code of the RocksDB write-stall benchmark (#2128)
//! on a tiny workload: every phase runs, readers and the writer both make
//! progress, the persist phase persists and samples RocksDB, and the rows the
//! writer reports are on disk.
//!
//! No timing is asserted -- the phases are short and this is a debug build.
//! What is checked is that each number the bench reports was measured.

#[path = "../benches/rocksdb_stall_common/mod.rs"]
mod rocksdb_stall_common;

use std::time::Duration;

use rocksdb_stall_common::{Bench, Config, Dist};

fn tiny() -> Config {
    Config {
        people: 200,
        fanout: 2,
        readers: 2,
        phase: Duration::from_millis(300),
        batch: 20,
        value_bytes: 16,
        write_pause: Duration::ZERO,
        sample_every: Duration::from_millis(10),
        data_dir: None,
    }
}

#[test]
fn every_phase_measures_what_it_reports() {
    let mut bench = Bench::new(tiny()).expect("set up");
    let reports = bench.run_all();
    let names: Vec<_> = reports.iter().map(|r| r.name).collect();
    assert_eq!(names, ["baseline", "memory", "persist"]);

    for r in &reports {
        assert!(r.read.n > 0, "{}: no reads completed", r.name);
        assert_eq!(r.read_errors, 0, "{}: the read query failed", r.name);
        assert_eq!(
            r.read.n, r.read_lock_wait.n,
            "{}: one lock wait per read",
            r.name
        );
        assert!(
            r.read.p50 <= r.read.p99 && r.read.p99 <= r.read.max,
            "{}: {:?}",
            r.name,
            r.read
        );
    }

    let (base, mem, per) = (&reports[0], &reports[1], &reports[2]);
    assert_eq!(base.write_statements, 0, "the baseline has no writer");
    assert_eq!(base.stalls.samples, 0, "the baseline is not sampled");

    for r in [mem, per] {
        assert!(
            r.write_statements > 0,
            "{}: the writer made no progress",
            r.name
        );
        assert_eq!(r.write_errors, 0, "{}: a write failed", r.name);
        assert_eq!(r.rows_written, r.write_statements * 20, "{}", r.name);
        assert_eq!(r.lock_hold.n as u64, r.write_statements, "{}", r.name);
    }
    assert_eq!(mem.persist.n, 0, "the memory phase does not persist");
    assert_eq!(
        per.persist.n as u64, per.write_statements,
        "one persist per statement"
    );
    assert!(per.stalls.samples > 0, "the sampler read RocksDB");

    // What the persist phase wrote is on disk: the rows it reports, under the
    // ids the memory phase did not use.
    let on_disk = bench
        .persistence()
        .storage()
        .scan_nodes("default")
        .expect("scan");
    assert_eq!(
        on_disk.len() as u64,
        per.rows_written,
        "persisted rows reach RocksDB"
    );
}

#[test]
fn percentiles_are_taken_from_the_sorted_sample() {
    let d = Dist::of((1..=100).rev().map(f64::from).collect());
    assert_eq!(
        (d.n, d.p50, d.p95, d.p99, d.max),
        (100, 51.0, 95.0, 99.0, 100.0)
    );
    assert_eq!(Dist::of(Vec::new()), Dist::default());
}
