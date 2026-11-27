//! Do RocksDB write stalls raise concurrent Cypher read latency? (#2128)
//!
//! A write statement persists with `apply_mutations` while it still holds the
//! graph's write lock (`src/protocol/command.rs`), so any time RocksDB spends
//! in a write -- a memtable flush it has to wait for, a delayed or stopped
//! write behind a compaction backlog -- is time every reader spends queued
//! behind the lock.
//!
//! Three phases in one process, readers running Cypher point reads throughout:
//! `baseline` (no writer), `memory` (a writer that does not persist) and
//! `persist` (a writer that persists under the lock, as the server does). Read
//! `persist` against `memory` for what persistence adds, and the stall columns
//! for whether RocksDB pushed back while it did. See
//! `docs/benchmarks/rocksdb-write-stalls.md` and
//! `benches/rocksdb_stall_common/mod.rs`.
//!
//! Always `--release`, on a quiet host.
//!
//! Usage: cargo bench --bench rocksdb_write_stalls -- [--people N] [--fanout N]
//!        [--readers N] [--phase-secs N] [--batch N] [--value-bytes N]
//!        [--write-pause-ms N] [--sample-ms N] [--data-dir PATH]

#[path = "rocksdb_stall_common/mod.rs"]
mod rocksdb_stall_common;

use std::time::Duration;

use rocksdb_stall_common::{stall_lines, Bench, Config, Dist, PhaseReport};

fn arg<T: std::str::FromStr>(args: &[String], name: &str) -> Option<T> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
}

fn ms(us: f64) -> String {
    format!("{:9.3}", us / 1000.0)
}

fn dist_row(label: &str, d: &Dist) {
    println!(
        "  {label:<22} n={:<9} p50 {} p95 {} p99 {} max {}  (ms)",
        d.n,
        ms(d.p50),
        ms(d.p95),
        ms(d.p99),
        ms(d.max)
    );
}

fn print_phase(r: &PhaseReport) {
    let secs = r.elapsed.as_secs_f64().max(1e-9);
    println!("\n[{}] {:.1} s", r.name, secs);
    println!(
        "  reads/s {:.0}   read errors {}",
        r.read.n as f64 / secs,
        r.read_errors
    );
    dist_row("read latency", &r.read);
    dist_row("read lock wait", &r.read_lock_wait);
    if r.write_statements > 0 {
        println!(
            "  write statements {}   rows/s {:.0}   write errors {}",
            r.write_statements,
            r.rows_written as f64 / secs,
            r.write_errors
        );
        dist_row("write lock held", &r.lock_hold);
    }
    if r.persist.n > 0 {
        dist_row("apply_mutations", &r.persist);
    }
    if r.stalls.samples > 0 {
        let s = &r.stalls;
        println!(
            "  rocksdb ({} samples): stopped in {}, delayed in {} (min rate {} B/s), \
             max L0 files {}, max pending compaction {} B, max immutable memtables {}, \
             max running compactions {}",
            s.samples,
            s.stopped_samples,
            s.delayed_samples,
            s.min_delayed_rate,
            s.max_l0_files,
            s.max_pending_compaction_bytes,
            s.max_immutable_memtables,
            s.max_running_compactions
        );
    }
}

fn ratio(a: f64, b: f64) -> String {
    if b > 0.0 {
        format!("{:.2}x", a / b)
    } else {
        "n/a".to_string()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let d = Config::default();
    let cfg = Config {
        people: arg(&args, "--people").unwrap_or(d.people),
        fanout: arg(&args, "--fanout").unwrap_or(d.fanout),
        readers: arg(&args, "--readers").unwrap_or(d.readers),
        phase: arg(&args, "--phase-secs")
            .map(Duration::from_secs)
            .unwrap_or(d.phase),
        batch: arg(&args, "--batch").unwrap_or(d.batch),
        value_bytes: arg(&args, "--value-bytes").unwrap_or(d.value_bytes),
        write_pause: arg(&args, "--write-pause-ms")
            .map(Duration::from_millis)
            .unwrap_or(d.write_pause),
        sample_every: arg(&args, "--sample-ms")
            .map(Duration::from_millis)
            .unwrap_or(d.sample_every),
        data_dir: arg::<String>(&args, "--data-dir").map(Into::into),
    };
    if cfg!(debug_assertions) {
        eprintln!("WARNING: debug build; latencies are not comparable to a --release run");
    }
    eprintln!("config: {cfg:?}");
    eprintln!(
        "SAMYAMA_FSYNC={}",
        samyama::persistence::storage::fsync_enabled()
    );

    let mut bench = match Bench::new(cfg) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("SKIP: cannot set up the bench here: {e}");
            return;
        }
    };
    let reports = bench.run_all();
    for r in &reports {
        print_phase(r);
    }

    let (base, mem, per) = (&reports[0], &reports[1], &reports[2]);
    println!("\nread latency against baseline (same process)");
    println!("{}", "-".repeat(64));
    for (label, f) in [
        ("p50", (|d: &Dist| d.p50) as fn(&Dist) -> f64),
        ("p95", |d: &Dist| d.p95),
        ("p99", |d: &Dist| d.p99),
    ] {
        println!(
            "  {label}   memory {}   persist {}   persist/memory {}",
            ratio(f(&mem.read), f(&base.read)),
            ratio(f(&per.read), f(&base.read)),
            ratio(f(&per.read), f(&mem.read))
        );
    }
    println!(
        "\nRocksDB pushed back during persist: {}",
        if per.stalls.stalled() {
            "yes -- the persist phase includes stalls"
        } else {
            "no -- no stall was sampled; this run measures persistence cost, not stalls"
        }
    );

    let lines = stall_lines(bench.persistence());
    if !lines.is_empty() {
        println!("\nrocksdb.dbstats, stall lines:");
        for l in lines {
            println!("  {l}");
        }
    }
}
