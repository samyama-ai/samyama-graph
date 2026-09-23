//! What does durability cost? (REL-03, #1309)
//!
//! Until now nothing on the write path was synced: the WAL's `sync_mode`
//! defaulted to false and its setter had no callers, and even when true the
//! call was `flush()` on a `BufWriter`, which moves bytes into the OS page
//! cache and is not a durability barrier. RocksDB was opened with no
//! `WriteOptions`, so its writes were unsynced too. A write acknowledged
//! seconds earlier could be gone after a power cut.
//!
//! `SAMYAMA_FSYNC=1` now turns both on. **The default is unchanged** — this is
//! a change to what an operator can choose, not to what they get without
//! asking — and a choice offered without its price is not a choice, so this
//! measures it.
//!
//! ```bash
//! cargo run --release --example fsync_cost -- --json out.json
//! ```
//!
//! # What is measured, and what is not
//!
//! Writes per second through `apply_mutations`, both ways, on the same host in
//! the same process, alternating so that a warm cache or a busy moment lands
//! on both arms rather than one.
//!
//! The in-process comparison moves the **WAL barrier only**: RocksDB's
//! `WriteOptions` are fixed when the database is opened, so both arms run with
//! the store in whatever mode the environment set. `--from-env` measures one
//! configuration end to end, so running it twice — once with `SAMYAMA_FSYNC=1`
//! — gives the cost of both barriers together. Both numbers are worth having,
//! and reporting the smaller one alone would understate what durability costs.
//!
//! # `--from-env` is one arm, and a caller comparing two of them must interleave
//!
//! The in-process comparison alternates its arms deliberately: a background
//! process arriving halfway through then lands on both, not on one. `--from-env`
//! cannot do that — the whole point is that RocksDB's mode is fixed at open —
//! so each invocation measures one arm, and **two invocations run back to back
//! put every between-process change entirely into the ratio**. A caller wanting
//! both barriers must run the two modes alternately, several times, and look at
//! the spread of the per-pair ratios rather than at one pair.
//!
//! The output carries `samples` for exactly that reason. It used to print only
//! the median; a median with no spread beside it is a bare ratio, and the one
//! this mode produces is noisier than it looks — five rounds on an idle host
//! spanned 342k–457k writes/s, 1.34x, in a single process.
//!
//! It is **not** a claim about surviving a power cut. `sync_data` returns when
//! the kernel says the device has the bytes; whether the device lied is a
//! property of the device. REL-04's kill-point testing covers process death,
//! which is the failure this already survived.

use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::persistence::PersistenceManager;

const BATCH: usize = 200;
const ROUNDS: usize = 5;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();

    // One configuration, taken from the environment, for measuring the **whole**
    // path. The in-process comparison below can only move the WAL: RocksDB's
    // `WriteOptions` are fixed when the database is opened, so both its arms run
    // with RocksDB in whatever mode the environment set. Running this mode twice,
    // once per process, is the only way to see both barriers.
    if args.iter().any(|a| a == "--from-env") {
        let rounds = args
            .iter()
            .position(|a| a == "--rounds")
            .and_then(|i| args.get(i + 1))
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(ROUNDS)
            .max(1);
        let rates: Vec<f64> = (0..rounds).map(|_| one_run_env()).collect();
        let mut v = rates.clone();
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        // The samples, not only their median. This mode used to print the
        // median alone, and a caller dividing one process's median by
        // another's got a ratio with no spread attached -- which is the bare
        // ratio the conformance harness exists to refuse. Two runs of that
        // ratio on the same quiet host, hours apart, read 420x and 219x while
        // every other arm of the same suite reproduced to within 2%. Nothing
        // in the output could show that, because the five rounds behind each
        // median were thrown away.
        println!(
            "{{\"fsync\": {}, \"writes_per_second\": {:.1}, \"rounds\": {}, \"batch\": {}, \"samples\": {:?}}}",
            samyama::persistence::storage::fsync_enabled(),
            v[v.len() / 2],
            rounds,
            BATCH,
            rates
        );
        return;
    }

    // Alternated, not one arm then the other: a background process that
    // arrives halfway through would otherwise be attributed entirely to
    // whichever arm was running, and this is a ratio between two numbers
    // measured on one host.
    let mut synced = Vec::new();
    let mut unsynced = Vec::new();
    for round in 0..ROUNDS {
        for sync in [false, true] {
            let rate = one_run(sync);
            if sync {
                synced.push(rate)
            } else {
                unsynced.push(rate)
            }
            eprintln!(
                "[fsync] round {round} {:8} {rate:>10.0} writes/s",
                if sync { "synced" } else { "unsynced" }
            );
        }
    }

    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let s = med(&mut synced);
    let u = med(&mut unsynced);
    let ratio = u / s;

    println!();
    println!("Durability cost (REL-03)");
    println!("{}", "-".repeat(56));
    println!("  unsynced   {u:>12.0} writes/s   (the default)");
    println!("  synced     {s:>12.0} writes/s   (SAMYAMA_FSYNC=1)");
    println!("  fsync costs {ratio:.1}x throughput");
    println!();
    println!(
        "  Medians of {ROUNDS} alternated rounds of {BATCH} writes each. This is the\n  \
         cost of the barrier, not a claim about surviving a power cut: `sync_data`\n  \
         returns when the kernel says the device has the bytes."
    );

    if let Some(path) = json_out {
        let json = format!(
            "{{\n  \"unsynced_writes_per_second\": {u:.1},\n  \
             \"synced_writes_per_second\": {s:.1},\n  \
             \"fsync_throughput_cost\": {ratio:.3},\n  \
             \"rounds\": {ROUNDS},\n  \"batch\": {BATCH},\n  \
             \"synced_samples\": {synced:?},\n  \"unsynced_samples\": {unsynced:?}\n}}\n"
        );
        std::fs::write(&path, json).expect("write json");
        eprintln!("[fsync] wrote {path}");
    }
}

/// One timed batch at whatever durability level the environment set, WAL and
/// store together.
fn one_run_env() -> f64 {
    let dir = tempfile::tempdir().expect("tempdir");
    let pm = PersistenceManager::new(dir.path()).expect("persistence");
    let mut store = GraphStore::new();
    store.enable_write_log();
    for i in 0..BATCH {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "i", i as i64);
    }
    let mutations = store.take_write_log();
    let started = Instant::now();
    pm.apply_mutations("default", &store, &mutations).expect("apply");
    BATCH as f64 / started.elapsed().as_secs_f64()
}

/// One timed batch of persisted writes, at the given durability level.
fn one_run(sync: bool) -> f64 {
    let dir = tempfile::tempdir().expect("tempdir");
    let pm = PersistenceManager::new(dir.path()).expect("persistence");
    pm.set_wal_sync_for_test(sync);

    let mut store = GraphStore::new();
    store.enable_write_log();
    for i in 0..BATCH {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "i", i as i64);
    }
    let mutations = store.take_write_log();

    let started = Instant::now();
    pm.apply_mutations("default", &store, &mutations)
        .expect("apply");
    let elapsed = started.elapsed().as_secs_f64();
    BATCH as f64 / elapsed
}
