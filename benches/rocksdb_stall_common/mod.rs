//! Shared code of the RocksDB write-stall benchmark (#2128).
//!
//! The bench binary (`benches/rocksdb_write_stalls.rs`) parses arguments and
//! prints; this module runs the phases and returns numbers, so a test can run
//! it on a tiny workload in a debug build and check that every phase measured
//! something (`tests/rocksdb_write_stalls_harness.rs`). No timing is asserted
//! anywhere.
//!
//! # What is measured
//!
//! Readers run Cypher point reads in a loop while one writer ingests, in three
//! phases measured back to back in the same process:
//!
//! 1. `baseline` -- readers only. The latency every other phase is read against.
//! 2. `memory` -- readers plus a writer whose statements run in memory and are
//!    not persisted. What the write lock costs readers on its own.
//! 3. `persist` -- readers plus a writer that persists every statement with
//!    `PersistenceManager::apply_mutations` while it still holds the write
//!    lock, which is what `GRAPH.QUERY` does (`src/protocol/command.rs`). The
//!    difference between this phase and `memory` is what persistence, RocksDB
//!    stalls included, adds to read latency.
//!
//! The lock is the server's: a `tokio::sync::RwLock<GraphStore>`, taken with
//! the blocking forms from plain threads. A reader's latency is measured from
//! before it asks for the lock to after its result is built, so time spent
//! queued behind the writer is in it; the lock wait is also reported alone.
//!
//! During `persist` a sampler thread reads `PersistentStorage::stall_stats`
//! every `sample_every`, which is how the run says whether RocksDB actually
//! stalled: a run in which it never did measures persistence cost, not stalls.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use samyama::graph::{GraphStore, Label, NodeId, PropertyMap, PropertyValue};
use samyama::persistence::{PersistenceManager, StallStats};
use samyama::query::{BoundParams, QueryEngine};
use tokio::sync::RwLock;

pub const TENANT: &str = "default";

/// The read every reader runs: a point lookup by an indexed id and one hop.
pub const READ_QUERY: &str = "MATCH (p:Person {id: $id})-[:KNOWS]->(f) RETURN count(f)";

#[derive(Debug, Clone)]
pub struct Config {
    /// `Person` nodes the readers look up, each with `fanout` `KNOWS` edges.
    pub people: usize,
    pub fanout: usize,
    /// Reader threads.
    pub readers: usize,
    /// Length of each phase.
    pub phase: Duration,
    /// Rows created per write statement.
    pub batch: usize,
    /// Bytes of string payload per created row; larger fills memtables faster.
    pub value_bytes: usize,
    /// Pause between write statements; zero is ingestion flat out.
    pub write_pause: Duration,
    /// How often the sampler reads RocksDB's stall properties.
    pub sample_every: Duration,
    /// Where RocksDB lives. `None` is a temporary directory, removed after.
    pub data_dir: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            people: 100_000,
            fanout: 4,
            readers: 4,
            phase: Duration::from_secs(20),
            batch: 1_000,
            value_bytes: 256,
            write_pause: Duration::ZERO,
            sample_every: Duration::from_millis(50),
            data_dir: None,
        }
    }
}

/// Percentiles of a set of durations, in microseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Dist {
    pub n: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

impl Dist {
    pub fn of(mut us: Vec<f64>) -> Dist {
        if us.is_empty() {
            return Dist::default();
        }
        us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let at = |p: f64| us[(((us.len() - 1) as f64) * p).round() as usize];
        Dist {
            n: us.len(),
            p50: at(0.50),
            p95: at(0.95),
            p99: at(0.99),
            max: us[us.len() - 1],
        }
    }
}

/// What the sampler saw over one phase.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StallSummary {
    pub samples: u64,
    /// Samples taken while writes were stopped / being delayed.
    pub stopped_samples: u64,
    pub delayed_samples: u64,
    /// The lowest non-zero delayed write rate seen, bytes per second.
    pub min_delayed_rate: u64,
    pub max_l0_files: u64,
    pub max_pending_compaction_bytes: u64,
    pub max_immutable_memtables: u64,
    pub max_running_compactions: u64,
}

impl StallSummary {
    fn add(&mut self, s: &StallStats) {
        self.samples += 1;
        self.stopped_samples += u64::from(s.write_stopped);
        if s.delayed_write_rate > 0 {
            self.delayed_samples += 1;
            self.min_delayed_rate = if self.min_delayed_rate == 0 {
                s.delayed_write_rate
            } else {
                self.min_delayed_rate.min(s.delayed_write_rate)
            };
        }
        self.max_l0_files = self.max_l0_files.max(s.l0_files);
        self.max_pending_compaction_bytes = self
            .max_pending_compaction_bytes
            .max(s.pending_compaction_bytes);
        self.max_immutable_memtables = self.max_immutable_memtables.max(s.immutable_memtables);
        self.max_running_compactions = self.max_running_compactions.max(s.running_compactions);
    }

    /// Did RocksDB push back at all during the phase?
    pub fn stalled(&self) -> bool {
        self.stopped_samples > 0 || self.delayed_samples > 0
    }
}

#[derive(Debug, Clone, Default)]
pub struct PhaseReport {
    pub name: &'static str,
    pub elapsed: Duration,
    /// Reader latency, lock wait included.
    pub read: Dist,
    /// Reader time waiting for the read lock alone.
    pub read_lock_wait: Dist,
    pub read_errors: u64,
    pub write_statements: u64,
    pub rows_written: u64,
    pub write_errors: u64,
    /// How long the writer held the write lock per statement.
    pub lock_hold: Dist,
    /// `apply_mutations` per statement, inside the lock. Empty unless persisting.
    pub persist: Dist,
    pub stalls: StallSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Writes {
    None,
    Memory,
    Persist,
}

pub struct Bench {
    cfg: Config,
    store: Arc<RwLock<GraphStore>>,
    engine: Arc<QueryEngine>,
    pm: Arc<PersistenceManager>,
    next_seq: u64,
    _tmp: Option<tempfile::TempDir>,
}

impl Bench {
    /// Build the graph the readers query and open RocksDB. The seed graph is
    /// built in memory and not persisted: readers never touch disk, and the
    /// question is what the writer's persistence does to them.
    pub fn new(cfg: Config) -> Result<Bench, String> {
        let (dir, tmp) = match &cfg.data_dir {
            Some(d) => {
                std::fs::create_dir_all(d).map_err(|e| format!("{}: {e}", d.display()))?;
                (d.clone(), None)
            }
            None => {
                let t = tempfile::tempdir().map_err(|e| e.to_string())?;
                (t.path().to_path_buf(), Some(t))
            }
        };
        let pm = PersistenceManager::new(&dir).map_err(|e| e.to_string())?;
        // The default tenant's quota is 1M nodes, which heavy ingestion passes
        // in seconds; past it every write is refused and nothing is persisted.
        // `write_admission` is still asked, and with no ceiling admits
        // everything, as on a server whose ceilings have been lifted.
        pm.tenants()
            .update_quotas(TENANT, samyama::persistence::ResourceQuotas::unlimited())
            .map_err(|e| e.to_string())?;
        let engine = QueryEngine::new();

        let mut store = GraphStore::new();
        engine
            .execute_mut("CREATE INDEX ON :Person(id)", &mut store, TENANT)
            .map_err(|e| e.to_string())?;
        let mut ids: Vec<NodeId> = Vec::with_capacity(cfg.people);
        for i in 0..cfg.people {
            let mut props = PropertyMap::new();
            props.insert("id".to_string(), PropertyValue::Integer(i as i64));
            props.insert(
                "name".to_string(),
                PropertyValue::String(format!("p{i:08}")),
            );
            ids.push(store.create_node_with_properties(TENANT, vec![Label::new("Person")], props));
        }
        let n = ids.len();
        for (i, &src) in ids.iter().enumerate() {
            for k in 1..=cfg.fanout.min(n.saturating_sub(1)) {
                // A fixed stride rather than a random target: the same graph
                // every run, and no RNG in the shared code.
                let dst = ids[(i + k * 7919) % n];
                if dst != src {
                    store
                        .create_edge(src, dst, "KNOWS")
                        .map_err(|e| e.to_string())?;
                }
            }
        }

        Ok(Bench {
            cfg,
            store: Arc::new(RwLock::new(store)),
            engine: Arc::new(engine),
            pm: Arc::new(pm),
            next_seq: 0,
            _tmp: tmp,
        })
    }

    pub fn persistence(&self) -> &PersistenceManager {
        &self.pm
    }

    /// Run one phase for `cfg.phase`.
    pub fn run_phase(&mut self, name: &'static str, writes: Writes) -> PhaseReport {
        let stop = Arc::new(AtomicBool::new(false));
        let started = Instant::now();

        let readers: Vec<_> = (0..self.cfg.readers)
            .map(|r| {
                let (store, engine, stop) = (self.store.clone(), self.engine.clone(), stop.clone());
                let people = self.cfg.people.max(1) as u64;
                std::thread::spawn(move || read_loop(r as u64, people, &store, &engine, &stop))
            })
            .collect();

        let writer = (writes != Writes::None).then(|| {
            let (store, engine, pm, stop) = (
                self.store.clone(),
                self.engine.clone(),
                self.pm.clone(),
                stop.clone(),
            );
            let cfg = self.cfg.clone();
            let first = self.next_seq;
            std::thread::spawn(move || {
                write_loop(
                    &cfg,
                    first,
                    writes == Writes::Persist,
                    &store,
                    &engine,
                    &pm,
                    &stop,
                )
            })
        });

        let sampler = (writes == Writes::Persist).then(|| {
            let (pm, stop, every) = (self.pm.clone(), stop.clone(), self.cfg.sample_every);
            std::thread::spawn(move || {
                let mut s = StallSummary::default();
                while !stop.load(Ordering::Relaxed) {
                    s.add(&pm.storage().stall_stats());
                    std::thread::sleep(every);
                }
                s
            })
        });

        std::thread::sleep(self.cfg.phase);
        stop.store(true, Ordering::Relaxed);

        let mut report = PhaseReport {
            name,
            ..PhaseReport::default()
        };
        let (mut lat, mut wait) = (Vec::new(), Vec::new());
        for h in readers {
            let r = h.join().expect("reader panicked");
            lat.extend(r.latency_us);
            wait.extend(r.lock_wait_us);
            report.read_errors += r.errors;
        }
        report.read = Dist::of(lat);
        report.read_lock_wait = Dist::of(wait);
        if let Some(h) = writer {
            let w = h.join().expect("writer panicked");
            report.write_statements = w.hold_us.len() as u64;
            report.rows_written = w.rows;
            report.write_errors = w.errors;
            report.lock_hold = Dist::of(w.hold_us);
            report.persist = Dist::of(w.persist_us);
            self.next_seq = w.next_seq;
        }
        if let Some(h) = sampler {
            report.stalls = h.join().expect("sampler panicked");
        }
        report.elapsed = started.elapsed();
        report
    }

    /// The three phases, in order.
    pub fn run_all(&mut self) -> Vec<PhaseReport> {
        vec![
            self.run_phase("baseline", Writes::None),
            self.run_phase("memory", Writes::Memory),
            self.run_phase("persist", Writes::Persist),
        ]
    }
}

struct ReadOut {
    latency_us: Vec<f64>,
    lock_wait_us: Vec<f64>,
    errors: u64,
}

fn read_loop(
    reader: u64,
    people: u64,
    store: &RwLock<GraphStore>,
    engine: &QueryEngine,
    stop: &AtomicBool,
) -> ReadOut {
    let mut out = ReadOut {
        latency_us: Vec::new(),
        lock_wait_us: Vec::new(),
        errors: 0,
    };
    // A deterministic walk over ids, a different stride per reader.
    let mut id = reader.wrapping_mul(104_729) % people;
    let stride = 7_919 + 2 * reader;
    let mut params = BoundParams::new();
    while !stop.load(Ordering::Relaxed) {
        id = (id + stride) % people;
        params.insert("id".to_string(), PropertyValue::Integer(id as i64));
        let t0 = Instant::now();
        let guard = store.blocking_read();
        let waited = t0.elapsed();
        let res = engine.execute_with_params_within(READ_QUERY, &guard, &params, None);
        drop(guard);
        let total = t0.elapsed();
        if res.is_err() {
            out.errors += 1;
        }
        out.latency_us.push(total.as_secs_f64() * 1e6);
        out.lock_wait_us.push(waited.as_secs_f64() * 1e6);
    }
    out
}

struct WriteOut {
    hold_us: Vec<f64>,
    persist_us: Vec<f64>,
    rows: u64,
    errors: u64,
    /// The first `seq` the next phase may use.
    next_seq: u64,
}

/// One write statement at a time, the way `GRAPH.QUERY` runs one: take the
/// write lock, turn on the write log, run the statement, persist what it
/// changed, and only then release the lock.
fn write_loop(
    cfg: &Config,
    first_seq: u64,
    persist: bool,
    store: &RwLock<GraphStore>,
    engine: &QueryEngine,
    pm: &PersistenceManager,
    stop: &AtomicBool,
) -> WriteOut {
    let mut out = WriteOut {
        hold_us: Vec::new(),
        persist_us: Vec::new(),
        rows: 0,
        errors: 0,
        next_seq: 0,
    };
    let mut params = BoundParams::new();
    params.insert(
        "pad".to_string(),
        PropertyValue::String("x".repeat(cfg.value_bytes)),
    );
    let mut seq = first_seq;
    let batch = cfg.batch.max(1) as u64;
    while !stop.load(Ordering::Relaxed) {
        let (lo, hi) = (seq, seq + batch - 1);
        seq += batch;
        let query =
            format!("UNWIND range({lo}, {hi}) AS i CREATE (:Event {{seq: i, payload: $pad}})");

        let mut guard = store.blocking_write();
        let held = Instant::now();
        if persist {
            guard.enable_write_log();
            guard.set_write_admission(pm.write_admission(TENANT));
        }
        let res = engine.execute_mut_with_params_within(&query, &mut guard, TENANT, &params, None);
        if persist {
            let mutations = guard.take_write_log();
            let t = Instant::now();
            if pm.apply_mutations(TENANT, &guard, &mutations).is_err() {
                out.errors += 1;
            }
            out.persist_us.push(t.elapsed().as_secs_f64() * 1e6);
        }
        drop(guard);
        out.hold_us.push(held.elapsed().as_secs_f64() * 1e6);
        match res {
            Ok(_) => out.rows += batch,
            Err(_) => out.errors += 1,
        }
        if !cfg.write_pause.is_zero() {
            std::thread::sleep(cfg.write_pause);
        }
    }
    out.next_seq = seq;
    out
}

/// The lines of RocksDB's `dbstats` text that are about stalls.
pub fn stall_lines(pm: &PersistenceManager) -> Vec<String> {
    pm.storage()
        .db_stats_text()
        .unwrap_or_default()
        .lines()
        .filter(|l| l.to_ascii_lowercase().contains("stall"))
        .map(|l| l.trim().to_string())
        .collect()
}
