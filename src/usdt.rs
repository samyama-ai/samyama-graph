//! USDT probes: static tracepoints in the binary, free until something
//! attaches (#2219).
//!
//! `tracing` spans (#2077) cost something whenever they are recorded, and
//! cannot see the kernel. A USDT probe is a single `nop` plus an ELF note
//! (`.note.stapsdt`) naming it; it does nothing until `bpftrace`, `perf` or
//! `gdb` attaches, and then reports its arguments with the kernel's view of
//! the same moment beside them -- fsync latency, scheduler stalls, page
//! faults. On Linux the probes are SystemTap SDT notes (the `probe` crate);
//! elsewhere they compile to nothing.
//!
//! The probes, provider `samyama`, are listed in [`PROBES`] with their
//! arguments; `tests/usdt_probes.rs` checks that every one listed is in the
//! binary on Linux and is placed in the source everywhere. To list them on a
//! running host:
//!
//! ```text
//! bpftrace -l 'usdt:/usr/local/bin/samyama:samyama:*'
//! readelf -n /usr/local/bin/samyama | grep -A2 stapsdt
//! ```
//!
//! A query's text is passed as a pointer and a length rather than a hash, so
//! nothing is computed when nobody is listening; in bpftrace,
//! `str(arg0, arg1)` reads it.

/// `(name, arguments)` for every probe this binary carries.
pub const PROBES: &[(&str, &str)] = &[
    ("query__start", "query_ptr, query_len, is_write"),
    ("query__done", "query_ptr, query_len, elapsed_ns, rows, failed, is_write"),
    ("persist__start", "mutations"),
    ("persist__done", "mutations, elapsed_ns, failed"),
    ("lock__wait_start", "mode"),
    ("lock__acquired", "mode, wait_ns"),
    ("wal__fsync_start", "bytes"),
    ("wal__fsync_done", "elapsed_ns"),
    ("snapshot__io", "op, bytes, elapsed_ns"),
    ("vector__hnsw_search", "k, ef, dist_evals, elapsed_ns"),
];

/// Which way the store's lock is being taken, as the lock probes report it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Read = 0,
    Write = 1,
}

/// A statement is about to be parsed and run.
#[inline(always)]
pub fn query_start(query: &str, is_write: bool) {
    probe::probe!(samyama, query__start, query.as_ptr(), query.len(), is_write as isize);
}

/// A statement finished, or failed. `elapsed` is what the slow-query log and
/// `samyama_query_duration_seconds` record for it.
#[inline(always)]
pub fn query_done(query: &str, elapsed: std::time::Duration, rows: usize, failed: bool, is_write: bool) {
    probe::probe_lazy!(
        samyama,
        query__done,
        query.as_ptr(),
        query.len(),
        elapsed.as_nanos().min(isize::MAX as u128),
        rows,
        failed as isize,
        is_write as isize
    );
}

/// A batch of mutations is about to be written to disk.
#[inline(always)]
pub fn persist_start(mutations: usize) {
    probe::probe!(samyama, persist__start, mutations);
}

/// The batch was written, or failed to be.
#[inline(always)]
pub fn persist_done(mutations: usize, elapsed: std::time::Duration, failed: bool) {
    probe::probe_lazy!(
        samyama,
        persist__done,
        mutations,
        elapsed.as_nanos().min(isize::MAX as u128),
        failed as isize
    );
}

/// A task is about to wait for the store's lock (`src/store_lock.rs`).
///
/// Returns the moment it started waiting, and reads the clock only when this
/// probe is attached: detached, it is a load and a branch, with no
/// `Instant::now()` (Naresh, #2219). `lock__acquired` reports the wait only
/// when this returned a start, so attach both (`usdt:...:samyama:lock__*`).
#[inline(always)]
pub fn lock_wait_start(mode: LockMode) -> Option<std::time::Instant> {
    let enabled = probe::probe_lazy!(samyama, lock__wait_start, mode as isize);
    enabled.then(std::time::Instant::now)
}

/// The task got the lock, `since` after [`lock_wait_start`] said it began.
#[inline(always)]
pub fn lock_acquired(mode: LockMode, since: Option<std::time::Instant>) {
    if let Some(t0) = since {
        probe::probe_lazy!(samyama, lock__acquired, mode as isize, wait_ns(t0));
    }
}

fn wait_ns(since: std::time::Instant) -> u128 {
    since.elapsed().as_nanos().min(isize::MAX as u128)
}

/// The WAL is about to make `bytes` durable (`fsync`, in `Wal::sync`).
///
/// Returns when it started, always: an fsync takes milliseconds, so the clock
/// read costs nothing beside it, and `wal__fsync_done` needs it.
#[inline(always)]
pub fn wal_fsync_start(bytes: u64) -> std::time::Instant {
    probe::probe_lazy!(samyama, wal__fsync_start, bytes as isize);
    std::time::Instant::now()
}

/// The fsync `wal_fsync_start` began has returned.
#[inline(always)]
pub fn wal_fsync_done(started: std::time::Instant) {
    probe::probe_lazy!(samyama, wal__fsync_done, ns_since(started));
}

/// What a snapshot transfer was, for `snapshot__io`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotOp {
    Export = 0,
    Import = 1,
}

/// A `.sgsnap` was written (`Export`) or read (`Import`): the bytes of the
/// snapshot stream as it went to or came from its file (compressed; the
/// unencrypted stream when the file is encrypted), and how long the whole
/// transfer took.
#[inline(always)]
pub fn snapshot_io(op: SnapshotOp, bytes: u64, started: std::time::Instant) {
    probe::probe_lazy!(samyama, snapshot__io, op as isize, bytes as isize, ns_since(started));
}

/// A vector search returned. `dist_evals` is the number of distances it
/// computed when that is known -- an exact search computes one per vector --
/// and -1 when it is not: the HNSW library does not report its count.
#[inline(always)]
pub fn vector_search(k: usize, ef: usize, dist_evals: Option<usize>, started: std::time::Instant) {
    probe::probe_lazy!(samyama, vector__hnsw_search, k, ef, evals(dist_evals), ns_since(started));
}

fn evals(n: Option<usize>) -> isize {
    n.map_or(-1, |n| n.min(isize::MAX as usize) as isize)
}

fn ns_since(started: std::time::Instant) -> u128 {
    started.elapsed().as_nanos().min(isize::MAX as u128)
}

