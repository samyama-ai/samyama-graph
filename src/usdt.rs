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
];

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
