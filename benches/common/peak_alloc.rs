//! A counting global allocator that also records the **peak** of live heap
//! bytes (#2084).
//!
//! `memory_footprint.rs` asks how much the stored graph holds. Query memory is
//! a different question with a different shape: a query allocates, peaks, and
//! should give it all back. So this keeps three numbers:
//!
//! * **live**: bytes allocated and not yet freed, now;
//! * **peak**: the highest `live` has been since [`reset_peak`];
//! * **calls**: allocation calls, since per-allocation overhead is its own cost.
//!
//! Wraps the allocator the server ships (`samyama::allocator::SHIPPED`), so the
//! numbers are for the allocator users run. One per binary, by Rust's rules:
//! include it with `#[path = ".../peak_alloc.rs"] mod peak_alloc;` and the
//! `#[global_allocator]` below takes effect in that binary only.
//!
//! The peak is process-wide: in a binary running several things at once it
//! is their combined peak, which is what the concurrency measurement wants and
//! why a test using it must not run alongside other allocating tests.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static PEAK: AtomicIsize = AtomicIsize::new(0);
static CALLS: AtomicUsize = AtomicUsize::new(0);

pub struct PeakAlloc;

#[inline]
fn grew(by: usize) {
    let now = LIVE.fetch_add(by as isize, Ordering::Relaxed) + by as isize;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

#[inline]
fn shrank(by: usize) {
    LIVE.fetch_sub(by as isize, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        grew(layout.size());
        unsafe { samyama::allocator::SHIPPED.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shrank(layout.size());
        unsafe { samyama::allocator::SHIPPED.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        if new_size >= layout.size() {
            grew(new_size - layout.size());
        } else {
            shrank(layout.size() - new_size);
        }
        unsafe { samyama::allocator::SHIPPED.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: PeakAlloc = PeakAlloc;

/// Live heap bytes now.
pub fn live() -> isize {
    LIVE.load(Ordering::Relaxed)
}

/// The highest `live` has been since the last [`reset_peak`].
pub fn peak() -> isize {
    PEAK.load(Ordering::Relaxed)
}

/// Start a new peak window at the current live figure.
pub fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

/// Allocation calls so far.
pub fn calls() -> usize {
    CALLS.load(Ordering::Relaxed)
}

/// What one run of `f` cost: the peak above where it started, and what it
/// left allocated once its own result was dropped.
#[derive(Debug, Clone, Copy)]
pub struct Cost {
    pub peak_bytes: isize,
    pub retained_bytes: isize,
    pub calls: usize,
}

/// Run `f`, drop what it returns, and report its [`Cost`].
pub fn measure<T>(f: impl FnOnce() -> T) -> (Cost, T::Summary)
where
    T: Summarise,
{
    let before = live();
    let calls0 = calls();
    reset_peak();
    let out = f();
    let peak_bytes = peak() - before;
    let summary = out.summarise();
    drop(out);
    let retained_bytes = live() - before;
    (Cost { peak_bytes, retained_bytes, calls: calls() - calls0 }, summary)
}

/// What to keep from a result after it is dropped (a row count, an error
/// kind), so the summary itself is not what `retained_bytes` sees.
pub trait Summarise {
    type Summary;
    fn summarise(&self) -> Self::Summary;
}

impl Summarise for Result<samyama::query::RecordBatch, Box<dyn std::error::Error>> {
    /// `Ok(rows)` or `Err(())`.
    type Summary = Result<usize, ()>;
    fn summarise(&self) -> Self::Summary {
        match self {
            Ok(b) => Ok(b.records.len()),
            Err(_) => Err(()),
        }
    }
}
