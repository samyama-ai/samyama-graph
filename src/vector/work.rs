//! The work a vector search does, counted on the thread doing it (#1970).
//!
//! `PROFILE` reports how long `VectorSearch` took, which does not say why: a
//! search that widened four times and then fell back to an exact scan, and
//! one that walked the graph once, can take the same time on a small index
//! and very different times on a large one. These counters are what the
//! strategy actually did.
//!
//! Counted per thread, because a search over one index runs on the thread
//! that asked for it: the graph walk, the exact scans and the widening loop
//! are all sequential. A caller reads [`Work::now`] before and after and
//! takes the difference, so concurrent searches on other threads, and index
//! builds on rayon's threads, never show up in its numbers. The search across
//! several indexes at once (`search_all`) fans out over rayon and is not
//! covered; `VectorSearch` always names one index.
//!
//! Each count is a `Cell` increment on a `const`-initialised thread local,
//! about a nanosecond, beside a distance computation of tens.
use std::cell::Cell;

thread_local! {
    static DISTANCES: Cell<u64> = const { Cell::new(0) };
    static GRAPH_SEARCHES: Cell<u64> = const { Cell::new(0) };
    static EXACT_FALLBACKS: Cell<u64> = const { Cell::new(0) };
}

/// Counts since the thread started; only differences mean anything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    /// Distances computed, by the graph walk and by exact scans alike.
    pub distances: u64,
    /// HNSW searches run by the widening loop: one when the first fetch was
    /// enough, one more for every time it doubled.
    pub graph_searches: u64,
    /// Times widening gave up on the graph and scanned exactly instead.
    pub exact_fallbacks: u64,
}

impl Work {
    /// This thread's counts so far.
    pub fn now() -> Self {
        Work {
            distances: DISTANCES.with(Cell::get),
            graph_searches: GRAPH_SEARCHES.with(Cell::get),
            exact_fallbacks: EXACT_FALLBACKS.with(Cell::get),
        }
    }

    /// What this thread did between `earlier` and `self`.
    pub fn since(self, earlier: Work) -> Work {
        Work {
            distances: self.distances.wrapping_sub(earlier.distances),
            graph_searches: self.graph_searches.wrapping_sub(earlier.graph_searches),
            exact_fallbacks: self.exact_fallbacks.wrapping_sub(earlier.exact_fallbacks),
        }
    }
}

fn bump(counter: &'static std::thread::LocalKey<Cell<u64>>) {
    counter.with(|c| c.set(c.get().wrapping_add(1)));
}

#[inline]
pub(crate) fn distance() {
    bump(&DISTANCES);
}

pub(crate) fn graph_search() {
    bump(&GRAPH_SEARCHES);
}

pub(crate) fn exact_fallback() {
    bump(&EXACT_FALLBACKS);
}
