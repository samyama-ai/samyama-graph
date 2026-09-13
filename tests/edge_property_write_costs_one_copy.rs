//! `set_edge_property` must not keep a whole second copy of the edge's
//! properties (samyama-graph#602).
//!
//! It wrote the value to the column store, then called
//! `set_edge_property_sparse`, which wrote the column again and the row map,
//! and then cloned the edge's entire property map into the MVCC version log --
//! on every write, at a version nothing can read the copy at. The version log's
//! newest entry at the current version is never consulted: a read at the
//! current version uses the live properties, and a historical read picks an
//! entry older than the current version. `prop_scaling.rs` measured the cost at
//! 1,332 B/edge against 604 B/edge through the row-map setter for three
//! properties: 728 B/edge, 55% of what the properties cost.
//!
//! At the first version -- which is every store that does not use the
//! transaction API, since nothing else advances `current_version` -- an entry
//! says nothing that no entry does not, so none is made: the two setters cost
//! the same. The bound is 16 B/edge. A version-log entry alone is about 110
//! (a map slot and a one-element `Vec`); a property map copy is several
//! hundred.
//!
//! Counts bytes through a global allocator, so this file holds one test: a
//! second test running on another thread would count into the same totals.

use samyama::graph::{GraphStore, PropertyValue};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(l.size(), Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        FREED.fetch_add(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        if n >= l.size() {
            ALLOCATED.fetch_add(n - l.size(), Ordering::Relaxed);
        } else {
            FREED.fetch_add(l.size() - n, Ordering::Relaxed);
        }
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static G: Counting = Counting;

fn live() -> usize {
    ALLOCATED.load(Ordering::Relaxed).saturating_sub(FREED.load(Ordering::Relaxed))
}

const EDGES: usize = 5_000;

/// Three properties of the mix `prop_scaling.rs` uses: a reference string, an
/// amount, a count.
fn props(i: usize) -> [(String, PropertyValue); 3] {
    [
        ("ref".to_string(), PropertyValue::String(format!("TXN{i:012}"))),
        ("amount".to_string(), PropertyValue::Float(i as f64 * 1.37)),
        ("count".to_string(), PropertyValue::Integer(i as i64)),
    ]
}

/// Live bytes the property writes alone leave behind, over `EDGES` edges.
fn property_bytes(write: impl Fn(&mut GraphStore, samyama::graph::EdgeId, String, PropertyValue)) -> (GraphStore, usize) {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let edges: Vec<_> = (0..EDGES).map(|_| store.create_edge(a, b, "PAID").unwrap()).collect();
    let before = live();
    for (i, &e) in edges.iter().enumerate() {
        for (k, v) in props(i) {
            write(&mut store, e, k, v);
        }
    }
    let after = live();
    (store, after.saturating_sub(before))
}

#[test]
fn set_edge_property_costs_what_the_row_map_setter_costs() {
    let (row_store, row) = property_bytes(|s, e, k, v| s.set_edge_property_sparse(e, k, v));
    let (full_store, full) = property_bytes(|s, e, k, v| s.set_edge_property(e, k, v).unwrap());

    // Both stores hold the same values.
    let e = samyama::graph::EdgeId::new(EDGES as u64 / 2);
    assert_eq!(row_store.edge_property(e, "ref"), full_store.edge_property(e, "ref"));
    assert!(full_store.edge_property(e, "amount").is_some());

    let row_per_edge = row as f64 / EDGES as f64;
    let full_per_edge = full as f64 / EDGES as f64;
    let extra = full_per_edge - row_per_edge;
    eprintln!(
        "3 properties/edge: set_edge_property_sparse {row_per_edge:.1} B/edge, \
         set_edge_property {full_per_edge:.1} B/edge, extra {extra:.1} B/edge"
    );
    assert!(
        extra <= 16.0,
        "set_edge_property keeps {extra:.1} B/edge more than set_edge_property_sparse \
         ({full_per_edge:.1} against {row_per_edge:.1}); at the first version it should \
         keep no version log at all"
    );
}
