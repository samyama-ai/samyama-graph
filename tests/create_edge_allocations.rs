//! Creating a relationship must cost about one trip to the allocator, not
//! five (samyama-graph#491).
//!
//! `create_edge` built an `Edge` -- cloning the type string into it and
//! reading the clock -- and then never stored it: the store keeps endpoints,
//! type id and properties in their own arrays since DS-07c. The catalog then
//! built a `TriplePattern` of three cloned `String`s on every insert, only to
//! look up an entry that exists after the first edge of each shape.
//! `create_edge_with_properties` also cloned the whole property map into the
//! row store and dropped the original.
//!
//! What remains per edge is the caller's type string (`impl Into<EdgeType>`
//! from a `&str` allocates) plus amortised growth of the adjacency lists and
//! id-indexed arrays -- a few dozen reallocations over 10,000 edges. The bound
//! is 1.5 calls per edge; before the fix it was 5 for `create_edge` and 8 for
//! two integer properties.
//!
//! Counts calls through a global allocator, so this file holds one test: a
//! second test on another thread would count into the same total.

use samyama::graph::{GraphStore, PropertyMap, PropertyValue};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static CALLS: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static G: Counting = Counting;

fn calls() -> usize {
    CALLS.load(Ordering::Relaxed)
}

const EDGES: usize = 10_000;

#[test]
fn creating_a_relationship_costs_about_one_allocation() {
    let mut store = GraphStore::new();
    let a = store.create_node("Account");
    let b = store.create_node("Merchant");
    // First edge of the shape: the catalog and type tables take their keys.
    store.create_edge(a, b, "PAID").unwrap();
    let mut two = PropertyMap::new();
    two.insert("amount".to_string(), PropertyValue::Integer(0));
    two.insert("mcc".to_string(), PropertyValue::Integer(0));
    store.create_edge_with_properties(a, b, "PAID", two).unwrap();

    let before = calls();
    for _ in 0..EDGES {
        store.create_edge(a, b, "PAID").unwrap();
    }
    let bare = (calls() - before) as f64 / EDGES as f64;

    // The maps are the caller's; build them outside the counted window.
    let maps: Vec<PropertyMap> = (0..EDGES)
        .map(|i| {
            let mut m = PropertyMap::new();
            m.insert("amount".to_string(), PropertyValue::Integer(i as i64));
            m.insert("mcc".to_string(), PropertyValue::Integer((i % 900) as i64));
            m
        })
        .collect();
    let before = calls();
    let mut last = None;
    for m in maps {
        last = Some(store.create_edge_with_properties(a, b, "PAID", m).unwrap());
    }
    let with_props = (calls() - before) as f64 / EDGES as f64;

    eprintln!("allocator calls per edge: create_edge {bare:.2}, create_edge_with_properties (2 ints) {with_props:.2}");
    assert!(bare <= 1.5, "create_edge made {bare:.2} allocator calls per edge");
    assert!(
        with_props <= 1.5,
        "create_edge_with_properties made {with_props:.2} allocator calls per edge"
    );

    // Still the same graph: every edge is there, typed, and one carries its values.
    assert_eq!(store.edge_count(), 2 * EDGES + 2);
    let last = last.unwrap();
    assert_eq!(store.get_edge_endpoints(last), Some((a, b)));
    assert_eq!(store.get_edge_type(last).map(|t| t.as_str().to_string()), Some("PAID".to_string()));
    assert_eq!(store.edge_property(last, "amount"), Some(PropertyValue::Integer(EDGES as i64 - 1)));
    assert_eq!(store.edge_property(last, "mcc"), Some(PropertyValue::Integer((EDGES as i64 - 1) % 900)));
}
