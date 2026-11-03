//! Property-count scaling of node and edge memory in samyama-graph.
//!
//! `benches/memory_footprint.rs` fixes the fixture at two node properties and
//! zero edge properties. That is the right shape for LDBC, and the wrong shape
//! for a transaction graph, where the object carrying the money carries five to
//! ten attributes. This measures the slope: bytes per *property*, separately for
//! nodes and edges, and separately for the row store alone versus the row store
//! plus the columnar copy plus the MVCC version log.
//!
//! Same counting-allocator technique as `memory_footprint.rs`, deliberately, so
//! the two are comparable. Live heap only -- RSS is measured by the sibling
//! harness and tracks it at 1.05-1.16x.
//!
//!     cargo bench --bench prop_scaling
//!     cargo bench --bench prop_scaling -- --scale 200000 --degree 3
//!     cargo bench --bench prop_scaling -- --scale 200000 --degree 3 --sparse-edges
//!
//! `--sparse-edges` writes edge properties through `set_edge_property_sparse`,
//! which touches only the row map. It is NOT a valid configuration -- reads go
//! through `edge_columns` first and would return Null -- so the difference
//! between the two runs measures the size of the duplication, not the size of a
//! fix. Same framing as samyama-graph#545 on the node side.

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

/// A transaction-shaped property mix: reference strings, amounts, timestamps,
/// currency/MCC codes. Deliberately not all-integers -- a graph whose property
/// values are all 8-byte scalars flatters the storage layer.
fn props(i: usize, k: usize) -> Vec<(String, PropertyValue)> {
    let mut v = Vec::with_capacity(k);
    for p in 0..k {
        let (key, val) = match p % 4 {
            0 => (format!("s{p}"), PropertyValue::String(format!("TXN{:012}", i))),
            1 => (format!("f{p}"), PropertyValue::Float(i as f64 * 1.37)),
            2 => (format!("i{p}"), PropertyValue::Integer(i as i64)),
            _ => (format!("c{p}"), PropertyValue::String(format!("C{:03}", i % 900))),
        };
        v.push((key, val));
    }
    v
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let arg = |f: &str, d: usize| -> usize {
        a.iter()
            .position(|x| x == f)
            .and_then(|i| a.get(i + 1))
            .and_then(|s| s.parse().ok())
            .unwrap_or(d)
    };
    let n = arg("--scale", 200_000);
    let degree = arg("--degree", 3);
    let sparse_only = a.iter().any(|x| x == "--sparse-edges");

    println!(
        "Property scaling -- {n} nodes, degree {degree}{}",
        if sparse_only { "  [edge props: row map only]" } else { "" }
    );
    println!("{}", "-".repeat(72));
    println!(
        "{:<10} {:<10} {:>12} {:>14} {:>14} {:>10}",
        "node-props", "edge-props", "B/node", "B/edge(marg)", "B/edge(total)", "total MB"
    );

    for &np in &[0usize, 2, 4, 8, 12] {
        for &ep in &[0usize, 3] {
            let base = live();
            let mut s = GraphStore::new();
            let labels = ["Customer", "Account", "Txn", "Merchant"];
            let mut ids = Vec::with_capacity(n);
            for i in 0..n {
                ids.push(s.create_node(labels[i % 4]));
            }
            for (i, &id) in ids.iter().enumerate() {
                for (k, v) in props(i, np) {
                    let _ = s.set_node_property("default", id, k, v);
                }
            }
            let after_nodes = live();

            let ets = ["SENT", "RECEIVED", "AT", "OWNS"];
            let mut edges = 0usize;
            for (i, &src) in ids.iter().enumerate() {
                for d in 0..degree {
                    let tgt = ids[(i * 7 + d * 31 + 1) % n];
                    if let Ok(eid) = s.create_edge(src, tgt, ets[(i + d) % 4]) {
                        edges += 1;
                        for (k, v) in props(i, ep) {
                            if sparse_only {
                                s.set_edge_property_sparse(eid, k, v);
                            } else {
                                let _ = s.set_edge_property(eid, k, v);
                            }
                        }
                    }
                }
            }
            let after_edges = live();

            println!(
                "{:<10} {:<10} {:>12.1} {:>14.1} {:>14.1} {:>10.1}",
                np,
                ep,
                (after_nodes - base) as f64 / n as f64,
                (after_edges - after_nodes) as f64 / edges as f64,
                (after_edges - base) as f64 / edges as f64,
                (after_edges - base) as f64 / 1e6,
            );
            drop(s);
        }
    }
}
