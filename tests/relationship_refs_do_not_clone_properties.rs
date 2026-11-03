//! Binding a relationship reference must not copy the relationship's
//! properties (samyama-graph#1190).
//!
//! Four sites built a `Value::EdgeRef` -- id, endpoints, type -- by calling
//! `GraphStore::get_edge`, which builds an owned `Edge` with the type string
//! and the entire property map cloned, and then dropped everything but three
//! fields:
//!
//! - the relationship list a variable-length pattern binds (`[r:T*1..2]`),
//! - the relationship an `EXISTS { }` subpattern binds,
//! - a bound relationship list walked again (`MATCH ()-[rs*]->()` with `rs`
//!   bound),
//! - the representative value `DISTINCT` / grouping keeps per relationship.
//!
//! So the cost of those queries grew with how many properties each
//! relationship carried, though none of them reads one. Same class as the
//! per-read clone #1186 removed for property reads.
//!
//! No wall clock: the same queries run over two relationship types that
//! differ only in carrying 1 or 40 properties, and allocator calls are
//! compared. A property map copy is ~81 allocator calls per relationship at
//! 40 string properties; the bound allows 10%.
//!
//! Counts calls through a global allocator, so this file holds one test.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
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

const EDGES: usize = 200;
const WIDE: usize = 40;

/// Two relationship types over the same endpoints. `NARROW` carries one
/// property; `WIDE` carries forty strings.
fn build() -> GraphStore {
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");
    for i in 0..EDGES {
        let mid = store.create_node("Mid");
        let leaf = store.create_node("Leaf");
        for (ty, width) in [("NARROW", 1usize), ("WIDE", WIDE)] {
            for (s, t) in [(hub, mid), (mid, leaf)] {
                let e = store.create_edge(s, t, ty).unwrap();
                for k in 0..width {
                    store.set_edge_property_sparse(
                        e,
                        format!("p{k}"),
                        PropertyValue::String(format!("value {k} of edge {i}")),
                    );
                }
            }
        }
    }
    store
}

/// Allocator calls to run `cypher`, and its row count.
fn cost(store: &GraphStore, cypher: &str) -> (usize, usize) {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let before = CALLS.load(Ordering::Relaxed);
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let calls = CALLS.load(Ordering::Relaxed) - before;
    (calls, out.records.len())
}

#[test]
fn a_relationship_reference_costs_the_same_however_many_properties_it_has() {
    let store = build();
    let shapes = [
        // The relationship list a variable-length pattern binds.
        "MATCH (h:Hub)-[r:{T}*1..2]->(x) RETURN size(r) AS n",
        // DISTINCT over relationships keeps a representative per group. Not
        // `RETURN DISTINCT r`: returning a relationship hands its properties to
        // the caller, which is a copy the query asked for.
        "MATCH (h:Hub)-[r:{T}]->(m) WITH DISTINCT r RETURN count(r) AS n",
        // The relationship an EXISTS subpattern binds.
        "MATCH (m:Mid) WHERE EXISTS { MATCH (m)-[r:{T}]->(l:Leaf) } RETURN count(m) AS n",
        // A bound relationship list, walked again.
        "MATCH (h:Hub)-[rs:{T}*2..2]->(l) WITH h, rs MATCH (h)-[rs*]->(l2) RETURN count(l2) AS n",
    ];
    // Every shape is measured before any is judged, so a failure reports all
    // of them.
    let mut copied = Vec::new();
    for shape in shapes {
        let narrow_q = shape.replace("{T}", "NARROW");
        let wide_q = shape.replace("{T}", "WIDE");
        // Warm both once: first-run costs (type indexes, caches) are not the
        // thing measured.
        cost(&store, &narrow_q);
        cost(&store, &wide_q);
        let (narrow, narrow_rows) = cost(&store, &narrow_q);
        let (wide, wide_rows) = cost(&store, &wide_q);
        assert_eq!(narrow_rows, wide_rows, "`{shape}`: the two types should match the same rows");
        assert!(narrow_rows > 0, "`{shape}` matched nothing");
        let ratio = wide as f64 / narrow as f64;
        eprintln!("{shape}: {narrow_rows} rows, allocator calls narrow {narrow}, wide {wide}, ratio {ratio:.2}");
        if ratio > 1.10 {
            copied.push(format!(
                "`{shape}`: {wide} allocator calls over {WIDE}-property relationships against \
                 {narrow} over 1-property ones ({ratio:.2}x)"
            ));
        }
    }
    assert!(
        copied.is_empty(),
        "a relationship's properties are being copied:\n{}",
        copied.join("\n")
    );
}
