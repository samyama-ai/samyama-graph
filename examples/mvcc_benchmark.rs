//! Benchmark for MVCC and Arena Allocation performance
//!
//! Measures the overhead of versioning and the efficiency of arena-based storage.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use samyama::graph::{GraphStore, Label, PropertyValue};
use std::time::Instant;

fn benchmark_arena_allocation() {
    let mut store = GraphStore::new();
    let start = Instant::now();
    
    // Create 1 Million nodes (Stress test arena resizing)
    for i in 0..1_000_000 {
        store.create_node("BenchmarkNode");
    }
    
    let duration = start.elapsed();
    println!("Arena Allocation (1M nodes): {:?} ({:.2} nodes/sec)", 
        duration, 1_000_000.0 / duration.as_secs_f64());
}

fn benchmark_mvcc_access() {
    let mut store = GraphStore::new();
    let node_id = store.create_node("VersionedNode");
    
    // Simulate versions (manual push for benchmark)
    for i in 2..=100 {
        store.current_version = i as u64;
        // In a real scenario, update would COW. Here we just read.
    }
    
    let start = Instant::now();
    for _ in 0..1_000_000 {
        let _ = black_box(store.get_node_at_version(node_id, 50));
    }
    let duration = start.elapsed();
    println!("MVCC Access (1M reads): {:?} ({:.2} ops/sec)", 
        duration, 1_000_000.0 / duration.as_secs_f64());
}

fn main() {
    println!("--- MVCC & Arena Benchmark ---");
    benchmark_arena_allocation();
    benchmark_mvcc_access();
}
