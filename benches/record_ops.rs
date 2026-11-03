//! What a `Record` costs, per row (#546).
//!
//! Filtering `WHERE i.v > 500` costs 102 ns per row over a no-filter baseline
//! on a dedicated box, of which the property read is ~23 ns (#557) and the
//! comparison itself is a register operation. Most of the rest is the record:
//! looking a variable up by name, and cloning the row to carry it forward.
//!
//! `Record` is a `Vec<(Arc<str>, Value)>` — already past the `HashMap<String,
//! Value>` it started as (#546 phases 1 and 2). The question this bench exists
//! to answer is whether the remaining cost justifies phase 3, which replaces
//! names with **slots assigned at plan time** and is a change to every operator
//! that constructs a row.
//!
//! Two numbers decide it:
//!
//!   * **lookup** — a linear scan comparing `Arc<str>` against a `&str`, versus
//!     an index into a `Vec<Value>`;
//!   * **clone** — what `ExpandOperator` pays once per output row, and the
//!     `Arc` refcount traffic that comes with it. That traffic is why parallel
//!     filtering lost 3.6-5.6x (#559), so it is also what stands between here
//!     and intra-query parallelism (`PERF-06`, #479).
//!
//!   cargo bench --bench record_ops

use std::sync::Arc;
use std::time::Instant;

use samyama::graph::{NodeId, PropertyValue};
use samyama::query::executor::{Record, Value};

#[path = "common/bench_setup.rs"]
mod bench_setup;

fn time<F: FnMut() -> u64>(label: &str, count: usize, mut f: F) -> f64 {
    std::hint::black_box(f());
    let started = Instant::now();
    let sink = f();
    let ns = started.elapsed().as_secs_f64() * 1e9 / count as f64;
    std::hint::black_box(sink);
    println!("{label:<50} {ns:>10.1}");
    ns
}

/// A record shaped like the ones LDBC IC5 carries: three bindings, a node
/// reference and two scalars.
fn typical() -> Record {
    let mut r = Record::new();
    r.bind("person", Value::NodeRef(NodeId::from(1u64)));
    r.bind("friend", Value::NodeRef(NodeId::from(2u64)));
    r.bind("forum", Value::NodeRef(NodeId::from(3u64)));
    r
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let n = 5_000_000usize;
    let record = typical();

    println!("A record with 3 bindings, {n} operations each.\n");
    println!("{:<50} {:>10}", "operation", "ns");
    println!("{:-<50} {:->10}", "", "");

    // Reading the *first* binding and the *last* one, because a linear scan
    // costs what position you are in, and an index does not.
    let first = time("get, first of 3 bindings", n, || {
        let mut acc = 0u64;
        for _ in 0..n {
            if let Some(Value::NodeRef(id)) = record.get(std::hint::black_box("person")) {
                acc = acc.wrapping_add(id.as_u64());
            }
        }
        acc
    });

    let last = time("get, last of 3 bindings", n, || {
        let mut acc = 0u64;
        for _ in 0..n {
            if let Some(Value::NodeRef(id)) = record.get(std::hint::black_box("forum")) {
                acc = acc.wrapping_add(id.as_u64());
            }
        }
        acc
    });

    let missing = time("get, name that is not bound", n, || {
        let mut acc = 0u64;
        for _ in 0..n {
            if record.get(std::hint::black_box("absent")).is_none() {
                acc = acc.wrapping_add(1);
            }
        }
        acc
    });

    // What phase 3 would replace it with.
    let slots: Vec<Value> = vec![
        Value::NodeRef(NodeId::from(1u64)),
        Value::NodeRef(NodeId::from(2u64)),
        Value::NodeRef(NodeId::from(3u64)),
    ];
    let by_slot = time("a Vec<Value>, indexed by slot", n, || {
        let mut acc = 0u64;
        for _ in 0..n {
            if let Some(Value::NodeRef(id)) = slots.get(std::hint::black_box(2usize)) {
                acc = acc.wrapping_add(id.as_u64());
            }
        }
        acc
    });

    println!();

    // The clone `ExpandOperator` pays per output row.
    let clone_ns = time("clone a 3-binding record", n / 5, || {
        let mut acc = 0u64;
        for _ in 0..(n / 5) {
            let copy = std::hint::black_box(record.clone());
            acc = acc.wrapping_add(copy.bindings().len() as u64);
        }
        acc
    });

    let slot_clone = time("clone a Vec<Value> of 3", n / 5, || {
        let mut acc = 0u64;
        for _ in 0..(n / 5) {
            let copy = std::hint::black_box(slots.clone());
            acc = acc.wrapping_add(copy.len() as u64);
        }
        acc
    });

    // Isolating the Arc traffic: cloning just the names, which is the part a
    // slot representation removes entirely and the part that contends when
    // records cross threads.
    let names: Vec<Arc<str>> = vec!["person".into(), "friend".into(), "forum".into()];
    let arc_ns = time("clone 3 Arc<str> (refcount traffic alone)", n / 5, || {
        let mut acc = 0u64;
        for _ in 0..(n / 5) {
            let copy = std::hint::black_box(names.clone());
            acc = acc.wrapping_add(copy.len() as u64);
        }
        acc
    });

    // Binding, which every operator does to derive a row.
    let grow_ns = time("clone, then bind a 4th (clone sized exactly)", n / 5, || {
        let mut acc = 0u64;
        for i in 0..(n / 5) {
            let mut copy = record.clone();
            copy.bind("extra", Value::Property(PropertyValue::Integer(i as i64)));
            acc = acc.wrapping_add(copy.bindings().len() as u64);
        }
        acc
    });

    let reserved_ns = time("clone_with_capacity(1), then bind a 4th", n / 5, || {
        let mut acc = 0u64;
        for i in 0..(n / 5) {
            let mut copy = record.clone_with_capacity(1);
            copy.bind("extra", Value::Property(PropertyValue::Integer(i as i64)));
            acc = acc.wrapping_add(copy.bindings().len() as u64);
        }
        acc
    });

    // What an Expand binding a target and an edge variable pays.
    time("clone_with_capacity(2), then bind two more", n / 5, || {
        let mut acc = 0u64;
        for i in 0..(n / 5) {
            let mut copy = record.clone_with_capacity(2);
            copy.bind("edge", Value::Property(PropertyValue::Integer(i as i64)));
            copy.bind("target", Value::NodeRef(NodeId::from(i as u64)));
            acc = acc.wrapping_add(copy.bindings().len() as u64);
        }
        acc
    });

    println!();
    println!("Deriving a row: {grow_ns:.1} ns to clone and bind against {reserved_ns:.1} ns when the clone");
    println!("reserves the room first. `Vec::clone` allocates exact capacity, so the next bind");
    println!("reallocates -- and clone-then-bind is how nearly every operator derives a row (#562).");
    println!();
    println!("Lookup: {last:.1} ns by name against {by_slot:.1} ns by slot, and a name that is");
    println!("not bound costs {missing:.1} ns because the scan has to finish. Position matters --");
    println!("{first:.1} ns first against {last:.1} ns last -- which an index would not.");
    println!();
    println!("Clone: {clone_ns:.1} ns per record, of which {arc_ns:.1} ns is Arc refcount traffic on");
    println!("the names alone, against {slot_clone:.1} ns for the values by themselves. That refcount");
    println!("traffic is atomic and on cache lines every thread shares, which is why parallel");
    println!("filtering lost 3.6-5.6x (#559) and why PERF-06 (#479) needs this first.");

    bench_setup::report_drift(calibration);
}
