//! What one property read costs (#531).
//!
//! `CH-PROFILE-01` traced LDBC IC5's `Aggregate` cost to something more basic
//! than aggregation: an aggregate with **no grouping at all** — a global
//! `sum(r.weight)` — costs 427 ns per row, and almost all of that is reading
//! one property. The same constant is paid by `Filter` on every row it tests,
//! by `Sort` on every key it evaluates, and by `Project` on every column it
//! emits, so it is worth a reproducer of its own rather than being rediscovered
//! inside whichever operator is being looked at that week.
//!
//! The comparison that matters is against a dense array, because that is what
//! "columnar" normally means and what the cost *should* be:
//!
//!   * `ColumnStore::get_property` — the engine's path: property name to a
//!     column, then row index to a value, two hash lookups
//!   * a `Vec<i64>` indexed by row — one load, prefetchable
//!
//! Rows are visited **in id order**, which is the friendliest possible pattern
//! for both. A scan does exactly this, and the hash map still misses cache on
//! nearly every row because the hash scatters the access.
//!
//!   cargo bench --bench property_access
//!   cargo bench --bench property_access -- --rows 2000000 --fill 10

use std::time::Instant;

use samyama::graph::{GraphStore, PropertyValue};

#[path = "common/bench_setup.rs"]
mod bench_setup;

struct Fixture {
    store: GraphStore,
    /// Row indices to read, in ascending order.
    rows: Vec<usize>,
    /// Node ids carrying the same values in *row* storage rather than columns.
    ///
    /// The engine has two property paths and they perform very differently.
    /// `set_node_property` writes both; snapshot import writes only columns;
    /// and the LDBC loader writes only rows, via `node.set_property`. So the
    /// benchmark suite every performance decision rests on exercises the path
    /// that published `.sgsnap` KGs do not use (#534).
    row_nodes: Vec<samyama::graph::NodeId>,
}

/// `rows` nodes, of which one in `fill` carries the `sparse` property.
///
/// The fill factor is the parameter the design question turns on: a dense
/// array beats a hash map on a common property and loses on a rare one in a
/// large graph, so any decision to change the representation needs both
/// numbers rather than one.
fn build(rows: usize, fill: usize) -> Fixture {
    let mut store = GraphStore::new();
    let mut indices = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = store.create_node("Row");
        store.set_column_property(id, "dense_int", PropertyValue::Integer(i as i64));
        store.set_column_property(id, "dense_float", PropertyValue::Float(i as f64 * 0.5));
        store.set_column_property(
            id,
            "dense_str",
            PropertyValue::String(format!("row-{i:09}")),
        );
        if i % fill == 0 {
            store.set_column_property(id, "sparse_int", PropertyValue::Integer(i as i64));
        }
        indices.push(id.as_u64() as usize);
    }

    // A second population whose properties live only in row storage, written
    // the way the LDBC loader writes them.
    let mut row_nodes = Vec::with_capacity(rows);
    for i in 0..rows {
        let id = store.create_node("RowStored");
        if let Some(node) = store.get_node_mut(id) {
            node.set_property("row_int", PropertyValue::Integer(i as i64));
        }
        row_nodes.push(id);
    }

    Fixture { store, rows: indices, row_nodes }
}

fn time<F: FnMut() -> u64>(label: &str, count: usize, mut f: F) -> f64 {
    // Warm, so the first pass does not pay for page faults the others avoid.
    std::hint::black_box(f());
    let started = Instant::now();
    let sink = f();
    let ns = started.elapsed().as_secs_f64() * 1e9 / count as f64;
    std::hint::black_box(sink);
    println!("{label:<46} {ns:>10.1}");
    ns
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse().ok())
    };
    let rows = arg("--rows").unwrap_or(1_000_000);
    let fill = arg("--fill").unwrap_or(20);

    eprintln!("Building {rows} rows (sparse property on one in {fill})…");
    let started = Instant::now();
    let fx = build(rows, fill);
    eprintln!("built in {:.1}s\n", started.elapsed().as_secs_f64());

    let columns = &fx.store.node_columns;
    let ids = &fx.rows;

    println!("{:<46} {:>10}", "access", "ns");
    println!("{:-<46} {:->10}", "", "");

    let dense_int = time("get_property, dense integer column", rows, || {
        let mut acc = 0u64;
        for &idx in ids {
            if let PropertyValue::Integer(v) = columns.get_property(idx, "dense_int") {
                acc = acc.wrapping_add(v as u64);
            }
        }
        acc
    });

    time("get_property, dense float column", rows, || {
        let mut acc = 0u64;
        for &idx in ids {
            if let PropertyValue::Float(v) = columns.get_property(idx, "dense_float") {
                acc = acc.wrapping_add(v as u64);
            }
        }
        acc
    });

    time("get_property, dense string column (clones)", rows, || {
        let mut acc = 0u64;
        for &idx in ids {
            if let PropertyValue::String(s) = columns.get_property(idx, "dense_str") {
                acc = acc.wrapping_add(s.len() as u64);
            }
        }
        acc
    });

    time("get_property, sparse column, mostly misses", rows, || {
        let mut acc = 0u64;
        for &idx in ids {
            if let PropertyValue::Integer(v) = columns.get_property(idx, "sparse_int") {
                acc = acc.wrapping_add(v as u64);
            }
        }
        acc
    });

    time("get_column once, then Column::get per row", rows, || {
        let mut acc = 0u64;
        // Hoisting the name lookup out of the loop is available to any
        // operator that reads the same property for every row -- which is all
        // of them. If this is much cheaper than the line above, that is a
        // change an operator can make without touching the storage layout.
        if let Some(col) = columns.get_column("dense_int") {
            for &idx in ids {
                if let PropertyValue::Integer(v) = col.get(idx) {
                    acc = acc.wrapping_add(v as u64);
                }
            }
        }
        acc
    });

    let row_nodes = &fx.row_nodes;
    time("row storage, node.properties HashMap", rows, || {
        let mut acc = 0u64;
        for &id in row_nodes {
            if let Some(node) = fx.store.get_node(id) {
                if let Some(PropertyValue::Integer(v)) = node.get_property("row_int") {
                    acc = acc.wrapping_add(*v as u64);
                }
            }
        }
        acc
    });

    let plain: Vec<i64> = (0..rows as i64).collect();
    let vec_ns = time("a dense Vec<i64>, indexed by row", rows, || {
        let mut acc = 0u64;
        for &idx in ids {
            acc = acc.wrapping_add(plain[idx - 1] as u64);
        }
        acc
    });

    println!();
    println!(
        "A dense integer read costs {:.0}x an array index ({:.1} ns vs {:.1} ns).",
        dense_int / vec_ns.max(0.01),
        dense_int,
        vec_ns
    );
    println!("Most of that is cache, not hashing: at {rows} rows the inner table is tens of");
    println!("megabytes and the hash scatters access, so even a scan in id order misses.");

    bench_setup::report_drift(calibration);
}
