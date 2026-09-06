//! Measures resident memory per node and per edge (#477, PERF-10).
//!
//! `PERF-10` sets a target of ≤128 B/edge and records a current figure of
//! ~537 B/edge against FalkorDB's ~57. That figure had **no reproducer** — which
//! under spec 18 makes it unquotable, and in practice makes it unimprovable,
//! because nothing would tell us whether a change helped.
//!
//! Two independent measurements, deliberately:
//!
//!   * a **counting allocator** wrapping the system allocator, giving exact live
//!     heap bytes and letting each construction phase be attributed separately;
//!   * **RSS** from `/proc/self/statm`, which includes what the allocator cannot
//!     see — binary, stacks, allocator slack, fragmentation.
//!
//! They will not agree, and the gap is the point: allocator bytes are what a
//! layout change moves, RSS is what the machine actually has to have. Reporting
//! only one of them is how a "we cut memory 40%" claim survives an RSS that did
//! not move.
//!
//!   cargo bench --bench memory_footprint
//!   cargo bench --bench memory_footprint -- --json footprint.json
//!   cargo bench --bench memory_footprint -- --scale 200000

use samyama::graph::GraphStore;

// The LDBC loader, shared with `ldbc_benchmark` rather than re-implemented: a second
// loader would measure a second graph, and the point is to measure the one the
// benchmark builds.
mod ldbc_common;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

// ---------------------------------------------------------------- allocator

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);
static ALLOC_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Allocation-size histogram, bucketed by power of two (index = log2 of size,
/// capped). Knowing *what sizes* are being allocated is what turns "620 bytes
/// of overhead per node" into a specific structure to go and look at.
const BUCKETS: usize = 20;
static SIZE_HIST: [AtomicUsize; BUCKETS] = [const { AtomicUsize::new(0) }; BUCKETS];

fn bucket_of(size: usize) -> usize {
    if size == 0 {
        return 0;
    }
    let b = usize::BITS - size.leading_zeros();
    ((b as usize).saturating_sub(1)).min(BUCKETS - 1)
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        SIZE_HIST[bucket_of(layout.size())].fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREED.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= layout.size() {
            ALLOCATED.fetch_add(new_size - layout.size(), Ordering::Relaxed);
        } else {
            FREED.fetch_add(layout.size() - new_size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Live heap bytes: everything allocated minus everything freed.
fn live_heap() -> usize {
    ALLOCATED.load(Ordering::Relaxed).saturating_sub(FREED.load(Ordering::Relaxed))
}

/// Number of allocation calls so far. Allocation *count* matters
/// independently of bytes: each one carries a header and rounding, and the
/// per-object overhead is what a "collapse these Vecs" change removes.
fn alloc_calls() -> usize {
    ALLOC_CALLS.load(Ordering::Relaxed)
}

fn hist_snapshot() -> [usize; BUCKETS] {
    let mut out = [0usize; BUCKETS];
    for (i, slot) in SIZE_HIST.iter().enumerate() {
        out[i] = slot.load(Ordering::Relaxed);
    }
    out
}

/// Resident set size in bytes, or `None` off Linux.
fn rss() -> Option<usize> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: usize = statm.split_whitespace().nth(1)?.parse().ok()?;
    Some(resident_pages * 4096)
}

// ---------------------------------------------------------------- fixture

/// A synthetic graph with a realistic shape: labelled nodes with a couple of
/// properties, typed edges, average degree ~4.
///
/// Synthetic rather than LDBC so the harness runs anywhere; the numbers are
/// comparable across runs of this harness, which is what a regression check
/// needs. Absolute comparison against a competitor needs the same dataset on
/// both, and that is a separate exercise.
fn build(nodes: usize, avg_degree: usize) -> (GraphStore, usize, usize) {
    let mut store = GraphStore::new();
    let labels = ["Person", "Post", "Forum", "Tag"];
    let edge_types = ["KNOWS", "LIKES", "MEMBER_OF", "HAS_TAG"];

    let mut node_ids = Vec::with_capacity(nodes);
    for i in 0..nodes {
        let id = store.create_node(labels[i % labels.len()]);
        // Two properties per node: one string, one integer -- the common shape.
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            samyama::graph::PropertyValue::String(format!("n{i}")),
        );
        let _ = store.set_node_property(
            "default",
            id,
            "value".to_string(),
            samyama::graph::PropertyValue::Integer(i as i64),
        );
        node_ids.push(id);
    }

    let mut edges = 0usize;
    for (i, &src) in node_ids.iter().enumerate() {
        for d in 0..avg_degree {
            // Deterministic spread so the adjacency is not degenerate.
            let tgt = node_ids[(i * 7 + d * 31 + 1) % nodes];
            if store
                .create_edge(src, tgt, edge_types[(i + d) % edge_types.len()])
                .is_ok()
            {
                edges += 1;
            }
        }
    }
    (store, nodes, edges)
}

// ---------------------------------------------------------------- report

struct Phase {
    name: &'static str,
    heap_delta: usize,
}

/// Load a real LDBC SNB export and report its footprint (PERF-10 as specified).
///
/// RSS is the figure the spec names, and it is the honest one for "resident": the
/// live-heap number ignores allocator fragmentation and the return of freed pages,
/// both of which a customer pays for. Both are reported, with their ratio, because a
/// large gap between them is itself the finding — it says the cost is in the
/// allocator rather than in the data structures.
fn measure_real_dataset(dir: &std::path::Path, json_out: Option<String>) -> () {
    let base_heap = live_heap();
    let base_rss = rss();
    let hist_base = hist_snapshot();
    let t = std::time::Instant::now();

    let mut store = GraphStore::new();
    let loaded = match ldbc_common::load_dataset(&mut store, dir) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("could not load {}: {e}", dir.display());
            std::process::exit(1);
        }
    };
    let after_load = live_heap();

    // What a bulk load leaves behind in `Vec` slack, and what returning it is worth.
    // Reported as its own line rather than folded into the total: a load that never
    // shrinks is the state the engine ships in today, so both numbers are real.
    let before_shrink = live_heap();
    let rss_before_shrink = rss();
    store.shrink_to_fit();
    let after_shrink = live_heap();
    let rss_after_shrink = rss();

    // Does the allocator give the pages back?
    //
    // `shrink_to_fit` and CSR compaction both cut live heap and left RSS where it
    // was, which is the difference between "freed" and "returned". glibc keeps freed
    // arenas mapped; `malloc_trim` is the ask. If this moves RSS, the reclamations
    // are worth something against a *resident* target; if it does not, the footprint
    // has to be avoided at allocation time instead of reclaimed afterwards.
    #[cfg(target_env = "gnu")]
    let rss_after_trim = {
        unsafe extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        let returned = unsafe { malloc_trim(0) };
        eprintln!("malloc_trim returned {returned}");
        rss()
    };
    #[cfg(not(target_env = "gnu"))]
    let rss_after_trim = rss();

    // The planner's view, built here rather than lazily at first query, so the number
    // includes what a served graph actually holds.
    let _stats = store.statistics();
    let after_stats = live_heap();
    let final_rss = rss();
    let hist_end = hist_snapshot();
    let elapsed = t.elapsed();

    let nodes = loaded.total_nodes;
    let edges = loaded.total_edges;
    let heap = after_stats.saturating_sub(base_heap);
    // Filled by the structural walk below, so the JSON carries the *attribution*
    // and not only the `heap / nodes` ratio, which attributes the whole graph to
    // nodes and reads as "the footprint is node-side" when it is not.
    let mut node_side_bytes = 0usize;
    let rss_delta = match (base_rss, final_rss) {
        (Some(b), Some(f)) => f.saturating_sub(b) as i64,
        _ => -1,
    };

    println!("PERF-10 — real dataset at {}", dir.display());
    println!("{}", "-".repeat(78));
    println!("{:<28} {:>16}", "nodes", nodes);
    println!("{:<28} {:>16}", "edges", edges);
    println!("{:<28} {:>16}", "load seconds", elapsed.as_secs());
    println!("{:<28} {:>16}", "live heap (bytes)", heap);
    println!("{:<28} {:>16}", "RSS delta (bytes)", rss_delta);
    println!("{:<28} {:>16}", "statistics (bytes)", after_stats.saturating_sub(after_load));
    println!("{:<28} {:>16}", "returned by shrink_to_fit", before_shrink.saturating_sub(after_shrink));
    if let (Some(a), Some(b), Some(c)) = (rss_before_shrink, rss_after_shrink, rss_after_trim) {
        println!("{:<28} {:>16}", "  RSS before shrink", a);
        println!("{:<28} {:>16}", "  RSS after shrink", b);
        println!("{:<28} {:>16}", "  RSS after malloc_trim", c);
        println!("{:<28} {:>16}", "  RSS actually returned", a.saturating_sub(c));
    }
    if edges > 0 {
        println!("{:<28} {:>16.1}", "heap bytes/edge", heap as f64 / edges as f64);
        if rss_delta >= 0 {
            println!("{:<28} {:>16.1}", "RSS bytes/edge  <- PERF-10", rss_delta as f64 / edges as f64);
        }
    }
    if heap > 0 && rss_delta > 0 {
        println!("{:<28} {:>16.2}", "RSS / heap", rss_delta as f64 / heap as f64);
    }

    // What is *in* a node, since that is where the footprint sits.
    //
    // `bytes/node` scores the problem; this says which field to go and change. The
    // three quantities that matter are all about repetition: a property key is an
    // owned `String` per node, a label is an owned `String` per node, and SNB has a
    // handful of distinct values of each repeated across tens of millions of nodes.
    {
        let mut entries = 0usize;
        let mut key_bytes = 0usize;
        let mut label_count = 0usize;
        let mut label_bytes = 0usize;
        let mut distinct_keys: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut distinct_labels: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let mut value_bytes = 0usize;

        for node in store.all_nodes() {
            for (k, v) in node.properties.iter() {
                entries += 1;
                key_bytes += k.len();
                distinct_keys.insert(k.as_str());
                value_bytes += match v {
                    samyama::graph::PropertyValue::String(s) => s.len(),
                    _ => 0,
                };
            }
            for l in node.labels.iter() {
                label_count += 1;
                label_bytes += l.as_str().len();
                distinct_labels.insert(l.as_str());
            }
        }

        // What the repetition costs. A `String` is 24 bytes of handle plus a heap
        // buffer; interning replaces both with an index, so the recoverable amount
        // is the handles and buffers minus one copy of each distinct string.
        const STRING_HANDLE: usize = std::mem::size_of::<String>();
        let key_cost = entries * STRING_HANDLE + key_bytes;
        let key_interned = entries * std::mem::size_of::<u32>()
            + distinct_keys.iter().map(|k| k.len() + STRING_HANDLE).sum::<usize>();
        let label_cost = label_count * STRING_HANDLE + label_bytes;
        let label_interned = label_count * std::mem::size_of::<u32>()
            + distinct_labels.iter().map(|l| l.len() + STRING_HANDLE).sum::<usize>();
        let recoverable = key_cost.saturating_sub(key_interned)
            + label_cost.saturating_sub(label_interned);

        // The store's own decomposition, rather than a second walk here that could
        // drift from it. Capacity, not length: slack is resident.
        let report = store.memory_report();
        node_side_bytes = report.node_versions + report.node_properties + report.node_labels;

        println!("\nwhere the bytes are (structural walk, largest first)");
        println!("{:<30} {:>16} {:>10}", "structure", "bytes", "share");
        for (name, bytes) in report.lines() {
            if bytes == 0 {
                continue;
            }
            println!(
                "{:<30} {:>16} {:>9.1}%",
                name,
                bytes,
                if heap > 0 { 100.0 * bytes as f64 / heap as f64 } else { 0.0 }
            );
        }
        println!(
            "{:<30} {:>16} {:>9.1}%",
            "attributed",
            report.attributed(),
            if heap > 0 { 100.0 * report.attributed() as f64 / heap as f64 } else { 0.0 }
        );
        println!(
            "{:<30} {:>16}",
            "unattributed (indexes, etc.)",
            heap.saturating_sub(report.attributed())
        );

        // What the columnar store would cost for the same properties.
        //
        // `node_columns` exists and the snapshot path uses it; the LDBC path writes
        // row storage instead, so SNB pays a `HashMap<String, PropertyValue>` per
        // node. This estimates the alternative with the engine's *own* cost model
        // (`dense_is_smaller`) rather than a hand-rolled one, so the comparison
        // cannot flatter the design it is arguing for.
        {
            use std::collections::HashMap as Map;
            // key -> (entries, min index, max index, elem bytes)
            let mut cols: Map<&str, (usize, usize, usize, usize)> = Map::new();
            for node in store.all_nodes() {
                let idx = node.id.as_u64() as usize;
                for (k, v) in node.properties.iter() {
                    let elem = match v {
                        samyama::graph::PropertyValue::Boolean(_) => 1,
                        samyama::graph::PropertyValue::Integer(_)
                        | samyama::graph::PropertyValue::Float(_) => 8,
                        samyama::graph::PropertyValue::String(_) => STRING_HANDLE,
                        // No typed column: these land in `Other`, a sparse map of
                        // whole values.
                        _ => std::mem::size_of::<samyama::graph::PropertyValue>(),
                    };
                    let e = cols.entry(k.as_str()).or_insert((0, usize::MAX, 0, elem));
                    e.0 += 1;
                    e.1 = e.1.min(idx);
                    e.2 = e.2.max(idx);
                    e.3 = e.3.max(elem);
                }
            }
            let mut columnar = 0usize;
            let mut dense_cols = 0usize;
            for (name, (entries, lo, hi, elem)) in &cols {
                let span = hi.saturating_sub(*lo) + 1;
                let dense = span * elem + span.div_ceil(64) * 8;
                let sparse = entries * (8 + elem + 1) * 8 / 7;
                let bytes = if samyama::graph::storage::columnar::dense_is_smaller(span, *entries, *elem) {
                    dense_cols += 1;
                    dense
                } else {
                    sparse
                };
                columnar += bytes + name.len() + STRING_HANDLE;
            }
            // Plus the string contents, which are heap either way and cancel out of
            // the comparison; counted on both sides so neither is flattered.
            columnar += value_bytes;
            let row_based = report.node_properties;
            println!("\nnode properties: row storage vs the columnar store");
            println!("{:<30} {:>16}", "row storage (measured)", row_based);
            println!("{:<30} {:>16}", "columnar (engine cost model)", columnar);
            println!("{:<30} {:>16}", "columns", cols.len());
            println!("{:<30} {:>16}", "  of which dense", dense_cols);
            if row_based > columnar {
                let saved = row_based - columnar;
                println!("{:<30} {:>16}", "would save", saved);
                if heap > 0 {
                    println!("{:<30} {:>15.1}%", "  as a share of live heap", 100.0 * saved as f64 / heap as f64);
                }
                if edges > 0 {
                    println!("{:<30} {:>16.1}", "  bytes/edge", saved as f64 / edges as f64);
                }
            }
        }

        println!("\nrepetition (what interning would remove)");
        println!("{:<30} {:>16}", "property entries", entries);
        println!("{:<30} {:>16}", "distinct property keys", distinct_keys.len());
        println!("{:<30} {:>16}", "labels held", label_count);
        println!("{:<30} {:>16}", "distinct labels", distinct_labels.len());
        println!("{:<30} {:>16}", "string property value bytes", value_bytes);
        println!("{:<30} {:>16}", "recoverable by interning", recoverable);
        if heap > 0 {
            println!("{:<30} {:>15.1}%", "  as a share of live heap", 100.0 * recoverable as f64 / heap as f64);
        }
        if edges > 0 {
            println!("{:<30} {:>16.1}", "  bytes/edge it would remove", recoverable as f64 / edges as f64);
        }
    }

    // Which allocation sizes the load made. **These are calls, not live blocks** --
    // most are transient parsing churn -- so the histogram measures allocator
    // pressure during load and says nothing directly about residency. Read together
    // with `RSS / heap` it is still decisive in one direction: a load dominated by
    // tiny allocations that nonetheless ends at RSS ~= live heap has *not* paid
    // per-allocation overhead in resident bytes, which rules the allocator out and
    // points at the data itself. One `load_dataset` call builds nodes and edges
    // together, so the phase split the synthetic path reports is not available here.
    println!("\nallocation sizes during load (calls, not live blocks)");
    println!("{:<16} {:>14} {:>12}", "size", "count", "share");
    let total: usize = (0..BUCKETS).map(|i| hist_end[i].saturating_sub(hist_base[i])).sum();
    for i in 0..BUCKETS {
        let n = hist_end[i].saturating_sub(hist_base[i]);
        if n == 0 {
            continue;
        }
        let label = if i + 1 >= BUCKETS { format!(">={}", 1usize << i) } else { format!("{}..{}", 1usize << i, (1usize << (i + 1)) - 1) };
        println!("{:<16} {:>14} {:>11.1}%", label, n, 100.0 * n as f64 / total.max(1) as f64);
    }

    if let Some(path) = json_out {
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".into());
        let name = dir.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let body = format!(
            "{{
  \"suite\": \"memory-footprint\",
  \"requirement_ids\": [\"PERF-10\"],
  \"run_id\": \"footprint-{commit}-{name}\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"hardware\": {{\"note\": \"single process; RSS is host-dependent, heap bytes are not\"}},
  \"dataset\": {{\"name\": \"{name}\", \"nodes\": {nodes}, \"edges\": {edges}, \"synthetic\": false}},
  \"measurements\": {{
    \"live_heap_bytes\": {heap},
    \"rss_delta_bytes\": {rss_delta},
    \"load_seconds\": {},
    \"bytes_per_node_heap\": {:.2},
    \"bytes_per_edge_heap\": {:.2},
    \"bytes_per_edge_rss\": {:.2},
    \"node_side_bytes\": {node_side_bytes},
    \"node_side_share\": {:.4}
  }}
}}
",
            env!("CARGO_PKG_VERSION"),
            elapsed.as_secs(),
            if nodes > 0 { heap as f64 / nodes as f64 } else { 0.0 },
            if edges > 0 { heap as f64 / edges as f64 } else { 0.0 },
            if edges > 0 && rss_delta >= 0 { rss_delta as f64 / edges as f64 } else { -1.0 },
            if heap > 0 { node_side_bytes as f64 / heap as f64 } else { 0.0 },
        );
        if let Err(e) = std::fs::write(&path, body) {
            eprintln!("could not write {path}: {e}");
        } else {
            println!("\n-> {path}");
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<String> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1).cloned())
    };
    let scale: usize = arg("--scale").and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let avg_degree: usize = arg("--degree").and_then(|s| s.parse().ok()).unwrap_or(4);
    // Bulk mode uses the stub inserts plus compact_adjacency() -- the path that
    // only snapshot import takes today (#504). The stubs skip endpoint
    // validation, the Edge struct, edge_type_index and the catalog, so this
    // measures the ceiling of that route rather than a drop-in replacement.
    let bulk = args.iter().any(|a| a == "--bulk");
    // Compacting only at the end lowers steady-state memory but not the peak,
    // because the whole write buffer is built before it is folded into CSR.
    // `compact_adjacency_if_needed` exists for incremental compaction; this
    // measures whether using it actually moves the peak.
    let compact_every: usize = arg("--compact-every")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    // PERF-10 is specified on SNB SF10 — "≤128 B/edge resident" — and the synthetic
    // graph above is a proxy for it whose value moves 4.5x across degree 2..16. So a
    // number from it can bound nothing: the spec's target is a property of a
    // particular dataset, and the only way to report against that target is to load
    // that dataset.
    //
    // This path loads a real SNB export with the same loader the LDBC benchmark uses
    // and reports the same allocator and RSS figures. Nothing else in the bench
    // changes, so the synthetic and real numbers stay comparable.
    if let Some(dir) = arg("--ldbc-data") {
        return measure_real_dataset(std::path::Path::new(&dir), arg("--json"));
    }

    println!(
        "Memory footprint — {scale} nodes, target degree {avg_degree}{}",
        if bulk { "  [BULK: stub inserts + compact_adjacency]" } else { "" }
    );
    println!("{}", "-".repeat(78));

    let base_heap = live_heap();
    let base_calls = alloc_calls();
    let hist_base = hist_snapshot();
    let base_rss = rss();
    let t_start = std::time::Instant::now();

    // Phase 1: nodes only.
    let mut store = GraphStore::new();
    let labels = ["Person", "Post", "Forum", "Tag"];
    let mut node_ids = Vec::with_capacity(scale);
    for i in 0..scale {
        let id = if bulk {
            store.create_node_stub(labels[i % labels.len()])
        } else {
            store.create_node(labels[i % labels.len()])
        };
        node_ids.push(id);
    }
    let after_bare_nodes = live_heap();
    let calls_bare_nodes = alloc_calls();
    let hist_bare_nodes = hist_snapshot();
    let t_nodes = t_start.elapsed();

    // Phase 2: node properties.
    for (i, &id) in node_ids.iter().enumerate() {
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            samyama::graph::PropertyValue::String(format!("n{i}")),
        );
        let _ = store.set_node_property(
            "default",
            id,
            "value".to_string(),
            samyama::graph::PropertyValue::Integer(i as i64),
        );
    }
    let after_props = live_heap();
    let calls_props = alloc_calls();
    let hist_props = hist_snapshot();
    let t_props = t_start.elapsed();

    // Phase 3: edges.
    let edge_types = ["KNOWS", "LIKES", "MEMBER_OF", "HAS_TAG"];
    let mut edges = 0usize;
    for (i, &src) in node_ids.iter().enumerate() {
        for d in 0..avg_degree {
            let tgt = node_ids[(i * 7 + d * 31 + 1) % scale];
            let ok = if bulk {
                store.create_edge_stub(src, tgt, edge_types[(i + d) % edge_types.len()]).is_ok()
            } else {
                store.create_edge(src, tgt, edge_types[(i + d) % edge_types.len()]).is_ok()
            };
            if ok {
                edges += 1;
            }
        }
        if bulk && compact_every > 0 && i % compact_every == 0 && i > 0 {
            store.compact_adjacency_if_needed(1);
        }
    }
    if bulk {
        // The full finishing sequence, not just compaction. Measuring only
        // `compact_adjacency()` here overstated the bulk path badly: it made
        // edge inserts look 5.1x faster while omitting the edge-type index,
        // catalog and vector-index rebuilds that a bulk load must also do.
        // On real LDBC SF1 those rebuilds cost 14.3 s and consumed the entire
        // insert gain, leaving load time unchanged (#504).
        store.finish_bulk_load();
    }
    let after_edges = live_heap();
    let calls_edges = alloc_calls();
    let hist_edges = hist_snapshot();
    let t_edges = t_start.elapsed();

    // Phase 4: statistics (the planner's view; built lazily elsewhere).
    let _stats = store.statistics();
    let after_stats = live_heap();

    let final_rss = rss();

    let phases = [
        Phase { name: "node structs", heap_delta: after_bare_nodes.saturating_sub(base_heap) },
        Phase { name: "node properties", heap_delta: after_props.saturating_sub(after_bare_nodes) },
        Phase { name: "edges + adjacency", heap_delta: after_edges.saturating_sub(after_props) },
        Phase { name: "statistics", heap_delta: after_stats.saturating_sub(after_edges) },
    ];

    let total_heap = after_stats.saturating_sub(base_heap);

    println!("{:<24} {:>14} {:>12}", "phase", "bytes", "share");
    for p in &phases {
        let share = if total_heap > 0 { 100.0 * p.heap_delta as f64 / total_heap as f64 } else { 0.0 };
        println!("{:<24} {:>14} {:>11.1}%", p.name, p.heap_delta, share);
    }
    println!("{:<24} {:>14}", "total (live heap)", total_heap);
    println!();

    println!(
        "allocations: {:>10} for nodes ({:.2}/node), {:>10} for properties ({:.2}/node), {:>10} for edges ({:.2}/edge)",
        calls_bare_nodes - base_calls,
        (calls_bare_nodes - base_calls) as f64 / scale as f64,
        calls_props - calls_bare_nodes,
        (calls_props - calls_bare_nodes) as f64 / scale as f64,
        calls_edges - calls_props,
        if edges > 0 { (calls_edges - calls_props) as f64 / edges as f64 } else { 0.0 },
    );
    // Throughput matters as much as bytes here: allocation count caps insert
    // rate independently of how much memory each object ends up occupying,
    // which is the PERF-14 half of this measurement.
    let edge_secs = (t_edges - t_props).as_secs_f64();
    println!(
        "insert rate: {:.0} nodes/s, {:.0} edges/s",
        scale as f64 / t_nodes.as_secs_f64().max(1e-9),
        edges as f64 / edge_secs.max(1e-9),
    );
    // Where the node-phase allocations went, by size class.
    println!();
    println!("node-phase allocations by size class:");
    let mut shown = false;
    for i in 0..BUCKETS {
        let n = hist_bare_nodes[i].saturating_sub(hist_base[i]);
        if n == 0 {
            continue;
        }
        let lo = if i == 0 { 0 } else { 1usize << i };
        let hi = (1usize << (i + 1)) - 1;
        println!(
            "  {:>6}..{:<6} B  {:>10} allocs  {:>6.2}/node",
            lo,
            hi,
            n,
            n as f64 / scale as f64
        );
        shown = true;
    }
    if !shown {
        println!("  (none)");
    }

    // Same for the edge phase. The node histogram is what located the 512-byte
    // MVCC version vector (#495); this is the equivalent view of the 14.5
    // allocations each edge still costs.
    println!();
    println!("edge-phase allocations by size class:");
    let mut shown_e = false;
    for i in 0..BUCKETS {
        let n = hist_edges[i].saturating_sub(hist_props[i]);
        if n == 0 {
            continue;
        }
        let lo = if i == 0 { 0 } else { 1usize << i };
        let hi = (1usize << (i + 1)) - 1;
        println!(
            "  {:>6}..{:<6} B  {:>10} allocs  {:>6.2}/edge",
            lo,
            hi,
            n,
            if edges > 0 { n as f64 / edges as f64 } else { 0.0 }
        );
        shown_e = true;
    }
    if !shown_e {
        println!("  (none)");
    }

    println!();
    println!("nodes: {scale}    edges: {edges}");
    println!("{:<28} {:>10.1}", "bytes/node (heap)", total_heap as f64 / scale as f64);
    println!(
        "{:<28} {:>10.1}",
        "bytes/edge (heap)",
        if edges > 0 { total_heap as f64 / edges as f64 } else { 0.0 }
    );

    // Edge-attributable cost on its own: the number PERF-10 is written against.
    let edge_only = phases[2].heap_delta;
    println!(
        "{:<28} {:>10.1}   <- edges+adjacency only",
        "bytes/edge (edge phase)",
        if edges > 0 { edge_only as f64 / edges as f64 } else { 0.0 }
    );

    if let (Some(b), Some(f)) = (base_rss, final_rss) {
        let rss_delta = f.saturating_sub(b);
        println!();
        println!("{:<28} {:>10}", "RSS delta (bytes)", rss_delta);
        println!(
            "{:<28} {:>10.1}",
            "bytes/edge (RSS)",
            if edges > 0 { rss_delta as f64 / edges as f64 } else { 0.0 }
        );
        let ratio = if total_heap > 0 { rss_delta as f64 / total_heap as f64 } else { 0.0 };
        println!("{:<28} {:>10.2}x", "RSS / live heap", ratio);
        println!();
        println!("The gap between the two is allocator slack, fragmentation and the");
        println!("binary itself. A layout change moves the heap number; only the RSS");
        println!("number decides whether a graph fits in a given machine.");
    } else {
        println!();
        println!("(RSS unavailable — /proc/self/statm is Linux-only)");
    }

    if let Some(path) = arg("--json") {
        let commit = std::process::Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_else(|| "unknown".into());
        let rss_delta = match (base_rss, final_rss) {
            (Some(b), Some(f)) => f.saturating_sub(b) as i64,
            _ => -1,
        };
        let phase_json = phases
            .iter()
            .map(|p| format!("{{\"phase\": \"{}\", \"bytes\": {}}}", p.name, p.heap_delta))
            .collect::<Vec<_>>()
            .join(",\n      ");
        let envelope = format!(
            "{{
  \"suite\": \"memory-footprint\",
  \"requirement_ids\": [\"PERF-10\"],
  \"run_id\": \"footprint-{commit}-{scale}n-{avg_degree}d\",
  \"engine\": {{\"name\": \"samyama\", \"version\": \"{}\", \"commit\": \"{commit}\"}},
  \"hardware\": {{\"note\": \"single process; RSS is host-dependent, heap bytes are not\"}},
  \"dataset\": {{\"name\": \"synthetic\", \"nodes\": {scale}, \"edges\": {edges}, \"avg_degree\": {avg_degree}}},
  \"measurements\": {{
    \"live_heap_bytes\": {total_heap},
    \"rss_delta_bytes\": {rss_delta},
    \"bytes_per_node_heap\": {:.2},
    \"bytes_per_edge_heap\": {:.2},
    \"bytes_per_edge_edge_phase\": {:.2},
    \"phases\": [
      {phase_json}
    ]
  }},
  \"status\": \"measured\",
  \"artifacts\": [\"benches/memory_footprint.rs\"],
  \"caveat\": \"Synthetic dataset. Comparable across runs of this harness; not directly comparable to a competitor's figure on a different dataset.\"
}}
",
            env!("CARGO_PKG_VERSION"),
            total_heap as f64 / scale as f64,
            if edges > 0 { total_heap as f64 / edges as f64 } else { 0.0 },
            if edges > 0 { edge_only as f64 / edges as f64 } else { 0.0 },
        );
        match std::fs::write(&path, envelope) {
            Ok(()) => println!("\nwrote result envelope: {path}"),
            Err(e) => {
                eprintln!("could not write {path}: {e}");
                std::process::exit(1);
            }
        }
    }

    // Keep the store alive to here so nothing is freed before measurement.
    drop(store);
    let _ = build; // the shared fixture builder is kept for future LDBC wiring
}
