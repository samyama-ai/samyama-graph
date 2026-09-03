//! Where BI-17's time actually goes (#1078).
//!
//! Four hypotheses about this query have been tested and all four were wrong,
//! so this one does not propose a fifth. It runs the query under conditions
//! that let a profiler attribute the time, and prints the shape of the work
//! (candidates considered vs rows emitted) so the ratio between them is a
//! measured number rather than an estimate.
//!
//!   cargo run --release --example bi17_profile -- --data-dir <sf1-dir>
//!   perf record -g ./target/release/examples/bi17_profile --data-dir <dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

/// Counting allocator.
///
/// The per-row cost is ~600-900 ns and flat across hop count (#1078), which
/// points at per-row machinery rather than traversal. `Record::clone_with_capacity`
/// allocates **twice** per emitted row -- once for the bindings vector, once for
/// `used_edges` -- so the question is whether those allocations are the cost or
/// merely present. Counting them is the difference between knowing and assuming,
/// and four assumptions about this query have already been wrong.
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(n as u64, Ordering::Relaxed);
        System.realloc(p, l, n)
    }
}

#[global_allocator]
static A: Counting = Counting;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;

/// The real BI-17, bounded on the outer scan so it terminates while still
/// running the full triangle logic for every row the bound keeps.
fn bounded(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
         WHERE a.id < {limit} AND a.id < b.id AND b.id < c.id
         RETURN count(a) AS triangles"
    )
}

/// One hop. The point of having it is to get a cost *per emitted row* that
/// owes nothing to the triangle shape, so the two- and three-hop numbers have
/// something to be compared against.
fn one_hop(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)
         WHERE a.id < {limit} AND a.id < b.id
         RETURN count(a) AS pairs"
    )
}

/// Two hops with **no predicates at all**, to price the `WHERE` clause.
///
/// The allocation count killed the hypothesis it was written to test: two hops
/// allocate 6.2 times per row against one hop's 1.05, yet cost only ~18% more
/// per row, which puts an allocation at ~23 ns and all six at a sixth of the
/// row. So the ~600 ns is something else, and property comparison is the next
/// candidate -- `a.id < b.id` reads two properties through the store per row,
/// and this query has two such comparisons.
///
/// Row counts differ from `open_path` (no predicate filters them), so only
/// ns/row is comparable between the two, never total time.
fn open_path_no_pred(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)
         WHERE a.id < {limit}
         RETURN count(a) AS candidates"
    )
}

/// The same shape with the closing hop removed: paths, not triangles. The
/// difference between this and `bounded` is what closing the cycle costs; the
/// value of this one alone is how many candidates the closing hop is handed.
fn open_path(limit: u64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)
         WHERE a.id < {limit} AND a.id < b.id AND b.id < c.id
         RETURN count(a) AS candidates"
    )
}

/// Trials per query. One run of each was not enough: the closing-hop cost is a
/// difference between two whole-query timings, and at ~0.2 s each the run-to-run
/// spread swamped it. The first version reported 2135, 344, **-257** and 93 ns
/// per candidate across four bounds of the same run -- a negative cost is not a
/// small cost, it is a measurement that cannot see what it is subtracting.
const TRIALS: usize = 7;

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn run(graph: &GraphStore, label: &str, cypher: &str) -> Option<(i64, f64)> {
    let q = match parse_query(cypher) {
        Ok(q) => q,
        Err(e) => {
            println!("{label:<26} parse failed: {e}");
            return None;
        }
    };
    // Warm once, then take the median of TRIALS. The first execution of any
    // query here pays one-off costs -- caches, lazily built indexes -- which is
    // why the smallest bound used to report 84,000 ns/row and look like a
    // finding.
    let _ = QueryExecutor::new(graph).execute(&q);
    let mut times = Vec::with_capacity(TRIALS);
    let mut last = None;
    for _ in 0..TRIALS {
        let t0 = std::time::Instant::now();
        last = Some(QueryExecutor::new(graph).execute(&q));
        times.push(t0.elapsed().as_secs_f64());
    }
    let secs_med = median(times);
    // One extra execution, measured for allocations rather than time. Kept
    // separate from the timed trials so the counter's own atomics do not
    // appear in the timings.
    let a0 = ALLOCS.load(Ordering::Relaxed);
    let b0 = BYTES.load(Ordering::Relaxed);
    let _ = QueryExecutor::new(graph).execute(&q);
    let allocs = ALLOCS.load(Ordering::Relaxed) - a0;
    let abytes = BYTES.load(Ordering::Relaxed) - b0;
    let t = std::time::Instant::now();
    match last.unwrap() {
        Ok(out) => {
            let v = out.records.first().and_then(|r| r.values().next().cloned());
            let n = format!("{v:?}")
                .chars()
                .filter(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse::<i64>()
                .ok();
            let _ = t;
            let secs = secs_med;
            let rate = n.map(|v| v as f64 / secs).unwrap_or(0.0);
            println!(
                "{label:<26} {secs:>8.3}s  rows={:>9}  {:>6.0} ns/row   allocs={:>10}  {:>6.1} allocs/row  {:>5} B/alloc",
                n.unwrap_or(-1),
                if rate > 0.0 { 1e9 / rate } else { 0.0 },
                allocs,
                allocs as f64 / n.unwrap_or(1).max(1) as f64,
                if allocs > 0 { abytes / allocs } else { 0 },
            );
            n.map(|v| (v, secs))
        }
        Err(e) => {
            println!("{label:<26} failed: {e}");
            None
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .expect("--data-dir <path> is required");

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    ldbc_bi_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded");

    // Growing bounds, so the scaling is visible rather than assumed: if the
    // cost per candidate is flat, time tracks the candidate count exactly, and
    // a departure from that is itself the finding.
    // What type is `Person.id` actually stored as? The predicates add ~3
    // allocations per row, and `Column::String::get` clones the String on every
    // read while `Column::Int::get` copies an i64 and allocates nothing. Which
    // one this is decides whether the cost is inherent to property reads or a
    // loader storing a number as text -- and it is one line to check rather
    // than infer from an allocation count.
    {
        use samyama::query::executor::QueryExecutor as QE;
        let q = parse_query("MATCH (a:Person) RETURN a.id AS v LIMIT 1").unwrap();
        match QE::new(&graph).execute(&q) {
            Ok(out) => {
                let v = out.records.first().and_then(|r| r.get("v")).cloned();
                println!("Person.id is stored as: {v:?}");
            }
            Err(e) => println!("Person.id probe failed: {e}"),
        }
    }

    // `perf` is unavailable here (perf_event_paranoid=4, and sysctl is not
    // ours to change), so the attribution has to come from the shape of the
    // numbers rather than from a profile. Rows per second at one, two and
    // three hops answers the question that matters: if it is roughly constant,
    // the cost is per-row machinery -- record construction, cloning,
    // allocation -- and not anything about traversal depth or the closing hop.
    for limit in [200u64, 400, 800, 1600] {
        println!("--- a.id < {limit} ---");
        let pairs = run(&graph, "  1 hop  (a-b)", &one_hop(limit));
        let cands = run(&graph, "  2 hops (a-b-c)", &open_path(limit));
        run(&graph, "  2 hops, no id preds", &open_path_no_pred(limit));
        let tris = run(&graph, "  3 hops (triangle)", &bounded(limit));
        // The triangle line's own `ns/row` is per *emitted triangle*, which is
        // one output per ~23 candidates and therefore reads as an alarming
        // five-figure number that means nothing. What closing the cycle
        // actually costs is the difference between the three-hop and two-hop
        // times spread over the candidates the closing hop was handed, so
        // derive that here rather than leaving the misleading column to be
        // quoted.
        if let (Some((c, c_s)), Some((t, t_s))) = (cands, tris) {
            if t > 0 {
                println!("  candidates per triangle: {:.1}", c as f64 / t as f64);
            }
            if c > 0 {
                println!(
                    "  closing hop:             {:>6.0} ns per candidate  ({:.3}s - {:.3}s over {c} candidates)",
                    (t_s - c_s) / c as f64 * 1e9, t_s, c_s
                );
            }
        }
        let _ = pairs;
    }
    Ok(())
}
