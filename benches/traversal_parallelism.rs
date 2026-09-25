//! Does a single deep traversal use the cores it has? (PERF-06)
//!
//! PERF-06 asks that one deep traversal reach >=70% of available cores, with an
//! H1 target of >=4 cores at >=50% efficiency. Nothing had ever measured it.
//!
//! # What makes this measurable rather than a timing anecdote
//!
//! Mean cores used is `process CPU time / wall time` over the query, both read
//! from the kernel: CPU time from `/proc/self/stat` (utime + stime in clock
//! ticks), wall from a monotonic `Instant`. A query that pins one core reads
//! ~1.0 whatever the host is doing; a query across four reads ~4.0. It is a
//! ratio of two things the query itself caused, so it does not need a quiet
//! machine the way a latency number does.
//!
//! # The control is the point
//!
//! A traversal arm alone cannot tell "traversal is single-threaded" from "the
//! instrument reads 1.0 for everything". So a second arm runs a query on the
//! path that *is* parallel -- a node scan wide enough to clear the 1024-row
//! threshold in `NodeScanOperator`, with a filter predicate expensive enough to
//! clear the 256-row threshold in the filter operator (#559). If the control
//! also reads ~1.0, the instrument is broken and the run says so instead of
//! publishing a false absence.
//!
//! Reading the source first says what to expect: `rayon` appears at exactly two
//! places in `src/query/executor/operator.rs`, and neither is expansion. So the
//! expected result is a control well above 1 and a traversal at about 1. The
//! bench exists to check that reading against the running engine, because a
//! grep is not a measurement.
//!
//! # Cores alone can be satisfied by wasting cores
//!
//! Mean cores measures *parallelism*, and PERF-06 wants parallelism as a proxy
//! for speed. The two come apart whenever coordination costs more than the work
//! being coordinated, which for graph expansion is the common case: the unit of
//! work is "follow one edge and bind a record".
//!
//! #1460 demonstrated it. A morsel-driven parallel expansion raised core use
//! from 1.00 to 6.88 on a one-hop pattern and **cut throughput from 975 to 494
//! iterations** in the same window -- 13x the CPU to answer 2x slower, the extra
//! cores being rayon coordination rather than work. On cores alone that branch
//! moves PERF-06 most of the way to its H1 target while making the engine worse.
//!
//! So this reports **throughput beside cores** (#1461). A reader comparing two
//! runs can then see the case that matters: cores up and throughput down is a
//! regression wearing a requirement's colours, and no core count on its own can
//! tell you that happened.
//!
//! Usage: cargo bench --bench traversal_parallelism -- [--scale N] [--depth D]

use std::time::Instant;

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

/// Process CPU time in seconds (user + system), from the kernel.
///
/// Fields 14 and 15 of `/proc/self/stat` are utime and stime in clock ticks.
/// Everything before them is skipped from the closing parenthesis of the comm
/// field rather than by splitting on whitespace: a process name may contain a
/// space, and splitting would shift every field after it.
fn cpu_seconds() -> f64 {
    let s = match std::fs::read_to_string("/proc/self/stat") {
        Ok(s) => s,
        Err(_) => return 0.0,
    };
    let rest = match s.rfind(')') {
        Some(i) => &s[i + 1..],
        None => return 0.0,
    };
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After the comm field, field 3 is state, so utime is index 11 and stime 12.
    let utime: f64 = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let stime: f64 = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let hz = 100.0; // USER_HZ is 100 on every Linux target we build for.
    (utime + stime) / hz
}

/// Seconds each arm must run for before its ratio is trustworthy.
///
/// CPU time comes from the kernel in clock ticks, and USER_HZ is 100 -- a 10 ms
/// quantum. The first version of this bench ran its arms for 12 ms and 42 ms,
/// so the traversal arm's "0.96 cores" rested on about four ticks and carried
/// an error bar wide enough to swallow the finding. Each arm now repeats until
/// it has run for at least this long, which puts hundreds of ticks in the
/// numerator and makes the quantisation error negligible.
const MIN_WALL: f64 = 3.0;

/// Mean cores used by `f` repeated for at least `min_wall` seconds: the wall
/// time it actually took, and how many iterations that was.
fn cores_used_for<F: FnMut()>(min_wall: f64, mut f: F) -> (f64, f64, u64) {
    let c0 = cpu_seconds();
    let t0 = Instant::now();
    let mut iters = 0u64;
    loop {
        f();
        iters += 1;
        if t0.elapsed().as_secs_f64() >= min_wall { break; }
    }
    let wall = t0.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - c0;
    if wall <= 0.0 { (0.0, 0.0, iters) } else { (cpu / wall, wall, iters) }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let scale: usize = arg(&args, "--scale").and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let depth: usize = arg(&args, "--depth").and_then(|v| v.parse().ok()).unwrap_or(10);

    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);

    // ── fixture ─────────────────────────────────────────────────────────────
    //
    // A chain deep enough that a var-length traversal has real work to do, plus
    // a wide population of scannable nodes for the control arm. Built through
    // Cypher so the operators under test are the ones a user reaches.
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();

    let chain = scale / 2;

    // Built through the store API, not Cypher. `QueryEngine::execute` is
    // read-only, and more to the point a CREATE naming 100k nodes is a
    // multi-megabyte statement whose parse time would dwarf the query being
    // measured. The fixture must not cost more than the thing it sets up.
    let mut chain_ids = Vec::with_capacity(chain);
    for i in 0..chain {
        let id = store.create_node("Chain");
        if let Some(n) = store.get_node_mut(id) {
            n.set_property("id", i as i64);
        }
        chain_ids.push(id);
    }
    for w in chain_ids.windows(2) {
        let _ = store.create_edge(w[0], w[1], "NEXT");
    }

    for i in 0..scale {
        let id = store.create_node("Wide");
        if let Some(n) = store.get_node_mut(id) {
            n.set_property("id", i as i64);
            n.set_property("name", format!("node-{i}"));
        }
    }

    // ── arm 1: the control, on the path that is parallel ────────────────────
    //
    // A scan over every :Wide node with a predicate that costs enough per row
    // to be worth splitting. If this does not exceed one core, the instrument
    // is wrong and the traversal result below means nothing.
    let control_q = "MATCH (w:Wide) WHERE toUpper(w.name) CONTAINS 'NODE-9' \
                     AND size(w.name) > 3 RETURN count(w)";
    let (control_cores, control_wall, control_iters) = cores_used_for(MIN_WALL, || {
        let _ = engine.execute(control_q, &store);
    });

    // ── arm 2: the requirement, a single deep traversal ─────────────────────
    let trav_q = format!(
        "MATCH p=(a:Chain)-[:NEXT*1..{depth}]->(b:Chain) RETURN count(p)"
    );
    let (trav_cores, trav_wall, trav_iters) = cores_used_for(MIN_WALL, || {
        let _ = engine.execute(&trav_q, &store);
    });

    let efficiency = trav_cores / cores as f64;
    let instrument_ok = control_cores > 1.2;

    println!("{{");
    println!("  \"available_cores\": {cores},");
    println!("  \"scale\": {scale}, \"depth\": {depth}, \"chain_nodes\": {chain},");
    println!("  \"control_query\": {control_q:?},");
    println!("  \"control_mean_cores\": {control_cores:.3},");
    println!("  \"control_wall_s\": {control_wall:.3}, \"control_iters\": {control_iters},");
    println!("  \"control_throughput_per_s\": {:.2},",
             control_iters as f64 / control_wall.max(f64::MIN_POSITIVE));
    println!("  \"instrument_ok\": {instrument_ok},");
    println!("  \"traversal_query\": {trav_q:?},");
    println!("  \"traversal_mean_cores\": {trav_cores:.3},");
    println!("  \"traversal_wall_s\": {trav_wall:.3}, \"traversal_iters\": {trav_iters},");
    // The number a core count cannot substitute for. Compared across runs, a
    // rise in cores beside a fall here is the #1460 signature.
    println!("  \"traversal_throughput_per_s\": {:.2},",
             trav_iters as f64 / trav_wall.max(f64::MIN_POSITIVE));
    println!("  \"traversal_core_efficiency\": {efficiency:.4}");
    println!("}}");
}
