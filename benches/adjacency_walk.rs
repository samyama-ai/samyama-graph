//! What one adjacency step costs, and where it goes (#520, Axiom 2).
//!
//! `Expand` is the largest operator in every hot LDBC query. Normalising its
//! cost by **edges visited** rather than rows emitted makes the two hot cases
//! agree: IC5 and IC9 both sit around 300-365 ns per edge visited, and the
//! reason IC9 looks four times worse per *row* is that an LDBC `Person` has
//! ~680 incoming edges of which its pattern keeps 125, where IC5's keeps 513.
//!
//! 300 ns is what needs explaining. The adjacency itself is CSR — `offsets`
//! into a packed `Vec<(NodeId, EdgeId)>`, contiguous per node, sequential to
//! walk. But the type filter is:
//!
//! ```ignore
//! self.edge_type_ids.get(edge_id.as_u64() as usize)
//! ```
//!
//! and edge ids within one node's adjacency run are **not** contiguous — they
//! are whatever order the edges were created. So every edge visited makes a
//! scattered read into an array with one entry per edge in the graph, which at
//! LDBC SF1's 21.1M edges is 42 MB and misses cache essentially every time.
//!
//! **That hypothesis is wrong, and this bench is what refuted it** — but the
//! first version of the bench refuted it on a graph that never scattered
//! anything. It wrote each node's whole adjacency consecutively, so the edge
//! ids inside one run *were* contiguous and the probe walked the array in
//! order. The premise in the paragraph above was not the thing being measured.
//!
//! `--order by-type` builds the layout a loader actually produces — every edge
//! of one type across every node, then the next type, which is how LDBC loads
//! `KNOWS`, then `HAS_INTEREST`, then `LIKES` — so a node's adjacency holds ids
//! from widely separated ranges. It is now the default. 300k nodes x 30 edges
//! over 6 types, one host, one run:
//!
//! | edges written | walk, wildcard | walk, one type of six | as Cypher |
//! |---|---:|---:|---:|
//! | node by node (the old build) | 2.6 ns/edge | 2.6 | 64.3 |
//! | **type by type** | **1.5 ns/edge** | **1.6** | **60.0** |
//!
//! So the conclusion survives the corrected premise, and by a wider margin: the
//! walk, type probe included, costs **1.5 ns** per edge. Running the same
//! traversal through the query engine costs **40x** that. Storage is a couple
//! of percent of what `Expand` spends; the rest is the operator.
//!
//! That caveat about cache is now settled, and the answer is no. At 9M edges
//! the type array is 18 MB and fits this host's 24 MB L3; at 36M it is 72 MB
//! and does not. The walk is **1.4 ns/edge at both**, so the scattered type
//! probe is not cache-bound — the prefetcher handles it, helped by `ByType`
//! giving each node's run a constant stride.
//!
//! What *does* scale is the **target-label check**, which is not storage at
//! all. `ExpandOperator::keeps` tests a far-end label by probing
//! `nodes_with_label`, a `HashSet<NodeId>` holding every node carrying it, once
//! per candidate edge:
//!
//! | nodes / edges | traversal, far end unlabelled | the label probe | total |
//! |---|---:|---:|---:|
//! | 300k / 9M | 52.5 ns/edge | **10.2** | 62.7 |
//! | 1.2M / 36M | 67.2 ns/edge | **36.7** | ~104 |
//!
//! The probe grows 3.6x with the label, to **35% of the whole traversal**,
//! because a label covering most of the graph is a multi-megabyte hash table
//! and every candidate edge is a random probe into it. A dense bitset indexed
//! by node id would be 150 KB for 1.2M nodes and a load-shift-mask (#665).
//!
//! It also fixes the normalisation. Per *edge visited*, IC5 (407 ns/row, keeps
//! 513 of ~680) and IC9 (1976 ns/row, keeps 125 of ~680) look four times apart;
//! per *emitted row* they agree, and they agree with the synthetic case here.
//! Cost tracks rows produced, not edges walked — which is the opposite of what
//! "make the traversal faster" would suggest, and worth knowing before anyone
//! rewrites the adjacency layout.
//!
//!   cargo bench --bench adjacency_walk
//!   cargo bench --bench adjacency_walk -- --nodes 500000 --degree 40

use std::time::Instant;

use samyama::graph::GraphStore;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// The order edges are created in, which decides how the edge ids inside one
/// node's adjacency run are laid out.
///
/// This distinction is the point of the bench and the first version did not
/// make it. `ByNode` writes a node's whole adjacency consecutively, so its edge
/// ids are contiguous and the `edge_type_ids` probe walks the array in order.
/// `ByType` writes every edge of one type across every node before starting the
/// next type, so a node's adjacency holds ids from `types` widely separated
/// ranges — which is what a real loader produces, LDBC's included: it loads
/// `KNOWS` for every person, then `HAS_INTEREST` for every person, then
/// `LIKES`.
#[derive(Clone, Copy, PartialEq)]
enum Order {
    ByNode,
    ByType,
}

/// `nodes` nodes, each with `degree` outgoing edges to scattered targets, of
/// which one in `types` is the type a query would ask for.
///
/// Scattered *targets* in both orders; the edge ids are scattered only under
/// `ByType`. The original bench built `ByNode`, described itself as measuring
/// "what a scattered probe keyed on those ids costs", and concluded the
/// scattered probe was cheap — on a layout that never scattered it.
fn build(
    nodes: usize,
    degree: usize,
    types: usize,
    order: Order,
) -> (GraphStore, Vec<samyama::graph::NodeId>) {
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..nodes).map(|_| store.create_node("N")).collect();
    let names: Vec<String> = (0..types).map(|t| format!("TYPE{t}")).collect();
    let target_of = |i: usize, d: usize| -> samyama::graph::NodeId {
        let x = (i as u64)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add((d as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9));
        let x = x ^ (x >> 31);
        ids[(x % nodes as u64) as usize]
    };
    match order {
        Order::ByNode => {
            for (i, &src) in ids.iter().enumerate() {
                for d in 0..degree {
                    let target = target_of(i, d);
                    if target != src {
                        let _ = store.create_edge(src, target, names[d % types].as_str());
                    }
                }
            }
        }
        Order::ByType => {
            for d in 0..degree {
                for (i, &src) in ids.iter().enumerate() {
                    let target = target_of(i, d);
                    if target != src {
                        let _ = store.create_edge(src, target, names[d % types].as_str());
                    }
                }
            }
        }
    }
    (store, ids)
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok())
    };
    let nodes = arg("--nodes").unwrap_or(300_000);
    let degree = arg("--degree").unwrap_or(30);
    let types = arg("--types").unwrap_or(6);

    let order = match args.iter().position(|a| a == "--order").and_then(|i| args.get(i + 1)) {
        Some(o) if o == "by-type" => Order::ByType,
        Some(o) if o == "by-node" => Order::ByNode,
        Some(o) => panic!("--order takes by-node or by-type, not {o}"),
        // `by-type` is the default because it is the layout a loader produces.
        // The old default was the other one, and it is what made this bench
        // report that a scattered type probe costs nothing.
        None => Order::ByType,
    };

    eprintln!(
        "Building {nodes} nodes x {degree} edges over {types} types, {}…",
        if order == Order::ByType { "edges written type by type" } else { "edges written node by node" }
    );
    let started = Instant::now();
    let (store, ids) = build(nodes, degree, types, order);
    eprintln!(
        "built {} edges in {:.1}s\n",
        store.edge_count(),
        started.elapsed().as_secs_f64()
    );

    let wanted = store
        .edge_type_id(&samyama::graph::EdgeType::new("TYPE0"))
        .expect("the type exists");
    let filter = [wanted];

    // Every edge incident to every node, both variants, same order.
    let visited = nodes * degree;

    let run = |label: &str, f: &dyn Fn() -> (u64, u64)| -> f64 {
        let _ = f();
        let started = Instant::now();
        let (seen, kept) = f();
        let ns = started.elapsed().as_secs_f64() * 1e9 / visited as f64;
        println!("{label:<48} {ns:>9.1}  {seen:>12} {kept:>12}");
        ns
    };

    println!("{} edges visited per pass\n", visited);
    println!("{:<48} {:>9}  {:>12} {:>12}", "walk", "ns/edge", "visited", "kept");
    println!("{:-<48} {:->9}  {:->12} {:->12}", "", "", "", "");

    // The no-probe baseline. `get_outgoing_neighbor_slice` hands back the write
    // buffer's raw adjacency run without consulting `edge_type_ids` at all, so
    // the difference between this line and the wildcard below **is** the type
    // probe — one random read per edge into an array with one `u16` per edge in
    // the graph.
    //
    // The bench previously had no such line: both of its walks probed, so the
    // probe could only be inferred from how the total moved with graph size.
    // At 18 MB and 72 MB that inference said "free"; #738 measures 25 ns/edge
    // at SF10, where the array is 353 MB, and this is the line that says how
    // much of that is the probe (#738).
    let raw = run("raw adjacency slice (no type probe)", &|| {
        let (mut seen, mut kept) = (0u64, 0u64);
        for &id in &ids {
            for &(_t, _e) in store.get_outgoing_neighbor_slice(id) {
                seen += 1;
                kept += 1;
            }
        }
        (seen, kept)
    });

    let unfiltered = run("wildcard (still probes the type array)", &|| {
        let (mut seen, mut kept) = (0u64, 0u64);
        for &id in &ids {
            store.for_each_outgoing_neighbor(id, None, |_t, _e| {
                seen += 1;
                kept += 1;
            });
        }
        (seen, kept)
    });

    let filtered = run("one type of six", &|| {
        let (mut seen, mut kept) = (0u64, 0u64);
        for &id in &ids {
            store.for_each_outgoing_neighbor(id, Some(&filter), |_t, _e| {
                kept += 1;
            });
            seen += 1;
        }
        (seen, kept)
    });

    // The same traversal through the query engine, so the raw walk and the
    // operator around it are bracketed on one host in one run. `Expand` on
    // LDBC measures 300-365 ns per edge visited; if the walk is 3 ns then the
    // rest is the operator, and this says how much of it.
    let expand_ns = {
        use samyama::query::executor::QueryExecutor;
        use samyama::query::parser::parse_query;
        // Two forms, differing only in whether the far end carries a label.
        //
        // `keeps` tests a target label by probing `nodes_with_label`, a
        // `HashSet<NodeId>` holding every node with that label — so a label
        // covering most of the graph means a random probe into a multi-megabyte
        // hash set **per candidate edge**. The difference between these two
        // lines is what that costs, and it is the thing that scales with graph
        // size while the walk itself does not (#665).
        let labelled = "MATCH (a:N)-[:TYPE0]->(b:N) RETURN count(b) AS c";
        let unlabelled = "MATCH (a:N)-[:TYPE0]->(b) RETURN count(b) AS c";
        let time_it = |cypher: &str| -> f64 {
            let query = parse_query(cypher).expect("query should parse");
            let _ = QueryExecutor::new(&store).execute(&query).expect("query should run");
            let started = Instant::now();
            let out = QueryExecutor::new(&store).execute(&query).expect("query should run");
            let ms = started.elapsed().as_secs_f64() * 1000.0;
            let _ = out;
            ms
        };
        let ms_unlabelled = time_it(unlabelled);
        let ms = time_it(labelled);
        println!(
            "{:<48} {:>9.1}  {:>12} {:>12}",
            "the same traversal, far end unlabelled",
            ms_unlabelled * 1e6 / visited as f64,
            visited,
            visited / types
        );
        println!(
            "{:<48} {:>9.1}  {:>12} {:>12}",
            "  ... so the target-label probe costs",
            (ms - ms_unlabelled) * 1e6 / visited as f64,
            "",
            ""
        );
        // Rows emitted is the traversal's keep-count: one row per surviving edge.
        let emitted = visited / types;
        println!(
            "{:<48} {:>9.1}  {:>12} {:>12}",
            "the same traversal as Cypher (whole query)",
            ms * 1e6 / visited as f64,
            visited,
            emitted
        );
        println!(
            "{:<48} {:>9.1}  {:>12} {:>12}",
            "  ... per emitted row",
            ms * 1e6 / emitted as f64,
            "",
            ""
        );
        ms * 1e6 / visited as f64
    };

    println!();
    println!("Both numbers include one scattered probe of `edge_type_ids` per edge visited:");
    println!("the wildcard case still probes it, because an edge whose type is UNSET has to be");
    println!("rejected. At {} edges the array is {:.0} MB.", store.edge_count(), store.edge_count() as f64 * 2.0 / 1e6);
    println!();
    println!("LDBC IC5 and IC9 both measure ~300-365 ns per edge visited on SF1, whose type");
    println!("array is 42 MB (#520).");
    println!();
    println!(
        "The type probe costs {:.1} ns per edge here: {raw:.1} ns to walk the raw adjacency",
        (unfiltered - raw).max(0.0)
    );
    println!(
        "slice against {unfiltered:.1} with the probe, on a {:.0} MB array. That difference is what",
        store.edge_count() as f64 * 2.0 / 1e6
    );
    println!("#738 is about, and it is the number to watch as the array outgrows cache.");
    println!();
    println!("The walk itself is {unfiltered:.1} ns per edge. Running the same traversal through the");
    println!("query engine costs {expand_ns:.1} ns per edge visited -- {:.0}x the walk. Whatever `Expand`", expand_ns / unfiltered.max(0.01));
    println!("costs, storage is not where it goes.");
    println!();
    println!("Read the per-emitted-row line, not the per-edge one. Per edge visited, IC5 and IC9");
    println!("look four times apart; per emitted row they agree with each other and with the");
    println!("figure here. Expand's cost tracks rows produced, not edges walked.");
    let _ = filtered;

    bench_setup::report_drift(calibration);
}
