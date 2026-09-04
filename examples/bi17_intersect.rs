//! What is BI-17 worth if the closing hop is an intersection? (#1082)
//!
//! BI-17 counts friend triangles:
//!
//!   MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
//!   WHERE a.id < b.id AND b.id < c.id
//!   RETURN count(a)
//!
//! The executor builds every two-hop row and tests the closing edge. The
//! profile taken on 2026-09-03 put 63% of the query in *building* those
//! 454,404 rows, 22% in the two id comparisons and 14% in the closing hop.
//! Two levers were already implemented and neither moved it, and the frozen
//! CSR -- the sorted layout a closing-hop *lookup* would need -- came in at
//! 1.00x on BI-17 in a same-host A/B
//! (samyama-graph-competitor-benchmarks#131).
//!
//! So the remaining lever is not a cheaper test at the end. It is not
//! building the rows: for each ordered edge (a,b), intersect N(a) with N(b)
//! and count the members above b. This example measures the ceiling of that
//! idea *outside* the executor -- same store, same data, same answer -- so
//! that #1082 is costed before an operator is written for it.
//!
//! It is deliberately not an operator. A hand-rolled loop over the store is
//! the cheapest way to find out whether the idea is worth 2x or 20x, and a
//! number that comes back at 1.2x would have saved building one.
//!
//!   cargo run --release --example bi17_intersect -- --data-dir <sf1-dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::{EdgeType, GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

const TRIALS: usize = 3;

/// The engine's answer, and what it costs. The query the BI bench runs,
/// verbatim from `benches/ldbc_bi_benchmark.rs`.
fn engine_arm(graph: &GraphStore) -> (f64, i64) {
    let q = parse_query(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
         WHERE a.id < b.id AND b.id < c.id
         RETURN count(a) AS triangleCount",
    )
    .expect("parse");
    let mut times = Vec::new();
    let mut count = -1i64;
    for _ in 0..TRIALS {
        let t = Instant::now();
        let out = QueryExecutor::new(graph).execute(&q).expect("execute");
        times.push(t.elapsed().as_secs_f64());
        count = out
            .records
            .first()
            .and_then(|r| r.values().next())
            .and_then(|v| match v {
                Value::Property(PropertyValue::Integer(i)) => Some(*i),
                _ => None,
            })
            .unwrap_or(-1);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[TRIALS / 2], count)
}

/// Sorted undirected KNOWS adjacency, keyed by the `id` **rank** rather than
/// by `NodeId`.
///
/// Ranked because the query orders by `a.id`, and comparing ranks is the same
/// order as comparing ids while being a `u32` compare instead of a property
/// read. An operator would not have this luxury for free; the point here is to
/// measure the traversal, so the comparison is made as cheap as it can be and
/// the result read as a ceiling, not a promise.
fn build_ranked(graph: &GraphStore) -> (Vec<Vec<u32>>, usize) {
    let persons = graph
        .nodes_with_label(&Label::new("Person"))
        .expect("Person label");
    let mut by_id: Vec<(i64, NodeId)> = persons
        .iter()
        .map(|&n| {
            // The **columnar** store, not `Node::get_property`. The LDBC
            // loader writes properties through `set_node_property`, which is
            // the path `CREATE` and snapshot import take; row storage is a
            // third path no user takes (see benches/ldbc_common/mod.rs).
            let id = match graph.node_columns.get_property(n.as_u64() as usize, "id") {
                PropertyValue::Integer(i) => i,
                other => panic!("Person {n:?} has id = {other:?}, expected an Integer"),
            };
            (id, n)
        })
        .collect();
    by_id.sort_unstable();
    let rank: HashMap<NodeId, u32> = by_id
        .iter()
        .enumerate()
        .map(|(r, &(_, n))| (n, r as u32))
        .collect();

    let knows = EdgeType::new("KNOWS");
    let mut adj: Vec<Vec<u32>> = vec![Vec::new(); by_id.len()];
    let mut edges = 0usize;
    for (r, &(_, n)) in by_id.iter().enumerate() {
        // Undirected, as the pattern is: `-[:KNOWS]-` matches either way.
        graph.for_each_outgoing_neighbor_of_type(n, &knows, |m| {
            if let Some(&rm) = rank.get(&m) {
                adj[r].push(rm);
            }
        });
        graph.for_each_incoming_neighbor_of_type(n, &knows, |m| {
            if let Some(&rm) = rank.get(&m) {
                adj[r].push(rm);
            }
        });
    }
    for list in adj.iter_mut() {
        list.sort_unstable();
        // LDBC KNOWS is stored both ways for some extracts and one way for
        // others; either produces duplicates once both directions are read.
        // A duplicate would count a triangle twice, which is the kind of
        // wrong answer that looks like a speed-up.
        list.dedup();
        edges += list.len();
    }
    (adj, edges / 2)
}

/// For each ordered edge (a,b) with a < b, count the c in N(a) ∩ N(b) with
/// c > b. Sorted-merge intersection, no rows built.
fn intersect_arm(adj: &[Vec<u32>]) -> (f64, i64) {
    let mut times = Vec::new();
    let mut count = 0i64;
    for _ in 0..TRIALS {
        let t = Instant::now();
        let mut n = 0i64;
        for a in 0..adj.len() as u32 {
            let na = &adj[a as usize];
            for &b in na.iter() {
                if b <= a {
                    continue; // a < b, the query's first ordering filter
                }
                let nb = &adj[b as usize];
                // Both lists are sorted, so the intersection is one pass. The
                // `c > b` filter is a starting offset, not a test per element:
                // everything at or below b is skipped by seeking once.
                let mut i = na.partition_point(|&x| x <= b);
                let mut j = nb.partition_point(|&x| x <= b);
                while i < na.len() && j < nb.len() {
                    match na[i].cmp(&nb[j]) {
                        std::cmp::Ordering::Less => i += 1,
                        std::cmp::Ordering::Greater => j += 1,
                        std::cmp::Ordering::Equal => {
                            n += 1;
                            i += 1;
                            j += 1;
                        }
                    }
                }
            }
        }
        times.push(t.elapsed().as_secs_f64());
        count = n;
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[TRIALS / 2], count)
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
    eprintln!("loaded\n");

    let t = Instant::now();
    let (adj, undirected_edges) = build_ranked(&graph);
    let build = t.elapsed().as_secs_f64();
    println!(
        "{} persons, {} undirected KNOWS edges, ranked adjacency built in {:.3}s",
        adj.len(),
        undirected_edges,
        build
    );

    let (ie, ce) = engine_arm(&graph);
    println!("engine  (expand and test)   {:8.3}s   {} triangles", ie, ce);
    let (ii, ci) = intersect_arm(&adj);
    println!("loop    (sorted intersect)  {:8.3}s   {} triangles", ii, ci);

    // The answers must agree, or the speed-up is a different question being
    // answered faster. This is the check that makes the ratio mean anything.
    if ce != ci {
        println!(
            "\nDISAGREE: engine {} vs loop {}. The ratio below is meaningless \
             until this is resolved.",
            ce, ci
        );
    } else {
        println!("\nagree on {} triangles", ce);
    }
    println!(
        "speed-up {:.1}x on the traversal, {:.1}x including the {:.3}s build",
        ie / ii,
        ie / (ii + build),
        build
    );
    Ok(())
}
