//! Instruction counts of five hot inner loops, under Valgrind (#2207, PERF-18).
//!
//! Wall-clock benchmarks cannot gate a pull request on a shared runner: the
//! same code moves 20-30% between runs. Retired instructions do not. This
//! counts them with gungraun (Callgrind), and `scripts/check_instruction_counts.py`
//! fails CI when one grows more than 3% over `benches/instruction_counts.json`.
//!
//!     cargo bench --bench instruction_counts -- --save-summary=json   # Linux + Valgrind
//!
//! Every input is built in a `setup` function, which Callgrind does not count,
//! and handed back in the return value, so that dropping it -- freeing a
//! 2,000-node store costs more than probing a hierarchy 1,000 times -- happens
//! after the count stops. Every input is deterministic: a random graph or vector set would move
//! the count between runs, which is exactly what a gate must not do.
//!
//! Not measured here: the HNSW search. hnsw_rs draws each node's layer from an
//! unseeded RNG, so the graph -- and the instructions a search takes -- differ
//! on every build of the index. The distance kernels it spends its time in are
//! measured instead.

#[cfg(target_os = "linux")]
use std::hint::black_box;

#[cfg(target_os = "linux")]
use gungraun::prelude::*;
#[cfg(target_os = "linux")]
use samyama::graph::{GraphStore, NodeId};
#[cfg(target_os = "linux")]
use samyama::index::hierarchy::{OehIndex, Poset};
#[cfg(target_os = "linux")]
use samyama::query::executor::QueryExecutor;
#[cfg(target_os = "linux")]
use samyama::query::ast::Query;
#[cfg(target_os = "linux")]
use samyama::query::parser::parse_query;
#[cfg(target_os = "linux")]
use samyama::vector::distance;

#[cfg(target_os = "linux")]
const QUERY: &str = "MATCH (a:Person {name: $name})-[:KNOWS]->(b:Person) \
                     WHERE b.age > 30 AND b.city IN ['Pune', 'Hyderabad'] \
                     RETURN b.name, count(*) AS c ORDER BY c DESC, b.name LIMIT 10";

#[cfg(target_os = "linux")]
/// 2,000 `:P` nodes, each with edges to the next three: every node has the
/// same out-degree, so the expansion's work is fixed.
fn graph() -> GraphStore {
    let mut g = GraphStore::new();
    let ids: Vec<NodeId> = (0..2000)
        .map(|i| {
            let n = g.create_node("P");
            g.set_node_property("default", n, "i", i as i64).unwrap();
            g.set_node_property("default", n, "name", format!("p{i}")).unwrap();
            n
        })
        .collect();
    for (i, &a) in ids.iter().enumerate() {
        for d in 1..=3 {
            g.create_edge(a, ids[(i + d) % ids.len()], "K").unwrap();
        }
    }
    g
}

#[cfg(target_os = "linux")]
fn expand_setup(cypher: &str) -> (GraphStore, Query) {
    (graph(), parse_query(cypher).unwrap())
}

#[cfg(target_os = "linux")]
/// A 9,331-node tree, depth 5, fan-out 6, as in `benches/hierarchy_benchmark.rs`.
fn oeh_setup() -> (OehIndex, u32) {
    let mut edges = Vec::new();
    let (mut frontier, mut next) = (vec![0u64], 1u64);
    for _ in 0..5 {
        let mut grown = Vec::new();
        for &p in &frontier {
            for _ in 0..6 {
                edges.push((NodeId(next), NodeId(p)));
                grown.push(next);
                next += 1;
            }
        }
        frontier = grown;
    }
    let poset = Poset::from_edges(edges, std::iter::empty()).unwrap();
    let n = poset.n() as u32;
    (OehIndex::build(poset).unwrap(), n)
}

/// Pairs of vectors whose distances are measured.
#[cfg(target_os = "linux")]
type Pairs = Vec<(Vec<f32>, Vec<f32>)>;

#[cfg(target_os = "linux")]
/// 256 pairs of 128-dimensional vectors from a fixed linear congruential sequence.
fn vectors_setup() -> Pairs {
    let mut s = 0x2207_u64;
    let mut next = move || {
        s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        ((s >> 40) as f32) / ((1u64 << 24) as f32) - 0.5
    };
    (0..256).map(|_| ((0..128).map(|_| next()).collect(), (0..128).map(|_| next()).collect())).collect()
}

#[cfg(target_os = "linux")]
fn snapshot_setup() -> (GraphStore, Vec<u8>) {
    let g = graph();
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(&g, &mut buf).unwrap();
    (g, buf)
}

#[cfg(target_os = "linux")]
#[library_benchmark]
#[bench::match_where_return(QUERY)]
fn parse(cypher: &str) -> Query {
    black_box(parse_query(black_box(cypher)).unwrap())
}

#[cfg(target_os = "linux")]
#[library_benchmark]
#[bench::one_hop(args = ("MATCH (a:P)-[:K]->(b) RETURN b.name"), setup = expand_setup)]
#[bench::two_hop(args = ("MATCH (a:P)-[:K]->()-[:K]->(c) RETURN c.name"), setup = expand_setup)]
fn expand(input: (GraphStore, Query)) -> (usize, (GraphStore, Query)) {
    let rows = QueryExecutor::new(&input.0).execute(black_box(&input.1)).unwrap().len();
    (black_box(rows), input)
}

#[cfg(target_os = "linux")]
#[library_benchmark]
#[bench::subsumes_1000(setup = oeh_setup)]
fn oeh_probe(input: (OehIndex, u32)) -> (u32, (OehIndex, u32)) {
    let (idx, n) = (&input.0, input.1);
    let mut hits = 0;
    for i in 0..1000u32 {
        if idx.subsumes(black_box((i * 7) % n), black_box((i * 13) % n)) {
            hits += 1;
        }
    }
    (black_box(hits), input)
}

#[cfg(target_os = "linux")]
#[library_benchmark]
#[bench::cosine_and_l2_256x128(setup = vectors_setup)]
fn vector_distance(pairs: Pairs) -> (f32, Pairs) {
    let mut acc = 0.0;
    for (a, b) in &pairs {
        let (dot, na, nb) = distance::cosine_parts(black_box(a), black_box(b));
        acc += dot / (na.sqrt() * nb.sqrt()) + distance::l2_sq(black_box(a), black_box(b));
    }
    (black_box(acc), pairs)
}

#[cfg(target_os = "linux")]
#[library_benchmark]
#[bench::export_2000(setup = snapshot_setup)]
fn snapshot_export(input: (GraphStore, Vec<u8>)) -> (Vec<u8>, (GraphStore, Vec<u8>)) {
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(black_box(&input.0), &mut buf).unwrap();
    (black_box(buf), input)
}

#[cfg(target_os = "linux")]
#[library_benchmark]
#[bench::import_2000(setup = snapshot_setup)]
fn snapshot_import(input: (GraphStore, Vec<u8>)) -> (GraphStore, (GraphStore, Vec<u8>)) {
    let mut g = GraphStore::new();
    samyama::snapshot::import_tenant(&mut g, black_box(&input.1[..])).unwrap();
    (black_box(g), input)
}

#[cfg(target_os = "linux")]
library_benchmark_group!(
    name = hot_loops;
    benchmarks = parse, expand, oeh_probe, vector_distance, snapshot_export, snapshot_import
);

#[cfg(target_os = "linux")]
main!(library_benchmark_groups = hot_loops);

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("instruction_counts runs under Valgrind, which is Linux only (#2207)");
}
