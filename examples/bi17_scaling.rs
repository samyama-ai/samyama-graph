//! How does BI-17 scale, now that the cyclic prune has made it 7.88x faster?
//!
//! The gate is at SF10, where BI-17 times out at 120 s; this host holds SF1,
//! where it now answers in 0.87 s. "7.88x would put it near 16 s" is a
//! projection resting on an assumption nobody has checked: that the improved
//! query scales the way the old one did.
//!
//! It is checkable without SF10. Restricting the pattern to persons below the
//! p-th percentile of `id` gives a family of subgraphs of one dataset, and the
//! exponent of runtime against triangles found is a property of the algorithm
//! rather than of the scale factor. If the exponent is ~1, a graph with k times
//! the triangles costs ~k times as much and the SF10 arithmetic is sound. If it
//! is 2, the projection is worthless.
//!
//! This measures the *shape* of the curve, not SF10. It cannot turn H1
//! condition 2 green -- only an SF10 run can do that -- but it can say whether
//! the projection is worth making at all.
//!
//!   cargo run --release --example bi17_scaling -- --data-dir <sf1-dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

const TRIALS: usize = 3;

/// BI-17, restricted to persons whose `id` is below `limit`.
///
/// The bound goes on all three variables, so the subgraph is closed: a
/// triangle is counted only when every member is inside it. Bounding `a`
/// alone would leave the two-hop walk ranging over the whole graph and measure
/// something else.
fn query(limit: i64) -> String {
    format!(
        "MATCH (a:Person)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)-[:KNOWS]-(a)
         WHERE a.id < b.id AND b.id < c.id
           AND a.id < {limit} AND b.id < {limit} AND c.id < {limit}
         RETURN count(a) AS triangleCount"
    )
}

fn run(graph: &GraphStore, q: &str) -> (f64, i64) {
    let parsed = parse_query(q).expect("parse");
    // Warm-up discarded: the type index builds after 512 rows, and the first
    // trial would otherwise price building it as if it were traversal.
    let _ = QueryExecutor::new(graph).execute(&parsed);
    let mut times = Vec::new();
    let mut n = -1i64;
    for _ in 0..TRIALS {
        let t = Instant::now();
        let out = QueryExecutor::new(graph).execute(&parsed).expect("execute");
        times.push(t.elapsed().as_secs_f64());
        n = out
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
    (times[TRIALS / 2], n)
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

    // Bounds from the id distribution, not literals. LDBC person ids are
    // sparse -- the maximum at SF1 is 10,995,116,286,015 -- so `a.id < 20000`
    // is not "20,000 people"; it once kept an unknown 7% of the graph and a
    // conclusion was drawn from it.
    let mut ids: Vec<i64> = {
        let q = parse_query("MATCH (a:Person) RETURN a.id AS id").expect("parse");
        QueryExecutor::new(&graph)
            .execute(&q)
            .expect("id scan")
            .records
            .iter()
            .filter_map(|r| match r.values().next() {
                Some(Value::Property(PropertyValue::Integer(i))) => Some(*i),
                _ => None,
            })
            .collect()
    };
    ids.sort_unstable();
    println!("{} persons\n", ids.len());
    println!("{:>5} {:>9} {:>12} {:>10} {:>10}", "pct", "persons", "triangles", "median", "ns/tri");

    let mut points: Vec<(f64, f64)> = Vec::new();
    for p in [25u32, 50, 75, 100] {
        let idx = ((ids.len() as u64 * p as u64 / 100) as usize).min(ids.len() - 1);
        let limit = (ids[idx] as u64 as i64).saturating_add(1);
        let (t, n) = run(&graph, &query(limit));
        let persons = idx + 1;
        println!(
            "{p:>4}% {persons:>9} {n:>12} {:>9.3}s {:>9.0}",
            t,
            if n > 0 { t / n as f64 * 1e9 } else { 0.0 }
        );
        if n > 0 {
            points.push((n as f64, t));
        }
    }

    // The exponent of a power-law fit through (triangles, seconds), by least
    // squares in log space. One number, and the one the projection rests on.
    if points.len() >= 2 {
        let n = points.len() as f64;
        let (sx, sy): (f64, f64) = points
            .iter()
            .fold((0.0, 0.0), |(a, b), (x, y)| (a + x.ln(), b + y.ln()));
        let (mx, my) = (sx / n, sy / n);
        let num: f64 = points.iter().map(|(x, y)| (x.ln() - mx) * (y.ln() - my)).sum();
        let den: f64 = points.iter().map(|(x, _)| (x.ln() - mx).powi(2)).sum();
        let k = num / den;
        println!("\nruntime ~ triangles^{k:.2}");
        println!(
            "{}",
            if k < 1.25 {
                "Near-linear in the answer size, so scaling by the triangle count is sound."
            } else {
                "Superlinear: a projection by triangle count understates the cost at scale."
            }
        );
    }
    Ok(())
}
