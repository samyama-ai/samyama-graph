//! Does a wider record make an expand's rows more expensive? (#1078)
//!
//! Stage isolation (#1081) put the second expand at ~2.5x the per-row cost of
//! the first, on the same edge type over the same store. The one hypothesis
//! that survived was record **width**: `Record::clone_with_capacity` copies
//! every accumulated binding plus `used_edges` for each emitted row, so a
//! deeper pattern pays for its own history on every row it produces.
//!
//! That is testable without touching the traversal. A `WITH` that carries
//! extra scalar columns widens the record and changes nothing else -- same
//! scan, same expands, same row count. If width is the cost, the wide arm is
//! slower per row by a margin that grows with the number of carried columns.
//! If the arms tie, width is not the answer and the 2.5x is still unexplained.
//!
//!   cargo run --release --example bi17_width -- --data-dir <sf1-dir>

#[path = "../benches/ldbc_bi_common/mod.rs"]
mod ldbc_bi_common;

use samyama::graph::GraphStore;
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

/// Median of an odd number of trials, after a warm-up.
///
/// A single trial published a closing-hop cost of -257 ns once (#1078); the
/// median of seven is what made three successive runs of that measurement
/// agree. Same discipline here.
const TRIALS: usize = 5;

fn carried(n: usize) -> String {
    // `a.id` n times over, so every extra column is the same width and the
    // same read -- the arms differ in how many bindings the record holds and
    // in nothing else.
    (0..n)
        .map(|i| format!(", a.id AS x{i}"))
        .collect::<String>()
}

/// Two hops, with `n` extra columns carried through both of them.
fn two_hop(n: usize, limit: u64) -> String {
    format!(
        "MATCH (a:Person) WHERE a.id < {limit}
         WITH a{}
         MATCH (a)-[:KNOWS]-(b:Person)-[:KNOWS]-(c:Person)
         RETURN count(c) AS n",
        carried(n)
    )
}

/// One hop, same carrying. The first expand is the control: if width costs
/// something it should cost it here too, in proportion to the rows emitted.
fn one_hop(n: usize, limit: u64) -> String {
    format!(
        "MATCH (a:Person) WHERE a.id < {limit}
         WITH a{}
         MATCH (a)-[:KNOWS]-(b:Person)
         RETURN count(b) AS n",
        carried(n)
    )
}

fn run(graph: &GraphStore, q: &str) -> (f64, i64) {
    let parsed = parse_query(q).expect("parse");
    // Warm-up, discarded: the type index builds after 512 rows and the first
    // trial would otherwise price building it as if it were traversal.
    let _ = QueryExecutor::new(graph).execute(&parsed);

    let mut times = Vec::new();
    let mut rows = 0i64;
    for _ in 0..TRIALS {
        let t = Instant::now();
        let out = QueryExecutor::new(graph).execute(&parsed).expect("execute");
        times.push(t.elapsed().as_secs_f64());
        rows = out
            .records
            .first()
            .and_then(|r| r.values().next())
            .and_then(|v| match v {
                samyama::query::executor::Value::Property(
                    samyama::graph::PropertyValue::Integer(i),
                ) => Some(*i),
                _ => None,
            })
            .unwrap_or(-1);
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (times[TRIALS / 2], rows)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .expect("--data-dir <path> is required");
    let pct: Vec<u32> = args
        .iter()
        .position(|a| a == "--percentiles")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.split(',').filter_map(|p| p.parse().ok()).collect())
        .unwrap_or_else(|| vec![25, 50, 100]);

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    ldbc_bi_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded\n");

    // Bounds taken from the id distribution rather than written as literals.
    // LDBC person ids are sparse, so `a.id < 20000` is not "20,000 people" --
    // it kept 25,176 of 361,246 one-hop rows, an unknown 7% of the graph. A
    // percentile is the same fraction on any scale factor.
    let mut ids: Vec<i64> = {
        let q = parse_query("MATCH (a:Person) RETURN a.id AS id").expect("parse id scan");
        QueryExecutor::new(&graph)
            .execute(&q)
            .expect("id scan")
            .records
            .iter()
            .filter_map(|r| match r.values().next() {
                Some(samyama::query::executor::Value::Property(
                    samyama::graph::PropertyValue::Integer(i),
                )) => Some(*i),
                _ => None,
            })
            .collect()
    };
    ids.sort_unstable();
    println!("{} persons, id range {}..={}\n", ids.len(), ids[0], ids[ids.len() - 1]);

    for p in &pct {
        let idx = ((ids.len() as u64 * *p as u64 / 100) as usize).min(ids.len() - 1);
        let limit = (ids[idx] as u64).saturating_add(1);
        for (label, build) in [
            ("one hop ", one_hop as fn(usize, u64) -> String),
            ("two hops", two_hop as fn(usize, u64) -> String),
        ] {
            println!("--- {label}  p{p} of persons (a.id < {limit}) ---");
            println!("{:>8}  {:>12}  {:>10}  {:>12}", "carried", "rows", "median s", "ns/row");
            let mut baseline: Option<f64> = None;
            for n in [0usize, 4, 8] {
                let (secs, rows) = run(&graph, &build(n, limit));
                let per_row = if rows > 0 { secs * 1e9 / rows as f64 } else { f64::NAN };
                let delta = match baseline {
                    None => {
                        baseline = Some(per_row);
                        String::new()
                    }
                    Some(b) => format!("   {:+.1} ns/row vs 0 carried  ({:+.1}/column)",
                                       per_row - b, (per_row - b) / n as f64),
                };
                println!("{n:>8}  {rows:>12}  {secs:>10.3}  {per_row:>12.1}{delta}");
            }
            println!();
        }
    }
    Ok(())
}
