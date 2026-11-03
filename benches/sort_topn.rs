//! What a bounded `ORDER BY … LIMIT k` costs, by input order (#568, #518).
//!
//! Input order is the parameter, because a top-k over data already sorted by
//! the key is a different path from one over scattered data — and because it is
//! what decided a rejection-cutoff optimisation was not worth having. The
//! cutoff (discard a row once the buffer's worst retained key bounds it) was
//! 1-8% *slower* in every column below: records are moved into the buffer
//! rather than cloned, so skipping the move saves little, and the extra
//! comparison costs more than it saves.
//!
//! What did help was locating the sort key's column once instead of once per
//! row (#557), which is what the operator now does:
//!
//! | input order | before | after |
//! |---|---:|---:|
//! | best-first  | 176.8 ms | 154.5 ms |
//! | scattered   | 256.4 ms | 251.3 ms |
//! | worst-first | 243.3 ms | 232.1 ms |
//!
//! LDBC IC9 is the worst case for anything order-dependent here: its messages
//! arrive roughly in creation order and it sorts by creation date descending,
//! so the winners are in the last batch.
//!
//!   cargo bench --bench sort_topn
//!   cargo bench --bench sort_topn -- --rows 1000000 --limit 100

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;
use std::time::Instant;

#[path = "common/bench_setup.rs"]
mod bench_setup;

/// `n` nodes whose sort key arrives in the given order.
fn build(order: &str, n: usize) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..n {
        let id = store.create_node("N");
        let v = match order {
            "best-first" => (n - i) as i64,
            "worst-first" => i as i64,
            _ => {
                let x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                ((x ^ (x >> 31)) % 1_000_000) as i64
            }
        };
        let _ = store.set_node_property("default", id, "v", PropertyValue::Integer(v));
    }
    store
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).and_then(|v| v.parse().ok())
    };
    let n = arg("--rows").unwrap_or(400_000);
    let limit = arg("--limit").unwrap_or(20);
    let runs = arg("--runs").unwrap_or(5);

    println!("{n} rows, top-{limit}, minimum of {runs}\n");
    println!("{:<14} {:>12} {:>14}", "input order", "ms", "ns per row");
    println!("{:-<14} {:->12} {:->14}", "", "", "");

    for order in ["best-first", "scattered", "worst-first"] {
        let store = build(order, n);
        let cypher = format!("MATCH (n:N) RETURN n.v AS v ORDER BY n.v DESC LIMIT {limit}");
        let q = parse_query(&cypher).expect("query should parse");
        let _ = QueryExecutor::new(&store).execute(&q).expect("query should run");
        let mut best = f64::INFINITY;
        for _ in 0..runs {
            let t = Instant::now();
            let out = QueryExecutor::new(&store).execute(&q).expect("query should run");
            assert_eq!(out.records.len(), limit.min(n));
            best = best.min(t.elapsed().as_secs_f64() * 1000.0);
        }
        println!("{order:<14} {best:>12.1} {:>14.0}", best * 1e6 / n as f64);
    }

    println!();
    println!("Worst-first is the adversarial order: every row that belongs in the answer");
    println!("arrives after the buffer has already been trimmed, so nothing can be skipped");
    println!("early. It is also LDBC IC9's shape.");

    bench_setup::report_drift(calibration);
}
