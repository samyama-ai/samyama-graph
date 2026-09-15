//! Where IC5's time goes, by rewriting it (#612). Throwaway, branch `profile-ic`.
//!
//! The SF1 profile put IC5 at Aggregate 45%, Expand 33%, Sort 11% and Project 9%.
//! The aggregate groups 1,054,545 membership rows into 78,295 forums by
//! `(forum.id, forum.title)`, which reads two properties and hashes a string key
//! per row. The hypothesis is that grouping by the forum *node*, and reading the
//! properties only for the rows that survive, removes most of that. This times
//! the hypothesis instead of assuming it.
//!
//!   cargo run --release --example ic5_variants -- --data-dir <sf1-dir>

#[path = "../benches/ldbc_common/mod.rs"]
mod ldbc_common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

static ALLOCS: AtomicU64 = AtomicU64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        System.dealloc(p, l)
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.realloc(p, l, n)
    }
}

#[global_allocator]
static A: Counting = Counting;

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

const RUNS: usize = 11;

/// The IC5 prefix: friends and friends-of-friends of the p50 anchor, distinct.
fn prefix(person: &str) -> String {
    format!(
        "MATCH (p:Person {{id: {person}}})-[:KNOWS*1..2]-(friend:Person)
         WHERE friend.id <> {person}
         WITH DISTINCT friend "
    )
}

/// Times one query: one warm-up, then RUNS timed runs. Returns its rows,
/// canonicalised and sorted, for comparing variants.
fn time(graph: &GraphStore, label: &str, cypher: &str) -> Option<Vec<String>> {
    let q = match parse_query(cypher) {
        Ok(q) => q,
        Err(e) => {
            println!("{label:<44} parse failed: {e}");
            return None;
        }
    };
    let first = match QueryExecutor::new(graph).execute(&q) {
        Ok(r) => r,
        Err(e) => {
            println!("{label:<44} failed: {e}");
            return None;
        }
    };
    let mut ts = Vec::with_capacity(RUNS);
    for _ in 0..RUNS {
        let t = Instant::now();
        let _ = QueryExecutor::new(graph).execute(&q);
        ts.push(t.elapsed().as_secs_f64() * 1e3);
    }
    ts.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let a0 = ALLOCS.load(Ordering::Relaxed);
    let _ = QueryExecutor::new(graph).execute(&q);
    let allocs = ALLOCS.load(Ordering::Relaxed) - a0;
    println!(
        "{label:<44} rows {:>6}  min {:>7.1}  median {:>7.1}  max {:>7.1} ms  allocs {:>9}",
        first.records.len(),
        ts[0],
        ts[RUNS / 2],
        ts[RUNS - 1],
        allocs
    );
    let mut rows: Vec<String> = first
        .records
        .iter()
        .map(|r| {
            first
                .columns
                .iter()
                .map(|c| format!("{:?}", r.get(c)))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    rows.sort();
    Some(rows)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let data_dir = args
        .iter()
        .position(|a| a == "--data-dir")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .expect("--data-dir <path> is required");
    // The p50 anchor `--derive-params 50` chose on SF1 (degree 21, 4,375 persons within 2 hops).
    let person = args
        .iter()
        .position(|a| a == "--person")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "13194139533588".to_string());

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    ldbc_common::load_dataset(&mut graph, &data_dir)?;
    {
        let mut m = MutQueryExecutor::new(&mut graph, "default".into());
        for (l, p) in [
            ("Person", "id"), ("Person", "firstName"), ("Post", "id"), ("Comment", "id"),
            ("Forum", "id"), ("Place", "id"), ("Place", "name"), ("Organisation", "id"),
            ("Organisation", "name"), ("Tag", "id"), ("Tag", "name"), ("TagClass", "id"),
            ("TagClass", "name"),
        ] {
            let _ = m.execute(&parse_query(&format!("CREATE INDEX ON :{l}({p})"))?);
        }
    }
    eprintln!("loaded; anchor person {person}");
    let pre = prefix(&person);
    let exp = "MATCH (friend)<-[:HAS_MEMBER]-(forum:Forum) ";

    println!("--- floors ---");
    time(&graph, "F1 prefix only (count friends)", &format!("{pre}RETURN count(friend) AS n"));
    time(&graph, "F2 prefix + expand (count rows)", &format!("{pre}{exp}RETURN count(*) AS n"));

    println!("--- IC5 and rewrites (ORDER BY memberCount DESC LIMIT 20) ---");
    let v0 = time(&graph, "V0 original: group by (forum.id, forum.title)",
        &format!("{pre}{exp}RETURN forum.id, forum.title, count(friend) AS memberCount ORDER BY memberCount DESC LIMIT 20"));
    let v1 = time(&graph, "V1 group by node, then properties",
        &format!("{pre}{exp}WITH forum, count(friend) AS memberCount RETURN forum.id, forum.title, memberCount ORDER BY memberCount DESC LIMIT 20"));
    let v2 = time(&graph, "V2 group by node, top 20, then properties",
        &format!("{pre}{exp}WITH forum, count(friend) AS memberCount ORDER BY memberCount DESC LIMIT 20 RETURN forum.id, forum.title, memberCount"));
    time(&graph, "V3 group by forum.id only",
        &format!("{pre}{exp}RETURN forum.id, count(friend) AS memberCount ORDER BY memberCount DESC LIMIT 20"));

    // Top-20 sets can differ on ties at rank 20, so compare the count multisets.
    let counts = |v: &Option<Vec<String>>| -> Vec<String> {
        let mut c: Vec<String> = v.iter().flatten().map(|r| r.rsplit('|').next().unwrap_or("").to_string()).collect();
        c.sort();
        c
    };
    println!("top-20 memberCount multiset: V1 == V0 {}, V2 == V0 {}", counts(&v1) == counts(&v0), counts(&v2) == counts(&v0));

    println!("--- equivalence on the full result (no ORDER BY / LIMIT) ---");
    let f0 = time(&graph, "full V0", &format!("{pre}{exp}RETURN forum.id, forum.title, count(friend) AS memberCount"));
    let f1 = time(&graph, "full V1", &format!("{pre}{exp}WITH forum, count(friend) AS memberCount RETURN forum.id, forum.title, memberCount"));
    match (&f0, &f1) {
        (Some(a), Some(b)) => println!("full results equal: {} ({} vs {} rows)", a == b, a.len(), b.len()),
        _ => println!("full results: a variant failed"),
    }
    Ok(())
}
