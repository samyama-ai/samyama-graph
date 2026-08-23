//! Where LDBC IC11's expand spends its time (#665).
//!
//! IC11's `Expand ((friend)-[:WORK_AT]->(org))` is 68% of the query at SF1 and
//! 74% at SF10, and it emits **2 rows from 3,272 input rows**. Two explanations
//! fit that shape and they call for opposite fixes:
//!
//! * the **walk** — each friend's whole outgoing adjacency (~280 edges at SF1)
//!   is visited to find the ~2 that are `:WORK_AT`; or
//! * the **per-input-row overhead** — whatever `ExpandOperator` does once per
//!   record before it walks anything.
//!
//! `benches/adjacency_walk` already measured the walk at under 3 ns per edge
//! including the type probe, which makes the first explanation too small by
//! itself. The variants below separate them directly: `NO_SUCH_TYPE` keeps
//! every input row and every per-row setup but resolves to an empty type
//! filter, so it walks nothing.
//!
//!   cargo run --release --example ic11_probe -- --data-dir <ldbc-sf1-dir> \
//!       --person-id <id> --org-name <name>

#[path = "../benches/ldbc_common/mod.rs"]
mod ldbc_common;

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1));
    let data_dir = arg("--data-dir").map(PathBuf::from).expect("--data-dir <path> is required");
    let person_id: i64 = arg("--person-id").expect("--person-id <id> is required").parse()?;
    let org_name = arg("--org-name").expect("--org-name <name> is required").clone();
    let runs: usize = arg("--runs").map(|s| s.parse()).transpose()?.unwrap_or(7);

    let mut graph = GraphStore::new();
    eprintln!("loading {} ...", data_dir.display());
    let t = Instant::now();
    ldbc_common::load_dataset(&mut graph, &data_dir)?;
    eprintln!("loaded in {:.1}s", t.elapsed().as_secs_f64());
    for (label, prop) in [("Person", "id"), ("Organisation", "id")] {
        let q = parse_query(&format!("CREATE INDEX ON :{label}({prop})"))?;
        MutQueryExecutor::new(&mut graph, "default".to_string()).execute(&q)?;
    }

    let full = format!(
        "MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(friend:Person)-[wa:WORK_AT]->(org:Organisation)
WHERE friend.id <> {person_id} AND org.name = \"{org_name}\" AND wa.workFrom < 2012
RETURN DISTINCT friend.id, wa.workFrom ORDER BY wa.workFrom LIMIT 10"
    );
    // Same inputs, same per-row setup, nothing to walk: the type resolves to
    // an empty id set, so the adjacency scan is skipped entirely.
    let no_such_type = full.replace("[wa:WORK_AT]", "[wa:NO_SUCH_TYPE]");
    // The input side alone: no expand over WORK_AT at all.
    let inputs_only = format!(
        "MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(friend:Person)
WHERE friend.id <> {person_id} RETURN count(friend) AS c"
    );
    // The expand without the pushed-down equality, to price the pushdown.
    let no_org_filter = format!(
        "MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(friend:Person)-[wa:WORK_AT]->(org:Organisation)
WHERE friend.id <> {person_id} RETURN count(org) AS c"
    );
    // The expand with no edge variable bound, to price materialising `wa`.
    let no_edge_var = format!(
        "MATCH (p:Person {{id: {person_id}}})-[:KNOWS*1..2]-(friend:Person)-[:WORK_AT]->(org:Organisation)
WHERE friend.id <> {person_id} AND org.name = \"{org_name}\" RETURN count(org) AS c"
    );

    for (label, cypher) in [
        ("IC11 as written", &full),
        ("  same, type matches nothing", &no_such_type),
        ("  inputs only (no WORK_AT expand)", &inputs_only),
        ("  expand, no org.name pushdown", &no_org_filter),
        ("  expand, no edge variable bound", &no_edge_var),
    ] {
        let q = parse_query(cypher)?;
        let mut times = Vec::with_capacity(runs);
        let mut rows = 0usize;
        for _ in 0..runs {
            let t = Instant::now();
            match QueryExecutor::new(&graph).execute(&q) {
                Ok(out) => {
                    rows = out.records.len();
                    times.push(t.elapsed().as_secs_f64() * 1000.0);
                }
                Err(e) => {
                    println!("{label:<38} failed: {e}");
                    times.clear();
                    break;
                }
            }
        }
        if times.is_empty() {
            continue;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        println!("{label:<38} median {:>8.3} ms   rows {rows}", times[times.len() / 2]);
    }
    Ok(())
}
