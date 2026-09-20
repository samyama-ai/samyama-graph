//! Does a snapshot export say what it dropped? (INT-06)
//!
//! INT-06 asks for full-fidelity export **plus an explicit loss report** — not
//! a standing disclaimer of everything the format could drop, but a list of
//! what happened to *this* graph. `ExportStats::dropped` carries it and the
//! HTTP export surfaces it, and neither of those is visible to a check that
//! reads the source.
//!
//! So this builds a graph that is guaranteed to lose something on the way out,
//! exports it, and reports whether the loss came back named and counted. A
//! graph that loses nothing cannot tell an empty report from a missing one,
//! which is why the fixture declares an index rather than being a bare graph.
//!
//!     cargo run --release --example export_loss_report -- --json out.json
//!
//! Exits non-zero when the export reports no losses for a graph that certainly
//! has one, so the example is usable as a gate on its own.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let json_out = args
        .iter()
        .position(|a| a == "--json")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in [
        "CREATE (:Person {name: 'Alice', age: 30})",
        "CREATE (:Person {name: 'Bob', age: 41})",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
        // A declared index is the reliable loss: the snapshot carries the data,
        // not the structures built over it, so restoring leaves the index to be
        // rebuilt. If this stops being dropped, the report should stop naming
        // it and this example should start failing — which is the point.
        "CREATE INDEX ON :Person(name)",
    ] {
        engine
            .execute_mut(q, &mut store, "default")
            .unwrap_or_else(|e| panic!("{q}: {e}"));
    }

    let mut bytes: Vec<u8> = Vec::new();
    let stats = samyama::snapshot::export_tenant(&store, &mut bytes).expect("export");

    let has_report = !stats.dropped.is_empty();
    let every_row_named = stats
        .dropped
        .iter()
        .all(|d| !d.what.trim().is_empty() && !d.detail.trim().is_empty());
    let ok = has_report && every_row_named;

    println!("Snapshot export loss report (INT-06)");
    println!("  bytes written {}", bytes.len());
    println!("  losses reported {}", stats.dropped.len());
    for d in &stats.dropped {
        println!("    {} x{} — {}", d.what, d.count, d.detail);
    }
    println!(
        "  {}",
        if ok {
            "the export says what it dropped"
        } else if has_report {
            "A LOSS WAS REPORTED WITHOUT SAYING WHAT IT MEANS"
        } else {
            "NO LOSS REPORT: a graph with a declared index exported as if nothing was lost"
        }
    );

    if let Some(path) = json_out {
        let rows: Vec<String> = stats
            .dropped
            .iter()
            .map(|d| {
                format!(
                    "{{\"what\": {}, \"count\": {}, \"detail\": {}}}",
                    json_string(&d.what),
                    d.count,
                    json_string(&d.detail)
                )
            })
            .collect();
        let json = format!(
            "{{\n  \"export_reports_its_losses\": {ok},\n  \"losses_reported\": {},\n  \
             \"every_loss_explained\": {every_row_named},\n  \"bytes_written\": {},\n  \
             \"losses\": [{}]\n}}\n",
            stats.dropped.len(),
            bytes.len(),
            rows.join(", ")
        );
        std::fs::write(&path, json).expect("write json");
        eprintln!("[export] wrote {path}");
    }

    if !ok {
        std::process::exit(1);
    }
}

fn json_string(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ");
    format!("\"{escaped}\"")
}
