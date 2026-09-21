//! Import a Neo4j graph from `apoc.export.json` output (INT-02).
//!
//! `examples/compatibility_report` tells a Neo4j user whether their *queries*
//! will run here. This brings their *data*. Together they are a migration;
//! either one alone is a report about a migration that cannot happen.
//!
//! ```bash
//! # In Neo4j, first:
//! #   CALL apoc.export.json.all('graph.json', {})
//! cargo run --release --example neo4j_import -- --file graph.json
//! cargo run --release --example neo4j_import -- --file graph.json --json report.json
//! ```
//!
//! Both APOC shapes are read: the default JSON Lines, and the single object
//! written by `{jsonFormat:'JSON'}`.
//!
//! # Read the report, not just the exit code
//!
//! The import reports what it could not carry across rather than stopping at
//! the first problem, because a migration is planned against the whole list:
//!
//! - **dangling edges** — a relationship whose endpoints were not in the
//!   export. A subgraph export produces these by construction. The ids are
//!   named so you can widen the export query.
//! - **values that look temporal or spatial** — JSON has no date and no point,
//!   so APOC writes them as strings and maps. They arrive as strings and maps.
//!   Converting them on sight would turn a version number into a date, so the
//!   conversion is yours to make deliberately, with `SET n.p = datetime(n.p)`.
//! - **null properties dropped** — Neo4j cannot store a null, so a null in the
//!   file came from the exporter. The property is left absent, which is what
//!   the source graph had.
//!
//! The process exits non-zero when nothing at all was imported, and zero for a
//! partial import — a partial import is a real result and the report says how
//! partial. `--require-lossless` makes any loss an error instead, which is the
//! form to use in a script.

use samyama::graph::GraphStore;
use samyama::migrate::neo4j_json::import_str;
use samyama::query::QueryEngine;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let present = |name: &str| args.iter().any(|a| a == name);

    let Some(path) = flag("--file") else {
        eprintln!(
            "usage: neo4j_import --file <apoc.export.json> [--json <report.json>] \
             [--require-lossless]\n\n\
             In Neo4j: CALL apoc.export.json.all('graph.json', {{}})"
        );
        std::process::exit(2);
    };

    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {path}: {e}");
            std::process::exit(2);
        }
    };

    let mut store = GraphStore::new();
    let report = match import_str(&text, &mut store, "default") {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    println!("Imported {path}");
    println!("  nodes            {}", report.nodes_created);
    println!("  relationships    {}", report.edges_created);
    println!("  labels applied   {}", report.labels_applied);
    println!(
        "  properties       {} on nodes, {} on relationships",
        report.node_properties_set, report.edge_properties_set
    );

    if report.dangling_edges > 0 {
        println!(
            "\n  {} relationship(s) had an endpoint that was not exported.",
            report.dangling_edges
        );
        println!(
            "  Missing node ids: {}",
            report.missing_endpoints.join(", ")
        );
        println!("  A subgraph export produces these; widen the export query to include them.");
    }
    if report.null_properties_dropped > 0 {
        println!(
            "\n  {} property value(s) were JSON null and were left absent.",
            report.null_properties_dropped
        );
        println!("  Neo4j cannot store a null, so these came from the exporter, not the graph.");
    }
    if report.values_that_look_temporal > 0 || report.values_that_look_spatial > 0 {
        println!(
            "\n  {} value(s) look temporal and {} look spatial. They are strings and maps \
             here, exactly as the file held them.",
            report.values_that_look_temporal, report.values_that_look_spatial
        );
        println!("  JSON has no date and no point type, so converting on sight would turn a");
        println!("  version number or an identifier into a date. Convert deliberately:");
        println!("    MATCH (n:Label) SET n.when = datetime(n.when)");
    }
    if !report.unknown_record_types.is_empty() {
        println!(
            "\n  Ignored record type(s): {}. These carry no graph data.",
            report.unknown_record_types.join(", ")
        );
    }
    if !report.unparseable_lines.is_empty() {
        println!(
            "\n  {} line(s) were not valid JSON, first at line {}: {}",
            report.unparseable_lines.len(),
            report.unparseable_lines[0].0,
            report.unparseable_lines[0].1
        );
    }

    // A count taken from the store, not from the importer, so the two can
    // disagree if the import is wrong. A report that only quotes itself cannot
    // notice anything that failed to arrive.
    let engine = QueryEngine::new();
    let as_stored = |q: &str| -> i64 {
        engine
            .execute(q, &store)
            .ok()
            .and_then(|b| {
                b.records.first().and_then(|r| match r.values().next() {
                    Some(samyama::query::executor::record::Value::Property(
                        samyama::graph::PropertyValue::Integer(n),
                    )) => Some(*n),
                    _ => None,
                })
            })
            .unwrap_or(-1)
    };
    let nodes_in_store = as_stored("MATCH (n) RETURN count(n)");
    let edges_in_store = as_stored("MATCH ()-[r]->() RETURN count(r)");
    println!("\n  In the store: {nodes_in_store} nodes, {edges_in_store} relationships");
    let agrees = nodes_in_store == report.nodes_created as i64
        && edges_in_store == report.edges_created as i64;
    if !agrees {
        println!("  The store disagrees with the import report. Treat the import as failed.");
    }

    if let Some(out) = flag("--json") {
        let payload = serde_json::json!({
            "file": path,
            "report": report,
            "lossless": report.lossless(),
            "nodes_in_store": nodes_in_store,
            "edges_in_store": edges_in_store,
            "store_agrees_with_report": agrees,
        });
        if let Err(e) = std::fs::write(&out, serde_json::to_string_pretty(&payload).unwrap()) {
            eprintln!("cannot write {out}: {e}");
            std::process::exit(2);
        }
        println!("  Report written to {out}");
    }

    if !agrees {
        std::process::exit(1);
    }
    if report.nodes_created == 0 && report.edges_created == 0 && report.records_read > 0 {
        eprintln!(
            "\nNothing was imported from a file that held {} record(s).",
            report.records_read
        );
        std::process::exit(1);
    }
    if present("--require-lossless") && !report.lossless() {
        eprintln!("\n--require-lossless: the import lost something. See the report above.");
        std::process::exit(1);
    }
}
