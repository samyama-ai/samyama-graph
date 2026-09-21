//! Write the graph as GraphML, and say what the format could not carry (INT-05).
//!
//! GraphML is what Gephi, yEd and Cytoscape open. It is an **exit** format
//! before it is an interchange format, which is why this prints a loss report
//! rather than a success message: the measure of lock-in is what the exit
//! costs, and a user is entitled to know the cost before they need it, not
//! after they have decommissioned the source.
//!
//! ```bash
//! cargo run --release --example graphml_export -- --out graph.graphml
//! cargo run --release --example graphml_export -- --snapshot data.sgsnap --out graph.graphml
//! cargo run --release --example graphml_export -- --demo --out /dev/null --json report.json
//! ```
//!
//! With no `--snapshot` it exports whatever `--demo` builds, which exists so
//! the tool can be run — and measured — without a database to hand.

use samyama::export::graphml::to_graphml;
use samyama::graph::{GraphStore, PropertyValue};

fn demo_graph() -> GraphStore {
    // Deliberately exercises all three ways GraphML is narrower than a
    // property graph: a type conflict, a list, and a multi-label node. A demo
    // of scalars only would report no loss and teach the user nothing about
    // the format they are about to rely on.
    let mut g = GraphStore::new();
    let a = g.create_node("Person");
    let b = g.create_node("Person");
    let c = g.create_node("Company");
    g.add_label_to_node("default", b, "Employee").ok();
    g.set_node_property("default", a, "name", "Alice").ok();
    g.set_node_property("default", a, "age", 34i64).ok();
    g.set_node_property(
        "default",
        a,
        "tags",
        PropertyValue::Array(vec![
            PropertyValue::String("founder".into()),
            PropertyValue::String("board".into()),
        ]),
    )
    .ok();
    g.set_node_property("default", b, "name", "Bob").ok();
    g.set_node_property("default", b, "age", "unknown").ok();
    g.set_node_property("default", c, "name", "Acme").ok();
    if let Ok(e) = g.create_edge(a, b, "KNOWS") {
        g.set_edge_property(e, "since", 2019i64).ok();
    }
    g.create_edge(b, c, "WORKS_AT").ok();
    g
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| -> Option<String> {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };

    let store = match flag("--snapshot") {
        Some(path) => {
            let file = match std::fs::File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("cannot open snapshot {path}: {e}");
                    std::process::exit(2);
                }
            };
            let mut s = GraphStore::new();
            if let Err(e) = samyama::snapshot::import_tenant(&mut s, file) {
                eprintln!("cannot read snapshot {path}: {e}");
                std::process::exit(2);
            }
            s
        }
        None => demo_graph(),
    };

    let (xml, report) = to_graphml(&store);

    let out = flag("--out").unwrap_or_else(|| "graph.graphml".to_string());
    if let Err(e) = std::fs::write(&out, &xml) {
        eprintln!("cannot write {out}: {e}");
        std::process::exit(2);
    }

    println!("Wrote {out}");
    println!("  nodes            {}", report.nodes_written);
    println!("  relationships    {}", report.edges_written);
    println!("  key declarations {}", report.keys_declared);

    if report.typed_losslessly() {
        println!("\n  Every value kept a GraphML type that means what it meant here.");
    } else {
        println!("\n  What GraphML could not carry:");
        if !report.attributes_widened_to_string.is_empty() {
            println!(
                "  - {} attribute(s) had values of more than one type, so the declaration \
                 widened to `string`: {}",
                report.attributes_widened_to_string.len(),
                report.attributes_widened_to_string.join(", ")
            );
            println!(
                "    The digits are still there; they have stopped being numbers to a reader."
            );
        }
        if report.values_written_as_json > 0 {
            println!(
                "  - {} list/map/vector value(s) written as JSON inside a string attribute.",
                report.values_written_as_json
            );
            println!("    GraphML has no type for them. The content survives; the structure");
            println!("    is invisible to a reader that does not parse the JSON.");
        }
        if report.temporal_values_written_as_text > 0 {
            println!(
                "  - {} temporal value(s) written as ISO-8601 text. Lossless as text, and \
                 not as a type.",
                report.temporal_values_written_as_text
            );
        }
        if !report.labels_containing_a_space.is_empty() {
            println!(
                "  - {} label(s) contain a space, which the space-separated `labels` \
                 convention cannot represent unambiguously: {}",
                report.labels_containing_a_space.len(),
                report.labels_containing_a_space.join(", ")
            );
        }
    }

    if let Some(json) = flag("--json") {
        let payload = serde_json::json!({
            "out": out,
            "report": report,
            "typed_losslessly": report.typed_losslessly(),
            "bytes": xml.len(),
        });
        if let Err(e) = std::fs::write(&json, serde_json::to_string_pretty(&payload).unwrap()) {
            eprintln!("cannot write {json}: {e}");
            std::process::exit(2);
        }
        println!("\n  Report written to {json}");
    }
}
