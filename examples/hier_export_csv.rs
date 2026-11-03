//! Export the HIER dataset to CSV for loading into another engine.
//!
//! The cross-engine comparison is only meaningful if both engines hold the *same* graph.
//! Rather than reimplement the generator in Python or Cypher and hope the two stay in
//! step, this dumps the graph from `hier_common::build()` — the identical code path that
//! builds the Samyama store the benchmark runs against. Any drift is then impossible by
//! construction rather than by discipline.
//!
//! Emits `neo4j-admin import` style CSVs:
//!
//! ```text
//! nodes.csv  id:ID,code,units:long,level:long,y:long,co:long,:LABEL
//! rels.csv   :START_ID,:END_ID,:TYPE
//! ```
//!
//! The `emb` vector property is deliberately omitted — HIER class H9 (hierarchy-filtered
//! vector search) is blocked on this engine anyway (#348), so carrying embeddings across
//! would only inflate the load without any query using them.
//!
//! ```bash
//! cargo run --release --example hier_export_csv -- --out /tmp/hier-csv
//! ```

#[path = "../benches/hier_common/mod.rs"]
mod hier_common;

use std::io::Write;

use samyama::graph::PropertyValue;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = args
        .iter()
        .position(|a| a == "--out")
        .and_then(|i| args.get(i + 1))
        .cloned()
        .unwrap_or_else(|| "hier-csv".to_string());
    std::fs::create_dir_all(&out).expect("create out dir");

    let scale = hier_common::HierScale::default();
    let data = hier_common::build(&scale);
    let store = &data.store;

    let int_prop = |id: samyama::graph::NodeId, key: &str| -> String {
        match store.node_columns.get_property(id.as_u64() as usize, key) {
            PropertyValue::Integer(i) => i.to_string(),
            _ => String::new(),
        }
    };

    let mut nodes = std::io::BufWriter::new(
        std::fs::File::create(format!("{out}/nodes.csv")).expect("nodes.csv"),
    );
    writeln!(nodes, "id:ID,code,units:long,level:long,y:long,co:long,:LABEL").unwrap();
    let mut node_count = 0usize;
    for node in store.all_nodes() {
        let id = node.id;
        let code = match store.node_columns.get_property(id.as_u64() as usize, "code") {
            PropertyValue::String(s) => s,
            _ => String::new(),
        };
        // A node carries at most one label in this dataset; join defensively anyway.
        let label = node
            .labels
            .iter()
            .map(|l| l.as_str().to_string())
            .collect::<Vec<_>>()
            .join(";");
        writeln!(
            nodes,
            "{},{},{},{},{},{},{}",
            id.as_u64(),
            code,
            int_prop(id, "units"),
            int_prop(id, "level"),
            int_prop(id, "y"),
            int_prop(id, "co"),
            label
        )
        .unwrap();
        node_count += 1;
    }
    nodes.flush().unwrap();

    let mut rels = std::io::BufWriter::new(
        std::fs::File::create(format!("{out}/rels.csv")).expect("rels.csv"),
    );
    writeln!(rels, ":START_ID,:END_ID,:TYPE").unwrap();
    let mut rel_count = 0usize;
    for node in store.all_nodes() {
        for (_eid, src, tgt, et) in store.get_outgoing_edge_targets_owned(node.id) {
            writeln!(rels, "{},{},{}", src.as_u64(), tgt.as_u64(), et.as_str()).unwrap();
            rel_count += 1;
        }
    }
    rels.flush().unwrap();

    println!("wrote {out}/nodes.csv  ({node_count} nodes)");
    println!("wrote {out}/rels.csv   ({rel_count} relationships)");
    assert_eq!(node_count, data.nodes, "exported every node");
    assert_eq!(rel_count, data.edges, "exported every relationship");
    println!("counts match the generator: {} nodes, {} edges", data.nodes, data.edges);
}
