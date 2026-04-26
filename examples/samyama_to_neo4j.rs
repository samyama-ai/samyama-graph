// Samyama → Neo4j CSV converter
//
// Usage:
//   cargo run --release --example samyama_to_neo4j -- <snapshot.sgsnap> <output_dir>
//
// Output: nodes-<Label>.csv and rels-<TYPE>.csv files for `neo4j-admin database import`.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::snapshot::import_tenant;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: {} <snapshot.sgsnap> <output_dir>", args[0]);
        std::process::exit(1);
    }
    let snap_path = &args[1];
    let out_dir = PathBuf::from(&args[2]);
    std::fs::create_dir_all(&out_dir)?;

    eprintln!("[convert] loading snapshot: {}", snap_path);
    let mut store = GraphStore::new();
    let reader = BufReader::new(File::open(snap_path)?);
    let stats = import_tenant(&mut store, reader)?;
    eprintln!(
        "[convert] imported nodes={} edges={} stats={:?}",
        store.node_count(),
        store.edge_count(),
        stats
    );

    // Group nodes by primary label, collect property-key union per label.
    let mut nodes_by_label: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
    let mut node_props: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for node in store.all_nodes() {
        let label = node
            .labels
            .iter()
            .next()
            .map(|l| l.as_str().to_string())
            .unwrap_or_else(|| "_NoLabel".to_string());
        nodes_by_label
            .entry(label.clone())
            .or_default()
            .push(node.id);
        let entry = node_props.entry(label).or_default();
        for key in node.properties.keys() {
            entry.insert(key.clone());
        }
    }

    for (label, ids) in &nodes_by_label {
        let path = out_dir.join(format!("nodes-{}.csv", label));
        let mut w = BufWriter::new(File::create(&path)?);
        let keys: Vec<&String> = node_props[label].iter().collect();

        write!(w, "nodeId:ID")?;
        for k in &keys {
            write!(w, ",{}", k)?;
        }
        writeln!(w, ",:LABEL")?;

        for &nid in ids {
            let node = match store.get_node(nid) {
                Some(n) => n,
                None => continue,
            };
            write!(w, "{}", nid.as_u64())?;
            for k in &keys {
                let cell = node
                    .properties
                    .get(*k)
                    .map(format_value)
                    .unwrap_or_default();
                write!(w, ",{}", csv_escape(&cell))?;
            }
            let labels: Vec<String> =
                node.labels.iter().map(|l| l.as_str().to_string()).collect();
            writeln!(w, ",{}", labels.join(";"))?;
        }
        eprintln!("[convert] wrote {} ({} rows)", path.display(), ids.len());
    }

    // Edges, grouped by type. Edge property data is not exported here
    // (these KGs typically carry no edge properties; if any are present,
    // extend this loop with `store.get_edge(eid)` lookups.)
    let mut edges_by_type: BTreeMap<String, Vec<(NodeId, NodeId)>> = BTreeMap::new();
    for node in store.all_nodes() {
        for (_eid, _src, tgt, etype) in store.get_outgoing_edge_targets(node.id) {
            edges_by_type
                .entry(etype.as_str().to_string())
                .or_default()
                .push((node.id, tgt));
        }
    }

    for (etype, edges) in &edges_by_type {
        let path = out_dir.join(format!("rels-{}.csv", etype));
        let mut w = BufWriter::new(File::create(&path)?);
        writeln!(w, ":START_ID,:END_ID,:TYPE")?;
        for (src, tgt) in edges {
            writeln!(w, "{},{},{}", src.as_u64(), tgt.as_u64(), etype)?;
        }
        eprintln!("[convert] wrote {} ({} rows)", path.display(), edges.len());
    }

    eprintln!(
        "[convert] done — {} node files, {} relationship files",
        nodes_by_label.len(),
        edges_by_type.len()
    );
    Ok(())
}

fn format_value(v: &PropertyValue) -> String {
    match v {
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => f.to_string(),
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::DateTime(ts) => ts.to_string(), // Unix ms timestamp
        PropertyValue::Null => String::new(),
        PropertyValue::Array(a) => a
            .iter()
            .map(format_value)
            .collect::<Vec<_>>()
            .join(";"),
        _ => String::new(), // skip Map / nested types
    }
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
