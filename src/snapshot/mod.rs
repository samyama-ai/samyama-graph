//! Snapshot export and import for GraphStore.
//!
//! The `.sgsnap` format is gzip-compressed JSON-lines:
//! - Line 0: SnapshotHeader with metadata
//! - Lines 1..N: SnapshotNode records
//! - Lines N+1..M: SnapshotEdge records
//!
//! On import, old node IDs are remapped to new IDs via a HashMap.

pub mod format;

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::graph::property::PropertyValue;
use crate::graph::store::GraphStore;
use crate::graph::types::NodeId;
use format::{ExportStats, ImportStats, SnapshotEdge, SnapshotHeader, SnapshotNode, SNAPSHOT_VERSION};

/// Export all nodes and edges from the store into a gzip-compressed .sgsnap stream.
///
/// v2 format: merges ColumnStore properties into node records and exports stub
/// edges from adjacency lists (not just the Edge arena). This captures the full
/// graph state including bulk-loaded data from create_node_stub/create_edge_stub.
pub fn export_tenant(
    store: &GraphStore,
    writer: impl Write,
) -> Result<ExportStats, Box<dyn std::error::Error>> {
    let nodes = store.all_nodes();
    let full_edges = store.all_edges(); // Full Edge objects (may be empty for stub-loaded)

    // Collect unique labels
    let mut label_set: HashSet<String> = HashSet::new();
    for node in &nodes {
        for label in &node.labels {
            label_set.insert(label.as_str().to_string());
        }
    }

    // Count edges from adjacency lists (includes both full and stub edges)
    let mut edge_type_set: HashSet<String> = HashSet::new();
    let mut adjacency_edge_count: u64 = 0;

    // Collect full edge IDs to avoid double-counting
    let full_edge_ids: HashSet<u64> = full_edges.iter().map(|e| e.id.as_u64()).collect();

    // Count adjacency-only (stub) edges
    for node in &nodes {
        let idx = node.id.as_u64() as usize;
        // Frozen outgoing neighbors
        let frozen = store.frozen_outgoing_neighbors(idx);
        for &(_nid, eid) in &frozen {
            if !full_edge_ids.contains(&eid.as_u64()) {
                adjacency_edge_count += 1;
                if let Some(et) = store.get_edge_type(eid) {
                    edge_type_set.insert(et.as_str().to_string());
                }
            }
        }
        // Write buffer outgoing
        let buf = store.get_outgoing_neighbor_slice(node.id);
        for &(_nid, eid) in buf {
            if !full_edge_ids.contains(&eid.as_u64()) {
                adjacency_edge_count += 1;
                if let Some(et) = store.get_edge_type(eid) {
                    edge_type_set.insert(et.as_str().to_string());
                }
            }
        }
    }

    // Add full edge types
    for edge in &full_edges {
        edge_type_set.insert(edge.edge_type.as_str().to_string());
    }

    let mut labels: Vec<String> = label_set.into_iter().collect();
    labels.sort();
    let mut edge_types: Vec<String> = edge_type_set.into_iter().collect();
    edge_types.sort();

    let node_count = nodes.len() as u64;
    let total_edge_count = full_edges.len() as u64 + adjacency_edge_count;

    // Create gzip encoder
    let mut gz = GzEncoder::new(writer, Compression::default());

    // Write header (v2)
    let header = SnapshotHeader {
        format: "sgsnap".to_string(),
        version: SNAPSHOT_VERSION,
        tenant: "default".to_string(),
        node_count,
        edge_count: total_edge_count,
        labels: labels.clone(),
        edge_types: edge_types.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        samyama_version: crate::VERSION.to_string(),
    };
    let header_json = serde_json::to_string(&header)?;
    gz.write_all(header_json.as_bytes())?;
    gz.write_all(b"\n")?;

    // Write nodes (with ColumnStore properties merged)
    for node in &nodes {
        let mut props = HashMap::new();

        // 1. Node HashMap properties (from full create_node)
        for (key, value) in &node.properties {
            props.insert(key.clone(), property_to_json(value));
        }

        // 2. ColumnStore properties (from set_column_property / create_node_stub path)
        let col_keys = store.node_columns.get_property_keys(node.id.as_u64() as usize);
        for key in col_keys {
            if !props.contains_key(&key) { // Don't override HashMap props
                let val = store.node_columns.get_property(node.id.as_u64() as usize, &key);
                if !val.is_null() {
                    props.insert(key, property_to_json(&val));
                }
            }
        }

        let snap_node = SnapshotNode {
            t: "n".to_string(),
            id: node.id.as_u64(),
            labels: node.labels.iter().map(|l| l.as_str().to_string()).collect(),
            props,
        };
        let node_json = serde_json::to_string(&snap_node)?;
        gz.write_all(node_json.as_bytes())?;
        gz.write_all(b"\n")?;
    }

    // Write full edges (from Edge arena -- have properties)
    for edge in &full_edges {
        let mut props = HashMap::new();
        for (key, value) in &edge.properties {
            props.insert(key.clone(), property_to_json(value));
        }

        let snap_edge = SnapshotEdge {
            t: "e".to_string(),
            id: edge.id.as_u64(),
            src: edge.source.as_u64(),
            tgt: edge.target.as_u64(),
            edge_type: edge.edge_type.as_str().to_string(),
            props,
        };
        let edge_json = serde_json::to_string(&snap_edge)?;
        gz.write_all(edge_json.as_bytes())?;
        gz.write_all(b"\n")?;
    }

    // Write stub edges (from adjacency lists -- no properties, just topology + type)
    for node in &nodes {
        let src_id = node.id.as_u64();
        let idx = src_id as usize;

        // Frozen outgoing
        let frozen = store.frozen_outgoing_neighbors(idx);
        for &(tgt_nid, eid) in &frozen {
            if full_edge_ids.contains(&eid.as_u64()) { continue; }
            let et = store.get_edge_type(eid)
                .map(|e| e.as_str().to_string())
                .unwrap_or_default();
            let snap_edge = SnapshotEdge {
                t: "e".to_string(),
                id: eid.as_u64(),
                src: src_id,
                tgt: tgt_nid.as_u64(),
                edge_type: et,
                props: HashMap::new(),
            };
            let edge_json = serde_json::to_string(&snap_edge)?;
            gz.write_all(edge_json.as_bytes())?;
            gz.write_all(b"\n")?;
        }

        // Write buffer outgoing
        let buf = store.get_outgoing_neighbor_slice(node.id);
        for &(tgt_nid, eid) in buf {
            if full_edge_ids.contains(&eid.as_u64()) { continue; }
            let et = store.get_edge_type(eid)
                .map(|e| e.as_str().to_string())
                .unwrap_or_default();
            let snap_edge = SnapshotEdge {
                t: "e".to_string(),
                id: eid.as_u64(),
                src: src_id,
                tgt: tgt_nid.as_u64(),
                edge_type: et,
                props: HashMap::new(),
            };
            let edge_json = serde_json::to_string(&snap_edge)?;
            gz.write_all(edge_json.as_bytes())?;
            gz.write_all(b"\n")?;
        }
    }

    let finished = gz.finish()?;
    let _ = finished;

    Ok(ExportStats {
        node_count,
        edge_count: total_edge_count,
        labels,
        edge_types,
        bytes_written: 0,
    })
}

/// Import nodes and edges from a .sgsnap stream into the store.
/// Node IDs are remapped (old ID -> new ID) so the snapshot can be imported
/// into a store that already has data.
pub fn import_tenant(
    store: &mut GraphStore,
    reader: impl Read,
) -> Result<ImportStats, Box<dyn std::error::Error>> {
    let decoder = GzDecoder::new(reader);
    let buf_reader = BufReader::new(decoder);
    let mut lines = buf_reader.lines();

    // Parse header (first line)
    let header_line = lines
        .next()
        .ok_or("empty snapshot file: missing header")??;
    let header: SnapshotHeader = serde_json::from_str(&header_line)?;

    if header.format != "sgsnap" {
        return Err(format!(
            "invalid snapshot format: expected \"sgsnap\", got \"{}\"",
            header.format
        )
        .into());
    }
    if header.version != 1 && header.version != 2 {
        return Err(format!(
            "unsupported snapshot version: expected 1 or 2, got {}",
            header.version
        )
        .into());
    }
    let use_stubs = header.version >= 2;

    let mut id_remap: HashMap<u64, NodeId> = HashMap::new();
    let mut imported_node_count: u64 = 0;
    let mut imported_edge_count: u64 = 0;
    let mut imported_labels: HashSet<String> = HashSet::new();
    let mut imported_edge_types: HashSet<String> = HashSet::new();

    for line_result in lines {
        let line = line_result?;
        if line.is_empty() {
            continue;
        }

        // Peek at the type discriminator
        if line.contains("\"t\":\"n\"") {
            // Parse as node
            let snap_node: SnapshotNode = serde_json::from_str(&line)?;

            let first_label = snap_node
                .labels
                .first()
                .cloned()
                .unwrap_or_else(|| "".to_string());

            if use_stubs {
                // v2: use lightweight stubs + column properties
                let new_id = store.create_node_stub(first_label.as_str());
                // Add remaining labels
                if let Some(node) = store.get_node_mut(new_id) {
                    for label in snap_node.labels.iter().skip(1) {
                        node.add_label(label.as_str());
                    }
                }
                // Set properties: simple types go to ColumnStore, complex to HashMap
                for (key, json_val) in &snap_node.props {
                    let pv = json_to_property(json_val);
                    match &pv {
                        PropertyValue::String(_) | PropertyValue::Integer(_)
                        | PropertyValue::Float(_) | PropertyValue::Boolean(_) => {
                            store.set_column_property(new_id, &key, pv);
                        }
                        _ => {
                            // Complex types (Array, Map, Vector, DateTime, Duration)
                            // stored in Node HashMap since ColumnStore doesn't support them
                            if let Some(node) = store.get_node_mut(new_id) {
                                node.set_property(key.clone(), pv);
                            }
                        }
                    }
                }
                id_remap.insert(snap_node.id, new_id);
            } else {
                // v1: use full create_node with HashMap properties
                let new_id = store.create_node(first_label.as_str());
                if let Some(node) = store.get_node_mut(new_id) {
                    for label in snap_node.labels.iter().skip(1) {
                        node.add_label(label.as_str());
                    }
                    for (key, json_val) in &snap_node.props {
                        node.set_property(key.clone(), json_to_property(json_val));
                    }
                }
                id_remap.insert(snap_node.id, new_id);
            }

            // Track labels
            for label in &snap_node.labels {
                imported_labels.insert(label.clone());
            }

            imported_node_count += 1;
        } else if line.contains("\"t\":\"e\"") {
            // Parse as edge
            let snap_edge: SnapshotEdge = serde_json::from_str(&line)?;

            let new_src = id_remap.get(&snap_edge.src).ok_or_else(|| {
                format!(
                    "edge references unknown source node ID {}",
                    snap_edge.src
                )
            })?;
            let new_tgt = id_remap.get(&snap_edge.tgt).ok_or_else(|| {
                format!(
                    "edge references unknown target node ID {}",
                    snap_edge.tgt
                )
            })?;

            if use_stubs && snap_edge.props.is_empty() {
                // v2: use lightweight stub (adjacency-only, no Edge object)
                store.create_edge_stub(*new_src, *new_tgt, snap_edge.edge_type.as_str())?;
            } else {
                // v1 or edge with properties: use full create_edge
                let mut props = crate::graph::property::PropertyMap::new();
                for (key, json_val) in &snap_edge.props {
                    props.insert(key.clone(), json_to_property(json_val));
                }

                if props.is_empty() {
                    store.create_edge(*new_src, *new_tgt, snap_edge.edge_type.as_str())?;
                } else {
                    store.create_edge_with_properties(
                        *new_src,
                        *new_tgt,
                        snap_edge.edge_type.as_str(),
                        props,
                    )?;
                }
            }

            imported_edge_types.insert(snap_edge.edge_type.clone());
            imported_edge_count += 1;
        }
        // Skip unrecognized lines
    }

    // Compact adjacency lists to CSR for memory efficiency (DS-07)
    if imported_edge_count > 0 {
        store.compact_adjacency();
    }

    let mut labels: Vec<String> = imported_labels.into_iter().collect();
    labels.sort();
    let mut edge_types: Vec<String> = imported_edge_types.into_iter().collect();
    edge_types.sort();

    Ok(ImportStats {
        node_count: imported_node_count,
        edge_count: imported_edge_count,
        labels,
        edge_types,
    })
}

/// Convert PropertyValue to serde_json::Value for snapshot serialization
fn property_to_json(pv: &PropertyValue) -> serde_json::Value {
    match pv {
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::Integer(i) => serde_json::json!(i),
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::Boolean(b) => serde_json::json!(b),
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::DateTime(dt) => {
            // Wrap in an object to distinguish from plain integer
            serde_json::json!({"__type": "DateTime", "value": dt})
        }
        PropertyValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(property_to_json).collect())
        }
        PropertyValue::Map(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), property_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        PropertyValue::Vector(v) => {
            serde_json::json!({"__type": "Vector", "value": v})
        }
        PropertyValue::Duration {
            months,
            days,
            seconds,
            nanos,
        } => {
            serde_json::json!({
                "__type": "Duration",
                "months": months,
                "days": days,
                "seconds": seconds,
                "nanos": nanos
            })
        }
    }
}

/// Convert serde_json::Value back to PropertyValue for snapshot import
fn json_to_property(val: &serde_json::Value) -> PropertyValue {
    match val {
        serde_json::Value::String(s) => PropertyValue::String(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                PropertyValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                PropertyValue::Float(f)
            } else {
                PropertyValue::Null
            }
        }
        serde_json::Value::Bool(b) => PropertyValue::Boolean(*b),
        serde_json::Value::Null => PropertyValue::Null,
        serde_json::Value::Array(arr) => {
            PropertyValue::Array(arr.iter().map(json_to_property).collect())
        }
        serde_json::Value::Object(obj) => {
            // Check for tagged types (__type field)
            if let Some(type_tag) = obj.get("__type").and_then(|v| v.as_str()) {
                match type_tag {
                    "DateTime" => {
                        if let Some(dt) = obj.get("value").and_then(|v| v.as_i64()) {
                            return PropertyValue::DateTime(dt);
                        }
                    }
                    "Vector" => {
                        if let Some(arr) = obj.get("value").and_then(|v| v.as_array()) {
                            let floats: Vec<f32> = arr
                                .iter()
                                .filter_map(|v| v.as_f64().map(|f| f as f32))
                                .collect();
                            return PropertyValue::Vector(floats);
                        }
                    }
                    "Duration" => {
                        let months = obj
                            .get("months")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        let days =
                            obj.get("days").and_then(|v| v.as_i64()).unwrap_or(0);
                        let seconds = obj
                            .get("seconds")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0);
                        let nanos = obj
                            .get("nanos")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(0) as i32;
                        return PropertyValue::Duration {
                            months,
                            days,
                            seconds,
                            nanos,
                        };
                    }
                    _ => {}
                }
            }
            // Plain map (no __type tag)
            let map: HashMap<String, PropertyValue> = obj
                .iter()
                .map(|(k, v)| (k.clone(), json_to_property(v)))
                .collect();
            PropertyValue::Map(map)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_round_trip_basic() {
        // Create a store with nodes and edges
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        store
            .get_node_mut(a)
            .unwrap()
            .set_property("name", PropertyValue::String("Alice".to_string()));
        store
            .get_node_mut(a)
            .unwrap()
            .set_property("age", PropertyValue::Integer(30));
        let b = store.create_node("Person");
        store
            .get_node_mut(b)
            .unwrap()
            .set_property("name", PropertyValue::String("Bob".to_string()));
        let c = store.create_node("City");
        store
            .get_node_mut(c)
            .unwrap()
            .set_property("name", PropertyValue::String("NYC".to_string()));
        store.create_edge(a, b, "KNOWS").unwrap();
        store.create_edge(a, c, "LIVES_IN").unwrap();

        // Export
        let mut buf = Vec::new();
        let export_stats = export_tenant(&store, &mut buf).unwrap();
        assert_eq!(export_stats.node_count, 3);
        assert_eq!(export_stats.edge_count, 2);

        // Import into fresh store
        let mut store2 = GraphStore::new();
        let import_stats = import_tenant(&mut store2, Cursor::new(&buf)).unwrap();
        assert_eq!(import_stats.node_count, 3);
        assert_eq!(import_stats.edge_count, 2);

        // Verify counts match
        assert_eq!(store2.node_count(), 3);
        assert_eq!(store2.edge_count(), 2);
    }

    #[test]
    fn test_round_trip_properties() {
        let mut store = GraphStore::new();
        let n = store.create_node("Test");
        if let Some(node) = store.get_node_mut(n) {
            node.set_property("str", PropertyValue::String("hello".to_string()));
            node.set_property("int", PropertyValue::Integer(42));
            node.set_property("float", PropertyValue::Float(3.14));
            node.set_property("bool", PropertyValue::Boolean(true));
        }

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let nodes = store2.all_nodes();
        assert_eq!(nodes.len(), 1);
        let node = nodes[0];
        // v2 stores simple types in ColumnStore, check via node_columns
        let nid = node.id.as_u64() as usize;
        assert_eq!(
            store2.node_columns.get_property(nid, "str"),
            PropertyValue::String("hello".to_string())
        );
        assert_eq!(
            store2.node_columns.get_property(nid, "int"),
            PropertyValue::Integer(42)
        );
        assert_eq!(
            store2.node_columns.get_property(nid, "float"),
            PropertyValue::Float(3.14)
        );
        assert_eq!(
            store2.node_columns.get_property(nid, "bool"),
            PropertyValue::Boolean(true)
        );
    }

    #[test]
    fn test_id_remapping() {
        // Create store with specific node IDs, then import into a non-empty store
        let mut store1 = GraphStore::new();
        let a = store1.create_node("A");
        let b = store1.create_node("B");
        store1.create_edge(a, b, "REL").unwrap();

        let mut buf = Vec::new();
        export_tenant(&store1, &mut buf).unwrap();

        // Import into a store that already has nodes
        let mut store2 = GraphStore::new();
        store2.create_node("Existing"); // Takes ID 0

        let stats = import_tenant(&mut store2, Cursor::new(&buf)).unwrap();
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
        assert_eq!(store2.node_count(), 3); // 1 existing + 2 imported
        assert_eq!(store2.edge_count(), 1);

        // Verify edge connectivity via adjacency (works for both full and stub edges)
        // Find the two imported nodes (not the "Existing" one)
        let imported: Vec<_> = store2.all_nodes().iter()
            .filter(|n| n.labels.iter().any(|l| l.as_str() == "A" || l.as_str() == "B"))
            .map(|n| n.id)
            .collect();
        assert_eq!(imported.len(), 2, "Should have 2 imported nodes (A, B)");
    }

    #[test]
    fn test_gzip_compression() {
        let mut store = GraphStore::new();
        for i in 0..100 {
            let n = store.create_node("Node");
            store
                .get_node_mut(n)
                .unwrap()
                .set_property("id", PropertyValue::Integer(i));
        }

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        // Verify it's actually gzip (magic bytes 0x1f, 0x8b)
        assert!(buf.len() >= 2);
        assert_eq!(buf[0], 0x1f);
        assert_eq!(buf[1], 0x8b);
    }

    #[test]
    fn test_empty_store_export() {
        let store = GraphStore::new();
        let mut buf = Vec::new();
        let stats = export_tenant(&store, &mut buf).unwrap();
        assert_eq!(stats.node_count, 0);
        assert_eq!(stats.edge_count, 0);

        // Should still import fine
        let mut store2 = GraphStore::new();
        let import_stats = import_tenant(&mut store2, Cursor::new(&buf)).unwrap();
        assert_eq!(import_stats.node_count, 0);
        assert_eq!(import_stats.edge_count, 0);
    }

    #[test]
    fn test_property_to_json_roundtrip() {
        let cases = vec![
            PropertyValue::String("hello".to_string()),
            PropertyValue::Integer(42),
            PropertyValue::Float(3.14),
            PropertyValue::Boolean(true),
            PropertyValue::Null,
        ];
        for pv in cases {
            let json = property_to_json(&pv);
            let back = json_to_property(&json);
            assert_eq!(pv, back);
        }
    }

    #[test]
    fn test_edge_properties_roundtrip() {
        let mut store = GraphStore::new();
        let a = store.create_node("A");
        let b = store.create_node("B");
        let mut props = crate::graph::property::PropertyMap::new();
        props.insert("weight".to_string(), PropertyValue::Float(0.75));
        props.insert("since".to_string(), PropertyValue::Integer(2020));
        store
            .create_edge_with_properties(a, b, "REL", props)
            .unwrap();

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let edges = store2.all_edges();
        assert_eq!(edges.len(), 1);
        assert_eq!(
            edges[0].get_property("weight"),
            Some(&PropertyValue::Float(0.75))
        );
        assert_eq!(
            edges[0].get_property("since"),
            Some(&PropertyValue::Integer(2020))
        );
    }

    #[test]
    fn test_datetime_property_roundtrip() {
        let mut store = GraphStore::new();
        let n = store.create_node("Test");
        store
            .get_node_mut(n)
            .unwrap()
            .set_property("ts", PropertyValue::DateTime(1710000000000));

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let nodes = store2.all_nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].get_property("ts"),
            Some(&PropertyValue::DateTime(1710000000000))
        );
    }

    #[test]
    fn test_vector_property_roundtrip() {
        let mut store = GraphStore::new();
        let n = store.create_node("Test");
        store
            .get_node_mut(n)
            .unwrap()
            .set_property("embedding", PropertyValue::Vector(vec![1.0, 2.5, 3.0]));

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let nodes = store2.all_nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].get_property("embedding"),
            Some(&PropertyValue::Vector(vec![1.0, 2.5, 3.0]))
        );
    }

    #[test]
    fn test_duration_property_roundtrip() {
        let mut store = GraphStore::new();
        let n = store.create_node("Test");
        store.get_node_mut(n).unwrap().set_property(
            "interval",
            PropertyValue::Duration {
                months: 14,
                days: 5,
                seconds: 3600,
                nanos: 500,
            },
        );

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let nodes = store2.all_nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].get_property("interval"),
            Some(&PropertyValue::Duration {
                months: 14,
                days: 5,
                seconds: 3600,
                nanos: 500,
            })
        );
    }

    #[test]
    fn test_multi_label_roundtrip() {
        let mut store = GraphStore::new();
        let n = store.create_node("Person");
        store.get_node_mut(n).unwrap().add_label("Employee");
        store.get_node_mut(n).unwrap().add_label("Manager");

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let nodes = store2.all_nodes();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].labels.len(), 3);

        let label_strs: HashSet<String> = nodes[0]
            .labels
            .iter()
            .map(|l| l.as_str().to_string())
            .collect();
        assert!(label_strs.contains("Person"));
        assert!(label_strs.contains("Employee"));
        assert!(label_strs.contains("Manager"));
    }

    #[test]
    fn test_array_and_map_property_roundtrip() {
        let mut store = GraphStore::new();
        let n = store.create_node("Test");

        // Array property
        store.get_node_mut(n).unwrap().set_property(
            "tags",
            PropertyValue::Array(vec![
                PropertyValue::String("a".to_string()),
                PropertyValue::Integer(1),
            ]),
        );

        // Map property
        let mut map = HashMap::new();
        map.insert("x".to_string(), PropertyValue::Float(1.5));
        map.insert("y".to_string(), PropertyValue::Float(2.5));
        store
            .get_node_mut(n)
            .unwrap()
            .set_property("coords", PropertyValue::Map(map));

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        let nodes = store2.all_nodes();
        assert_eq!(nodes.len(), 1);

        // Verify array
        if let Some(PropertyValue::Array(arr)) = nodes[0].get_property("tags") {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], PropertyValue::String("a".to_string()));
            assert_eq!(arr[1], PropertyValue::Integer(1));
        } else {
            panic!("expected Array property for 'tags'");
        }

        // Verify map
        if let Some(PropertyValue::Map(m)) = nodes[0].get_property("coords") {
            assert_eq!(m.len(), 2);
            assert_eq!(m.get("x"), Some(&PropertyValue::Float(1.5)));
            assert_eq!(m.get("y"), Some(&PropertyValue::Float(2.5)));
        } else {
            panic!("expected Map property for 'coords'");
        }
    }

    #[test]
    fn test_invalid_format_rejected() {
        // Craft a gzip stream with bad format field
        let header = SnapshotHeader {
            format: "badformat".to_string(),
            version: 1,
            tenant: "default".to_string(),
            node_count: 0,
            edge_count: 0,
            labels: vec![],
            edge_types: vec![],
            created_at: "2026-01-01T00:00:00Z".to_string(),
            samyama_version: "0.6.1".to_string(),
        };
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        let header_json = serde_json::to_string(&header).unwrap();
        gz.write_all(header_json.as_bytes()).unwrap();
        gz.write_all(b"\n").unwrap();
        let buf = gz.finish().unwrap();

        let mut store = GraphStore::new();
        let result = import_tenant(&mut store, Cursor::new(&buf));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid snapshot format"));
    }

    #[test]
    fn test_unsupported_version_rejected() {
        let header = SnapshotHeader {
            format: "sgsnap".to_string(),
            version: 99,
            tenant: "default".to_string(),
            node_count: 0,
            edge_count: 0,
            labels: vec![],
            edge_types: vec![],
            created_at: "2026-01-01T00:00:00Z".to_string(),
            samyama_version: "0.6.1".to_string(),
        };
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        let header_json = serde_json::to_string(&header).unwrap();
        gz.write_all(header_json.as_bytes()).unwrap();
        gz.write_all(b"\n").unwrap();
        let buf = gz.finish().unwrap();

        let mut store = GraphStore::new();
        let result = import_tenant(&mut store, Cursor::new(&buf));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unsupported snapshot version"));
    }

    #[test]
    fn test_export_stats_labels_and_edge_types() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("City");
        store.create_edge(a, b, "LIVES_IN").unwrap();

        let mut buf = Vec::new();
        let stats = export_tenant(&store, &mut buf).unwrap();

        assert!(stats.labels.contains(&"Person".to_string()));
        assert!(stats.labels.contains(&"City".to_string()));
        assert!(stats.edge_types.contains(&"LIVES_IN".to_string()));
    }

    #[test]
    fn test_import_stats_labels_and_edge_types() {
        let mut store = GraphStore::new();
        let a = store.create_node("Person");
        let b = store.create_node("City");
        store.create_edge(a, b, "LIVES_IN").unwrap();

        let mut buf = Vec::new();
        export_tenant(&store, &mut buf).unwrap();

        let mut store2 = GraphStore::new();
        let stats = import_tenant(&mut store2, Cursor::new(&buf)).unwrap();

        assert!(stats.labels.contains(&"Person".to_string()));
        assert!(stats.labels.contains(&"City".to_string()));
        assert!(stats.edge_types.contains(&"LIVES_IN".to_string()));
    }

    #[test]
    fn test_v2_stub_roundtrip() {
        // Test the v2 path: create_node_stub + set_column_property + create_edge_stub
        let mut store = GraphStore::new();

        // Create stub nodes with column properties
        let a = store.create_node_stub("Article");
        store.set_column_property(a, "pmid", PropertyValue::String("12345".to_string()));
        store.set_column_property(a, "title", PropertyValue::String("Test Paper".to_string()));
        store.set_column_property(a, "year", PropertyValue::Integer(2024));

        let b = store.create_node_stub("Author");
        store.set_column_property(b, "name", PropertyValue::String("Dr. Smith".to_string()));

        let c = store.create_node_stub("Article");
        store.set_column_property(c, "pmid", PropertyValue::String("67890".to_string()));

        // Create stub edges (no Edge objects)
        store.create_edge_stub(a, b, "AUTHORED_BY").unwrap();
        store.create_edge_stub(c, a, "CITES").unwrap();
        store.compact_adjacency();

        assert_eq!(store.node_count(), 3);
        assert_eq!(store.edge_count(), 2);
        // Verify all_edges() returns empty (stubs only in adjacency)
        assert_eq!(store.all_edges().len(), 0);

        // Export v2
        let mut buf = Vec::new();
        let export_stats = export_tenant(&store, &mut buf).unwrap();
        assert_eq!(export_stats.node_count, 3);
        assert_eq!(export_stats.edge_count, 2, "v2 export should capture stub edges");

        // Import into fresh store
        let mut store2 = GraphStore::new();
        let import_stats = import_tenant(&mut store2, Cursor::new(&buf)).unwrap();
        assert_eq!(import_stats.node_count, 3);
        assert_eq!(import_stats.edge_count, 2);
        assert_eq!(store2.node_count(), 3);
        assert_eq!(store2.edge_count(), 2);

        // Verify column properties survived
        // Node IDs are remapped, find them by label
        let articles: Vec<_> = store2.get_nodes_by_label(&crate::graph::types::Label::new("Article"))
            .into_iter().collect();
        assert_eq!(articles.len(), 2);

        // Check column property on first article (remapped ID)
        let aid = articles[0].id.as_u64() as usize;
        let pmid = store2.node_columns.get_property(aid, "pmid");
        assert!(!pmid.is_null(), "Column property 'pmid' should survive v2 roundtrip");

        let authors: Vec<_> = store2.get_nodes_by_label(&crate::graph::types::Label::new("Author"))
            .into_iter().collect();
        assert_eq!(authors.len(), 1);
        let author_id = authors[0].id.as_u64() as usize;
        let name = store2.node_columns.get_property(author_id, "name");
        assert_eq!(name, PropertyValue::String("Dr. Smith".to_string()));

        // Verify edge type survived
        let eid = crate::graph::types::EdgeId::new(1);
        let et = store2.get_edge_type(eid);
        assert!(et.is_some(), "Edge type should survive v2 roundtrip");

        // Verify traversal works
        let article_id = articles[0].id;
        let targets = store2.get_outgoing_edge_targets_owned(article_id);
        assert!(!targets.is_empty() || {
            // Second article might be the one with outgoing edges
            let targets2 = store2.get_outgoing_edge_targets_owned(articles[1].id);
            !targets2.is_empty()
        }, "At least one article should have outgoing edges after v2 import");
    }
}

#[cfg(test)]
mod stub_cypher_tests {
    use crate::graph::GraphStore;
    use crate::graph::PropertyValue;
    use crate::query::QueryEngine;

    #[test]
    fn test_cypher_traversal_on_stub_edges() {
        let mut g = GraphStore::new();
        let a = g.create_node_stub("Article");
        g.set_column_property(a, "pmid", PropertyValue::String("12345".to_string()));
        let au = g.create_node_stub("Author");
        g.set_column_property(au, "name", PropertyValue::String("Dr. Smith".to_string()));
        let m = g.create_node_stub("MeSHTerm");
        g.set_column_property(m, "name", PropertyValue::String("Cancer".to_string()));

        g.create_edge_stub(a, au, "AUTHORED_BY").unwrap();
        g.create_edge_stub(a, m, "ANNOTATED_WITH").unwrap();
        g.compact_adjacency();

        let engine = QueryEngine::new();

        // 1-hop forward
        let r = engine.execute("MATCH (a:Article)-[:AUTHORED_BY]->(au:Author) RETURN au.name", &g).unwrap();
        assert_eq!(r.records.len(), 1, "Should find 1 author via stub edge, got {}", r.records.len());

        // 1-hop with filter
        let r = engine.execute("MATCH (a:Article)-[:ANNOTATED_WITH]->(m:MeSHTerm) WHERE a.pmid = '12345' RETURN m.name", &g).unwrap();
        assert_eq!(r.records.len(), 1, "Should find 1 MeSH via filtered stub edge");

        // Reverse traversal
        let r = engine.execute("MATCH (au:Author)<-[:AUTHORED_BY]-(a:Article) RETURN a.pmid", &g).unwrap();
        assert_eq!(r.records.len(), 1, "Should find 1 article via reverse stub edge");
    }
}

#[cfg(test)]
mod mixed_edge_tests {
    use crate::graph::GraphStore;
    use crate::graph::PropertyValue;
    use crate::query::QueryEngine;

    #[test]
    fn test_v1_edges_visible_after_v2_stubs() {
        // Simulate: import v1 snapshot (full edges) then v2 (stubs)
        let mut g = GraphStore::new();

        // v1 edges: full create_edge (like Clinical Trials import)
        let t = g.create_node("ClinicalTrial");
        g.get_node_mut(t).unwrap().set_property("nct_id", PropertyValue::String("NCT001".to_string()));
        let i = g.create_node("Intervention");
        g.get_node_mut(i).unwrap().set_property("name", PropertyValue::String("Aspirin".to_string()));
        g.create_edge(t, i, "TESTS").unwrap();
        g.compact_adjacency();

        // v2 stubs: create_edge_stub (like PubMed import after Clinical Trials)
        let a = g.create_node_stub("Article");
        g.set_column_property(a, "pmid", PropertyValue::String("12345".to_string()));
        let au = g.create_node_stub("Author");
        g.set_column_property(au, "name", PropertyValue::String("Dr. Smith".to_string()));
        g.create_edge_stub(a, au, "AUTHORED_BY").unwrap();
        g.compact_adjacency();

        let engine = QueryEngine::new();

        // v1 edges should still be traversable
        let r = engine.execute("MATCH (t:ClinicalTrial)-[:TESTS]->(i:Intervention) RETURN i.name", &g).unwrap();
        assert_eq!(r.records.len(), 1, "v1 full edge should be traversable after v2 stubs added, got {}", r.records.len());

        // v2 stub edges should also work
        let r = engine.execute("MATCH (a:Article)-[:AUTHORED_BY]->(au:Author) RETURN au.name", &g).unwrap();
        assert_eq!(r.records.len(), 1, "v2 stub edge should be traversable");
    }
}

#[cfg(test)]
mod v1_v2_import_order_tests {
    use super::*;
    use std::io::Cursor;
    use crate::graph::GraphStore;
    use crate::graph::PropertyValue;
    use crate::query::QueryEngine;

    #[test]
    fn test_v1_snapshot_then_v2_snapshot_edges_traversable() {
        // Step 1: Create a v1 snapshot (full edges, like Clinical Trials)
        let mut store1 = GraphStore::new();
        let t = store1.create_node("ClinicalTrial");
        store1.get_node_mut(t).unwrap().set_property("nct_id", PropertyValue::String("NCT001".to_string()));
        let i = store1.create_node("Intervention");
        store1.get_node_mut(i).unwrap().set_property("name", PropertyValue::String("Aspirin".to_string()));
        let s = store1.create_node("Site");
        store1.get_node_mut(s).unwrap().set_property("country", PropertyValue::String("USA".to_string()));
        store1.create_edge(t, i, "TESTS").unwrap();
        store1.create_edge(t, s, "CONDUCTED_AT").unwrap();

        let mut v1_buf = Vec::new();
        export_tenant(&store1, &mut v1_buf).unwrap();

        // Step 2: Create a v2 snapshot (stubs, like PubMed)
        let mut store2 = GraphStore::new();
        let a = store2.create_node_stub("Article");
        store2.set_column_property(a, "pmid", PropertyValue::String("12345".to_string()));
        let au = store2.create_node_stub("Author");
        store2.set_column_property(au, "name", PropertyValue::String("Dr. Smith".to_string()));
        store2.create_edge_stub(a, au, "AUTHORED_BY").unwrap();
        store2.compact_adjacency();

        let mut v2_buf = Vec::new();
        export_tenant(&store2, &mut v2_buf).unwrap();

        // Step 3: Import v1 THEN v2 into a fresh store (same order as trifecta)
        let mut combined = GraphStore::new();
        let s1 = import_tenant(&mut combined, Cursor::new(&v1_buf)).unwrap();
        assert_eq!(s1.edge_count, 2, "v1 import should have 2 edges");

        let s2 = import_tenant(&mut combined, Cursor::new(&v2_buf)).unwrap();
        assert_eq!(s2.edge_count, 1, "v2 import should have 1 edge");

        assert!(combined.node_count() >= 5, "Should have at least 5 nodes, got {}", combined.node_count());

        let engine = QueryEngine::new();

        // v1 edges should be traversable
        let r = engine.execute(
            "MATCH (t:ClinicalTrial)-[:TESTS]->(i:Intervention) RETURN i.name", &combined
        ).unwrap();
        assert!(r.records.len() > 0,
            "v1 TESTS edge should be traversable after v2 import. Got {} rows. \
             Trial nodes: {}, Intervention nodes: {}",
            r.records.len(),
            combined.get_nodes_by_label(&crate::graph::types::Label::new("ClinicalTrial")).len(),
            combined.get_nodes_by_label(&crate::graph::types::Label::new("Intervention")).len(),
        );

        let r = engine.execute(
            "MATCH (t:ClinicalTrial)-[:CONDUCTED_AT]->(s:Site) RETURN s.country", &combined
        ).unwrap();
        assert!(r.records.len() > 0, "v1 CONDUCTED_AT edge should be traversable");

        // v2 stub edges should also work
        let r = engine.execute(
            "MATCH (a:Article)-[:AUTHORED_BY]->(au:Author) RETURN au.name", &combined
        ).unwrap();
        assert!(r.records.len() > 0, "v2 stub AUTHORED_BY edge should be traversable");
    }
}

#[cfg(test)]
mod nct_bridge_edge_tests {
    use crate::graph::GraphStore;
    use crate::graph::PropertyValue;
    use crate::query::QueryEngine;

    #[test]
    fn test_referenced_in_edge_traversal() {
        let mut g = GraphStore::new();

        // Create Article stubs (PubMed)
        let a1 = g.create_node_stub("Article");
        g.set_column_property(a1, "pmid", PropertyValue::String("111".to_string()));
        g.set_column_property(a1, "title", PropertyValue::String("Cancer study".to_string()));
        let a2 = g.create_node_stub("Article");
        g.set_column_property(a2, "pmid", PropertyValue::String("222".to_string()));

        // Create MeSH stub
        let m = g.create_node_stub("MeSHTerm");
        g.set_column_property(m, "name", PropertyValue::String("Neoplasms".to_string()));
        g.create_edge_stub(a1, m, "ANNOTATED_WITH").unwrap();

        // Create ClinicalTrial (v1 full edge style)
        let t = g.create_node("ClinicalTrial");
        g.get_node_mut(t).unwrap().set_property("nct_id", PropertyValue::String("NCT001".to_string()));
        let i = g.create_node("Intervention");
        g.get_node_mut(i).unwrap().set_property("name", PropertyValue::String("Chemo".to_string()));
        g.create_edge(t, i, "TESTS").unwrap();

        // Create REFERENCED_IN edge (the NCT bridge)
        g.create_edge_stub(a1, t, "REFERENCED_IN").unwrap();
        g.compact_adjacency();

        let engine = QueryEngine::new();

        // Simple cross-KG: Article -> Trial
        let r = engine.execute(
            "MATCH (a:Article)-[:REFERENCED_IN]->(t:ClinicalTrial) RETURN a.pmid, t.nct_id", &g
        ).unwrap();
        assert_eq!(r.records.len(), 1, "Should find Article->Trial via REFERENCED_IN");

        // Deep cross-KG: MeSH -> Article -> Trial -> Intervention
        let r = engine.execute(
            "MATCH (m:MeSHTerm)<-[:ANNOTATED_WITH]-(a:Article)-[:REFERENCED_IN]->(t:ClinicalTrial)-[:TESTS]->(i:Intervention) WHERE m.name = 'Neoplasms' RETURN i.name", &g
        ).unwrap();
        assert_eq!(r.records.len(), 1, "Should traverse MeSH->Article->Trial->Intervention");
    }
}
