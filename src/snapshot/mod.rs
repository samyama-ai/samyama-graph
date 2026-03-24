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
use format::{ExportStats, ImportStats, SnapshotEdge, SnapshotHeader, SnapshotNode};

/// Export all nodes and edges from the store into a gzip-compressed .sgsnap stream.
pub fn export_tenant(
    store: &GraphStore,
    writer: impl Write,
) -> Result<ExportStats, Box<dyn std::error::Error>> {
    let nodes = store.all_nodes();
    let edges = store.all_edges();

    // Collect unique labels and edge types
    let mut label_set: HashSet<String> = HashSet::new();
    let mut edge_type_set: HashSet<String> = HashSet::new();

    for node in &nodes {
        for label in &node.labels {
            label_set.insert(label.as_str().to_string());
        }
    }
    for edge in &edges {
        edge_type_set.insert(edge.edge_type.as_str().to_string());
    }

    let mut labels: Vec<String> = label_set.into_iter().collect();
    labels.sort();
    let mut edge_types: Vec<String> = edge_type_set.into_iter().collect();
    edge_types.sort();

    let node_count = nodes.len() as u64;
    let edge_count = edges.len() as u64;

    // Create gzip encoder
    let mut gz = GzEncoder::new(writer, Compression::default());

    // Write header
    let header = SnapshotHeader {
        format: "sgsnap".to_string(),
        version: 1,
        tenant: "default".to_string(),
        node_count,
        edge_count,
        labels: labels.clone(),
        edge_types: edge_types.clone(),
        created_at: chrono::Utc::now().to_rfc3339(),
        samyama_version: crate::VERSION.to_string(),
    };
    let header_json = serde_json::to_string(&header)?;
    gz.write_all(header_json.as_bytes())?;
    gz.write_all(b"\n")?;

    // Write nodes
    for node in &nodes {
        let mut props = HashMap::new();
        for (key, value) in &node.properties {
            props.insert(key.clone(), property_to_json(value));
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

    // Write edges
    for edge in &edges {
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

    // Finish gzip stream and get underlying writer to measure bytes
    let finished = gz.finish()?;
    // We can't easily get bytes_written from the trait, so report 0
    // (the caller can measure the Vec length if using Vec<u8>)
    let _ = finished;

    Ok(ExportStats {
        node_count,
        edge_count,
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
    if header.version != 1 {
        return Err(format!(
            "unsupported snapshot version: expected 1, got {}",
            header.version
        )
        .into());
    }

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

            // Create node with the first label
            let first_label = snap_node
                .labels
                .first()
                .cloned()
                .unwrap_or_else(|| "".to_string());
            let new_id = store.create_node(first_label.as_str());

            // Add remaining labels
            if let Some(node) = store.get_node_mut(new_id) {
                for label in snap_node.labels.iter().skip(1) {
                    node.add_label(label.as_str());
                }

                // Set properties
                for (key, json_val) in &snap_node.props {
                    node.set_property(key.clone(), json_to_property(json_val));
                }
            }

            // Track labels
            for label in &snap_node.labels {
                imported_labels.insert(label.clone());
            }

            id_remap.insert(snap_node.id, new_id);
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

            // Convert properties
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
        assert_eq!(
            node.get_property("str"),
            Some(&PropertyValue::String("hello".to_string()))
        );
        assert_eq!(
            node.get_property("int"),
            Some(&PropertyValue::Integer(42))
        );
        assert_eq!(
            node.get_property("float"),
            Some(&PropertyValue::Float(3.14))
        );
        assert_eq!(
            node.get_property("bool"),
            Some(&PropertyValue::Boolean(true))
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

        // Verify edge points to correct nodes (remapped IDs)
        let edges = store2.all_edges();
        assert_eq!(edges.len(), 1);
        let edge = edges[0];
        // Source and target should be valid nodes
        assert!(store2.get_node(edge.source).is_some());
        assert!(store2.get_node(edge.target).is_some());
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
            samyama_version: "0.6.0".to_string(),
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
            samyama_version: "0.6.0".to_string(),
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
}
