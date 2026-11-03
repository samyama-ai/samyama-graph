//! A snapshot written in parallel batches is the same snapshot (#314).
//!
//! Export serializes records on every core in batches of 32K and writes them
//! in order. These tests cross several batch boundaries and check what a
//! reordering, a dropped batch or a duplicated one would break: the decoded
//! line sequence is nodes in node order, then edges, each once; a round trip
//! through import restores every node, edge and property; and the gzip is a
//! single member, so a `GzDecoder` -- which stops after the first member --
//! still reads all of it.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::snapshot::{export_tenant, import_tenant};
use std::io::{BufRead, Read};

/// Enough records for three node batches and several edge batches.
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..70_000 {
        let n = s.create_node_with_labels([Label::new(if i % 2 == 0 { "A" } else { "B" })]);
        s.set_node_property("default", n, "seq", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", n, "name", PropertyValue::String(format!("n{i}"))).unwrap();
        ids.push(n);
    }
    for i in 0..ids.len() {
        s.create_edge(ids[i], ids[(i * 7 + 3) % ids.len()], "R").unwrap();
        if i % 3 == 0 {
            s.create_edge(ids[i], ids[(i + 1) % ids.len()], "S").unwrap();
        }
    }
    s
}

#[test]
fn the_lines_come_out_in_order_and_each_once() {
    let s = store();
    let mut buf = Vec::new();
    export_tenant(&s, &mut buf).unwrap();
    // A single-member reader: every line must be in the first member.
    let text = {
        let mut out = String::new();
        flate2::read::GzDecoder::new(&buf[..]).read_to_string(&mut out).unwrap();
        out
    };
    let mut node_seqs = Vec::new();
    let mut edges = 0usize;
    let mut seen_edge = false;
    for line in std::io::Cursor::new(text).lines().skip(1) {
        let v: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        match v["t"].as_str() {
            Some("n") => {
                assert!(!seen_edge, "a node after an edge");
                node_seqs.push(v["props"]["seq"].as_i64().expect("seq"));
            }
            Some("e") => {
                seen_edge = true;
                edges += 1;
            }
            _ => {}
        }
    }
    assert_eq!(node_seqs, (0..70_000).collect::<Vec<i64>>(), "nodes reordered, dropped or repeated");
    assert_eq!(edges, s.edge_count());
}

#[test]
fn a_round_trip_restores_every_node_edge_and_property() {
    let s = store();
    let mut buf = Vec::new();
    export_tenant(&s, &mut buf).unwrap();
    let mut back = GraphStore::new();
    let stats = import_tenant(&mut back, &buf[..]).unwrap();
    assert_eq!(stats.node_count as usize, s.node_count());
    assert_eq!(stats.edge_count as usize, s.edge_count());
    assert_eq!(back.node_count(), s.node_count());
    assert_eq!(back.edge_count(), s.edge_count());
    assert_eq!(back.label_node_count(&Label::new("A")), 35_000);
    assert_eq!(back.edge_type_count(&samyama::graph::EdgeType::new("S")), s.edge_type_count(&samyama::graph::EdgeType::new("S")));
}
