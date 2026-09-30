//! The GraphStore -> GraphView adapter: filters, weights and edge times.

use super::*;
use crate::graph::NodeId;

fn pv_edge(
    store: &mut GraphStore,
    a: NodeId,
    b: NodeId,
    ty: &str,
    props: &[(&str, PropertyValue)],
) {
    let e = store.create_edge(a, b, ty).unwrap();
    for (k, v) in props {
        store.set_edge_property(e, *k, v.clone()).unwrap();
    }
}

#[test]
fn build_view_filters_by_label_and_edge_type_and_reads_weights() {
    let mut s = GraphStore::new();
    let a = s.create_node("P");
    let b = s.create_node("P");
    let c = s.create_node("Q");
    pv_edge(&mut s, a, b, "R", &[("w", PropertyValue::Integer(3))]);
    pv_edge(&mut s, b, a, "R", &[("w", PropertyValue::Float(0.5))]);
    pv_edge(
        &mut s,
        a,
        b,
        "R",
        &[("w", PropertyValue::String("x".into()))],
    );
    pv_edge(&mut s, a, b, "OTHER", &[]);
    pv_edge(&mut s, a, c, "R", &[]); // leaves the P subgraph

    let v = build_view(&s, Some("P"), Some("R"), Some("w"));
    assert_eq!(v.node_count, 2);
    assert_eq!(v.index_to_node, vec![a.as_u64(), b.as_u64()]);
    assert_eq!(v.out_offsets, vec![0, 2, 3]);
    assert_eq!(v.out_targets, vec![1, 1, 0]);
    assert_eq!(v.in_offsets, vec![0, 1, 3]);
    let mut w = v.weights.clone().unwrap();
    // Per-source order is the store's; compare the multiset per source.
    w[..2].sort_by(|x, y| x.partial_cmp(y).unwrap());
    assert_eq!(w, vec![1.0, 3.0, 0.5], "non-numeric weight counts as 1.0");

    let all = build_view(&s, None, None, None);
    assert_eq!(all.node_count, 3);
    assert_eq!(all.out_targets.len(), 5);
    assert!(all.weights.is_none());
}

#[test]
fn temporal_view_aligns_times_with_targets_and_reads_every_time_type() {
    let mut s = GraphStore::new();
    let src = s.create_node("N");
    let mut targets = Vec::new();
    let values = vec![
        PropertyValue::Integer(10),
        PropertyValue::DateTime(20),
        PropertyValue::LocalDateTime { secs: 30, nanos: 5 },
        PropertyValue::ZonedDateTime {
            secs: 40,
            nanos: 0,
            offset_seconds: 3600,
            zone: None,
        },
        PropertyValue::Date(2),
        PropertyValue::Float(60.9),
    ];
    for v in &values {
        let t = s.create_node("N");
        targets.push(t);
        pv_edge(&mut s, src, t, "E", &[("at", v.clone())]);
    }
    let (view, times) = build_temporal_view(&s, None, None, Some("at"));
    assert_eq!(view.out_targets.len(), times.len());
    let mut got: Vec<(u64, i64)> = view
        .out_targets
        .iter()
        .zip(&times)
        .map(|(idx, t)| (view.index_to_node[*idx], *t))
        .collect();
    got.sort();
    let want: Vec<(u64, i64)> = targets
        .iter()
        .map(|t| t.as_u64())
        .zip([10, 20, 30, 40, 2 * 86_400, 60])
        .collect();
    assert_eq!(got, want);
}

#[test]
fn edge_time_falls_back_to_created_at() {
    let mut s = GraphStore::new();
    let a = s.create_node("N");
    let b = s.create_node("N");
    pv_edge(
        &mut s,
        a,
        b,
        "E",
        &[("at", PropertyValue::String("soon".into()))],
    );
    let edge = s.get_outgoing_edges(a).remove(0);
    assert_eq!(
        edge_time(&edge, Some("at")),
        edge.created_at,
        "a non-time value is not zero"
    );
    assert_eq!(edge_time(&edge, Some("missing")), edge.created_at);
    assert_eq!(edge_time(&edge, None), edge.created_at);

    let (_, times) = build_temporal_view(&s, None, None, None);
    assert_eq!(times, vec![edge.created_at]);
}
