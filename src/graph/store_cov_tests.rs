//! Coverage-focused unit tests for `GraphStore`: error paths, rarely-taken
//! branches and the private tiers (frozen CSR, type adjacency, undo logs) that
//! the main test module does not reach.

use super::*;
use crate::graph::event::{IndexEvent, Mutation};
use crate::index::catalog::{IndexCatalog, IndexDefinition};

fn props(pairs: &[(&str, PropertyValue)]) -> PropertyMap {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn s(v: &str) -> PropertyValue {
    PropertyValue::String(v.to_string())
}

// ------------------------------------------------------------------
// MemoryReport
// ------------------------------------------------------------------

#[test]
fn memory_report_default_attributes_nothing() {
    let r = MemoryReport::default();
    assert_eq!(r.attributed(), 0);
    let lines = r.lines();
    assert_eq!(lines.len(), 12);
    assert!(lines.iter().all(|(_, b)| *b == 0));
}

#[test]
fn memory_report_walks_every_structure_and_sums_to_attributed() {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    let b = store.create_node("Person");
    store.get_node_mut(a).unwrap().set_property("name", "alice");
    store.set_node_property("default", b, "age", 30i64).unwrap();
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    store.set_edge_property(e, "since", 2020i64).unwrap();
    store
        .get_edge_properties_mut(e)
        .unwrap()
        .insert("note".to_string(), s("row-only"));
    // A second edge frozen into the CSR tier.
    store.create_edge(b, a, "KNOWS").unwrap();
    store.compact_adjacency();

    let r = store.memory_report();
    assert!(r.node_versions > 0);
    assert!(r.node_properties > 0, "row property of `a` is counted");
    assert!(r.node_labels > 0);
    assert!(r.node_columns > 0);
    assert!(r.edge_columns > 0);
    assert!(r.edge_endpoints > 0);
    assert!(r.edge_type_ids > 0);
    assert!(r.edge_properties > 0, "row map of `e` is counted");
    assert!(r.adjacency_frozen > 0);
    assert!(r.label_index > 0);
    assert!(r.edge_type_index > 0);

    let expected = r.node_columns
        + r.edge_columns
        + r.node_versions
        + r.node_properties
        + r.node_labels
        + r.edge_endpoints
        + r.edge_type_ids
        + r.edge_properties
        + r.adjacency_write_buffer
        + r.adjacency_frozen
        + r.label_index
        + r.edge_type_index;
    assert_eq!(r.attributed(), expected);

    let lines = r.lines();
    assert!(
        lines.windows(2).all(|w| w[0].1 >= w[1].1),
        "largest first: {lines:?}"
    );
    assert_eq!(lines.iter().map(|(_, b)| *b).sum::<usize>(), r.attributed());
}

// ------------------------------------------------------------------
// Write log and admission
// ------------------------------------------------------------------

#[test]
fn write_log_is_off_until_enabled_and_take_leaves_it_on() {
    let mut store = GraphStore::new();
    assert!(!store.write_log_enabled());
    store.create_node("A");
    assert!(
        store.take_write_log().is_empty(),
        "nothing recorded while off"
    );

    store.enable_write_log();
    store.enable_write_log(); // idempotent
    assert!(store.write_log_enabled());
    let n = store.create_node("A");
    let log = store.take_write_log();
    assert_eq!(log, vec![Mutation::NodeUpserted(n)]);
    assert!(store.write_log_enabled());
    assert!(store.take_write_log().is_empty());
}

#[test]
fn write_admission_round_trips() {
    let mut store = GraphStore::new();
    assert_eq!(store.write_admission(), None);
    let a = WriteAdmission {
        nodes_used: 3,
        max_nodes: Some(10),
        edges_used: 1,
        max_edges: None,
    };
    store.set_write_admission(Some(a));
    assert_eq!(store.write_admission(), Some(a));
}

#[test]
fn admit_node_refuses_at_the_ceiling_and_counts_births() {
    let mut store = GraphStore::new();
    assert!(store.admit_node().is_ok(), "no admission admits everything");
    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 0,
        max_nodes: None,
        edges_used: 0,
        max_edges: None,
    }));
    assert!(store.admit_node().is_ok(), "no node ceiling");

    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 1,
        max_nodes: Some(2),
        edges_used: 0,
        max_edges: None,
    }));
    assert!(store.admit_node().is_ok());
    store.create_node("A");
    match store.admit_node() {
        Err(GraphError::QuotaExceeded(msg)) => assert_eq!(msg, "nodes (2/2)"),
        other => panic!("expected quota error, got {other:?}"),
    }
    // Taking the log clears the statement's tally.
    store.take_write_log();
    assert!(store.admit_node().is_ok());
}

#[test]
fn admit_edge_refuses_through_every_edge_creation_path() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("A");
    assert!(store.admit_edge().is_ok());
    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 0,
        max_nodes: None,
        edges_used: 0,
        max_edges: None,
    }));
    assert!(store.admit_edge().is_ok(), "no edge ceiling");
    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 0,
        max_nodes: None,
        edges_used: 4,
        max_edges: Some(5),
    }));
    store.create_edge(a, b, "R").unwrap();
    let err = store.create_edge(a, b, "R").unwrap_err();
    assert_eq!(err, GraphError::QuotaExceeded("edges (5/5)".to_string()));
    assert!(matches!(
        store.create_edge_stub(a, b, "R"),
        Err(GraphError::QuotaExceeded(_))
    ));
    assert!(matches!(
        store.create_edge_with_properties(a, b, "R", PropertyMap::new()),
        Err(GraphError::QuotaExceeded(_))
    ));
    assert_eq!(store.edge_count(), 1);
}

#[test]
fn admits_bulk_checks_nodes_and_edges_against_the_ceiling() {
    let mut store = GraphStore::new();
    assert!(store.admits_bulk(1_000, 1_000).is_ok(), "no admission");

    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 5,
        max_nodes: Some(10),
        edges_used: 7,
        max_edges: Some(10),
    }));
    assert!(
        store.admits_bulk(5, 3).is_ok(),
        "exactly at the ceiling fits"
    );
    assert_eq!(
        store.admits_bulk(6, 0).unwrap_err(),
        GraphError::QuotaExceeded("nodes (11/10)".to_string())
    );
    assert_eq!(
        store.admits_bulk(0, 4).unwrap_err(),
        GraphError::QuotaExceeded("edges (11/10)".to_string())
    );

    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 0,
        max_nodes: None,
        edges_used: 0,
        max_edges: None,
    }));
    assert!(
        store.admits_bulk(u64::MAX, u64::MAX).is_ok(),
        "no limits at all"
    );

    // Births in the current statement count against the bulk question too.
    store.set_write_admission(Some(WriteAdmission {
        nodes_used: 0,
        max_nodes: Some(2),
        edges_used: 0,
        max_edges: None,
    }));
    store.create_node("A");
    assert!(store.admits_bulk(1, 0).is_ok());
    assert!(store.admits_bulk(2, 0).is_err());
}

// ------------------------------------------------------------------
// shrink_to_fit / integrity
// ------------------------------------------------------------------

#[test]
fn shrink_to_fit_releases_slack_without_changing_data() {
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..20).map(|_| store.create_node("N")).collect();
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "NEXT").unwrap();
    }
    store.delete_node("default", ids[5]).unwrap();
    let (nodes, edges) = (store.node_count(), store.edge_count());
    store.shrink_to_fit();
    assert_eq!(store.node_count(), nodes);
    assert_eq!(store.edge_count(), edges);
    assert_eq!(store.edge_endpoints.capacity(), store.edge_endpoints.len());
    assert_eq!(store.edge_type_ids.capacity(), store.edge_type_ids.len());
    assert!(store.edge_between(ids[0], ids[1], None).is_some());
}

#[test]
fn check_integrity_caps_the_report_at_one_hundred() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    store.edge_type_table.push(EdgeType::new("E"));
    // Edge id 0 is the "never created" sentinel; start at 1.
    store.edge_endpoints.push((NodeId::new(0), NodeId::new(0)));
    store.edge_type_ids.push(GraphStore::EDGE_TYPE_UNSET);
    for i in 0..150u64 {
        store.edge_endpoints.push((a, NodeId::new(10_000 + i)));
        store.edge_type_ids.push(0);
    }
    let found = store.check_integrity();
    assert_eq!(found.len(), 100);
    assert!(found
        .iter()
        .all(|v| matches!(v, IntegrityViolation::DanglingEdge { end: "target", .. })));
}

// ------------------------------------------------------------------
// Frozen CSR internals
// ------------------------------------------------------------------

#[test]
fn frozen_adjacency_empty_has_no_edges_and_one_offset() {
    let f = FrozenAdjacency::empty();
    assert!(f.is_empty());
    assert_eq!(f.edge_count(), 0);
    assert_eq!(f.node_capacity(), 0);
    assert!(f.neighbors(0).is_empty());
    assert!(f.find_neighbor(0, NodeId::new(1)).is_none());
    assert!(f.find_all_neighbors(3, NodeId::new(1)).is_empty());
    assert!(f.neighbor_range(3, NodeId::new(1)).is_empty());
}

#[test]
fn frozen_adjacency_finds_neighbours_by_binary_search() {
    let adj = vec![
        vec![
            (NodeId::new(9), EdgeId::new(1)),
            (NodeId::new(3), EdgeId::new(2)),
            (NodeId::new(3), EdgeId::new(3)),
        ],
        vec![],
    ];
    let f = FrozenAdjacency::from_vec_of_vec(&adj);
    assert!(!f.is_empty());
    assert_eq!(f.edge_count(), 3);
    assert_eq!(f.node_capacity(), 2);
    assert!(f.heap_bytes() > 0);
    assert_eq!(
        f.find_neighbor(0, NodeId::new(9)),
        Some((NodeId::new(9), EdgeId::new(1)))
    );
    assert_eq!(f.find_neighbor(0, NodeId::new(4)), None);
    assert_eq!(f.find_all_neighbors(0, NodeId::new(3)).len(), 2);
    assert!(f.find_all_neighbors(0, NodeId::new(4)).is_empty());
    assert_eq!(f.neighbor_range(0, NodeId::new(3)).len(), 2);
}

#[test]
fn frozen_store_single_segment_fast_path_and_multi_segment_panic() {
    let mut fs = FrozenAdjacencyStore::new();
    assert!(fs.is_empty());
    assert!(fs.is_single_segment());
    assert!(fs.neighbors(0).is_empty());
    assert!(fs.neighbors_collected(0).is_empty());
    assert_eq!(fs.heap_bytes(), 0);

    fs.push(FrozenAdjacency::from_vec_of_vec(&[vec![(
        NodeId::new(2),
        EdgeId::new(1),
    )]]));
    assert!(fs.is_single_segment());
    assert_eq!(fs.neighbors(0), &[(NodeId::new(2), EdgeId::new(1))]);
    assert!(fs.heap_bytes() > 0);

    fs.push(FrozenAdjacency::from_vec_of_vec(&[vec![(
        NodeId::new(1),
        EdgeId::new(2),
    )]]));
    assert!(!fs.is_single_segment());
    assert_eq!(fs.edge_count(), 2);
    // Collected across segments and sorted by neighbour id.
    assert_eq!(
        fs.neighbors_collected(0),
        vec![
            (NodeId::new(1), EdgeId::new(2)),
            (NodeId::new(2), EdgeId::new(1))
        ]
    );
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fs.neighbors(0).len()));
    assert!(r.is_err(), "neighbors() must refuse a multi-segment store");
    fs.clear();
    assert!(fs.is_empty());
}

// ------------------------------------------------------------------
// Typed adjacency walks over frozen + buffer tiers
// ------------------------------------------------------------------

/// a -KNOWS-> b, a -KNOWS-> c (frozen), a -LIKES-> b (frozen), then
/// a -KNOWS-> d and d -KNOWS-> a in the write buffer.
fn two_tier_store() -> (GraphStore, [NodeId; 4]) {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let c = store.create_node("P");
    let d = store.create_node("P");
    store.create_edge(a, b, "KNOWS").unwrap();
    store.create_edge(a, c, "KNOWS").unwrap();
    store.create_edge(a, b, "LIKES").unwrap();
    store.compact_adjacency();
    store.create_edge(a, d, "KNOWS").unwrap();
    store.create_edge(d, a, "KNOWS").unwrap();
    (store, [a, b, c, d])
}

#[test]
fn degree_for_type_counts_both_tiers_and_zero_for_unknown_types() {
    let (store, [a, b, _c, d]) = two_tier_store();
    let knows = EdgeType::new("KNOWS");
    let likes = EdgeType::new("LIKES");
    assert_eq!(store.outgoing_degree_for_type(a, &knows), 3);
    assert_eq!(store.outgoing_degree_for_type(a, &likes), 1);
    assert_eq!(store.incoming_degree_for_type(b, &knows), 1);
    assert_eq!(store.incoming_degree_for_type(b, &likes), 1);
    assert_eq!(
        store.incoming_degree_for_type(a, &knows),
        1,
        "d -> a in the buffer"
    );
    assert_eq!(store.outgoing_degree_for_type(d, &knows), 1);
    let none = EdgeType::new("NOPE");
    assert_eq!(store.outgoing_degree_for_type(a, &none), 0);
    assert_eq!(store.incoming_degree_for_type(a, &none), 0);
}

#[test]
fn for_each_neighbor_of_type_visits_both_tiers() {
    let (store, [a, b, c, d]) = two_tier_store();
    let knows = EdgeType::new("KNOWS");
    let mut out = Vec::new();
    store.for_each_outgoing_neighbor_of_type(a, &knows, |n| out.push(n));
    out.sort_by_key(|n| n.as_u64());
    assert_eq!(out, vec![b, c, d]);

    let mut inc = Vec::new();
    store.for_each_incoming_neighbor_of_type(b, &EdgeType::new("LIKES"), |n| inc.push(n));
    assert_eq!(inc, vec![a]);
    let mut inc_a = Vec::new();
    store.for_each_incoming_neighbor_of_type(a, &knows, |n| inc_a.push(n));
    assert_eq!(inc_a, vec![d]);

    let mut none = 0;
    store.for_each_outgoing_neighbor_of_type(a, &EdgeType::new("NOPE"), |_| none += 1);
    store.for_each_incoming_neighbor_of_type(a, &EdgeType::new("NOPE"), |_| none += 1);
    assert_eq!(none, 0);
}

#[test]
fn for_each_edge_between_typed_filters_by_type_and_can_stop_early() {
    let (store, [a, b, _c, d]) = two_tier_store();
    let knows = store.edge_type_id(&EdgeType::new("KNOWS")).unwrap();
    let likes = store.edge_type_id(&EdgeType::new("LIKES")).unwrap();

    let mut all = Vec::new();
    store.for_each_edge_between_typed(a, b, None, |e| {
        all.push(e);
        false
    });
    assert_eq!(all.len(), 2, "KNOWS and LIKES, both frozen");

    let mut only_likes = Vec::new();
    store.for_each_edge_between_typed(a, b, Some(&[likes]), |e| {
        only_likes.push(e);
        false
    });
    assert_eq!(only_likes.len(), 1);
    assert_eq!(
        store.get_edge_type(only_likes[0]),
        Some(EdgeType::new("LIKES"))
    );

    let mut first = 0;
    store.for_each_edge_between_typed(a, b, None, |_| {
        first += 1;
        true
    });
    assert_eq!(first, 1, "stops at the first accepted edge");

    let mut buffered = Vec::new();
    store.for_each_edge_between_typed(a, d, Some(&[knows]), |e| {
        buffered.push(e);
        true
    });
    assert_eq!(buffered.len(), 1, "write-buffer edge found and stopped on");

    let mut none = 0;
    store.for_each_edge_between_typed(a, d, Some(&[]), |_| {
        none += 1;
        false
    });
    assert_eq!(none, 0, "an empty filter accepts nothing");
}

#[test]
fn owned_edge_listings_include_the_frozen_tier() {
    let (store, [a, b, c, d]) = two_tier_store();
    let out = store.get_outgoing_edge_targets_owned(a);
    let mut targets: Vec<u64> = out.iter().map(|t| t.2.as_u64()).collect();
    targets.sort();
    assert_eq!(
        targets,
        vec![b.as_u64(), b.as_u64(), c.as_u64(), d.as_u64()]
    );
    let inc = store.get_incoming_edge_sources(b);
    assert_eq!(inc.len(), 2);
    assert!(inc.iter().all(|t| t.1 == a && t.2 == b));
    assert_eq!(
        store.frozen_outgoing_neighbors(a.as_u64() as usize).len(),
        3
    );
    assert_eq!(
        store.frozen_incoming_neighbors(b.as_u64() as usize).len(),
        2
    );
    assert_eq!(store.get_outgoing_neighbor_slice(a).len(), 1);
    assert_eq!(store.get_incoming_neighbor_slice(a).len(), 1);
    assert!(store
        .get_outgoing_neighbor_slice(NodeId::new(999))
        .is_empty());
    assert!(store
        .get_incoming_neighbor_slice(NodeId::new(999))
        .is_empty());
}

#[test]
fn edge_type_of_an_unknown_or_deleted_edge_is_none() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let e = store.create_edge(a, a, "SELF").unwrap();
    assert_eq!(store.get_edge_type(e), Some(EdgeType::new("SELF")));
    assert_eq!(store.get_edge_type(EdgeId::new(500)), None);
    store.delete_edge(e).unwrap();
    assert_eq!(store.get_edge_type(e), None);
    assert!(!store.edge_traversable_by(e, None));
}

// ------------------------------------------------------------------
// Type adjacency
// ------------------------------------------------------------------

#[test]
fn type_adjacency_fast_path_is_sorted_cached_and_invalidated() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let c = store.create_node("P");
    let e2 = store.create_edge(a, c, "KNOWS").unwrap();
    let e1 = store.create_edge(a, b, "KNOWS").unwrap();
    store.create_edge(b, c, "LIKES").unwrap();
    let knows = store.edge_type_id(&EdgeType::new("KNOWS")).unwrap();

    assert!(store.type_adjacency_if_built(knows, true).is_none());
    let out = store.type_adjacency(knows, true).expect("built");
    assert_eq!(out.len(), 2);
    assert!(!out.is_empty());
    assert_eq!(out.neighbors(a), &[(b, e1), (c, e2)], "sorted by target");
    assert!(out.neighbors(c).is_empty());
    let inc = store.type_adjacency(knows, false).expect("built");
    assert_eq!(inc.neighbors(c), &[(a, e2)]);
    assert_eq!(store.type_adjacency_cached(), 2);
    assert!(store.type_adjacency_if_built(knows, true).is_some());
    // Second call is served from the cache.
    let again = store.type_adjacency(knows, true).unwrap();
    assert!(std::sync::Arc::ptr_eq(&out, &again));

    // Deleting an edge drops every derived index.
    store.delete_edge(e1).unwrap();
    assert_eq!(store.type_adjacency_cached(), 0);
    let rebuilt = store.type_adjacency(knows, true).unwrap();
    assert_eq!(rebuilt.neighbors(a), &[(c, e2)]);
}

#[test]
fn type_adjacency_walks_when_the_type_index_is_incomplete() {
    // Stubs skip `edge_type_index`, so the fast build must not be used.
    let mut store = GraphStore::new();
    let a = store.create_node_stub("P");
    let b = store.create_node_stub("P");
    let c = store.create_node_stub("P");
    let e1 = store.create_edge_stub(a, c, "R").unwrap();
    let e2 = store.create_edge_stub(a, b, "R").unwrap();
    store.create_edge_stub(b, a, "Q").unwrap();
    let r = store.edge_type_id(&EdgeType::new("R")).unwrap();
    assert!(!store.edge_type_index_is_complete());
    assert!(store.type_adjacency_from_type_index(r, true).is_none());

    let out = store.type_adjacency(r, true).unwrap();
    assert_eq!(out.neighbors(a), &[(b, e2), (c, e1)]);
    let inc = store.type_adjacency(r, false).unwrap();
    assert_eq!(inc.neighbors(b), &[(a, e2)]);
    assert_eq!(inc.len(), 2);
}

#[test]
fn type_adjacency_from_type_index_rejects_unknown_type_ids() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    store.create_edge(a, a, "R").unwrap();
    assert!(store.edge_type_index_is_complete());
    assert!(store.type_adjacency_from_type_index(42, true).is_none());
    // An unknown type id falls through to the walk, which finds nothing.
    let adj = store.type_adjacency(42, true).unwrap();
    assert!(adj.is_empty());
}

// ------------------------------------------------------------------
// Merge of frozen segments
// ------------------------------------------------------------------

#[test]
fn merge_frozen_segments_on_an_empty_store_is_a_no_op() {
    let mut store = GraphStore::new();
    store.merge_frozen_segments();
    assert_eq!(store.adjacency_stats().frozen_segments, 0);
    assert!(!store.merge_frozen_segments_if_needed(0, 0.0));
}

#[test]
fn merge_frozen_segments_drops_dead_entries_and_frees_their_ids() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let c = store.create_node("P");
    let e1 = store.create_edge(a, b, "R").unwrap();
    store.compact_adjacency();
    let e2 = store.create_edge(a, c, "R").unwrap();
    store.compact_adjacency();
    store.create_edge(c, a, "R").unwrap(); // stays in the buffer
    store.delete_edge(e1).unwrap();

    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_segments, 2);
    assert_eq!(stats.frozen_dead_edges, 1);
    assert_eq!(store.edge_count(), 2);

    // Neither threshold crossed.
    assert!(!store.merge_frozen_segments_if_needed(5, 0.9));
    // Segment count crosses.
    assert!(store.merge_frozen_segments_if_needed(1, 0.9));

    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_segments, 1);
    assert_eq!(stats.frozen_dead_edges, 0);
    assert_eq!(stats.buffer_edges, 0);
    assert_eq!(stats.frozen_edges, 2);
    assert_eq!(store.edge_count(), 2);
    assert_eq!(store.edge_between(a, c, None), Some(e2));
    assert!(store.edge_between(a, b, None).is_none());
    assert!(store.edge_between(c, a, None).is_some());

    // The dead id is free again and is the next one handed out.
    let reused = store.create_edge(b, c, "R").unwrap();
    assert_eq!(reused, e1);
    assert_eq!(store.get_edge_endpoints(reused), Some((b, c)));
}

#[test]
fn merge_frozen_segments_if_needed_triggers_on_dead_fraction() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let e = store.create_edge(a, b, "R").unwrap();
    store.create_edge(b, a, "R").unwrap();
    store.compact_adjacency();
    store.delete_edge(e).unwrap();
    assert!(store.merge_frozen_segments_if_needed(10, 0.25));
    assert_eq!(store.adjacency_stats().frozen_dead_edges, 0);
    assert_eq!(store.edge_count(), 1);
}

#[test]
fn compact_adjacency_parallel_clear_for_large_stores() {
    let mut store = GraphStore::new();
    let first = store.create_node_stub("N");
    let mut last = first;
    for _ in 0..10_000 {
        last = store.create_node_stub("N");
    }
    store.create_edge_stub(first, last, "R").unwrap();
    assert!(store.compact_adjacency_if_needed(0));
    let stats = store.adjacency_stats();
    assert_eq!(stats.frozen_edges, 1);
    assert_eq!(stats.buffer_edges, 0);
    assert_eq!(store.get_outgoing_edge_targets_owned(first)[0].2, last);
}

// ------------------------------------------------------------------
// Property writes: errors and constraint enforcement
// ------------------------------------------------------------------

#[test]
fn set_node_property_rejects_a_map_inside_a_list() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    let bad = PropertyValue::Array(vec![PropertyValue::Map(HashMap::new())]);
    match store.set_node_property("default", n, "m", bad) {
        Err(GraphError::ConstraintViolation(msg)) => assert!(msg.contains("InvalidPropertyType")),
        other => panic!("expected constraint violation, got {other:?}"),
    }
    assert!(store.node_property(n, "m").is_none());
}

#[test]
fn set_node_property_on_a_missing_node_is_not_found() {
    let mut store = GraphStore::new();
    let missing = NodeId::new(77);
    assert_eq!(
        store.set_node_property("default", missing, "k", 1i64),
        Err(GraphError::NodeNotFound(missing))
    );
}

#[test]
fn unique_constraint_is_enforced_on_the_write_path() {
    let mut store = GraphStore::new();
    let label = Label::new("User");
    let a = store.create_node("User");
    let b = store.create_node("User");
    store
        .set_node_property("default", a, "email", "x@y")
        .unwrap();
    assert_eq!(store.create_unique_constraint(&label, "email"), Ok(1));

    // Same node, same value: not a violation.
    assert!(store
        .set_node_property("default", a, "email", "x@y")
        .is_ok());
    match store.set_node_property("default", b, "email", "x@y") {
        Err(GraphError::ConstraintViolation(msg)) => {
            assert!(msg.contains(":User(email)"), "{msg}");
        }
        other => panic!("expected violation, got {other:?}"),
    }
    // A different value is fine, and is then itself protected.
    store
        .set_node_property("default", b, "email", "z@y")
        .unwrap();
    let c = store.create_node("User");
    assert!(store
        .set_node_property("default", c, "email", "z@y")
        .is_err());
    // A property with no constraint on the label is not checked.
    store
        .set_node_property("default", c, "name", "same")
        .unwrap();
    store
        .set_node_property("default", b, "name", "same")
        .unwrap();
}

#[test]
fn create_unique_constraint_refuses_existing_duplicates_and_skips_nulls() {
    let mut store = GraphStore::new();
    let label = Label::new("User");
    for _ in 0..2 {
        let n = store.create_node("User");
        store
            .set_node_property("default", n, "email", "dup")
            .unwrap();
    }
    store.create_node("User"); // no email at all
    let err = store.create_unique_constraint(&label, "email").unwrap_err();
    assert!(err.contains("duplicate value"), "{err}");
    assert!(!store.property_index.has_unique_constraint(&label, "email"));
}

#[test]
fn set_edge_property_errors_and_null_removal() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let e = store.create_edge(a, a, "R").unwrap();
    let bad = PropertyValue::Array(vec![PropertyValue::Map(HashMap::new())]);
    assert!(matches!(
        store.set_edge_property(e, "m", bad),
        Err(GraphError::ConstraintViolation(_))
    ));
    let missing = EdgeId::new(99);
    assert_eq!(
        store.set_edge_property(missing, "k", 1i64),
        Err(GraphError::EdgeNotFound(missing))
    );

    store.set_edge_property(e, "k", 1i64).unwrap();
    assert_eq!(store.edge_property(e, "k"), Some(PropertyValue::Integer(1)));
    store
        .set_edge_property(e, "k", PropertyValue::Null)
        .unwrap();
    assert_eq!(store.edge_property(e, "k"), None);
    assert!(store.edge_property(missing, "k").is_none());
}

#[test]
fn set_edge_properties_sparse_with_only_nulls_does_not_journal() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let e = store.create_edge(a, a, "R").unwrap();
    store.set_edge_property(e, "k", 1i64).unwrap();
    store.enable_write_log();
    store.set_edge_properties_sparse(e, [("k", PropertyValue::Null)]);
    assert_eq!(store.edge_property(e, "k"), None);
    // The removal journals the edge once; no extra upsert for "wrote".
    assert_eq!(store.take_write_log(), vec![Mutation::EdgeUpserted(e)]);
}

#[test]
fn set_edge_property_sparse_supersedes_a_row_value() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let e = store.create_edge(a, a, "R").unwrap();
    store
        .get_edge_properties_mut(e)
        .unwrap()
        .insert("k".into(), s("row"));
    assert_eq!(store.edge_property(e, "k"), Some(s("row")));
    store.set_edge_properties_sparse(e, [("k", s("col"))]);
    assert_eq!(store.edge_property(e, "k"), Some(s("col")));
    assert!(
        store.get_edge_properties(e).unwrap().get("k").is_none(),
        "row copy dropped"
    );
    assert!(store.get_edge_properties_mut(EdgeId::new(1234)).is_none());
}

#[test]
fn remove_label_from_node_reports_whether_it_had_the_label() {
    let mut store = GraphStore::new();
    let only = Label::new("Only");
    let n = store.create_node("Only");
    assert_eq!(
        store.remove_label_from_node(n, &Label::new("Other")),
        Ok(false)
    );
    assert_eq!(store.remove_label_from_node(n, &only), Ok(true));
    assert!(
        store.nodes_with_label(&only).is_none(),
        "last member removes the set"
    );
    assert_eq!(
        store.remove_label_from_node(NodeId::new(55), &only),
        Err(GraphError::NodeNotFound(NodeId::new(55)))
    );
}

// ------------------------------------------------------------------
// Index events: the async sender path
// ------------------------------------------------------------------

#[test]
fn async_indexing_indexes_synchronously_and_forwards_every_event() {
    let (mut store, mut rx) = GraphStore::with_async_indexing();
    let label = Label::new("Doc");
    store
        .property_index
        .create_index(label.clone(), "title".to_string());

    let a = store.create_node("Doc");
    match rx.try_recv().unwrap() {
        IndexEvent::NodeCreated { id, properties, .. } => {
            assert_eq!(id, a);
            assert!(properties.is_empty());
        }
        other => panic!("unexpected {other:?}"),
    }

    let b = store.create_node_with_properties(
        "t1",
        vec![label.clone()],
        props(&[("title", s("hello"))]),
    );
    // The property index is written on this thread, not via the channel.
    let idx = store.property_index.get_index(&label, "title").unwrap();
    assert!(idx.read().unwrap().get(&s("hello")).contains(&b));
    match rx.try_recv().unwrap() {
        IndexEvent::NodeCreated { tenant_id, id, .. } => {
            assert_eq!(tenant_id, "t1");
            assert_eq!(id, b);
        }
        other => panic!("unexpected {other:?}"),
    }

    store.set_node_property("t2", b, "title", "world").unwrap();
    assert!(idx.read().unwrap().get(&s("hello")).is_empty());
    assert!(idx.read().unwrap().get(&s("world")).contains(&b));
    match rx.try_recv().unwrap() {
        IndexEvent::PropertySet {
            tenant_id,
            key,
            old_value,
            new_value,
            ..
        } => {
            assert_eq!(tenant_id, "t2");
            assert_eq!(key, "title");
            assert_eq!(old_value, Some(s("hello")));
            assert_eq!(new_value, s("world"));
        }
        other => panic!("unexpected {other:?}"),
    }

    let c = store.create_node("Other");
    let _ = rx.try_recv();
    store
        .get_node_mut(c)
        .unwrap()
        .set_property("title", "inline");
    store.add_label_to_node("t3", c, "Doc").unwrap();
    assert!(idx.read().unwrap().get(&s("inline")).contains(&c));
    match rx.try_recv().unwrap() {
        IndexEvent::LabelAdded {
            tenant_id,
            label: l,
            ..
        } => {
            assert_eq!(tenant_id, "t3");
            assert_eq!(l, label);
        }
        other => panic!("unexpected {other:?}"),
    }

    store.delete_node("t4", c).unwrap();
    match rx.try_recv().unwrap() {
        IndexEvent::NodeDeleted { tenant_id, id, .. } => {
            assert_eq!(tenant_id, "t4");
            assert_eq!(id, c);
        }
        other => panic!("unexpected {other:?}"),
    }
    assert!(!idx.read().unwrap().get(&s("inline")).contains(&c));
}

#[test]
fn handle_index_event_maintains_property_and_vector_indexes() {
    let store = GraphStore::new();
    let label = Label::new("Doc");
    store
        .property_index
        .create_index(label.clone(), "k".to_string());
    store
        .create_vector_index("Doc", "v", 2, DistanceMetric::Cosine)
        .unwrap();
    let id = NodeId::new(7);
    let idx = store.property_index.get_index(&label, "k").unwrap();

    store.handle_index_event(
        IndexEvent::NodeCreated {
            tenant_id: "default".into(),
            id,
            labels: vec![label.clone()],
            properties: props(&[("k", s("a")), ("v", PropertyValue::Vector(vec![1.0, 0.0]))]),
        },
        None,
    );
    assert!(idx.read().unwrap().get(&s("a")).contains(&id));
    let hits = store
        .vector_index
        .search("Doc", "v", &[1.0, 0.0], 1)
        .unwrap();
    assert_eq!(hits[0].0, id);

    store.handle_index_event(
        IndexEvent::PropertySet {
            tenant_id: "default".into(),
            id,
            labels: vec![label.clone()],
            key: "k".into(),
            old_value: Some(s("a")),
            new_value: s("b"),
        },
        None,
    );
    assert!(idx.read().unwrap().get(&s("a")).is_empty());
    assert!(idx.read().unwrap().get(&s("b")).contains(&id));

    let other = NodeId::new(8);
    store.handle_index_event(
        IndexEvent::PropertySet {
            tenant_id: "default".into(),
            id: other,
            labels: vec![label.clone()],
            key: "v".into(),
            old_value: None,
            new_value: PropertyValue::Vector(vec![0.0, 1.0]),
        },
        None,
    );
    let hits = store
        .vector_index
        .search("Doc", "v", &[0.0, 1.0], 1)
        .unwrap();
    assert_eq!(hits[0].0, other);

    let third = NodeId::new(9);
    store.handle_index_event(
        IndexEvent::LabelAdded {
            tenant_id: "default".into(),
            id: third,
            label: label.clone(),
            properties: props(&[("k", s("c")), ("v", PropertyValue::Vector(vec![0.7, 0.7]))]),
        },
        None,
    );
    assert!(idx.read().unwrap().get(&s("c")).contains(&third));
    assert_eq!(
        store
            .vector_index
            .search("Doc", "v", &[0.7, 0.7], 1)
            .unwrap()[0]
            .0,
        third
    );

    store.handle_index_event(
        IndexEvent::NodeDeleted {
            tenant_id: "default".into(),
            id: third,
            labels: vec![label.clone()],
            properties: props(&[("k", s("c"))]),
        },
        None,
    );
    assert!(idx.read().unwrap().get(&s("c")).is_empty());
}

#[test]
fn fulltext_index_follows_property_writes_and_type_changes() {
    let mut store = GraphStore::new();
    let n = store.create_node("Doc");
    store
        .set_node_property("default", n, "body", "graph databases")
        .unwrap();
    assert_eq!(store.create_fulltext_index("ft", "Doc", "body"), 1);
    assert!(store.index_catalog_is_dirty());
    assert_eq!(store.fulltext.search("ft", "graph", 10).unwrap().len(), 1);

    let m = store.create_node("Doc");
    store
        .set_node_property("default", m, "body", "graph theory")
        .unwrap();
    assert_eq!(store.fulltext.search("ft", "graph", 10).unwrap().len(), 2);

    // A property that stops being a string leaves the index.
    store
        .set_node_property("default", m, "body", 42i64)
        .unwrap();
    let hits = store.fulltext.search("ft", "graph", 10).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].node, n);

    store.clear_index_catalog_dirty();
    assert!(!store.drop_fulltext_index("missing"));
    assert!(
        !store.index_catalog_is_dirty(),
        "a failed drop changes nothing"
    );
    assert!(store.drop_fulltext_index("ft"));
    assert!(store.index_catalog_is_dirty());
    assert!(store.fulltext.search("ft", "graph", 10).is_none());
}

// ------------------------------------------------------------------
// Background indexer
// ------------------------------------------------------------------

fn mock_embed_config(policies: &[(&str, &str)]) -> crate::persistence::tenant::AutoEmbedConfig {
    let mut embedding_policies: HashMap<String, Vec<String>> = HashMap::new();
    for (label, key) in policies {
        embedding_policies
            .entry(label.to_string())
            .or_default()
            .push(key.to_string());
    }
    crate::persistence::tenant::AutoEmbedConfig {
        provider: crate::persistence::tenant::LLMProvider::Mock,
        embedding_model: "mock".to_string(),
        api_key: None,
        api_base_url: None,
        chunk_size: 100,
        chunk_overlap: 10,
        vector_dimension: 64,
        embedding_policies,
        embedding_property: "embedding".to_string(),
    }
}

#[tokio::test]
async fn background_indexer_adds_vectors_for_every_event_kind() {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let vector_index = Arc::new(VectorIndexManager::new());
    vector_index
        .create_index("Doc", "v", 2, DistanceMetric::Cosine)
        .unwrap();
    let property_index = Arc::new(IndexManager::new());
    let tenants = Arc::new(crate::persistence::TenantManager::new());
    let label = Label::new("Doc");

    tx.send(IndexEvent::NodeCreated {
        tenant_id: "default".into(),
        id: NodeId::new(1),
        labels: vec![label.clone()],
        properties: props(&[(
            "v",
            PropertyValue::Array(vec![PropertyValue::Float(1.0), PropertyValue::Float(0.0)]),
        )]),
    })
    .unwrap();
    tx.send(IndexEvent::PropertySet {
        tenant_id: "default".into(),
        id: NodeId::new(2),
        labels: vec![label.clone()],
        key: "v".into(),
        old_value: None,
        new_value: PropertyValue::Vector(vec![0.0, 1.0]),
    })
    .unwrap();
    tx.send(IndexEvent::LabelAdded {
        tenant_id: "default".into(),
        id: NodeId::new(3),
        label: label.clone(),
        properties: props(&[("v", PropertyValue::Vector(vec![-1.0, 0.0]))]),
    })
    .unwrap();
    tx.send(IndexEvent::NodeDeleted {
        tenant_id: "default".into(),
        id: NodeId::new(3),
        labels: vec![label.clone()],
        properties: PropertyMap::new(),
    })
    .unwrap();
    // An unknown tenant is skipped, not an error.
    tx.send(IndexEvent::PropertySet {
        tenant_id: "nobody".into(),
        id: NodeId::new(4),
        labels: vec![label.clone()],
        key: "title".into(),
        old_value: None,
        new_value: s("text"),
    })
    .unwrap();
    drop(tx);

    GraphStore::start_background_indexer(rx, vector_index.clone(), property_index, tenants).await;

    for (q, want) in [([1.0f32, 0.0], 1u64), ([0.0, 1.0], 2), ([-1.0, 0.0], 3)] {
        let hits = vector_index.search("Doc", "v", &q, 1).unwrap();
        assert_eq!(hits[0].0, NodeId::new(want), "query {q:?}");
    }
}

/// Wait until `check` holds, polling briefly. The auto-embed task is spawned
/// and not awaited by the indexer, so the test has to wait for it.
async fn eventually(mut check: impl FnMut() -> bool) -> bool {
    for _ in 0..500 {
        if check() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    check()
}

#[tokio::test]
async fn auto_embed_writes_the_embedding_onto_the_node() {
    let tenants = Arc::new(crate::persistence::TenantManager::new());
    tenants
        .update_embed_config("default", Some(mock_embed_config(&[("Doc", "text")])))
        .unwrap();
    let vector_index = Arc::new(VectorIndexManager::new());
    vector_index
        .create_index("Doc", "embedding", 64, DistanceMetric::Cosine)
        .unwrap();

    let mut inner = GraphStore::new();
    inner.vector_index = vector_index.clone();
    let n = inner.create_node("Doc");
    let store = Arc::new(tokio::sync::RwLock::new(inner));

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(IndexEvent::NodeCreated {
        tenant_id: "default".into(),
        id: n,
        labels: vec![Label::new("Doc")],
        properties: props(&[("text", s("hello world"))]),
    })
    .unwrap();
    drop(tx);
    GraphStore::start_background_indexer_with_store(
        rx,
        vector_index.clone(),
        Arc::new(IndexManager::new()),
        tenants,
        Some(store.clone()),
    )
    .await;

    let mut stored = None;
    for _ in 0..500 {
        stored = store.read().await.node_property(n, "embedding");
        if stored.is_some() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    match stored {
        Some(PropertyValue::Vector(v)) => assert_eq!(v.len(), 64),
        other => panic!("embedding was not written onto the node: {other:?}"),
    }
    assert_eq!(
        vector_index.model_id("Doc", "embedding").as_deref(),
        Some("mock")
    );
}

#[tokio::test]
async fn auto_embed_without_a_store_indexes_directly_for_property_and_label_events() {
    let tenants = Arc::new(crate::persistence::TenantManager::new());
    tenants
        .update_embed_config("default", Some(mock_embed_config(&[("Doc", "text")])))
        .unwrap();
    let vector_index = Arc::new(VectorIndexManager::new());
    vector_index
        .create_index("Doc", "embedding", 64, DistanceMetric::Cosine)
        .unwrap();
    // A different model already bound: the write still happens, with a warning.
    vector_index.set_model_id("Doc", "embedding", "some-other-model");

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(IndexEvent::PropertySet {
        tenant_id: "default".into(),
        id: NodeId::new(11),
        labels: vec![Label::new("Doc")],
        key: "text".into(),
        old_value: None,
        new_value: s("first text"),
    })
    .unwrap();
    tx.send(IndexEvent::LabelAdded {
        tenant_id: "default".into(),
        id: NodeId::new(12),
        label: Label::new("Doc"),
        properties: props(&[("text", s("second text")), ("other", s("not embedded"))]),
    })
    .unwrap();
    drop(tx);
    GraphStore::start_background_indexer(
        rx,
        vector_index.clone(),
        Arc::new(IndexManager::new()),
        tenants,
    )
    .await;

    let vi = vector_index.clone();
    let both = eventually(move || {
        vi.get_index("Doc", "embedding")
            .map(|i| i.read().unwrap().len())
            .unwrap_or(0)
            == 2
    })
    .await;
    assert!(both, "both auto-embedded vectors reach the index");
    assert_eq!(
        vector_index.model_id("Doc", "embedding").as_deref(),
        Some("some-other-model"),
        "an existing binding is not overwritten"
    );
}

#[tokio::test]
async fn auto_embed_falls_back_to_the_index_when_the_node_is_missing() {
    let tenants = Arc::new(crate::persistence::TenantManager::new());
    tenants
        .update_embed_config("default", Some(mock_embed_config(&[("Doc", "text")])))
        .unwrap();
    let vector_index = Arc::new(VectorIndexManager::new());
    vector_index
        .create_index("Doc", "embedding", 64, DistanceMetric::Cosine)
        .unwrap();
    let store = Arc::new(tokio::sync::RwLock::new(GraphStore::new()));

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(IndexEvent::NodeCreated {
        tenant_id: "default".into(),
        id: NodeId::new(40),
        labels: vec![Label::new("Doc")],
        properties: props(&[("text", s("orphan"))]),
    })
    .unwrap();
    drop(tx);
    GraphStore::start_background_indexer_with_store(
        rx,
        vector_index.clone(),
        Arc::new(IndexManager::new()),
        tenants,
        Some(store),
    )
    .await;
    let vi = vector_index.clone();
    let indexed = eventually(move || {
        vi.get_index("Doc", "embedding")
            .map(|i| i.read().unwrap().len())
            .unwrap_or(0)
            == 1
    })
    .await;
    assert!(
        indexed,
        "the vector is indexed even though the node write failed"
    );
}

#[tokio::test]
async fn agent_trigger_runs_for_a_labelled_node_with_a_policy() {
    let tenants = Arc::new(crate::persistence::TenantManager::new());
    let mut policies = HashMap::new();
    policies.insert("Company".to_string(), "Enrich this company".to_string());
    for api_key in [None, Some("k".to_string())] {
        tenants
            .update_agent_config(
                "default",
                Some(crate::persistence::tenant::AgentConfig {
                    enabled: true,
                    provider: crate::persistence::tenant::LLMProvider::Mock,
                    model: "mock".into(),
                    api_key,
                    api_base_url: None,
                    system_prompt: None,
                    tools: vec![],
                    policies: policies.clone(),
                }),
            )
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(IndexEvent::NodeCreated {
            tenant_id: "default".into(),
            id: NodeId::new(1),
            labels: vec![Label::new("Company"), Label::new("NoPolicy")],
            properties: props(&[("name", s("Acme"))]),
        })
        .unwrap();
        tx.send(IndexEvent::PropertySet {
            tenant_id: "default".into(),
            id: NodeId::new(1),
            labels: vec![Label::new("Company")],
            key: "name".into(),
            old_value: None,
            new_value: s("Acme Inc"),
        })
        .unwrap();
        drop(tx);
        let vi = Arc::new(VectorIndexManager::new());
        GraphStore::start_background_indexer(
            rx,
            vi.clone(),
            Arc::new(IndexManager::new()),
            tenants.clone(),
        )
        .await;
        // Let the spawned agent tasks run to completion against the mock model.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        // Text properties are never vectors, whatever the agent does.
        assert!(vi.list_indices().is_empty());
    }
    // A disabled agent does nothing either.
    tenants
        .update_agent_config(
            "default",
            Some(crate::persistence::tenant::AgentConfig {
                enabled: false,
                provider: crate::persistence::tenant::LLMProvider::Mock,
                model: "mock".into(),
                api_key: None,
                api_base_url: None,
                system_prompt: None,
                tools: vec![],
                policies,
            }),
        )
        .unwrap();
    assert!(
        !tenants
            .get_tenant("default")
            .unwrap()
            .agent_config
            .unwrap()
            .enabled
    );
}

// ------------------------------------------------------------------
// Vector index rebuilds
// ------------------------------------------------------------------

#[test]
fn embedding_candidate_accepts_vectors_and_float_arrays_only() {
    use PropertyValue as P;
    assert_eq!(
        GraphStore::embedding_candidate(&P::Vector(vec![1.0])),
        Some(vec![1.0])
    );
    assert_eq!(GraphStore::embedding_candidate(&P::Vector(vec![])), None);
    assert_eq!(
        GraphStore::embedding_candidate(&P::Array(vec![P::Integer(1), P::Float(0.5)])),
        Some(vec![1.0, 0.5])
    );
    assert_eq!(
        GraphStore::embedding_candidate(&P::Array(vec![P::Integer(1), P::Integer(2)])),
        None
    );
    assert_eq!(GraphStore::embedding_candidate(&P::Array(vec![])), None);
    assert_eq!(GraphStore::embedding_candidate(&s("x")), None);
}

#[test]
fn rebuild_vector_index_full_discovers_and_populates_embeddings() {
    let mut store = GraphStore::new();
    let a = store.create_node("Doc");
    let b = store.create_node("Doc");
    let c = store.create_node("Doc");
    store
        .set_node_property(
            "default",
            a,
            "emb",
            PropertyValue::Array(vec![PropertyValue::Float(1.0), PropertyValue::Float(0.0)]),
        )
        .unwrap();
    store
        .set_node_property("default", b, "emb", PropertyValue::Vector(vec![0.0, 1.0]))
        .unwrap();
    // Integers only: not an embedding.
    store
        .set_node_property(
            "default",
            c,
            "scores",
            PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]),
        )
        .unwrap();
    // An unlabelled node is skipped.
    let u = store.create_node_with_labels(std::iter::empty::<Label>());
    store
        .set_node_property("default", u, "emb", PropertyValue::Vector(vec![1.0, 1.0]))
        .unwrap();

    assert!(!store.index_catalog_is_dirty());
    assert_eq!(store.rebuild_vector_index_full(), 1);
    assert!(store.index_catalog_is_dirty());
    assert!(store.vector_index.get_index("Doc", "scores").is_none());
    let hits = store.vector_search("Doc", "emb", &[0.0, 1.0], 1).unwrap();
    assert_eq!(hits[0].0, b);
    // Already registered: a second pass registers nothing new.
    store.clear_index_catalog_dirty();
    assert_eq!(store.rebuild_vector_index_full(), 1);
    assert!(!store.index_catalog_is_dirty());
}

#[test]
fn rebuild_vector_index_reads_inline_and_columnar_embeddings() {
    let mut store = GraphStore::new();
    let a = store.create_node("Doc");
    let b = store.create_node("Doc");
    store
        .get_node_mut(a)
        .unwrap()
        .set_property("v", PropertyValue::Vector(vec![1.0, 0.0]));
    store.set_column_property(b, "v", PropertyValue::Vector(vec![0.0, 1.0]));
    store
        .create_vector_index("Doc", "v", 2, DistanceMetric::Cosine)
        .unwrap();
    store.rebuild_vector_index();
    assert_eq!(
        store.vector_search("Doc", "v", &[1.0, 0.0], 1).unwrap()[0].0,
        a
    );
    assert_eq!(
        store.vector_search("Doc", "v", &[0.0, 1.0], 1).unwrap()[0].0,
        b
    );
    let all = store.vector_search_all(&[0.0, 1.0], 2).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].0, b);
}

#[test]
fn named_vector_indexes_resolve_by_name() {
    let store = GraphStore::new();
    store
        .create_vector_index_named(
            Some("docs"),
            "Doc",
            "v",
            3,
            DistanceMetric::Cosine,
            crate::vector::index::Quantization::None,
        )
        .unwrap();
    assert!(store.index_catalog_is_dirty());
    assert_eq!(
        store.resolve_vector_index("docs"),
        Some(("Doc".to_string(), "v".to_string()))
    );
    assert_eq!(store.resolve_vector_index("nope"), None);
    assert_eq!(store.vector_index_names(), vec!["docs".to_string()]);
}

// ------------------------------------------------------------------
// Index catalog
// ------------------------------------------------------------------

#[test]
fn drop_property_index_reports_whether_one_existed() {
    let mut store = GraphStore::new();
    let label = Label::new("P");
    assert!(!store.drop_property_index(&label, "x"));
    assert!(!store.index_catalog_is_dirty());
    store.create_property_index(&label, "x");
    store.clear_index_catalog_dirty();
    assert!(store.drop_property_index(&label, "x"));
    assert!(store.index_catalog_is_dirty());
    assert!(!store.property_index.has_index(&label, "x"));
}

#[test]
fn create_property_index_backfills_row_and_column_values() {
    let mut store = GraphStore::new();
    let label = Label::new("P");
    let a = store.create_node("P");
    let b = store.create_node("P");
    store.create_node("P"); // no value
    store.get_node_mut(a).unwrap().set_property("x", 1i64);
    store.set_node_property("default", b, "x", 2i64).unwrap();
    assert_eq!(store.create_property_index(&label, "x"), 2);
    let idx = store.property_index.get_index(&label, "x").unwrap();
    assert!(idx
        .read()
        .unwrap()
        .get(&PropertyValue::Integer(1))
        .contains(&a));
    assert!(idx
        .read()
        .unwrap()
        .get(&PropertyValue::Integer(2))
        .contains(&b));
}

#[test]
fn index_catalog_lists_every_kind_in_a_stable_order() {
    let mut store = GraphStore::new();
    store.create_property_index(&Label::new("B"), "p");
    store.create_property_index(&Label::new("A"), "p");
    store
        .create_unique_constraint(&Label::new("A"), "id")
        .unwrap();
    store.create_fulltext_index("ft", "A", "body");
    store
        .create_vector_index_named(
            Some("vec"),
            "A",
            "emb",
            4,
            DistanceMetric::Cosine,
            Default::default(),
        )
        .unwrap();

    let cat = store.index_catalog();
    // Property rows come first, then constraints, full-text, vector.
    let kinds: Vec<u8> = cat
        .definitions
        .iter()
        .map(|d| match d {
            IndexDefinition::Property { .. } => 0,
            IndexDefinition::UniqueConstraint { .. } => 1,
            IndexDefinition::FullText { .. } => 2,
            IndexDefinition::Vector { .. } => 3,
        })
        .collect();
    let mut sorted = kinds.clone();
    sorted.sort();
    assert_eq!(kinds, sorted);
    assert!(cat
        .definitions
        .contains(&IndexDefinition::UniqueConstraint {
            label: "A".into(),
            property: "id".into()
        }));
    assert!(cat.definitions.contains(&IndexDefinition::FullText {
        name: "ft".into(),
        label: "A".into(),
        property: "body".into()
    }));
    assert!(cat.definitions.iter().any(|d| matches!(d,
        IndexDefinition::Vector { name: Some(n), dimensions: 4, .. } if n == "vec")));
    // Two property indexes on different labels, A before B.
    let props: Vec<&str> = cat
        .definitions
        .iter()
        .filter_map(|d| match d {
            IndexDefinition::Property { label, property } if property == "p" => {
                Some(label.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(props, vec!["A", "B"]);
    assert_eq!(store.index_catalog(), cat, "deterministic");
}

#[test]
fn restore_index_catalog_rebuilds_from_rows_and_reports_failures() {
    let mut store = GraphStore::new();
    let a = store.create_node("Doc");
    let b = store.create_node("Doc");
    store
        .set_node_property("default", a, "title", "graph")
        .unwrap();
    store
        .set_node_property("default", b, "title", "graph")
        .unwrap();
    store.set_node_property("default", a, "id", 1i64).unwrap();
    store.set_node_property("default", b, "id", 2i64).unwrap();
    store
        .set_node_property("default", a, "v", PropertyValue::Vector(vec![1.0, 0.0]))
        .unwrap();

    let catalog = IndexCatalog {
        definitions: vec![
            IndexDefinition::Property {
                label: "Doc".into(),
                property: "title".into(),
            },
            IndexDefinition::UniqueConstraint {
                label: "Doc".into(),
                property: "id".into(),
            },
            // Duplicate titles: this constraint cannot be restored.
            IndexDefinition::UniqueConstraint {
                label: "Doc".into(),
                property: "title".into(),
            },
            IndexDefinition::FullText {
                name: "ft".into(),
                label: "Doc".into(),
                property: "title".into(),
            },
            IndexDefinition::Vector {
                name: Some("vi".into()),
                label: "Doc".into(),
                property: "v".into(),
                dimensions: 2,
                metric: DistanceMetric::Cosine,
                quantization: Default::default(),
            },
        ],
    };
    let report = store.restore_index_catalog(&catalog);
    assert_eq!(report.property, 1);
    assert_eq!(report.unique, 1);
    assert_eq!(report.fulltext, 1);
    assert_eq!(report.vector, 1);
    assert_eq!(report.failed, 1);
    assert_eq!(report.total(), 4);
    assert!(
        !store.index_catalog_is_dirty(),
        "a restore is not a change to persist"
    );

    assert!(store
        .property_index
        .has_unique_constraint(&Label::new("Doc"), "id"));
    assert!(!store
        .property_index
        .has_unique_constraint(&Label::new("Doc"), "title"));
    assert_eq!(store.fulltext.search("ft", "graph", 10).unwrap().len(), 2);
    assert_eq!(
        store.vector_search("Doc", "v", &[1.0, 0.0], 1).unwrap()[0].0,
        a
    );
    assert_eq!(
        store.resolve_vector_index("vi"),
        Some(("Doc".into(), "v".into()))
    );
}

// ------------------------------------------------------------------
// Hierarchy invalidation hooks
// ------------------------------------------------------------------

#[test]
fn hierarchy_hooks_mark_stale_only_when_a_hierarchy_is_declared() {
    use crate::index::hierarchy::manager::HierarchySpec;
    use crate::index::hierarchy::monoid::RollupOp;
    let mut store = GraphStore::new();
    // No hierarchy: the hooks are no-ops.
    store.invalidate_hierarchies_for_property("cost");
    store.invalidate_hierarchies_for_edge_type(&EdgeType::new("PARENT"));

    let root = store.create_node("T");
    let child = store.create_node("T");
    store.create_edge(child, root, "PARENT").unwrap();
    let spec = HierarchySpec::new("h", vec![EdgeType::new("PARENT")]).with_measure(
        None,
        "cost",
        vec![RollupOp::Sum],
    );
    let mgr = store.hierarchy_index.clone();
    mgr.create(&store, spec).unwrap();
    assert!(mgr.any_usable());

    store.invalidate_hierarchies_for_property("unrelated");
    assert!(mgr.any_usable());
    store.invalidate_hierarchies_for_property("cost");
    assert!(
        !mgr.any_usable(),
        "a measure write elsewhere marks the index stale"
    );
}

// ------------------------------------------------------------------
// Statistics cache
// ------------------------------------------------------------------

#[test]
fn statistics_cache_is_filled_on_read_and_dropped_on_write() {
    let mut store = GraphStore::new();
    assert!(!store.has_cached_statistics());
    store.create_node("A");
    let stats = store.statistics();
    assert_eq!(stats.total_nodes, 1);
    assert!(store.has_cached_statistics());
    assert!(std::sync::Arc::ptr_eq(&stats, &store.statistics()));
    store.create_node("A");
    assert!(!store.has_cached_statistics());
    assert_eq!(store.statistics().total_nodes, 2);
}

#[test]
fn schema_summary_samples_the_lowest_edge_of_a_large_type() {
    let mut store = GraphStore::new();
    let hub = store.create_node("Hub");
    for _ in 0..8 {
        let leaf = store.create_node("Leaf");
        store.create_edge(hub, leaf, "HAS").unwrap();
    }
    store
        .set_node_property("default", hub, "name", "h")
        .unwrap();
    let summary = store.schema_summary();
    assert!(
        summary.contains("(Hub)-[:HAS]->(Leaf) (8 edges)"),
        "{summary}"
    );
    assert!(summary.contains(":Hub has properties: name"), "{summary}");
    assert!(summary.contains(":Leaf (8 nodes)"), "{summary}");
}

// ------------------------------------------------------------------
// Session transactions (BEGIN / COMMIT / ROLLBACK)
// ------------------------------------------------------------------

#[test]
fn session_transaction_lifecycle_errors() {
    let mut store = GraphStore::new();
    assert_eq!(store.session_transaction_version(), None);
    assert_eq!(
        store.commit_session_transaction(),
        Err(GraphError::TransactionNotFound(0))
    );
    assert_eq!(
        store.rollback_session_transaction(),
        Err(GraphError::TransactionNotFound(0))
    );
    let v = store.begin_session_transaction().unwrap();
    assert_eq!(v, 2);
    assert_eq!(store.session_transaction_version(), Some(2));
    assert_eq!(
        store.begin_session_transaction(),
        Err(GraphError::TransactionNotActive(2))
    );
    assert_eq!(store.commit_session_transaction(), Ok(2));
    assert_eq!(store.session_transaction_version(), None);
}

#[test]
fn session_transaction_timeout_defaults_to_thirty_seconds() {
    let t = GraphStore::session_transaction_timeout();
    match std::env::var("SAMYAMA_TX_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(secs) if secs > 0 => assert_eq!(t, std::time::Duration::from_secs(secs)),
        _ => assert_eq!(t, std::time::Duration::from_secs(30)),
    }
}

#[test]
fn rollback_restores_a_deleted_relationship_with_its_properties() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let e = store.create_edge(a, b, "KNOWS").unwrap();
    store.set_edge_property(e, "since", 2001i64).unwrap();

    store.begin_session_transaction().unwrap();
    store.delete_edge(e).unwrap();
    assert!(!store.has_edge(e));
    store.rollback_session_transaction().unwrap();

    assert!(store.has_edge(e));
    assert_eq!(
        store.edge_property(e, "since"),
        Some(PropertyValue::Integer(2001))
    );
    assert_eq!(
        store.edge_between(a, b, Some(&EdgeType::new("KNOWS"))),
        Some(e)
    );
    assert!(!store.free_edge_ids.contains(&e.as_u64()));
    assert_eq!(store.current_version, 1);
}

#[test]
fn rollback_undoes_edge_property_writes_and_removals() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let e = store.create_edge(a, a, "R").unwrap();
    store.set_edge_property(e, "keep", 1i64).unwrap();
    store.set_edge_property(e, "drop", 2i64).unwrap();

    store.begin_session_transaction().unwrap();
    store.set_edge_property(e, "keep", 10i64).unwrap(); // overwrite
    store
        .set_edge_property(e, "drop", PropertyValue::Null)
        .unwrap(); // removal
    store.set_edge_property(e, "new", 3i64).unwrap(); // added
    store.rollback_session_transaction().unwrap();

    assert_eq!(
        store.edge_property(e, "keep"),
        Some(PropertyValue::Integer(1))
    );
    assert_eq!(
        store.edge_property(e, "drop"),
        Some(PropertyValue::Integer(2))
    );
    assert_eq!(store.edge_property(e, "new"), None);
}

#[test]
fn rollback_undoes_a_whole_map_handed_out_mutably() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let e = store.create_edge(a, a, "R").unwrap();
    store.set_edge_property(e, "k", 1i64).unwrap();

    store.begin_session_transaction().unwrap();
    {
        let row = store.get_edge_properties_mut(e).unwrap();
        row.insert("extra".to_string(), s("x"));
    }
    // A key-level write at the same version records nothing further: the Map
    // entry already covers every key.
    store.set_edge_property(e, "k", 5i64).unwrap();
    let before = store.get_edge_at_version(e, 1).unwrap();
    assert_eq!(before.properties.get("k"), Some(&PropertyValue::Integer(1)));
    assert!(!before.properties.contains_key("extra"));
    store.rollback_session_transaction().unwrap();

    let now = store.edge_properties_merged(e);
    assert_eq!(now.get("k"), Some(&PropertyValue::Integer(1)));
    assert!(!now.contains_key("extra"), "{now:?}");
}

#[test]
fn rollback_undoes_label_changes_both_ways() {
    let mut store = GraphStore::new();
    let n = store.create_node("Old");
    store.begin_session_transaction().unwrap();
    store.remove_label_from_node(n, &Label::new("Old")).unwrap();
    store.add_label_to_node("default", n, "New").unwrap();
    store.rollback_session_transaction().unwrap();
    let node = store.get_node(n).unwrap();
    assert!(node.has_label(&Label::new("Old")));
    assert!(!node.has_label(&Label::new("New")));
    assert!(store
        .nodes_with_label(&Label::new("Old"))
        .unwrap()
        .contains(&n));
}

#[test]
fn rollback_removes_a_relationship_created_inside_it() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    store.begin_session_transaction().unwrap();
    let e = store.create_edge(a, b, "R").unwrap();
    store.rollback_session_transaction().unwrap();
    assert!(!store.has_edge(e));
    assert_eq!(store.edge_count(), 0);
}

// ------------------------------------------------------------------
// Buffered MVCC transactions
// ------------------------------------------------------------------

#[test]
fn txn_set_node_property_validates_value_node_and_state() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    let bad = PropertyValue::Array(vec![PropertyValue::Map(HashMap::new())]);
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    assert!(matches!(
        store.txn_set_node_property(t, n, "m", bad),
        Err(GraphError::ConstraintViolation(_))
    ));
    assert_eq!(
        store.txn_set_node_property(t, NodeId::new(90), "k", 1i64),
        Err(GraphError::NodeNotFound(NodeId::new(90)))
    );
    assert_eq!(
        store.txn_set_node_property(999, n, "k", 1i64),
        Err(GraphError::TransactionNotFound(999))
    );
    store.abort_transaction(t).unwrap();
    assert_eq!(
        store.txn_set_node_property(t, n, "k", 1i64),
        Err(GraphError::TransactionNotActive(t))
    );
    assert_eq!(
        store.txn_create_node(t, [Label::new("P")]),
        Err(GraphError::TransactionNotActive(t))
    );
    assert_eq!(
        store.abort_transaction(t),
        Err(GraphError::TransactionNotActive(t))
    );
    assert_eq!(
        store.abort_transaction(12345),
        Err(GraphError::TransactionNotFound(12345))
    );
    assert_eq!(
        store.commit_transaction(t),
        Err(GraphError::TransactionNotActive(t))
    );
}

#[test]
fn txn_reads_its_own_buffered_writes_and_creations() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    store.set_node_property("default", n, "gone", 1i64).unwrap();
    let t = store.begin_transaction(IsolationLevel::ReadCommitted);
    store.txn_set_node_property(t, n, "k", "v").unwrap();
    store
        .txn_set_node_property(t, n, "gone", PropertyValue::Null)
        .unwrap();
    let created = store.txn_create_node(t, [Label::new("New")]).unwrap();
    store.txn_set_node_property(t, created, "x", 5i64).unwrap();

    let seen = store.get_node_for_txn(t, n).unwrap();
    assert_eq!(seen.properties.get("k"), Some(&s("v")));
    assert!(!seen.properties.contains_key("gone"));
    let fresh = store.get_node_for_txn(t, created).unwrap();
    assert!(fresh.has_label(&Label::new("New")));
    assert_eq!(fresh.properties.get("x"), Some(&PropertyValue::Integer(5)));
    // Nobody else sees them.
    assert!(store.get_node(created).is_none());
    assert_eq!(store.node_property(n, "k"), None);
    assert!(store.get_node_for_txn(999, n).is_none());
    assert!(store.get_edge_for_txn(999, EdgeId::new(1)).is_none());

    let v = store.commit_transaction(t).unwrap();
    assert_eq!(v, 2);
    assert_eq!(store.node_property(n, "k"), Some(s("v")));
    assert_eq!(store.node_property(n, "gone"), None);
    assert_eq!(
        store.node_property(created, "x"),
        Some(PropertyValue::Integer(5))
    );
    assert!(store
        .get_node(created)
        .unwrap()
        .has_label(&Label::new("New")));
}

#[test]
fn a_failed_commit_puts_everything_back() {
    let mut store = GraphStore::new();
    let label = Label::new("U");
    let holder = store.create_node("U");
    store
        .set_node_property("default", holder, "email", "taken")
        .unwrap();
    store.create_unique_constraint(&label, "email").unwrap();
    let other = store.create_node("U");
    store
        .set_node_property("default", other, "name", "before")
        .unwrap();
    let nodes_before = store.node_count();

    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let created = store.txn_create_node(t, [label.clone()]).unwrap();
    store
        .txn_set_node_property(t, other, "name", "after")
        .unwrap();
    store
        .txn_set_node_property(t, other, "email", "taken")
        .unwrap();
    let err = store.commit_transaction(t).unwrap_err();
    assert!(matches!(err, GraphError::ConstraintViolation(_)), "{err:?}");

    assert_eq!(store.active_transactions[&t].status, TxnStatus::Aborted);
    assert_eq!(
        store.node_count(),
        nodes_before,
        "created node removed again"
    );
    assert!(store.get_node(created).is_none());
    assert_eq!(store.node_property(other, "name"), Some(s("before")));
    assert_eq!(store.node_property(other, "email"), None);
}

#[test]
fn a_commit_removing_a_property_from_a_deleted_node_fails() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    store
        .txn_set_node_property(t, n, "k", PropertyValue::Null)
        .unwrap();
    store.delete_node("default", n).unwrap();
    assert_eq!(
        store.commit_transaction(t),
        Err(GraphError::NodeNotFound(n))
    );
}

#[test]
fn abort_returns_reserved_ids_to_the_free_list() {
    let mut store = GraphStore::new();
    let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
    let reserved = store.txn_create_node(t, [Label::new("P")]).unwrap();
    store.abort_transaction(t).unwrap();
    assert!(store.get_node(reserved).is_none());
    assert_eq!(
        store.create_node("Q"),
        reserved,
        "the reserved id is reused"
    );
}

#[test]
fn txn_write_set_on_an_unknown_transaction_is_ignored() {
    let mut store = GraphStore::new();
    store.txn_write_node(42, NodeId::new(1));
    store.txn_write_edge(42, EdgeId::new(1));
    assert!(store.active_transactions.is_empty());
}

// ------------------------------------------------------------------
// Rebuilding the edge-type index after a stub load
// ------------------------------------------------------------------

#[test]
fn rebuild_edge_type_index_skips_deleted_edges() {
    let mut store = GraphStore::new();
    let a = store.create_node_stub("P");
    let b = store.create_node_stub("P");
    let keep = store.create_edge_stub(a, b, "R").unwrap();
    let gone = store.create_edge(a, b, "R").unwrap();
    store.delete_edge(gone).unwrap();
    store.rebuild_edge_type_index();
    assert_eq!(store.edge_type_count(&EdgeType::new("R")), 1);
    assert_eq!(store.get_edges_by_type(&EdgeType::new("R"))[0].id, keep);
}

#[test]
fn rebuild_edge_type_index_skips_an_out_of_table_type_id() {
    let mut store = GraphStore::new();
    let a = store.create_node_stub("P");
    let e = store.create_edge_stub(a, a, "R").unwrap();
    // Corrupt the compact type to an id with no table entry.
    store.edge_type_ids[e.as_u64() as usize] = 7;
    store.rebuild_edge_type_index();
    assert!(store.all_edge_types().is_empty());
}

// ------------------------------------------------------------------
// Label-index scans
// ------------------------------------------------------------------

#[test]
fn node_ids_by_label_limit_zero_and_unknown_label() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let label = Label::new("P");
    assert!(store.node_ids_by_label(&label, Some(0)).is_empty());
    assert!(store.node_ids_by_label(&Label::new("Q"), None).is_empty());
    assert_eq!(store.node_ids_by_label(&label, Some(1)), vec![a]);
    assert_eq!(store.node_ids_by_label(&label, None), vec![a, b]);
    assert_eq!(store.label_index_ids(&label).map(|s| s.len()), Some(2));
    let bits = store.label_bitset(&label).unwrap();
    assert!(GraphStore::bitset_contains(&bits, a));
    assert!(!GraphStore::bitset_contains(&bits, NodeId::new(10_000)));
}

#[test]
fn create_node_with_properties_reuses_a_freed_id() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    store.delete_node("default", a).unwrap();
    let b = store.create_node_with_properties(
        "default",
        vec![Label::new("P")],
        props(&[("k", s("v"))]),
    );
    assert_eq!(a, b);
    assert_eq!(store.node_property(b, "k"), Some(s("v")));
    let c = store.create_node_stub("P");
    store.delete_node("default", c).unwrap();
    assert_eq!(store.create_node_stub("P"), c);
}

// ------------------------------------------------------------------
// Further edge cases
// ------------------------------------------------------------------

#[test]
fn selectivity_without_most_common_values_is_the_uniform_estimate() {
    let label = Label::new("P");
    let mut property_stats = HashMap::new();
    property_stats.insert(
        (label.clone(), "k".to_string()),
        PropertyStats {
            null_fraction: 0.0,
            distinct_count: 4,
            selectivity: 0.25,
            most_common: vec![],
        },
    );
    let stats = GraphStatistics {
        total_nodes: 4,
        total_edges: 0,
        label_counts: HashMap::new(),
        edge_type_counts: HashMap::new(),
        avg_out_degree: 0.0,
        property_stats,
    };
    assert_eq!(
        stats.estimate_equality_selectivity_for_value(&label, "k", &s("x")),
        0.25
    );
    assert_eq!(
        stats.estimate_equality_selectivity_for_value(&label, "other", &s("x")),
        0.1
    );
}

#[test]
fn a_constrained_write_to_a_missing_node_is_not_found() {
    let mut store = GraphStore::new();
    let label = Label::new("U");
    let a = store.create_node("U");
    store
        .set_node_property("default", a, "email", "a@x")
        .unwrap();
    store.create_unique_constraint(&label, "email").unwrap();
    let missing = NodeId::new(999);
    assert_eq!(
        store.set_node_property("default", missing, "email", "b@x"),
        Err(GraphError::NodeNotFound(missing))
    );
}

#[test]
fn create_unique_constraint_skips_a_null_held_in_the_row() {
    let mut store = GraphStore::new();
    let label = Label::new("U");
    for _ in 0..2 {
        let n = store.create_node("U");
        store
            .get_node_mut(n)
            .unwrap()
            .set_property("email", PropertyValue::Null);
    }
    assert_eq!(store.create_unique_constraint(&label, "email"), Ok(0));
}

#[test]
fn schema_summary_skips_a_type_whose_edges_were_all_deleted() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let gone = store.create_edge(a, b, "GONE").unwrap();
    store.create_edge(a, b, "KEPT").unwrap();
    store.delete_edge(gone).unwrap();
    let summary = store.schema_summary();
    assert!(summary.contains("(A)-[:KEPT]->(B)"), "{summary}");
    assert!(!summary.contains("GONE"), "{summary}");
}

#[test]
fn create_fulltext_index_backfills_only_string_values() {
    let mut store = GraphStore::new();
    let a = store.create_node("Doc");
    let b = store.create_node("Doc");
    store
        .set_node_property("default", a, "body", "graph text")
        .unwrap();
    store
        .set_node_property("default", b, "body", 42i64)
        .unwrap();
    assert_eq!(store.create_fulltext_index("ft", "Doc", "body"), 1);
}

#[test]
fn node_properties_full_merges_row_and_column_with_the_column_winning() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    store
        .get_node_mut(n)
        .unwrap()
        .set_property("row_only", 1i64);
    store.get_node_mut(n).unwrap().set_property("both", "row");
    store.set_column_property(n, "both", s("column"));
    let full = store.node_properties_full(n);
    assert_eq!(full.get("row_only"), Some(&PropertyValue::Integer(1)));
    assert_eq!(full.get("both"), Some(&s("column")));
    assert!(store.node_properties_full(NodeId::new(404)).is_empty());
}

#[test]
fn a_recovered_edge_keeps_its_properties() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let mut edge = Edge::new(EdgeId::new(40), a, b, "R");
    edge.properties
        .insert("w".to_string(), PropertyValue::Integer(3));
    store.insert_recovered_edge(edge).unwrap();
    assert_eq!(
        store.edge_property(EdgeId::new(40), "w"),
        Some(PropertyValue::Integer(3))
    );
    assert_eq!(
        store.create_edge(a, b, "R").unwrap(),
        EdgeId::new(41),
        "ids continue past it"
    );
}

#[test]
fn a_read_committed_transaction_reads_the_latest_edge() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let e = store.create_edge(a, a, "R").unwrap();
    let t = store.begin_transaction(IsolationLevel::ReadCommitted);
    store.set_edge_property(e, "k", 1i64).unwrap();
    let seen = store.get_edge_for_txn(t, e).unwrap();
    assert_eq!(seen.properties.get("k"), Some(&PropertyValue::Integer(1)));
}

#[test]
fn a_failed_commit_restores_every_key_whatever_order_it_applied_them_in() {
    // The buffered writes are a HashMap, so the order they are applied in, and
    // therefore how much is applied before the failing one, varies per store.
    for _ in 0..32 {
        let mut store = GraphStore::new();
        let label = Label::new("U");
        let holder = store.create_node("U");
        store
            .set_node_property("default", holder, "email", "taken")
            .unwrap();
        store.create_unique_constraint(&label, "email").unwrap();
        let other = store.create_node("U");
        store
            .set_node_property("default", other, "name", "before")
            .unwrap();

        let t = store.begin_transaction(IsolationLevel::SnapshotIsolation);
        store
            .txn_set_node_property(t, other, "fresh", 1i64)
            .unwrap();
        store
            .txn_set_node_property(t, other, "name", "after")
            .unwrap();
        store
            .txn_set_node_property(t, other, "email", "taken")
            .unwrap();
        assert!(store.commit_transaction(t).is_err());
        assert_eq!(store.node_property(other, "fresh"), None);
        assert_eq!(store.node_property(other, "name"), Some(s("before")));
        assert_eq!(store.node_property(other, "email"), None);
    }
}

#[test]
fn gc_drops_the_birth_record_of_an_old_relationship() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    store.begin_session_transaction().unwrap();
    let e = store.create_edge(a, a, "R").unwrap();
    store.commit_session_transaction().unwrap();
    assert!(
        store.get_edge_at_version(e, 1).is_none(),
        "born at version 2"
    );
    assert!(!store.edge_history.is_empty());
    store.gc_versions(2);
    assert!(store.edge_history.is_empty());
    assert!(
        store.get_edge_at_version(e, 1).is_some(),
        "history below the watermark is gone"
    );
}

#[tokio::test]
async fn auto_embed_into_an_index_of_the_wrong_dimension_stores_nothing() {
    let tenants = Arc::new(crate::persistence::TenantManager::new());
    tenants
        .update_embed_config(
            "default",
            Some(mock_embed_config(&[("Bad", "text"), ("Doc", "text")])),
        )
        .unwrap();
    let vector_index = Arc::new(VectorIndexManager::new());
    vector_index
        .create_index("Bad", "embedding", 3, DistanceMetric::Cosine)
        .unwrap();
    vector_index
        .create_index("Doc", "embedding", 64, DistanceMetric::Cosine)
        .unwrap();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(IndexEvent::NodeCreated {
        tenant_id: "default".into(),
        id: NodeId::new(5),
        labels: vec![Label::new("Bad"), Label::new("Doc")],
        properties: props(&[("text", s("some words"))]),
    })
    .unwrap();
    drop(tx);
    GraphStore::start_background_indexer(
        rx,
        vector_index.clone(),
        Arc::new(IndexManager::new()),
        tenants,
    )
    .await;
    let vi = vector_index.clone();
    let doc_done = eventually(move || {
        vi.get_index("Doc", "embedding")
            .map(|i| i.read().unwrap().len())
            .unwrap_or(0)
            == 1
    })
    .await;
    assert!(doc_done);
    assert_eq!(
        vector_index
            .get_index("Bad", "embedding")
            .unwrap()
            .read()
            .unwrap()
            .len(),
        0
    );
}
