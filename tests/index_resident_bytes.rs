//! Every index kind reports its resident bytes, and the vector index counts its
//! HNSW graph (#1660's sibling, NDS-14 / #1673).
//!
//! Two of four index kinds reported memory: the property index and the
//! hierarchy index. `FullTextIndex` had no method at all, and the vector index
//! counted its vectors and not the navigable graph built over them -- at the
//! default `M = 16` the larger share for small dimensions. `GraphStore::
//! index_memory` now reports all of them, and the HTTP memory and metrics
//! endpoints read it.
//!
//! The counts are estimates (a floor: allocator slack is invisible), so these
//! tests assert relations that hold for any honest estimate -- present, growing
//! with what is indexed, graph larger than nothing -- never exact byte figures.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;
use samyama::vector::DistanceMetric;

fn run(store: &mut GraphStore, q: &str) {
    MutQueryExecutor::new(store, "default".into())
        .execute(&parse_query(q).unwrap())
        .unwrap_or_else(|e| panic!("{q}: {e}"));
}

fn store_with(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    s.create_vector_index("Doc", "emb", 8, DistanceMetric::Cosine).unwrap();
    run(&mut s, "CREATE INDEX ON :Doc(id)");
    run(&mut s, "CREATE FULLTEXT INDEX docs FOR (n:Doc) ON (n.body)");
    for i in 0..n {
        let mut props = std::collections::HashMap::new();
        props.insert("id".to_string(), PropertyValue::Integer(i as i64));
        props.insert(
            "body".to_string(),
            PropertyValue::String(format!("graph engine document number {i} about indexes")),
        );
        let v: Vec<f32> = (0..8).map(|d| ((i * 7 + d * 3) % 11) as f32 + 0.5).collect();
        props.insert("emb".to_string(), PropertyValue::Vector(v));
        s.create_node_with_properties("default", vec![Label::new("Doc")], props);
    }
    s
}

fn bytes_of(store: &GraphStore, kind: &str) -> (usize, usize) {
    let row = store
        .index_memory()
        .into_iter()
        .find(|r| r.kind == kind)
        .unwrap_or_else(|| panic!("no {kind} row in {:?}", store.index_memory()));
    (row.bytes, row.entries)
}

#[test]
fn every_index_kind_reports_its_bytes() {
    let s = store_with(200);
    for kind in ["index", "fulltext", "vector"] {
        let (bytes, entries) = bytes_of(&s, kind);
        assert!(bytes > 0, "{kind} reports no bytes");
        assert!(entries > 0, "{kind} reports no entries");
    }
    let fulltext = s.index_memory().into_iter().find(|r| r.kind == "fulltext").unwrap();
    assert_eq!(fulltext.name.as_deref(), Some("docs"));
}

#[test]
fn the_byte_counts_grow_with_what_is_indexed() {
    let small = store_with(50);
    let large = store_with(500);
    for kind in ["index", "fulltext", "vector"] {
        let (a, _) = bytes_of(&small, kind);
        let (b, _) = bytes_of(&large, kind);
        assert!(b > a, "{kind}: {b} bytes for 500 documents is not more than {a} for 50");
    }
}

/// A node created with its properties through `create_node_with_properties`
/// is in the full-text index. It was not: the full-text index was fed on the
/// property-set path only, which this call does not take, so these 500
/// documents counted zero bytes because they were never indexed.
#[test]
fn a_node_created_with_its_properties_is_full_text_searchable() {
    let s = store_with(20);
    let hits = s.fulltext.search("docs", "number", 50).expect("the index exists");
    assert_eq!(hits.len(), 20, "{hits:?}");
}

#[test]
fn the_vector_index_counts_its_hnsw_graph_not_only_its_vectors() {
    let s = store_with(300);
    let idx = s.vector_index.get_index("Doc", "emb").expect("vector index");
    let idx = idx.read().unwrap();
    let graph = idx.graph_bytes();
    assert!(graph > 0, "the HNSW graph reports no bytes over 300 points");
    assert!(
        idx.resident_bytes() >= idx.vector_bytes() + graph,
        "resident {} is less than vectors {} plus graph {graph}",
        idx.resident_bytes(),
        idx.vector_bytes()
    );
    let (reported, entries) = bytes_of(&s, "vector");
    assert_eq!(reported, idx.resident_bytes());
    assert_eq!(entries, 300);
}
