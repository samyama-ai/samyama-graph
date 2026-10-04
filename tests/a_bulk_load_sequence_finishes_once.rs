//! Importing N snapshots back to back should finish the bulk load once, not N
//! times (#1807).
//!
//! # What the cost actually is
//!
//! `import_tenant` ends in `GraphStore::finish_bulk_load()`, which does the
//! four things `create_node_stub` / `create_edge_stub` deliberately skip:
//! compact the write buffer into a frozen CSR segment, rebuild the edge-type
//! index, recompute the catalog, and rebuild the vector index. Three of those
//! read **the whole accumulated store**, not the snapshot that was just
//! imported. So the eleventh import of a 1,098-node snapshot into a 103M-node
//! store paid for 103M nodes, and cost 201.3 s.
//!
//! Measured per phase at 8M nodes / 16M edges (`examples/import_tax_probe.rs`):
//!
//! ```text
//! compact_adjacency         0.088 s    1.3%
//! rebuild_edge_type_index   1.830 s   27.1%
//! rebuild_catalog           4.836 s   71.6%
//! rebuild_vector_index      0.000 s    0.0%  (no vector index registered)
//! ```
//!
//! Compaction is not the tax. `rebuild_catalog` is, and it tracks **edges**
//! about twice as strongly as nodes per unit: holding nodes at 1M, going from
//! 1M to 4M edges took it 0.400 s -> 0.684 s; holding edges at 1M, going from
//! 1M to 4M nodes took it 0.400 s -> 0.533 s. Segment count adds about 1.6%
//! per segment at 4M nodes, which is third order.
//!
//! # What may be deferred, and what may not
//!
//! Three of the four are deferred across a sequence; the vector index is not.
//!
//! * **compaction** — a layout change only. Every adjacency read walks the
//!   frozen segments *and* the write buffer (`for_each_outgoing_neighbor`,
//!   `for_each_edge_between_typed`), so an uncompacted buffer is read in full.
//! * **edge-type index** — left short, which `edge_type_index_is_complete()`
//!   detects by comparing the indexed count against `edge_count()`. Both
//!   readers consult that guard and fall back to a scan. `create_edge_stub`
//!   already leaves it short mid-import, so this is the existing state, held
//!   for longer.
//! * **catalog** — `create_edge_stub` already calls
//!   `note_degrees_may_be_stale()`, so `degrees_are_exact_for` already says no
//!   from the first stub edge; only `rebuild_catalog` clears it. Label counts
//!   are maintained by `create_node_stub`, so they stay exact. What is left
//!   stale is the triple statistics, which are planner *estimates*: a worse
//!   plan, never a different answer.
//! * **vector index** — has no completeness guard. A vector query reads the
//!   HNSW graph, and an HNSW graph missing a node returns fewer rows with no
//!   way for the reader to know (#1467's class). So `finish_bulk_load` runs it
//!   on every import even inside a deferred sequence. It is not part of the
//!   tax: it is O(nodes carrying an embedding), and a no-op when no vector
//!   index is registered.
//!
//! A query that arrives mid-sequence is therefore correct without forcing
//! anything. `force_bulk_load_finish()` exists for a caller that wants the
//! plan quality back as well.

use samyama::graph::{EdgeType, GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::snapshot::{export_tenant, import_tenant};

/// Three snapshots with disjoint labels, each small.
fn snapshots() -> Vec<Vec<u8>> {
    [("Alpha", "A_REL", 40usize), ("Beta", "B_REL", 25), ("Gamma", "C_REL", 10)]
        .iter()
        .map(|&(label, edge_type, n)| {
            let mut src = GraphStore::new();
            let ids: Vec<_> = (0..n)
                .map(|i| {
                    let id = src.create_node(label);
                    src.set_node_property("default", id, "k", PropertyValue::Integer(i as i64)).unwrap();
                    id
                })
                .collect();
            for i in 0..n {
                src.create_edge(ids[i], ids[(i + 1) % n], edge_type).unwrap();
                src.create_edge(ids[i], ids[(i + 7) % n], edge_type).unwrap();
            }
            let mut buf = Vec::new();
            export_tenant(&src, &mut buf).expect("export");
            buf
        })
        .collect()
}

fn imported_eagerly(snaps: &[Vec<u8>]) -> GraphStore {
    let mut g = GraphStore::new();
    for s in snaps {
        import_tenant(&mut g, &s[..]).expect("import");
    }
    g
}

fn imported_deferred(snaps: &[Vec<u8>]) -> GraphStore {
    let mut g = GraphStore::new();
    g.begin_deferred_bulk_load();
    for s in snaps {
        import_tenant(&mut g, &s[..]).expect("import");
    }
    g.end_deferred_bulk_load();
    g
}

fn scalar(store: &GraphStore, cypher: &str, col: &str) -> Vec<Value> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}` should parse: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}` should run: {e}"))
        .records
        .iter()
        .map(|r| r.get(col).cloned().unwrap_or(Value::Null))
        .collect()
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    match scalar(store, cypher, "c").first() {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("`{cypher}` should return one integer, got {other:?}"),
    }
}

/// (a) N imports, one finish.
#[test]
fn three_back_to_back_imports_finish_the_bulk_load_once() {
    let snaps = snapshots();

    let eager = imported_eagerly(&snaps);
    assert_eq!(
        eager.bulk_load_finish_count(),
        3,
        "eagerly, each import pays for the whole store"
    );
    assert_eq!(
        eager.adjacency_stats().frozen_segments,
        3,
        "and leaves one frozen segment per import"
    );

    let deferred = imported_deferred(&snaps);
    assert_eq!(
        deferred.bulk_load_finish_count(),
        1,
        "deferred, the sequence pays once"
    );
    assert_eq!(
        deferred.adjacency_stats().frozen_segments,
        1,
        "one compaction, so one segment"
    );
}

/// (b) The same graph either way.
#[test]
fn a_deferred_sequence_yields_an_identical_graph() {
    let snaps = snapshots();
    let eager = imported_eagerly(&snaps);
    let deferred = imported_deferred(&snaps);

    assert_eq!(eager.node_count(), deferred.node_count(), "node count");
    assert_eq!(eager.edge_count(), deferred.edge_count(), "edge count");

    let mut a: Vec<String> = eager.all_labels().iter().map(|l| l.as_str().to_string()).collect();
    let mut b: Vec<String> = deferred.all_labels().iter().map(|l| l.as_str().to_string()).collect();
    a.sort();
    b.sort();
    assert_eq!(a, b, "label sets");

    for label in ["Alpha", "Beta", "Gamma"] {
        assert_eq!(
            eager.get_nodes_by_label(&Label::new(label)).len(),
            deferred.get_nodes_by_label(&Label::new(label)).len(),
            "{label} count"
        );
    }
    // Absolute, not just equal: `get_edges_by_type` returned an empty vector
    // for a short index before this change, and 0 == 0 would have passed.
    for (t, expected) in [("A_REL", 80usize), ("B_REL", 50), ("C_REL", 20)] {
        assert_eq!(eager.get_edges_by_type(&EdgeType::new(t)).len(), expected, "eager {t}");
        assert_eq!(
            deferred.get_edges_by_type(&EdgeType::new(t)).len(),
            expected,
            "deferred {t}"
        );
    }
    assert_eq!(eager.node_count(), 75, "40 + 25 + 10");
    assert_eq!(eager.edge_count(), 150);

    // The answers, not just the counts.
    for cypher in [
        "MATCH (n) RETURN count(n) AS c",
        "MATCH ()-[r]->() RETURN count(r) AS c",
        "MATCH (a:Alpha)-[:A_REL]->(b:Alpha) RETURN count(b) AS c",
        "MATCH (a:Beta)-[:B_REL]->()-[:B_REL]->(c2) RETURN count(c2) AS c",
        "MATCH (a:Gamma {k: 3})-[:C_REL]->(b) RETURN count(b) AS c",
        "MATCH (a:Alpha)-[:A_REL]->(b) WHERE b.k > 20 RETURN count(b) AS c",
    ] {
        assert_eq!(count(&eager, cypher), count(&deferred, cypher), "`{cypher}`");
    }

    // Per-node degree, which is the read `degrees_are_exact_for` guards.
    let mut deg_e: Vec<usize> = eager
        .get_nodes_by_label(&Label::new("Alpha"))
        .iter()
        .map(|n| eager.get_outgoing_edge_targets(n.id).len())
        .collect();
    let mut deg_d: Vec<usize> = deferred
        .get_nodes_by_label(&Label::new("Alpha"))
        .iter()
        .map(|n| deferred.get_outgoing_edge_targets(n.id).len())
        .collect();
    deg_e.sort();
    deg_d.sort();
    assert_eq!(deg_e, deg_d, "out-degree multiset");
}

/// (c) A query between two imports of a deferred sequence.
#[test]
fn a_query_between_deferred_imports_reads_the_truth() {
    let snaps = snapshots();
    let mut g = GraphStore::new();
    g.begin_deferred_bulk_load();

    import_tenant(&mut g, &snaps[0][..]).expect("import 1");
    import_tenant(&mut g, &snaps[1][..]).expect("import 2");

    // Nothing has been compacted, no index has been rebuilt, and the catalog
    // still holds whatever the first import's stubs left. Every one of these
    // must already be right.
    assert_eq!(g.bulk_load_finish_count(), 0, "nothing finished yet");
    assert_eq!(count(&g, "MATCH (n) RETURN count(n) AS c"), 65, "nodes after two imports");
    assert_eq!(count(&g, "MATCH ()-[r]->() RETURN count(r) AS c"), 130, "edges");
    assert_eq!(count(&g, "MATCH (a:Alpha)-[:A_REL]->(b) RETURN count(b) AS c"), 80);
    assert_eq!(count(&g, "MATCH (a:Beta)-[:B_REL]->(b) RETURN count(b) AS c"), 50);
    assert_eq!(count(&g, "MATCH (a:Alpha)-[:B_REL]->(b) RETURN count(b) AS c"), 0,
        "a type that does not join these labels");
    assert_eq!(
        g.get_edges_by_type(&EdgeType::new("A_REL")).len(),
        80,
        "the edge-type read falls back to a scan while the index is short"
    );
    assert_eq!(g.get_nodes_by_label(&Label::new("Beta")).len(), 25);

    // A forced finish mid-sequence changes no answer, and the sequence stays
    // deferred afterwards.
    g.force_bulk_load_finish();
    assert_eq!(g.bulk_load_finish_count(), 1);
    assert_eq!(count(&g, "MATCH (n) RETURN count(n) AS c"), 65);
    assert_eq!(count(&g, "MATCH ()-[r]->() RETURN count(r) AS c"), 130);

    import_tenant(&mut g, &snaps[2][..]).expect("import 3");
    assert_eq!(g.bulk_load_finish_count(), 1, "still deferred after the force");
    g.end_deferred_bulk_load();
    assert_eq!(g.bulk_load_finish_count(), 2);

    let eager = imported_eagerly(&snaps);
    assert_eq!(g.node_count(), eager.node_count());
    assert_eq!(g.edge_count(), eager.edge_count());
}

/// (d) The sibling guard class: the vector index has no completeness guard, so
/// it is never deferred. A vector query mid-sequence must answer.
#[test]
fn a_deferred_sequence_never_defers_the_vector_index() {
    let mut src = GraphStore::new();
    for i in 0..8 {
        let id = src.create_node("Doc");
        src.set_node_property(
            "default",
            id,
            "embedding",
            PropertyValue::Vector(vec![i as f32, 1.0 - i as f32, 0.5]),
        )
        .unwrap();
    }
    let mut buf = Vec::new();
    export_tenant(&src, &mut buf).expect("export");

    let mut g = GraphStore::new();
    g.begin_deferred_bulk_load();
    import_tenant(&mut g, &buf[..]).expect("import");
    // Mid-sequence, before any end_deferred_bulk_load.
    assert_eq!(g.bulk_load_finish_count(), 0, "the deferred three have not run");
    assert_eq!(
        g.rebuild_vector_index_full(),
        1,
        "the Doc/embedding index is discoverable mid-sequence"
    );
    let hits = g
        .vector_index
        .search("Doc", "embedding", &[0.0, 1.0, 0.5], 4)
        .expect("vector search");
    assert_eq!(hits.len(), 4, "a vector search mid-sequence returns rows");
    g.end_deferred_bulk_load();
    assert_eq!(g.bulk_load_finish_count(), 1);
}

/// An unbalanced `begin` must not wedge the store, and a plain import outside
/// any sequence must still finish eagerly.
#[test]
fn deferral_nests_and_a_plain_import_still_finishes_eagerly() {
    let snaps = snapshots();

    let mut g = GraphStore::new();
    import_tenant(&mut g, &snaps[0][..]).expect("import");
    assert_eq!(g.bulk_load_finish_count(), 1, "no sequence, so eager");

    g.begin_deferred_bulk_load();
    g.begin_deferred_bulk_load();
    import_tenant(&mut g, &snaps[1][..]).expect("import");
    g.end_deferred_bulk_load();
    assert_eq!(g.bulk_load_finish_count(), 1, "the inner end does not finish");
    g.end_deferred_bulk_load();
    assert_eq!(g.bulk_load_finish_count(), 2, "the outer end does");

    // An end with no begin is a no-op, not a panic and not a finish.
    g.end_deferred_bulk_load();
    assert_eq!(g.bulk_load_finish_count(), 2);

    // Nothing owed: ending a sequence that imported nothing finishes nothing.
    g.begin_deferred_bulk_load();
    g.end_deferred_bulk_load();
    assert_eq!(g.bulk_load_finish_count(), 2);
}
