//! Everything that walks the edge types reads them in the same order on every
//! store built from the same data (#1509, the part #1519 left).
//!
//! #1519 made `get_edges_by_type` ascending by edge id, so a scan of one type
//! is reproducible. The walks *across* types were not: `all_edge_types()`
//! returned `edge_type_index.keys()`, a `HashMap` whose order is keyed per
//! instance, and three consumers exposed that order:
//!
//! * `db.schema.visualization()` emitted its rows type by type in that order,
//!   and within a type in `Node::labels` (`HashSet`) order.
//! * `GraphStore::schema_summary`, the schema text the NLQ prompt is given,
//!   sampled "the first" edge of each type straight out of its `HashSet`, so
//!   on a type joining several label pairs a different pattern was reported
//!   on each start. It also listed labels in `label_index` order.
//! * The HTTP sample-graph response listed edges type by type in that order.
//!
//! No wrong answer, in the same sense as the issue: each result was a correct
//! set in an irreproducible order (or, for `schema_summary`, a correct but
//! arbitrarily chosen sample). The signature measured here is the issue's own:
//! identical stores, distinct outputs.

use samyama::graph::{EdgeType, GraphStore, PropertyValue};
use samyama::query::executor::record::Value;
use samyama::query::QueryEngine;
use std::collections::HashSet;

const STORES: usize = 12;

/// Eight edge types; `LINK` joins eight different label pairs, and several
/// nodes carry two labels.
fn store() -> GraphStore {
    let mut s = GraphStore::new();
    let labels = ["A", "B", "C", "D", "E", "F", "G", "H"];
    let ids: Vec<_> = labels.iter().map(|l| s.create_node(*l)).collect();
    for (i, id) in ids.iter().enumerate().step_by(2) {
        s.add_label_to_node("default", *id, format!("X{i}")).expect("label");
    }
    for i in 0..labels.len() {
        s.create_edge(ids[i], ids[(i + 1) % labels.len()], "LINK").expect("edge");
    }
    for t in ["T1", "T2", "T3", "T4", "T5", "T6", "T7"] {
        s.create_edge(ids[0], ids[1], t).expect("edge");
        s.create_edge(ids[2], ids[3], t).expect("edge");
    }
    s
}

fn distinct<T: std::hash::Hash + Eq>(f: impl Fn(&mut GraphStore) -> T) -> usize {
    (0..STORES).map(|_| f(&mut store())).collect::<HashSet<_>>().len()
}

#[test]
fn all_edge_types_is_sorted_by_name() {
    let s = store();
    let got: Vec<&str> = s.all_edge_types().iter().map(|t| t.as_str()).collect();
    assert_eq!(got, ["LINK", "T1", "T2", "T3", "T4", "T5", "T6", "T7"]);
}

#[test]
fn walking_every_type_gives_one_edge_order() {
    let n = distinct(|s| {
        s.all_edge_types()
            .into_iter()
            .flat_map(|t| s.get_edges_by_type(&EdgeType::new(t.as_str())))
            .map(|e| e.id.as_u64())
            .collect::<Vec<_>>()
    });
    assert_eq!(n, 1, "{STORES} identical stores gave {n} distinct cross-type edge orders");
}

#[test]
fn schema_visualization_rows_are_the_same_on_every_store() {
    let engine = QueryEngine::new();
    let n = distinct(|s| {
        let batch = engine
            .execute_mut(
                "CALL db.schema.visualization() YIELD source_label, relationship_type, target_label \
                 RETURN source_label, relationship_type, target_label",
                s,
                "default",
            )
            .expect("query");
        batch
            .records
            .iter()
            .map(|r| {
                r.bindings()
                    .iter()
                    .map(|(_, v)| match v {
                        Value::Property(PropertyValue::String(s)) => s.clone(),
                        other => format!("{other:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(n, 1, "{STORES} identical stores gave {n} distinct db.schema.visualization() row orders");
}

#[test]
fn schema_summary_is_the_same_on_every_store() {
    let n = distinct(|s| s.schema_summary());
    assert_eq!(n, 1, "{STORES} identical stores gave {n} distinct schema summaries");
}

#[test]
fn schema_summary_samples_the_lowest_edge_of_each_type() {
    // LINK's lowest edge id is A -> B; A also carries X0, and "A" < "X0".
    let s = store().schema_summary();
    assert!(s.contains("(A)-[:LINK]->(B) (8 edges)"), "{s}");
}
