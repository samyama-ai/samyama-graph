//! The Cypher an export emits rebuilds the graph it came from (INT-07).
//!
//! INT-07 asks for a published "how to leave Samyama" guide **with tested
//! steps**. A guide whose steps nobody runs is the same promise the spec
//! already made and the repository did not keep, so the step that matters is
//! tested here: feed the emitted script back in and compare the graphs.
//!
//! **Re-importing into Samyama is not proof that Neo4j will accept it** — it
//! proves the script is valid Cypher in *our* dialect and that it reconstructs
//! the topology and the property values. That is the part we can test in CI,
//! and it is stated in the guide rather than implied to be more.

use std::collections::BTreeSet;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::QueryEngine;

#[path = "../examples/export_cypher.rs"]
mod export_cypher;

fn build(statements: &[&str]) -> GraphStore {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for q in statements {
        engine.execute_mut(q, &mut store, "default").unwrap_or_else(|e| panic!("{q}: {e}"));
    }
    store
}

/// Re-run the emitted script against a fresh store.
fn round_trip(store: &GraphStore) -> GraphStore {
    let mut script = Vec::new();
    export_cypher::write_cypher(store, &mut script).expect("write");
    let text = String::from_utf8(script).expect("utf-8");

    let mut restored = GraphStore::new();
    let engine = QueryEngine::new();
    for stmt in text.split(";\n") {
        // Comments and the blank tail are not statements.
        let cleaned: String = stmt
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        if cleaned.trim().is_empty() {
            continue;
        }
        engine
            .execute_mut(cleaned.trim(), &mut restored, "default")
            .unwrap_or_else(|e| panic!("the emitted script does not run: {e}\n{cleaned}"));
    }
    restored
}

/// Every (sorted labels, sorted properties) pair, as comparable text.
fn node_shapes(store: &GraphStore) -> BTreeSet<String> {
    store
        .all_nodes()
        .iter()
        .map(|n| {
            let mut labels: Vec<&str> = n.labels.iter().map(|l| l.as_str()).collect();
            labels.sort_unstable();
            let mut props: Vec<String> = store
                .node_properties_merged(n.id)
                .into_iter()
                // The scaffolding is removed by the script's last statement, so
                // it must not be here; a test that filtered it would hide that.
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect();
            props.sort();
            format!("{}|{}", labels.join(":"), props.join(","))
        })
        .collect()
}

/// Every (source labels, type, target labels) triple with edge properties.
fn edge_shapes(store: &GraphStore) -> BTreeSet<String> {
    store
        .all_edges()
        .iter()
        .map(|e| {
            let label_of = |id| {
                store
                    .get_node(id)
                    .map(|n| {
                        let mut l: Vec<&str> = n.labels.iter().map(|x| x.as_str()).collect();
                        l.sort_unstable();
                        l.join(":")
                    })
                    .unwrap_or_default()
            };
            let mut props: Vec<String> = store
                .edge_properties_merged(e.id)
                .into_iter()
                .map(|(k, v)| format!("{k}={v:?}"))
                .collect();
            props.sort();
            format!(
                "{}-[{}{}]->{}",
                label_of(e.source),
                e.edge_type.as_str(),
                props.join(","),
                label_of(e.target)
            )
        })
        .collect()
}

#[test]
fn a_graph_survives_the_round_trip() {
    let original = build(&[
        "CREATE (:Person {name: 'Alice', age: 30, active: true})",
        "CREATE (:Person {name: 'Bob', age: 25, score: 1.5})",
        "CREATE (:Company:Employer {name: 'Acme'})",
        "MATCH (a:Person {name: 'Alice'}), (c:Company) CREATE (a)-[:WORKS_AT {since: 2019}]->(c)",
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    ]);
    let restored = round_trip(&original);

    assert_eq!(restored.node_count(), original.node_count(), "node count");
    assert_eq!(restored.edge_count(), original.edge_count(), "edge count");
    assert_eq!(node_shapes(&restored), node_shapes(&original), "nodes differ");
    assert_eq!(edge_shapes(&restored), edge_shapes(&original), "edges differ");
}

#[test]
fn the_scaffolding_does_not_survive() {
    // `_sgid` and `:_Imported` are ours, not the user's data. If the last
    // statement of the script stops working, every node in the restored graph
    // carries a property the original did not.
    let original = build(&["CREATE (:Person {name: 'Alice'})"]);
    let restored = round_trip(&original);
    for n in restored.all_nodes() {
        let props = restored.node_properties_merged(n.id);
        assert!(
            !props.contains_key(export_cypher::ID_PROPERTY),
            "the import scaffolding is still on the node: {props:?}"
        );
        assert!(
            !n.labels.iter().any(|l| l.as_str() == "_Imported"),
            "the import label is still on the node"
        );
    }
}

#[test]
fn a_float_that_is_a_whole_number_stays_a_float() {
    // `2.0` written as `2` reads back as an integer, which changes the
    // property's type across the move without changing its value -- the sort
    // of difference that surfaces three queries later.
    let original = build(&["CREATE (:M {score: 2.0})"]);
    let restored = round_trip(&original);
    let n = restored.all_nodes()[0].id;
    assert!(
        matches!(
            restored.node_properties_merged(n).get("score"),
            Some(PropertyValue::Float(_))
        ),
        "score came back as {:?}",
        restored.node_properties_merged(n).get("score")
    );
}

#[test]
fn a_quote_in_a_string_does_not_break_the_script() {
    // The classic injection shape, here as a correctness problem: an unescaped
    // apostrophe ends the literal and the rest of the name becomes Cypher.
    let original = build(&["CREATE (:P {name: \"O'Brien\"})"]);
    let restored = round_trip(&original);
    let n = restored.all_nodes()[0].id;
    assert_eq!(
        restored.node_properties_merged(n).get("name"),
        Some(&PropertyValue::String("O'Brien".to_string()))
    );
}

#[test]
fn a_node_with_no_labels_still_comes_back() {
    // It has no label to hang the id index on, which is why the exporter adds
    // one of its own.
    let mut original = GraphStore::new();
    original.create_node("");
    let restored = round_trip(&original);
    assert_eq!(restored.node_count(), 1);
}

#[test]
fn an_empty_graph_produces_a_script_that_runs() {
    let restored = round_trip(&GraphStore::new());
    assert_eq!(restored.node_count(), 0);
}
