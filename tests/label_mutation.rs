//! `SET n:Label` and `REMOVE n:Label` (#596).
//!
//! `SET n:Label` did not parse. `REMOVE n:Label` parsed, reported a successful
//! write, and **did nothing** — the planner matched only
//! `RemoveItem::Property` and dropped the label variant on the floor while
//! still marking the statement as a write. So the second was the same silent
//! class as #594, not the honest gap the issue took it for.
//!
//! Both now go through `GraphStore`, which maintains `label_index`. That is the
//! part with teeth: a label added to the node but not to the index is invisible
//! to `MATCH (n:Label)` **and**, since #592, to expansion filtering — invisible
//! to exactly the queries that look for it. Every test here checks the index as
//! well as `labels()`.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph() -> (GraphStore, samyama::graph::NodeId) {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let _ = store.set_node_property("default", a, "name".to_string(), PropertyValue::String("Ada".into()));
    let b = store.create_node("P");
    let _ = store.set_node_property("default", b, "name".to_string(), PropertyValue::String("Bob".into()));
    store.create_edge(a, b, "KNOWS").unwrap();
    (store, a)
}

fn run(store: &mut GraphStore, cypher: &str) {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut mutating = MutQueryExecutor::new(store, "default".to_string());
    mutating.execute(&query).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let query = parse_query(cypher).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records.first().and_then(|r| r.get("r")) {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        None => 0,
        other => panic!("{other:?}"),
    }
}

fn labels_of(store: &GraphStore, name: &str) -> Vec<String> {
    let query = parse_query(&format!("MATCH (p) WHERE p.name = \"{name}\" RETURN labels(p) AS r")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("r") {
        Some(Value::Property(PropertyValue::Array(items))) => {
            let mut v: Vec<String> = items
                .iter()
                .map(|p| match p {
                    PropertyValue::String(s) => s.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            v.sort();
            v
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn set_adds_a_label() {
    let (mut store, a) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin");

    assert_eq!(labels_of(&store, "Ada"), vec!["Admin", "P"]);
    assert!(
        store.nodes_with_label(&Label::new("Admin")).map(|s| s.contains(&a)).unwrap_or(false),
        "the label index was not updated, so MATCH (n:Admin) cannot find it"
    );
    assert_eq!(count(&store, "MATCH (p:Admin) RETURN count(p) AS r"), 1);
}

#[test]
fn set_adds_several_labels() {
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin:Archived");
    assert_eq!(labels_of(&store, "Ada"), vec!["Admin", "Archived", "P"]);
    assert_eq!(count(&store, "MATCH (p:Admin) RETURN count(p) AS r"), 1);
    assert_eq!(count(&store, "MATCH (p:Archived) RETURN count(p) AS r"), 1);
}

#[test]
fn remove_takes_a_label_away() {
    let (mut store, a) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin");
    run(&mut store, "MATCH (p:Admin) REMOVE p:Admin");

    assert_eq!(labels_of(&store, "Ada"), vec!["P"]);
    assert!(
        !store.nodes_with_label(&Label::new("Admin")).map(|s| s.contains(&a)).unwrap_or(false),
        "the node is still in the index for a label it no longer carries"
    );
    assert_eq!(count(&store, "MATCH (p:Admin) RETURN count(p) AS r"), 0);
}

#[test]
fn the_original_label_is_untouched() {
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin");
    run(&mut store, "MATCH (p:Admin) REMOVE p:Admin");
    assert_eq!(count(&store, "MATCH (p:P) RETURN count(p) AS r"), 2, "both P nodes remain");
}

#[test]
fn removing_a_label_the_node_does_not_have_is_a_no_op() {
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" REMOVE p:NotThere");
    assert_eq!(labels_of(&store, "Ada"), vec!["P"]);
}

#[test]
fn only_the_matched_node_changes() {
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin");
    assert_eq!(labels_of(&store, "Bob"), vec!["P"], "Bob should not have been labelled");
    assert_eq!(count(&store, "MATCH (p:Admin) RETURN count(p) AS r"), 1);
}

#[test]
fn a_label_added_is_visible_to_an_expansion_filter() {
    // Since #592, expansion filters read `label_index` rather than asking the
    // node. A label added without updating the index would be invisible here
    // while `labels()` still showed it — the sharpest form of an index that has
    // drifted.
    let (mut store, _) = graph();
    assert_eq!(count(&store, "MATCH (a:P)-[:KNOWS]->(b:Admin) RETURN count(b) AS r"), 0);
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Bob\" SET p:Admin");
    assert_eq!(
        count(&store, "MATCH (a:P)-[:KNOWS]->(b:Admin) RETURN count(b) AS r"),
        1,
        "the expansion filter cannot see the new label"
    );
}

#[test]
fn a_label_removed_is_hidden_from_an_expansion_filter() {
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Bob\" SET p:Admin");
    assert_eq!(count(&store, "MATCH (a:P)-[:KNOWS]->(b:Admin) RETURN count(b) AS r"), 1);
    run(&mut store, "MATCH (p:Admin) REMOVE p:Admin");
    assert_eq!(
        count(&store, "MATCH (a:P)-[:KNOWS]->(b:Admin) RETURN count(b) AS r"),
        0,
        "the expansion filter still sees a removed label"
    );
}

#[test]
fn setting_a_label_twice_is_idempotent() {
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin");
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p:Admin");
    assert_eq!(labels_of(&store, "Ada"), vec!["Admin", "P"]);
    assert_eq!(count(&store, "MATCH (p:Admin) RETURN count(p) AS r"), 1);
}

#[test]
fn set_a_property_and_a_label_in_one_statement() {
    // The grammar tries `set_item` first and it needs a `.`, so `p:Admin`
    // falls through to the label form. Both in one clause is where that
    // ordering could go wrong.
    let (mut store, _) = graph();
    run(&mut store, "MATCH (p:P) WHERE p.name = \"Ada\" SET p.age = 36, p:Admin");
    assert_eq!(labels_of(&store, "Ada"), vec!["Admin", "P"]);
    assert_eq!(
        count(&store, "MATCH (p:Admin) WHERE p.age = 36 RETURN count(p) AS r"),
        1
    );
}

#[test]
fn explain_shows_the_label_mutation() {
    let (store, _) = graph();
    let query = parse_query("EXPLAIN MATCH (p:P) SET p:Admin").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    assert!(text.contains("LabelMutation"), "{text}");
    assert!(text.contains("+p:Admin"), "{text}");
}
