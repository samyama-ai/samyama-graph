//! `MATCH (a:A:B)` means A **and** B (#944).
//!
//! It returned the union: six rows where two are correct, and `(a:A:B:C)`
//! returned every labelled node in the graph. A single label was right, so
//! adding labels *widened* the result instead of narrowing it.
//!
//! It fails **open** — strictly more rows than asked for, from a query that
//! reports success. Multi-label patterns are ordinary in real schemas
//! (`(:Person:Employee)`), and every one of them had been matching
//! `Person OR Employee`, so any filter, count or write scoped that way was
//! operating on a superset.
//!
//! The scan code did exactly what its own comment said — *"Multi-label: union
//! via HashSet"* — and a unit test asserted it. The behaviour was pinned in
//! place rather than caught.

use samyama::graph::{GraphStore, Label};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// The openCypher `Match1` [3] fixture.
fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    for labels in [
        vec!["A", "B", "C"], vec!["A", "B"], vec!["A", "C"], vec!["B", "C"],
        vec!["A"], vec!["B"], vec!["C"],
    ] {
        store.create_node_with_labels(labels.iter().map(|l| Label::new(*l)));
    }
    store
}

/// The sorted label sets of the matched nodes, as `"A:B"` strings.
fn matched(store: &GraphStore, cypher: &str) -> Vec<String> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut out: Vec<String> = QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .iter()
        .map(|r| {
            let id = match r.get("a") {
                Some(Value::Node(id, _)) | Some(Value::NodeRef(id)) => *id,
                other => panic!("{other:?}"),
            };
            let mut ls: Vec<String> = store
                .get_node(id)
                .unwrap()
                .labels
                .iter()
                .map(|l| l.as_str().to_string())
                .collect();
            ls.sort();
            ls.join(":")
        })
        .collect();
    out.sort();
    out
}

#[test]
fn two_labels_mean_both() {
    let store = graph();
    assert_eq!(matched(&store, "MATCH (a:A:B) RETURN a"), vec!["A:B", "A:B:C"]);
}

#[test]
fn three_labels_mean_all_three() {
    // This one returned all seven labelled nodes.
    let store = graph();
    assert_eq!(matched(&store, "MATCH (a:A:B:C) RETURN a"), vec!["A:B:C"]);
}

#[test]
fn a_single_label_is_unchanged() {
    let store = graph();
    assert_eq!(
        matched(&store, "MATCH (a:A) RETURN a"),
        vec!["A", "A:B", "A:B:C", "A:C"]
    );
}

#[test]
fn adding_a_label_never_widens_the_result() {
    // The property the union violated, stated directly: each extra label can
    // only remove rows.
    let store = graph();
    let one = matched(&store, "MATCH (a:A) RETURN a");
    let two = matched(&store, "MATCH (a:A:B) RETURN a");
    let three = matched(&store, "MATCH (a:A:B:C) RETURN a");
    assert!(two.len() <= one.len() && three.len() <= two.len(), "{one:?} {two:?} {three:?}");
    assert!(two.iter().all(|x| one.contains(x)), "each is a subset of the last");
    assert!(three.iter().all(|x| two.contains(x)));
}

#[test]
fn a_label_no_node_carries_makes_it_empty() {
    let store = graph();
    assert!(matched(&store, "MATCH (a:A:Nonexistent) RETURN a").is_empty());
}

#[test]
fn the_order_the_labels_are_written_does_not_matter() {
    // The fix drives the intersection from the smallest label set, so the
    // written order stops being the scan order. It must not become the answer.
    let store = graph();
    assert_eq!(
        matched(&store, "MATCH (a:A:B) RETURN a"),
        matched(&store, "MATCH (a:B:A) RETURN a")
    );
}

#[test]
fn a_limit_returns_matching_nodes_not_merely_that_many() {
    // `early_limit` used to count insertions into the union, so a LIMIT could
    // stop before reaching a node carrying all the labels.
    let store = graph();
    let rows = matched(&store, "MATCH (a:A:B) RETURN a LIMIT 1");
    assert_eq!(rows.len(), 1);
    assert!(rows[0].contains('A') && rows[0].contains('B'), "{rows:?}");
}
