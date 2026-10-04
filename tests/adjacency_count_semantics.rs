//! The degree-counting rewrite answers the query that was asked (#601).
//!
//! `RETURN n.prop, count(neighbour)` over an expand is rewritten to
//! `AdjacencyCountAggregate`, which reads each source's **degree** off the
//! adjacency index. That is a good optimisation — it is why this shape does not
//! appear in profiles — but a degree is not a match count, and it was wrong two
//! ways at once:
//!
//! * it **ignored the neighbour's label**, because a degree counts every edge
//!   of the type whatever sits at the far end;
//! * it **emitted a row per scanned source**, so sources matching the pattern
//!   zero times appeared with count 0 — turning a required match into an
//!   optional one.
//!
//! The tell was that the same query grouped and ungrouped disagreed: `RETURN
//! count(f)` said 1 while `RETURN p.name, count(f)` said 2.
//!
//! These tests assert **the answer and that the rewrite is still taken**. Only
//! the first would let a future correctness fix silently disable the
//! optimisation; only the second would let it stay fast and wrong.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Ada KNOWS Bob (a `:P`) and Rex (an `:Animal`). Bob and Cy know nobody.
///
/// Built so the two defects give different wrong answers: ignoring the label
/// makes Ada 2 instead of 1, and keeping zero rows adds Bob and Cy.
fn fixture() -> GraphStore {
    let mut store = GraphStore::new();
    let mut mk = |store: &mut GraphStore, label: &str, name: &str| {
        let id = store.create_node(label);
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let ada = mk(&mut store, "P", "Ada");
    let bob = mk(&mut store, "P", "Bob");
    let _cy = mk(&mut store, "P", "Cy");
    let rex = mk(&mut store, "Animal", "Rex");
    store.create_edge(ada, bob, "KNOWS").unwrap();
    store.create_edge(ada, rex, "KNOWS").unwrap();
    store
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<(String, i64)> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let mut out: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let g = match r.get("g") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("{other:?}"),
            };
            let n = match r.get("c") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("{other:?}"),
            };
            (g, n)
        })
        .collect();
    out.sort();
    out
}

fn plan_unchecked(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// [`plan_unchecked`], refusing a plan tree that stops early.
///
/// `PhysicalOperator::describe` has a default printing `Unknown` with no
/// children, so an operator that does not implement it truncates the tree —
/// and an assertion that the plan does *not* contain something is then
/// satisfied by the missing half, whatever the planner did (#1826). Checked
/// here, where the text is read, so the hole fails loudly rather than passing.
fn plan(store: &GraphStore, cypher: &str) -> String {
    let text = plan_unchecked(store, cypher);
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "the plan tree stops at an operator with no describe, so what is below \
         it cannot be asserted about (#1826):\n{text}"
    );
    text
}

#[test]
fn the_neighbours_label_is_honoured() {
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c"),
        vec![("Ada".to_string(), 1)],
        "Rex is an :Animal and must not be counted for (f:P)"
    );
}

#[test]
fn an_unlabelled_neighbour_counts_everything() {
    // The shape the optimisation exists for, and the one it was always right
    // about. It must not have been broken by making the label case correct.
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (p:P)-[:KNOWS]->(f) RETURN p.name AS g, count(f) AS c"),
        vec![("Ada".to_string(), 2)]
    );
}

#[test]
fn a_source_matching_zero_times_yields_no_row() {
    // Bob and Cy know nobody. A *required* MATCH drops them; only
    // OPTIONAL MATCH would keep them with a zero.
    let store = fixture();
    let got = rows(&store, "MATCH (p:P)-[:KNOWS]->(f) RETURN p.name AS g, count(f) AS c");
    assert!(!got.iter().any(|(n, _)| n == "Bob" || n == "Cy"), "{got:?}");
}

#[test]
fn the_grouped_and_ungrouped_forms_agree() {
    // The tell that found this. Two ways of asking the same question must not
    // give different totals.
    let store = fixture();
    let grouped: i64 = rows(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c")
        .iter()
        .map(|(_, n)| n)
        .sum();

    let query = parse_query("MATCH (p:P)-[:KNOWS]->(f:P) RETURN count(f) AS c").unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let ungrouped = match batch.records[0].get("c") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{other:?}"),
    };

    assert_eq!(grouped, ungrouped, "grouped {grouped} vs ungrouped {ungrouped}");
    assert_eq!(grouped, 1);
}

#[test]
fn the_rewrite_is_still_taken() {
    // Correctness without this would be easy: disable the rewrite. Asserted so
    // a future fix cannot quietly trade the optimisation away.
    let store = fixture();
    for cypher in [
        "MATCH (p:P)-[:KNOWS]->(f) RETURN p.name AS g, count(f) AS c",
        "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c",
    ] {
        let text = plan(&store, cypher);
        assert!(
            text.contains("AdjacencyCountAggregate"),
            "the rewrite was lost for `{cypher}`:\n{text}"
        );
    }
}

#[test]
fn an_incoming_pattern_is_filtered_too() {
    let store = fixture();
    assert_eq!(
        rows(&store, "MATCH (f:P)<-[:KNOWS]-(p:P) RETURN f.name AS g, count(p) AS c"),
        vec![("Bob".to_string(), 1)],
        "Rex is not a :P, and Ada has no incoming KNOWS"
    );
}

#[test]
fn an_undirected_pattern_is_filtered_too() {
    let store = fixture();
    let got = rows(&store, "MATCH (p:P)-[:KNOWS]-(f:P) RETURN p.name AS g, count(f) AS c");
    assert_eq!(
        got,
        vec![("Ada".to_string(), 1), ("Bob".to_string(), 1)],
        "Ada-Bob counts once from each end; Rex and Cy never: {got:?}"
    );
}

#[test]
fn a_label_no_node_carries_counts_nothing() {
    // `label_index` has no entry, which is not the same as an empty one.
    let store = fixture();
    let got = rows(&store, "MATCH (p:P)-[:KNOWS]->(f:Ghost) RETURN p.name AS g, count(f) AS c");
    assert!(got.is_empty(), "{got:?}");
}

#[test]
fn the_answer_matches_expanding_the_pattern_by_hand() {
    // A differential check against the unrewritten form. `count(f.name)` is not
    // eligible for the rewrite, so it goes through the ordinary aggregate and
    // is the independent answer.
    let store = fixture();
    let by_rewrite = rows(&store, "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f) AS c");
    let by_aggregate = rows(
        &store,
        "MATCH (p:P)-[:KNOWS]->(f:P) RETURN p.name AS g, count(f.name) AS c",
    );
    assert_eq!(by_rewrite, by_aggregate);
}

/// The neighbour-label probe costs a bit test per edge, not a SipHash (#1812).
///
/// #601 made the labelled-neighbour form walk the adjacency and test each
/// neighbour for the label, which is what a correct answer costs. The probe
/// read the label's `std::collections::HashSet<NodeId>`, so every edge walked
/// paid a SipHash. That is invisible at low degree and is the whole query at
/// high degree: on PubMed's `(:Article)-[:ANNOTATED_WITH]->(:MeSHTerm)`, 1.04
/// billion edges over ~30k terms, it took the shape from 5.2 s to 32.4 s with
/// an unchanged plan. The dense label bitset answers the same question with a
/// shift and a mask.
///
/// Pinned as a **ratio measured in this process**, never a wall-clock bound:
/// the labelled form against the unlabelled one over the same graph and the
/// same walk. The only difference between them is the per-edge probe, so the
/// ratio is what the probe costs, whatever the host or the build profile. A
/// `WHERE` on the grouped node is what keeps both on the walk — the catalog's
/// degree maps hold ids and degrees, not properties, so they decline it.
#[test]
fn the_neighbour_label_probe_is_not_a_hash_per_edge() {
    use std::time::Instant;

    // 150 hubs over 2,000 leaves, ~206k edges. The group count is low and the
    // degree high on purpose: that is the shape the probe is multiplied by,
    // and the shape #1812 measured. Degrees are distinct per hub so the top
    // ten is one answer rather than an arbitrary ten of a hundred ties.
    let mut store = GraphStore::new();
    let mut leaves = Vec::new();
    for i in 0..2_000 {
        let id = store.create_node("Leaf");
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(format!("L{i}")),
        );
        leaves.push(id);
    }
    for h in 0..150 {
        let hub = store.create_node("Hub");
        let _ = store.set_node_property(
            "default",
            hub,
            "name".to_string(),
            PropertyValue::String(format!("H{h}")),
        );
        for leaf in leaves.iter().take(1_000 + h * 5) {
            store.create_edge(hub, *leaf, "E").unwrap();
        }
    }

    let labelled = "MATCH (h:Hub)-[:E]->(l:Leaf) WHERE h.name <> 'absent' \
                    RETURN h.name AS g, count(l) AS c ORDER BY c DESC LIMIT 10";
    let unlabelled = "MATCH (h:Hub)-[:E]->() WHERE h.name <> 'absent' \
                      RETURN h.name AS g, count(*) AS c ORDER BY c DESC LIMIT 10";

    // Both forms must still be answered by the rewrite and must agree on the
    // counts: every leaf carries `:Leaf`, so the label excludes nothing. A
    // ratio over two queries that disagree would be measuring two answers.
    assert_eq!(rows(&store, labelled), rows(&store, unlabelled));

    let best = |cypher: &str| {
        let query = parse_query(cypher).unwrap();
        let mut best = f64::MAX;
        for _ in 0..9 {
            let t = Instant::now();
            let batch = QueryExecutor::new(&store).execute(&query).unwrap();
            assert_eq!(batch.records.len(), 10);
            best = best.min(t.elapsed().as_secs_f64());
        }
        best
    };
    // Warm the caches both paths share (the label bitset, the edge type id)
    // so the first query measured does not pay for the second.
    let _ = best(labelled);
    let _ = best(unlabelled);

    let with_label = best(labelled);
    let without_label = best(unlabelled);
    let ratio = with_label / without_label;
    eprintln!(
        "label probe: {with_label:.6}s labelled against {without_label:.6}s unlabelled, {ratio:.2}x"
    );
    assert!(
        ratio < 7.0,
        "the label probe costs {ratio:.2}x the same walk without it \
         ({with_label:.4}s against {without_label:.4}s). The bitset measured \
         3.1x here in release and 4.3x in debug, the `HashSet` 12.3x and \
         15.7x, so 7x separates them in either profile (#1812)"
    );
}
