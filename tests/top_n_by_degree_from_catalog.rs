//! Top-N-by-degree is answered from the catalog's degree maps, or not at all (#304).
//!
//! `MATCH (a:Article)<-[:CITES]-() RETURN a.title, count(*) AS c ORDER BY c DESC
//! LIMIT 10` timed out at 120 s on a 1.4-billion-edge graph. Two separate
//! reasons, fixed here as two separate steps:
//!
//! 1. **The query as written missed the ADR-017 detector entirely.** It wanted
//!    `count(<endpoint variable>)` and both endpoints named, so `count(*)` over
//!    an anonymous `()` fell to the generic `Expand` → `Aggregate`, which
//!    materialises a row per edge. Naming the endpoint and writing `count(b)`
//!    already changed the plan — a one-word rewrite of the same question.
//! 2. **`AdjacencyCountAggregate` is O(nodes + edges), not O(nodes).**
//!    `outgoing_degree_for_type` walks the whole adjacency list of the node and
//!    filters by type id (`store.rs:3560`), so the edge term never goes away.
//!    `GraphCatalog` has held the answer per node per edge type since ADR-015
//!    (`catalog.rs` `source_degrees` / `target_degrees`) — it was private with
//!    no accessor, which is why no operator could read it.
//!
//! ## The shape set this is exact for
//!
//! Every one of these must hold, or the plan keeps the adjacency walk:
//!
//! * one non-`OPTIONAL` MATCH, one path, one segment, no variable length;
//! * exactly one concrete edge type, direction `->` or `<-` (not `--`);
//! * the aggregate is `count(*)`, `count(<the other endpoint>)` or
//!   `count(<the relationship>)` — never `count(DISTINCT …)`;
//! * a group-by on the counted endpoint only (its variable, or its properties);
//! * no `WHERE`, no property map on either endpoint, no `RETURN DISTINCT`;
//! * at most one label on each endpoint;
//! * the catalog's degrees are **exact** for that edge type — see below.
//!
//! `ORDER BY` either way, and a missing `LIMIT`, are all fine: the shortcut
//! replaces the row source only, and the same `Sort` and `Limit` sit above it.
//! Ties at the K boundary therefore resolve exactly as they did before. Nodes
//! of degree zero are absent from the degree maps, which is what a required
//! MATCH wants — it produces no row for them.
//!
//! ## Why exactness has to be asked for
//!
//! The degree maps are keyed by the **triple** `(source label, type, target
//! label)`, so an edge whose endpoints carry two labels each is filed four
//! times and an edge between unlabelled nodes is filed nowhere. Summing a
//! node's degree across the matching triples is only the true degree when every
//! edge of the type is filed exactly once. And `create_edge_stub` skips the
//! catalog altogether, so a bulk-loaded graph has degree maps that are silently
//! short until `finish_bulk_load` rebuilds them — the same hazard PR #1806
//! guarded on edge counts. `GraphCatalog::degrees_are_exact_for` asks all of
//! it; when the answer is no, the plan is the adjacency walk and the answer is
//! still right.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Five articles. a0 is cited three times, a1 once, a2/a3/a4 never.
///
/// a4 exists only to be a zero-degree node: a required MATCH gives it no row,
/// so neither may the shortcut.
fn citations() -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..5 {
        let id = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(format!("A{i}")),
        );
        ids.push(id);
    }
    store.create_edge(ids[1], ids[0], "CITES").unwrap();
    store.create_edge(ids[2], ids[0], "CITES").unwrap();
    store.create_edge(ids[3], ids[0], "CITES").unwrap();
    store.create_edge(ids[2], ids[1], "CITES").unwrap();
    store
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// Rows as `(title, count)`, sorted so the comparison does not depend on the
/// iteration order of whichever source answered.
fn titled(store: &GraphStore, cypher: &str) -> Vec<(String, i64)> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let mut out: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let t = match r.get("t") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("t: {other:?}"),
            };
            let c = match r.get("c") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("c: {other:?}"),
            };
            (t, c)
        })
        .collect();
    out.sort();
    out
}

const Q19: &str =
    "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC LIMIT 10";

// ---------------------------------------------------------------- the shortcut

#[test]
fn the_q19_shape_reads_its_counts_from_the_catalog() {
    let store = citations();
    let p = plan(&store, Q19);
    assert!(
        p.contains("AdjacencyCountAggregate"),
        "count(*) over an anonymous endpoint must reach the degree rewrite, not \
         Expand+Aggregate; plan was:\n{p}"
    );
    assert!(
        p.contains("source=catalog"),
        "and must read the degrees from the catalog rather than walking \
         adjacency; plan was:\n{p}"
    );
    assert!(
        !p.contains("Expand"),
        "no expand may survive in this plan:\n{p}"
    );
}

#[test]
fn the_answer_equals_the_full_expand_path() {
    // The real oracle: the same question asked in the form that has always
    // taken the generic Expand -> Aggregate path. `count(a)` counts the
    // grouped endpoint, which the detector declines, so this is the slow path
    // by construction.
    let store = citations();
    let slow = titled(
        &store,
        "MATCH (a:Article)<-[:CITES]-(b) RETURN a.title AS t, count(a) AS c",
    );
    assert!(
        !plan(
            &store,
            "MATCH (a:Article)<-[:CITES]-(b) RETURN a.title AS t, count(a) AS c"
        )
        .contains("AdjacencyCountAggregate"),
        "the oracle query must not itself take the rewrite"
    );
    assert_eq!(titled(&store, Q19), slow);
    assert_eq!(slow, vec![("A0".to_string(), 3), ("A1".to_string(), 1)]);
}

#[test]
fn a_degree_zero_node_produces_no_row() {
    // A2, A3 and A4 are cited by nobody. A required MATCH gives them no row,
    // and the degree maps hold no entry for them either.
    let store = citations();
    let rows = titled(&store, Q19);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(rows.iter().all(|(t, _)| t != "A4"));
}

#[test]
fn the_grouped_endpoint_needs_no_label() {
    // The label-free form #304 also lists. There is no label to scan, so the
    // fallback would be a scan of every node; the catalog answers it directly.
    let store = citations();
    let q = "MATCH (a)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC";
    let p = plan(&store, q);
    assert!(p.contains("source=catalog"), "{p}");
    assert_eq!(
        titled(&store, q),
        vec![("A0".to_string(), 3), ("A1".to_string(), 1)]
    );
}

#[test]
fn counting_the_relationship_is_counting_the_rows() {
    // One non-optional segment binds exactly one relationship per row, so
    // count(r) is count(*).
    let store = citations();
    let q = "MATCH (a:Article)<-[r:CITES]-() RETURN a.title AS t, count(r) AS c";
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        vec![("A0".to_string(), 3), ("A1".to_string(), 1)]
    );
}

#[test]
fn the_out_degree_direction_works_too() {
    // Q20's shape: group on the source rather than the target.
    let store = citations();
    let q = "MATCH (a:Article)-[:CITES]->() RETURN a.title AS t, count(*) AS c ORDER BY c DESC";
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        vec![
            ("A1".to_string(), 1),
            ("A2".to_string(), 2),
            ("A3".to_string(), 1)
        ]
    );
}

#[test]
fn order_by_ascending_and_no_limit_are_unaffected() {
    // The shortcut replaces the row source; Sort and Limit are untouched, so
    // neither the direction of the sort nor a missing LIMIT can change the
    // answer.
    let store = citations();
    let asc = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c ASC";
    assert!(plan(&store, asc).contains("source=catalog"));
    let query = parse_query(asc).unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let order: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(order, vec![1, 3]);
}

#[test]
fn the_neighbours_label_still_restricts_the_count() {
    // A degree counts every edge of the type whatever sits at the far end
    // (#601). The catalog is keyed by the triple, so the far label selects
    // which triples are summed rather than filtering afterwards.
    let mut store = GraphStore::new();
    let mk = |store: &mut GraphStore, label: &str, name: &str| {
        let id = store.create_node(label);
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let paper = mk(&mut store, "Article", "P");
    let other = mk(&mut store, "Article", "Q");
    let blog = mk(&mut store, "Blog", "B");
    store.create_edge(other, paper, "CITES").unwrap();
    store.create_edge(blog, paper, "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-(b:Article) RETURN a.title AS t, count(*) AS c";
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        vec![("P".to_string(), 1)],
        "the :Blog citation must not be counted"
    );
    // Unconstrained far end counts both.
    assert_eq!(
        titled(
            &store,
            "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c"
        ),
        vec![("P".to_string(), 2)]
    );
}

#[test]
fn nodes_sharing_a_title_are_one_group() {
    // Grouping is on `a.title`, not on the node. Two articles with the same
    // title are one row whose count is the sum -- the catalog gives per-node
    // degrees, so the operator still has to merge them.
    let mut store = GraphStore::new();
    let mk = |store: &mut GraphStore, name: &str| {
        let id = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let a = mk(&mut store, "Same");
    let b = mk(&mut store, "Same");
    let c = mk(&mut store, "Citer");
    store.create_edge(c, a, "CITES").unwrap();
    store.create_edge(c, b, "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(titled(&store, q), vec![("Same".to_string(), 2)]);
}

// ------------------------------------------------------- shapes that must not

/// Each of these must decline the catalog source **and** answer correctly.
/// Declining quietly and wrongly is the failure mode the guard exists for.
#[test]
fn count_distinct_declines_the_catalog_and_still_dedupes() {
    // Two parallel CITES edges between the same pair: the degree is 2, the
    // number of distinct citing articles is 1.
    let mut store = GraphStore::new();
    let mk = |store: &mut GraphStore, name: &str| {
        let id = store.create_node("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let cited = mk(&mut store, "Cited");
    let citer = mk(&mut store, "Citer");
    store.create_edge(citer, cited, "CITES").unwrap();
    store.create_edge(citer, cited, "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-(b) RETURN a.title AS t, count(DISTINCT b) AS c";
    assert!(
        !plan(&store, q).contains("source=catalog"),
        "a degree counts edges, not distinct neighbours"
    );
    assert_eq!(titled(&store, q), vec![("Cited".to_string(), 1)]);
    // The non-distinct form is the degree, and does use the catalog.
    let plain = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(plan(&store, plain).contains("source=catalog"));
    assert_eq!(titled(&store, plain), vec![("Cited".to_string(), 2)]);
}

#[test]
fn a_where_on_the_grouped_side_declines_the_catalog() {
    // The catalog holds node ids and degrees, not properties, so a predicate
    // that selects which nodes count cannot be applied to it. The adjacency
    // walk keeps the filter below the count.
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES]-() WHERE a.title = 'A0' \
             RETURN a.title AS t, count(*) AS c";
    let p = plan(&store, q);
    assert!(!p.contains("source=catalog"), "{p}");
    assert_eq!(titled(&store, q), vec![("A0".to_string(), 3)]);
}

#[test]
fn an_undirected_pattern_declines_the_catalog() {
    // There is no either-direction degree in the catalog, and summing the two
    // would double-count a self-loop.
    let store = citations();
    let q = "MATCH (a:Article)-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    let p = plan(&store, q);
    assert!(!p.contains("source=catalog"), "{p}");
    assert_eq!(
        titled(&store, q),
        vec![
            ("A0".to_string(), 3),
            ("A1".to_string(), 2),
            ("A2".to_string(), 2),
            ("A3".to_string(), 1)
        ]
    );
}

#[test]
fn two_edge_types_in_the_pattern_decline_the_catalog() {
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES|QUOTES]-() RETURN a.title AS t, count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        vec![("A0".to_string(), 3), ("A1".to_string(), 1)]
    );
}

#[test]
fn a_variable_length_pattern_declines_the_catalog() {
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES*1..2]-() RETURN a.title AS t, count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
    let rows = titled(&store, q);
    // A0 is reached by 3 one-hop and 1 two-hop path; A1 by 1 one-hop.
    assert_eq!(
        rows,
        vec![("A0".to_string(), 4), ("A1".to_string(), 1)]
    );
}

#[test]
fn a_property_map_on_an_endpoint_declines_the_catalog() {
    let store = citations();
    let q = "MATCH (a:Article {title: 'A0'})<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
    assert_eq!(titled(&store, q), vec![("A0".to_string(), 3)]);
}

#[test]
fn return_distinct_declines_the_catalog() {
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN DISTINCT a.title AS t, count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        vec![("A0".to_string(), 3), ("A1".to_string(), 1)]
    );
}

#[test]
fn optional_match_declines_the_catalog() {
    // OPTIONAL MATCH wants the zero rows the degree maps do not hold.
    let store = citations();
    let q = "OPTIONAL MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
}

#[test]
fn an_aggregate_with_no_grouping_key_declines_the_catalog() {
    // `RETURN count(*)` groups on nothing and is one scalar, not one row per
    // node (#301).
    let store = citations();
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
    let query = parse_query(q).unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.records[0].get("c"),
        Some(&Value::Property(PropertyValue::Integer(4)))
    );
}

// ------------------------------------------------------- the exactness guard

#[test]
fn a_stub_loaded_graph_declines_the_catalog_and_answers_right() {
    // `create_edge_stub` skips `catalog.on_edge_created`, so the degree maps
    // are empty while the adjacency is full. Reading them would answer "no
    // rows" -- fast, confident and wrong. This is PR #1806's hazard on a
    // different structure.
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..3 {
        let id = store.create_node_stub("Article");
        let _ = store.set_node_property(
            "default",
            id,
            "title".to_string(),
            PropertyValue::String(format!("S{i}")),
        );
        ids.push(id);
    }
    store.create_edge_stub(ids[1], ids[0], "CITES").unwrap();
    store.create_edge_stub(ids[2], ids[0], "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    let p = plan(&store, q);
    assert!(
        !p.contains("source=catalog"),
        "the degree maps are short by every stub edge:\n{p}"
    );
    assert_eq!(titled(&store, q), vec![("S0".to_string(), 2)]);

    // `finish_bulk_load` rebuilds the catalog, and then it is exact.
    store.finish_bulk_load();
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(titled(&store, q), vec![("S0".to_string(), 2)]);
}

#[test]
fn a_multi_label_endpoint_declines_the_catalog() {
    // The degree maps are keyed by (source label, type, target label). An edge
    // whose target carries two labels is filed under two triples, so summing
    // them would double the degree.
    let mut store = GraphStore::new();
    let cited = store.create_node("Article");
    store
        .add_label_to_node("default", cited, "Preprint")
        .unwrap();
    let _ = store.set_node_property(
        "default",
        cited,
        "title".to_string(),
        PropertyValue::String("M".to_string()),
    );
    let citer = store.create_node("Article");
    let _ = store.set_node_property(
        "default",
        citer,
        "title".to_string(),
        PropertyValue::String("C".to_string()),
    );
    store.create_edge(citer, cited, "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    let p = plan(&store, q);
    assert!(!p.contains("source=catalog"), "{p}");
    assert_eq!(
        titled(&store, q),
        vec![("M".to_string(), 1)],
        "one edge, not two"
    );
}

#[test]
fn an_unlabelled_endpoint_declines_the_catalog() {
    // An edge between unlabelled nodes is filed under no triple at all, so the
    // degree maps are short rather than long.
    let mut store = GraphStore::new();
    let cited = store.create_node("Article");
    let _ = store.set_node_property(
        "default",
        cited,
        "title".to_string(),
        PropertyValue::String("L".to_string()),
    );
    let bare = store.create_node_with_labels(std::iter::empty());
    store.create_edge(bare, cited, "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(!plan(&store, q).contains("source=catalog"));
    assert_eq!(titled(&store, q), vec![("L".to_string(), 1)]);
}

#[test]
fn a_label_added_after_the_edge_declines_the_catalog() {
    // The degree is filed under the labels the node had when the edge was
    // created. A later `SET a:X` moves the node without moving its degrees,
    // so a query on the new label would read zero.
    let mut store = GraphStore::new();
    let cited = store.create_node("Article");
    let _ = store.set_node_property(
        "default",
        cited,
        "title".to_string(),
        PropertyValue::String("C".to_string()),
    );
    let citer = store.create_node("Article");
    store.create_edge(citer, cited, "CITES").unwrap();

    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(
        plan(&store, q).contains("source=catalog"),
        "exact to start with"
    );

    store.add_label_to_node("default", citer, "Preprint").unwrap();
    let p = plan(&store, q);
    assert!(
        !p.contains("source=catalog"),
        "a label change on a connected node invalidates the degree maps:\n{p}"
    );
    assert_eq!(titled(&store, q), vec![("C".to_string(), 1)]);
}

#[test]
fn labelling_a_node_that_has_no_edges_keeps_the_catalog() {
    // The staleness mark is about a degree filed under the wrong key. A node
    // with no edges has no degree filed, so a loader that labels its nodes
    // before connecting them -- the ordinary two-phase shape -- keeps the
    // shortcut. The unconditional mark would have turned it off for every such
    // load.
    let mut store = citations();
    let fresh = store.create_node("Article");
    let _ = store.set_node_property(
        "default",
        fresh,
        "title".to_string(),
        PropertyValue::String("A9".to_string()),
    );
    store.add_label_to_node("default", fresh, "Preprint").unwrap();
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        vec![("A0".to_string(), 3), ("A1".to_string(), 1)]
    );
}

#[test]
fn a_deleted_edge_declines_the_catalog_until_the_rebuild() {
    // `on_edge_deleted` reads the endpoint labels at deletion time, which a
    // concurrent label teardown can have already removed. Rather than reason
    // about every order, a deletion marks the degree maps unfit and
    // `rebuild_catalog` is the repair.
    let mut store = citations();
    let q = "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    assert!(plan(&store, q).contains("source=catalog"));

    let edge = store.all_edges().first().map(|e| e.id).unwrap();
    store.delete_edge(edge).unwrap();
    let p = plan(&store, q);
    assert!(!p.contains("source=catalog"), "{p}");
    let after = titled(&store, q);

    store.rebuild_catalog();
    assert!(plan(&store, q).contains("source=catalog"));
    assert_eq!(
        titled(&store, q),
        after,
        "the rebuilt catalog must agree with the walk it replaces"
    );
}

// ------------------------------------------------------ the bounded sort

#[test]
fn the_limit_reaches_the_sort_in_the_specialised_plan() {
    // `SortOperator` has discarded rows past the limit as they arrive since
    // #518, but `try_push_limit` is called at one planner site and the three
    // specialised aggregate plans built their own Sort/Skip/Limit tail without
    // it. At 2M groups that sort was 99.6% of the query. The answer must not
    // change -- `LIMIT n` above a sort already makes row n+1 unobservable.
    let store = citations();
    let unbounded = titled(
        &store,
        "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC",
    );
    assert_eq!(
        unbounded,
        vec![("A0".to_string(), 3), ("A1".to_string(), 1)]
    );
    // Taking the top one gives the first row of the full ordering.
    let top1 = titled(
        &store,
        "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c DESC LIMIT 1",
    );
    assert_eq!(top1, vec![("A0".to_string(), 3)]);
    // And SKIP is counted into the bound, or the skipped rows would be the
    // ones that were kept.
    let second = titled(
        &store,
        "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c \
         ORDER BY c DESC SKIP 1 LIMIT 1",
    );
    assert_eq!(second, vec![("A1".to_string(), 1)]);
    // Ascending, to catch a bound applied to the wrong end.
    let lowest = titled(
        &store,
        "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c ORDER BY c ASC LIMIT 1",
    );
    assert_eq!(lowest, vec![("A1".to_string(), 1)]);
}

#[test]
fn the_bound_does_not_cut_the_count_itself() {
    // The hint stops at the sort. If it reached the aggregate, a `LIMIT 1`
    // would stop counting after one edge and the count would be 1 rather than
    // 3 -- the fast-wrong-answer version of this optimisation.
    let store = citations();
    assert_eq!(
        titled(
            &store,
            "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c \
             ORDER BY c DESC LIMIT 1"
        ),
        vec![("A0".to_string(), 3)]
    );
    // No ORDER BY: nothing consumes the hint, and the counts are still whole.
    let query = parse_query(
        "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c LIMIT 5",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let mut counts: Vec<i64> = batch
        .records
        .iter()
        .map(|r| match r.get("c") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("{other:?}"),
        })
        .collect();
    counts.sort();
    assert_eq!(counts, vec![1, 3]);
}

#[test]
fn the_with_bound_plan_takes_the_limit_too() {
    // Phase 3a's shape (`MATCH ... WITH g LIMIT n MATCH (g)-[:T]->(x) RETURN
    // g.prop, count(x) ORDER BY ...`) has the same hand-built tail, so it had
    // the same gap. The pre-WITH LIMIT and the post-RETURN LIMIT are different
    // bounds and must not be confused.
    let store = citations();
    let rows = titled(
        &store,
        "MATCH (a:Article) WITH a LIMIT 10 \
         MATCH (a)<-[:CITES]-(b) RETURN a.title AS t, count(b) AS c ORDER BY c DESC LIMIT 1",
    );
    assert_eq!(rows, vec![("A0".to_string(), 3)]);
}

// ------------------------------------------------------------- the two shapes

#[test]
fn both_ast_shapes_answer_the_same() {
    // `Query` carries two representations and a planner rule written against
    // one silently does nothing for the other (`tests/ast_shape_parity.rs`).
    // The by-kind grammar takes the Q19 shape, so the pipeline is reached by
    // putting a write in front of it -- and the detector must fire there too.
    let store = citations();
    let by_kind = titled(
        &store,
        "MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c",
    );

    let mut pipelined = citations();
    // No `ORDER BY` here: in the pipeline shape it re-evaluates the aggregate
    // expression instead of the alias the projection emitted, and the query
    // fails with "Unknown function: count". That is on `main` too — verified by
    // running this query against `origin/main` — and is a separate defect. The
    // parity this test is for is which row source answered, and that does not
    // depend on the sort.
    let cypher = "CREATE (:Marker) WITH 1 AS seed \
                  MATCH (a:Article)<-[:CITES]-() RETURN a.title AS t, count(*) AS c";
    let query = parse_query(cypher).unwrap();
    assert!(
        query.needs_clause_pipeline,
        "this fixture must exercise the pipeline shape, or the parity check is vacuous"
    );
    let batch = samyama::query::executor::MutQueryExecutor::new(&mut pipelined, "default".to_string())
        .execute(&query)
        .unwrap();
    let mut rows: Vec<(String, i64)> = batch
        .records
        .iter()
        .map(|r| {
            let t = match r.get("t") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("t: {other:?}"),
            };
            let c = match r.get("c") {
                Some(Value::Property(PropertyValue::Integer(n))) => *n,
                other => panic!("c: {other:?}"),
            };
            (t, c)
        })
        .collect();
    rows.sort();
    assert_eq!(rows, by_kind);
}
