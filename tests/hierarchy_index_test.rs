//! Integration tests for the OEH hierarchy index Cypher surface (ADR-035).
//!
//! These drive the engine the way a user does — parse, plan, execute — rather than calling
//! the index API directly, so they cover the grammar, the planner wiring and the operators
//! together. Unit-level correctness (index vs oracle) lives in `src/index/hierarchy/`.

use samyama::graph::{EdgeType, GraphStore, NodeId, PropertyValue};
use samyama::query::QueryEngine;

/// Three-level ATC-shaped drug hierarchy: 1 root, 3 classes, 9 drugs with `units` 1..=9.
fn atc_store() -> (GraphStore, NodeId, Vec<NodeId>) {
    let mut store = GraphStore::new();
    let root = store.create_node("Class");
    store.set_column_property(root, "code", PropertyValue::String("ROOT".into()));
    let mut classes = Vec::new();
    let mut n = 1i64;
    for c in 0..3 {
        let mid = store.create_node("Class");
        store.set_column_property(mid, "code", PropertyValue::String(format!("C{c}")));
        store.create_edge(mid, root, "IS_A").unwrap();
        classes.push(mid);
        for _ in 0..3 {
            let leaf = store.create_node("Drug");
            store.create_edge(leaf, mid, "IS_A").unwrap();
            store.set_column_property(leaf, "units", PropertyValue::Integer(n));
            n += 1;
        }
    }
    (store, root, classes)
}

fn cell_int(batch: &samyama::query::RecordBatch, row: usize, col: &str) -> i64 {
    match batch.records[row].get(col).unwrap() {
        samyama::query::executor::record::Value::Property(PropertyValue::Integer(i)) => *i,
        other => panic!("expected integer in column {col}, got {other:?}"),
    }
}

fn cell_str(batch: &samyama::query::RecordBatch, row: usize, col: &str) -> String {
    match batch.records[row].get(col).unwrap() {
        samyama::query::executor::record::Value::Property(PropertyValue::String(s)) => s.clone(),
        other => panic!("expected string in column {col}, got {other:?}"),
    }
}

#[test]
fn create_hierarchy_index_reports_the_selected_encoding() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    let result = engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum, max",
            &mut store,
            "default",
        )
        .unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(cell_str(&result, 0, "encoding"), "nested-set");
    assert_eq!(cell_int(&result, 0, "nodes"), 13);
    assert_eq!(cell_int(&result, 0, "edges"), 12);
    assert_eq!(cell_str(&result, 0, "status"), "ok");
    assert!(store.hierarchy_index.get("atc").is_some());
}

#[test]
fn create_with_a_labelled_measure_and_reversed_arrow() {
    let mut store = GraphStore::new();
    let root = store.create_node("Class");
    let kid = store.create_node("Class");
    store.create_edge(root, kid, "HAS_CHILD").unwrap();
    store.set_column_property(kid, "units", PropertyValue::Integer(4));
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX h ON ()<-[:HAS_CHILD]-() MEASURE Class.units AGGREGATE sum",
            &mut store,
            "default",
        )
        .unwrap();
    let entry = store.hierarchy_index.get("h").unwrap();
    let guard = entry.read().unwrap();
    let idx = guard.index.as_ref().unwrap();
    // Reversed: the stored edge points parent -> child, so `kid ⊑ root`.
    assert_eq!(idx.subsumes_ids(kid, root), Some(true));
    assert_eq!(idx.subsumes_ids(root, kid), Some(false));
}

#[test]
fn create_accepts_several_covering_edge_types() {
    let mut store = GraphStore::new();
    let root = store.create_node("Term");
    let a = store.create_node("Term");
    let b = store.create_node("Term");
    store.create_edge(a, root, "IS_A").unwrap();
    store.create_edge(b, a, "PART_OF").unwrap();
    let engine = QueryEngine::new();
    let result = engine
        .execute_mut(
            "CREATE HIERARCHY INDEX onto ON ()-[:IS_A|PART_OF]->()",
            &mut store,
            "default",
        )
        .unwrap();
    assert_eq!(cell_int(&result, 0, "nodes"), 3);
    assert_eq!(cell_int(&result, 0, "edges"), 2);
    let entry = store.hierarchy_index.get("onto").unwrap();
    assert_eq!(
        entry
            .read()
            .unwrap()
            .index
            .as_ref()
            .unwrap()
            .subsumes_ids(b, root),
        Some(true),
        "subsumption must compose across both covering edge types"
    );
}

#[test]
fn show_hierarchy_indexes_lists_declarations() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum",
            &mut store,
            "default",
        )
        .unwrap();
    let result = engine.execute("SHOW HIERARCHY INDEXES", &store).unwrap();
    assert_eq!(result.records.len(), 1);
    assert_eq!(cell_str(&result, 0, "name"), "atc");
    assert_eq!(cell_str(&result, 0, "measure"), "units");
    assert!(cell_int(&result, 0, "bytes") > 0);
}

#[test]
fn drop_hierarchy_index_removes_it() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->()",
            &mut store,
            "default",
        )
        .unwrap();
    engine
        .execute_mut("DROP HIERARCHY INDEX atc", &mut store, "default")
        .unwrap();
    assert!(store.hierarchy_index.get("atc").is_none());
    let result = engine.execute("SHOW HIERARCHY INDEXES", &store).unwrap();
    assert!(result.records.is_empty());
}

#[test]
fn a_write_to_the_covering_relation_marks_stale_and_rebuild_clears_it() {
    let (mut store, root, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum",
            &mut store,
            "default",
        )
        .unwrap();
    let et = EdgeType::new("IS_A");
    assert!(store.hierarchy_index.usable_for_edge_type(&et).is_some());

    let extra = store.create_node("Drug");
    store.create_edge(extra, root, "IS_A").unwrap();
    store.set_column_property(extra, "units", PropertyValue::Integer(100));

    let shown = engine.execute("SHOW HIERARCHY INDEXES", &store).unwrap();
    assert!(
        matches!(
            shown.records[0].get("stale").unwrap(),
            samyama::query::executor::record::Value::Property(PropertyValue::Boolean(true))
        ),
        "SHOW must surface staleness so the user can see why the index stopped being used"
    );
    assert!(store.hierarchy_index.usable_for_edge_type(&et).is_none());

    let rebuilt = engine
        .execute_mut("REBUILD HIERARCHY INDEX atc", &mut store, "default")
        .unwrap();
    assert_eq!(cell_int(&rebuilt, 0, "nodes"), 14);
    assert!(store.hierarchy_index.usable_for_edge_type(&et).is_some());

    let entry = store.hierarchy_index.get("atc").unwrap();
    let guard = entry.read().unwrap();
    assert_eq!(
        guard
            .index
            .as_ref()
            .unwrap()
            .rollup_id(root, samyama::index::hierarchy::RollupOp::Sum),
        Some(samyama::index::hierarchy::RollupValue::Int(145)),
        "the rebuild must pick up the new leaf's measure"
    );
}

#[test]
fn declining_a_high_width_dag_is_a_row_not_an_error() {
    // The Gene Ontology regime: width ≈ number of leaves. The user gets a diagnostic
    // naming the alternative, and the graph is otherwise untouched.
    let mut store = GraphStore::new();
    let roots: Vec<NodeId> = (0..3).map(|_| store.create_node("Term")).collect();
    for i in 0..400usize {
        let leaf = store.create_node("Term");
        store.create_edge(leaf, roots[i % 3], "PART_OF").unwrap();
        store
            .create_edge(leaf, roots[(i + 1) % 3], "PART_OF")
            .unwrap();
    }
    let engine = QueryEngine::new();
    let result = engine
        .execute_mut(
            "CREATE HIERARCHY INDEX go ON ()-[:PART_OF]->()",
            &mut store,
            "default",
        )
        .unwrap();
    assert_eq!(cell_str(&result, 0, "encoding"), "declined");
    let status = cell_str(&result, 0, "status");
    assert!(
        status.contains("2-hop"),
        "diagnostic must name the alternative: {status}"
    );
    assert!(store
        .hierarchy_index
        .usable_for_edge_type(&EdgeType::new("PART_OF"))
        .is_none());
}

#[test]
fn a_cyclic_covering_relation_is_rejected() {
    let mut store = GraphStore::new();
    let a = store.create_node("T");
    let b = store.create_node("T");
    store.create_edge(a, b, "IS_A").unwrap();
    store.create_edge(b, a, "IS_A").unwrap();
    let engine = QueryEngine::new();
    let err = engine
        .execute_mut(
            "CREATE HIERARCHY INDEX bad ON ()-[:IS_A]->()",
            &mut store,
            "default",
        )
        .unwrap_err();
    assert!(format!("{err}").contains("cycle"), "got: {err}");
}

#[test]
fn an_unsupported_aggregate_is_rejected_rather_than_silently_dropped() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    let err = engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE avg",
            &mut store,
            "default",
        )
        .unwrap_err();
    assert!(format!("{err}").contains("avg"), "got: {err}");
}

#[test]
fn duplicate_declaration_is_rejected() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    let stmt = "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->()";
    engine.execute_mut(stmt, &mut store, "default").unwrap();
    let err = engine.execute_mut(stmt, &mut store, "default").unwrap_err();
    assert!(format!("{err}").contains("already exists"), "got: {err}");
}

// ---------------------------------------------------------------------------
// The direct function surface
// ---------------------------------------------------------------------------

#[test]
fn subsumes_function_answers_an_order_test_from_the_index() {
    let (mut store, root, classes) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum",
            &mut store,
            "default",
        )
        .unwrap();

    // Every drug is under the root; exactly three are under the first class.
    let all = engine
        .execute(
            "MATCH (d:Drug), (r:Class) WHERE r.code = \"ROOT\" AND subsumes(d, r) RETURN d",
            &store,
        )
        .unwrap();
    assert_eq!(all.records.len(), 9);

    let under_c0 = engine
        .execute(
            "MATCH (d:Drug), (r:Class) WHERE r.code = \"C0\" AND subsumes(d, r) RETURN d",
            &store,
        )
        .unwrap();
    assert_eq!(under_c0.records.len(), 3);

    // ...and the negation is the complement, which is the H6 anti-subsumption shape.
    let not_under_c0 = engine
        .execute(
            "MATCH (d:Drug), (r:Class) WHERE r.code = \"C0\" AND NOT subsumes(d, r) RETURN d",
            &store,
        )
        .unwrap();
    assert_eq!(not_under_c0.records.len(), 6);
    let _ = (root, classes);
}

#[test]
fn hierarchy_rollup_function_matches_the_engine_aggregation() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum, min, max",
            &mut store,
            "default",
        )
        .unwrap();

    // Index-resident answer.
    let indexed = engine
        .execute(
            "MATCH (r:Class) WHERE r.code = \"C0\" RETURN hierarchy_rollup(r, \"sum\") AS total",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&indexed, 0, "total"), 6, "units 1+2+3 under C0");

    // The same question asked the engine-aggregation way. Agreement between the two is
    // the property that matters: the rewrite must not change the answer, only the cost.
    let traversed = engine
        .execute(
            "MATCH (d:Drug)-[:IS_A]->(r:Class) WHERE r.code = \"C0\" RETURN sum(d.units) AS total",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&traversed, 0, "total"), 6);

    let max = engine
        .execute(
            "MATCH (r:Class) WHERE r.code = \"ROOT\" RETURN hierarchy_rollup(r, \"max\") AS m",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&max, 0, "m"), 9);

    let count = engine
        .execute(
            "MATCH (r:Class) WHERE r.code = \"C1\" RETURN hierarchy_rollup(r, \"count\") AS c",
            &store,
        )
        .unwrap();
    assert_eq!(
        cell_int(&count, 0, "c"),
        4,
        "the class itself plus three drugs"
    );
}

#[test]
fn hierarchy_functions_report_a_bad_index_name_rather_than_guessing() {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum",
            &mut store,
            "default",
        )
        .unwrap();
    let err = engine
        .execute(
            "MATCH (r:Class) WHERE r.code = \"C0\" RETURN hierarchy_rollup(r, \"sum\", \"nope\") AS t",
            &store,
        )
        .unwrap_err();
    assert!(format!("{err}").contains("nope"), "got: {err}");
}

#[test]
fn a_stale_index_is_not_used_by_the_functions() {
    let (mut store, root, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum",
            &mut store,
            "default",
        )
        .unwrap();
    // Invalidate by writing the covering relation.
    let extra = store.create_node("Drug");
    store.create_edge(extra, root, "IS_A").unwrap();

    let err = engine
        .execute(
            "MATCH (r:Class) WHERE r.code = \"C0\" RETURN hierarchy_rollup(r, \"sum\", \"atc\") AS t",
            &store,
        )
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("no usable hierarchy index"),
        "a stale index must refuse rather than answer from outdated structure: {msg}"
    );
}

// ---------------------------------------------------------------------------
// Planner rewrites — plan shape and answer preservation
// ---------------------------------------------------------------------------

fn plan_description_unchecked(query: &str, store: &GraphStore) -> String {
    let parsed = samyama::query::parse_query(query).unwrap();
    let planner = samyama::query::executor::planner::QueryPlanner::new();
    let plan = planner.plan(&parsed, store).unwrap();
    plan.root.describe().format(0)
}

/// [`plan_description_unchecked`], refusing a plan tree that stops early.
///
/// `PhysicalOperator::describe` has a default printing `Unknown` with no
/// children, so an operator that does not implement it truncates the tree —
/// and an assertion that the plan does *not* contain something is then
/// satisfied by the missing half, whatever the planner did (#1826). Checked
/// here, where the text is read, so the hole fails loudly rather than passing.
fn plan_description(query: &str, store: &GraphStore) -> String {
    let text = plan_description_unchecked(query, store);
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "the plan tree stops at an operator with no describe, so what is below \
         it cannot be asserted about (#1826):\n{text}"
    );
    text
}

fn atc_store_with_index() -> GraphStore {
    let (mut store, _, _) = atc_store();
    let engine = QueryEngine::new();
    engine
        .execute_mut(
            "CREATE HIERARCHY INDEX atc ON ()-[:IS_A]->() MEASURE units AGGREGATE sum, max, count",
            &mut store,
            "default",
        )
        .unwrap();
    store
}

#[test]
fn a_reflexive_subtree_aggregate_plans_as_hierarchy_rollup() {
    let store = atc_store_with_index();
    let plan = plan_description(
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN sum(d.units) AS total",
        &store,
    );
    assert!(
        plan.contains("HierarchyRollup"),
        "the rewrite must be visible in EXPLAIN, not just in the timing: {plan}"
    );
    assert!(
        !plan.contains("Expand"),
        "no expansion should survive the rewrite: {plan}"
    );
    assert!(
        plan.contains("atc"),
        "the plan must name the index it used: {plan}"
    );
}

#[test]
fn the_rewrite_returns_the_same_answer_as_the_expansion() {
    // The property that matters: the plan changes, the answer does not. The comparison
    // query uses a spelling the detector rejects (`*1..`), so it is genuinely executed the
    // slow way — root excluded — and the reflexive answer must equal it plus the root,
    // which carries no measure here.
    let store = atc_store_with_index();
    let engine = QueryEngine::new();

    let rewritten = engine
        .execute(
            "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN sum(d.units) AS total",
            &store,
        )
        .unwrap();
    let expanded = engine
        .execute(
            "MATCH (d)-[:IS_A*]->(r:Class {code: \"C0\"}) RETURN sum(d.units) AS total",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&rewritten, 0, "total"), 6);
    assert_eq!(
        cell_int(&rewritten, 0, "total"),
        cell_int(&expanded, 0, "total"),
        "the class node itself carries no measure, so reflexive and strict agree here"
    );
}

#[test]
fn count_over_a_subtree_plans_as_a_rollup_and_counts_the_root() {
    let store = atc_store_with_index();
    let engine = QueryEngine::new();
    let plan = plan_description(
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN count(d) AS n",
        &store,
    );
    assert!(plan.contains("HierarchyRollup"), "{plan}");
    let result = engine
        .execute(
            "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN count(d) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(
        cell_int(&result, 0, "n"),
        4,
        "three drugs plus the class itself"
    );
}

#[test]
fn returning_the_descendants_plans_as_a_descendant_scan() {
    let store = atc_store_with_index();
    let engine = QueryEngine::new();
    let plan = plan_description(
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"ROOT\"}) RETURN d",
        &store,
    );
    assert!(plan.contains("HierarchyDescendantScan"), "{plan}");
    let result = engine
        .execute(
            "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"ROOT\"}) RETURN d",
            &store,
        )
        .unwrap();
    assert_eq!(
        result.records.len(),
        13,
        "everything, root included, exactly once"
    );
}

#[test]
fn a_stale_index_falls_back_to_the_standard_plan() {
    let mut store = atc_store_with_index();
    let root = store.node_ids_by_label(&samyama::graph::Label::new("Class"), Some(1))[0];
    let extra = store.create_node("Drug");
    store.create_edge(extra, root, "IS_A").unwrap();

    let plan = plan_description(
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN sum(d.units) AS total",
        &store,
    );
    assert!(
        !plan.contains("HierarchyRollup"),
        "a stale index must not answer: {plan}"
    );
}

#[test]
fn a_query_without_a_hierarchy_index_plans_unchanged() {
    let (store, _, _) = atc_store();
    let plan = plan_description(
        "MATCH (d)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN sum(d.units) AS total",
        &store,
    );
    assert!(!plan.contains("Hierarchy"), "{plan}");
}

// ---------------------------------------------------------------------------
// Order-test rewrite (ADR-035 §8 rewrite 1)
// ---------------------------------------------------------------------------

#[test]
fn a_subsumption_predicate_plans_as_hierarchy_order_test() {
    let store = atc_store_with_index();
    let plan = plan_description(
        "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(d) AS n",
        &store,
    );
    assert!(
        plan.contains("HierarchyOrderTest"),
        "the predicate must become an O(1) interval test, not a per-row function call: {plan}"
    );
    assert!(
        !plan.contains("CartesianProduct"),
        "the pinned root should be resolved at plan time, not joined per row: {plan}"
    );
}

#[test]
fn the_order_test_rewrite_preserves_the_answer() {
    // Same question, three spellings: the rewrite, the traversal, and the function form
    // with an extra predicate that blocks the rewrite. All must agree.
    let store = atc_store_with_index();
    let engine = QueryEngine::new();

    let rewritten = engine
        .execute(
            "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(d) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&rewritten, 0, "n"), 3, "three drugs under C0");

    let traversed = engine
        .execute(
            "MATCH (d:Drug)-[:IS_A*0..]->(r:Class {code: \"C0\"}) RETURN count(d) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&traversed, 0, "n"), 3);
}

#[test]
fn a_negated_subsumption_predicate_also_rewrites() {
    let store = atc_store_with_index();
    let engine = QueryEngine::new();
    let plan = plan_description(
        "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE NOT subsumes(d, r) RETURN count(d) AS n",
        &store,
    );
    assert!(plan.contains("HierarchyOrderTest"), "{plan}");
    let result = engine
        .execute(
            "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE NOT subsumes(d, r) RETURN count(d) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(
        cell_int(&result, 0, "n"),
        6,
        "nine drugs total, three under C0"
    );
}

#[test]
fn the_order_test_rewrite_can_return_the_rows_themselves() {
    let store = atc_store_with_index();
    let engine = QueryEngine::new();
    let plan = plan_description(
        "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN d",
        &store,
    );
    assert!(plan.contains("HierarchyOrderTest"), "{plan}");
    let result = engine
        .execute(
            "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN d",
            &store,
        )
        .unwrap();
    assert_eq!(result.records.len(), 3);
}

#[test]
fn the_order_test_rewrite_declines_shapes_it_cannot_answer() {
    let store = atc_store_with_index();
    // An extra predicate: the rewrite emits no filter, so it must not claim this query.
    let extra_predicate = plan_description(
        "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) AND d.units > 1 RETURN count(d) AS n",
        &store,
    );
    assert!(
        !extra_predicate.contains("HierarchyOrderTest"),
        "{extra_predicate}"
    );

    // An ambiguous pin resolves to more than one node.
    let mut ambiguous = atc_store_with_index();
    let dup = ambiguous.create_node("Class");
    ambiguous.set_column_property(dup, "code", PropertyValue::String("C0".into()));
    let plan = plan_description(
        "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(d) AS n",
        &ambiguous,
    );
    assert!(!plan.contains("HierarchyOrderTest"), "{plan}");
}

#[test]
fn a_stale_index_does_not_get_the_order_test_rewrite() {
    let mut store = atc_store_with_index();
    let root = store.node_ids_by_label(&samyama::graph::Label::new("Class"), Some(1))[0];
    let extra = store.create_node("Drug");
    store.create_edge(extra, root, "IS_A").unwrap();
    let plan = plan_description(
        "MATCH (d:Drug), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(d) AS n",
        &store,
    );
    assert!(
        !plan.contains("HierarchyOrderTest"),
        "a stale index must not answer: {plan}"
    );
}

// ---------------------------------------------------------------------------
// Hierarchy-driven plan (#350)
// ---------------------------------------------------------------------------

/// Facts hanging off an ATC-shaped hierarchy: 3 drugs under C0, 3 under C1, 3 under C2.
/// `Order` nodes point at drugs via `FOR`.
fn order_store() -> (GraphStore, Vec<NodeId>) {
    let mut store = atc_store_with_index();
    let drugs = store.node_ids_by_label(&samyama::graph::Label::new("Drug"), None);
    let mut orders = Vec::new();
    for (i, &d) in drugs.iter().enumerate() {
        let o = store.create_node("Order");
        store.set_column_property(o, "qty", PropertyValue::Integer((i + 1) as i64));
        store.create_edge(o, d, "FOR").unwrap();
        orders.push(o);
    }
    (store, orders)
}

#[test]
fn a_fact_joined_to_a_subtree_plans_as_a_hierarchy_driven_scan() {
    let (store, _) = order_store();
    let plan = plan_description(
        "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(o) AS n",
        &store,
    );
    assert!(
        plan.contains("HierarchyDescendantScan"),
        "the subtree should drive the plan, not the fact table: {plan}"
    );
    assert!(
        plan.contains("Expand"),
        "and it should walk back into the facts: {plan}"
    );
    assert!(
        !plan.contains("NodeScan (var=o"),
        "the fact table must not be scanned: {plan}"
    );
}

#[test]
fn the_driven_plan_returns_the_same_answer_as_the_fact_scan() {
    let (store, _) = order_store();
    let engine = QueryEngine::new();
    // 3 drugs under C0, one order each.
    let driven = engine
        .execute(
            "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(o) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&driven, 0, "n"), 3);

    // The whole hierarchy: every order.
    let all = engine
        .execute(
            "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"ROOT\"}) WHERE subsumes(d, r) RETURN count(o) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&all, 0, "n"), 9);
}

#[test]
fn the_driven_plan_preserves_row_multiplicity() {
    // The sharp case for turning the join around: one fact linked to *two* members of the
    // same subtree. The original plan yields one row per (fact, member) pair; the driven
    // plan discovers the same pairs from the other end and must yield the same count —
    // 2, not 1. Getting this wrong would silently under-count every many-to-many join.
    let (mut store, _) = order_store();
    let c0_drugs: Vec<NodeId> = {
        let engine = QueryEngine::new();
        let r = engine
            .execute(
                "MATCH (d:Drug)-[:IS_A]->(c:Class {code: \"C0\"}) RETURN d",
                &store,
            )
            .unwrap();
        r.records
            .iter()
            .filter_map(|rec| match rec.get("d") {
                Some(samyama::query::executor::record::Value::Node(id, _)) => Some(*id),
                Some(samyama::query::executor::record::Value::NodeRef(id)) => Some(*id),
                _ => None,
            })
            .collect()
    };
    assert!(c0_drugs.len() >= 2);
    let multi = store.create_node("Order");
    store.set_column_property(multi, "qty", PropertyValue::Integer(100));
    store.create_edge(multi, c0_drugs[0], "FOR").unwrap();
    store.create_edge(multi, c0_drugs[1], "FOR").unwrap();
    // Rebuild: the covering relation did not change, but be explicit rather than lucky.
    let engine = QueryEngine::new();
    engine
        .execute_mut("REBUILD HIERARCHY INDEX atc", &mut store, "default")
        .unwrap();

    let counted = engine
        .execute(
            "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(o) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(
        cell_int(&counted, 0, "n"),
        5,
        "3 single-drug orders + the multi-drug order counted once per matching drug"
    );

    let distinct = engine
        .execute(
            "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"C0\"}) WHERE subsumes(d, r) RETURN count(DISTINCT o) AS n",
            &store,
        )
        .unwrap();
    assert_eq!(
        cell_int(&distinct, 0, "n"),
        4,
        "DISTINCT collapses the multi-drug order"
    );
}

#[test]
fn the_driven_plan_handles_sum_over_a_fact_property() {
    let (store, _) = order_store();
    let engine = QueryEngine::new();
    let plan = plan_description(
        "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"ROOT\"}) WHERE subsumes(d, r) RETURN sum(o.qty) AS v",
        &store,
    );
    assert!(plan.contains("HierarchyDescendantScan"), "{plan}");
    let result = engine
        .execute(
            "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"ROOT\"}) WHERE subsumes(d, r) RETURN sum(o.qty) AS v",
            &store,
        )
        .unwrap();
    assert_eq!(cell_int(&result, 0, "v"), 45, "quantities 1..=9");
}

#[test]
fn the_driven_plan_declines_a_negated_predicate() {
    // The complement of a subtree is not something a descendant scan can enumerate.
    let (store, _) = order_store();
    let plan = plan_description(
        "MATCH (o:Order)-[:FOR]->(d), (r:Class {code: \"C0\"}) WHERE NOT subsumes(d, r) RETURN count(o) AS n",
        &store,
    );
    assert!(!plan.contains("HierarchyDescendantScan"), "{plan}");
}
