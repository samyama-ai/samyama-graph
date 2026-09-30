//! Unit tests for the hierarchy DDL and query operators, driven directly (no planner).

use super::*;
use crate::graph::{EdgeType, Label};
use crate::index::hierarchy::HierarchySpec;
use crate::query::executor::operator::NodeScanOperator;

/// A three-level tree: `root <- a <- a1`, `root <- a <- a2`, `root <- b`, with `units`
/// on every non-root node. Edges point child -> parent (`IS_A`).
struct Tree {
    store: GraphStore,
    root: NodeId,
    a: NodeId,
    a1: NodeId,
    a2: NodeId,
    b: NodeId,
}

fn tree() -> Tree {
    let mut store = GraphStore::new();
    let root = store.create_node("Class");
    let a = store.create_node("Class");
    let a1 = store.create_node("Leaf");
    let a2 = store.create_node("Leaf");
    let b = store.create_node("Leaf");
    store.create_edge(a, root, "IS_A").unwrap();
    store.create_edge(a1, a, "IS_A").unwrap();
    store.create_edge(a2, a, "IS_A").unwrap();
    store.create_edge(b, root, "IS_A").unwrap();
    store.set_column_property(a, "units", PropertyValue::Integer(10));
    store.set_column_property(a1, "units", PropertyValue::Integer(1));
    store.set_column_property(a2, "units", PropertyValue::Integer(2));
    store.set_column_property(b, "units", PropertyValue::Integer(5));
    Tree {
        store,
        root,
        a,
        a1,
        a2,
        b,
    }
}

fn spec(name: &str) -> HierarchySpec {
    HierarchySpec::new(name, vec![EdgeType::new("IS_A")]).with_measure(
        None,
        "units",
        vec![RollupOp::Sum, RollupOp::Max],
    )
}

/// Build an index named `name` over the tree's `IS_A` edges.
fn build(store: &mut GraphStore, name: &str) {
    let mut op = CreateHierarchyIndexOperator::new(spec(name));
    op.next_mut(store, "default").unwrap().unwrap();
}

/// A wide bipartite DAG whose width exceeds the probe's cap, so the build declines.
fn wide_store() -> GraphStore {
    let mut store = GraphStore::new();
    let roots: Vec<NodeId> = (0..3).map(|_| store.create_node("R")).collect();
    for i in 0..400usize {
        let leaf = store.create_node("L");
        store.create_edge(leaf, roots[i % 3], "IS_A").unwrap();
        store.create_edge(leaf, roots[(i + 1) % 3], "IS_A").unwrap();
    }
    store
}

fn prop<'a>(r: &'a Record, col: &str) -> &'a PropertyValue {
    match r.get(col) {
        Some(Value::Property(p)) => p,
        other => panic!("expected a property in {col}, got {other:?}"),
    }
}

fn int(r: &Record, col: &str) -> i64 {
    match prop(r, col) {
        PropertyValue::Integer(i) => *i,
        other => panic!("expected integer in {col}, got {other:?}"),
    }
}

fn string(r: &Record, col: &str) -> String {
    match prop(r, col) {
        PropertyValue::String(s) => s.clone(),
        other => panic!("expected string in {col}, got {other:?}"),
    }
}

fn node_of(r: &Record, var: &str) -> NodeId {
    match r.get(var) {
        Some(Value::NodeRef(id)) | Some(Value::Node(id, _)) => *id,
        other => panic!("expected a node in {var}, got {other:?}"),
    }
}

fn runtime_msg(e: ExecutionError) -> String {
    match e {
        ExecutionError::RuntimeError(m) => m,
        other => panic!("expected RuntimeError, got {other:?}"),
    }
}

/// Emits a fixed list of records, used to feed rows the scan operators never produce.
struct Rows {
    rows: Vec<Record>,
    at: usize,
}

impl PhysicalOperator for Rows {
    fn next(&mut self, _store: &GraphStore) -> ExecutionResult<Option<Record>> {
        let r = self.rows.get(self.at).cloned();
        self.at += 1;
        Ok(r)
    }
    fn reset(&mut self) {
        self.at = 0;
    }
    fn describe(&self) -> OperatorDescription {
        OperatorDescription {
            name: "Rows".to_string(),
            details: String::new(),
            children: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// rollup_to_property / hierarchy_info_columns
// ---------------------------------------------------------------------------

#[test]
fn rollup_int_that_fits_stays_an_integer() {
    assert_eq!(
        rollup_to_property(RollupValue::Int(42)),
        PropertyValue::Integer(42)
    );
}

#[test]
fn rollup_int_beyond_i64_becomes_a_float_not_a_wrapped_integer() {
    let big = i64::MAX as i128 + 10;
    match rollup_to_property(RollupValue::Int(big)) {
        PropertyValue::Float(f) => assert_eq!(f, big as f64),
        other => panic!("expected a float, got {other:?}"),
    }
}

#[test]
fn rollup_float_and_null_pass_through() {
    assert_eq!(
        rollup_to_property(RollupValue::Float(1.5)),
        PropertyValue::Float(1.5)
    );
    assert_eq!(rollup_to_property(RollupValue::Null), PropertyValue::Null);
}

#[test]
fn info_columns_are_the_twelve_reported_fields_in_order() {
    assert_eq!(
        hierarchy_info_columns(),
        vec![
            "name",
            "encoding",
            "nodes",
            "edges",
            "width",
            "measure",
            "aggregates",
            "bytes",
            "structural_bytes",
            "rollup_bytes",
            "stale",
            "status"
        ]
    );
}

// ---------------------------------------------------------------------------
// DDL operators
// ---------------------------------------------------------------------------

#[test]
fn create_via_read_only_next_is_refused() {
    let t = tree();
    let mut op = CreateHierarchyIndexOperator::new(spec("h"));
    let msg = runtime_msg(op.next(&t.store).unwrap_err());
    assert!(msg.contains("requires mutable store access"), "{msg}");
    assert!(t.store.hierarchy_index.get("h").is_none());
}

#[test]
fn create_reports_one_row_describing_the_built_index() {
    let mut t = tree();
    let mut op = CreateHierarchyIndexOperator::new(spec("h"));
    let row = op.next_mut(&mut t.store, "default").unwrap().unwrap();
    assert_eq!(string(&row, "name"), "h");
    assert_ne!(string(&row, "encoding"), "declined");
    assert_eq!(int(&row, "nodes"), 5);
    assert_eq!(int(&row, "edges"), 4);
    assert!(string(&row, "measure").contains("units"));
    assert_eq!(string(&row, "aggregates"), "sum,max");
    assert_eq!(prop(&row, "stale"), &PropertyValue::Boolean(false));
    assert_eq!(string(&row, "status"), "ok");
    assert!(int(&row, "bytes") > 0);
    // Exactly one row.
    assert!(op.next_mut(&mut t.store, "default").unwrap().is_none());
    assert!(t.store.hierarchy_index.get("h").is_some());
}

#[test]
fn create_after_reset_runs_again_and_hits_the_duplicate() {
    let mut t = tree();
    let mut op = CreateHierarchyIndexOperator::new(spec("h"));
    op.next_mut(&mut t.store, "default").unwrap();
    op.reset();
    let msg = runtime_msg(op.next_mut(&mut t.store, "default").unwrap_err());
    assert!(msg.contains("already exists"), "{msg}");
}

#[test]
fn create_without_measure_reports_null_measure() {
    let mut t = tree();
    let mut op =
        CreateHierarchyIndexOperator::new(HierarchySpec::new("plain", vec![EdgeType::new("IS_A")]));
    let row = op.next_mut(&mut t.store, "default").unwrap().unwrap();
    assert_eq!(prop(&row, "measure"), &PropertyValue::Null);
    assert_eq!(string(&row, "aggregates"), "count");
}

#[test]
fn create_on_an_out_of_regime_poset_reports_a_decline_row() {
    let mut store = wide_store();
    let mut op =
        CreateHierarchyIndexOperator::new(HierarchySpec::new("wide", vec![EdgeType::new("IS_A")]));
    let row = op.next_mut(&mut store, "default").unwrap().unwrap();
    assert_eq!(string(&row, "encoding"), "declined");
    assert_ne!(string(&row, "status"), "ok");
    assert!(
        matches!(prop(&row, "width"), PropertyValue::Integer(w) if *w > 0),
        "a decline must carry the measured width"
    );
}

#[test]
fn create_describes_name_and_every_edge_type() {
    let op = CreateHierarchyIndexOperator::new(HierarchySpec::new(
        "onto",
        vec![EdgeType::new("IS_A"), EdgeType::new("PART_OF")],
    ));
    let d = op.describe();
    assert_eq!(d.name, "CreateHierarchyIndex");
    assert_eq!(d.details, "onto on IS_A|PART_OF");
    assert!(d.children.is_empty());
}

#[test]
fn drop_via_read_only_next_is_refused() {
    let t = tree();
    let mut op = DropHierarchyIndexOperator::new("h".into());
    let msg = runtime_msg(op.next(&t.store).unwrap_err());
    assert!(msg.contains("requires mutable store access"), "{msg}");
}

#[test]
fn drop_removes_the_index_and_returns_no_rows() {
    let mut t = tree();
    build(&mut t.store, "h");
    let mut op = DropHierarchyIndexOperator::new("h".into());
    assert!(op.next_mut(&mut t.store, "default").unwrap().is_none());
    assert!(t.store.hierarchy_index.get("h").is_none());
    // Already executed: a second pull is a no-op, not an error.
    assert!(op.next_mut(&mut t.store, "default").unwrap().is_none());
    // After a reset it runs again, and the index is gone.
    op.reset();
    let msg = runtime_msg(op.next_mut(&mut t.store, "default").unwrap_err());
    assert!(msg.contains("no hierarchy index named 'h'"), "{msg}");
}

#[test]
fn drop_describes_the_index_name() {
    let d = DropHierarchyIndexOperator::new("h".into()).describe();
    assert_eq!(d.name, "DropHierarchyIndex");
    assert_eq!(d.details, "h");
}

#[test]
fn rebuild_via_read_only_next_is_refused() {
    let t = tree();
    let mut op = RebuildHierarchyIndexOperator::new("h".into());
    let msg = runtime_msg(op.next(&t.store).unwrap_err());
    assert!(msg.contains("requires mutable store access"), "{msg}");
}

#[test]
fn rebuild_picks_up_new_nodes_and_reports_once() {
    let mut t = tree();
    build(&mut t.store, "h");
    let c = t.store.create_node("Leaf");
    t.store.create_edge(c, t.b, "IS_A").unwrap();
    let mut op = RebuildHierarchyIndexOperator::new("h".into());
    let row = op.next_mut(&mut t.store, "default").unwrap().unwrap();
    assert_eq!(int(&row, "nodes"), 6);
    assert_eq!(int(&row, "edges"), 5);
    assert!(op.next_mut(&mut t.store, "default").unwrap().is_none());
    op.reset();
    assert!(op.next_mut(&mut t.store, "default").unwrap().is_some());
}

#[test]
fn rebuild_of_an_unknown_index_is_an_error() {
    let mut t = tree();
    let mut op = RebuildHierarchyIndexOperator::new("nope".into());
    let msg = runtime_msg(op.next_mut(&mut t.store, "default").unwrap_err());
    assert!(msg.contains("no hierarchy index named 'nope'"), "{msg}");
    let d = op.describe();
    assert_eq!(d.name, "RebuildHierarchyIndex");
    assert_eq!(d.details, "nope");
}

#[test]
fn show_lists_every_index_in_name_order_and_replays_after_reset() {
    let mut t = tree();
    build(&mut t.store, "zeta");
    build(&mut t.store, "alpha");
    let mut op = ShowHierarchyIndexesOperator::default();
    let first = op.next(&t.store).unwrap().unwrap();
    let second = op.next(&t.store).unwrap().unwrap();
    assert!(op.next(&t.store).unwrap().is_none());
    assert_eq!(string(&first, "name"), "alpha");
    assert_eq!(string(&second, "name"), "zeta");
    op.reset();
    assert_eq!(
        string(&op.next(&t.store).unwrap().unwrap(), "name"),
        "alpha"
    );
    let d = op.describe();
    assert_eq!(d.name, "ShowHierarchyIndexes");
    assert!(d.details.is_empty());
}

#[test]
fn show_on_an_empty_registry_returns_nothing() {
    let t = tree();
    let mut op = ShowHierarchyIndexesOperator::new();
    assert!(op.next(&t.store).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// HierarchyOrderTest
// ---------------------------------------------------------------------------

fn order_test(index: &str, ancestor: NodeId, negated: bool) -> HierarchyOrderTestOperator {
    HierarchyOrderTestOperator::new(
        Box::new(NodeScanOperator::new("x".into(), vec![Label::new("Leaf")])),
        index.into(),
        "x".into(),
        ancestor,
        negated,
    )
}

fn drain(op: &mut dyn PhysicalOperator, store: &GraphStore, var: &str) -> Vec<NodeId> {
    let mut out = Vec::new();
    while let Some(r) = op.next(store).unwrap() {
        out.push(node_of(&r, var));
    }
    out.sort();
    out
}

#[test]
fn order_test_keeps_only_rows_under_the_ancestor() {
    let mut t = tree();
    build(&mut t.store, "h");
    let mut op = order_test("h", t.a, false);
    assert_eq!(drain(&mut op, &t.store, "x"), vec![t.a1, t.a2]);
}

#[test]
fn negated_order_test_keeps_rows_not_under_the_ancestor() {
    let mut t = tree();
    build(&mut t.store, "h");
    let mut op = order_test("h", t.a, true);
    assert_eq!(drain(&mut op, &t.store, "x"), vec![t.b]);
    // Reset replays the input.
    op.reset();
    assert_eq!(drain(&mut op, &t.store, "x"), vec![t.b]);
}

#[test]
fn order_test_skips_rows_without_a_node_and_rejects_nodes_outside_the_hierarchy() {
    let mut t = tree();
    build(&mut t.store, "h");
    let outsider = t.store.create_node("Leaf");
    let mut not_a_node = Record::new();
    not_a_node.bind("x", Value::Property(PropertyValue::Integer(1)));
    let mut inside = Record::new();
    inside.bind("x", Value::NodeRef(t.a1));
    let mut outside = Record::new();
    outside.bind("x", Value::NodeRef(outsider));
    let rows = vec![not_a_node, outside.clone(), inside];
    let mut pos = HierarchyOrderTestOperator::new(
        Box::new(Rows {
            rows: rows.clone(),
            at: 0,
        }),
        "h".into(),
        "x".into(),
        t.root,
        false,
    );
    assert_eq!(drain(&mut pos, &t.store, "x"), vec![t.a1]);
    // An outsider is not under the ancestor, so the negated test keeps it — but a row
    // with no node in the variable is dropped either way.
    let mut neg = HierarchyOrderTestOperator::new(
        Box::new(Rows { rows, at: 0 }),
        "h".into(),
        "x".into(),
        t.root,
        true,
    );
    assert_eq!(drain(&mut neg, &t.store, "x"), vec![outsider]);
}

#[test]
fn order_test_on_a_missing_index_is_an_error() {
    let t = tree();
    let mut op = order_test("gone", t.root, false);
    let msg = runtime_msg(op.next(&t.store).unwrap_err());
    assert!(msg.contains("'gone' disappeared mid-query"), "{msg}");
}

#[test]
fn order_test_on_a_declined_index_is_an_error() {
    let mut store = wide_store();
    let mut create =
        CreateHierarchyIndexOperator::new(HierarchySpec::new("wide", vec![EdgeType::new("IS_A")]));
    create.next_mut(&mut store, "default").unwrap();
    let mut op = HierarchyOrderTestOperator::new(
        Box::new(NodeScanOperator::new("x".into(), vec![Label::new("L")])),
        "wide".into(),
        "x".into(),
        NodeId::new(1),
        false,
    );
    let msg = runtime_msg(op.next(&store).unwrap_err());
    assert!(msg.contains("'wide' is not built"), "{msg}");
}

#[test]
fn order_test_describes_the_predicate_and_its_input() {
    let t = tree();
    let mut op = order_test("h", t.root, true);
    let d = op.describe();
    assert_eq!(d.name, "HierarchyOrderTest");
    assert_eq!(d.details, format!("NOT x ⊑ {} via h", t.root.as_u64()));
    assert_eq!(d.children.len(), 1);
    assert_eq!(op.children_mut().len(), 1);
    let pos = order_test("h", t.root, false).describe();
    assert_eq!(pos.details, format!("x ⊑ {} via h", t.root.as_u64()));
}

// ---------------------------------------------------------------------------
// HierarchyRollup
// ---------------------------------------------------------------------------

#[test]
fn rollup_folds_the_subtree_measure_into_one_row() {
    let mut t = tree();
    build(&mut t.store, "h");
    let mut op = HierarchyRollupOperator::new("h".into(), t.a, RollupOp::Sum, "total".into());
    let row = op.next(&t.store).unwrap().unwrap();
    assert_eq!(int(&row, "total"), 13);
    assert!(op.next(&t.store).unwrap().is_none());
    op.reset();
    let again = op.next(&t.store).unwrap().unwrap();
    assert_eq!(int(&again, "total"), 13);
}

#[test]
fn rollup_max_over_the_whole_tree() {
    let mut t = tree();
    build(&mut t.store, "h");
    let mut op = HierarchyRollupOperator::new("h".into(), t.root, RollupOp::Max, "m".into());
    assert_eq!(int(&op.next(&t.store).unwrap().unwrap(), "m"), 10);
}

#[test]
fn rollup_of_a_node_outside_the_hierarchy_is_null() {
    let mut t = tree();
    build(&mut t.store, "h");
    let outsider = t.store.create_node("Leaf");
    let mut op = HierarchyRollupOperator::new("h".into(), outsider, RollupOp::Sum, "s".into());
    assert_eq!(
        prop(&op.next(&t.store).unwrap().unwrap(), "s"),
        &PropertyValue::Null
    );
}

#[test]
fn rollup_on_missing_or_declined_index_is_an_error() {
    let t = tree();
    let mut missing =
        HierarchyRollupOperator::new("gone".into(), t.root, RollupOp::Sum, "s".into());
    assert!(runtime_msg(missing.next(&t.store).unwrap_err()).contains("disappeared"));

    let mut store = wide_store();
    let mut create =
        CreateHierarchyIndexOperator::new(HierarchySpec::new("wide", vec![EdgeType::new("IS_A")]));
    create.next_mut(&mut store, "default").unwrap();
    let mut declined =
        HierarchyRollupOperator::new("wide".into(), NodeId::new(1), RollupOp::Count, "c".into());
    assert!(runtime_msg(declined.next(&store).unwrap_err()).contains("is not built"));
}

#[test]
fn rollup_describes_op_root_and_index() {
    let d = HierarchyRollupOperator::new("h".into(), NodeId::new(7), RollupOp::Sum, "s".into())
        .describe();
    assert_eq!(d.name, "HierarchyRollup");
    assert_eq!(d.details, "sum(measure) under 7 via h");
    assert!(d.children.is_empty());
}

// ---------------------------------------------------------------------------
// HierarchyDescendantScan
// ---------------------------------------------------------------------------

#[test]
fn descendant_scan_yields_the_root_and_its_subtree_once_each() {
    let mut t = tree();
    build(&mut t.store, "h");
    let mut op = HierarchyDescendantScanOperator::new("h".into(), t.a, "d".into());
    let mut want = vec![t.a, t.a1, t.a2];
    want.sort();
    assert_eq!(drain(&mut op, &t.store, "d"), want);
    op.reset();
    assert_eq!(drain(&mut op, &t.store, "d").len(), 3);
}

#[test]
fn descendant_scan_of_a_node_outside_the_hierarchy_is_empty() {
    let mut t = tree();
    build(&mut t.store, "h");
    let outsider = t.store.create_node("Leaf");
    let mut op = HierarchyDescendantScanOperator::new("h".into(), outsider, "d".into());
    assert!(op.next(&t.store).unwrap().is_none());
}

#[test]
fn descendant_scan_on_missing_or_declined_index_is_an_error() {
    let t = tree();
    let mut missing = HierarchyDescendantScanOperator::new("gone".into(), t.root, "d".into());
    assert!(runtime_msg(missing.next(&t.store).unwrap_err()).contains("disappeared"));

    let mut store = wide_store();
    let mut create =
        CreateHierarchyIndexOperator::new(HierarchySpec::new("wide", vec![EdgeType::new("IS_A")]));
    create.next_mut(&mut store, "default").unwrap();
    let mut declined =
        HierarchyDescendantScanOperator::new("wide".into(), NodeId::new(1), "d".into());
    assert!(runtime_msg(declined.next(&store).unwrap_err()).contains("is not built"));
}

#[test]
fn descendant_scan_describes_var_root_and_index() {
    let d = HierarchyDescendantScanOperator::new("h".into(), NodeId::new(3), "d".into()).describe();
    assert_eq!(d.name, "HierarchyDescendantScan");
    assert_eq!(d.details, "d under 3 via h");
}
