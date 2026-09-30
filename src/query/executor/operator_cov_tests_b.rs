//! Coverage tests for the second half of `operator.rs` (from `FilterOperator`
//! to the end of the file): the expand, aggregate, sort, join, DDL, write,
//! algorithm and path operators.
//!
//! Most tests drive an operator through a real Cypher query on a small
//! in-memory graph; the rest construct the operator directly to reach paths
//! the planner never takes (the read-only `next` of a write operator, `reset`,
//! `describe`).

use super::*;
use crate::graph::{EdgeId, Label};
use crate::query::executor::{MutQueryExecutor, QueryExecutor};
use crate::query::parser::parse_query;

const TENANT: &str = "default";

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Run a read query through the read-only executor.
fn read(store: &GraphStore, cypher: &str) -> RecordBatch {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse `{cypher}`: {e}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}`: {e}"))
}

/// The error a read query fails with.
fn read_err(store: &GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse `{cypher}`: {e}"));
    match QueryExecutor::new(store).execute(&q) {
        Ok(b) => panic!("`{cypher}` succeeded with {} rows", b.records.len()),
        Err(e) => e.to_string(),
    }
}

/// Run a query through the write executor.
fn write(store: &mut GraphStore, cypher: &str) -> RecordBatch {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse `{cypher}`: {e}"));
    MutQueryExecutor::new(store, TENANT.to_string())
        .execute(&q)
        .unwrap_or_else(|e| panic!("`{cypher}`: {e}"))
}

/// The error a write query fails with.
fn write_err(store: &mut GraphStore, cypher: &str) -> String {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse `{cypher}`: {e}"));
    match MutQueryExecutor::new(store, TENANT.to_string()).execute(&q) {
        Ok(b) => panic!("`{cypher}` succeeded with {} rows", b.records.len()),
        Err(e) => e.to_string(),
    }
}

/// A scalar cell as a `PropertyValue`, resolving node/edge refs to nothing.
fn cell(b: &RecordBatch, row: usize, col: &str) -> PropertyValue {
    match b.records[row].get(col) {
        Some(Value::Property(p)) => p.clone(),
        Some(Value::Null) | None => PropertyValue::Null,
        Some(other) => panic!("{col} is not a scalar: {other:?}"),
    }
}

fn int(b: &RecordBatch, row: usize, col: &str) -> i64 {
    match cell(b, row, col) {
        PropertyValue::Integer(i) => i,
        other => panic!("{col} is not an integer: {other:?}"),
    }
}

fn float(b: &RecordBatch, row: usize, col: &str) -> f64 {
    match cell(b, row, col) {
        PropertyValue::Float(f) => f,
        PropertyValue::Integer(i) => i as f64,
        other => panic!("{col} is not a number: {other:?}"),
    }
}

fn string(b: &RecordBatch, row: usize, col: &str) -> String {
    match cell(b, row, col) {
        PropertyValue::String(s) => s,
        other => panic!("{col} is not a string: {other:?}"),
    }
}

/// Every row's value of one string column.
fn strings(b: &RecordBatch, col: &str) -> Vec<String> {
    (0..b.records.len()).map(|i| string(b, i, col)).collect()
}

/// Every row's value of one integer column.
fn ints(b: &RecordBatch, col: &str) -> Vec<i64> {
    (0..b.records.len()).map(|i| int(b, i, col)).collect()
}

/// Drain a read operator.
fn drain(op: &mut dyn PhysicalOperator, store: &GraphStore) -> Vec<Record> {
    let mut out = Vec::new();
    while let Some(r) = op.next(store).unwrap() {
        out.push(r);
    }
    out
}

/// Drain an operator through `next_mut`.
fn drain_mut(op: &mut dyn PhysicalOperator, store: &mut GraphStore) -> Vec<Record> {
    let mut out = Vec::new();
    while let Some(r) = op.next_mut(store, TENANT).unwrap() {
        out.push(r);
    }
    out
}

fn prop_str(r: &Record, col: &str) -> String {
    match r.get(col) {
        Some(Value::Property(PropertyValue::String(s))) => s.clone(),
        other => panic!("{col}: {other:?}"),
    }
}

/// Three people and a company:
/// alice -KNOWS-> bob -KNOWS-> carol, alice -WORKS_AT-> acme.
fn people() -> GraphStore {
    let mut store = GraphStore::new();
    write(
        &mut store,
        "CREATE (a:Person {name: 'alice', age: 30}), (b:Person {name: 'bob', age: 40}), \
         (c:Person {name: 'carol', age: 50}), (x:Company {name: 'acme'}), \
         (a)-[:KNOWS {since: 2000}]->(b), (b)-[:KNOWS {since: 2010}]->(c), \
         (a)-[:WORKS_AT]->(x)",
    );
    store
}

fn a_record(var: &str, v: Value) -> Record {
    let mut r = Record::new();
    r.bind(var.to_string(), v);
    r
}

// ---------------------------------------------------------------------------
// DDL operators constructed directly
// ---------------------------------------------------------------------------

#[test]
fn ddl_operators_refuse_the_read_only_path() {
    let store = GraphStore::new();
    let mut ops: Vec<OperatorBox> = vec![
        Box::new(CreateNodeOperator::new(vec![])),
        Box::new(CreateIndexOperator::new(Label::new("P"), "x".into())),
        Box::new(CreateVectorIndexOperator::new(
            Label::new("P"),
            "v".into(),
            3,
            "cosine".into(),
        )),
        Box::new(CreateFullTextIndexOperator::new(
            "ft".into(),
            Label::new("P"),
            vec!["x".into()],
        )),
        Box::new(DropFullTextIndexOperator::new("ft".into())),
        Box::new(CompositeCreateIndexOperator::new(
            Label::new("P"),
            vec!["x".into()],
        )),
        Box::new(CreateConstraintOperator::new(Label::new("P"), "x".into())),
        Box::new(DropIndexOperator::new(Label::new("P"), "x".into())),
        Box::new(CreateEdgeOperator::new(
            None,
            "a".into(),
            "b".into(),
            EdgeType::new("R"),
            HashMap::new(),
            None,
        )),
        Box::new(CreateNodesAndEdgesOperator::new(
            Box::new(SingleRowOperator::new()),
            vec![],
        )),
        Box::new(MatchCreateEdgeOperator::new(
            Box::new(SingleRowOperator::new()),
            vec![],
        )),
    ];
    for op in ops.iter_mut() {
        assert!(op.is_mutating(), "{}", op.describe().name);
        match op.next(&store) {
            Err(ExecutionError::RuntimeError(msg)) => {
                assert!(
                    msg.contains("next_mut") || msg.contains("mutable store"),
                    "{msg}"
                )
            }
            other => panic!("{}: {other:?}", op.describe().name),
        }
    }
}

#[test]
fn create_index_operator_runs_once_until_reset() {
    let mut store = GraphStore::new();
    let n = store.create_node("P");
    store.set_node_property(TENANT, n, "x", 1i64).unwrap();
    let mut op = CreateIndexOperator::new(Label::new("P"), "x".into());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    op.reset();
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    let shown = read(&store, "SHOW INDEXES");
    assert_eq!(strings(&shown, "label"), vec!["P"]);
    assert_eq!(strings(&shown, "type"), vec!["BTREE"]);
}

#[test]
fn composite_index_operator_creates_one_index_per_property() {
    let mut store = GraphStore::new();
    let mut op = CompositeCreateIndexOperator::new(Label::new("P"), vec!["a".into(), "b".into()]);
    let d = op.describe();
    assert_eq!(d.name, "CreateCompositeIndex");
    assert_eq!(d.details, ":P(a, b)");
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    op.reset();
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    let shown = read(&store, "SHOW INDEXES");
    assert_eq!(strings(&shown, "property"), vec!["a", "b"]);
}

#[test]
fn create_constraint_operator_declares_and_rejects_duplicates() {
    let mut store = GraphStore::new();
    let mut op = CreateConstraintOperator::new(Label::new("P"), "id".into());
    let d = op.describe();
    assert_eq!(d.name, "CreateConstraint");
    assert_eq!(d.details, "UNIQUE :P(id)");
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    op.reset();
    // Declaring it again is idempotent: the listing still holds one constraint.
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    let shown = read(&store, "SHOW CONSTRAINTS");
    assert_eq!(shown.records.len(), 1);
    assert_eq!(string(&shown, 0, "type"), "UNIQUE");
    assert_eq!(string(&shown, 0, "property"), "id");
    op.reset();
}

#[test]
fn create_constraint_over_existing_duplicates_is_refused() {
    let mut store = GraphStore::new();
    for _ in 0..2 {
        let n = store.create_node("P");
        store.set_node_property(TENANT, n, "id", 7i64).unwrap();
    }
    let mut op = CreateConstraintOperator::new(Label::new("P"), "id".into());
    assert!(matches!(
        op.next_mut(&mut store, TENANT),
        Err(ExecutionError::RuntimeError(_))
    ));
}

#[test]
fn drop_index_operator_drops_and_refuses_a_missing_index() {
    let mut store = GraphStore::new();
    store.create_property_index(&Label::new("P"), "x");
    let mut op = DropIndexOperator::new(Label::new("P"), "x".into());
    let d = op.describe();
    assert_eq!(
        (d.name.as_str(), d.details.as_str()),
        ("DropIndex", ":P(x)")
    );
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    op.reset();
    match op.next_mut(&mut store, TENANT) {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("does not exist"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn create_vector_index_operator_rejects_an_unknown_metric() {
    let mut store = GraphStore::new();
    let mut op = CreateVectorIndexOperator::new(Label::new("P"), "v".into(), 3, "manhattan".into());
    match op.next_mut(&mut store, TENANT) {
        Err(ExecutionError::RuntimeError(m)) => assert!(
            m.contains("Unsupported similarity metric: manhattan"),
            "{m}"
        ),
        other => panic!("{other:?}"),
    }
}

#[test]
fn create_vector_index_operator_builds_l2_index_once_until_reset() {
    let mut store = GraphStore::new();
    let mut op = CreateVectorIndexOperator::new(Label::new("P"), "v".into(), 2, "L2".into())
        .with_name(Some("vi".into()))
        .with_quantization(crate::vector::index::Quantization::None);
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    let shown = read(&store, "SHOW INDEXES");
    assert_eq!(strings(&shown, "type"), vec!["VECTOR"]);
    op.reset();
    // Declaring it again replaces the index rather than adding a second one.
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_some());
    assert_eq!(read(&store, "SHOW INDEXES").records.len(), 1);
}

#[test]
fn fulltext_ddl_operators_create_search_and_drop() {
    let mut store = GraphStore::new();
    let n = store.create_node("Doc");
    store
        .set_node_property(TENANT, n, "title", "graph databases")
        .unwrap();
    store
        .set_node_property(TENANT, n, "body", "rust engine")
        .unwrap();

    let mut create = CreateFullTextIndexOperator::new(
        "docs".into(),
        Label::new("Doc"),
        vec!["title".into(), "body".into()],
    );
    let d = create.describe();
    assert_eq!(d.name, "CreateFullTextIndex");
    assert_eq!(d.details, "docs ON :Doc(title, body)");
    assert!(create.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(create.next_mut(&mut store, TENANT).unwrap().is_none());
    create.reset();

    let shown = read(&store, "SHOW INDEXES");
    let kinds = strings(&shown, "type");
    assert!(kinds.contains(&"FULLTEXT[docs]".to_string()), "{kinds:?}");
    assert!(
        kinds.contains(&"FULLTEXT[docs.body]".to_string()),
        "{kinds:?}"
    );

    let mut search = FullTextSearchOperator::new(
        "docs".into(),
        "graph".into(),
        10,
        "node".into(),
        "score".into(),
    );
    let d = search.describe();
    assert_eq!(d.name, "FullTextSearch");
    assert_eq!(d.details, "docs ~ \"graph\"");
    let hits = drain(&mut search, &store);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].get("node"), Some(&Value::NodeRef(n)));
    assert!(
        matches!(hits[0].get("score"), Some(Value::Property(PropertyValue::Float(s))) if *s > 0.0)
    );
    search.reset();
    assert_eq!(drain(&mut search, &store).len(), 1);

    let mut unknown =
        FullTextSearchOperator::new("nope".into(), "graph".into(), 10, "n".into(), "s".into());
    match unknown.next(&store) {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("Known: "), "{m}"),
        other => panic!("{other:?}"),
    }

    let mut drop = DropFullTextIndexOperator::new("docs".into());
    assert!(drop.next_mut(&mut store, TENANT).unwrap().is_some());
    assert!(drop.next_mut(&mut store, TENANT).unwrap().is_none());
    drop.reset();
    // `docs.body` is still there, so the refusal names it.
    match drop.next_mut(&mut store, TENANT) {
        Err(ExecutionError::RuntimeError(m)) => {
            assert!(m.contains("no full-text index named 'docs'"), "{m}");
            assert!(m.contains("Known: docs.body"), "{m}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn fulltext_search_and_drop_on_an_empty_catalog_say_none_exists() {
    let mut store = GraphStore::new();
    let mut search = FullTextSearchOperator::new("x".into(), "q".into(), 5, "n".into(), "s".into());
    match search.next(&store) {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("None has been created"), "{m}"),
        other => panic!("{other:?}"),
    }
    let mut drop = DropFullTextIndexOperator::new("x".into());
    match drop.next_mut(&mut store, TENANT) {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("None exists."), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn create_node_operator_emits_one_row_and_reset_replays_it() {
    let mut store = GraphStore::new();
    let mut op = CreateNodeOperator::new(vec![
        (
            vec![Label::new("A")],
            HashMap::from([("k".to_string(), PropertyValue::Integer(1))]),
            Some("a".into()),
            None,
        ),
        (vec![], HashMap::new(), None, None),
    ]);
    let rows = drain_mut(&mut op, &mut store);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("a").is_some());
    assert!(rows[0].get("__created_node_1").is_some());
    assert_eq!(store.node_count(), 2);
    op.reset();
    // Reset replays the row without creating the nodes again.
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(store.node_count(), 2);
}

#[test]
fn create_node_operator_refuses_an_unbound_property_reference() {
    let mut store = GraphStore::new();
    let exprs = HashMap::from([(
        "k".to_string(),
        Expression::Property {
            variable: "ghost".into(),
            property: "x".into(),
        },
    )]);
    let mut op = CreateNodeOperator::new(vec![(
        vec![Label::new("A")],
        HashMap::new(),
        Some("a".into()),
        Some(exprs),
    )]);
    match op.next_mut(&mut store, TENANT) {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("CREATE property `k`"), "{m}"),
        other => panic!("{other:?}"),
    }
    // The half-built node is removed again.
    assert_eq!(
        read(&store, "MATCH (n:A) RETURN count(n) AS c")
            .records
            .len(),
        1
    );
    assert_eq!(
        int(&read(&store, "MATCH (n:A) RETURN count(n) AS c"), 0, "c"),
        0
    );
}

// ---------------------------------------------------------------------------
// CreateEdgeOperator (never produced by the planner)
// ---------------------------------------------------------------------------

#[test]
fn create_edge_operator_wires_each_input_row() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let mut r1 = Record::new();
    r1.bind("s", Value::NodeRef(a));
    r1.bind("t", Value::NodeRef(b));
    let mut r2 = Record::new();
    r2.bind("s", Value::NodeRef(b));
    r2.bind("t", Value::NodeRef(c));
    let input: OperatorBox = Box::new(MaterializedOperator::new(vec![r1, r2]));
    let mut op = CreateEdgeOperator::new(
        Some(input),
        "s".into(),
        "t".into(),
        EdgeType::new("R"),
        HashMap::from([("w".to_string(), PropertyValue::Integer(5))]),
        Some("e".into()),
    );
    assert_eq!(op.children_mut().len(), 1);
    let rows = drain_mut(&mut op, &mut store);
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert!(matches!(r.get("e"), Some(Value::Edge(..))));
    }
    assert_eq!(
        int(
            &read(&store, "MATCH ()-[r:R]->() RETURN sum(r.w) AS s"),
            0,
            "s"
        ),
        10
    );

    // Reset re-reads the input and creates the edges again.
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 2);
    assert_eq!(
        int(
            &read(&store, "MATCH ()-[r:R]->() RETURN count(r) AS c"),
            0,
            "c"
        ),
        4
    );
}

#[test]
fn create_edge_operator_without_input_or_variable() {
    let mut store = GraphStore::new();
    let mut op = CreateEdgeOperator::new(
        None,
        "s".into(),
        "t".into(),
        EdgeType::new("R"),
        HashMap::new(),
        None,
    );
    assert!(op.children_mut().is_empty());
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    op.reset();

    let a = store.create_node("N");
    let mut r = Record::new();
    r.bind("s", Value::NodeRef(a));
    r.bind("t", Value::NodeRef(a));
    let mut op = CreateEdgeOperator::new(
        Some(Box::new(MaterializedOperator::new(vec![r]))),
        "s".into(),
        "t".into(),
        EdgeType::new("SELF"),
        HashMap::new(),
        None,
    );
    let rows = drain_mut(&mut op, &mut store);
    assert!(rows[0].get("__created_edge_0").is_some());
}

#[test]
fn create_edge_operator_errors_on_missing_or_non_node_endpoints() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let cases: Vec<(Record, &str)> = vec![
        (a_record("t", Value::NodeRef(a)), "Variable not found: s"),
        (a_record("s", Value::NodeRef(a)), "Variable not found: t"),
        (
            {
                let mut r = a_record("s", Value::Property(PropertyValue::Integer(1)));
                r.bind("t", Value::NodeRef(a));
                r
            },
            "s is not a node",
        ),
        (
            {
                let mut r = a_record("s", Value::NodeRef(a));
                r.bind("t", Value::Property(PropertyValue::Integer(1)));
                r
            },
            "t is not a node",
        ),
    ];
    for (rec, want) in cases {
        let mut op = CreateEdgeOperator::new(
            Some(Box::new(MaterializedOperator::new(vec![rec]))),
            "s".into(),
            "t".into(),
            EdgeType::new("R"),
            HashMap::new(),
            None,
        );
        let e = op.next_mut(&mut store, TENANT).unwrap_err().to_string();
        assert!(e.contains(want), "{e} !~ {want}");
    }
}

// ---------------------------------------------------------------------------
// catalog / introspection operators
// ---------------------------------------------------------------------------

#[test]
fn show_operators_list_catalog_sorted_and_reset() {
    let store = people();
    let mut labels = ShowLabelsOperator::new();
    assert_eq!(labels.describe().name, "ShowLabels");
    let got: Vec<String> = drain(&mut labels, &store)
        .iter()
        .map(|r| prop_str(r, "label"))
        .collect();
    assert_eq!(got, vec!["Company", "Person"]);
    labels.reset();
    assert_eq!(drain(&mut labels, &store).len(), 2);

    let mut types = ShowRelationshipTypesOperator::new();
    assert_eq!(types.describe().name, "ShowRelationshipTypes");
    let got: Vec<String> = drain(&mut types, &store)
        .iter()
        .map(|r| prop_str(r, "relationshipType"))
        .collect();
    assert_eq!(got, vec!["KNOWS", "WORKS_AT"]);
    types.reset();
    assert_eq!(drain(&mut types, &store).len(), 2);

    let mut keys = ShowPropertyKeysOperator::new();
    assert_eq!(keys.describe().name, "ShowPropertyKeys");
    let got: Vec<String> = drain(&mut keys, &store)
        .iter()
        .map(|r| prop_str(r, "propertyKey"))
        .collect();
    assert!(got.contains(&"since".to_string()), "{got:?}");
    assert!(got.contains(&"name".to_string()), "{got:?}");
    keys.reset();
    assert!(!drain(&mut keys, &store).is_empty());

    let mut viz = SchemaVisualizationOperator::new();
    assert_eq!(viz.describe().name, "SchemaVisualization");
    let got: Vec<String> = drain(&mut viz, &store)
        .iter()
        .map(|r| {
            format!(
                "{}-{}-{}",
                prop_str(r, "source_label"),
                prop_str(r, "relationship_type"),
                prop_str(r, "target_label")
            )
        })
        .collect();
    assert_eq!(got, vec!["Person-KNOWS-Person", "Person-WORKS_AT-Company"]);
    viz.reset();
    assert_eq!(drain(&mut viz, &store).len(), 2);

    let mut idx = ShowIndexesOperator::new();
    assert_eq!(idx.describe().name, "ShowIndexes");
    assert!(drain(&mut idx, &store).is_empty());
    idx.reset();

    let mut cons = ShowConstraintsOperator::new();
    assert_eq!(cons.describe().name, "ShowConstraints");
    assert!(drain(&mut cons, &store).is_empty());
    cons.reset();
}

#[test]
fn check_integrity_on_a_sound_store_yields_nothing() {
    let store = people();
    let mut op = CheckIntegrityOperator::default();
    let d = op.describe();
    assert_eq!(d.name, "CheckIntegrity");
    assert!(d.details.contains("two existing nodes"));
    assert!(drain(&mut op, &store).is_empty());
    op.reset();
    assert!(op.next(&store).unwrap().is_none());
}

#[test]
fn single_row_and_materialized_operators() {
    let store = GraphStore::new();
    let mut one = SingleRowOperator::new();
    assert_eq!(one.describe().name, "SingleRow");
    assert_eq!(drain(&mut one, &store).len(), 1);
    one.reset();
    assert_eq!(drain(&mut one, &store).len(), 1);

    let rows = vec![a_record("x", Value::Property(PropertyValue::Integer(1))); 3];
    let mut m = MaterializedOperator::new(rows);
    assert!(m.is_materialized());
    let d = m.describe();
    assert_eq!(
        (d.name.as_str(), d.details.as_str()),
        ("Materialized", "3 rows")
    );
    assert_eq!(drain(&mut m, &store).len(), 3);
    m.reset();
    assert_eq!(drain(&mut m, &store).len(), 3);
}

// ---------------------------------------------------------------------------
// SchemaForLlmOperator
// ---------------------------------------------------------------------------

fn schema_text(store: &GraphStore, budget: usize) -> (String, i64, bool) {
    let mut op = SchemaForLlmOperator::new(budget);
    let r = op.next(store).unwrap().unwrap();
    assert!(op.next(store).unwrap().is_none());
    let text = prop_str(&r, "schema");
    let toks = match r.get("estimated_tokens") {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("{other:?}"),
    };
    let complete = match r.get("complete") {
        Some(Value::Property(PropertyValue::Boolean(b))) => *b,
        other => panic!("{other:?}"),
    };
    (text, toks, complete)
}

#[test]
fn schema_for_llm_describes_labels_types_and_an_example() {
    let mut store = people();
    let n = store.create_node("Typed");
    let long = "x".repeat(60);
    store
        .set_node_property(TENANT, n, "s", long.as_str())
        .unwrap();
    store.set_node_property(TENANT, n, "f", 1.5f64).unwrap();
    store.set_node_property(TENANT, n, "b", true).unwrap();
    store
        .set_node_property(TENANT, n, "d", PropertyValue::Date(10))
        .unwrap();
    store
        .set_node_property(
            TENANT,
            n,
            "arr",
            PropertyValue::Array(vec![PropertyValue::Integer(1)]),
        )
        .unwrap();
    let (text, toks, complete) = schema_text(&store, SchemaForLlmOperator::DEFAULT_BUDGET);
    assert!(complete, "{text}");
    assert!(toks > 0);
    assert!(
        text.starts_with("graph: 5 nodes, 3 edges, 3 labels, 2 relationship types"),
        "{text}"
    );
    assert!(text.contains("(:Person) 3"), "{text}");
    assert!(text.contains("name:String"), "{text}");
    assert!(text.contains("age:Integer"), "{text}");
    assert!(text.contains("f:Float"), "{text}");
    assert!(text.contains("b:Boolean"), "{text}");
    assert!(text.contains("d:Date"), "{text}");
    assert!(text.contains("arr:Array"), "{text}");
    // A long sample is cut to 31 bytes and an ellipsis.
    assert!(text.contains(&format!("\"{}…", "x".repeat(30))), "{text}");
    assert!(text.contains("(:Person)-[:KNOWS]->(:Person) 2"), "{text}");
    assert!(
        text.contains("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b LIMIT 10"),
        "{text}"
    );
    assert!(!text.contains("truncated"), "{text}");

    let mut op = SchemaForLlmOperator::new(77);
    assert_eq!(op.describe().details, "token_budget=77");
    assert!(op.next(&store).unwrap().is_some());
    op.reset();
    assert!(op.next(&store).unwrap().is_some());
}

#[test]
fn schema_for_llm_truncates_and_says_so() {
    let store = people();
    let (text, _, complete) = schema_text(&store, SchemaForLlmOperator::MIN_BUDGET);
    assert!(!complete);
    assert!(
        text.contains("-- truncated to fit a 50-token budget"),
        "{text}"
    );
    assert!(text.contains("Ask again with a larger budget"), "{text}");
}

#[test]
fn schema_for_llm_truncates_relationships_when_labels_fit() {
    let store = people();
    // Find a budget that fits the labels but not every relationship line or
    // the example; walk down from a full budget until truncation starts.
    let (full, full_toks, _) = schema_text(&store, 2000);
    assert!(full.contains("example:"));
    let mut saw_rel_cut = false;
    for budget in (40..(full_toks as usize + 41)).rev() {
        let (text, _, complete) = schema_text(&store, budget);
        if !complete && text.contains("(:Person)") && text.contains("truncated") {
            saw_rel_cut = true;
            assert!(!text.contains("example:\n  MATCH"), "{text}");
            break;
        }
    }
    assert!(saw_rel_cut);
}

#[test]
fn schema_for_llm_type_names_cover_every_variant() {
    use SchemaForLlmOperator as S;
    assert_eq!(S::type_name(&PropertyValue::String("a".into())), "String");
    assert_eq!(S::type_name(&PropertyValue::Integer(1)), "Integer");
    assert_eq!(S::type_name(&PropertyValue::Float(1.0)), "Float");
    assert_eq!(S::type_name(&PropertyValue::Boolean(true)), "Boolean");
    assert_eq!(S::type_name(&PropertyValue::DateTime(0)), "DateTime");
    assert_eq!(S::type_name(&PropertyValue::Date(0)), "Date");
    assert_eq!(S::type_name(&PropertyValue::LocalTime(0)), "LocalTime");
    assert_eq!(S::type_name(&PropertyValue::Array(vec![])), "Array");
    assert_eq!(S::type_name(&PropertyValue::Vector(vec![1.0])), "Vector");
    assert_eq!(S::type_name(&PropertyValue::Map(Default::default())), "Map");
    assert_eq!(S::type_name(&PropertyValue::Null), "Null");
    assert_eq!(
        S::type_name(&PropertyValue::Time {
            nanos: 0,
            offset_seconds: 0
        }),
        "Time"
    );
    assert_eq!(
        S::type_name(&PropertyValue::LocalDateTime { secs: 0, nanos: 0 }),
        "LocalDateTime"
    );
    assert_eq!(
        S::type_name(&PropertyValue::ZonedDateTime {
            secs: 0,
            nanos: 0,
            offset_seconds: 0,
            zone: None
        }),
        "ZonedDateTime"
    );
    assert_eq!(
        S::type_name(&PropertyValue::Duration {
            months: 0,
            days: 0,
            seconds: 0,
            nanos: 0
        }),
        "Duration"
    );
    assert_eq!(S::sample(&PropertyValue::Integer(42)), "42");
    assert_eq!(S::sample(&PropertyValue::String("hi".into())), "\"hi\"");
}
