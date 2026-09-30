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

// ---------------------------------------------------------------------------
// AlgorithmOperator
// ---------------------------------------------------------------------------

fn li(i: i64) -> Expression {
    Expression::Literal(PropertyValue::Integer(i))
}

fn ls(s: &str) -> Expression {
    Expression::Literal(PropertyValue::String(s.to_string()))
}

fn lmap(entries: &[(&str, PropertyValue)]) -> Expression {
    Expression::Literal(PropertyValue::Map(
        entries.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
    ))
}

fn larr(items: Vec<PropertyValue>) -> Expression {
    Expression::Literal(PropertyValue::Array(items))
}

fn pint(i: i64) -> PropertyValue {
    PropertyValue::Integer(i)
}

fn pstr(s: &str) -> PropertyValue {
    PropertyValue::String(s.to_string())
}

/// A graph of `n` `:V` nodes and the given directed `:E` edges. Each edge
/// carries `w` (its weight, from `weights` or 1) and `t` (a time: 10, 20, ...
/// in edge order). Returns the store and the node ids as `i64`s.
fn algo_graph(n: usize, edges: &[(usize, usize)], weights: Option<&[f64]>) -> (GraphStore, Vec<i64>) {
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..n)
        .map(|i| {
            let id = store.create_node("V");
            store.set_node_property(TENANT, id, "x", i as i64).unwrap();
            store.set_node_property(TENANT, id, "y", (i * i) as f64).unwrap();
            id
        })
        .collect();
    for (k, &(a, b)) in edges.iter().enumerate() {
        let e = store.create_edge(ids[a], ids[b], "E").unwrap();
        let w = weights.map(|ws| ws[k]).unwrap_or(1.0);
        store.set_edge_property(e, "w", w).unwrap();
        store.set_edge_property(e, "t", (k as i64 + 1) * 10).unwrap();
    }
    (store, ids.iter().map(|i| i.as_u64() as i64).collect())
}

/// A directed path 0 -> 1 -> 2 -> 3.
fn path4() -> (GraphStore, Vec<i64>) {
    algo_graph(4, &[(0, 1), (1, 2), (2, 3)], None)
}

/// A directed triangle 0 -> 1 -> 2 -> 0.
fn triangle() -> (GraphStore, Vec<i64>) {
    algo_graph(3, &[(0, 1), (1, 2), (2, 0)], None)
}

fn run_algo(store: &GraphStore, name: &str, args: Vec<Expression>) -> ExecutionResult<Vec<Record>> {
    let mut op = AlgorithmOperator::new(name.to_string(), args);
    let mut out = Vec::new();
    while let Some(r) = op.next(store)? {
        out.push(r);
    }
    Ok(out)
}

fn run_algo_mut(store: &mut GraphStore, name: &str, args: Vec<Expression>) -> ExecutionResult<Vec<Record>> {
    let mut op = AlgorithmOperator::new(name.to_string(), args);
    let mut out = Vec::new();
    while let Some(r) = op.next_mut(store, TENANT)? {
        out.push(r);
    }
    Ok(out)
}

/// Run on both paths and check they agree on the row count.
fn algo_rows(store: &mut GraphStore, name: &str, args: Vec<Expression>) -> Vec<Record> {
    let a = run_algo(store, name, args.clone()).unwrap_or_else(|e| panic!("{name}: {e}"));
    let b = run_algo_mut(store, name, args).unwrap_or_else(|e| panic!("{name} (mut): {e}"));
    assert_eq!(a.len(), b.len(), "{name}: read and write paths disagree");
    a
}

/// The error on both paths, which must agree.
fn algo_err(store: &mut GraphStore, name: &str, args: Vec<Expression>) -> String {
    let a = match run_algo(store, name, args.clone()) {
        Ok(r) => panic!("{name} succeeded with {} rows", r.len()),
        Err(e) => e.to_string(),
    };
    let b = match run_algo_mut(store, name, args) {
        Ok(r) => panic!("{name} (mut) succeeded with {} rows", r.len()),
        Err(e) => e.to_string(),
    };
    assert_eq!(a, b);
    a
}

fn rec_int(r: &Record, col: &str) -> i64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Integer(i))) => *i,
        other => panic!("{col}: {other:?}"),
    }
}

fn rec_float(r: &Record, col: &str) -> f64 {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Float(f))) => *f,
        Some(Value::Property(PropertyValue::Integer(i))) => *i as f64,
        other => panic!("{col}: {other:?}"),
    }
}

fn rec_node(r: &Record, col: &str) -> i64 {
    r.get(col).and_then(|v| v.node_id()).unwrap_or_else(|| panic!("{col} not a node")).as_u64() as i64
}

fn rec_ints(r: &Record, col: &str) -> Vec<i64> {
    match r.get(col) {
        Some(Value::Property(PropertyValue::Array(a))) => a
            .iter()
            .map(|p| match p {
                PropertyValue::Integer(i) => *i,
                other => panic!("{other:?}"),
            })
            .collect(),
        other => panic!("{col}: {other:?}"),
    }
}

#[test]
fn algorithm_names_canonicalise_and_classify() {
    assert_eq!(AlgorithmOperator::canonical_name("algo.pageRank"), "pagerank");
    assert_eq!(AlgorithmOperator::canonical_name("samyama.WCC"), "wcc");
    assert_eq!(AlgorithmOperator::canonical_name("gds.pageRank.stream"), "pagerank");
    assert_eq!(AlgorithmOperator::canonical_name("gds.alpha.localClusteringCoefficient.stream"), "lcc");
    assert_eq!(AlgorithmOperator::canonical_name("gds.beta.spanningTree"), "mst");
    assert_eq!(AlgorithmOperator::canonical_name("gds.fastRP.stream"), "__gds_divergent__fastrp");
    assert!(!AlgorithmOperator::is_algorithm("gds.node2vec.stream"));
    assert!(AlgorithmOperator::is_algorithm("gds.shortestPath.dijkstra.stream"));
    assert!(AlgorithmOperator::is_algorithm("algo.hubsAndAuthorities"));
    assert!(!AlgorithmOperator::is_algorithm("algo.nope"));
    assert_eq!(
        AlgorithmOperator::unsupported_gds_mode("gds.alpha.pageRank.WRITE"),
        Some(("write", "pagerank".to_string()))
    );
    assert_eq!(AlgorithmOperator::unsupported_gds_mode("gds.pageRank.stream"), None);
    assert_eq!(AlgorithmOperator::unsupported_gds_mode("algo.pageRank.write"), None);
    assert_eq!(AlgorithmOperator::unsupported_gds_mode("gds.pageRank"), None);
    assert!(AlgorithmOperator::procedure_is_mutating("samyama.OR.Solve"));
    assert!(AlgorithmOperator::procedure_is_mutating("algo.fastRP"));
    assert!(!AlgorithmOperator::procedure_is_mutating("algo.pageRank"));
    assert!(AlgorithmOperator::new("algo.node2vec".into(), vec![]).is_mutating());
    assert!(!AlgorithmOperator::new("algo.wcc".into(), vec![]).is_mutating());
    assert!(AlgorithmOperator::is_known_solver("pso"));
    assert!(!AlgorithmOperator::is_known_solver("simplex"));
}

#[test]
fn unknown_algorithms_get_a_hint_and_the_catalogue() {
    let (mut store, _) = path4();
    for (name, hint) in [
        ("algo.bfs", "use algo.shortestPath"),
        ("algo.dijkstra", "use algo.weightedPath"),
        ("algo.prank", "did you mean algo.pageRank?"),
        ("algo.components", "use algo.wcc or algo.scc"),
        ("algo.zzz", "Available: pageRank"),
    ] {
        let e = algo_err(&mut store, name, vec![]);
        assert!(e.contains(&format!("Unknown algorithm: {name}")), "{e}");
        assert!(e.contains(hint), "{e}");
    }
}

#[test]
fn mutating_algorithms_are_refused_on_the_read_path() {
    let (store, _) = path4();
    for name in ["algo.or.solve", "algo.fastRP", "algo.node2vec"] {
        match run_algo(&store, name, vec![]) {
            Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("requires write access"), "{m}"),
            other => panic!("{name}: {other:?}"),
        }
    }
}

#[test]
fn yield_aliases_rename_and_reset_reruns() {
    let (store, ids) = path4();
    let items = vec![
        crate::query::ast::YieldItem { name: "node".into(), alias: Some("n".into()) },
        crate::query::ast::YieldItem { name: "componentId".into(), alias: None },
    ];
    let mut op = AlgorithmOperator::new("algo.wcc".into(), vec![]).with_aliases(&items);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 4);
    assert_eq!(rec_node(&rows[0], "n"), ids[0]);
    assert_eq!(rec_int(&rows[0], "componentId"), rec_int(&rows[3], "componentId"));
    op.reset();
    assert_eq!(drain(&mut op, &store).len(), 4);
}

#[test]
fn pagerank_config_label_and_edge_type() {
    let (mut store, _) = triangle();
    let rows = algo_rows(
        &mut store,
        "algo.pageRank",
        vec![ls("V"), ls("E"), lmap(&[("iterations", pint(5)), ("damping", PropertyValue::Float(0.5))])],
    );
    assert_eq!(rows.len(), 3);
    // A directed cycle: every node has the same score.
    let s0 = rec_float(&rows[0], "score");
    assert!(rows.iter().all(|r| (rec_float(r, "score") - s0).abs() < 1e-9));
}

#[test]
fn unknown_config_keys_are_refused_naming_the_rest() {
    let (mut store, _) = path4();
    let e = algo_err(&mut store, "algo.pageRank", vec![lmap(&[("writeProperty", pstr("pr")), ("zeta", pint(1))])]);
    assert!(e.contains("unknown config key `writeProperty` (and 1 more: zeta)"), "{e}");
    assert!(e.contains("This algorithm reads: damping, iterations"), "{e}");
    let e = algo_err(&mut store, "algo.wcc", vec![lmap(&[("bogus", pint(1))])]);
    assert!(e.contains("unknown config key `bogus`."), "{e}");
}

#[test]
fn shortest_path_argument_errors_and_weights() {
    let (mut store, ids) = algo_graph(3, &[(0, 1), (1, 2), (0, 2)], Some(&[1.0, 1.0, 5.0]));
    assert!(algo_err(&mut store, "algo.shortestPath", vec![li(ids[0])]).contains("requires source and target"));
    assert!(algo_err(&mut store, "algo.shortestPath", vec![ls("a"), li(ids[1])]).contains("Source must be integer ID"));
    assert!(algo_err(&mut store, "algo.shortestPath", vec![li(ids[0]), ls("b")]).contains("Target must be integer ID"));

    let rows = algo_rows(&mut store, "algo.shortestPath", vec![li(ids[0]), li(ids[2])]);
    assert_eq!(rec_ints(&rows[0], "path"), vec![ids[0], ids[2]]);
    let rows = algo_rows(
        &mut store,
        "algo.shortestPath",
        vec![li(ids[0]), li(ids[2]), lmap(&[("weight_property", pstr("w"))])],
    );
    assert_eq!(rec_ints(&rows[0], "path"), vec![ids[0], ids[1], ids[2]]);
    assert_eq!(rec_float(&rows[0], "cost"), 2.0);
    // Unreachable (edges are directed): no rows.
    assert!(algo_rows(&mut store, "algo.shortestPath", vec![li(ids[2]), li(ids[0])]).is_empty());
}

#[test]
fn negative_weights_are_refused_by_dijkstra_style_algorithms() {
    let (mut store, ids) = algo_graph(3, &[(0, 1), (1, 2)], Some(&[1.0, -2.0]));
    let e = algo_err(
        &mut store,
        "algo.shortestPath",
        vec![li(ids[0]), li(ids[2]), lmap(&[("weight_property", pstr("w"))])],
    );
    assert!(e.contains("holds a negative weight"), "{e}");
    let e = algo_err(&mut store, "algo.weightedPath", vec![li(ids[0]), li(ids[2]), ls("w")]);
    assert!(e.contains("weightedPath: edge property `w` holds a negative weight"), "{e}");
    let e = algo_err(
        &mut store,
        "algo.yens",
        vec![li(ids[0]), li(ids[2]), lmap(&[("weightProperty", pstr("w"))])],
    );
    assert!(e.contains("negative weight"), "{e}");
    // All-shortest-paths counts hops and is unaffected.
    let rows = algo_rows(
        &mut store,
        "algo.allShortestPaths",
        vec![li(ids[0]), li(ids[2]), lmap(&[("weightProperty", pstr("w"))])],
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rec_int(&rows[0], "cost"), 2);
}

#[test]
fn weighted_path_argument_errors_and_result() {
    let (mut store, ids) = algo_graph(3, &[(0, 1), (1, 2), (0, 2)], Some(&[1.0, 1.0, 5.0]));
    assert!(algo_err(&mut store, "algo.weightedPath", vec![li(ids[0]), li(ids[1])])
        .contains("requires source, target, and weight"));
    assert!(algo_err(&mut store, "algo.weightedPath", vec![ls("x"), li(ids[1]), ls("w")]).contains("Source must be integer ID"));
    assert!(algo_err(&mut store, "algo.weightedPath", vec![li(ids[0]), ls("x"), ls("w")]).contains("Target must be integer ID"));
    assert!(algo_err(&mut store, "algo.weightedPath", vec![li(ids[0]), li(ids[1]), li(3)])
        .contains("Weight property must be a string"));
    let rows = algo_rows(&mut store, "algo.weightedPath", vec![li(ids[0]), li(ids[2]), ls("w")]);
    assert_eq!(rec_float(&rows[0], "cost"), 2.0);
    assert_eq!(rec_ints(&rows[0], "path"), vec![ids[0], ids[1], ids[2]]);
    assert!(algo_rows(&mut store, "algo.weightedPath", vec![li(ids[2]), li(ids[0]), ls("w")]).is_empty());
}

#[test]
fn wcc_scc_cdlp_lcc_and_triangles() {
    // Two components: a directed triangle and a lone edge.
    let (mut store, ids) = algo_graph(5, &[(0, 1), (1, 2), (2, 0), (3, 4)], None);
    let rows = algo_rows(&mut store, "algo.wcc", vec![ls("V"), ls("E")]);
    assert_eq!(rows.len(), 5);
    let comps: HashSet<i64> = rows.iter().map(|r| rec_int(r, "componentId")).collect();
    assert_eq!(comps.len(), 2);

    let rows = algo_rows(&mut store, "algo.scc", vec![]);
    let comps: HashSet<i64> = rows.iter().map(|r| rec_int(r, "componentId")).collect();
    // The triangle is one SCC; 3 and 4 are each their own.
    assert_eq!(comps.len(), 3);

    let rows = algo_rows(&mut store, "algo.cdlp", vec![ls("V"), ls("E"), lmap(&[("maxIterations", pint(3))])]);
    assert_eq!(rows.len(), 5);
    let rows = algo_rows(&mut store, "algo.labelPropagation", vec![]);
    assert_eq!(rows.len(), 5);

    let rows = algo_rows(&mut store, "algo.lcc", vec![ls("V"), ls("E")]);
    assert_eq!(rows.len(), 5);
    // Triangle members have coefficient 1 and sort first.
    assert_eq!(rec_float(&rows[0], "coefficient"), 1.0);
    assert!(rows.iter().any(|r| rec_node(r, "node") == ids[3] && rec_float(r, "coefficient") == 0.0));

    let rows = algo_rows(&mut store, "algo.triangleCount", vec![]);
    assert_eq!(rec_int(&rows[0], "triangles"), 1);
}

#[test]
fn mst_and_max_flow() {
    let (mut store, ids) = algo_graph(3, &[(0, 1), (1, 2), (0, 2)], Some(&[1.0, 2.0, 5.0]));
    let rows = algo_rows(&mut store, "algo.mst", vec![ls("w")]);
    assert_eq!(rec_float(&rows[0], "total_weight"), 3.0);
    assert_eq!(rec_int(&rows[0], "components"), 1);
    assert_eq!(rows.len(), 3);
    let rows = algo_rows(&mut store, "algo.mst", vec![li(1)]);
    assert_eq!(rec_float(&rows[0], "total_weight"), 2.0);

    assert!(algo_err(&mut store, "algo.maxFlow", vec![li(ids[0])]).contains("requires source and sink"));
    assert!(algo_err(&mut store, "algo.maxFlow", vec![ls("a"), li(ids[1])]).contains("Source must be integer ID"));
    assert!(algo_err(&mut store, "algo.maxFlow", vec![li(ids[0]), ls("b")]).contains("Sink must be integer ID"));
    assert!(algo_err(&mut store, "algo.maxFlow", vec![li(ids[0]), li(ids[0])]).contains("source and sink are the same node"));
    assert!(algo_err(&mut store, "algo.maxFlow", vec![li(ids[0]), li(999)]).contains("no node 999"));
    assert!(algo_err(&mut store, "algo.maxFlow", vec![li(999), li(ids[0])]).contains("no node 999"));
    let rows = algo_rows(&mut store, "algo.maxFlow", vec![li(ids[0]), li(ids[2]), ls("w")]);
    assert_eq!(rec_float(&rows[0], "max_flow"), 6.0);
    let rows = algo_rows(&mut store, "algo.maxFlow", vec![li(ids[0]), li(ids[2]), li(7)]);
    assert_eq!(rec_float(&rows[0], "max_flow"), 2.0);
}

#[test]
fn temporal_algorithms() {
    // 0 -(t=10)-> 1 -(t=20)-> 2 -(t=30)-> 3
    let (mut store, ids) = path4();
    let cfg = lmap(&[("timeProperty", pstr("t")), ("label", pstr("V")), ("edgeType", pstr("E"))]);
    // Every node but the source is reached.
    let rows = algo_rows(&mut store, "algo.temporalReachability", vec![li(ids[0]), cfg.clone()]);
    assert_eq!(rows.len(), 3);
    let last = rows.iter().find(|r| rec_node(r, "node") == ids[3]).unwrap();
    assert_eq!(rec_int(last, "time"), 30);
    assert_eq!(rec_ints(last, "path"), ids);
    assert_eq!(rec_ints(last, "times"), vec![10, 20, 30]);

    let ranked = algo_rows(&mut store, "algo.propagationRanking", vec![li(ids[0]), cfg.clone()]);
    assert_eq!(rec_int(&ranked[0], "rank"), 1);

    // Starting after the first edge fired, nothing is reachable.
    let late = lmap(&[("timeProperty", pstr("t")), ("startTime", pint(15))]);
    let rows = algo_rows(&mut store, "algo.temporalReachability", vec![li(ids[0]), late]);
    assert!(rows.is_empty());
    let dt = lmap(&[("timeProperty", pstr("t")), ("startTime", PropertyValue::DateTime(0))]);
    assert_eq!(algo_rows(&mut store, "algo.temporalReachability", vec![li(ids[0]), dt]).len(), 3);

    let rows = algo_rows(&mut store, "algo.temporalShortestPath", vec![li(ids[0]), li(ids[3]), cfg.clone()]);
    assert_eq!(rec_ints(&rows[0], "path"), ids);
    assert_eq!(rec_ints(&rows[0], "times"), vec![10, 20, 30]);
    assert_eq!(rec_int(&rows[0], "arrival"), 30);
    // No time-respecting route backwards: no rows.
    assert!(algo_rows(&mut store, "algo.temporalShortestPath", vec![li(ids[3]), li(ids[0]), cfg.clone()]).is_empty());

    let e = algo_err(&mut store, "algo.temporalShortestPath", vec![li(ids[0])]);
    assert!(e.contains("requires a target as argument 2"), "{e}");
    let e = algo_err(&mut store, "algo.temporalReachability", vec![li(999)]);
    assert!(e.contains("node 999 is not in the projected graph"), "{e}");

    let rows = algo_rows(
        &mut store,
        "algo.symptomExplanation",
        vec![larr(vec![PropertyValue::Array(vec![pint(ids[3]), pint(100)])]), cfg.clone()],
    );
    assert!(!rows.is_empty());
    assert!(rows.iter().any(|r| rec_node(r, "node") == ids[0]));
    assert!(rows.iter().all(|r| rec_int(r, "explains") == 1));

    for (args, want) in [
        (vec![li(1)], "requires a list of [nodeId, seenAt] pairs"),
        (vec![larr(vec![pint(1)])], "each symptom must be a [nodeId, seenAt] pair"),
        (vec![larr(vec![PropertyValue::Array(vec![pstr("a"), pint(1)])])], "pair of integers"),
        (
            vec![larr(vec![PropertyValue::Array(vec![pint(999), pint(1)])])],
            "symptom node 999 is not in the projected graph",
        ),
    ] {
        let e = algo_err(&mut store, "algo.symptomExplanation", args);
        assert!(e.contains(want), "{e} !~ {want}");
    }
}

#[test]
fn centrality_family() {
    // A star: 0 is joined to 1, 2, 3.
    let (mut store, ids) = algo_graph(4, &[(0, 1), (0, 2), (0, 3)], None);
    for name in [
        "algo.degree",
        "algo.closeness",
        "algo.betweenness",
        "algo.harmonic",
        "algo.kcore",
    ] {
        let rows = algo_rows(
            &mut store,
            name,
            vec![ls("V"), lmap(&[("edgeType", pstr("E")), ("undirected", PropertyValue::Boolean(true))])],
        );
        assert_eq!(rows.len(), 4, "{name}");
        assert_eq!(rec_node(&rows[0], "node"), ids[0], "{name}: the hub ranks first");
    }
    let rows = algo_rows(
        &mut store,
        "algo.degreeCentrality",
        vec![lmap(&[("label", pstr("V")), ("undirected", PropertyValue::Boolean(false))])],
    );
    assert_eq!(rows.len(), 4);
    let e = algo_err(&mut store, "algo.degree", vec![lmap(&[("iterations", pint(1))])]);
    assert!(e.contains("unknown config key `iterations`"), "{e}");

    // A star is bipartite, so the power iteration oscillates and never settles.
    let e = algo_err(&mut store, "algo.eigenvector", vec![]);
    assert!(e.contains("did not converge"), "{e}");
    let (mut tri, _) = triangle();
    assert_eq!(algo_rows(&mut tri, "algo.eigenvector", vec![]).len(), 3);
    let (mut empty, _) = algo_graph(3, &[], None);
    let e = algo_err(&mut empty, "algo.eigenvector", vec![]);
    assert!(e.contains("did not converge"), "{e}");
}

#[test]
fn link_prediction_pairs_and_rankings() {
    // 0 and 2 share neighbour 1; 3 is joined to 1 too.
    let (mut store, ids) = algo_graph(4, &[(0, 1), (2, 1), (3, 1)], None);
    for name in ["algo.commonNeighbors", "algo.jaccard", "algo.adamicAdar"] {
        let rows = algo_rows(&mut store, name, vec![li(ids[0]), li(ids[2])]);
        assert_eq!(rows.len(), 1);
        assert_eq!(rec_node(&rows[0], "node1"), ids[0]);
        assert_eq!(rec_node(&rows[0], "node2"), ids[2]);
        assert!(rec_float(&rows[0], "score") > 0.0, "{name}");
        let ranked = algo_rows(
            &mut store,
            name,
            vec![lmap(&[("limit", pint(2)), ("label", pstr("V")), ("edgeType", pstr("E"))])],
        );
        assert_eq!(ranked.len(), 2, "{name}");
    }
    let e = algo_err(&mut store, "algo.jaccard", vec![li(ids[0]), li(999)]);
    assert!(e.contains("node 999 is not in the projected graph"), "{e}");
    let e = algo_err(&mut store, "algo.jaccard", vec![lmap(&[("k", pint(1))])]);
    assert!(e.contains("unknown config key `k`"), "{e}");
}

#[test]
fn structural_algorithms_on_a_path_and_a_cycle() {
    let (mut dag, ids) = path4();
    let rows = algo_rows(&mut dag, "algo.topologicalSort", vec![ls("V")]);
    assert_eq!(rows.iter().map(|r| rec_node(r, "node")).collect::<Vec<_>>(), ids);
    assert_eq!(rec_int(&rows[3], "position"), 3);
    assert!(algo_rows(&mut dag, "algo.findCycle", vec![]).is_empty());
    let bridges = algo_rows(&mut dag, "algo.bridges", vec![lmap(&[("label", pstr("V")), ("edgeType", pstr("E"))])]);
    assert_eq!(bridges.len(), 3);
    let aps = algo_rows(&mut dag, "algo.articulationPoints", vec![]);
    let mut ap_ids: Vec<i64> = aps.iter().map(|r| rec_node(r, "node")).collect();
    ap_ids.sort();
    assert_eq!(ap_ids, vec![ids[1], ids[2]]);

    let (mut cyc, _) = triangle();
    let e = algo_err(&mut cyc, "algo.topologicalSort", vec![]);
    assert!(e.contains("no topological order: the graph has a cycle"), "{e}");
    assert_eq!(algo_rows(&mut cyc, "algo.findCycle", vec![]).len(), 3);
}

#[test]
fn graph_shape_metrics() {
    let (mut store, _) = path4();
    let rows = algo_rows(&mut store, "algo.diameter", vec![]);
    assert_eq!(rec_int(&rows[0], "diameter"), 3);
    let rows = algo_rows(&mut store, "algo.radius", vec![lmap(&[("undirected", PropertyValue::Boolean(true))])]);
    assert_eq!(rec_int(&rows[0], "radius"), 2);
    let rows = algo_rows(&mut store, "algo.eccentricity", vec![]);
    assert_eq!(rows.len(), 4);
    let rows = algo_rows(&mut store, "algo.averageNeighborDegree", vec![]);
    assert_eq!(rows.len(), 4);
    let rows = algo_rows(&mut store, "algo.degreeAssortativity", vec![]);
    assert!(rec_float(&rows[0], "assortativity") < 0.0);
    // Directed: the path's far end cannot reach back, so some eccentricity is null.
    let rows = algo_rows(&mut store, "algo.eccentricity", vec![lmap(&[("undirected", PropertyValue::Boolean(false))])]);
    assert!(rows.iter().any(|r| matches!(r.get("eccentricity"), Some(Value::Property(PropertyValue::Null)))));
    let e = algo_err(&mut store, "algo.diameter", vec![lmap(&[("k", pint(1))])]);
    assert!(e.contains("unknown config key `k`"), "{e}");

    let (mut split, _) = algo_graph(4, &[(0, 1), (2, 3)], None);
    assert!(algo_err(&mut split, "algo.diameter", vec![]).contains("not connected, so it has no diameter"));
    assert!(algo_err(&mut split, "algo.radius", vec![]).contains("no radius"));
    let (mut tri, _) = triangle();
    assert!(algo_err(&mut tri, "algo.degreeAssortativity", vec![]).contains("degree assortativity is undefined"));
}

#[test]
fn louvain_and_modularity() {
    let (mut store, ids) =
        algo_graph(6, &[(0, 1), (1, 2), (2, 0), (3, 4), (4, 5), (5, 3), (2, 3)], None);
    let rows = algo_rows(&mut store, "algo.louvain", vec![]);
    assert_eq!(rows.len(), 6);
    assert!(rec_float(&rows[0], "modularity") > 0.0);

    let partition = larr(
        ids.iter()
            .enumerate()
            .map(|(i, id)| PropertyValue::Array(vec![pint(*id), pint((i / 3) as i64)]))
            .collect(),
    );
    let rows = algo_rows(&mut store, "algo.modularity", vec![partition]);
    assert!(rec_float(&rows[0], "modularity") > 0.3);

    for (args, want) in [
        (vec![], "requires a list of [nodeId, community] pairs"),
        (vec![larr(vec![pint(1)])], "each entry must be a [nodeId, community] pair"),
        (vec![larr(vec![PropertyValue::Array(vec![pint(1)])])], "pair of integers"),
        (
            vec![larr(vec![PropertyValue::Array(vec![pint(999), pint(0)])])],
            "node 999 is not in the projected graph",
        ),
        (vec![larr(vec![PropertyValue::Array(vec![pint(ids[0]), pint(0)])])], "has no community"),
    ] {
        let e = algo_err(&mut store, "algo.modularity", args);
        assert!(e.contains(want), "{e} !~ {want}");
    }

    let (mut bare, bare_ids) = algo_graph(2, &[], None);
    let all = larr(bare_ids.iter().map(|id| PropertyValue::Array(vec![pint(*id), pint(0)])).collect());
    assert!(algo_err(&mut bare, "algo.modularity", vec![all]).contains("undefined on a graph with no edges"));
    let rows = algo_rows(&mut bare, "algo.louvain", vec![]);
    assert!(rows.iter().all(|r| matches!(r.get("modularity"), Some(Value::Property(PropertyValue::Null)))));
}

#[test]
fn path_enumeration_algorithms() {
    // A diamond: 0 -> 1 -> 3 and 0 -> 2 -> 3, the lower branch heavier.
    let (mut store, ids) =
        algo_graph(4, &[(0, 1), (1, 3), (0, 2), (2, 3)], Some(&[1.0, 1.0, 2.0, 2.0]));
    let rows = algo_rows(&mut store, "algo.allShortestPaths", vec![li(ids[0]), li(ids[3]), lmap(&[("limit", pint(5))])]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rec_int(&rows[1], "rank"), 2);

    let rows = algo_rows(
        &mut store,
        "algo.yens",
        vec![li(ids[0]), li(ids[3]), li(2), lmap(&[("weightProperty", pstr("w"))])],
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rec_float(&rows[0], "cost"), 2.0);
    assert_eq!(rec_float(&rows[1], "cost"), 4.0);
    let rows = algo_rows(&mut store, "algo.yens", vec![li(ids[0]), li(ids[3]), lmap(&[("k", pint(1))])]);
    assert_eq!(rows.len(), 1);

    let rows = algo_rows(
        &mut store,
        "algo.aStar",
        vec![li(ids[0]), li(ids[3]), lmap(&[("weightProperty", pstr("w")), ("heuristicProperty", pstr("x"))])],
    );
    assert_eq!(rec_ints(&rows[0], "path"), vec![ids[0], ids[1], ids[3]]);
    let rows = algo_rows(&mut store, "algo.aStar", vec![li(ids[0]), li(ids[3]), lmap(&[("heuristicProperty", pstr("y"))])]);
    assert_eq!(rows.len(), 1);
    // Unreachable: no rows.
    assert!(algo_rows(&mut store, "algo.aStar", vec![li(ids[3]), li(ids[0])]).is_empty());

    assert!(algo_err(&mut store, "algo.yens", vec![li(ids[0])]).contains("requires a source and a target node id"));
    assert!(algo_err(&mut store, "algo.yens", vec![li(ids[0]), li(999)]).contains("node 999 is not in the projected graph"));
    assert!(algo_err(&mut store, "algo.aStar", vec![li(ids[0]), li(ids[3]), lmap(&[("q", pint(1))])])
        .contains("unknown config key `q`"));
}

#[test]
fn random_walk_is_seeded_and_validated() {
    let (mut store, ids) = triangle();
    let cfg = lmap(&[("steps", pint(5)), ("seed", pint(7))]);
    let a = algo_rows(&mut store, "algo.randomWalk", vec![li(ids[0]), cfg.clone()]);
    let b = algo_rows(&mut store, "algo.randomWalk", vec![li(ids[0]), cfg]);
    assert_eq!(a.len(), 6);
    assert_eq!(
        a.iter().map(|r| rec_node(r, "node")).collect::<Vec<_>>(),
        b.iter().map(|r| rec_node(r, "node")).collect::<Vec<_>>()
    );
    assert_eq!(rec_node(&a[0], "node"), ids[0]);
    assert_eq!(rec_int(&a[5], "step"), 5);
    assert!(algo_err(&mut store, "algo.randomWalk", vec![]).contains("requires a source node id"));
    assert!(algo_err(&mut store, "algo.randomWalk", vec![li(999)]).contains("node 999 is not in the projected graph"));
    assert!(algo_err(&mut store, "algo.randomWalk", vec![li(ids[0]), lmap(&[("length", pint(1))])])
        .contains("unknown config key"));
}

#[test]
fn ranking_algorithms_beyond_pagerank() {
    let (mut store, ids) = algo_graph(4, &[(0, 1), (2, 1), (3, 1), (1, 0)], None);
    let rows = algo_rows(
        &mut store,
        "algo.articleRank",
        vec![lmap(&[("dampingFactor", PropertyValue::Float(0.8)), ("iterations", pint(10))])],
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rec_node(&rows[0], "node"), ids[1]);

    let rows = algo_rows(
        &mut store,
        "algo.katz",
        vec![lmap(&[
            ("alpha", PropertyValue::Float(0.05)),
            ("beta", PropertyValue::Float(1.0)),
            ("iterations", pint(500)),
            ("tolerance", PropertyValue::Float(1e-6)),
        ])],
    );
    assert_eq!(rows.len(), 4);
    let e = algo_err(
        &mut store,
        "algo.katz",
        vec![lmap(&[("alpha", PropertyValue::Float(5.0)), ("iterations", pint(20))])],
    );
    assert!(e.contains("did not converge in 20 iterations at alpha=5"), "{e}");

    let rows = algo_rows(
        &mut store,
        "algo.hits",
        vec![lmap(&[("iterations", pint(100)), ("tolerance", PropertyValue::Float(1e-6))])],
    );
    assert_eq!(rows.len(), 4);
    assert_eq!(rec_node(&rows[0], "node"), ids[1], "node 1 is the authority");
    assert!(rec_float(&rows[0], "authority") > 0.0);

    let rows = algo_rows(
        &mut store,
        "algo.personalizedPageRank",
        vec![
            larr(vec![pint(ids[2]), pstr("skip")]),
            li(ids[3]),
            li(999),
            lmap(&[("dampingFactor", PropertyValue::Float(0.85)), ("iterations", pint(50))]),
            ls("ignored"),
        ],
    );
    assert_eq!(rows.len(), 4);

    let rows = algo_rows(&mut store, "algo.voteRank", vec![li(1)]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rec_node(&rows[0], "node"), ids[1]);
    let rows = algo_rows(&mut store, "algo.voteRank", vec![lmap(&[("k", pint(2))]), ls("x")]);
    assert_eq!(rec_int(rows.last().unwrap(), "rank"), rows.len() as i64);
}

#[test]
fn path_and_closure_algorithms() {
    let (mut store, ids) = path4();
    let rows = algo_rows(&mut store, "algo.bellmanFord", vec![li(ids[0])]);
    assert_eq!(rows.len(), 4);
    let far = rows.iter().find(|r| rec_node(r, "node") == ids[3]).unwrap();
    assert_eq!(rec_float(far, "distance"), 3.0);
    assert!(algo_err(&mut store, "algo.bellmanFord", vec![]).contains("requires a source as argument 1"));
    // Only nodes reachable from the source get a row.
    assert_eq!(algo_rows(&mut store, "algo.bellmanFord", vec![li(ids[3])]).len(), 1);

    let rows = algo_rows(&mut store, "algo.allPairs", vec![]);
    assert!(rows
        .iter()
        .any(|r| rec_node(r, "source") == ids[0] && rec_node(r, "target") == ids[3] && rec_int(r, "hops") == 3));

    let rows = algo_rows(&mut store, "algo.dagLongestPath", vec![]);
    assert_eq!(rows.iter().map(|r| rec_node(r, "node")).collect::<Vec<_>>(), ids);
    let rows = algo_rows(&mut store, "algo.transitiveClosure", vec![]);
    assert_eq!(rows.len(), 6);

    let (mut cyc, _) = triangle();
    let rows = algo_rows(&mut cyc, "algo.wienerIndex", vec![]);
    assert!(rec_float(&rows[0], "wienerIndex") > 0.0);
    assert!(algo_err(&mut cyc, "algo.dagLongestPath", vec![]).contains("the graph has a cycle"));

    let (mut split, _) = algo_graph(3, &[(0, 1)], None);
    assert!(algo_err(&mut split, "algo.wienerIndex", vec![]).contains("some pair is unreachable"));
}

#[test]
fn cohesion_algorithms() {
    let (mut path, _) = path4();
    let rows = algo_rows(&mut path, "algo.bipartite", vec![]);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows.iter().filter(|r| rec_int(r, "side") == 0).count(), 2);
    assert_eq!(algo_rows(&mut path, "algo.maximalMatching", vec![]).len(), 2);
    let colours = algo_rows(&mut path, "algo.colouring", vec![]);
    assert!(colours.iter().all(|r| rec_int(r, "colour") < 2));
    assert!(!algo_rows(&mut path, "algo.dominatingSet", vec![]).is_empty());
    let rows = algo_rows(&mut path, "algo.transitivity", vec![]);
    assert_eq!(rec_float(&rows[0], "transitivity"), 0.0);
    let rows = algo_rows(&mut path, "algo.globalEfficiency", vec![]);
    assert!(rec_float(&rows[0], "efficiency") > 0.0);
    let rows = algo_rows(&mut path, "algo.squareClustering", vec![]);
    // Only the two middle nodes have two neighbours.
    assert_eq!(rows.len(), 2);
    let rows = algo_rows(&mut path, "algo.richClub", vec![lmap(&[("k", pint(1))])]);
    assert!(rec_float(&rows[0], "coefficient") > 0.0);
    assert!(algo_err(&mut path, "algo.richClub", vec![li(5)]).contains("fewer than two nodes have degree above 5"));
    let rows = algo_rows(&mut path, "algo.biconnectedComponents", vec![]);
    let comps: HashSet<i64> = rows.iter().map(|r| rec_int(r, "componentId")).collect();
    assert_eq!(comps.len(), 3);

    let (mut tri, _) = triangle();
    assert!(algo_err(&mut tri, "algo.bipartite", vec![]).contains("odd cycle"));
    assert_eq!(algo_rows(&mut tri, "algo.kTruss", vec![lmap(&[("k", pint(3))])]).len(), 3);
    assert_eq!(rec_float(&algo_rows(&mut tri, "algo.transitivity", vec![])[0], "transitivity"), 1.0);

    let (mut one_edge, _) = algo_graph(2, &[(0, 1)], None);
    assert!(algo_err(&mut one_edge, "algo.transitivity", vec![]).contains("no connected triple exists"));
    let (mut single, _) = algo_graph(1, &[], None);
    assert!(algo_err(&mut single, "algo.globalEfficiency", vec![]).contains("needs at least two nodes"));
}

#[test]
fn similarity_and_structural_holes() {
    // 0 and 2 both point at 1 and 3; 4 is isolated.
    let (mut store, ids) = algo_graph(5, &[(0, 1), (0, 3), (2, 1), (2, 3)], None);
    let rows = algo_rows(&mut store, "algo.nodeSimilarity", vec![lmap(&[("cutoff", PropertyValue::Float(0.1))])]);
    assert!(rows.iter().any(|r| rec_node(r, "node") == ids[0]
        && rec_node(r, "other") == ids[2]
        && rec_float(r, "similarity") == 1.0));
    assert!(algo_err(&mut store, "algo.nodeSimilarity", vec![lmap(&[("topK", pint(1))])])
        .contains("unknown config key `topK`"));

    for name in ["algo.overlap", "algo.cosine"] {
        let rows = algo_rows(&mut store, name, vec![]);
        assert!(
            rows.iter().any(|r| rec_node(r, "node") == ids[0]
                && rec_node(r, "other") == ids[2]
                && (rec_float(r, "similarity") - 1.0).abs() < 1e-9),
            "{name}"
        );
    }
    for name in ["algo.effectiveSize", "algo.constraint"] {
        let rows = algo_rows(&mut store, name, vec![]);
        // The isolated node gets no row.
        assert_eq!(rows.len(), 4, "{name}");
        assert!(rows.iter().all(|r| rec_node(r, "node") != ids[4]));
    }

    let (mut recip, _) = algo_graph(2, &[(0, 1), (1, 0)], None);
    assert_eq!(rec_float(&algo_rows(&mut recip, "algo.reciprocity", vec![])[0], "reciprocity"), 1.0);
    let (mut none, _) = algo_graph(2, &[], None);
    assert!(algo_err(&mut none, "algo.reciprocity", vec![]).contains("no directed edges"));
}

#[test]
fn pca_projects_numeric_properties() {
    let (mut store, ids) = path4();
    let rows = algo_rows(&mut store, "algo.pca", vec![ls("V"), larr(vec![pstr("x"), pstr("y")]), li(1)]);
    assert_eq!(rows.len(), 4);
    assert_eq!(rec_node(&rows[0], "node"), ids[0]);
    match rows[0].get("projection") {
        Some(Value::List(v)) => assert_eq!(v.len(), 1),
        other => panic!("{other:?}"),
    }
    // A null label reads every node; a list expression and a null nComponents are accepted.
    let list_expr = Expression::ListExpr(vec![ls("x"), ls("missing")]);
    let null = Expression::Literal(PropertyValue::Null);
    let rows = algo_rows(&mut store, "algo.pca", vec![null.clone(), list_expr, null]);
    assert_eq!(rows.len(), 4);
    // A label with no nodes: no rows.
    assert!(algo_rows(&mut store, "algo.pca", vec![ls("Nope"), larr(vec![pstr("x")])]).is_empty());

    for (args, want) in [
        (vec![li(1), larr(vec![pstr("x")])], "the label must be a string or null"),
        (vec![ls("V"), larr(vec![pint(1)])], "property names must be strings"),
        (vec![ls("V"), ls("x")], "requires a list of property names"),
        (vec![ls("V"), larr(vec![])], "needs at least one property"),
        (vec![ls("V"), larr(vec![pstr("x")]), li(0)], "nComponents must be a positive integer"),
    ] {
        let e = algo_err(&mut store, "algo.pca", args);
        assert!(e.contains(want), "{e} !~ {want}");
    }
}

#[test]
fn fastrp_writes_embeddings_and_validates_config() {
    let (mut store, ids) = triangle();
    let rows = run_algo_mut(
        &mut store,
        "algo.fastRP",
        vec![lmap(&[
            ("embeddingDimension", pint(4)),
            ("iterationWeights", PropertyValue::Array(vec![PropertyValue::Float(1.0), pint(1)])),
            ("seed", pint(3)),
            ("normalize", PropertyValue::Boolean(true)),
            ("writeProperty", pstr("emb")),
        ])],
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    match rows[0].get("embedding") {
        Some(Value::List(v)) => assert_eq!(v.len(), 4),
        other => panic!("{other:?}"),
    }
    match store.node_property(NodeId::new(ids[0] as u64), "emb") {
        Some(PropertyValue::Array(v)) => assert_eq!(v.len(), 4),
        other => panic!("{other:?}"),
    }
    for (cfg, want) in [
        (lmap(&[("embeddingDimension", pint(0))]), "embeddingDimension must be a positive integer"),
        (lmap(&[("iterationWeights", PropertyValue::Array(vec![pstr("a")]))]), "iterationWeights must be numbers"),
        (lmap(&[("iterationWeights", PropertyValue::Array(vec![]))]), "iterationWeights must not be empty"),
        (lmap(&[("bogus", pint(1))]), "unknown config key `bogus`"),
    ] {
        let e = run_algo_mut(&mut store, "algo.fastRP", vec![cfg]).unwrap_err().to_string();
        assert!(e.contains(want), "{e} !~ {want}");
    }
}

#[test]
fn node2vec_writes_embeddings_and_validates_config() {
    let (mut store, ids) = triangle();
    let rows = run_algo_mut(
        &mut store,
        "algo.node2vec",
        vec![lmap(&[
            ("embeddingDimension", pint(3)),
            ("walkLength", pint(4)),
            ("walksPerNode", pint(2)),
            ("returnFactor", PropertyValue::Float(1.0)),
            ("inOutFactor", pint(2)),
            ("windowSize", pint(2)),
            ("seed", pint(9)),
            ("writeProperty", pstr("n2v")),
        ])],
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    match store.node_property(NodeId::new(ids[1] as u64), "n2v") {
        Some(PropertyValue::Array(v)) => assert_eq!(v.len(), 3),
        other => panic!("{other:?}"),
    }
    let rows = run_algo_mut(
        &mut store,
        "algo.node2vec",
        vec![lmap(&[("returnFactor", pint(1)), ("inOutFactor", PropertyValue::Float(0.5))])],
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    for (cfg, want) in [
        (lmap(&[("embeddingDimension", pstr("x"))]), "embeddingDimension must be a positive integer"),
        (lmap(&[("walkLength", pint(1))]), "walkLength must be at least 2"),
        (lmap(&[("walksPerNode", pint(0))]), "walksPerNode must be at least 1"),
        (lmap(&[("returnFactor", pstr("x"))]), "returnFactor must be a number"),
        (lmap(&[("inOutFactor", pstr("x"))]), "inOutFactor must be a number"),
        (lmap(&[("windowSize", pint(0))]), "windowSize must be at least 1"),
    ] {
        let e = run_algo_mut(&mut store, "algo.node2vec", vec![cfg]).unwrap_err().to_string();
        assert!(e.contains(want), "{e} !~ {want}");
    }
}

fn solve_store() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..3 {
        let n = store.create_node("Item");
        store.set_node_property(TENANT, n, "cost", (i + 1) as f64).unwrap();
        store.set_node_property(TENANT, n, "risk", (3 - i) as f64).unwrap();
    }
    store
}

#[test]
fn or_solve_single_objective_writes_back() {
    let mut store = solve_store();
    let rows = run_algo_mut(
        &mut store,
        "algo.or.solve",
        vec![lmap(&[
            ("algorithm", pstr("Jaya")),
            ("label", pstr("Item")),
            ("property", pstr("qty")),
            ("costProperty", pstr("cost")),
            ("lowerBound", PropertyValue::Float(0.0)),
            ("upperBound", PropertyValue::Float(10.0)),
            ("minTotal", PropertyValue::Float(3.0)),
            ("budget", PropertyValue::Float(100.0)),
            ("populationSize", pint(8)),
            ("maxIterations", pint(5)),
        ])],
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(prop_str(&rows[0], "algorithm"), "Jaya");
    assert_eq!(rec_int(&rows[0], "max_iterations"), 5);
    assert!(rec_int(&rows[0], "iterations") <= 5);
    let written = read(&store, "MATCH (n:Item) WHERE n.qty IS NOT NULL RETURN count(n) AS c");
    assert_eq!(int(&written, 0, "c"), 3);
}

#[test]
fn or_solve_every_single_objective_solver_dispatches() {
    for name in [
        "Rao1", "Rao2", "Rao3", "QORao", "TLBO", "ITLBO", "GOTLBO", "SAMPJaya", "QOJaya", "EHRJaya", "BMR", "BWR",
        "BMWR", "PSO", "DE", "GA", "SA", "ABC", "GSA", "HS", "FPA", "Firefly", "Cuckoo", "GWO", "Bat", "SAPHR",
    ] {
        let mut store = solve_store();
        let rows = run_algo_mut(
            &mut store,
            "algo.or.solve",
            vec![lmap(&[
                ("algorithm", pstr(name)),
                ("label", pstr("Item")),
                ("property", pstr("qty")),
                ("population_size", pint(6)),
                ("max_iterations", pint(3)),
                ("min", PropertyValue::Float(0.0)),
                ("max", PropertyValue::Float(5.0)),
            ])],
        )
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(prop_str(&rows[0], "algorithm"), name);
    }
}

#[test]
fn or_solve_multi_objective_reports_a_front() {
    for name in ["NSGA2", "MOTLBO", "MOBMWR", "MORaoDE"] {
        let mut store = solve_store();
        let rows = run_algo_mut(
            &mut store,
            "algo.or.solve",
            vec![lmap(&[
                ("algorithm", pstr(name)),
                ("label", pstr("Item")),
                ("property", pstr("qty")),
                ("costProperties", PropertyValue::Array(vec![pstr("cost"), pstr("risk")])),
                ("populationSize", pint(6)),
                ("maxIterations", pint(3)),
            ])],
        )
        .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(prop_str(&rows[0], "algorithm"), name);
        assert!(rec_int(&rows[0], "front_size") >= 1, "{name}");
    }
    // Two cost properties route the default solver to the multi-objective branch as well.
    let mut store = solve_store();
    let rows = run_algo_mut(
        &mut store,
        "algo.or.solve",
        vec![lmap(&[
            ("label", pstr("Item")),
            ("property", pstr("qty")),
            ("cost_properties", PropertyValue::Array(vec![pstr("cost"), pstr("risk")])),
            ("populationSize", pint(6)),
            ("maxIterations", pint(2)),
        ])],
    )
    .unwrap();
    assert!(rows[0].get("front_size").is_some());
}

#[test]
fn or_solve_argument_errors() {
    let mut store = solve_store();
    for (args, want) in [
        (vec![], "requires a config map"),
        (vec![ls("x")], "First argument must be a map"),
        (vec![lmap(&[("algorithm", pstr("Simplex")), ("label", pstr("Item"))])], "unknown algorithm `Simplex`"),
        (vec![lmap(&[("property", pstr("q"))])], "Missing 'label' in config"),
        (vec![lmap(&[("label", pstr("Item"))])], "Missing 'property' in config"),
        (vec![lmap(&[("label", pstr("Item")), ("iterations", pint(1))])], "unknown config key `iterations`"),
    ] {
        let e = run_algo_mut(&mut store, "algo.or.solve", args).unwrap_err().to_string();
        assert!(e.contains(want), "{e} !~ {want}");
    }
    // No nodes under the label: no rows.
    let rows = run_algo_mut(
        &mut store,
        "algo.or.solve",
        vec![lmap(&[("label", pstr("Nothing")), ("property", pstr("q"))])],
    )
    .unwrap();
    assert!(rows.is_empty());
}

#[test]
fn read_only_algorithms_agree_across_both_dispatch_tables() {
    // `next_mut` has its own dispatch table; drive every alias through both.
    let (mut store, ids) = algo_graph(4, &[(0, 1), (1, 2), (2, 3), (3, 0), (0, 2)], None);
    let two = vec![li(ids[0]), li(ids[2])];
    let cases: Vec<(&str, Vec<Expression>)> = vec![
        ("algo.harmonicCentrality", vec![]),
        ("algo.closenessCentrality", vec![]),
        ("algo.betweennessCentrality", vec![]),
        ("algo.eigenvectorCentrality", vec![]),
        ("algo.coreNumber", vec![]),
        ("algo.commonNeighbours", two.clone()),
        ("algo.toposort", vec![]),
        ("algo.cycleDetection", vec![]),
        ("algo.averageNeighbourDegree", vec![]),
        ("algo.katzCentrality", vec![]),
        ("algo.hubsAndAuthorities", vec![]),
        ("algo.personalisedPageRank", vec![li(ids[0])]),
        ("algo.allPairsShortestPath", vec![]),
        ("algo.longestPath", vec![]),
        ("algo.bipartiteSets", vec![]),
        ("algo.matching", vec![]),
        ("algo.coloring", vec![]),
        ("algo.greedyColouring", vec![]),
        ("algo.truss", vec![]),
        ("algo.richClubCoefficient", vec![]),
        ("algo.biconnected", vec![]),
        ("algo.overlapCoefficient", vec![]),
        ("algo.cosineSimilarity", vec![]),
        ("algo.burtConstraint", vec![]),
    ];
    for (name, args) in cases {
        let via_mut = run_algo_mut(&mut store, name, args.clone()).map(|r| r.len());
        let via_read = run_algo(&store, name, args).map(|r| r.len());
        match (via_mut, via_read) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "{name}"),
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string(), "{name}"),
            (a, b) => panic!("{name}: {a:?} vs {b:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming and write operators constructed directly
// ---------------------------------------------------------------------------

fn var(name: &str) -> Expression {
    Expression::Variable(name.to_string())
}

/// A materialized input of one row per node, bound to `n`.
fn node_rows(ids: &[NodeId]) -> OperatorBox {
    Box::new(MaterializedOperator::new(ids.iter().map(|id| a_record("n", Value::NodeRef(*id))).collect()))
}

fn int_rows(var_name: &str, n: i64) -> OperatorBox {
    Box::new(MaterializedOperator::new(
        (0..n).map(|i| a_record(var_name, Value::Property(PropertyValue::Integer(i)))).collect(),
    ))
}

#[test]
fn skip_operator_paths() {
    let store = GraphStore::new();
    let mut op = SkipOperator::new(int_rows("x", 5), 2);
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("Skip", "2"));
    assert_eq!(d.children[0].name, "Materialized");
    assert_eq!(op.children_mut().len(), 1);
    assert!(!op.hint_early_stop(3));
    assert_eq!(drain(&mut op, &store).len(), 3);
    op.reset();
    // The batch the skip lands in is returned from the skip onwards.
    let b = op.next_batch(&store, 3).unwrap().unwrap();
    assert_eq!(b.records.len(), 1);
    assert_eq!(b.records[0].get("x"), Some(&Value::Property(PropertyValue::Integer(2))));
    let b = op.next_batch(&store, 3).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    assert!(op.next_batch(&store, 3).unwrap().is_none());

    // Whole batches inside the skip are consumed and the next one returned.
    let mut op = SkipOperator::new(int_rows("x", 5), 4);
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 1);
    // A skip that lands exactly at a batch boundary.
    let mut op = SkipOperator::new(int_rows("x", 4), 2);
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records[0].get("x"), Some(&Value::Property(PropertyValue::Integer(2))));

    // A skip longer than the input.
    let mut op = SkipOperator::new(int_rows("x", 2), 5);
    assert!(op.next(&store).unwrap().is_none());
    let mut store = GraphStore::new();
    let mut op = SkipOperator::new(int_rows("x", 2), 5);
    assert!(op.next_mut(&mut store, TENANT).unwrap().is_none());
    let mut op = SkipOperator::new(int_rows("x", 3), 1);
    assert_eq!(drain_mut(&mut op, &mut store).len(), 2);
}

#[test]
fn delete_operator_paths() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let e = store.create_edge(a, b, "R").unwrap();

    // Read path is a pass-through that deletes nothing.
    let mut op = DeleteOperator::new(node_rows(&[a]), vec![var("n")], false);
    assert!(op.is_mutating());
    assert_eq!(op.describe().name, "Delete");
    assert_eq!(op.describe().details, "n");
    assert_eq!(op.children_mut().len(), 1);
    assert_eq!(drain(&mut op, &store).len(), 1);
    op.reset();
    assert_eq!(op.next_batch(&store, 10).unwrap().unwrap().records.len(), 1);
    op.reset();
    // A plain DELETE of a connected node is refused.
    match op.next_mut(&mut store, TENANT) {
        Err(ExecutionError::ConstraintVerificationFailed(m)) => assert!(m.contains("still has 1 relationship"), "{m}"),
        other => panic!("{other:?}"),
    }

    let d = DeleteOperator::new(
        node_rows(&[]),
        vec![
            Expression::PathVariable("p".into()),
            Expression::Property { variable: "m".into(), property: "k".into() },
            li(3),
        ],
        true,
    )
    .describe();
    assert_eq!(d.name, "DetachDelete");
    assert!(d.details.starts_with("p, m.k, Literal"), "{}", d.details);

    // Entities inside lists, maps and paths are all deleted.
    let c = store.create_node("N");
    let mut row = Record::new();
    row.bind("l", Value::List(vec![Value::EdgeRef(e, a, b, EdgeType::new("R"))]));
    row.bind(
        "m",
        Value::Map(std::collections::BTreeMap::from([("x".to_string(), Value::NodeRef(c))])),
    );
    row.bind("p", Value::Path { nodes: vec![a, b], edges: vec![] });
    row.bind("s", Value::Property(PropertyValue::Integer(1)));
    let mut op = DeleteOperator::new(
        Box::new(MaterializedOperator::new(vec![row])),
        vec![var("l"), var("m"), var("p"), var("s")],
        false,
    );
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(store.node_count(), 0);

    // DETACH DELETE removes the relationships first.
    let a = store.create_node("N");
    let b = store.create_node("N");
    store.create_edge(a, b, "R").unwrap();
    store.create_edge(b, a, "R").unwrap();
    let mut op = DeleteOperator::new(node_rows(&[a]), vec![var("n")], true);
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(store.node_count(), 1);
    assert_eq!(int(&read(&store, "MATCH ()-[r]->() RETURN count(r) AS c"), 0, "c"), 0);
}

#[test]
fn set_property_operator_paths() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    store.set_node_property(TENANT, b, "k", 5i64).unwrap();
    let e = store.create_edge(a, b, "R").unwrap();
    let mut row = Record::new();
    row.bind("n", Value::NodeRef(a));
    row.bind("m", Value::NodeRef(b));
    row.bind("r", Value::EdgeRef(e, a, b, EdgeType::new("R")));
    row.bind("s", Value::Property(PropertyValue::Integer(1)));
    let items = vec![
        ("n".to_string(), "list".to_string(), Expression::ListExpr(vec![var("m"), var("r"), li(1), Expression::Literal(PropertyValue::Null)])),
        ("n".to_string(), "map".to_string(), Expression::MapExpr(vec![("a".into(), var("m")), ("b".into(), var("r")), ("c".into(), li(2))])),
        ("n".to_string(), "node".to_string(), var("m")),
        ("n".to_string(), "edge".to_string(), var("r")),
        ("n".to_string(), "err".to_string(), var("unbound")),
        ("r".to_string(), "w".to_string(), li(9)),
        ("s".to_string(), "ignored".to_string(), li(9)),
        ("missing".to_string(), "ignored".to_string(), li(9)),
    ];
    let mut op = SetPropertyOperator::with_entity_items(
        Box::new(MaterializedOperator::new(vec![row.clone()])),
        items,
        vec![("missing".to_string(), false, lmap(&[])), ("m".to_string(), true, lmap(&[("z", pint(1))]))],
    );
    assert!(op.is_mutating());
    let d = op.describe();
    assert_eq!(d.name, "SetProperty");
    assert!(d.details.contains("m += "), "{}", d.details);
    assert_eq!(op.children_mut().len(), 1);
    assert_eq!(drain(&mut op, &store).len(), 1, "the read path is a pass-through");
    op.reset();
    assert_eq!(op.next_batch(&store, 4).unwrap().unwrap().records.len(), 1);
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);

    let (ai, bi, ei) = (a.as_u64() as i64, b.as_u64() as i64, e.as_u64() as i64);
    assert_eq!(
        store.node_property(a, "list"),
        Some(PropertyValue::Array(vec![pint(bi), pint(ei), pint(1), PropertyValue::Null]))
    );
    match store.node_property(a, "map") {
        Some(PropertyValue::Map(m)) => {
            assert_eq!(m.get("a"), Some(&pint(bi)));
            assert_eq!(m.get("b"), Some(&pint(ei)));
            assert_eq!(m.get("c"), Some(&pint(2)));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(store.node_property(a, "node"), Some(pint(bi)));
    assert_eq!(store.node_property(a, "edge"), Some(pint(ei)));
    // An expression that fails to evaluate stores nothing.
    assert_eq!(store.node_property(a, "err"), None);
    assert_eq!(store.get_edge(e).unwrap().properties.get("w"), Some(&pint(9)));
    assert_eq!(store.node_property(b, "z"), Some(pint(1)));
    assert_eq!(store.node_property(b, "k"), Some(pint(5)));
    let _ = ai;

    // Null removes, on nodes and on edges.
    let mut op = SetPropertyOperator::new(
        Box::new(MaterializedOperator::new(vec![row.clone()])),
        vec![
            ("n".to_string(), "node".to_string(), Expression::Literal(PropertyValue::Null)),
            ("r".to_string(), "w".to_string(), Expression::Literal(PropertyValue::Null)),
        ],
    );
    drain_mut(&mut op, &mut store);
    assert_eq!(store.node_property(a, "node"), None);
    assert_eq!(store.get_edge(e).unwrap().properties.get("w"), None);

    // A list holding a map is refused.
    let mut op = SetPropertyOperator::new(
        Box::new(MaterializedOperator::new(vec![row])),
        vec![("n".to_string(), "bad".to_string(), larr(vec![PropertyValue::Map(Default::default())]))],
    );
    match op.next_mut(&mut store, TENANT) {
        Err(ExecutionError::TypeError(m)) => assert!(m.contains("InvalidPropertyType: `bad`"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn entity_assignment_on_edges_and_its_sources() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let e = store.create_edge(a, b, "R").unwrap();
    store.set_edge_property(e, "old", 1i64).unwrap();
    let edge = Value::EdgeRef(e, a, b, EdgeType::new("R"));
    let map = Value::Property(PropertyValue::Map([("new".to_string(), pint(2))].into_iter().collect()));
    apply_entity_assignment(&edge, &map, false, &mut store, TENANT).unwrap();
    let props = store.get_edge(e).unwrap().properties;
    assert_eq!(props.get("old"), None);
    assert_eq!(props.get("new"), Some(&pint(2)));
    apply_entity_assignment(&edge, &map, true, &mut store, TENANT).unwrap();
    // Something that is not an entity is left alone.
    apply_entity_assignment(&Value::Property(pint(1)), &map, true, &mut store, TENANT).unwrap();

    let full = Value::Edge(e, Box::new(store.get_edge(e).unwrap()));
    let from_edge = SetPropertyOperator::source_properties(&full, &store).unwrap();
    assert_eq!(from_edge.get("new"), Some(&pint(2)));
    assert!(SetPropertyOperator::source_properties(&Value::Property(PropertyValue::Null), &store).unwrap().is_empty());
    match SetPropertyOperator::source_properties(&Value::Property(pint(3)), &store) {
        Err(ExecutionError::TypeError(m)) => assert!(m.contains("expects a map or another entity"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn label_and_remove_operators_paths() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    store.set_node_property(TENANT, a, "k", 1i64).unwrap();
    let b = store.create_node("A");
    let e = store.create_edge(a, b, "R").unwrap();
    store.set_edge_property(e, "w", 1i64).unwrap();

    let mut op = LabelMutationOperator::new(node_rows(&[a]), vec![("n".into(), Label::new("B"))], vec![("n".into(), Label::new("A"))]);
    let d = op.describe();
    assert_eq!(d.name, "LabelMutation");
    assert_eq!(d.details, "+n:B, -n:A");
    assert_eq!(op.children_mut().len(), 1);
    assert_eq!(drain(&mut op, &store).len(), 1);
    op.reset();
    assert!(op.next_batch(&store, 5).unwrap().is_some());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    let labels = &store.get_node(a).unwrap().labels;
    assert!(labels.contains(&Label::new("B")) && !labels.contains(&Label::new("A")));

    let mut row = a_record("n", Value::NodeRef(a));
    row.bind("r", Value::EdgeRef(e, a, b, EdgeType::new("R")));
    row.bind("s", Value::Property(pint(1)));
    let mut op = RemovePropertyOperator::new(
        Box::new(MaterializedOperator::new(vec![row])),
        vec![("n".into(), "k".into()), ("r".into(), "w".into()), ("s".into(), "x".into()), ("zz".into(), "x".into())],
    );
    assert!(op.is_mutating());
    assert_eq!(op.describe().details, "n.k, r.w, s.x, zz.x");
    assert_eq!(op.children_mut().len(), 1);
    assert_eq!(drain(&mut op, &store).len(), 1);
    op.reset();
    assert!(op.next_batch(&store, 5).unwrap().is_some());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(store.node_property(a, "k"), None);
    assert_eq!(store.get_edge(e).unwrap().properties.get("w"), None);
}

#[test]
fn unwind_operator_paths() {
    let store = GraphStore::new();
    let vector = Expression::Literal(PropertyValue::Vector(vec![1.0, 2.0]));
    let mut op = UnwindOperator::new(Box::new(SingleRowOperator::new()), vector, "v".into());
    let d = op.describe();
    assert_eq!(d.name, "Unwind");
    assert!(d.details.ends_with("AS v"), "{}", d.details);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].get("v"), Some(&Value::Property(PropertyValue::Float(2.0))));
    op.reset();
    let b = op.next_batch(&store, 10).unwrap().unwrap();
    assert_eq!(b.columns, vec!["v".to_string()]);
    assert!(op.next_batch(&store, 10).unwrap().is_none());

    // A scalar unwinds to nothing.
    let mut op = UnwindOperator::new(Box::new(SingleRowOperator::new()), li(3), "v".into());
    assert!(drain(&mut op, &store).is_empty());
    assert_eq!(op.children_mut().len(), 1);
}

#[test]
fn load_csv_rejects_a_non_string_source_before_touching_disk() {
    let store = GraphStore::new();
    let clause = crate::query::ast::LoadCsvClause {
        source: li(1),
        variable: "row".into(),
        with_headers: true,
        field_terminator: Some(';'),
    };
    let mut op = LoadCsvOperator::new(Box::new(SingleRowOperator::new()), clause.clone());
    let d = op.describe();
    assert_eq!(d.name, "LoadCsv");
    assert!(d.details.ends_with("AS row (with headers)"), "{}", d.details);
    assert_eq!(op.children_mut().len(), 1);
    match op.next(&store) {
        Err(ExecutionError::TypeError(m)) => assert!(m.contains("expects a string path"), "{m}"),
        other => panic!("{other:?}"),
    }
    op.reset();
    // No input rows: nothing to open, and it stays exhausted.
    let mut empty = LoadCsvOperator::new(Box::new(MaterializedOperator::new(vec![])), clause);
    assert!(empty.next(&store).unwrap().is_none());
    assert!(empty.next(&store).unwrap().is_none());
    let no_headers = crate::query::ast::LoadCsvClause {
        source: ls("x.csv"),
        variable: "r".into(),
        with_headers: false,
        field_terminator: None,
    };
    let details = LoadCsvOperator::new(Box::new(SingleRowOperator::new()), no_headers).describe().details;
    assert!(details.contains("x.csv") && details.ends_with(" AS r"), "{details}");
}

#[test]
fn foreach_operator_paths() {
    let mut store = GraphStore::new();
    let mut op = ForeachOperator::new(int_rows("x", 1), "i".into(), li(3), vec![]);
    assert_eq!(op.children_mut().len(), 1);
    assert!(matches!(op.next(&store), Err(ExecutionError::RuntimeError(_))));
    // The batch form goes through the refusing read path and yields nothing.
    assert!(op.next_batch(&store, 5).unwrap().is_none());
    op.reset();
    match op.next_mut(&mut store, TENANT) {
        Err(ExecutionError::TypeError(m)) => assert!(m.contains("FOREACH expects a list"), "{m}"),
        other => panic!("{other:?}"),
    }
    // A null list is empty.
    let mut op = ForeachOperator::new(int_rows("x", 1), "i".into(), Expression::Literal(PropertyValue::Null), vec![]);
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    let mut op = ForeachOperator::new(int_rows("x", 1), "i".into(), larr(vec![pint(1), pint(2)]), vec![]);
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
}

#[test]
fn shortest_path_operator_describe_batch_and_reset() {
    let (store, ids) = path4();
    let mut row = Record::new();
    row.bind("a", Value::NodeRef(NodeId::new(ids[0] as u64)));
    row.bind("b", Value::NodeRef(NodeId::new(ids[3] as u64)));
    let mut op = ShortestPathOperator::new(
        Box::new(MaterializedOperator::new(vec![row])),
        "a".into(),
        "b".into(),
        Some("p".into()),
        vec!["E".into(), "F".into()],
        Direction::Outgoing,
        true,
    );
    let d = op.describe();
    assert_eq!(d.name, "AllShortestPaths");
    assert!(d.details.contains("(a)-[:E|F*]-(b)"), "{}", d.details);
    assert_eq!(op.children_mut().len(), 1);
    let b = op.next_batch(&store, 10).unwrap().unwrap();
    assert_eq!(b.records.len(), 1);
    assert!(op.next_batch(&store, 10).unwrap().is_none());
    op.reset();
    assert_eq!(drain(&mut op, &store).len(), 1);
    let mut store = store;
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);

    let plain = ShortestPathOperator::new(
        Box::new(SingleRowOperator::new()),
        "a".into(),
        "b".into(),
        None,
        vec![],
        Direction::Both,
        false,
    );
    let d = plain.describe();
    assert_eq!(d.name, "ShortestPath");
    assert!(d.details.contains("(a)-[*]-(b)"), "{}", d.details);
}

#[test]
fn expand_into_operator_paths() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let e = store.create_edge(a, b, "R").unwrap();
    let pair = |s: NodeId, t: NodeId| {
        let mut r = a_record("s", Value::NodeRef(s));
        r.bind("t", Value::NodeRef(t));
        r
    };
    let input = || -> OperatorBox { Box::new(MaterializedOperator::new(vec![pair(a, c), pair(a, b)])) };
    let mut op = ExpandIntoOperator::new(input(), "s".into(), "t".into(), Some("R".into()), Some("r".into()));
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("ExpandInto", "(s)--[:R]-->(t)"));
    assert_eq!(op.children_mut().len(), 1);
    let rows = drain(&mut op, &store);
    // Only the connected pair survives, with the edge bound.
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].get("r"), Some(Value::EdgeRef(id, ..)) if *id == e));
    op.reset();
    assert_eq!(op.next_batch(&store, 5).unwrap().unwrap().records.len(), 1);
    assert!(op.next_batch(&store, 5).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);

    let any = ExpandIntoOperator::new(input(), "s".into(), "t".into(), None, None);
    assert_eq!(any.describe().details, "(s)--[:*]-->(t)");

    let mut missing = ExpandIntoOperator::new(
        Box::new(MaterializedOperator::new(vec![a_record("t", Value::NodeRef(b))])),
        "s".into(),
        "t".into(),
        None,
        None,
    );
    assert!(matches!(missing.next(&store), Err(ExecutionError::VariableNotFound(v)) if v == "s"));
    let mut missing = ExpandIntoOperator::new(
        Box::new(MaterializedOperator::new(vec![a_record("s", Value::NodeRef(a))])),
        "s".into(),
        "t".into(),
        None,
        None,
    );
    assert!(matches!(missing.next(&store), Err(ExecutionError::VariableNotFound(v)) if v == "t"));
}

#[test]
fn node_by_id_operator_checks_labels_and_existence() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("Q");
    let mut op = NodeByIdOperator::new(vec![a, b, NodeId::new(999)], "n".into()).with_labels(vec![Label::new("P")]);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n"), Some(&Value::NodeRef(a)));
    op.reset();
    let batch = op.next_batch(&store, 10).unwrap().unwrap();
    assert_eq!(batch.columns, vec!["n".to_string()]);
    assert!(op.next_batch(&store, 10).unwrap().is_none());
    let d = NodeByIdOperator::new(vec![a], "n".into()).describe();
    assert_eq!(d.name, "NodeById");
    assert!(d.details.contains("var=n"));
}

#[test]
fn distinct_operator_paths() {
    let mut store = GraphStore::new();
    let rows = vec![
        a_record("x", Value::Property(pint(1))),
        a_record("x", Value::Property(pint(1))),
        a_record("x", Value::Property(pint(2))),
    ];
    let mut op = DistinctOperator::new(Box::new(MaterializedOperator::new(rows)));
    assert_eq!(op.describe().name, "Distinct");
    assert_eq!(op.describe().children.len(), 1);
    assert_eq!(op.children_mut().len(), 1);
    assert!(!op.try_push_limit(1));
    assert!(!op.hint_early_stop(1));
    assert_eq!(drain(&mut op, &store).len(), 2);
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 2);
}

// ---------------------------------------------------------------------------
// MERGE through Cypher
// ---------------------------------------------------------------------------

fn count_of(store: &GraphStore, cypher: &str) -> i64 {
    int(&read(store, cypher), 0, "c")
}

#[test]
fn merge_path_creates_then_matches_with_on_create_and_on_match() {
    let mut store = GraphStore::new();
    let q = "MERGE (a:P {id: 1})-[r:R {w: 1}]->(b:P {id: 2}) \
             ON CREATE SET a.created = true ON MATCH SET a.matched = true \
             RETURN a.id AS a, b.id AS b, r.w AS w";
    let first = write(&mut store, q);
    assert_eq!((int(&first, 0, "a"), int(&first, 0, "b"), int(&first, 0, "w")), (1, 2, 1));
    let second = write(&mut store, q);
    assert_eq!(second.records.len(), 1);
    assert_eq!(count_of(&store, "MATCH (n:P) RETURN count(n) AS c"), 2);
    assert_eq!(count_of(&store, "MATCH ()-[r:R]->() RETURN count(r) AS c"), 1);
    let b = read(&store, "MATCH (a:P {id: 1}) RETURN a.created AS c, a.matched AS m");
    assert_eq!(cell(&b, 0, "c"), PropertyValue::Boolean(true));
    assert_eq!(cell(&b, 0, "m"), PropertyValue::Boolean(true));
}

#[test]
fn merge_path_incoming_and_undirected_patterns() {
    let mut store = GraphStore::new();
    write(&mut store, "MERGE (x:Q {k: 1})<-[:S]-(y:Q {k: 2})");
    assert_eq!(count_of(&store, "MATCH (:Q {k: 2})-[:S]->(:Q {k: 1}) RETURN count(*) AS c"), 1);
    // Undirected: the existing edge matches in either direction.
    write(&mut store, "MERGE (x:Q {k: 1})-[:S]-(y:Q {k: 2})");
    assert_eq!(count_of(&store, "MATCH ()-[r:S]->() RETURN count(r) AS c"), 1);
}

#[test]
fn merge_path_binds_a_path_variable() {
    let mut store = GraphStore::new();
    let q = "MERGE p = (a:T1 {n: 1})-[:U]->(b:T2)-[:V]->(c:T3) RETURN length(p) AS l";
    assert_eq!(int(&write(&mut store, q), 0, "l"), 2);
    assert_eq!(int(&write(&mut store, q), 0, "l"), 2);
    assert_eq!(count_of(&store, "MATCH (n:T2) RETURN count(n) AS c"), 1);
}

#[test]
fn merge_path_with_a_partial_match_creates_the_whole_pattern() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE INDEX ON :P(id)");
    write(&mut store, "CREATE (:P {id: 1})");
    write(&mut store, "MERGE (a:P {id: 1})-[:R]->(b:P {id: 3})");
    // The pattern as a whole was absent, so both ends are fresh.
    assert_eq!(count_of(&store, "MATCH (n:P {id: 1}) RETURN count(n) AS c"), 2);
    // Now it exists, and the index finds its candidates.
    write(&mut store, "MERGE (a:P {id: 1})-[:R]->(b:P {id: 3})");
    assert_eq!(count_of(&store, "MATCH (n:P) RETURN count(n) AS c"), 3);
}

#[test]
fn merge_path_per_row_with_property_expressions() {
    let mut store = GraphStore::new();
    let b = write(&mut store, "UNWIND [1, 2, 2] AS x MERGE (a:W {v: x})-[:L {x: x}]->(b:W2 {v: x}) RETURN count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 3);
    assert_eq!(count_of(&store, "MATCH (n:W) RETURN count(n) AS c"), 2);
    assert_eq!(count_of(&store, "MATCH ()-[r:L]->() RETURN sum(r.x) AS c"), 3);
}

#[test]
fn merge_path_entity_sets_and_labels() {
    let mut store = GraphStore::new();
    let q = "MERGE (a:E1)-[:R]->(b:E2) ON CREATE SET a += {k: 1} ON MATCH SET a = {z: 2}, b:Seen";
    write(&mut store, q);
    let r = read(&store, "MATCH (a:E1) RETURN a.k AS k, a.z AS z");
    assert_eq!((cell(&r, 0, "k"), cell(&r, 0, "z")), (pint(1), PropertyValue::Null));
    write(&mut store, q);
    let r = read(&store, "MATCH (a:E1) RETURN a.k AS k, a.z AS z");
    assert_eq!((cell(&r, 0, "k"), cell(&r, 0, "z")), (PropertyValue::Null, pint(2)));
    assert_eq!(count_of(&store, "MATCH (b:E2:Seen) RETURN count(b) AS c"), 1);
}

#[test]
fn merge_path_reuses_a_bound_node() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (a:B0) WITH a MERGE (a)-[:R]->(b:B1)");
    write(&mut store, "MATCH (a:B0) MERGE (a)-[:R]->(b:B1)");
    assert_eq!(count_of(&store, "MATCH (n) RETURN count(n) AS c"), 2);
}

#[test]
fn merge_node_binds_every_match_and_applies_on_match() {
    let mut store = GraphStore::new();
    write(&mut store, "CREATE (:M {i: 1}), (:M {i: 2})");
    let b = write(&mut store, "MERGE (m:M) ON MATCH SET m.seen = m.i * 10, m:Old RETURN m.i AS i");
    assert_eq!(b.records.len(), 2);
    assert_eq!(count_of(&store, "MATCH (m:M:Old) RETURN sum(m.seen) AS c"), 30);
}

#[test]
fn merge_node_on_create_null_removes_and_labels() {
    let mut store = GraphStore::new();
    let b = write(
        &mut store,
        "MERGE (n:Fresh {id: 1}) ON CREATE SET n.x = null, n.y = 2, n:New, n += {z: 3} RETURN n.y AS y, n.z AS z",
    );
    assert_eq!(int(&b, 0, "y"), 2);
    assert_eq!(int(&b, 0, "z"), 3);
    assert_eq!(count_of(&store, "MATCH (n:Fresh:New) WHERE n.x IS NULL RETURN count(n) AS c"), 1);
}

#[test]
fn merge_refuses_a_non_scalar_property() {
    let mut store = GraphStore::new();
    let e = write_err(&mut store, "WITH [{a: 1}] AS m MERGE (n:Bad {v: m}) RETURN n");
    assert!(e.contains("must be a scalar value") || e.contains("Type"), "{e}");
}

#[test]
fn merge_operator_direct_paths() {
    let store = GraphStore::new();
    let q = parse_query("MERGE (n:X)").unwrap();
    let pattern = match &q.merge_clause {
        Some(m) => m.pattern.clone(),
        None => panic!("no merge clause"),
    };
    let mut op = MergeOperator::new(pattern, vec![], vec![], vec![], vec![]);
    assert!(matches!(op.next(&store), Err(ExecutionError::RuntimeError(_))));
    assert!(op.next_batch(&store, 5).unwrap().is_none());
    op.reset();
    let mut store = store;
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(store.node_count(), 1, "the second run matches the node the first made");
}

// ---------------------------------------------------------------------------
// shortestPath and WITH through Cypher
// ---------------------------------------------------------------------------

#[test]
fn shortest_path_queries_in_every_direction() {
    let store = people();
    let len = |q: &str| -> Vec<i64> { ints(&read(&store, q), "l") };
    assert_eq!(
        len("MATCH (a:Person {name: 'alice'}), (c:Person {name: 'carol'}) \
             MATCH p = shortestPath((a)-[:KNOWS*]->(c)) RETURN length(p) AS l"),
        vec![2]
    );
    assert_eq!(
        len("MATCH (a:Person {name: 'alice'}), (c:Person {name: 'carol'}) \
             MATCH p = shortestPath((c)<-[:KNOWS*]-(a)) RETURN length(p) AS l"),
        vec![2]
    );
    assert_eq!(
        len("MATCH (a:Person {name: 'alice'}), (c:Person {name: 'carol'}) \
             MATCH p = allShortestPaths((c)-[*]-(a)) RETURN length(p) AS l"),
        vec![2]
    );
    // Against the direction: no path, no row.
    assert!(len(
        "MATCH (a:Person {name: 'alice'}), (c:Person {name: 'carol'}) \
         MATCH p = shortestPath((c)-[:KNOWS*]->(a)) RETURN length(p) AS l"
    )
    .is_empty());
    // A type that does not exist.
    assert!(len(
        "MATCH (a:Person {name: 'alice'}), (c:Person {name: 'carol'}) \
         MATCH p = shortestPath((a)-[:NOPE*]->(c)) RETURN length(p) AS l"
    )
    .is_empty());
}

#[test]
fn with_barrier_orders_pages_filters_and_aggregates() {
    let store = people();
    let b = read(&store, "MATCH (n:Person) WITH n.age AS age ORDER BY age DESC SKIP 1 LIMIT 1 RETURN age");
    assert_eq!(ints(&b, "age"), vec![40]);
    let b = read(&store, "MATCH (n:Person) WITH DISTINCT n.age > 35 AS old RETURN count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 2);
    let b = read(&store, "MATCH (n:Person) WITH count(n) AS c WHERE c > 1 RETURN c");
    assert_eq!(int(&b, 0, "c"), 3);
    let b = read(&store, "MATCH (n:Person) WITH count(n) AS c WHERE c > 10 RETURN c");
    assert!(b.records.is_empty());
    let b = read(
        &store,
        "MATCH (n:Person) WITH n.age > 35 AS old, collect(n.name) AS names, avg(n.age) AS a \
         ORDER BY old DESC LIMIT 1 RETURN old, size(names) AS s, a",
    );
    assert_eq!(cell(&b, 0, "old"), PropertyValue::Boolean(true));
    assert_eq!(int(&b, 0, "s"), 2);
    assert_eq!(float(&b, 0, "a"), 45.0);
    let b = read(&store, "MATCH (n:Person) WITH n ORDER BY n.name DESC SKIP 2 RETURN n.name AS name");
    assert_eq!(strings(&b, "name"), vec!["alice"]);
    let b = read(&store, "MATCH (n:Person) WITH n.name AS name WHERE name STARTS WITH 'b' RETURN name");
    assert_eq!(strings(&b, "name"), vec!["bob"]);
}

// ---------------------------------------------------------------------------
// AggregatorState and CountDistinctSet
// ---------------------------------------------------------------------------

fn pv(p: PropertyValue) -> Value {
    Value::Property(p)
}

fn agg(func: AggregateType, distinct: bool, values: &[Value]) -> AggregatorState {
    let mut s = AggregatorState::new(&func, distinct);
    for v in values {
        s.update(v);
    }
    s
}

/// Fold `values` split in two halves and merged, which must equal one pass.
fn agg_split(func: AggregateType, distinct: bool, values: &[Value]) -> Value {
    let mid = values.len() / 2;
    let mut a = agg(func.clone(), distinct, &values[..mid]);
    let b = agg(func, distinct, &values[mid..]);
    a.merge(b);
    a.result()
}

#[test]
fn count_distinct_set_mixes_ids_and_properties() {
    let mut s = CountDistinctSet::new();
    assert_eq!(s.len(), 0);
    s.insert_id(1);
    s.insert_id(1);
    s.insert_id(2);
    assert_eq!(s.len(), 2);
    // A property after ids converts the set; the id 2 and the integer 2 collide.
    s.insert_prop(pint(2));
    s.insert_prop(pstr("x"));
    assert_eq!(s.len(), 3);
    s.insert_id(9);
    assert_eq!(s.len(), 4);

    let mut t = CountDistinctSet::new();
    t.merge(CountDistinctSet::new());
    assert_eq!(t.len(), 0);
    let mut ids = CountDistinctSet::new();
    ids.insert_id(5);
    t.merge(ids);
    let mut props = CountDistinctSet::new();
    props.insert_prop(pstr("y"));
    t.merge(props);
    assert_eq!(t.len(), 2);
}

#[test]
fn count_distinct_state_over_every_value_shape() {
    let n = NodeId::new(1);
    let e = EdgeId::new(7);
    let values = vec![
        Value::NodeRef(n),
        Value::NodeRef(n),
        Value::EdgeRef(e, n, n, EdgeType::new("R")),
        pv(pint(1)),
        pv(PropertyValue::Null),
        Value::Null,
        Value::List(vec![pv(pint(1))]),
        Value::Map(Default::default()),
        Value::Path { nodes: vec![n], edges: vec![] },
    ];
    // node 1, edge 7, 1 (collides with node id 1), the list and the map.
    assert_eq!(agg(AggregateType::Count, true, &values).result(), pv(pint(4)));
    // Plain count skips only the two nulls.
    assert_eq!(agg(AggregateType::Count, false, &values).result(), pv(pint(7)));
    assert_eq!(agg_split(AggregateType::Count, false, &values), pv(pint(7)));
    assert_eq!(agg_split(AggregateType::Count, true, &values[..3]), pv(pint(2)));
}

#[test]
fn sum_and_avg_promote_and_merge() {
    let ints = [pv(pint(1)), pv(pint(2)), pv(pint(3)), pv(pint(4))];
    assert_eq!(agg(AggregateType::Sum, false, &ints).result(), pv(pint(10)));
    assert_eq!(agg_split(AggregateType::Sum, false, &ints), pv(pint(10)));
    let mixed = [pv(pint(1)), pv(PropertyValue::Float(0.5)), pv(pint(2)), pv(pstr("x"))];
    assert_eq!(agg(AggregateType::Sum, false, &mixed).result(), pv(PropertyValue::Float(3.5)));
    // Float half merged into an integer half, and the other way round.
    assert_eq!(
        agg_split(AggregateType::Sum, false, &[pv(pint(1)), pv(pint(1)), pv(PropertyValue::Float(0.5)), pv(pint(1))]),
        pv(PropertyValue::Float(3.5))
    );
    assert_eq!(
        agg_split(AggregateType::Sum, false, &[pv(PropertyValue::Float(0.5)), pv(pint(1)), pv(pint(1)), pv(pint(1))]),
        pv(PropertyValue::Float(3.5))
    );

    assert_eq!(agg(AggregateType::Avg, false, &[]).result(), Value::Null);
    assert_eq!(agg_split(AggregateType::Avg, false, &ints), pv(PropertyValue::Float(2.5)));
    assert_eq!(
        agg(AggregateType::Avg, false, &[pv(PropertyValue::Float(1.0)), pv(pint(3)), pv(pstr("x"))]).result(),
        pv(PropertyValue::Float(2.0))
    );
}

#[test]
fn min_max_skip_nulls_and_merge() {
    let values = [pv(pint(3)), pv(PropertyValue::Null), pv(pint(1)), pv(pint(5)), Value::NodeRef(NodeId::new(0))];
    assert_eq!(agg(AggregateType::Min, false, &values).result(), pv(pint(1)));
    assert_eq!(agg(AggregateType::Max, false, &values).result(), pv(pint(5)));
    assert_eq!(agg_split(AggregateType::Min, false, &values), pv(pint(1)));
    assert_eq!(agg_split(AggregateType::Max, false, &values), pv(pint(5)));
    assert_eq!(agg(AggregateType::Min, false, &[]).result(), Value::Null);
    assert_eq!(agg(AggregateType::Max, false, &[pv(PropertyValue::Null)]).result(), Value::Null);
    // Merging an empty half leaves the other alone.
    let mut a = agg(AggregateType::Max, false, &[pv(pint(2))]);
    a.merge(agg(AggregateType::Max, false, &[]));
    assert_eq!(a.result(), pv(pint(2)));
    let mut a = agg(AggregateType::Min, false, &[pv(pint(2))]);
    a.merge(agg(AggregateType::Min, false, &[]));
    assert_eq!(a.result(), pv(pint(2)));
}

#[test]
fn collect_plain_and_distinct() {
    let values = [pv(pint(2)), pv(PropertyValue::Null), Value::Null, pv(pint(1)), pv(pint(2))];
    assert_eq!(
        agg(AggregateType::Collect, false, &values).result(),
        pv(PropertyValue::Array(vec![pint(2), pint(1), pint(2)]))
    );
    assert_eq!(agg_split(AggregateType::Collect, true, &values), pv(PropertyValue::Array(vec![pint(1), pint(2)])));
    assert_eq!(
        agg_split(AggregateType::Collect, false, &values),
        pv(PropertyValue::Array(vec![pint(2), pint(1), pint(2)]))
    );
    // Entities make it a list of values, not an array of properties.
    let n = Value::NodeRef(NodeId::new(3));
    assert_eq!(agg(AggregateType::Collect, false, &[n.clone(), pv(pint(1))]).result(), Value::List(vec![n, pv(pint(1))]));
}

#[test]
fn percentiles_cont_and_disc() {
    let values: Vec<Value> = (1..=4).map(|i| pv(pint(i))).chain([pv(pstr("x"))]).collect();
    let mut cont = agg(AggregateType::PercentileCont, false, &values);
    assert_eq!(cont.result(), pv(PropertyValue::Float(2.5)));
    cont.set_percentile(&pv(PropertyValue::Float(0.25))).unwrap();
    assert_eq!(cont.result(), pv(PropertyValue::Float(1.75)));
    let mut disc = agg(AggregateType::PercentileDisc, false, &[pv(PropertyValue::Float(1.0)), pv(pint(2)), pv(pint(3))]);
    disc.set_percentile(&pv(pint(1))).unwrap();
    assert_eq!(disc.result(), pv(PropertyValue::Float(3.0)));
    disc.set_percentile(&pv(PropertyValue::Null)).unwrap();
    assert!(matches!(disc.set_percentile(&pv(pstr("x"))), Err(ExecutionError::TypeError(_))));
    match disc.set_percentile(&pv(PropertyValue::Float(1.5))) {
        Err(ExecutionError::RuntimeError(m)) => assert!(m.contains("between 0.0 and 1.0"), "{m}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(agg(AggregateType::PercentileCont, false, &[]).result(), Value::Null);
    assert_eq!(agg_split(AggregateType::PercentileDisc, false, &values[..4]), pv(PropertyValue::Float(2.0)));
    // Setting a percentile on a state that has none is a no-op.
    agg(AggregateType::Count, false, &[]).set_percentile(&pv(pint(1))).unwrap();
}

#[test]
fn stdev_sample_and_population() {
    let values = [pv(pint(2)), pv(pint(4)), pv(PropertyValue::Float(4.0)), pv(pint(4)), pv(pint(5)), pv(pint(5)), pv(pint(7)), pv(pint(9))];
    assert_eq!(agg(AggregateType::StDevP, false, &values).result(), pv(PropertyValue::Float(2.0)));
    match agg_split(AggregateType::StDev, false, &values) {
        Value::Property(PropertyValue::Float(f)) => assert!((f - 2.138089935).abs() < 1e-6, "{f}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(agg(AggregateType::StDev, false, &[]).result(), Value::Null);
    // One value: the sample denominator is clamped to 1.
    assert_eq!(agg(AggregateType::StDev, false, &[pv(pint(3))]).result(), pv(PropertyValue::Float(0.0)));
}

#[test]
fn approximate_aggregates() {
    let values: Vec<Value> = (0..100).map(|i| pv(pint(i % 10))).collect();
    assert_eq!(agg_split(AggregateType::ApproxCountDistinct, false, &values), pv(pint(10)));
    let floats: Vec<Value> = (0..101).map(|i| pv(PropertyValue::Float(i as f64))).collect();
    match agg_split(AggregateType::ApproxPercentile, false, &floats) {
        Value::Property(PropertyValue::Float(f)) => assert!((f - 50.0).abs() < 5.0, "{f}"),
        other => panic!("{other:?}"),
    }
    let mut p = agg(AggregateType::ApproxPercentile, false, &[pv(pint(1)), pv(pint(2))]);
    p.set_percentile(&pv(PropertyValue::Float(1.0))).unwrap();
    let mut q = agg(AggregateType::ApproxPercentile, false, &[]);
    // The merged-in half's percentile is taken when this one is still the default.
    q.merge(p);
    match q.result() {
        Value::Property(PropertyValue::Float(f)) => assert!(f >= 1.5, "{f}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(agg(AggregateType::ApproxPercentile, false, &[]).result(), Value::Null);
}

// ---------------------------------------------------------------------------
// Expand helpers
// ---------------------------------------------------------------------------

#[test]
fn extend_path_appends_or_starts_fresh() {
    let (a, b, c) = (NodeId::new(1), NodeId::new(2), NodeId::new(3));
    let (e1, e2) = (EdgeId::new(10), EdgeId::new(11));
    let base = Value::Path { nodes: vec![a, b], edges: vec![e1] };
    assert_eq!(extend_path(Some(&base), b, c, e2), Value::Path { nodes: vec![a, b, c], edges: vec![e1, e2] });
    // The base does not end at the source: a fresh one-hop path.
    assert_eq!(extend_path(Some(&base), c, a, e2), Value::Path { nodes: vec![c, a], edges: vec![e2] });
    assert_eq!(extend_path(None, a, b, e1), Value::Path { nodes: vec![a, b], edges: vec![e1] });
    assert_eq!(
        extend_path(Some(&Value::Property(pint(1))), a, b, e1),
        Value::Path { nodes: vec![a, b], edges: vec![e1] }
    );
}

#[test]
fn pinned_run_and_co_cursor() {
    let n = NodeId::new;
    let e = EdgeId::new;
    let list = vec![(n(1), e(1)), (n(3), e(2)), (n(3), e(3)), (n(5), e(4))];
    assert_eq!(pinned_run(&list, n(3)).len(), 2);
    assert!(pinned_run(&list, n(4)).is_empty());
    assert!(pinned_run(&list, n(9)).is_empty());

    let other = vec![(n(2), e(9)), (n(5), e(8))];
    let lists: Vec<&[(NodeId, EdgeId)]> = vec![&list, &other];
    let mut cur = CoCursor::new(&lists);
    assert!(cur.contains(n(1)));
    assert!(cur.contains(n(2)));
    assert!(!cur.contains(n(4)));
    assert!(cur.contains(n(5)));
    // Positions only move forward until restarted.
    assert!(!cur.contains(n(1)));
    cur.restart();
    assert!(cur.contains(n(1)));
}

// ---------------------------------------------------------------------------
// Queries that drive the read operators
// ---------------------------------------------------------------------------

/// A small social graph with multiple edge types, properties on edges, and a
/// cycle, for the expand and join queries below.
fn social() -> GraphStore {
    let mut store = GraphStore::new();
    write(
        &mut store,
        "CREATE (a:Person {name: 'a', age: 1}), (b:Person {name: 'b', age: 2}), \
         (c:Person {name: 'c', age: 3}), (d:Person:Admin {name: 'd', age: 4}), \
         (x:City {name: 'x'}), \
         (a)-[:KNOWS {w: 1}]->(b), (b)-[:KNOWS {w: 2}]->(c), (c)-[:KNOWS {w: 3}]->(a), \
         (a)-[:LIKES {w: 5}]->(c), (d)-[:KNOWS {w: 4}]->(a), \
         (a)-[:LIVES_IN]->(x), (b)-[:LIVES_IN]->(x)",
    );
    store
}

fn names(store: &GraphStore, q: &str) -> Vec<String> {
    let mut v = strings(&read(store, q), "name");
    v.sort();
    v
}

#[test]
fn single_hop_expands_in_every_shape() {
    let s = social();
    assert_eq!(names(&s, "MATCH (:Person {name: 'a'})-[:KNOWS]->(n) RETURN n.name AS name"), vec!["b"]);
    assert_eq!(names(&s, "MATCH (:Person {name: 'a'})<-[:KNOWS]-(n) RETURN n.name AS name"), vec!["c", "d"]);
    assert_eq!(names(&s, "MATCH (:Person {name: 'a'})-[:KNOWS]-(n) RETURN n.name AS name"), vec!["b", "c", "d"]);
    assert_eq!(names(&s, "MATCH (:Person {name: 'a'})-[:KNOWS|LIKES]->(n) RETURN n.name AS name"), vec!["b", "c"]);
    assert_eq!(names(&s, "MATCH (:Person {name: 'a'})-->(n:City) RETURN n.name AS name"), vec!["x"]);
    assert_eq!(names(&s, "MATCH (:Person {name: 'a'})-[r {w: 5}]->(n) RETURN n.name AS name"), vec!["c"]);
    assert_eq!(names(&s, "MATCH (n)-[r]->(:City) WHERE r.w IS NULL RETURN n.name AS name"), vec!["a", "b"]);
    assert_eq!(names(&s, "MATCH (n:Admin)-[:KNOWS]->(m:Person) RETURN m.name AS name"), vec!["a"]);
    assert_eq!(names(&s, "MATCH (:City)<-[:LIVES_IN]-(n:Person) RETURN n.name AS name"), vec!["a", "b"]);
    let b = read(&s, "MATCH p = (:Person {name: 'a'})-[:KNOWS]->(:Person)-[:KNOWS]->(n) RETURN n.name AS name, length(p) AS l");
    assert_eq!((string(&b, 0, "name"), int(&b, 0, "l")), ("c".to_string(), 2));
    let b = read(&s, "MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN type(r) AS t, count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 4);
    // A self-join via a repeated variable (expand into a bound node).
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b)-[:KNOWS]->(c)-[:KNOWS]->(a) RETURN count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 3);
    // A missing type is empty, not an error.
    assert!(names(&s, "MATCH (:Person {name: 'a'})-[:NOPE]->(n) RETURN n.name AS name").is_empty());
}

#[test]
fn variable_length_expands() {
    let s = social();
    assert_eq!(
        names(&s, "MATCH (:Person {name: 'd'})-[:KNOWS*2]->(n) RETURN n.name AS name"),
        vec!["b"]
    );
    assert_eq!(
        names(&s, "MATCH (:Person {name: 'd'})-[:KNOWS*1..3]->(n) RETURN DISTINCT n.name AS name"),
        vec!["a", "b", "c"]
    );
    assert_eq!(
        names(&s, "MATCH (:Person {name: 'd'})-[:KNOWS*0..1]->(n) RETURN n.name AS name"),
        vec!["a", "d"]
    );
    assert_eq!(
        names(&s, "MATCH (:Person {name: 'b'})<-[:KNOWS*2..2]-(n) RETURN n.name AS name"),
        vec!["c", "d"]
    );
    assert_eq!(
        names(&s, "MATCH (:Person {name: 'x'})-[*]-(n) RETURN n.name AS name"),
        Vec::<String>::new()
    );
    let b = read(&s, "MATCH (:City)-[*1..2]-(n:Person) RETURN count(DISTINCT n) AS c");
    assert_eq!(int(&b, 0, "c"), 4);
    let b = read(&s, "MATCH p = (:Person {name: 'd'})-[rs:KNOWS*]->(n {name: 'c'}) RETURN length(p) AS l, size(rs) AS s");
    assert_eq!((int(&b, 0, "l"), int(&b, 0, "s")), (3, 3));
    let b = read(&s, "MATCH (:Person {name: 'a'})-[rs:KNOWS*1..3 {w: 1}]->(n) RETURN n.name AS name");
    assert_eq!(strings(&b, "name"), vec!["b"]);
    let b = read(
        &s,
        "MATCH (a:Person {name: 'd'}), (c:Person {name: 'c'}) MATCH (a)-[:KNOWS*]->(c) RETURN count(*) AS c",
    );
    assert_eq!(int(&b, 0, "c"), 1);
    let b = read(&s, "MATCH (:Person {name: 'a'})-[r:KNOWS|LIKES*1..2]->(n) RETURN count(*) AS c");
    assert!(int(&b, 0, "c") >= 3);
}

#[test]
fn projection_aggregation_and_grouping_queries() {
    let s = social();
    let b = read(&s, "MATCH (n:Person) RETURN count(DISTINCT n.age % 2) AS c, percentileCont(n.age, 0.5) AS p, \
                      percentileDisc(n.age, 0.5) AS d, stDev(n.age) AS sd, stDevP(n.age) AS sp, \
                      collect(DISTINCT n.age % 2) AS parity, min(n.name) AS mn, max(n.age) AS mx, avg(n.age) AS av");
    assert_eq!(int(&b, 0, "c"), 2);
    assert_eq!(float(&b, 0, "p"), 2.5);
    assert_eq!(float(&b, 0, "d"), 2.0);
    assert!(float(&b, 0, "sd") > float(&b, 0, "sp"));
    assert_eq!(cell(&b, 0, "parity"), PropertyValue::Array(vec![pint(0), pint(1)]));
    assert_eq!(string(&b, 0, "mn"), "a");
    assert_eq!(int(&b, 0, "mx"), 4);
    assert_eq!(float(&b, 0, "av"), 2.5);

    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name AS name, count(b) AS c ORDER BY name");
    assert_eq!(strings(&b, "name"), vec!["a", "b", "c", "d"]);
    assert_eq!(ints(&b, "c"), vec![1, 1, 1, 1]);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]-(b) RETURN a.name AS name, count(DISTINCT b) AS c ORDER BY name");
    assert_eq!(ints(&b, "c"), vec![3, 2, 2, 1]);
    let b = read(&s, "MATCH (a:Person)<-[:KNOWS]-(b) RETURN a, count(*) AS c");
    assert_eq!(b.records.len(), 3);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b) RETURN count(b) AS c");
    assert_eq!(int(&b, 0, "c"), 4);
    let b = read(&s, "MATCH (n:Person) RETURN n.age % 2 AS k, sum(n.age) AS s, collect(n.name) AS ns ORDER BY k");
    assert_eq!(ints(&b, "s"), vec![6, 4]);
    let b = read(&s, "MATCH (n:Person) WHERE n.age > 100 RETURN count(n) AS c, sum(n.age) AS s, avg(n.age) AS a, collect(n) AS l");
    assert_eq!(int(&b, 0, "c"), 0);
    assert_eq!(int(&b, 0, "s"), 0);
    assert_eq!(cell(&b, 0, "a"), PropertyValue::Null);
    let b = read(&s, "MATCH (n:Person) WHERE n.age > 100 RETURN n.name AS k, count(*) AS c");
    assert!(b.records.is_empty());
    let b = read(&s, "MATCH (n:Person) RETURN n AS node, count(*) AS c");
    assert_eq!(b.records.len(), 4);
    let b = read(&s, "MATCH (n:Person) RETURN {name: n.name, age: n.age} AS m, [n.age, n.age * 2] AS l ORDER BY n.age LIMIT 1");
    assert_eq!(b.records.len(), 1);
    let b = read(&s, "UNWIND [1, 2, 2, 3] AS x RETURN x, count(*) AS c ORDER BY x");
    assert_eq!(ints(&b, "c"), vec![1, 2, 1]);
}

#[test]
fn ordering_paging_and_distinct_queries() {
    let s = social();
    let q = |cypher: &str| strings(&read(&s, cypher), "name");
    assert_eq!(q("MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC"), vec!["d", "c", "b", "a"]);
    assert_eq!(q("MATCH (n:Person) RETURN n.name AS name ORDER BY n.age DESC LIMIT 2"), vec!["d", "c"]);
    assert_eq!(q("MATCH (n:Person) RETURN n.name AS name ORDER BY n.age SKIP 1 LIMIT 2"), vec!["b", "c"]);
    assert_eq!(q("MATCH (n:Person) RETURN n.name AS name ORDER BY n.age % 2, n.name DESC"), vec!["d", "b", "c", "a"]);
    assert_eq!(q("MATCH (n) RETURN n.name AS name ORDER BY n.age, n.name LIMIT 1"), vec!["a"]);
    // Nulls sort last ascending and first descending.
    assert_eq!(q("MATCH (n) RETURN n.name AS name ORDER BY n.age DESC LIMIT 1"), vec!["x"]);
    assert_eq!(q("MATCH (n:Person) RETURN DISTINCT n.name AS name ORDER BY name DESC SKIP 3"), vec!["a"]);
    assert_eq!(q("MATCH (n:Person) RETURN n.name AS name ORDER BY toString(n.age) DESC LIMIT 1"), vec!["d"]);
    let b = read(&s, "MATCH (n:Person) RETURN DISTINCT n.age % 2 AS p ORDER BY p LIMIT 1");
    assert_eq!(ints(&b, "p"), vec![0]);
    let b = read(&s, "UNWIND [3.5, 1, 'z', null, 2.0, true] AS v RETURN v ORDER BY v");
    assert_eq!(b.records.len(), 6);
    let b = read(&s, "UNWIND ['b', 'a', 'c'] AS v RETURN v ORDER BY v DESC LIMIT 2");
    assert_eq!(strings(&b, "v"), vec!["c", "b"]);
    let b = read(&s, "MATCH (n:Person) RETURN n.name AS name SKIP 10");
    assert!(b.records.is_empty());
    let b = read(&s, "MATCH (n:Person) RETURN n.name AS name LIMIT 0");
    assert!(b.records.is_empty());
}

#[test]
fn join_cartesian_and_optional_queries() {
    let s = social();
    let b = read(&s, "MATCH (a:Person), (c:City) RETURN count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 4);
    let b = read(&s, "MATCH (a:Person), (b:Person) WHERE a.age = b.age + 1 RETURN count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 3);
    let b = read(
        &s,
        "MATCH (a:Person)-[:LIVES_IN]->(x), (b:Person)-[:LIVES_IN]->(x) WHERE a <> b RETURN count(*) AS c",
    );
    assert_eq!(int(&b, 0, "c"), 2);
    let b = read(&s, "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIVES_IN]->(x) RETURN a.name AS name, x.name AS city ORDER BY name");
    assert_eq!(strings(&b, "name"), vec!["a", "b", "c", "d"]);
    assert_eq!(cell(&b, 2, "city"), PropertyValue::Null);
    assert_eq!(string(&b, 0, "city"), "x");
    let b = read(
        &s,
        "MATCH (a:Person) OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b.age > 2 RETURN a.name AS name, b.name AS other ORDER BY name",
    );
    assert_eq!(b.records.len(), 4);
    assert_eq!(string(&b, 1, "other"), "c");
    assert_eq!(cell(&b, 0, "other"), PropertyValue::Null);
    let b = read(&s, "OPTIONAL MATCH (n:Nope) RETURN n");
    assert_eq!(b.records.len(), 1);
    let b = read(&s, "MATCH (a:Person {name: 'a'}) OPTIONAL MATCH (a)-[:KNOWS*1..2]->(b) RETURN count(b) AS c");
    assert_eq!(int(&b, 0, "c"), 2);
    let b = read(&s, "MATCH (a:Person) WHERE EXISTS { (a)-[:LIVES_IN]->(:City) } RETURN count(a) AS c");
    assert_eq!(int(&b, 0, "c"), 2);
    let b = read(&s, "MATCH (a:Person) WHERE NOT (a)-[:LIVES_IN]->() RETURN count(a) AS c");
    assert_eq!(int(&b, 0, "c"), 2);
    let b = read(&s, "MATCH (a:Person) CALL { WITH a MATCH (a)-[:KNOWS]->(b) RETURN b.name AS friend } RETURN a.name AS name, friend ORDER BY name");
    assert_eq!(strings(&b, "friend"), vec!["b", "c", "a", "a"]);
    let b = read(&s, "MATCH (a:Person) WITH a, [(a)-[:KNOWS]->(b) | b.name] AS fs RETURN a.name AS name, size(fs) AS n ORDER BY name");
    assert_eq!(ints(&b, "n"), vec![1, 1, 1, 1]);
    let b = read(&s, "MATCH (a:Person {name: 'a'}) RETURN a.name AS name UNION MATCH (b:City) RETURN b.name AS name");
    assert_eq!(b.records.len(), 2);
}

#[test]
fn index_backed_queries() {
    let mut s = social();
    write(&mut s, "CREATE INDEX ON :Person(age)");
    write(&mut s, "CREATE INDEX ON :Person(name)");
    assert_eq!(names(&s, "MATCH (n:Person) WHERE n.age = 2 RETURN n.name AS name"), vec!["b"]);
    assert_eq!(names(&s, "MATCH (n:Person) WHERE n.age > 2 RETURN n.name AS name"), vec!["c", "d"]);
    assert_eq!(names(&s, "MATCH (n:Person) WHERE n.age >= 2 AND n.age < 4 RETURN n.name AS name"), vec!["b", "c"]);
    assert_eq!(names(&s, "MATCH (n:Person) WHERE n.age <= 1 RETURN n.name AS name"), vec!["a"]);
    assert_eq!(names(&s, "MATCH (n:Person) WHERE n.age IN [1, 4] RETURN n.name AS name"), vec!["a", "d"]);
    assert_eq!(names(&s, "MATCH (n:Person {name: 'c'}) RETURN n.name AS name"), vec!["c"]);
    assert_eq!(names(&s, "MATCH (n:Person) WHERE n.name STARTS WITH 'd' RETURN n.name AS name"), vec!["d"]);
    assert!(names(&s, "MATCH (n:Person) WHERE n.age = 99 RETURN n.name AS name").is_empty());
    // An index lookup correlated with an earlier row.
    let b = read(&s, "MATCH (c:City) MATCH (n:Person) WHERE n.age = 1 RETURN count(*) AS c");
    assert_eq!(int(&b, 0, "c"), 1);
    let b = read(&s, "UNWIND [1, 3, 7] AS v MATCH (n:Person) WHERE n.age = v RETURN n.name AS name ORDER BY name");
    assert_eq!(strings(&b, "name"), vec!["a", "c"]);
    let b = read(&s, "MATCH (a:Person {name: 'a'}) MATCH (n:Person) WHERE n.age = a.age + 1 RETURN n.name AS name");
    assert_eq!(strings(&b, "name"), vec!["b"]);
}

#[test]
fn explain_describes_every_operator_in_the_plan() {
    let mut s = social();
    write(&mut s, "CREATE INDEX ON :Person(age)");
    for q in [
        "EXPLAIN MATCH (n:Person) WHERE n.age > 1 RETURN n.name ORDER BY n.name SKIP 1 LIMIT 2",
        "EXPLAIN MATCH (a:Person)-[:KNOWS*1..2]->(b) RETURN DISTINCT b.name",
        "EXPLAIN MATCH (a:Person)-[:KNOWS]->(b) RETURN a, count(b)",
        "EXPLAIN MATCH (a:Person), (c:City) RETURN a, c",
        "EXPLAIN MATCH (a:Person) OPTIONAL MATCH (a)-[:LIVES_IN]->(c) RETURN a, c",
        "EXPLAIN MATCH (a:Person) WITH a.age AS x, count(*) AS c WHERE c > 0 RETURN x",
        "EXPLAIN UNWIND [1, 2] AS x RETURN x",
        "EXPLAIN MATCH p = shortestPath((a:Person)-[*]-(b:City)) RETURN p",
        "EXPLAIN MATCH (a:Person) WHERE EXISTS { (a)-->() } RETURN a",
        "EXPLAIN MATCH (a:Person) CALL { WITH a MATCH (a)-->(b) RETURN b.name AS n } RETURN n",
        "EXPLAIN MATCH (a)-[r]->(b) RETURN a, r, b",
        "EXPLAIN CALL algo.pageRank() YIELD node RETURN node",
        "EXPLAIN MATCH (n) WHERE id(n) = 1 RETURN n",
    ] {
        let b = read(&s, q);
        assert!(!b.records.is_empty(), "{q}");
    }
    for q in [
        "EXPLAIN MATCH (n:Person) SET n.x = 1, n += {y: 2}",
        "EXPLAIN MATCH (n:Person) REMOVE n.age, n:Admin",
        "EXPLAIN MATCH (n:Person) SET n:Tag",
        "EXPLAIN MATCH (n:Person) DETACH DELETE n",
        "EXPLAIN MERGE (n:Person {name: 'z'})",
        "EXPLAIN CREATE (n:Q)-[:R]->(m:Q)",
        "EXPLAIN MATCH (a:Person), (b:City) CREATE (a)-[:R]->(b)",
        "EXPLAIN FOREACH (x IN [1] | CREATE (:F))",
        "EXPLAIN CREATE INDEX ON :Q(k)",
    ] {
        let b = write(&mut s, q);
        assert!(!b.records.is_empty(), "{q}");
    }
    // EXPLAIN runs nothing.
    assert_eq!(count_of(&s, "MATCH (n:Person) RETURN count(n) AS c"), 4);
}

// ---------------------------------------------------------------------------
// Subquery plumbing, eager, bind-path and limit operators
// ---------------------------------------------------------------------------

type Seed = std::sync::Arc<std::sync::Mutex<Option<Record>>>;

fn poisoned_seed() -> Seed {
    let seed: Seed = Default::default();
    let held = seed.clone();
    let _ = std::thread::spawn(move || {
        let _guard = held.lock().unwrap();
        panic!("poisoning the seed on purpose");
    })
    .join();
    assert!(seed.is_poisoned());
    seed
}

#[test]
fn seed_operator_replays_its_cell_once_per_reset() {
    let store = GraphStore::new();
    let seed: Seed = Default::default();
    *seed.lock().unwrap() = Some(a_record("x", Value::Property(pint(1))));
    let mut op = SeedOperator::new(seed.clone());
    assert_eq!(op.describe().name, "Seed");
    assert_eq!(drain(&mut op, &store).len(), 1);
    op.reset();
    assert_eq!(drain(&mut op, &store).len(), 1);
    let mut bad = SeedOperator::new(poisoned_seed());
    assert!(matches!(bad.next(&store), Err(ExecutionError::RuntimeError(m)) if m.contains("poisoned")));
}

#[test]
fn correlated_call_runs_the_body_per_outer_row() {
    let store = GraphStore::new();
    let seed: Seed = Default::default();
    let body: OperatorBox = Box::new(SeedOperator::new(seed.clone()));
    let mut op = CorrelatedCallOperator::new(int_rows("x", 3), body, seed);
    let d = op.describe();
    assert_eq!(d.name, "CorrelatedCall");
    assert_eq!(d.children.len(), 2);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2].get("x"), Some(&Value::Property(pint(2))));
    op.reset();
    assert_eq!(drain(&mut op, &store).len(), 3);

    let poisoned = poisoned_seed();
    let mut op = CorrelatedCallOperator::new(int_rows("x", 1), Box::new(SeedOperator::new(poisoned.clone())), poisoned);
    assert!(matches!(op.next(&store), Err(ExecutionError::RuntimeError(m)) if m.contains("poisoned")));
}

#[test]
fn semi_apply_keeps_or_drops_rows_by_existence() {
    let store = GraphStore::new();
    let seed: Seed = Default::default();
    let mut keep = SemiApplyOperator::new(int_rows("x", 2), Box::new(SeedOperator::new(seed.clone())), seed.clone(), false);
    assert_eq!(keep.describe().name, "SemiApply");
    assert_eq!(drain(&mut keep, &store).len(), 2);
    keep.reset();
    assert_eq!(drain(&mut keep, &store).len(), 2);

    let mut anti = SemiApplyOperator::new(int_rows("x", 2), Box::new(SeedOperator::new(seed.clone())), seed, true);
    assert_eq!(anti.describe().name, "AntiSemiApply");
    assert!(drain(&mut anti, &store).is_empty());

    let poisoned = poisoned_seed();
    let mut op = SemiApplyOperator::new(int_rows("x", 1), Box::new(SingleRowOperator::new()), poisoned, false);
    assert!(matches!(op.next(&store), Err(ExecutionError::RuntimeError(m)) if m.contains("EXISTS")));
}

#[test]
fn eager_operator_buffers_then_pages() {
    let mut store = GraphStore::new();
    let mut op = EagerOperator::new(int_rows("x", 5), 1, Some(2));
    assert_eq!(op.describe().name, "Eager");
    assert_eq!(op.children_mut().len(), 1);
    assert!(!op.try_push_limit(1));
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("x"), Some(&Value::Property(pint(1))));
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 2);
    let mut all = EagerOperator::new(int_rows("x", 3), 0, None);
    assert_eq!(drain(&mut all, &store).len(), 3);
    let mut past = EagerOperator::new(int_rows("x", 3), 5, None);
    assert!(drain(&mut past, &store).is_empty());
}

#[test]
fn bind_path_operator_binds_only_complete_paths() {
    let mut store = GraphStore::new();
    let (a, b) = (NodeId::new(1), NodeId::new(2));
    let e = EdgeId::new(3);
    let mut full = a_record("a", Value::NodeRef(a));
    full.bind("b", Value::NodeRef(b));
    full.bind("r", Value::EdgeRef(e, a, b, EdgeType::new("R")));
    let mut no_edge = a_record("a", Value::NodeRef(a));
    no_edge.bind("b", Value::NodeRef(b));
    no_edge.bind("r", Value::Property(pint(1)));
    let no_node = a_record("a", Value::NodeRef(a));
    let input = || -> OperatorBox {
        Box::new(MaterializedOperator::new(vec![full.clone(), no_edge.clone(), no_node.clone()]))
    };
    let paths = vec![("p".to_string(), vec!["a".to_string(), "b".to_string()], vec!["r".to_string()])];
    let mut op = BindPathOperator::new(input(), paths.clone());
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("BindPath", "p"));
    assert_eq!(op.children_mut().len(), 1);
    let rows = drain(&mut op, &store);
    assert_eq!(rows[0].get("p"), Some(&Value::Path { nodes: vec![a, b], edges: vec![e] }));
    assert!(rows[1].get("p").is_none());
    assert!(rows[2].get("p").is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 3);
}

#[test]
fn limit_operator_paths() {
    let mut store = GraphStore::new();
    let mut op = LimitOperator::new(int_rows("x", 5), 3);
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("Limit", "3"));
    assert_eq!(op.children_mut().len(), 1);
    assert!(!op.hint_early_stop(10));
    assert!(!op.try_push_limit(2));
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 1);
    assert!(op.next_batch(&store, 2).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 3);
    let mut short = LimitOperator::new(int_rows("x", 1), 3);
    assert_eq!(drain_mut(&mut short, &mut store).len(), 1);
    let mut short = LimitOperator::new(int_rows("x", 0), 3);
    assert!(short.next_batch(&store, 2).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Index-driven leaf operators constructed directly
// ---------------------------------------------------------------------------

/// Five `:P` nodes with `k` = 0..5, indexed on `k`.
fn indexed_store() -> (GraphStore, Vec<NodeId>) {
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..5)
        .map(|i| {
            let n = store.create_node("P");
            store.set_node_property(TENANT, n, "k", i as i64).unwrap();
            n
        })
        .collect();
    store.create_property_index(&Label::new("P"), "k");
    (store, ids)
}

#[test]
fn index_scan_operator_every_comparison() {
    let (store, _) = indexed_store();
    let count = |op: BinaryOp, v: i64| {
        let mut s = IndexScanOperator::new("n".into(), Label::new("P"), "k".into(), op, pint(v));
        drain(&mut s, &store).len()
    };
    assert_eq!(count(BinaryOp::Eq, 2), 1);
    assert_eq!(count(BinaryOp::Gt, 2), 2);
    assert_eq!(count(BinaryOp::Ge, 2), 3);
    assert_eq!(count(BinaryOp::Lt, 2), 2);
    assert_eq!(count(BinaryOp::Le, 2), 3);
    assert_eq!(count(BinaryOp::Ne, 2), 0);

    let mut op = IndexScanOperator::new("n".into(), Label::new("P"), "k".into(), BinaryOp::Ge, pint(1));
    assert!(op.filter_predicate().is_none());
    let d = op.describe();
    assert_eq!(d.name, "IndexScan");
    assert!(d.details.contains(">="), "{}", d.details);
    let b = op.next_batch(&store, 3).unwrap().unwrap();
    assert_eq!(b.records.len(), 3);
    assert_eq!(b.columns, vec!["n".to_string()]);
    assert_eq!(op.next_batch(&store, 3).unwrap().unwrap().records.len(), 1);
    assert!(op.next_batch(&store, 3).unwrap().is_none());
    op.reset();
    assert_eq!(drain(&mut op, &store).len(), 4);

    let eq = IndexScanOperator::new("n".into(), Label::new("P"), "k".into(), BinaryOp::Eq, pint(1));
    assert!(eq.filter_predicate().is_some());
    for (op, sym) in [(BinaryOp::Eq, "="), (BinaryOp::Gt, ">"), (BinaryOp::Lt, "<"), (BinaryOp::Le, "<="), (BinaryOp::Ne, "?")] {
        let d = IndexScanOperator::new("n".into(), Label::new("P"), "k".into(), op, pint(1)).describe();
        assert!(d.details.contains(&format!(" {sym} ")), "{}", d.details);
    }

    // No index on the property: nothing.
    let mut none = IndexScanOperator::new("n".into(), Label::new("P"), "zz".into(), BinaryOp::Eq, pint(1));
    assert!(none.next(&store).unwrap().is_none());
    assert!(none.next_batch(&store, 2).unwrap().is_none());
}

#[test]
fn correlated_index_lookup_probes_per_row() {
    let (mut store, ids) = indexed_store();
    let rows = vec![
        a_record("v", Value::Property(pint(3))),
        a_record("v", Value::Property(PropertyValue::Null)),
        a_record("v", Value::NodeRef(ids[0])),
        a_record("v", Value::Property(pint(42))),
        a_record("v", Value::Property(pint(1))),
    ];
    let make = |rows: Vec<Record>, prop: &str| {
        CorrelatedIndexLookupOperator::new(
            Box::new(MaterializedOperator::new(rows)),
            "n".into(),
            Label::new("P"),
            prop.into(),
            var("v"),
        )
    };
    let mut op = make(rows.clone(), "k");
    let d = op.describe();
    assert_eq!(d.name, "CorrelatedIndexLookup");
    assert!(d.details.contains("= <per row>"));
    assert_eq!(op.children_mut().len(), 1);
    let out = drain(&mut op, &store);
    assert_eq!(out.iter().map(|r| r.get("n").cloned()).collect::<Vec<_>>(), vec![Some(Value::NodeRef(ids[3])), Some(Value::NodeRef(ids[1]))]);
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 2);
    // A property with no index finds nothing.
    let mut unindexed = make(rows, "other");
    assert!(drain(&mut unindexed, &store).is_empty());
    // An unbound key is an error.
    let mut bad = CorrelatedIndexLookupOperator::new(
        Box::new(SingleRowOperator::new()),
        "n".into(),
        Label::new("P"),
        "k".into(),
        var("missing"),
    );
    assert!(bad.next(&store).is_err());
}

#[test]
fn vector_search_operator_ranks_by_similarity() {
    let mut store = GraphStore::new();
    store
        .create_vector_index("Doc", "emb", 2, crate::vector::DistanceMetric::Cosine)
        .unwrap();
    let a = store.create_node("Doc");
    store.set_node_property(TENANT, a, "emb", PropertyValue::Vector(vec![1.0, 0.0])).unwrap();
    let b = store.create_node("Doc");
    store.set_node_property(TENANT, b, "emb", PropertyValue::Vector(vec![0.0, 1.0])).unwrap();
    store.rebuild_vector_index();

    let mut op = VectorSearchOperator::new("Doc".into(), "emb".into(), vec![1.0, 0.1], 1, "n".into(), Some("s".into()));
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("VectorSearch", "Doc.emb, k=1"));
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("n"), Some(&Value::NodeRef(a)));
    assert!(rows[0].get("s").is_some());
    op.reset();
    assert_eq!(drain(&mut op, &store).len(), 1);

    let mut no_score = VectorSearchOperator::new("Doc".into(), "emb".into(), vec![0.0, 1.0], 5, "n".into(), None);
    let rows = drain(&mut no_score, &store);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("n"), Some(&Value::NodeRef(b)));

    let mut missing = VectorSearchOperator::new("Nope".into(), "emb".into(), vec![1.0, 0.0], 1, "n".into(), None);
    // No index for the label: nothing to rank.
    assert!(missing.next(&store).unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Joins constructed directly
// ---------------------------------------------------------------------------

fn kv_rows(key: &str, keys: &[i64], tag: &str, tags: &[i64]) -> OperatorBox {
    Box::new(MaterializedOperator::new(
        keys.iter()
            .zip(tags)
            .map(|(k, t)| {
                let mut r = a_record(key, Value::Property(pint(*k)));
                r.bind(tag.to_string(), Value::Property(pint(*t)));
                r
            })
            .collect(),
    ))
}

#[test]
fn cartesian_product_paths() {
    let mut store = GraphStore::new();
    let mut op = CartesianProductOperator::new(int_rows("a", 3), int_rows("b", 2));
    assert!(op.amplifies_rows());
    assert_eq!(op.children_mut().len(), 2);
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.children.len()), ("CartesianProduct", 2));
    assert_eq!(drain(&mut op, &store).len(), 6);
    op.reset();
    let b1 = op.next_batch(&store, 4).unwrap().unwrap();
    let b2 = op.next_batch(&store, 4).unwrap().unwrap();
    assert_eq!((b1.records.len(), b2.records.len()), (4, 2));
    assert!(op.next_batch(&store, 4).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 6);

    let mut empty = CartesianProductOperator::new(int_rows("a", 0), int_rows("b", 2));
    assert!(empty.next(&store).unwrap().is_none());
    assert!(empty.next_batch(&store, 2).unwrap().is_none());

    // Enough rows on each side to cross the periodic deadline check.
    let mut big = CartesianProductOperator::new(int_rows("a", 10_000), int_rows("b", 1));
    assert_eq!(drain(&mut big, &store).len(), 10_000);
    let mut big = CartesianProductOperator::new(int_rows("a", 10_000), int_rows("b", 10_000));
    assert!(big.next_mut(&mut store, TENANT).unwrap().is_some());
}

#[test]
fn hash_join_paths() {
    let mut store = GraphStore::new();
    let left = || kv_rows("k", &[1, 1, 2, 3], "l", &[10, 11, 20, 30]);
    let right = || kv_rows("k", &[1, 3, 4], "r", &[100, 300, 400]);
    let mut op = JoinOperator::new(left(), right(), vec!["k".into()]);
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("HashJoin", "on=k"));
    assert_eq!(op.children_mut().len(), 2);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 3);
    op.reset();
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 1);
    assert!(op.next_batch(&store, 2).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 3);

    // A right row without the join variable matches nothing.
    let mut op = JoinOperator::new(left(), int_rows("x", 2), vec!["k".into()]);
    assert!(drain(&mut op, &store).is_empty());
    let mut op = JoinOperator::new(left(), int_rows("x", 2), vec!["k".into()]);
    assert!(op.next_batch(&store, 5).unwrap().is_none());

    let many: Vec<i64> = (0..10_000).collect();
    let mut big = JoinOperator::new(kv_rows("k", &many, "l", &many), kv_rows("k", &many, "r", &many), vec!["k".into()]);
    assert_eq!(drain(&mut big, &store).len(), 10_000);
    let mut big = JoinOperator::new(kv_rows("k", &many, "l", &many), kv_rows("k", &many, "r", &many), vec!["k".into()]);
    assert!(big.next_mut(&mut store, TENANT).unwrap().is_some());
}

#[test]
fn left_outer_join_paths() {
    let mut store = GraphStore::new();
    let left = || {
        let mut rows: Vec<Record> = [1, 2, 3]
            .iter()
            .map(|k| a_record("k", Value::Property(pint(*k))))
            .collect();
        rows.push(a_record("other", Value::Property(pint(0))));
        Box::new(MaterializedOperator::new(rows)) as OperatorBox
    };
    let right = || kv_rows("k", &[1, 1, 2], "r", &[5, 50, 7]);
    let mut op = LeftOuterJoinOperator::new(left(), right(), vec!["k".into()], vec!["r".into()]);
    let d = op.describe();
    assert_eq!((d.name.as_str(), d.details.as_str()), ("LeftOuterJoin", "on=k"));
    assert_eq!(op.children_mut().len(), 2);
    let rows = drain(&mut op, &store);
    // 1 matches twice, 2 once, 3 and the keyless row get nulls.
    assert_eq!(rows.len(), 5);
    assert_eq!(rows.iter().filter(|r| r.get("r") == Some(&Value::Null)).count(), 2);
    op.reset();
    let b = op.next_batch(&store, 3).unwrap().unwrap();
    assert_eq!(b.records.len(), 3);
    assert_eq!(op.next_batch(&store, 3).unwrap().unwrap().records.len(), 2);
    assert!(op.next_batch(&store, 3).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 5);

    // With a predicate that rejects every candidate, matched keys fall back to nulls.
    let never = Expression::Binary { left: Box::new(var("r")), op: BinaryOp::Gt, right: Box::new(li(1000)) };
    let mut op = LeftOuterJoinOperator::new(left(), right(), vec!["k".into()], vec!["r".into()]).with_join_predicate(never);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(|r| r.get("r") == Some(&Value::Null)));
    let some = Expression::Binary { left: Box::new(var("r")), op: BinaryOp::Gt, right: Box::new(li(6)) };
    let mut op = LeftOuterJoinOperator::new(left(), right(), vec!["k".into()], vec!["r".into()]).with_join_predicate(some);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows.iter().filter(|r| r.get("r") != Some(&Value::Null)).count(), 2);

    let many: Vec<i64> = (0..10_000).collect();
    let mut big = LeftOuterJoinOperator::new(kv_rows("k", &many, "l", &many), kv_rows("k", &many, "r", &many), vec!["k".into()], vec!["r".into()]);
    assert_eq!(drain(&mut big, &store).len(), 10_000);
    let mut big = LeftOuterJoinOperator::new(kv_rows("k", &many, "l", &many), kv_rows("k", &many, "r", &many), vec!["k".into()], vec!["r".into()]);
    assert!(big.next_mut(&mut store, TENANT).unwrap().is_some());
}

// ---------------------------------------------------------------------------
// FilterOperator expression evaluation, constructed directly
// ---------------------------------------------------------------------------

fn expr_of(text: &str) -> Expression {
    let q = parse_query(&format!("MATCH (n) WITH n, 0 AS x, [] AS l RETURN {text} AS out"))
        .unwrap_or_else(|e| panic!("{text}: {e}"));
    q.return_clause.expect("RETURN").items.into_iter().next().unwrap().expression
}

/// Four `:F` nodes with `v` = 1..4 (the last without `v`), each row binding
/// `n` to a node, `x` to its index and `l` to a list.
fn filter_rows() -> (GraphStore, Vec<Record>) {
    let mut store = GraphStore::new();
    let mut rows = Vec::new();
    for i in 0..4i64 {
        let n = store.create_node("F");
        if i < 3 {
            store.set_node_property(TENANT, n, "v", i + 1).unwrap();
        }
        let mut r = a_record("n", Value::NodeRef(n));
        r.bind("x", Value::Property(pint(i)));
        r.bind("l", Value::Property(PropertyValue::Array(vec![pint(i), pint(i + 1)])));
        r.bind("$p", Value::Property(pint(2)));
        r.bind("path", Value::Path { nodes: vec![n], edges: vec![] });
        rows.push(r);
    }
    (store, rows)
}

fn filter_count(store: &GraphStore, rows: &[Record], predicate: Expression) -> ExecutionResult<usize> {
    let mut op = FilterOperator::new(Box::new(MaterializedOperator::new(rows.to_vec())), predicate);
    let mut n = 0;
    while op.next(store)?.is_some() {
        n += 1;
    }
    Ok(n)
}

#[test]
fn filter_evaluates_every_expression_kind() {
    let (store, rows) = filter_rows();
    let cases: &[(&str, usize)] = &[
        ("n.v IS NULL", 1),
        ("n.v IS NOT NULL", 3),
        ("NOT (n.v > 1)", 1),
        ("-n.v < -1", 2),
        ("-(x * 1.5) < -1.0", 3),
        ("CASE WHEN x > 1 THEN true ELSE false END", 2),
        ("CASE x WHEN 0 THEN true ELSE false END", 1),
        ("l[1] = 2", 1),
        ("size(l[0..1]) = 1", 4),
        ("size([y IN l WHERE y > 1]) = 2", 2),
        ("any(y IN l WHERE y = 3)", 2),
        ("reduce(s = 0, y IN l | s + y) > 3", 2),
        ("{a: x}.a = 3", 1),
        ("x IN [0, 3]", 2),
        ("toString(x) STARTS WITH '1'", 1),
        ("id(n) >= 0", 4),
        ("labels(n) = ['F']", 4),
        ("n.v", 0),
    ];
    for (text, want) in cases {
        match filter_count(&store, &rows, expr_of(text)) {
            Ok(n) => assert_eq!(n, *want, "{text}"),
            Err(e) if *text == "n.v" => assert!(e.to_string().contains("Predicate must evaluate to boolean"), "{e}"),
            Err(e) => panic!("{text}: {e}"),
        }
    }
    // A parameter bound in the row, a path variable and a bare variable.
    assert_eq!(
        filter_count(&store, &rows, Expression::Binary { left: Box::new(Expression::Parameter("p".into())), op: BinaryOp::Eq, right: Box::new(var("x")) }).unwrap(),
        1
    );
    assert_eq!(
        filter_count(&store, &rows, Expression::Unary { op: UnaryOp::IsNotNull, expr: Box::new(Expression::PathVariable("path".into())) }).unwrap(),
        4
    );
    assert!(filter_count(&store, &rows, Expression::Parameter("missing".into()))
        .unwrap_err()
        .to_string()
        .contains("Unresolved parameter: $missing"));
    assert!(filter_count(&store, &rows, Expression::PathVariable("nope".into())).is_err());
    assert!(filter_count(&store, &rows, var("nope")).is_err());
}

#[test]
fn filter_unary_errors_and_nulls() {
    let (store, rows) = filter_rows();
    let not_int = Expression::Unary { op: UnaryOp::Not, expr: Box::new(var("x")) };
    assert!(matches!(filter_count(&store, &rows, not_int), Err(ExecutionError::TypeError(m)) if m.contains("NOT requires boolean")));
    let neg_str = Expression::Unary { op: UnaryOp::Minus, expr: Box::new(ls("a")) };
    let neg_str = Expression::Binary { left: Box::new(neg_str), op: BinaryOp::Eq, right: Box::new(li(1)) };
    assert!(matches!(filter_count(&store, &rows, neg_str), Err(ExecutionError::TypeError(m)) if m.contains("Negation requires numeric")));
    let overflow = Expression::Unary { op: UnaryOp::Minus, expr: Box::new(li(i64::MIN)) };
    let overflow = Expression::Binary { left: Box::new(overflow), op: BinaryOp::Eq, right: Box::new(li(1)) };
    assert!(matches!(filter_count(&store, &rows, overflow), Err(ExecutionError::RuntimeError(m)) if m.contains("out of range")));
    // NOT null and -null are null, which filters the row out.
    let null = Expression::Literal(PropertyValue::Null);
    assert_eq!(filter_count(&store, &rows, Expression::Unary { op: UnaryOp::Not, expr: Box::new(null.clone()) }).unwrap(), 0);
    let neg_null = Expression::Unary { op: UnaryOp::Minus, expr: Box::new(null) };
    assert_eq!(filter_count(&store, &rows, Expression::Unary { op: UnaryOp::IsNull, expr: Box::new(neg_null) }).unwrap(), 4);
}

#[test]
fn filter_operator_batch_mut_reset_and_describe() {
    let (mut store, rows) = filter_rows();
    let mut op = FilterOperator::new(Box::new(MaterializedOperator::new(rows.clone())), expr_of("x > 1"));
    let d = op.describe();
    assert_eq!(d.name, "Filter");
    assert_eq!(d.children.len(), 1);
    assert!(op.filter_predicate().is_some());
    assert_eq!(op.children_mut().len(), 1);
    let b = op.next_batch(&store, 10).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    assert!(op.next_batch(&store, 10).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 2);

    // Retaining the filter's read hands the values back with the batch.
    let mut op = FilterOperator::new(Box::new(MaterializedOperator::new(rows)), expr_of("n.v >= 2"));
    assert!(op.retain_property_reads("n", "v"));
    assert!(op.retain_property_reads("n", "v"), "asking again for the same read is fine");
    assert!(!op.retain_property_reads("n", "w"), "a second, different read is refused");
    let b = op.next_batch(&store, 10).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    assert_eq!(op.take_retained_reads(), Some(vec![Some(pint(2)), Some(pint(3))]));
    assert_eq!(op.take_retained_reads(), None);
}


// ---------------------------------------------------------------------------
// SortOperator
// ---------------------------------------------------------------------------

/// `:S` nodes whose `p` mixes strings, integers, a float, a boolean and absent.
fn mixed_sort_store() -> GraphStore {
    let mut store = GraphStore::new();
    let values: Vec<Option<PropertyValue>> = vec![
        Some(pstr("b")),
        Some(pint(2)),
        Some(pstr("a")),
        None,
        Some(PropertyValue::Float(1.5)),
        Some(PropertyValue::Boolean(true)),
        Some(pstr("c")),
        Some(pint(1)),
    ];
    for (i, v) in values.into_iter().enumerate() {
        let n = store.create_node("S");
        store.set_node_property(TENANT, n, "i", i as i64).unwrap();
        store.set_node_property(TENANT, n, "g", (i % 2) as i64).unwrap();
        if let Some(v) = v {
            store.set_node_property(TENANT, n, "p", v).unwrap();
        }
    }
    store
}

#[test]
fn sort_mixes_strings_with_other_types_in_cypher_order() {
    let store = mixed_sort_store();
    // Strings sort before booleans, which sort before numbers; null is last.
    let asc = ints(&read(&store, "MATCH (n:S) RETURN n.i AS i ORDER BY n.p"), "i");
    assert_eq!(asc, vec![2, 0, 6, 5, 7, 4, 1, 3]);
    let desc = ints(&read(&store, "MATCH (n:S) RETURN n.i AS i ORDER BY n.p DESC"), "i");
    assert_eq!(desc, vec![3, 1, 4, 7, 5, 6, 0, 2]);
    let top = ints(&read(&store, "MATCH (n:S) RETURN n.i AS i ORDER BY n.p LIMIT 3"), "i");
    assert_eq!(top, vec![2, 0, 6]);
    // Three keys use the heap-allocated key.
    let three = ints(&read(&store, "MATCH (n:S) RETURN n.i AS i ORDER BY n.g, n.p DESC, n.i LIMIT 4"), "i");
    assert_eq!(three, vec![4, 6, 0, 2]);
    let distinct = read(&store, "MATCH (n:S) RETURN DISTINCT n.g AS g ORDER BY n.g DESC LIMIT 1");
    assert_eq!(ints(&distinct, "g"), vec![1]);
    let expr_key = ints(&read(&store, "MATCH (n:S) RETURN n.i AS i ORDER BY n.i % 3, n.i DESC"), "i");
    assert_eq!(expr_key, vec![6, 3, 0, 7, 4, 1, 5, 2]);
}

#[test]
fn sort_operator_direct_paths() {
    let mut store = GraphStore::new();
    let rows = || -> OperatorBox {
        Box::new(MaterializedOperator::new(
            [3i64, 1, 2].iter().map(|i| a_record("x", Value::Property(pint(*i)))).collect(),
        ))
    };
    let mut op = SortOperator::new(rows(), vec![(var("x"), false)]);
    let d = op.describe();
    assert_eq!(d.name, "Sort");
    assert_eq!(op.children_mut().len(), 1);
    let got: Vec<i64> = drain(&mut op, &store).iter().map(|r| rec_int(r, "x")).collect();
    assert_eq!(got, vec![3, 2, 1]);
    op.reset();
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    assert_eq!(op.next_batch(&store, 2).unwrap().unwrap().records.len(), 1);
    assert!(op.next_batch(&store, 2).unwrap().is_none());
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 3);

    let mut limited = SortOperator::new(rows(), vec![(var("x"), true)]);
    assert!(limited.try_push_limit(1));
    let got: Vec<i64> = drain(&mut limited, &store).iter().map(|r| rec_int(r, "x")).collect();
    assert_eq!(got, vec![1]);
    let mut soft = SortOperator::new(rows(), vec![(var("x"), true)]);
    soft.hint_early_stop(1);
    let got: Vec<i64> = drain(&mut soft, &store).iter().map(|r| rec_int(r, "x")).collect();
    assert_eq!(got, vec![1, 2, 3], "a soft hint still yields every row in order");

    let mut bad = SortOperator::new(rows(), vec![(var("missing"), true)]);
    assert!(bad.next(&store).is_err());
}

#[test]
#[ignore = "bug: `[1, x] = [1, 2]` is a TypeError (a list built from a variable is treated as an entity by `=`)"]
fn list_equality_with_a_variable_element() {
    let store = GraphStore::new();
    let b = read(&store, "UNWIND [0, 1, 2] AS x WITH x WHERE [1, x] = [1, 2] RETURN x");
    assert_eq!(ints(&b, "x"), vec![2]);
}

// ---------------------------------------------------------------------------
// Variable-length paths: selectors, restrictors and bound walks
// ---------------------------------------------------------------------------

/// A directed 4-cycle 0 -> 1 -> 2 -> 3 -> 0 plus a chord 0 -> 2, all `:R`,
/// with node property `i` and edge property `w`.
fn cycle_store() -> GraphStore {
    let mut store = GraphStore::new();
    write(
        &mut store,
        "CREATE (a:C {i: 0}), (b:C {i: 1}), (c:C {i: 2}), (d:C {i: 3}), \
         (a)-[:R {w: 1}]->(b), (b)-[:R {w: 1}]->(c), (c)-[:R {w: 1}]->(d), (d)-[:R {w: 1}]->(a), \
         (a)-[:R {w: 2}]->(c)",
    );
    store
}

fn count_rows(store: &GraphStore, q: &str) -> usize {
    read(store, q).records.len()
}

#[test]
fn path_selectors_pick_among_matches() {
    let s = cycle_store();
    // Two trails from 0 to 2 within three hops: the chord and 0->1->2.
    assert_eq!(count_rows(&s, "MATCH (a:C {i: 0})-[:R*1..3]->(b:C {i: 2}) RETURN b"), 2);
    assert_eq!(count_rows(&s, "MATCH ALL SHORTEST (a:C {i: 0})-[:R*1..3]->(b:C {i: 2}) RETURN b"), 1);
    assert_eq!(count_rows(&s, "MATCH ANY SHORTEST (a:C {i: 0})-[:R*1..3]->(b:C {i: 2}) RETURN b"), 1);
    assert_eq!(count_rows(&s, "MATCH ANY (a:C {i: 0})-[:R*1..3]->(b:C {i: 2}) RETURN b"), 1);
    assert_eq!(count_rows(&s, "MATCH ALL (a:C {i: 0})-[:R*1..3]->(b:C {i: 2}) RETURN b"), 2);
    let b = read(&s, "MATCH ANY SHORTEST p = (a:C {i: 0})-[:R*]->(b:C) RETURN b.i AS i, length(p) AS l ORDER BY i");
    assert_eq!(ints(&b, "i"), vec![0, 1, 2, 3]);
    // Back to the start through the chord: 0 -> 2 -> 3 -> 0.
    assert_eq!(ints(&b, "l"), vec![3, 1, 1, 2]);
}

#[test]
fn path_restrictors_bound_repetition() {
    let s = cycle_store();
    // Back to the start: a trail may revisit the first node at the end.
    assert_eq!(count_rows(&s, "MATCH TRAIL (a:C {i: 0})-[:R*1..4]->(b:C {i: 0}) RETURN b"), 2);
    assert_eq!(count_rows(&s, "MATCH SIMPLE (a:C {i: 0})-[:R*1..4]->(b:C {i: 0}) RETURN b"), 2);
    assert_eq!(count_rows(&s, "MATCH ACYCLIC (a:C {i: 0})-[:R*1..4]->(b:C {i: 0}) RETURN b"), 0);
    // A walk may repeat edges; bounded, it is finite.
    let walks = count_rows(&s, "MATCH WALK (a:C {i: 0})-[:R*1..5]->(b) RETURN b");
    let trails = count_rows(&s, "MATCH TRAIL (a:C {i: 0})-[:R*1..5]->(b) RETURN b");
    assert!(walks > trails, "{walks} walks vs {trails} trails");
    assert_eq!(count_rows(&s, "MATCH ANY SHORTEST ACYCLIC (a:C {i: 0})-[:R*]->(b:C {i: 3}) RETURN b"), 1);
}

#[test]
fn variable_length_details() {
    let s = cycle_store();
    // An edge-property filter on every hop.
    assert_eq!(count_rows(&s, "MATCH (a:C {i: 0})-[:R*1..3 {w: 1}]->(b:C {i: 3}) RETURN b"), 1);
    // A bound end on both sides.
    let b = read(&s, "MATCH (a:C {i: 1}), (b:C {i: 0}) MATCH p = (a)-[:R*]->(b) RETURN length(p) AS l");
    assert_eq!(ints(&b, "l"), vec![3]);
    // Incoming and undirected.
    // Only 2 -> 3 -> 0 arrives at 0 in exactly two hops.
    assert_eq!(count_rows(&s, "MATCH (a:C {i: 0})<-[:R*2]-(b) RETURN b"), 1);
    let b = read(&s, "MATCH (a:C {i: 0})-[:R*1]-(b) RETURN count(DISTINCT b) AS c");
    assert_eq!(int(&b, 0, "c"), 3);
    // Target labels and target properties restrict the far end.
    assert_eq!(count_rows(&s, "MATCH (a:C {i: 0})-[:R*1..2]->(b:C {i: 3}) RETURN b"), 1);
    assert_eq!(count_rows(&s, "MATCH (a:C {i: 0})-[:R*1..2]->(b:Nope) RETURN b"), 0);
    // A relationship list variable, then walked again as a bound list.
    let b = read(
        &s,
        "MATCH (a:C {i: 0})-[rs:R*2]->(b:C {i: 2}) WITH a, rs MATCH (a)-[rs*]->(c) RETURN c.i AS i",
    );
    assert_eq!(ints(&b, "i"), vec![2]);
    let b = read(&s, "MATCH (a:C {i: 0})-[rs:R*2]->(b) RETURN [r IN rs | r.w] AS ws ORDER BY b.i");
    assert_eq!(b.records.len(), 2);
    // Zero hops binds the start itself.
    let b = read(&s, "MATCH (a:C {i: 2})-[:R*0]->(b) RETURN b.i AS i");
    assert_eq!(ints(&b, "i"), vec![2]);
    // Unbounded from a pinned target.
    let b = read(&s, "MATCH (b:C {i: 3}) MATCH (a:C)-[:R*]->(b) RETURN count(DISTINCT a) AS c");
    assert_eq!(int(&b, 0, "c"), 4);
    // An optional variable-length match with no path.
    let b = read(&s, "MATCH (a:C {i: 0}) OPTIONAL MATCH (a)-[:NONE*]->(b) RETURN b");
    assert_eq!(cell(&b, 0, "b"), PropertyValue::Null);
}

#[test]
fn variable_length_operator_direct() {
    let (store, ids) = path4();
    let start = NodeId::new(ids[0] as u64);
    let mut op = VarLengthExpandOperator::new(
        node_rows(&[start]),
        "n".into(),
        "m".into(),
        vec!["E".into()],
        Direction::Outgoing,
        1,
        3,
    )
    .with_target_labels(vec![Label::new("V")])
    .with_path_variable("p".into())
    .with_edge_properties(HashMap::from([("w".to_string(), PropertyValue::Float(1.0))]));
    let d = op.describe();
    assert!(!d.name.is_empty());
    assert_eq!(op.children_mut().len(), 1);
    let rows = drain(&mut op, &store);
    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|r| matches!(r.get("p"), Some(Value::Path { .. }))));
    op.reset();
    let b = op.next_batch(&store, 2).unwrap().unwrap();
    assert_eq!(b.records.len(), 2);
    let mut store = store;
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 3);

    // A null source yields nothing; a non-node source is a type error.
    let mut op = VarLengthExpandOperator::new(
        Box::new(MaterializedOperator::new(vec![a_record("n", Value::Null)])),
        "n".into(),
        "m".into(),
        vec![],
        Direction::Both,
        1,
        2,
    )
    .with_reversed_walk();
    assert!(drain(&mut op, &store).is_empty());
    let mut op = VarLengthExpandOperator::new(
        Box::new(MaterializedOperator::new(vec![a_record("n", Value::Property(pint(1)))])),
        "n".into(),
        "m".into(),
        vec![],
        Direction::Incoming,
        1,
        2,
    );
    assert!(op.next(&store).is_err());
}

// ---------------------------------------------------------------------------
// WITH and aggregation corners
// ---------------------------------------------------------------------------

#[test]
fn with_barrier_corners() {
    let s = social();
    let b = read(&s, "MATCH (n:Person) WITH n.age % 2 AS k, count(*) AS c ORDER BY k DESC RETURN k, c");
    assert_eq!(ints(&b, "k"), vec![1, 0]);
    let b = read(&s, "MATCH (n:Person) WITH DISTINCT n.age % 2 AS k ORDER BY k SKIP 1 RETURN k");
    assert_eq!(ints(&b, "k"), vec![1]);
    let b = read(&s, "MATCH (n:Person) WITH n.age % 2 AS k, collect(n.name) AS ns WHERE size(ns) > 1 RETURN k ORDER BY k");
    assert_eq!(ints(&b, "k"), vec![0, 1]);
    let b = read(&s, "MATCH (n:Person) WITH n ORDER BY n.age LIMIT 2 RETURN collect(n.name) AS ns");
    assert_eq!(cell(&b, 0, "ns"), PropertyValue::Array(vec![pstr("a"), pstr("b")]));
    let b = read(&s, "MATCH (n:Person) WITH sum(n.age) AS total, max(n.age) AS top RETURN total, top");
    assert_eq!((int(&b, 0, "total"), int(&b, 0, "top")), (10, 4));
    let b = read(&s, "MATCH (n:Nope) WITH count(n) AS c RETURN c");
    assert_eq!(int(&b, 0, "c"), 0);
    let b = read(&s, "MATCH (n:Nope) WITH n.x AS x, count(*) AS c RETURN c");
    assert!(b.records.is_empty());
    let b = read(&s, "MATCH (n:Person) WITH n.name AS name, n.age AS age WHERE age > 1 WITH name ORDER BY name DESC LIMIT 1 RETURN name");
    assert_eq!(strings(&b, "name"), vec!["d"]);
    let b = read(&s, "MATCH (n:Person) WITH percentileCont(n.age, 0.5) AS p, stDevP(n.age) AS sd RETURN p, sd");
    assert_eq!(float(&b, 0, "p"), 2.5);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b) WITH a, count(b) AS c, collect(b) AS bs RETURN sum(c) AS s");
    assert_eq!(int(&b, 0, "s"), 4);
    let b = read(&s, "UNWIND [1, 2, 3] AS x WITH x WHERE x > 1 RETURN sum(x) AS s");
    assert_eq!(int(&b, 0, "s"), 5);
}

#[test]
fn aggregation_corners() {
    let s = social();
    let b = read(&s, "MATCH (n:Person) RETURN count(DISTINCT n) AS c, count(n.missing) AS m, collect(DISTINCT n.age > 2) AS flags");
    assert_eq!((int(&b, 0, "c"), int(&b, 0, "m")), (4, 0));
    let b = read(&s, "MATCH (n) RETURN labels(n) AS l, count(*) AS c ORDER BY c DESC");
    assert!(b.records.len() >= 2);
    let b = read(&s, "MATCH (n:Person)-[r]->(m) RETURN type(r) AS t, count(DISTINCT m) AS c, sum(r.w) AS w ORDER BY t");
    assert_eq!(strings(&b, "t"), vec!["KNOWS", "LIKES", "LIVES_IN"]);
    let b = read(&s, "MATCH (n:Person) RETURN n.age > 2 AS old, avg(n.age) AS a, min(n.age) AS mn ORDER BY old");
    assert_eq!(b.records.len(), 2);
    let b = read(&s, "MATCH (n:Person) RETURN percentileDisc(n.age, 1.0) AS p");
    assert_eq!(float(&b, 0, "p"), 4.0);
    let e = read_err(&s, "MATCH (n:Person) RETURN percentileCont(n.age, 2.0) AS p");
    assert!(e.contains("percentile"), "{e}");
    let b = read(&s, "MATCH (n:Person) RETURN n.name AS name, n.age AS age, count(*) AS c ORDER BY name LIMIT 2");
    assert_eq!(strings(&b, "name"), vec!["a", "b"]);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS name, count(DISTINCT b.age) AS c ORDER BY name");
    assert_eq!(strings(&b, "name"), vec!["a", "b", "c", "d"]);
    assert_eq!(ints(&b, "c"), vec![1, 1, 1, 1]);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, count(DISTINCT b.age) AS c");
    let mut sources: Vec<String> = b
        .records
        .iter()
        .map(|r| match r.get("a") {
            Some(v) => match v.resolve_property("name", &s) {
                PropertyValue::String(n) => n,
                other => format!("{other:?}"),
            },
            None => "<unbound>".to_string(),
        })
        .collect();
    sources.sort();
    assert_eq!(sources, vec!["a", "b", "c", "d"], "rows: {:?}", b.records);
    let b = read(&s, "MATCH (a:Person) OPTIONAL MATCH (a)-[:LIVES_IN]->(c) RETURN a.name AS name, count(c) AS n ORDER BY name");
    assert_eq!(ints(&b, "n"), vec![1, 1, 0, 0]);
    let b = read(&s, "MATCH (a:Person)-[:KNOWS]-(b) WHERE b.age > 1 RETURN a.name AS name, count(b) AS n ORDER BY name");
    // d's only neighbour is a, who is too young, so d has no group.
    assert_eq!(strings(&b, "name"), vec!["a", "b", "c"]);
    assert_eq!(ints(&b, "n"), vec![2, 1, 1]);
}

// ---------------------------------------------------------------------------
// The per-operator expression evaluators agree with the shared one
// ---------------------------------------------------------------------------

#[test]
fn every_operator_evaluator_agrees_with_eval_expression() {
    let mut store = GraphStore::new();
    let a = store.create_node("E");
    store.set_node_property(TENANT, a, "v", 3i64).unwrap();
    store.set_node_property(TENANT, a, "s", "abc").unwrap();
    let b = store.create_node("E");
    store.create_edge(a, b, "R").unwrap();

    let mut row = a_record("n", Value::NodeRef(a));
    row.bind("x", Value::Property(pint(2)));
    row.bind("l", Value::Property(PropertyValue::Array(vec![pint(1), pint(2), pint(3)])));
    row.bind("$p", Value::Property(pint(7)));
    row.bind("path", Value::Path { nodes: vec![a], edges: vec![] });

    let mut exprs: Vec<(String, Expression)> = [
        "n.v + x",
        "n.s STARTS WITH 'a'",
        "n.s = 'abc'",
        "NOT (x > 1)",
        "-x",
        "-(1.5)",
        "n.missing IS NULL",
        "n.v IS NOT NULL",
        "toUpper(n.s)",
        "size(l)",
        "CASE WHEN x > 1 THEN 'big' ELSE 'small' END",
        "CASE x WHEN 2 THEN 1 END",
        "l[0]",
        "l[1..]",
        "l[..2]",
        "[y IN l WHERE y > 1 | y * 10]",
        "all(y IN l WHERE y > 0)",
        "reduce(acc = 0, y IN l | acc + y)",
        "EXISTS { (n)-[:R]->() }",
        "[(n)-[:R]->(m) | id(m)]",
        "[x, n.v]",
        "{k: x}",
        "x * 2 = 4",
        "3",
    ]
    .iter()
    .map(|t| (t.to_string(), expr_of(t)))
    .collect();
    exprs.push(("$p".into(), Expression::Parameter("p".into())));
    exprs.push(("path".into(), Expression::PathVariable("path".into())));

    let filter = FilterOperator::new(Box::new(SingleRowOperator::new()), expr_of("true"));
    let project = ProjectOperator::new(Box::new(SingleRowOperator::new()), vec![]);
    for (text, e) in &exprs {
        let want = eval_expression(e, &row, &store).unwrap_or_else(|err| panic!("{text}: {err}"));
        let got = [
            ("filter", filter.evaluate_expression(e, &row, &store)),
            ("project", project.evaluate_expression(e, &row, &store)),
            ("aggregate", AggregateOperator::evaluate_expression(e, &row, &store)),
            ("sort", SortOperator::evaluate_expression(e, &row, &store)),
            ("with", WithBarrierOperator::evaluate_expression(e, &row, &store)),
        ];
        for (who, v) in got {
            let v = v.unwrap_or_else(|err| panic!("{who} {text}: {err}"));
            assert_eq!(v, want, "{who} disagrees on `{text}`");
        }
    }

    // Unresolved names are errors everywhere a lookup can fail.
    for e in [Expression::Parameter("zz".into()), Expression::PathVariable("zz".into())] {
        assert!(filter.evaluate_expression(&e, &row, &store).is_err());
        assert!(project.evaluate_expression(&e, &row, &store).is_err());
        assert!(AggregateOperator::evaluate_expression(&e, &row, &store).is_err());
        assert!(SortOperator::evaluate_expression(&e, &row, &store).is_err());
        assert!(WithBarrierOperator::evaluate_expression(&e, &row, &store).is_err());
    }
}

// ---------------------------------------------------------------------------
// The per-type adjacency fast path, which starts after 512 input rows
// ---------------------------------------------------------------------------

/// 600 `:N` nodes on a ring with `:T` edges `i -> i+1` and `i -> i+2`
/// (mod 600), one `:T` self-loop on node 0, and an `:U` edge per node so the
/// type filter has something to exclude.
fn ring_store() -> GraphStore {
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..600).map(|_| store.create_node("N")).collect();
    for i in 0..600 {
        store.create_edge(ids[i], ids[(i + 1) % 600], "T").unwrap();
        store.create_edge(ids[i], ids[(i + 2) % 600], "T").unwrap();
        store.create_edge(ids[i], ids[(i + 3) % 600], "U").unwrap();
    }
    store.create_edge(ids[0], ids[0], "T").unwrap();
    store
}

#[test]
fn typed_expands_past_the_type_index_threshold() {
    let s = ring_store();
    assert_eq!(count_of(&s, "MATCH (a:N)-[:T]->(b) RETURN count(*) AS c"), 1201);
    assert_eq!(count_of(&s, "MATCH (a:N)<-[:T]-(b) RETURN count(*) AS c"), 1201);
    // Undirected: every edge from both ends, the self-loop once.
    assert_eq!(count_of(&s, "MATCH (a:N)-[:T]-(b) RETURN count(*) AS c"), 2401);
    // A cyclic close: a -> a+1 -> a+2 closed by a -> a+2.
    assert_eq!(
        count_of(&s, "MATCH (a:N)-[:T]->(b)-[:T]->(c)<-[:T]-(a) RETURN count(*) AS c"),
        600
    );
    assert_eq!(
        count_of(&s, "MATCH (a:N)-[:T]->(c), (a)-[:T]->(b)-[:T]->(c) RETURN count(*) AS c"),
        600
    );
    // The same close written from the other end.
    assert_eq!(
        count_of(&s, "MATCH (c:N)<-[:T]-(b)<-[:T]-(a)-[:T]->(c) RETURN count(*) AS c"),
        600
    );
    // Undirected closes see each directed triangle-with-chord more than once,
    // but the typed and untyped walks must agree.
    let typed = count_of(&s, "MATCH (a:N)-[:T]-(b)-[:T]-(c)-[:T]-(a) RETURN count(*) AS c");
    let any = count_of(&s, "MATCH (a:N)-[x]-(b)-[y]-(c)-[z]-(a) WHERE type(x) = 'T' AND type(y) = 'T' AND type(z) = 'T' RETURN count(*) AS c");
    assert_eq!(typed, any);
    assert!(typed > 0);
}

#[test]
fn typed_variable_length_walks_agree_with_fixed_hops() {
    let s = ring_store();
    let var2 = count_of(&s, "MATCH (a:N)-[:T*2]->(b) RETURN count(*) AS c");
    let fixed2 = count_of(&s, "MATCH (a:N)-[:T]->(m)-[:T]->(b) RETURN count(*) AS c");
    assert_eq!(var2, fixed2);
    let var_in = count_of(&s, "MATCH (a:N)<-[:T*2]-(b) RETURN count(*) AS c");
    assert_eq!(var_in, fixed2);
    let var_both = count_of(&s, "MATCH (a:N)-[:T*1..2]-(b) RETURN count(*) AS c");
    let fixed_both1 = count_of(&s, "MATCH (a:N)-[:T]-(b) RETURN count(*) AS c");
    let fixed_both2 = count_of(&s, "MATCH (a:N)-[:T]-(m)-[:T]-(b) RETURN count(*) AS c");
    assert_eq!(var_both, fixed_both1 + fixed_both2);
    // Pinned far end, many sources.
    let pinned = count_of(&s, "MATCH (z:N) WITH z LIMIT 1 MATCH (a:N)-[:T*1..2]->(z) RETURN count(*) AS c");
    assert!(pinned >= 4, "{pinned}");
}

// ---------------------------------------------------------------------------
// MATCH ... CREATE / MERGE edge operators and combined CREATE, directly
// ---------------------------------------------------------------------------

fn edge_count(store: &GraphStore, ty: &str) -> i64 {
    count_of(store, &format!("MATCH ()-[r:{ty}]->() RETURN count(r) AS c"))
}

#[test]
fn match_create_edge_operator_paths() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let mut row = a_record("a", Value::NodeRef(a));
    row.bind("b", Value::NodeRef(b));
    row.bind("x", Value::Property(pint(4)));
    let input = |r: Record| -> OperatorBox { Box::new(MaterializedOperator::new(vec![r])) };
    let nodes = vec![(
        "c".to_string(),
        vec![Label::new("C")],
        HashMap::from([("lit".to_string(), pint(1))]),
        Some(HashMap::from([("fromx".to_string(), var("x"))])),
    )];
    let edges: Vec<EdgeToCreate> = vec![
        ("a".into(), "c".into(), EdgeType::new("R"), HashMap::from([("w".to_string(), pint(1))]), Some("r".into()), Some(HashMap::from([("x2".to_string(), var("x"))]))),
        ("missing".into(), "b".into(), EdgeType::new("R"), HashMap::new(), None, None),
        ("a".into(), "missing".into(), EdgeType::new("R"), HashMap::new(), None, None),
    ];
    let mut op = MatchCreateEdgeOperator::with_nodes(input(row.clone()), nodes.clone(), edges);
    assert!(op.is_mutating());
    assert_eq!(op.children_mut().len(), 1);
    assert!(matches!(op.next(&store), Err(ExecutionError::RuntimeError(_))));
    let rows = drain_mut(&mut op, &mut store);
    // Only the edge whose ends are bound is made.
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].get("r"), Some(Value::Edge(..))));
    assert!(rows[0].get("_edge").is_some());
    assert_eq!(edge_count(&store, "R"), 1);
    let b2 = read(&store, "MATCH (a:A)-[r:R]->(c:C) RETURN c.lit AS lit, c.fromx AS fx, r.w AS w, r.x2 AS x2");
    assert_eq!((int(&b2, 0, "lit"), int(&b2, 0, "fx"), int(&b2, 0, "w"), int(&b2, 0, "x2")), (1, 4, 1, 4));
    op.reset();
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(edge_count(&store, "R"), 2);

    // Nodes only: each input row passes through.
    let mut nodes_only = MatchCreateEdgeOperator::with_nodes(input(row.clone()), nodes, vec![]);
    assert_eq!(drain_mut(&mut nodes_only, &mut store).len(), 1);
    let mut plain = MatchCreateEdgeOperator::new(input(row.clone()), vec![]);
    assert_eq!(drain_mut(&mut plain, &mut store).len(), 1);

    // A non-scalar property expression is refused.
    let bad_nodes = vec![(
        "c".to_string(),
        vec![Label::new("C")],
        HashMap::new(),
        Some(HashMap::from([("bad".to_string(), var("a"))])),
    )];
    let mut bad = MatchCreateEdgeOperator::with_nodes(input(row), bad_nodes, vec![]);
    match bad.next_mut(&mut store, TENANT) {
        Err(ExecutionError::TypeError(m)) => assert!(m.contains("property `bad` must be a scalar"), "{m}"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn match_merge_edge_operator_paths() {
    let mut store = GraphStore::new();
    let a = store.create_node("A");
    let b = store.create_node("B");
    let mut row = a_record("a", Value::NodeRef(a));
    row.bind("b", Value::NodeRef(b));
    let input = |r: Record| -> OperatorBox { Box::new(MaterializedOperator::new(vec![r])) };
    let edges = vec![
        ("a".to_string(), "b".to_string(), EdgeType::new("M"), HashMap::from([("k".to_string(), pint(1))]), Some("r".to_string()), false),
        ("zz".to_string(), "b".to_string(), EdgeType::new("M"), HashMap::new(), None, false),
        ("a".to_string(), "zz".to_string(), EdgeType::new("M"), HashMap::new(), None, false),
    ];
    let on_create = vec![
        ("r".to_string(), "created".to_string(), li(1)),
        ("_edge".to_string(), "gone".to_string(), Expression::Literal(PropertyValue::Null)),
        ("r".to_string(), "list".to_string(), Expression::ListExpr(vec![var("a")])),
    ];
    let on_match = vec![
        ("r".to_string(), "matched".to_string(), li(2)),
        ("r".to_string(), "created".to_string(), Expression::Literal(PropertyValue::Null)),
        ("r".to_string(), "entity".to_string(), var("a")),
    ];
    let make = |store_row: Record| {
        MatchMergeEdgeOperator::new(input(store_row), edges.clone(), on_create.clone(), on_match.clone()).with_entity_sets(
            vec![("r".to_string(), true, lmap(&[("ce", pint(3))]))],
            vec![("_edge".to_string(), true, lmap(&[("me", pint(4))]))],
        )
    };
    let mut op = make(row.clone());
    assert!(op.is_mutating());
    assert_eq!(op.children_mut().len(), 1);
    assert!(matches!(op.next(&store), Err(ExecutionError::RuntimeError(_))));
    let rows = drain_mut(&mut op, &mut store);
    assert_eq!(rows.len(), 1);
    let b1 = read(&store, "MATCH ()-[r:M]->() RETURN r.k AS k, r.created AS c, r.ce AS ce, r.matched AS m");
    assert_eq!((int(&b1, 0, "k"), int(&b1, 0, "c"), int(&b1, 0, "ce")), (1, 1, 3));
    assert_eq!(cell(&b1, 0, "m"), PropertyValue::Null);

    // Second run matches the edge: ON MATCH applies, ON CREATE does not.
    op.reset();
    let rows = drain_mut(&mut op, &mut store);
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0].get("r"), Some(Value::Edge(..))));
    assert_eq!(edge_count(&store, "M"), 1);
    let b2 = read(&store, "MATCH ()-[r:M]->() RETURN r.created AS c, r.matched AS m, r.me AS me");
    assert_eq!(cell(&b2, 0, "c"), PropertyValue::Null);
    assert_eq!((int(&b2, 0, "m"), int(&b2, 0, "me")), (2, 4));

    // Undirected: an edge the other way round matches too.
    let mut rev = a_record("a", Value::NodeRef(b));
    rev.bind("b", Value::NodeRef(a));
    let undirected = vec![("a".to_string(), "b".to_string(), EdgeType::new("M"), HashMap::new(), None, true)];
    let mut op = MatchMergeEdgeOperator::new(input(rev), undirected, vec![], vec![]);
    assert_eq!(drain_mut(&mut op, &mut store).len(), 1);
    assert_eq!(edge_count(&store, "M"), 1);
}

#[test]
fn create_nodes_and_edges_operator_paths() {
    let mut store = GraphStore::new();
    let nodes = CreateNodeOperator::new(vec![
        (vec![Label::new("A")], HashMap::from([("id".to_string(), pint(7))]), Some("a".into()), None),
        (vec![Label::new("B")], HashMap::new(), Some("b".into()), None),
    ]);
    let edges: Vec<EdgeToCreate> = vec![
        (
            "a".into(),
            "b".into(),
            EdgeType::new("R"),
            HashMap::from([("lit".to_string(), pint(1))]),
            Some("r".into()),
            Some(HashMap::from([
                ("fromA".to_string(), Expression::Property { variable: "a".into(), property: "id".into() }),
                ("entity".to_string(), var("a")),
            ])),
        ),
        ("b".into(), "a".into(), EdgeType::new("S"), HashMap::new(), None, None),
    ];
    let mut op = CreateNodesAndEdgesOperator::new(Box::new(nodes), edges);
    assert!(op.is_mutating());
    assert_eq!(op.children_mut().len(), 1);
    let rows = drain_mut(&mut op, &mut store);
    assert_eq!(rows.len(), 1);
    assert!(rows[0].get("r").is_some());
    assert!(rows[0].get("__created_edge_1").is_some());
    let b = read(&store, "MATCH (:A)-[r:R]->(:B) RETURN r.lit AS l, r.fromA AS f, r.entity AS e");
    assert_eq!((int(&b, 0, "l"), int(&b, 0, "f")), (1, 7));
    assert_eq!(cell(&b, 0, "e"), PropertyValue::Null, "an entity is not a storable property");
    op.reset();

    let missing: Vec<EdgeToCreate> = vec![("a".into(), "nope".into(), EdgeType::new("R"), HashMap::new(), None, None)];
    let nodes = CreateNodeOperator::new(vec![(vec![Label::new("A")], HashMap::new(), Some("a".into()), None)]);
    let mut op = CreateNodesAndEdgesOperator::new(Box::new(nodes), missing);
    assert!(matches!(op.next_mut(&mut store, TENANT), Err(ExecutionError::VariableNotFound(v)) if v == "nope"));
    let missing: Vec<EdgeToCreate> = vec![("nope".into(), "a".into(), EdgeType::new("R"), HashMap::new(), None, None)];
    let nodes = CreateNodeOperator::new(vec![(vec![Label::new("A")], HashMap::new(), Some("a".into()), None)]);
    let mut op = CreateNodesAndEdgesOperator::new(Box::new(nodes), missing);
    assert!(matches!(op.next_mut(&mut store, TENANT), Err(ExecutionError::VariableNotFound(v)) if v == "nope"));
}
