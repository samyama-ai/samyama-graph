//! A named path spans every segment of its pattern (#966).
//!
//! ```cypher
//! MATCH p = (a {name: 'A'})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c) RETURN p
//! ```
//!
//! returned the right *number* of rows and every path a segment short:
//! `<()>`, `<()>`, `<()-[]->()>`  where the answers are `<(A)>`,
//! `<(A)-[:KNOWS]->(B)>`, `<(A)-[:KNOWS]->(B)-[:FRIEND]->(C)>`.
//!
//! Every segment of a pattern is given the same path variable, and each expand
//! **replaced** the binding with its own walk. With two segments the second
//! won, so `p` held only the last hop.
//!
//! The row count being right is what makes this quiet: the query looks like it
//! matched correctly and only the paths are wrong, so a test that counts rows
//! sees nothing.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `(:A {name:'A'})-[:KNOWS]->(:B {name:'B'})-[:FRIEND]->(:C {name:'C'})`
fn chain() -> GraphStore {
    let mut store = GraphStore::new();
    let ids: Vec<_> = ["A", "B", "C"]
        .iter()
        .map(|l| {
            let n = store.create_node_with_labels([Label::new(*l)]);
            let _ = store.set_node_property(
                "default", n, "name".to_string(), PropertyValue::String((*l).into()));
            n
        })
        .collect();
    store.create_edge(ids[0], ids[1], "KNOWS").unwrap();
    store.create_edge(ids[1], ids[2], "FRIEND").unwrap();
    store
}

/// `(nodes, edges)` of each returned path, sorted by length.
fn shapes(store: &GraphStore, cypher: &str) -> Vec<(usize, usize)> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let mut out: Vec<(usize, usize)> = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"))
        .records
        .iter()
        .map(|r| match r.get("p") {
            Some(Value::Path { nodes, edges }) => (nodes.len(), edges.len()),
            other => panic!("{cypher}: {other:?}"),
        })
        .collect();
    out.sort();
    out
}

#[test]
fn two_variable_length_segments_give_one_whole_path() {
    let store = chain();
    assert_eq!(
        shapes(&store, "MATCH p = (a {name: \"A\"})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c) RETURN p"),
        vec![(1, 0), (2, 1), (3, 2)]
    );
}

#[test]
fn a_single_variable_length_segment_is_unchanged() {
    let store = chain();
    assert_eq!(
        shapes(&store, "MATCH p = (a {name: \"A\"})-[:KNOWS*0..1]->(b) RETURN p"),
        vec![(1, 0), (2, 1)]
    );
}

#[test]
fn every_path_is_internally_consistent() {
    // A path of n nodes has n-1 relationships. The broken binding produced
    // shapes that satisfied this too, which is why the lengths above are
    // asserted as well — but a violation here would be worse still.
    let store = chain();
    for (n, e) in shapes(
        &store,
        "MATCH p = (a {name: \"A\"})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c) RETURN p",
    ) {
        assert_eq!(n, e + 1, "path with {n} nodes and {e} relationships");
    }
}

#[test]
fn length_agrees_with_the_path_it_reports() {
    let store = chain();
    let q = parse_query(
        "MATCH p = (a {name: \"A\"})-[:KNOWS*0..1]->(b)-[:FRIEND*0..1]->(c) \
         RETURN length(p) AS len, size(nodes(p)) AS n",
    )
    .unwrap();
    for rec in &QueryExecutor::new(&store).execute(&q).unwrap().records {
        let g = |c: &str| match rec.get(c) {
            Some(Value::Property(PropertyValue::Integer(v))) => *v,
            other => panic!("{c}: {other:?}"),
        };
        assert_eq!(g("n"), g("len") + 1);
    }
}

#[test]
fn a_fixed_length_pattern_still_spans_both_segments() {
    let store = chain();
    assert_eq!(
        shapes(&store, "MATCH p = (a:A)-[:KNOWS]->(b)-[:FRIEND]->(c) RETURN p"),
        vec![(3, 2)]
    );
}

#[test]
fn three_segments_accumulate_too() {
    let mut store = chain();
    let d = store.create_node_with_labels([Label::new("D")]);
    let c = {
        let q = parse_query("MATCH (c:C) RETURN c").unwrap();
        match QueryExecutor::new(&store).execute(&q).unwrap().records[0].get("c") {
            Some(Value::Node(id, _)) | Some(Value::NodeRef(id)) => *id,
            other => panic!("{other:?}"),
        }
    };
    store.create_edge(c, d, "NEXT").unwrap();
    assert_eq!(
        shapes(&store, "MATCH p = (a:A)-[:KNOWS*1..1]->(b)-[:FRIEND*1..1]->(c)-[:NEXT*1..1]->(d) RETURN p"),
        vec![(4, 3)]
    );
}
