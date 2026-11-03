//! `CALL algo.pca(label, properties, nComponents?) YIELD node, projection`
//! (samyama-graph#1022).
//!
//! PCA was implemented in the algorithms crate, re-exported, used by the SDK,
//! and refused by Cypher as an unknown algorithm. These tests call it from
//! Cypher and hold it to the library: the projections a query returns are the
//! library's own `pca(..).transform(..)` of the same feature matrix.
//!
//! Features are read column-first. A node restored from a snapshot has no row
//! copy of its properties; reading the row would have fed PCA zeros.

use samyama::algo::{pca, PcaConfig};
use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

type Rows = Vec<(u64, Vec<f64>)>;

fn call(store: &GraphStore, cypher: &str) -> Result<Rows, String> {
    let q = parse_query(cypher).map_err(|e| format!("parse: {e}"))?;
    let out = QueryExecutor::new(store).execute(&q).map_err(|e| e.to_string())?;
    Ok(out
        .records
        .iter()
        .map(|r| {
            let id = r.get("node").and_then(|v| v.node_id()).expect("node").as_u64();
            let proj = match r.get("projection") {
                Some(samyama::query::executor::Value::List(items)) => items
                    .iter()
                    .map(|v| match v {
                        samyama::query::executor::Value::Property(PropertyValue::Float(f)) => *f,
                        other => panic!("projection holds {other:?}"),
                    })
                    .collect(),
                other => panic!("projection is {other:?}"),
            };
            (id, proj)
        })
        .collect())
}

/// Thirty `:P` nodes whose `a` and `b` move together and whose `c` is fixed,
/// plus two `:Q` nodes the label filter must leave out.
fn features(i: usize) -> [i64; 3] {
    [i as i64, 2 * i as i64 + (i % 3) as i64, 5]
}

fn row_store() -> (GraphStore, Vec<NodeId>) {
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..30)
        .map(|i| {
            let n = store.create_node("P");
            for (k, v) in ["a", "b", "c"].iter().zip(features(i)) {
                store.set_node_property("default", n, *k, v).unwrap();
            }
            n
        })
        .collect();
    for _ in 0..2 {
        let q = store.create_node("Q");
        store.set_node_property("default", q, "a", 1000i64).unwrap();
    }
    (store, ids)
}

/// The same values held only in columns, as a snapshot import leaves them.
fn column_store() -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..30 {
        let n = store.create_node_stub(Label::new("P"));
        for (k, v) in ["a", "b", "c"].iter().zip(features(i)) {
            store.set_column_property(n, k, PropertyValue::Integer(v));
        }
    }
    store
}

fn library_projection(k: usize) -> Vec<Vec<f64>> {
    let data: Vec<Vec<f64>> = (0..30).map(|i| features(i).iter().map(|&v| v as f64).collect()).collect();
    let config = PcaConfig { n_components: k, ..PcaConfig::default() };
    pca(&data, config).transform(&data)
}

fn close(a: &[f64], b: &[f64]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| (x - y).abs() < 1e-9)
}

#[test]
fn pca_is_callable_from_cypher_and_matches_the_library() {
    let (store, ids) = row_store();
    let got = call(&store, "CALL algo.pca('P', ['a', 'b', 'c'], 2) YIELD node, projection RETURN node, projection")
        .expect("algo.pca should run");
    assert_eq!(got.len(), 30, "one row per :P node, none for :Q");
    let expected = library_projection(2);
    for (i, (id, proj)) in got.iter().enumerate() {
        assert_eq!(*id, ids[i].as_u64(), "rows are in node-id order");
        assert!(close(proj, &expected[i]), "node {id}: {proj:?} vs the library's {:?}", expected[i]);
    }
}

#[test]
fn column_only_nodes_are_read() {
    let (rows, _) = row_store();
    let cols = column_store();
    let q = "CALL algo.pca('P', ['a', 'b', 'c'], 2) YIELD node, projection RETURN node, projection";
    let from_rows: Vec<Vec<f64>> = call(&rows, q).unwrap().into_iter().map(|(_, p)| p).collect();
    let from_cols: Vec<Vec<f64>> = call(&cols, q).unwrap().into_iter().map(|(_, p)| p).collect();
    assert_eq!(from_cols.len(), 30);
    for (a, b) in from_rows.iter().zip(&from_cols) {
        assert!(close(a, b), "a column-only node projected differently: {b:?} vs {a:?}");
    }
    assert!(from_cols.iter().any(|p| p.iter().any(|x| x.abs() > 1e-6)), "column-only features read as zeros");
}

#[test]
fn n_components_defaults_to_two_and_is_honoured() {
    let (store, _) = row_store();
    let two = call(&store, "CALL algo.pca('P', ['a', 'b', 'c']) YIELD node, projection RETURN node, projection").unwrap();
    assert!(two.iter().all(|(_, p)| p.len() == 2));
    let one = call(&store, "CALL algo.pca('P', ['a', 'b', 'c'], 1) YIELD node, projection RETURN node, projection").unwrap();
    assert!(one.iter().all(|(_, p)| p.len() == 1));
    let expected = library_projection(1);
    assert!(one.iter().zip(&expected).all(|((_, p), e)| close(p, e)));
}

#[test]
fn bad_arguments_are_errors() {
    let (store, _) = row_store();
    for q in [
        "CALL algo.pca('P', [], 2) YIELD node, projection RETURN node",
        "CALL algo.pca('P', [1, 2], 2) YIELD node, projection RETURN node",
        "CALL algo.pca('P', ['a'], 0) YIELD node, projection RETURN node",
        "CALL algo.pca('P') YIELD node, projection RETURN node",
    ] {
        assert!(call(&store, q).is_err(), "`{q}` should be refused");
    }
}
