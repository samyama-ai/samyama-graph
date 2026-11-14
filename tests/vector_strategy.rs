use samyama::vector::strategy::{FilterPlan, FilterStrategy};
#[test]
fn selective_predicates_prefer_exact_while_dense_large_indexes_widen() {
    let rare = FilterPlan::estimate(1_000_000, 128, 10, 64, 16, 0.001);
    let dense = FilterPlan::estimate(1_000_000, 128, 10, 64, 16, 1.0);
    assert_eq!(rare.strategy, FilterStrategy::Exact);
    assert_eq!(dense.strategy, FilterStrategy::Widen);
    assert!(rare.describe().contains("selectivity=0.001000"));
}
#[test]
fn tiny_and_empty_indexes_use_exact_without_nonfinite_costs() {
    for n in [0, 10] {
        for s in [0.0, 1.0, f64::NAN] {
            let p = FilterPlan::estimate(n, 4, 10, 64, 16, s);
            assert_eq!(p.strategy, FilterStrategy::Exact);
            assert!(p.exact_work.is_finite() && p.widen_work.is_finite());
        }
    }
}
#[test]
fn planned_exact_scan_preserves_labels_and_filters() {
    use samyama::{
        graph::GraphStore,
        query::{
            executor::{MutQueryExecutor, QueryExecutor},
            parser::parse_query,
        },
    };
    let mut s = GraphStore::new();
    for text in [
        "CREATE (:V {cat: 1, v: [0.0, 0.0]}), (:V {cat: 2, v: [1.0, 0.0]})",
        "CREATE VECTOR INDEX vx FOR (n:V) ON (n.v) OPTIONS {dimensions: 2, similarity: 'l2'}",
    ] {
        MutQueryExecutor::new(&mut s, "default".into())
            .execute(&parse_query(text).unwrap())
            .unwrap();
    }
    let q = "CALL db.index.vector.queryNodes('vx', 10, [0.0,0.0]) YIELD node WHERE node.cat = 2 RETURN node.cat AS cat";
    let result = QueryExecutor::new(&s)
        .execute(&parse_query(q).unwrap())
        .unwrap();
    assert_eq!(result.records.len(), 1);
    let plan = QueryExecutor::new(&s)
        .execute(&parse_query(&format!("EXPLAIN {q}")).unwrap())
        .unwrap();
    let printed = format!("{plan:?}");
    assert!(
        printed.contains("pre-exact") && printed.contains("selectivity="),
        "{printed}"
    );
}
