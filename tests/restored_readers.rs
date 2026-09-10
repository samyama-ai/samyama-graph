//! Readers that consulted only the row copy answer the same on a restored graph
//! as on the graph it was restored from (#1187).
//!
//! Snapshot import leaves the row `Node.properties` empty for every scalar; the
//! values are in the column store. Each test runs the same statement on a store
//! built with CREATE and on that store after export -> import, and asserts the
//! two agree. Differential, so no test encodes an expectation of its own.

use samyama::graph::GraphStore;
use samyama::query::QueryEngine;

fn restore(src: &GraphStore) -> GraphStore {
    let mut buf = Vec::new();
    samyama::snapshot::export_tenant(src, &mut buf).expect("export");
    let mut dst = GraphStore::new();
    samyama::snapshot::import_tenant(&mut dst, &buf[..]).expect("import");
    dst
}

fn built(setup: &str) -> GraphStore {
    let mut s = GraphStore::new();
    QueryEngine::new().execute_mut(setup, &mut s, "default").expect(setup);
    s
}

fn count(store: &GraphStore, q: &str) -> usize {
    QueryEngine::new().execute(q, store).expect(q).records.len()
}

const GRAPH: &str = r#"CREATE (a:S {id: 1})-[:R]->(m:M {id: 2})-[:R]->(t:T {k: 1, name: "tgt"}),
                              (a)-[:R]->(u:T {k: 2, name: "other"}),
                              (p1:Person {id: 1})-[:WORK_AT]->(w:Company {name: "Wanted"}),
                              (p2:Person {id: 2})-[:WORK_AT]->(o:Company {name: "Other"})"#;

fn agree(q: &str) {
    let b = built(GRAPH);
    let r = restore(&b);
    let (nb, nr) = (count(&b, q), count(&r, q));
    assert!(nb > 0, "control: the query should match on the built store: {q}");
    assert_eq!(nr, nb, "restored store answered {nr} rows where its source answered {nb}: {q}");
}

#[test]
fn a_var_length_walk_to_a_pinned_target_agrees_after_restore() {
    agree(r#"MATCH (a:S {id: 1})-[:R*1..2]->(b:T {k: 1}) RETURN b.name"#);
}

#[test]
fn a_small_label_target_equality_agrees_after_restore() {
    agree(r#"MATCH (p:Person)-[:WORK_AT]->(c:Company) WHERE c.name = "Wanted" RETURN c.name"#);
}

#[test]
fn a_single_hop_inline_target_agrees_after_restore() {
    agree(r#"MATCH (a:S {id: 1})-[:R]->(b:T {k: 2}) RETURN b.name"#);
}

/// A unique constraint created on a restored graph must see the values already
/// there. Its backfill read the row, found nothing, and the first duplicate of
/// any pre-existing value went through.
#[test]
fn a_unique_constraint_on_a_restored_graph_refuses_a_duplicate() {
    let engine = QueryEngine::new();
    for (name, mut s) in [("built", built(r#"CREATE (:K {id: 7})"#)),
                          ("restored", restore(&built(r#"CREATE (:K {id: 7})"#)))] {
        engine.execute_mut("CREATE CONSTRAINT FOR (n:K) REQUIRE n.id IS UNIQUE", &mut s, "default")
            .expect("constraint");
        assert!(
            engine.execute_mut(r#"CREATE (:K {id: 7})"#, &mut s, "default").is_err(),
            "{name}: a duplicate of an existing value was accepted under a unique constraint"
        );
    }
}

/// Creating a unique constraint over values that already repeat must fail. Its
/// duplicate check read the row, so on a restored graph it saw no values and
/// created the constraint over data that already violated it.
#[test]
fn a_unique_constraint_over_existing_duplicates_is_refused_after_restore() {
    let engine = QueryEngine::new();
    for (name, mut s) in [("built", built(r#"CREATE (:K {id: 7}), (:K {id: 7})"#)),
                          ("restored", restore(&built(r#"CREATE (:K {id: 7}), (:K {id: 7})"#)))] {
        assert!(
            engine.execute_mut("CREATE CONSTRAINT FOR (n:K) REQUIRE n.id IS UNIQUE", &mut s, "default")
                .is_err(),
            "{name}: a unique constraint was created over two nodes that already share id 7"
        );
    }
}

/// `or.solve` reads each node's cost. It read the row, so on a restored graph
/// every cost was missing and silently defaulted to 1.0: the optimizer solved a
/// different problem and returned a confident, wrong fitness.
#[test]
fn or_solve_reads_costs_the_same_after_restore() {
    use samyama::graph::PropertyValue;
    use samyama::query::executor::MutQueryExecutor;
    use samyama::query::parser::parse_query;

    let mut src = GraphStore::new();
    for _ in 0..10 {
        let n = src.create_node("Resource");
        src.set_node_property("default", n, "cost", PropertyValue::Float(10.0)).unwrap();
        src.set_node_property("default", n, "allocation", PropertyValue::Float(0.0)).unwrap();
    }
    let mut restored = restore(&src);

    let q = parse_query(r#"
        CALL algo.or.solve({
            algorithm: 'GWO', label: 'Resource', property: 'allocation',
            min: 5.0, max: 100.0, cost_property: 'cost',
            population_size: 20, max_iterations: 50
        }) YIELD fitness, algorithm
    "#).unwrap();
    let fitness = |store: &mut GraphStore| -> f64 {
        let r = MutQueryExecutor::new(store, "default".to_string()).execute(&q).unwrap();
        r.records[0].get("fitness").unwrap().as_property().unwrap().as_float().unwrap()
    };
    let (fb, fr) = (fitness(&mut src), fitness(&mut restored));
    // 10 nodes x allocation 5.0 x cost 10.0 = 500 at the optimum.
    assert!((500.0..505.0).contains(&fb), "control: built-store fitness {fb}");
    assert!(
        (fr - fb).abs() < 10.0,
        "restored fitness {fr} against built {fb}: the costs were not read"
    );
}
