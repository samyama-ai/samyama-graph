//! `operator__batch` (#2219): with a tracer attached the executor wraps every
//! step of the plan to time it. Wrapping must not change what a query answers,
//! for reads (collected and streamed) or writes.

use samyama::graph::GraphStore;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn graph() -> GraphStore {
    let mut store = GraphStore::new();
    let q = parse_query(
        "UNWIND range(1, 50) AS i CREATE (:P {i: i})-[:R]->(:Q {i: i % 7})",
    )
    .unwrap();
    MutQueryExecutor::new(&mut store, "default".into()).execute(&q).unwrap();
    store
}

const READS: &[&str] = &[
    "MATCH (p:P)-[:R]->(q:Q) WHERE q.i > 2 RETURN p.i AS p, q.i AS q ORDER BY p",
    "MATCH (q:Q) RETURN q.i AS i, count(*) AS n ORDER BY i",
    "MATCH (p:P) RETURN p.i AS i ORDER BY i DESC LIMIT 5",
];

#[test]
fn a_traced_read_answers_what_an_untraced_one_does() {
    let store = graph();
    for q in READS {
        let query = parse_query(q).unwrap();
        let plain = QueryExecutor::new(&store).execute(&query).unwrap();
        let traced = QueryExecutor::new(&store).with_step_trace(true).execute(&query).unwrap();
        assert_eq!(format!("{:?}", traced.records), format!("{:?}", plain.records), "{q}");
        assert_eq!(traced.columns, plain.columns);
        assert!(traced.plan_hash.is_none(), "the response carries a hash only when asked: {q}");
        let mut streamed = Vec::new();
        QueryExecutor::new(&store)
            .with_step_trace(true)
            .execute_each(&query, 2, &mut |_, mut rows| {
                streamed.append(&mut rows);
                Ok(())
            })
            .unwrap();
        assert_eq!(format!("{streamed:?}"), format!("{:?}", plain.records), "streamed: {q}");
    }
}

#[test]
fn a_traced_write_writes_what_an_untraced_one_does() {
    let write = parse_query("MATCH (q:Q) WHERE q.i < 3 SET q.low = true RETURN count(q) AS n").unwrap();
    let mut a = graph();
    let mut b = graph();
    let plain = MutQueryExecutor::new(&mut a, "default".into()).execute(&write).unwrap();
    let traced = MutQueryExecutor::new(&mut b, "default".into()).with_step_trace(true).execute(&write).unwrap();
    assert_eq!(format!("{:?}", traced.records), format!("{:?}", plain.records));
    let check = parse_query("MATCH (q:Q {low: true}) RETURN count(q) AS n").unwrap();
    assert_eq!(
        format!("{:?}", QueryExecutor::new(&b).execute(&check).unwrap().records),
        format!("{:?}", QueryExecutor::new(&a).execute(&check).unwrap().records),
    );
}
