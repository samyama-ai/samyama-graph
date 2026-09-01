//! Do the LDBC `name`/`firstName` anchors lower to an IndexScan?
//!
//! IC1 anchors on `(friend:Person {firstName: "..."})` inline; IC11 and IC3
//! filter with `WHERE org.name = "..."`. Those are two different planner
//! paths -- inline properties and deferred predicates -- and the shortestPath
//! work showed they are not equally well served. Adding an index is only half
//! a fix; the plan has to change. A query getting faster is also what a warm
//! cache looks like, so this reads the plan first.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn fixture(indexed: bool) -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..40_000i64 {
        let p = s.create_node_with_labels([Label::new("Person")]);
        s.set_node_property("default", p, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", p, "firstName",
            PropertyValue::String(format!("name{}", i % 500))).unwrap();
        let o = s.create_node_with_labels([Label::new("Organisation")]);
        s.set_node_property("default", o, "name",
            PropertyValue::String(format!("org{}", i % 500))).unwrap();
    }
    if indexed {
        for stmt in ["CREATE INDEX ON :Person(firstName)", "CREATE INDEX ON :Organisation(name)"] {
            let q = parse_query(stmt).unwrap();
            MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();
        }
    }
    s
}

fn probe(label: &str, q: &str) {
    let (plain, idx) = (fixture(false), fixture(true));
    let plan = |s: &GraphStore| -> String {
        parse_query(&format!("EXPLAIN {q}")).ok()
            .and_then(|p| QueryExecutor::new(s).execute(&p).ok())
            .and_then(|b| b.records.first().and_then(|r| r.get("plan")).map(|v| format!("{v:?}")))
            .unwrap_or_default()
    };
    let run = |s: &GraphStore| {
        let p = parse_query(q).unwrap();
        let t = std::time::Instant::now();
        let n = QueryExecutor::new(s).execute(&p).map(|b| b.records.len()).unwrap_or(0);
        (t.elapsed(), n)
    };
    let (t0, n0) = run(&plain);
    let (t1, n1) = run(&idx);
    let ops = |pl: &str| ["IndexScan", "NodeScan", "Filter"].iter()
        .filter(|o| pl.contains(*o)).cloned().collect::<Vec<_>>().join("+");
    println!("{label}");
    println!("   without index: {t0:>10?}  rows={n0}  [{}]", ops(&plan(&plain)));
    println!("   with index   : {t1:>10?}  rows={n1}  [{}]", ops(&plan(&idx)));
    println!("   same answer: {}   speedup: {:.0}x",
        n0 == n1, t0.as_secs_f64() / t1.as_secs_f64().max(1e-9));
}

fn main() {
    probe("IC1 shape — inline (p:Person {firstName: ...})",
          "MATCH (p:Person {firstName: \"name7\"}) RETURN count(p) AS n");
    probe("IC11 shape — WHERE org.name = ...",
          "MATCH (o:Organisation) WHERE o.name = \"org7\" RETURN count(o) AS n");
}
