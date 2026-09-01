//! Which operators does `shortestPath` between two indexed anchors plan?
//!
//! Bidirectional BFS did not move CR-3 at all (541 ms -> 547 ms on SF10), so
//! the BFS was not the cost. This asks what the plan actually is.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn main() {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..40_000i64 {
        let n = s.create_node_with_labels([Label::new("Account")]);
        s.set_node_property("default", n, "id", PropertyValue::Integer(i)).unwrap();
        ids.push(n);
    }
    for i in 0..(ids.len() - 1) {
        let _ = s.create_edge(ids[i], ids[i + 1], "TRANSFER");
    }
    let idx = parse_query("CREATE INDEX ON :Account(id)").unwrap();
    MutQueryExecutor::new(&mut s, "default".to_string()).execute(&idx).unwrap();

    for (label, q) in [
        ("inline properties (what CR-3 writes)",
         "MATCH p = shortestPath((a:Account {id: 5})-[:TRANSFER*]-(b:Account {id: 40})) RETURN length(p) AS l"),
        ("WHERE predicates",
         "MATCH (a:Account), (b:Account) WHERE a.id = 5 AND b.id = 40 \
          MATCH p = shortestPath((a)-[:TRANSFER*]-(b)) RETURN length(p) AS l"),
    ] {
        let plan = parse_query(&format!("EXPLAIN {q}")).ok()
            .and_then(|p| QueryExecutor::new(&s).execute(&p).ok())
            .and_then(|b| b.records.first().and_then(|r| r.get("plan")).map(|v| format!("{v:?}")))
            .unwrap_or_default();
        let ops: Vec<&str> = ["IndexScan", "NodeScan", "NodeById", "Filter", "CartesianProduct", "ShortestPath"]
            .into_iter().filter(|o| plan.contains(o)).collect();
        let t = std::time::Instant::now();
        let rows = parse_query(q).ok()
            .and_then(|p| QueryExecutor::new(&s).execute(&p).ok())
            .map(|b| b.records.len());
        println!("{label:38} {:>9?}  rows={rows:?}", t.elapsed());
        println!("    operators: {ops:?}");
    }
}
