//! Does `MATCH (a:Account {id: N})` use an index once one exists?
//!
//! The FinBench bench never created indexes, so every anchor was a label scan
//! over millions of accounts. Adding the `CREATE INDEX` is only half the fix:
//! the planner has to lower the inline-property MATCH to an IndexScan, and a
//! benchmark cannot tell you whether it did -- it only tells you the query got
//! faster, which a warm cache also does.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;

fn main() {
    let mut s = GraphStore::new();
    for i in 0..50_000i64 {
        let n = s.create_node_with_labels([Label::new("Account")]);
        s.set_node_property("default", n, "id", PropertyValue::Integer(i)).unwrap();
    }

    let q = "MATCH (a:Account {id: 49999}) RETURN a.id";
    let explain = |s: &GraphStore| -> String {
        let parsed = parse_query(&format!("EXPLAIN {q}")).unwrap();
        QueryExecutor::new(s).execute(&parsed).ok()
            .and_then(|b| b.records.first().and_then(|r| r.get("plan")).map(|v| format!("{v:?}")))
            .unwrap_or_default()
    };
    let time = |s: &GraphStore| {
        let parsed = parse_query(q).unwrap();
        let t = std::time::Instant::now();
        let n = QueryExecutor::new(s).execute(&parsed).map(|b| b.records.len());
        (t.elapsed(), n)
    };

    let before_plan = explain(&s);
    let (before, _) = time(&s);
    println!("without an index: {before:?}");
    println!("  plan: {}", before_plan.lines().next().unwrap_or("").chars().take(90).collect::<String>());

    let stmt = parse_query("CREATE INDEX ON :Account(id)").unwrap();
    MutQueryExecutor::new(&mut s, "default".to_string()).execute(&stmt).expect("index created");

    let after_plan = explain(&s);
    let (after, rows) = time(&s);
    println!("with an index   : {after:?}  rows={rows:?}");
    println!("  plan: {}", after_plan.lines().next().unwrap_or("").chars().take(90).collect::<String>());
    println!("\nplan mentions IndexScan: {}", after_plan.contains("IndexScan"));
    println!("speedup: {:.1}x", before.as_secs_f64() / after.as_secs_f64().max(1e-9));
}
