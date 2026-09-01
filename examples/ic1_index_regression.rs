//! Did indexing `Person.firstName` make IC1 slower?
//!
//! CH-REGRESS reported IC1 at 1.2x (+6ms), confirmed at 1.25x over two runs on
//! vm-1, in the same window that #1053 added a `Person.firstName` index and
//! #1051 made IC13/IC6 dramatically faster. #1054 showed the index cannot help
//! IC1 -- the plan filters the name after the expansion -- which leaves the
//! possibility that it cost something instead.
//!
//! One store, measured before and after `CREATE INDEX`, so the graph, the
//! parameters and the process are identical and the index is the only
//! difference. Built at SF1's shape (~11K Person) because that is where
//! CH-REGRESS runs; the SF10 run saw IC1 move 710ms -> 704ms, so whatever this
//! is, it is scale-dependent.
use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor};
use samyama::query::parser::parse_query;
use std::time::{Duration, Instant};

const IC1: &str = "MATCH (p:Person {id: 0})-[:KNOWS*1..3]-(f:Person {firstName: \"Chen\"}) \
                   RETURN f.id AS id";

fn exec(s: &GraphStore, q: &str) -> (usize, Duration) {
    let p = parse_query(q).expect("parse");
    let t = Instant::now();
    let b = QueryExecutor::new(s).execute(&p).expect("execute");
    (b.records.len(), t.elapsed())
}

fn plan(s: &GraphStore, q: &str) -> String {
    let p = parse_query(&format!("EXPLAIN {q}")).expect("parse");
    let b = QueryExecutor::new(s).execute(&p).expect("execute");
    b.records.first().and_then(|r| r.get("plan")).map(|v| format!("{v:?}"))
        .unwrap_or_default().replace("\\n", "\n")
}

fn anchor(plan: &str) -> String {
    plan.lines().rev().find(|l| l.contains("Scan"))
        .map(|l| l.trim().trim_matches(|c| c == '"' || c == ' ').to_string())
        .unwrap_or_else(|| "<no scan>".into())
}

/// Median of `n` runs. A single timing cannot separate a 1.25x plan change
/// from ordinary variance at this size.
fn median(s: &GraphStore, q: &str, n: usize) -> (usize, Duration) {
    let mut rows = 0;
    let mut ds: Vec<Duration> = Vec::new();
    for _ in 0..n {
        let (r, d) = exec(s, q);
        rows = r;
        ds.push(d);
    }
    ds.sort();
    (rows, ds[ds.len() / 2])
}

fn main() {
    // SF1 shape: ~11K Person, KNOWS degree ~20, LDBC-like first names.
    const PERSONS: i64 = 11_000;
    const DEGREE: usize = 20;
    const NAMES: [&str; 12] = ["Chen", "Wei", "Ali", "Maria", "John", "Fatima",
                               "Yuki", "Ana", "Ivan", "Sara", "Omar", "Li"];
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..PERSONS {
        let p = s.create_node_with_labels([Label::new("Person")]);
        s.set_node_property("default", p, "id", PropertyValue::Integer(i)).unwrap();
        s.set_node_property("default", p, "firstName",
            PropertyValue::String(NAMES[(i as usize * 7) % NAMES.len()].into())).unwrap();
        ids.push(p);
    }
    for i in 0..ids.len() {
        for d in 1..=DEGREE {
            let _ = s.create_edge(ids[i], ids[(i * 13 + d * 397) % ids.len()], "KNOWS");
        }
    }
    // `Person.id` is indexed in both arms: it predates #1053 and is what IC1
    // anchors on. Only `firstName` differs.
    let q = parse_query("CREATE INDEX ON :Person(id)").unwrap();
    MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();

    println!("{PERSONS} Person, {} KNOWS (degree {DEGREE}), {} distinct first names\n",
             PERSONS as usize * DEGREE, NAMES.len());

    let before_plan = plan(&s, IC1);
    let (rows_before, t_before) = median(&s, IC1, 9);
    println!("without Person.firstName index");
    println!("   anchor : {}", anchor(&before_plan));
    println!("   median : {t_before:?}   rows={rows_before}");

    let q = parse_query("CREATE INDEX ON :Person(firstName)").unwrap();
    MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();

    let after_plan = plan(&s, IC1);
    let (rows_after, t_after) = median(&s, IC1, 9);
    println!("\nwith Person.firstName index");
    println!("   anchor : {}", anchor(&after_plan));
    println!("   median : {t_after:?}   rows={rows_after}");

    // A plan change that returns different rows is a wrong answer, not a
    // performance question, and must be reported as the more serious finding.
    assert_eq!(rows_before, rows_after,
        "the index changed the ANSWER: {rows_before} rows -> {rows_after}");

    let ratio = t_after.as_secs_f64() / t_before.as_secs_f64();
    println!("\nratio with/without: {ratio:.2}x  (CH-REGRESS saw IC1 at 1.25x)");
    println!("plan changed: {}", before_plan != after_plan);
    if before_plan != after_plan {
        println!("\n--- without ---\n{}\n--- with ---\n{}",
                 before_plan.trim_matches('"'), after_plan.trim_matches('"'));
    }
}
