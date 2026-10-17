//! A range over an indexed property reads only the range from the index (#1482).
//!
//! `MATCH (p:P) WHERE p.id > $k AND p.id < $k + 50` planned
//! `IndexScan(P.id > k)` under a `Filter(p.id < k + 50)`: the index handed
//! over every node above `k` -- half the label on average -- for the filter to
//! keep fifty. On CH-PERF-CONC that one query cost 68 ms of CPU, a quarter of
//! the read mix, and its cost alone is the ~780 qps ceiling the issue measured.
//! The scan now takes the opposite conjunct as its other end.
//!
//! The answers are pinned against the same queries over a store with no index,
//! across the cases a second bound could get wrong: an `Integer`/`Float` tie at
//! either end, a value of another type in the label, an inverted range, and a
//! bound written as arithmetic over a parameter.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `n` `:P` nodes with integer ids `1..=n`, plus the awkward ones: floats on
/// and between the integers, a string, and a node with no `id`.
fn store(n: i64, indexed: bool) -> GraphStore {
    let mut s = GraphStore::new();
    if indexed {
        let q = parse_query("CREATE INDEX ON :P(id)").unwrap();
        samyama::query::executor::MutQueryExecutor::new(&mut s, "default".into())
            .execute(&q)
            .unwrap();
    }
    let add = |s: &mut GraphStore, v: Option<PropertyValue>| {
        let id = s.create_node("P");
        if let Some(v) = v {
            s.set_node_property("default", id, "id".to_string(), v).unwrap();
        }
    };
    for i in 1..=n {
        add(&mut s, Some(PropertyValue::Integer(i)));
    }
    for f in [100.0, 120.5, 150.0, 149.999, 50.0] {
        add(&mut s, Some(PropertyValue::Float(f)));
    }
    add(&mut s, Some(PropertyValue::String("120".into())));
    add(&mut s, None);
    s
}

fn ids(store: &GraphStore, cypher: &str) -> Vec<String> {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let mut out: Vec<String> = batch.records.iter().map(|r| format!("{:?}", r.get("v"))).collect();
    out.sort();
    out
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

const CASES: &[&str] = &[
    "MATCH (p:P) WHERE p.id > 100 AND p.id < 150 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id >= 100 AND p.id <= 150 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id >= 100.0 AND p.id <= 150.0 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id > 100.0 AND p.id < 150.0 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id < 150 AND p.id > 100 RETURN p.id AS v",
    "MATCH (p:P) WHERE 150 >= p.id AND 100 <= p.id RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id > 100 AND p.id < 100 + 50 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id > 150 AND p.id < 100 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id > 150 AND p.id <= 150 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id >= 150 AND p.id <= 150 RETURN p.id AS v",
    "MATCH (p:P) WHERE p.id > 100 AND p.id < '2' RETURN p.id AS v",
];

#[test]
fn a_two_sided_range_answers_what_the_unindexed_store_answers() {
    let with = store(300, true);
    let without = store(300, false);
    for cypher in CASES {
        assert_eq!(ids(&with, cypher), ids(&without, cypher), "{cypher}\n{}", plan(&with, cypher));
    }
    // Spot-check the ties are really in there, so the comparison above is
    // not two empty answers agreeing.
    let both = ids(&with, CASES[1]);
    assert!(both.iter().any(|v| v.contains("Float(150.0)")), "{both:?}");
    assert!(both.iter().any(|v| v.contains("Float(100.0)")), "{both:?}");
}

#[test]
fn the_scan_reads_both_ends_from_the_index() {
    let s = store(300, true);
    let text = plan(&s, "MATCH (p:P) WHERE p.id > 100 AND p.id < 100 + 50 RETURN count(p)");
    assert!(text.contains("P.id > Integer(100), <= Float(150.0)"), "{text}");
    // Single-bound scans at a tie, which missed the other spelling before.
    for (q, want) in [("MATCH (p:P) WHERE p.id >= 100.0 RETURN count(p) AS v", "Integer(205)"), ("MATCH (p:P) WHERE p.id <= 150 RETURN count(p) AS v", "Integer(155)")] {
        assert_eq!(ids(&s, q), ids(&store(300, false), q), "{q}");
        assert!(ids(&s, q)[0].contains(want), "{q}: {:?}", ids(&s, q));
    }
    // A bound of another type is not a bound on this index.
    let text = plan(&s, "MATCH (p:P) WHERE p.id > 100 AND p.id < '2' RETURN count(p)");
    assert!(!text.contains(", <="), "{text}");
}

/// Pinned as a **ratio in one process**: the same fifty-wide range over a
/// label ten times larger. Read from both ends, the scan touches the same
/// fifty keys either way; read from one end, it touches half the label.
#[test]
fn a_narrow_range_does_not_cost_the_size_of_the_label() {
    use std::time::Instant;
    let small = store(20_000, true);
    let large = store(200_000, true);
    let cypher = "MATCH (p:P) WHERE p.id > 5000 AND p.id < 5050 RETURN count(p) AS v";
    assert_eq!(ids(&small, cypher), ids(&large, cypher));
    let best = |s: &GraphStore| {
        let q = parse_query(cypher).unwrap();
        let mut best = f64::MAX;
        for _ in 0..15 {
            let t = Instant::now();
            QueryExecutor::new(s).execute(&q).unwrap();
            best = best.min(t.elapsed().as_secs_f64());
        }
        best
    };
    let _ = (best(&small), best(&large));
    let (a, b) = (best(&small), best(&large));
    let ratio = b / a;
    eprintln!("range of 49: {a:.6}s over 20k nodes, {b:.6}s over 200k, {ratio:.2}x");
    assert!(
        ratio < 3.0,
        "a fifty-key range costs {ratio:.2}x more over a label ten times larger \
         ({a:.6}s against {b:.6}s): the scan is reading one end only (#1482)"
    );
}
