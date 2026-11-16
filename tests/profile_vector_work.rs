//! PROFILE says what a vector search did, not only how long it took (#1970).
//!
//! `VectorSearch` reports four counters: distances computed, HNSW searches
//! the widening loop ran, exact fallbacks, and evaluations of the pushed-down
//! filter. They appear as `work` in `PROFILE (FORMAT JSON)`, `null` for
//! operators that report none, and as `key=value` pairs on the operator's line
//! of the text profile.
//!
//! The counts are per thread: what a search on another thread does is not
//! added to this one's.
use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::work::Work;
use samyama::vector::{DistanceMetric, VectorIndex};
use std::collections::HashMap;

// Above the size at which the widening loop scans exactly from the start
// (width 64 x 2m 32 >= len), so an unfiltered search walks the graph.
const N: usize = 3_000;
const DIM: usize = 8;

fn points(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut x = seed;
    (0..n)
        .map(|_| {
            (0..DIM)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
                })
                .collect()
        })
        .collect()
}

/// Built through Cypher, the path a user takes: the index first, then the
/// nodes, each indexed as its CREATE writes it.
fn docs() -> GraphStore {
    let mut s = GraphStore::new();
    let run = |s: &mut GraphStore, q: &str| {
        let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
        MutQueryExecutor::new(s, "default".to_string())
            .execute(&query)
            .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    };
    run(
        &mut s,
        &format!("CREATE VECTOR INDEX doc_v FOR (d:Doc) ON (d.v) OPTIONS {{dimensions: {DIM}, similarity: 'euclidean'}}"),
    );
    for (i, v) in points(N, 0x9E37_79B9_7F4A_7C15).into_iter().enumerate() {
        run(
            &mut s,
            &format!("CREATE (:Doc {{g: {}, v: {}}})", i % 10, literal(&v)),
        );
    }
    assert_eq!(
        s.vector_index
            .get_index("Doc", "v")
            .expect("index")
            .read()
            .unwrap()
            .len(),
        N
    );
    s
}

fn literal(v: &[f32]) -> String {
    let parts: Vec<String> = v.iter().map(|x| format!("{x:?}")).collect();
    format!("[{}]", parts.join(", "))
}

fn plan(s: &GraphStore, q: &str) -> PropertyValue {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    match batch.records[0].get("plan") {
        Some(Value::Property(p)) => p.clone(),
        other => panic!("{q}: {other:?}"),
    }
}

fn field<'a>(v: &'a PropertyValue, key: &str) -> &'a PropertyValue {
    match v {
        PropertyValue::Map(m) => m.get(key).unwrap_or_else(|| panic!("no `{key}` in {v:?}")),
        other => panic!("not a map: {other:?}"),
    }
}

fn walk<'a>(node: &'a PropertyValue, out: &mut Vec<&'a PropertyValue>) {
    out.push(node);
    if let PropertyValue::Array(children) = field(node, "children") {
        for c in children {
            walk(c, out);
        }
    }
}

/// The `work` counters of the plan's VectorSearch, and that every other
/// operator reports `null`.
fn vector_work(json: &PropertyValue) -> HashMap<String, i64> {
    let mut nodes = Vec::new();
    walk(field(json, "root"), &mut nodes);
    let mut found = None;
    for node in nodes {
        let name = match field(node, "operator") {
            PropertyValue::String(s) => s.clone(),
            other => panic!("{other:?}"),
        };
        match (name.as_str(), field(node, "work")) {
            ("VectorSearch", PropertyValue::Map(m)) => {
                found = Some(
                    m.iter()
                        .map(|(k, v)| match v {
                            PropertyValue::Integer(i) => (k.clone(), *i),
                            other => panic!("{k}: {other:?}"),
                        })
                        .collect(),
                )
            }
            ("VectorSearch", other) => panic!("VectorSearch work: {other:?}"),
            (_, PropertyValue::Null) => {}
            (name, other) => panic!("{name} reported work {other:?}"),
        }
    }
    found.expect("a VectorSearch in the plan")
}

fn query_vector() -> String {
    literal(&points(1, 42)[0])
}

#[test]
fn an_unfiltered_search_reports_its_graph_walk() {
    let s = docs();
    let json = plan(
        &s,
        &format!(
            "PROFILE (FORMAT JSON) CALL db.index.vector.queryNodes('Doc', 'v', {}, 10) \
             YIELD node RETURN node",
            query_vector()
        ),
    );
    let work = vector_work(&json);
    assert!(work["distances"] > 0, "{work:?}");
    assert_eq!(work["graph_searches"], 1, "{work:?}");
    assert_eq!(work["exact_fallbacks"], 0, "{work:?}");
    assert_eq!(
        work["filter_checks"], 0,
        "no filter, nothing checked: {work:?}"
    );
}

#[test]
fn a_filtered_search_reports_its_filter_checks() {
    let s = docs();
    let json = plan(
        &s,
        &format!(
            "PROFILE (FORMAT JSON) CALL db.index.vector.queryNodes('Doc', 'v', {}, 10) \
             YIELD node WHERE node.g = 3 RETURN node",
            query_vector()
        ),
    );
    let work = vector_work(&json);
    assert!(work["distances"] > 0, "{work:?}");
    assert!(work["filter_checks"] >= 10, "{work:?}");
    assert_eq!(
        field(&json, "total_actual_rows"),
        &PropertyValue::Integer(10)
    );
}

#[test]
fn the_text_profile_carries_the_counters_on_the_vector_line_only() {
    let s = docs();
    let text = match plan(
        &s,
        &format!(
            "PROFILE CALL db.index.vector.queryNodes('Doc', 'v', {}, 10) YIELD node RETURN node",
            query_vector()
        ),
    ) {
        PropertyValue::String(t) => t,
        other => panic!("{other:?}"),
    };
    let section = text
        .split("Estimated vs actual rows")
        .nth(1)
        .expect("estimate section");
    let lines: Vec<&str> = section
        .lines()
        .filter(|l| l.contains("estimated="))
        .collect();
    assert!(!lines.is_empty(), "{text}");
    for line in lines {
        if line.trim_start().starts_with("VectorSearch") {
            for key in [
                "distances=",
                "graph_searches=",
                "exact_fallbacks=",
                "filter_checks=",
            ] {
                assert!(line.contains(key), "{key} missing: {line}");
            }
        } else {
            assert!(!line.contains("distances="), "{line}");
        }
    }
}

#[test]
fn an_exact_scan_counts_one_distance_per_entry_on_this_thread_only() {
    let data = points(500, 7);
    let mut index = VectorIndex::new(DIM, DistanceMetric::L2);
    for (i, v) in data.iter().enumerate() {
        index.add(NodeId::new(i as u64), v).unwrap();
    }
    let before = Work::now();
    index.search_exact_where(&data[3], 5, &|_| true).unwrap();
    let did = Work::now().since(before);
    assert!(did.distances >= 500, "{did:?}");

    // A search on another thread is not this thread's work.
    let index = std::sync::Arc::new(index);
    let before = Work::now();
    let other = std::sync::Arc::clone(&index);
    let q = data[4].clone();
    std::thread::spawn(move || other.search_exact_where(&q, 5, &|_| true).unwrap())
        .join()
        .unwrap();
    assert_eq!(Work::now().since(before), Work::default());
}
