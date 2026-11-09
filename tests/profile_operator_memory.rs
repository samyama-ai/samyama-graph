//! PROFILE reports what each buffering operator held, and its machine-read
//! section keeps every operator's details whole (#2078).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `n` `:P` nodes with a 40-character `name` and a `city` cycling a/b/c/d.
fn fixture(n: i64) -> GraphStore {
    let mut s = GraphStore::new();
    for i in 0..n {
        let id = s.create_node("P");
        let name = format!("{:0>40}", i);
        s.set_node_property("default", id, "name", PropertyValue::String(name))
            .unwrap();
        let city = ["a", "b", "c", "d"][(i % 4) as usize];
        s.set_node_property("default", id, "city", PropertyValue::String(city.into()))
            .unwrap();
    }
    s
}

fn profile(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(cypher).expect("parse");
    match QueryExecutor::new(store).execute(&query).unwrap().records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

/// The machine-read lines, one per operator.
fn operator_lines(profile: &str) -> Vec<&str> {
    profile
        .split("Estimated vs actual rows")
        .nth(1)
        .unwrap_or_else(|| panic!("no per-operator section:\n{profile}"))
        .lines()
        .filter(|l| l.contains(" actual="))
        .collect()
}

fn memory(line: &str) -> Option<u64> {
    let v = line.split(" memory=").nth(1)?.split_whitespace().next()?;
    v.strip_suffix('B')?.parse().ok()
}

#[test]
fn buffering_operators_report_memory_and_streaming_ones_do_not() {
    let small = fixture(50);
    let large = fixture(500);
    let q = "PROFILE MATCH (p:P) RETURN p.name AS name ORDER BY name";
    let (small_p, large_p) = (profile(&small, q), profile(&large, q));
    let sort = |p: &str| {
        let line = operator_lines(p)
            .into_iter()
            .find(|l| l.trim_start().starts_with("Sort"))
            .unwrap_or_else(|| panic!("no Sort line:\n{p}"))
            .to_string();
        memory(&line).unwrap_or_else(|| panic!("Sort reported no memory: {line}"))
    };
    let (s, l) = (sort(&small_p), sort(&large_p));
    assert!(s > 0, "{small_p}");
    // Ten times the rows held, so several times the bytes: the number moves
    // with what the operator buffers rather than being a constant.
    assert!(l > 5 * s, "50 rows -> {s} B, 500 rows -> {l} B");

    // A scan streams: it reports no memory rather than zero.
    let scan = operator_lines(&large_p)
        .into_iter()
        .find(|l| l.contains("Scan"))
        .unwrap_or_else(|| panic!("no scan line:\n{large_p}"));
    assert!(scan.contains(" memory=-"), "{scan}");

    let agg = profile(&large, "PROFILE MATCH (p:P) RETURN p.city AS c, count(*) AS n");
    let agg_line = operator_lines(&agg)
        .into_iter()
        .find(|l| l.contains("Aggregate"))
        .unwrap_or_else(|| panic!("no Aggregate line:\n{agg}"));
    assert!(memory(agg_line).is_some_and(|b| b > 0), "{agg_line}");
}

#[test]
fn the_per_operator_section_keeps_details_whole_and_is_always_printed() {
    let store = fixture(20);
    let long = "0000000000000000000000000000000000000007";
    let p = profile(
        &store,
        &format!("PROFILE MATCH (p:P) WHERE p.name = '{long}' AND p.city = 'd' RETURN p.city"),
    );
    // The table above still truncates, for people; this section must not.
    assert!(
        operator_lines(&p).iter().any(|l| l.contains(long)),
        "the 40-character literal was cut from the per-operator section:\n{p}"
    );
    // Every operator has a line, whether or not it has an estimate.
    let table_rows = p
        .split("--- Profile (per operator) ---")
        .nth(1)
        .unwrap()
        .split("\n\n")
        .next()
        .unwrap()
        .lines()
        .skip(2)
        .filter(|l| !l.trim().is_empty())
        .count();
    assert_eq!(operator_lines(&p).len(), table_rows, "{p}");
}
