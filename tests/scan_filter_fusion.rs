//! A filter directly on a node scan is fused into it (#1615), and PROFILE sets
//! an estimate beside each operator's actual rows (#1624).

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::budget::WorkMeter;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `n` `:P` nodes: `v` = i, `city` cycling a/b/c/d, and every fifth `v`
/// missing from `w` so three-valued logic has nulls to work on. Each node
/// points at the next with `:K`.
fn fixture(n: i64) -> GraphStore {
    let mut s = GraphStore::new();
    let mut prev = None;
    for i in 0..n {
        let id = s.create_node("P");
        s.set_node_property("default", id, "v", PropertyValue::Integer(i))
            .unwrap();
        let city = ["a", "b", "c", "d"][(i % 4) as usize];
        s.set_node_property("default", id, "city", PropertyValue::String(city.into()))
            .unwrap();
        if i % 5 != 0 {
            s.set_node_property("default", id, "w", PropertyValue::Integer(i % 7))
                .unwrap();
        }
        if let Some(p) = prev {
            s.create_edge(p, id, "K").unwrap();
        }
        prev = Some(id);
    }
    s
}

fn text(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(cypher).expect("parse");
    match QueryExecutor::new(store).execute(&query).unwrap().records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    }
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    text(store, &format!("EXPLAIN {cypher}"))
        .lines()
        .take_while(|l| !l.starts_with("---"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Every row, rendered, in the order returned.
fn rows(store: &GraphStore, cypher: &str) -> Vec<String> {
    let query = parse_query(cypher).expect("parse");
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{cypher}: {e}"))
        .records
        .iter()
        .map(|r| format!("{r:?}"))
        .collect()
}

/// Rows produced across every operator, as the catalog's work meter counts.
fn work(store: &GraphStore, cypher: &str) -> u64 {
    let meter = WorkMeter::new(None);
    QueryExecutor::new(store)
        .with_work_meter(std::sync::Arc::clone(&meter))
        .execute(&parse_query(cypher).unwrap())
        .unwrap();
    meter.produced()
}

#[test]
fn a_filter_on_a_scan_is_one_operator() {
    let store = fixture(40);
    let p = plan(&store, "MATCH (p:P) WHERE p.city = 'a' RETURN p");
    let operators: Vec<&str> = p
        .lines()
        .map(|l| l.trim_start().trim_start_matches("+- ").trim_start())
        .collect();
    assert!(
        !operators.iter().any(|l| l.starts_with("Filter (")),
        "a separate Filter is left over the scan:\n{p}"
    );
    assert!(
        operators
            .iter()
            .any(|l| l.starts_with("FilteredNodeScan") && l.contains("city")),
        "{p}"
    );
}

/// Fused and unfused answer alike, row for row and in the same order, over
/// the shapes a filter has to get right: comparisons, nulls, OR and NOT,
/// a LIMIT, an ORDER BY on the filtered property (which asks the filter to
/// keep what it reads, #593), and the filter below an expand and an aggregate.
///
/// One test, because it switches fusion off through the environment and the
/// tests of one binary share it.
#[test]
fn fused_and_unfused_plans_answer_the_same() {
    let store = fixture(300);
    let queries = [
        "MATCH (p:P) WHERE p.city = 'a' RETURN p.v",
        "MATCH (p:P) WHERE p.v > 250 RETURN p.v",
        "MATCH (p:P) WHERE p.w = 3 RETURN p.v",
        "MATCH (p:P) WHERE p.w IS NULL RETURN p.v",
        "MATCH (p:P) WHERE NOT p.w = 3 RETURN p.v",
        "MATCH (p:P) WHERE p.w = 3 OR p.city = 'b' RETURN p.v",
        "MATCH (p:P) WHERE p.v % 2 = 0 RETURN p.v LIMIT 7",
        "MATCH (p:P) WHERE p.v % 2 = 0 RETURN p.v SKIP 3 LIMIT 4",
        "MATCH (p:P) WHERE p.v < 40 RETURN p.v ORDER BY p.v DESC",
        "MATCH (p:P) WHERE p.v < 40 RETURN p.v ORDER BY p.v DESC LIMIT 5",
        "MATCH (p:P) WHERE p.city = 'c' RETURN p.city, count(*) AS c",
        "MATCH (p:P)-[:K]->(q) WHERE p.v > 100 AND q.w = 1 RETURN p.v, q.v",
        "MATCH (p:P) WHERE p.nope = 1 RETURN p.v",
        "MATCH (p) WHERE p.v = 17 RETURN p.v",
    ];
    let fused: Vec<Vec<String>> = queries.iter().map(|q| rows(&store, q)).collect();
    let fused_work: Vec<u64> = queries.iter().map(|q| work(&store, q)).collect();
    std::env::set_var("SAMYAMA_SCAN_FILTER_FUSION", "off");
    let unfused: Vec<Vec<String>> = queries.iter().map(|q| rows(&store, q)).collect();
    let unfused_work: Vec<u64> = queries.iter().map(|q| work(&store, q)).collect();
    let plain = plan(&store, "MATCH (p:P) WHERE p.city = 'a' RETURN p");
    std::env::remove_var("SAMYAMA_SCAN_FILTER_FUSION");

    assert!(
        plain.contains("Filter"),
        "the switch did not turn fusion off:\n{plain}"
    );
    for ((q, f), u) in queries.iter().zip(&fused).zip(&unfused) {
        assert_eq!(f, u, "{q}");
    }
    // The catalog's work ceiling (#1156) counts the rows every operator
    // produces. A fused scan still tests every node and must say so, or a
    // template's recorded work -- and its ceiling with it -- would shrink.
    // Equal, except under a LIMIT: there the fused scan stops testing nodes
    // once it has enough, where the unfused one had already produced a few
    // more for the filter to drop -- less work, not less reported.
    for ((q, f), u) in queries.iter().zip(&fused_work).zip(&unfused_work) {
        if q.contains("LIMIT") {
            assert!(f <= u && *f > 0, "work for {q}: fused {f}, unfused {u}");
        } else {
            assert_eq!(f, u, "work for {q}");
        }
    }
    assert_eq!(fused[6].len(), 7);
    assert!(fused[2].len() > 1 && fused[3].len() == 60, "{:?}", fused[3]);
}

#[test]
fn a_write_under_a_fused_scan_still_writes() {
    let mut store = fixture(30);
    let q = parse_query("MATCH (p:P) WHERE p.v % 3 = 0 SET p.hit = true").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&q)
        .unwrap();
    assert_eq!(
        rows(&store, "MATCH (p:P) WHERE p.hit = true RETURN p.v").len(),
        10
    );
}

/// The operator lines PROFILE prints for a plan viewer: each must carry both
/// numbers, read the way the viewer reads them.
fn estimate_lines(profile: &str) -> Vec<(u64, u64, String)> {
    let number = |line: &str, key: &str| -> Option<u64> {
        let rest = &line[line.find(key)? + key.len()..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        digits.parse().ok()
    };
    profile
        .lines()
        .filter_map(|l| {
            Some((
                number(l, "estimated=")?,
                number(l, "actual=")?,
                l.to_string(),
            ))
        })
        .collect()
}

#[test]
fn profile_sets_an_estimate_beside_every_operator_it_can_model() {
    let store = fixture(400);
    let profile = text(
        &store,
        "PROFILE MATCH (p:P)-[:K]->(q:P) WHERE p.city = 'a' RETURN q.v ORDER BY q.v LIMIT 10",
    );
    let lines = estimate_lines(&profile);
    assert!(
        lines.len() >= 4,
        "operators with both numbers: {}\n{profile}",
        lines.len()
    );

    // The scan's estimate is the label count times the guessed selectivity,
    // and its actual count is what passed: 400 x 0.1 against 100.
    let scan = lines
        .iter()
        .find(|(_, _, l)| l.contains("FilteredNodeScan"))
        .unwrap_or_else(|| panic!("{profile}"));
    assert_eq!((scan.0, scan.1), (40, 100), "{}", scan.2);
    assert!(
        scan.2.contains("q-error=2.50") && scan.2.trim_end().ends_with('!'),
        "{}",
        scan.2
    );

    let limit = lines
        .iter()
        .find(|(_, _, l)| l.trim_start().starts_with("Limit"))
        .unwrap();
    assert_eq!((limit.0, limit.1), (10, 10), "{}", limit.2);
    assert!(
        !limit.2.trim_end().ends_with('!'),
        "an exact estimate is not flagged: {}",
        limit.2
    );
    assert!(profile.contains("time="), "{profile}");
}

#[test]
fn an_operator_without_a_model_says_so() {
    let store = fixture(10);
    let profile = text(&store, "PROFILE UNWIND range(1, 5) AS i RETURN i");
    let line = profile
        .lines()
        .find(|l| l.contains("Unwind") && l.contains("actual="))
        .unwrap_or_else(|| panic!("{profile}"));
    assert!(line.contains("estimated=-"), "{line}");
}

#[test]
fn an_index_scan_estimates_from_the_property_statistics() {
    let mut store = fixture(200);
    let idx = parse_query("CREATE INDEX ON :P(city)").unwrap();
    MutQueryExecutor::new(&mut store, "default".to_string())
        .execute(&idx)
        .unwrap();
    let profile = text(&store, "PROFILE MATCH (p:P) WHERE p.city = 'b' RETURN p.v");
    let scan = estimate_lines(&profile)
        .into_iter()
        .find(|(_, _, l)| l.contains("IndexScan"))
        .unwrap_or_else(|| panic!("{profile}"));
    // Four cities, evenly spread: the statistics say a quarter.
    assert_eq!(scan.1, 50, "{}", scan.2);
    assert!((45..=55).contains(&scan.0), "{}", scan.2);
}
