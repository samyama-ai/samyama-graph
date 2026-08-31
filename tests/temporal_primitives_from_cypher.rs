//! The four causal/temporal primitives, called from Cypher (ALGO-15).
//!
//! ```cypher
//! CALL algo.temporalReachability(id, {timeProperty: 'at'}) YIELD node, time
//! CALL algo.propagationRanking(id, {...})                  YIELD node, time, rank
//! CALL algo.temporalShortestPath(src, dst, {...})          YIELD path, times, arrival
//! CALL algo.symptomExplanation([[id, seenAt], ...], {...}) YIELD node, explains, onset
//! ```
//!
//! The property that makes these worth having, and the one every test below
//! turns on: **reachability in a temporal graph is not transitive.** If
//! `api → db` fires at 10 and `db → cache` fired at 5, then `api` cannot reach
//! `cache` through `db` — the second call already happened when we arrive. A
//! plain BFS says it can, and an RCA built on a plain BFS blames a service
//! that failed before its supposed cause did.

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `api → db` at 10, `db → cache` at **5**, `api → auth` at 12, `auth → ui` at 20.
/// `cache` is the node only a time-blind traversal would reach.
fn incident() -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for name in ["api", "db", "cache", "auth", "ui"] {
        let n = s.create_node_with_labels([Label::new("Svc")]);
        s.set_node_property("default", n, "name", PropertyValue::String(name.into())).unwrap();
        ids.push(n);
    }
    for (a, b, t) in [(0, 1, 10i64), (1, 2, 5), (0, 3, 12), (3, 4, 20)] {
        let e = s.create_edge(ids[a], ids[b], "CALLS").unwrap();
        s.set_edge_property(e, "at", PropertyValue::Integer(t)).unwrap();
    }
    (s, ids)
}

const CFG: &str = "{timeProperty: 'at', startTime: 0}";

fn rows(store: &GraphStore, cypher: &str) -> Vec<Vec<String>> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let cols = r.columns.clone();
    r.records.iter()
        .map(|rec| cols.iter().map(|c| format!("{:?}", rec.get(c))).collect())
        .collect()
}

#[test]
fn reachability_stops_at_an_edge_that_already_fired() {
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.temporalReachability({}, {CFG}) YIELD node, time RETURN node.name, time",
        ids[0].as_u64()));
    let names: Vec<&str> = got.iter().map(|r| r[0].as_str()).collect();
    assert_eq!(got.len(), 3, "{got:?}");
    assert!(names.iter().all(|n| !n.contains("cache")),
            "cache is only reachable if the traversal ignores time: {got:?}");
    assert!(got[0][0].contains("db") && got[2][0].contains("ui"), "{got:?}");
}

#[test]
fn propagation_is_ranked_in_the_order_it_would_spread() {
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.propagationRanking({}, {CFG}) YIELD node, time, rank \
         RETURN node.name, time, rank", ids[0].as_u64()));
    assert_eq!(got.len(), 3);
    for (i, r) in got.iter().enumerate() {
        assert!(r[2].contains(&format!("Integer({})", i + 1)), "rank {i}: {r:?}");
    }
}

#[test]
fn the_shortest_path_is_the_earliest_arrival() {
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.temporalShortestPath({}, {}, {CFG}) YIELD arrival RETURN arrival",
        ids[0].as_u64(), ids[4].as_u64()));
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(got[0][0].contains("Integer(20)"), "{got:?}");
}

#[test]
fn no_time_respecting_route_is_no_rows_not_an_error() {
    // The ordinary answer to "can this have caused that", not a failure.
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.temporalShortestPath({}, {}, {CFG}) YIELD arrival RETURN arrival",
        ids[0].as_u64(), ids[2].as_u64()));
    assert!(got.is_empty(), "{got:?}");
}

#[test]
fn symptom_explanation_names_the_common_cause() {
    // ui and db both broken. `api` is the only node that reaches both in
    // time, and it must outrank `auth`, which reaches one.
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.symptomExplanation([[{}, 30], [{}, 30]], {CFG}) \
         YIELD node, explains, onset RETURN node.name, explains, onset",
        ids[4].as_u64(), ids[1].as_u64()));
    assert!(!got.is_empty(), "no explanation offered");
    assert!(got[0][0].contains("api"), "expected api first: {got:?}");
    assert!(got[0][1].contains("Integer(2)"), "api should explain both: {got:?}");
    assert!(got[1..].iter().all(|r| r[1].contains("Integer(1)")), "{got:?}");
}

#[test]
fn a_symptom_seen_before_its_cause_could_act_is_unexplained() {
    // ui was seen broken at t=15, but the only edge into it fires at t=20.
    // Nothing upstream can account for it, and inventing a cause would be
    // worse than saying so.
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.symptomExplanation([[{}, 15]], {CFG}) YIELD node RETURN node.name",
        ids[4].as_u64()));
    assert!(got.is_empty(), "{got:?}");
}

#[test]
fn they_run_under_a_write_executor_too() {
    // The read and write executors dispatch algorithms through separate
    // matches. Registering only the read one left every primitive
    // "Unknown algorithm" in an ordinary session, which is the executor a
    // session actually uses.
    let (mut s, ids) = incident();
    let cypher = format!(
        "CALL algo.temporalReachability({}, {CFG}) YIELD node RETURN count(*) AS n",
        ids[0].as_u64());
    let q = parse_query(&cypher).unwrap();
    let r = MutQueryExecutor::new(&mut s, "default".to_string()).execute(&q).unwrap();
    assert_eq!(r.records.len(), 1);
    assert!(format!("{:?}", r.records[0].get("n")).contains("Integer(3)"));
}

#[test]
fn a_node_outside_the_projection_is_refused_not_silently_empty() {
    let (s, _) = incident();
    let q = parse_query("CALL algo.temporalReachability(9999) YIELD node RETURN node").unwrap();
    let e = QueryExecutor::new(&s).execute(&q).unwrap_err();
    assert!(format!("{e:?}").contains("not in the projected graph"), "{e:?}");
}

#[test]
fn without_a_time_property_the_edges_own_timestamps_are_used() {
    // What makes these usable on a graph nobody prepared: every edge has a
    // `created_at`. All four edges are created in the same instant here, so
    // everything is reachable -- the point is that it runs and returns.
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.temporalReachability({}) YIELD node RETURN node.name", ids[0].as_u64()));
    assert!(!got.is_empty(), "created_at fallback produced nothing");
}

#[test]
fn a_start_time_after_everything_reaches_nothing() {
    let (s, ids) = incident();
    let got = rows(&s, &format!(
        "CALL algo.temporalReachability({}, {{timeProperty: 'at', startTime: 999}}) \
         YIELD node RETURN node.name", ids[0].as_u64()));
    assert!(got.is_empty(), "{got:?}");
}
