//! A negative edge weight is refused, not skipped (#1303).
//!
//! `dijkstra` and `a_star` are correct only on non-negative weights, and both
//! implementations drop a negative edge and carry on. The path that comes back
//! is then the shortest path in a graph the caller did not ask about — the one
//! without those edges — and nothing in the result distinguishes it from an
//! answer. That is the wrong-answer shape this repository treats as worse than
//! an error, so the call surfaces refuse.
//!
//! The fixture is the smallest graph where skipping and refusing differ: the
//! cheap route uses the negative edge, so skipping it returns the *expensive*
//! route with full confidence.

use samyama::graph::{EdgeType, GraphStore, PropertyValue};
use samyama::query::executor::QueryExecutor;
use samyama::query::parser::parse_query;

/// a -> b -> c costs 5 + (-4) = 1; a -> c costs 3.
///
/// Skip the negative edge and the answer is 3, by the direct hop. Use it and
/// the answer is 1. Both are defensible readings of a graph; only one of them
/// is the graph the user built.
fn store_with_a_negative_edge() -> (GraphStore, u64, u64) {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let t = EdgeType::new("LINK");

    let ab = store.create_edge(a, b, t.clone()).unwrap();
    store.set_edge_property_sparse(ab, "w", PropertyValue::Float(5.0));
    let bc = store.create_edge(b, c, t.clone()).unwrap();
    store.set_edge_property_sparse(bc, "w", PropertyValue::Float(-4.0));
    let ac = store.create_edge(a, c, t).unwrap();
    store.set_edge_property_sparse(ac, "w", PropertyValue::Float(3.0));

    (store, a.as_u64(), c.as_u64())
}

fn run(store: &GraphStore, q: &str) -> Result<usize, String> {
    let query = parse_query(q).expect("parse failed");
    QueryExecutor::new(store)
        .execute(&query)
        .map(|r| r.records.len())
        .map_err(|e| e.to_string())
}

#[test]
fn weighted_calls_refuse_a_negative_weight_and_name_the_one_that_handles_it() {
    let (store, a, c) = store_with_a_negative_edge();

    for q in [
        format!("CALL algo.weightedPath({a}, {c}, 'w') YIELD cost RETURN count(*) AS v"),
        format!("CALL algo.shortestPath({a}, {c}, {{weight_property: 'w'}}) YIELD cost RETURN count(*) AS v"),
        format!("CALL algo.aStar({a}, {c}, {{weightProperty: 'w'}}) YIELD cost RETURN count(*) AS v"),
        format!("CALL algo.yens({a}, {c}, 2, {{weightProperty: 'w'}}) YIELD cost RETURN count(*) AS v"),
    ] {
        let err = run(&store, &q).expect_err(&format!("expected a refusal from: {q}"));
        assert!(
            err.contains("negative weight"),
            "the message has to say what is wrong: {err}"
        );
        assert!(
            err.contains("bellmanFord"),
            "a refusal without somewhere to go is a dead end: {err}"
        );
    }
}

#[test]
fn the_same_calls_still_answer_when_every_weight_is_positive() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let t = EdgeType::new("LINK");
    let ab = store.create_edge(a, b, t).unwrap();
    store.set_edge_property_sparse(ab, "w", PropertyValue::Float(2.0));

    let q = format!(
        "CALL algo.weightedPath({}, {}, 'w') YIELD cost RETURN count(*) AS v",
        a.as_u64(),
        b.as_u64()
    );
    assert_eq!(run(&store, &q), Ok(1), "a positive-weight graph is unaffected");
}

/// What the refusal is protecting against, stated as a fact about the graph.
///
/// `dijkstra` used to skip the `-4` edge and return 3.0 -- the direct hop --
/// with full confidence, when the cost over the graph as built is 1.0. It now
/// refuses, so the Cypher guard above is belt and braces rather than the only
/// thing standing there (#1303).
///
/// The error carries the offending weight and names `bellmanFord`, because a
/// refusal that does not say what to do instead is a dead end.
#[test]
fn the_algorithm_itself_refuses_rather_than_skipping() {
    let (store, a, c) = store_with_a_negative_edge();
    let view = samyama::algo::build_view(&store, None, None, Some("w"));
    assert_eq!(view.first_negative_weight(), Some(-4.0), "the fixture has one");

    let err = samyama::algo::dijkstra(&view, a, c)
        .expect_err("a negative-weight graph is refused, not answered");
    assert_eq!(err.weight, -4.0);
    let msg = err.to_string();
    assert!(msg.contains("bellmanFord"), "the refusal must name the way out: {msg}");
}

/// The same shape without the negative edge still answers, so the refusal is a
/// property of the weights rather than of the fixture.
#[test]
fn the_algorithm_answers_once_the_negative_edge_is_gone() {
    let mut store = GraphStore::new();
    let a = store.create_node("N");
    let b = store.create_node("N");
    let c = store.create_node("N");
    let t = EdgeType::new("LINK");
    for (from, to, w) in [(a, b, 5.0f64), (b, c, 4.0), (a, c, 3.0)] {
        let e = store.create_edge(from, to, t.clone()).unwrap();
        store.set_edge_property_sparse(e, "w", PropertyValue::Float(w));
    }
    let view = samyama::algo::build_view(&store, None, None, Some("w"));
    let path = samyama::algo::dijkstra(&view, a.as_u64(), c.as_u64())
        .expect("no negative weights")
        .expect("a path exists");
    assert_eq!(path.cost, 3.0, "the direct hop is genuinely cheapest here");
}

/// Hop counting does not read weights, so it is not refused. Without this the
/// guard could be pushed up to every call that takes a weight property and the
/// suite would not notice it had started refusing a correct answer.
#[test]
fn hop_counting_is_not_refused_by_a_negative_weight() {
    let (store, a, c) = store_with_a_negative_edge();
    let q = format!("CALL algo.shortestPath({a}, {c}) YIELD cost RETURN count(*) AS v");
    assert_eq!(run(&store, &q), Ok(1), "shortestPath without a weight counts hops");
}
