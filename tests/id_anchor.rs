//! `WHERE id(n) = …` anchors the scan instead of filtering one (#538).
//!
//! `id()` is unique by construction, so this needs no statistics and no cost
//! model — which is what made it worth handling separately from the
//! index-selection path. Before, `MATCH (n) WHERE id(n) = 5` scanned the whole
//! label and filtered, and `shortestPath(…) WHERE id(a) = 1 AND id(b) = …` ran
//! ~1000× slower than the same query written with inline properties.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn graph(n: i64) -> GraphStore {
    let mut store = GraphStore::new();
    for i in 0..n {
        let id = store.create_node("N");
        let _ = store.set_node_property("default", id, "v".to_string(), PropertyValue::Integer(i));
    }
    store
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).expect("parse");
    match QueryExecutor::new(store).execute(&query).unwrap().records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => {
            t.lines().take_while(|l| !l.starts_with("---")).collect::<Vec<_>>().join("\n")
        }
        other => panic!("{other:?}"),
    }
}

fn values(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let query = parse_query(cypher).expect("parse");
    QueryExecutor::new(store)
        .execute(&query)
        .unwrap()
        .records
        .iter()
        .map(|r| match r.get("v") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i,
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn an_id_equality_anchors_the_scan() {
    let store = graph(1000);
    let text = plan(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
    assert!(!text.contains("NodeScan"), "the label scan should be gone:\n{text}");
}

#[test]
fn it_works_without_a_label_too() {
    let store = graph(1000);
    let text = plan(&store, "MATCH (n) WHERE id(n) = 5 RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
}

#[test]
fn the_answer_is_unchanged() {
    let store = graph(1000);
    assert_eq!(values(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v").len(), 1);
    // Ids start at 1, and `v` counts from 0, so id 5 holds v = 4.
    assert_eq!(values(&store, "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v"), vec![4]);
}

#[test]
fn a_label_that_does_not_match_returns_nothing() {
    // The hazard of scanning by id: it bypasses the label index, so the
    // pattern's labels have to be checked per node or `(n:Other)` would match
    // whatever node holds that id.
    let mut store = graph(10);
    let other = store.create_node("Other");
    let _ = store.set_node_property("default", other, "v".to_string(), PropertyValue::Integer(99));

    let other_id = other.as_u64() as i64;
    assert_eq!(
        values(&store, &format!("MATCH (n:Other) WHERE id(n) = {other_id} RETURN n.v AS v")),
        vec![99]
    );
    assert!(
        values(&store, &format!("MATCH (n:N) WHERE id(n) = {other_id} RETURN n.v AS v")).is_empty(),
        "an :N pattern must not match an :Other node by id"
    );
}

#[test]
fn an_id_that_does_not_exist_returns_nothing() {
    let store = graph(10);
    assert!(values(&store, "MATCH (n:N) WHERE id(n) = 99999 RETURN n.v AS v").is_empty());
}

#[test]
fn a_negative_id_returns_nothing_rather_than_wrapping() {
    // `as u64` on a negative literal wraps to a very large positive id, which
    // matches nothing but does so after a full scan.
    let store = graph(10);
    assert!(values(&store, "MATCH (n:N) WHERE id(n) = -1 RETURN n.v AS v").is_empty());
}

#[test]
fn the_literal_may_be_on_either_side() {
    let store = graph(100);
    let text = plan(&store, "MATCH (n:N) WHERE 5 = id(n) RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
    assert_eq!(values(&store, "MATCH (n:N) WHERE 5 = id(n) RETURN n.v AS v"), vec![4]);
}

#[test]
fn an_id_list_anchors_on_every_element() {
    let store = graph(1000);
    let text = plan(&store, "MATCH (n:N) WHERE id(n) IN [3, 5, 7] RETURN n.v AS v");
    assert!(text.contains("NodeById"), "{text}");
    let mut got = values(&store, "MATCH (n:N) WHERE id(n) IN [3, 5, 7] RETURN n.v AS v");
    got.sort();
    assert_eq!(got, vec![2, 4, 6]);
}

/// Total time to run `cypher` against `store` `reps` times.
///
/// Repeated because a single anchored lookup is microseconds, and a ratio
/// between two microsecond measurements is mostly timer noise.
fn repeat(store: &GraphStore, cypher: &str, reps: u32) -> std::time::Duration {
    let query = parse_query(cypher).expect("parse");
    // Warm, so the first call does not pay for statistics the rest skip.
    let _ = QueryExecutor::new(store).execute(&query).unwrap();
    let started = std::time::Instant::now();
    for _ in 0..reps {
        let batch = QueryExecutor::new(store).execute(&query).unwrap();
        std::hint::black_box(batch.records.len());
    }
    started.elapsed()
}

#[test]
fn a_point_lookup_does_not_scale_with_the_graph() {
    // The property this is really about: the cost should not depend on how
    // many nodes exist.
    //
    // Asserted as a *ratio* between two graph sizes rather than against a wall
    // clock. An absolute bound encodes the speed of the machine and the build
    // profile that wrote it: the first version of this test asserted 5 ms,
    // passed everywhere in `--release`, and failed in CI, which builds in debug
    // (#549 is the same mistake, made by someone else, in the other direction).
    //
    // A ratio has neither problem. A scan grows with the graph; an anchored
    // lookup does not, and 20x the nodes is the difference between the two.
    const SMALL: i64 = 2_500;
    const LARGE: i64 = 50_000;
    const REPS: u32 = 300;

    let small = graph(SMALL);
    let large = graph(LARGE);

    let cypher = "MATCH (n:N) WHERE id(n) = 5 RETURN n.v AS v";
    assert_eq!(values(&large, cypher), vec![4], "the answer first");

    let t_small = repeat(&small, cypher, REPS);
    let t_large = repeat(&large, cypher, REPS);
    let ratio = t_large.as_secs_f64() / t_small.as_secs_f64().max(1e-9);

    // A scan would be ~20x. Anything under 4x is flat within measurement noise
    // and cannot be a scan.
    assert!(
        ratio < 4.0,
        "20x the nodes cost {ratio:.1}x the time ({t_small:?} -> {t_large:?}) — it is still scanning"
    );
}

#[test]
fn both_shortest_path_endpoints_reach_a_node_by_id() {
    // The plan-shape form of the test below, and the one that would have caught
    // #584 immediately. A timing assertion can be satisfied by a plan that is
    // merely fast enough on the machine that wrote it; this cannot.
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..50).map(|_| store.create_node("N")).collect();
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "KNOWS").unwrap();
    }
    let (a, b) = (ids[0].as_u64(), ids[5].as_u64());

    let text = plan(
        &store,
        &format!(
            "MATCH p = shortestPath((a:N)-[:KNOWS*]-(b:N)) WHERE id(a) = {a} AND id(b) = {b} \
             RETURN length(p) AS v"
        ),
    );
    assert_eq!(
        text.matches("NodeById").count(),
        2,
        "both endpoints must be pinned, not just the start:\n{text}"
    );
    assert!(
        !text.contains("NodeScan"),
        "neither endpoint should be scanning the label:\n{text}"
    );
}

#[test]
fn both_endpoints_of_a_path_can_be_pinned_by_id() {
    // #538's original shape, and what motivated the issue: written with
    // `WHERE id(a) = ... AND id(b) = ...` it ran ~1000x slower than the same
    // query written with inline properties, because only the inline form
    // anchored.
    //
    // So the assertion is against the inline form on the same graph, not
    // against a wall clock. That is the comparison the issue is actually about,
    // and it does not encode the speed of the machine or the build profile.
    let mut store = GraphStore::new();
    let ids: Vec<NodeId> = (0..1200)
        .map(|i| {
            let id = store.create_node("N");
            let _ = store.set_node_property(
                "default",
                id,
                "seq".to_string(),
                PropertyValue::Integer(i),
            );
            id
        })
        .collect();
    for w in ids.windows(2) {
        store.create_edge(w[0], w[1], "KNOWS").unwrap();
    }
    let (a, b) = (ids[0].as_u64(), ids[5].as_u64());

    let by_id = format!(
        "MATCH p = shortestPath((a:N)-[:KNOWS*]-(b:N)) WHERE id(a) = {a} AND id(b) = {b} \
         RETURN length(p) AS v"
    );
    let by_property =
        "MATCH p = shortestPath((a:N {seq: 0})-[:KNOWS*]-(b:N {seq: 5})) RETURN length(p) AS v";

    let query = parse_query(&by_id).unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert_eq!(batch.records.len(), 1, "the chain connects them");
    assert_eq!(
        QueryExecutor::new(&store).execute(&parse_query(by_property).unwrap()).unwrap().records.len(),
        1,
        "and the inline form finds the same path"
    );

    let t_id = repeat(&store, &by_id, 20);
    let t_prop = repeat(&store, by_property, 20);
    let ratio = t_id.as_secs_f64() / t_prop.as_secs_f64().max(1e-9);

    // It was ~1000x. Anything within an order of magnitude means both ends are
    // anchored; the two forms are not required to be identical, since they
    // reach the same plan by different routes.
    assert!(
        ratio < 10.0,
        "the id() form cost {ratio:.1}x the inline form ({t_prop:?} -> {t_id:?}) \
         — the endpoints are not being pinned"
    );
}
