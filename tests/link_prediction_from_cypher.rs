//! Link prediction from Cypher (ALGO-01).
//!
//! ```cypher
//! CALL algo.jaccard({limit: 5})        YIELD node1, node2, score   -- ranking
//! CALL algo.adamicAdar(id1, id2)       YIELD node1, node2, score   -- one pair
//! ```
//!
//! All three read the same signal — shared neighbours — and differ only in
//! what a shared neighbour is worth:
//!
//! | score | a shared neighbour is worth |
//! |---|---|
//! | common neighbours | 1 |
//! | Jaccard | 1, divided by the union |
//! | Adamic–Adar | `1 / ln(degree)` |
//!
//! Adamic–Adar is the one worth testing hardest. Two people who both know a
//! hub share almost nothing; two who both know someone obscure share a great
//! deal. The fixture below is built so that distinction changes the answer.

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `hub` is joined to everyone; `quiet` is joined only to a and b.
///
/// So (a, b) share two neighbours — the hub and the quiet one — while
/// (a, c) share only the hub. Common neighbours ranks a–b above a–c by count;
/// Adamic–Adar ranks it *further* above, because the quiet neighbour is worth
/// much more than the hub.
fn hub_and_quiet() -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for n in ["a", "b", "c", "d", "hub", "quiet"] {
        let x = s.create_node_with_labels([Label::new("P")]);
        s.set_node_property("default", x, "name", PropertyValue::String(n.into())).unwrap();
        ids.push(x);
    }
    let (a, b, c, d, hub, quiet) = (ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]);
    for x in [a, b, c, d] {
        s.create_edge(hub, x, "KNOWS").unwrap();
    }
    s.create_edge(quiet, a, "KNOWS").unwrap();
    s.create_edge(quiet, b, "KNOWS").unwrap();
    (s, ids)
}

fn rows(store: &GraphStore, cypher: &str) -> Vec<(String, String, f64)> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    r.records.iter().map(|rec| {
        let name = |k: &str| match rec.get(k) {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => format!("{other:?}"),
        };
        let sc = match rec.get("score") {
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            other => panic!("score was {other:?}"),
        };
        (name("n1"), name("n2"), sc)
    }).collect()
}

const RANK: &str = "CALL algo.%({limit: 20}) YIELD node1, node2, score \
                    RETURN node1.name AS n1, node2.name AS n2, score";

fn rank(s: &GraphStore, algo: &str) -> Vec<(String, String, f64)> {
    rows(s, &RANK.replace('%', algo))
}

fn pair(s: &GraphStore, algo: &str, a: NodeId, b: NodeId) -> f64 {
    let q = format!(
        "CALL algo.{algo}({}, {}) YIELD node1, node2, score \
         RETURN node1.name AS n1, node2.name AS n2, score",
        a.as_u64(), b.as_u64());
    rows(s, &q)[0].2
}

#[test]
fn a_rare_shared_neighbour_counts_for_more_than_a_hub() {
    let (s, ids) = hub_and_quiet();
    let (a, b, c) = (ids[0], ids[1], ids[2]);
    // a–b share {hub, quiet}; a–c share {hub} only.
    assert_eq!(pair(&s, "commonNeighbors", a, b), 2.0);
    assert_eq!(pair(&s, "commonNeighbors", a, c), 1.0);

    let ab = pair(&s, "adamicAdar", a, b);
    let ac = pair(&s, "adamicAdar", a, c);
    // Under raw counting a–b is worth twice a–c. Under Adamic-Adar it is worth
    // more than twice, because `quiet` has degree 2 and `hub` has degree 4:
    // 1/ln(2) is much larger than 1/ln(4).
    assert!(ab > 2.0 * ac, "adamic-adar should reward the rare neighbour: {ab} vs {ac}");
}

#[test]
fn jaccard_normalises_by_how_connected_the_pair_is() {
    let (s, ids) = hub_and_quiet();
    let (a, b, c, d) = (ids[0], ids[1], ids[2], ids[3]);
    // a–b: |{hub,quiet}| / |{hub,quiet}| = 1.0 -- their neighbourhoods are
    // identical. c–d: |{hub}| / |{hub}| = 1.0 as well.
    assert!((pair(&s, "jaccard", a, b) - 1.0).abs() < 1e-9);
    assert!((pair(&s, "jaccard", c, d) - 1.0).abs() < 1e-9);
    // a–c: share {hub}, union is {hub, quiet} -> 0.5.
    assert!((pair(&s, "jaccard", a, c) - 0.5).abs() < 1e-9);
}

#[test]
fn the_ranking_excludes_pairs_that_are_already_connected() {
    let (s, ids) = hub_and_quiet();
    let names: Vec<(String, String)> =
        rank(&s, "commonNeighbors").into_iter().map(|(a, b, _)| (a, b)).collect();
    // `hub` is joined to a, b, c and d, so none of those pairs may appear.
    for joined in [("hub", "a"), ("hub", "b"), ("hub", "c"), ("hub", "d"),
                   ("quiet", "a"), ("quiet", "b")] {
        assert!(
            !names.iter().any(|(x, y)| (x.as_str(), y.as_str()) == joined
                || (y.as_str(), x.as_str()) == joined),
            "an existing edge appeared as a prediction: {joined:?} in {names:?}"
        );
    }
    let _ = ids;
}

#[test]
fn the_ranking_is_sorted_and_respects_its_limit() {
    let (s, _) = hub_and_quiet();
    for algo in ["commonNeighbors", "jaccard", "adamicAdar"] {
        let v = rank(&s, algo);
        assert!(v.windows(2).all(|w| w[0].2 >= w[1].2), "{algo} not sorted: {v:?}");
        let two = rows(&s, &RANK.replace('%', algo).replace("limit: 20", "limit: 2"));
        assert!(two.len() <= 2, "{algo} ignored its limit: {two:?}");
    }
}

#[test]
fn the_two_spellings_of_common_neighbours_agree() {
    let (s, _) = hub_and_quiet();
    assert_eq!(rank(&s, "commonNeighbors"), rank(&s, "commonNeighbours"));
}

#[test]
fn a_node_outside_the_projection_is_refused() {
    let (s, ids) = hub_and_quiet();
    let q = format!(
        "CALL algo.jaccard({}, 99999) YIELD score RETURN score", ids[0].as_u64());
    let parsed = parse_query(&q).unwrap();
    let e = QueryExecutor::new(&s).execute(&parsed).unwrap_err();
    assert!(format!("{e:?}").contains("not in the projected graph"), "{e:?}");
}

#[test]
fn two_isolated_nodes_score_zero_rather_than_perfect_similarity() {
    // Both neighbourhoods are empty, so "identical sets" would argue for 1.0.
    // That would rank every pair of isolated nodes above every real candidate.
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for n in ["i1", "i2"] {
        let x = s.create_node_with_labels([Label::new("P")]);
        s.set_node_property("default", x, "name", PropertyValue::String(n.into())).unwrap();
        ids.push(x);
    }
    assert_eq!(pair(&s, "jaccard", ids[0], ids[1]), 0.0);
}
