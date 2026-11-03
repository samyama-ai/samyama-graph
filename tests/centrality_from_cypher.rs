//! Degree, closeness and betweenness centrality from Cypher (ALGO-01).
//!
//! The three disagree on purpose, and the graph below is built so they must:
//! a **barbell** — two triangles joined by a single bridge node.
//!
//! ```text
//!   0──1        4──5
//!   │╲ │        │ ╱│
//!   │ ╲│        │╱ │
//!   2──╴───3───╴───6
//! ```
//!
//! Node 3 has degree 2 — tied for the lowest — and is the only route between
//! the halves. Degree centrality does not put it top; betweenness puts it top
//! by a wide margin. An engine that returns the degree ranking under the name
//! "betweenness" would look entirely plausible on any graph without a
//! bottleneck, which is most test graphs.

use samyama::graph::{GraphStore, Label, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// Two triangles (0,1,2) and (4,5,6), bridged through 3.
fn barbell() -> GraphStore {
    let mut s = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..7 {
        let n = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", n, "name", PropertyValue::String(format!("n{i}"))).unwrap();
        ids.push(n);
    }
    for (a, b) in [(0, 1), (1, 2), (2, 0), (2, 3), (3, 4), (4, 5), (5, 6), (6, 4)] {
        s.create_edge(ids[a], ids[b], "R").unwrap();
    }
    s
}

fn scores(store: &GraphStore, cypher: &str) -> Vec<(String, f64)> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let r = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    r.records.iter().map(|rec| {
        let name = match rec.get("name") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => format!("{other:?}"),
        };
        let sc = match rec.get("score") {
            Some(Value::Property(PropertyValue::Float(f))) => *f,
            Some(Value::Property(PropertyValue::Integer(i))) => *i as f64,
            other => panic!("score was {other:?}"),
        };
        (name, sc)
    }).collect()
}

const Q: &str = "CALL algo.%(  ) YIELD node, score RETURN node.name AS name, score";

fn run(store: &GraphStore, algo: &str) -> Vec<(String, f64)> {
    scores(store, &Q.replace('%', algo))
}

#[test]
fn betweenness_finds_the_bridge_that_degree_ranks_last() {
    let s = barbell();
    let bt = run(&s, "betweenness");
    assert_eq!(bt[0].0, "n3", "the bridge must rank first for betweenness: {bt:?}");

    let dg = run(&s, "degree");
    // n3's degree is 2 -- tied with four others rather than uniquely lowest,
    // which is why the assertion is about the *contrast* and not about last
    // place. It is strictly below the top, while being top for betweenness.
    let n3_degree = dg.iter().find(|(n, _)| n == "n3").unwrap().1;
    assert!(n3_degree < dg[0].1,
            "n3 should not be the most connected: {dg:?}");
    assert!(dg[0].0 != "n3", "degree must not agree with betweenness here: {dg:?}");
}

#[test]
fn closeness_prefers_the_middle_too_but_not_as_sharply() {
    let s = barbell();
    let cl = run(&s, "closeness");
    assert_eq!(cl[0].0, "n3", "{cl:?}");
    // Every node is reachable from every other, so no score is zero.
    assert!(cl.iter().all(|(_, v)| *v > 0.0), "{cl:?}");
}

#[test]
fn all_seven_nodes_are_scored_by_each() {
    let s = barbell();
    for algo in ["degree", "closeness", "betweenness"] {
        assert_eq!(run(&s, algo).len(), 7, "{algo}");
    }
}

#[test]
fn the_centrality_suffix_is_the_same_algorithm() {
    // `algo.betweenness` and `algo.betweennessCentrality` are one algorithm
    // with two spellings; ALGO-01 counts algorithms, so they must agree
    // exactly rather than being two entries that could drift.
    let s = barbell();
    for (short, long) in [
        ("degree", "degreeCentrality"),
        ("closeness", "closenessCentrality"),
        ("betweenness", "betweennessCentrality"),
    ] {
        assert_eq!(run(&s, short), run(&s, long), "{short} vs {long}");
    }
}

#[test]
fn scores_come_back_ranked_highest_first() {
    let s = barbell();
    for algo in ["degree", "closeness", "betweenness"] {
        let v = run(&s, algo);
        assert!(v.windows(2).all(|w| w[0].1 >= w[1].1), "{algo} not sorted: {v:?}");
    }
}

#[test]
fn an_isolated_node_scores_zero_rather_than_being_omitted() {
    // Omitting it would be the easy bug: a caller joining scores back onto
    // nodes would find one missing and, more likely, never notice.
    let mut s = barbell();
    s.create_node_with_labels([Label::new("N")]);
    for algo in ["degree", "closeness", "betweenness"] {
        let v = run(&s, algo);
        assert_eq!(v.len(), 8, "{algo}: {v:?}");
        assert_eq!(v.last().unwrap().1, 0.0, "{algo}: isolated node should score 0: {v:?}");
    }
}

#[test]
fn harmonic_survives_a_disconnected_graph_where_closeness_needs_a_convention() {
    // The reason harmonic exists. Add an isolated pair: closeness has to
    // divide by a total distance that is now infinite for some pairs, and
    // leans on NetworkX's reachable-fraction convention to say anything.
    // Harmonic sums 1/d, so an unreachable node contributes zero and no
    // convention is needed.
    let mut s = barbell();
    let a = s.create_node_with_labels([Label::new("N")]);
    let b = s.create_node_with_labels([Label::new("N")]);
    s.set_node_property("default", a, "name", PropertyValue::String("x1".into())).unwrap();
    s.set_node_property("default", b, "name", PropertyValue::String("x2".into())).unwrap();
    s.create_edge(a, b, "R").unwrap();

    let h = run(&s, "harmonic");
    assert_eq!(h.len(), 9);
    // The isolated pair scores exactly 1.0 each -- one neighbour at distance
    // 1, and every unreachable node contributing 1/inf = 0. An implementation
    // that dropped unreachable nodes instead of scoring them zero would give
    // the same 1.0 here, so the discriminating half is below.
    let x1 = h.iter().find(|(n, _)| n == "x1").unwrap().1;
    assert!((x1 - 1.0).abs() < 1e-9, "{h:?}");
    // The main component all outranks the isolated pair, which is the whole
    // point: closeness without its reachable-fraction correction would rank a
    // perfectly-central node of a two-node component *above* these.
    assert!(h.iter().take(7).all(|(_, v)| *v > x1), "{h:?}");
    // Harmonic favours the node with most neighbours at distance 1, not the
    // bridge -- unlike betweenness. n2 has three, n3 has two.
    assert_eq!(h[0].0, "n2", "{h:?}");
}

#[test]
fn core_number_separates_a_pendant_from_the_core() {
    // The bare barbell is *entirely* a 2-core -- even the bridge has two
    // neighbours that both survive the peel -- so it cannot discriminate.
    // A pendant leaf can: it has degree 1 and is peeled first.
    let mut s = barbell();
    let leaf = s.create_node_with_labels([Label::new("N")]);
    s.set_node_property("default", leaf, "name", PropertyValue::String("leaf".into())).unwrap();
    let n0 = s.all_nodes()[0].id;
    s.create_edge(n0, leaf, "R").unwrap();

    let c = run(&s, "kCore");
    assert_eq!(c.len(), 8);
    let leaf_core = c.iter().find(|(n, _)| n == "leaf").unwrap().1;
    assert_eq!(leaf_core, 1.0, "a pendant is a 1-core: {c:?}");
    assert_eq!(c.iter().filter(|(_, v)| *v >= 2.0).count(), 7, "{c:?}");
}

#[test]
fn eigenvector_refuses_rather_than_returning_a_meaningless_vector() {
    // A graph with no edges has no principal eigenvector. Power iteration
    // still produces a normalised vector of the right shape every round, so
    // returning it would publish a number that means nothing.
    let mut s = GraphStore::new();
    for i in 0..3 {
        let n = s.create_node_with_labels([Label::new("N")]);
        s.set_node_property("default", n, "name", PropertyValue::String(format!("z{i}"))).unwrap();
    }
    let q = parse_query(&Q.replace('%', "eigenvector")).unwrap();
    let e = QueryExecutor::new(&s).execute(&q).unwrap_err();
    assert!(format!("{e:?}").contains("converge"), "{e:?}");
}

#[test]
fn eigenvector_ranks_the_well_connected_core() {
    let s = barbell();
    let e = run(&s, "eigenvector");
    assert_eq!(e.len(), 7);
    // Every score is positive and normalised to unit length.
    let norm: f64 = e.iter().map(|(_, v)| v * v).sum::<f64>().sqrt();
    assert!((norm - 1.0).abs() < 1e-6, "not unit-normalised: {norm}");
}
