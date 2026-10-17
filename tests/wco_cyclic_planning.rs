//! A cyclic pattern plans a worst-case-optimal join by default (#1614, PERF-07).
//!
//! The planner used to close a cycle as an `Expand` into a synthetic
//! `__self_a_2` plus `Filter (__self_a_2 = a)`: a row per surviving candidate,
//! built to be checked. The hop that binds the cycle's last new variable now
//! also binds the closing relationship, from the intersection of the two
//! adjacency lists, and `EXPLAIN` prints it as `TrieJoin`. That is the fused
//! form #1082 left open after its pruning half shipped.
//!
//! Each phrasing is checked two ways: the plan says `TrieJoin`, and the answer
//! equals the one the old plan gives. The old plan is still reachable, because
//! the fusion requires the two hops to list the same types -- so
//! `(c)-[:R|R]->(a)`, which matches exactly what `(c)-[:R]->(a)` matches, is
//! the control for it.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// `n` `:P` nodes on a ring with chords two and three apart, a back-edge
/// every fifth node that closes directed triangles and four-cycles, two
/// parallel edges on one chord, and one self-loop -- the shapes a closing hop
/// has to get right: relationship isomorphism, parallel edges as separate
/// matches, and a self-relationship counted once.
fn fixture(n: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..n)
        .map(|i| {
            let id = store.create_node("P");
            let _ = store.set_node_property(
                "default",
                id,
                "id".to_string(),
                PropertyValue::Integer(i as i64),
            );
            id
        })
        .collect();
    for i in 0..n {
        store.create_edge(ids[i], ids[(i + 1) % n], "R").unwrap();
        store.create_edge(ids[i], ids[(i + 2) % n], "R").unwrap();
        if i % 3 == 0 {
            store.create_edge(ids[i], ids[(i + 3) % n], "R").unwrap();
        }
        if i % 5 == 0 {
            store.create_edge(ids[(i + 3) % n], ids[i], "R").unwrap();
        }
    }
    store.create_edge(ids[1], ids[3], "R").unwrap();
    store.create_edge(ids[1], ids[3], "R").unwrap();
    store.create_edge(ids[2], ids[2], "R").unwrap();
    store.create_edge(ids[0], ids[1], "Q").unwrap();
    store
}

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let query = parse_query(cypher).unwrap_or_else(|e| panic!("{cypher}: {e:?}"));
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    assert_eq!(batch.records.len(), 1, "{cypher}");
    match batch.records[0].get("n") {
        Some(Value::Property(PropertyValue::Integer(n))) => *n,
        other => panic!("{cypher}: {other:?}"),
    }
}

fn plan(store: &GraphStore, cypher: &str) -> String {
    let query = parse_query(&format!("EXPLAIN {cypher}")).unwrap();
    let batch = QueryExecutor::new(store).execute(&query).unwrap();
    let text = match batch.records[0].get("plan") {
        Some(Value::Property(PropertyValue::String(t))) => t.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        samyama::query::executor::undescribed_plan_lines(&text).is_empty(),
        "plan tree stops at an operator with no describe (#1826):\n{text}"
    );
    text
}

/// The five cyclic phrasings CH-PLAN-02 sends, each with the phrasing that
/// keeps the old plan, and the acyclic control.
const CYCLIC: &[(&str, &str, &str)] = &[
    (
        "triangle_anon",
        "MATCH (a)-[]->(b)-[]->(c)-[]->(a) RETURN count(*) AS n",
        "MATCH (a)-[]->(b)-[]->(c)-[:R|Q]->(a) RETURN count(*) AS n",
    ),
    (
        "triangle_comma",
        "MATCH (a:P)-[:R]->(b:P), (b)-[:R]->(c:P), (c)-[:R]->(a) RETURN count(*) AS n",
        "MATCH (a:P)-[:R]->(b:P), (b)-[:R]->(c:P), (c)-[:R|R]->(a) RETURN count(*) AS n",
    ),
    (
        "diamond",
        "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P), (a)-[:R]->(c) RETURN count(*) AS n",
        "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P), (a)-[:R|R]->(c) RETURN count(*) AS n",
    ),
    (
        "four_cycle",
        "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P)-[:R]->(d:P)-[:R]->(a) RETURN count(*) AS n",
        "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P)-[:R]->(d:P)-[:R|R]->(a) RETURN count(*) AS n",
    ),
    (
        "varlen_loop",
        "MATCH p=(a:P)-[:R*3]->(a) RETURN count(*) AS n",
        "MATCH p=(a:P)-[:R]->()-[:R]->()-[:R|R]->(a) RETURN count(*) AS n",
    ),
];

const CONTROL: &str = "MATCH (a:P)-[:R]->(b:P)<-[:R]-(c:P) RETURN count(*) AS n";

#[test]
fn every_cyclic_phrasing_plans_a_trie_join_and_the_control_does_not() {
    let store = fixture(40);
    for (name, cyclic, _) in CYCLIC {
        let text = plan(&store, cyclic);
        assert!(text.contains("TrieJoin"), "{name}: no TrieJoin in\n{text}");
        assert!(!text.contains("__self_"), "{name}: the close is still an expand-and-test\n{text}");
    }
    let text = plan(&store, CONTROL);
    assert!(!text.contains("TrieJoin"), "acyclic control planned a TrieJoin:\n{text}");
}

#[test]
fn the_fused_close_answers_what_the_expand_and_test_answered() {
    // Small enough that no expand reaches its type index, and large enough
    // that every expand does: the fused close has a path for each.
    for n in [40usize, 700] {
        let store = fixture(n);
        for (name, cyclic, reference) in CYCLIC {
            let old = plan(&store, reference);
            assert!(!old.contains("TrieJoin"), "{name}: the reference phrasing fused too:\n{old}");
            assert_eq!(count(&store, cyclic), count(&store, reference), "{name} at n={n}");
            assert!(count(&store, cyclic) > 0, "{name} at n={n}: the fixture has no match");
        }
    }
}

#[test]
fn the_closing_relationship_and_path_are_bound_on_the_fused_row() {
    let store = fixture(40);
    // `r3` is the closing relationship: distinct ids, and one of the parallel
    // pair between 1 and 3 is among them (`(1)->(3)` closes `(3)->(?)->(1)`).
    let rels = "MATCH (a:P)-[r1:R]->(b:P)-[r2:R]->(c:P)-[r3:R]->(a) \
                RETURN count(DISTINCT r3) AS n";
    assert!(plan(&store, rels).contains("binds r2, r3"), "{}", plan(&store, rels));
    let reference = "MATCH (a:P)-[r1:R]->(b:P)-[r2:R]->(c:P)-[r3:R|R]->(a) \
                     RETURN count(DISTINCT r3) AS n";
    assert_eq!(count(&store, rels), count(&store, reference));

    // The path includes the closing hop.
    let query = parse_query(
        "MATCH p=(a:P)-[:R]->(b:P)-[:R]->(c:P)-[:R]->(a) RETURN length(p) AS n",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    assert!(!batch.records.is_empty());
    for r in &batch.records {
        assert_eq!(r.get("n"), Some(&Value::Property(PropertyValue::Integer(3))), "{r:?}");
    }

    // Relationship isomorphism holds on the closing hop: an undirected
    // 2-cycle over one edge is not a match.
    assert_eq!(count(&store, "MATCH (a:P)-[:Q]-(b)-[:Q]-(a) RETURN count(*) AS n"), 0);
}

/// The fused close is not slower than the expand-and-test it replaces.
///
/// A **ratio in one process**, never a wall-clock bound: the same triangle
/// query, fused and in the control phrasing that keeps the old plan, over the
/// same graph. The control's unequal type lists also switch off the
/// co-neighbour prune, so it is the plan a cycle got before #1088: a row per
/// candidate, then an expand and a filter to prove the close. The fused one
/// binds the closing edge on the row it was going to emit anyway. Pinned
/// loosely, because what this guards is a fusion that costs more than it
/// saves, not a speed-up figure: 0.41x in debug on this fixture.
#[test]
fn the_fused_close_is_not_slower_than_the_plan_it_replaces() {
    use std::time::Instant;
    let store = fixture(4_000);
    let fused = "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P)-[:R]->(a) RETURN count(*) AS n";
    let control = "MATCH (a:P)-[:R]->(b:P)-[:R]->(c:P)-[:R|R]->(a) RETURN count(*) AS n";
    assert!(plan(&store, fused).contains("TrieJoin"));
    assert!(!plan(&store, control).contains("TrieJoin"));
    assert_eq!(count(&store, fused), count(&store, control));
    let best = |cypher: &str| {
        let query = parse_query(cypher).unwrap();
        let mut best = f64::MAX;
        for _ in 0..7 {
            let t = Instant::now();
            QueryExecutor::new(&store).execute(&query).unwrap();
            best = best.min(t.elapsed().as_secs_f64());
        }
        best
    };
    let _ = best(control);
    let _ = best(fused);
    let old = best(control);
    let new = best(fused);
    let ratio = new / old;
    eprintln!("fused close: {new:.6}s against {old:.6}s expand-and-test, {ratio:.2}x");
    assert!(
        ratio < 1.25,
        "the fused close costs {ratio:.2}x the expand-and-test it replaces \
         ({new:.6}s against {old:.6}s) (#1614)"
    );
}
