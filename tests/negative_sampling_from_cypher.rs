//! Negative sampling from Cypher (ML-04, #1725).
//!
//! ```cypher
//! CALL algo.negativeSampling({strategy: 'random', count: 100, seed: 7})
//! YIELD node1, node2
//! ```
//!
//! `algo.negativeSampling` used to be refused as `UnknownAlgorithm`. The three
//! strategies are tested for what they draw in
//! `samyama_graph_algorithms::negative_sampling`; this file checks what a
//! caller sees: each strategy runs, every row is a pair that is not an edge,
//! the same seed gives the same rows, and a bad option is refused with a
//! message that says what to pass instead.

use std::collections::{HashSet, VecDeque};

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

const NODES: usize = 30;
const SEED: u64 = 7;

/// A ring of 30 people with a few chords, so pairs 2 and 3 hops apart exist,
/// and a `Company` node with edges of its own type.
fn fixture() -> (GraphStore, Vec<NodeId>) {
    let mut s = GraphStore::new();
    let ids: Vec<NodeId> = (0..NODES)
        .map(|i| {
            let x = s.create_node_with_labels([Label::new("Person")]);
            s.set_node_property("default", x, "name", PropertyValue::String(format!("p{i}")))
                .unwrap();
            x
        })
        .collect();
    for i in 0..NODES {
        s.create_edge(ids[i], ids[(i + 1) % NODES], "KNOWS")
            .unwrap();
    }
    for (a, b) in [(0, 10), (5, 20), (12, 25)] {
        s.create_edge(ids[a], ids[b], "KNOWS").unwrap();
    }
    let company = s.create_node_with_labels([Label::new("Company")]);
    s.create_edge(ids[0], company, "WORKS_AT").unwrap();
    (s, ids)
}

fn run(store: &GraphStore, cypher: &str) -> Result<Vec<(NodeId, NodeId)>, String> {
    let q = parse_query(cypher).map_err(|e| format!("{e:?}"))?;
    let r = QueryExecutor::new(store)
        .execute(&q)
        .map_err(|e| e.to_string())?;
    Ok(r.records
        .iter()
        .map(|rec| {
            let id = |k: &str| match rec.get(k) {
                Some(Value::Node(id, _)) | Some(Value::NodeRef(id)) => *id,
                other => panic!("{k} was {other:?}"),
            };
            (id("node1"), id("node2"))
        })
        .collect())
}

fn sample(store: &GraphStore, config: &str) -> Vec<(NodeId, NodeId)> {
    let cypher =
        format!("CALL algo.negativeSampling({{{config}}}) YIELD node1, node2 RETURN node1, node2");
    run(store, &cypher).unwrap_or_else(|e| panic!("{cypher}: {e}"))
}

fn refusal(store: &GraphStore, config: &str) -> String {
    let cypher =
        format!("CALL algo.negativeSampling({{{config}}}) YIELD node1, node2 RETURN node1, node2");
    run(store, &cypher).expect_err(&format!("{cypher} should be refused"))
}

/// Undirected adjacency among the Person nodes over KNOWS.
fn knows(store: &GraphStore, ids: &[NodeId]) -> Vec<HashSet<usize>> {
    let index = |id: NodeId| ids.iter().position(|&x| x == id);
    let mut adj = vec![HashSet::new(); ids.len()];
    for (i, &id) in ids.iter().enumerate() {
        for edge in store.get_outgoing_edges(id) {
            if edge.edge_type.as_str() == "KNOWS" {
                if let Some(j) = index(edge.target) {
                    adj[i].insert(j);
                    adj[j].insert(i);
                }
            }
        }
    }
    adj
}

fn hops(adj: &[HashSet<usize>], from: usize, to: usize) -> usize {
    let mut seen = vec![usize::MAX; adj.len()];
    seen[from] = 0;
    let mut queue = VecDeque::from([from]);
    while let Some(u) = queue.pop_front() {
        for &v in &adj[u] {
            if seen[v] == usize::MAX {
                seen[v] = seen[u] + 1;
                queue.push_back(v);
            }
        }
    }
    seen[to]
}

#[test]
fn every_strategy_answers_with_pairs_that_are_not_edges() {
    let (store, ids) = fixture();
    let adj = knows(&store, &ids);
    let index = |id: NodeId| ids.iter().position(|&x| x == id).expect("a Person");
    for strategy in ["random", "degree", "khop"] {
        let pairs = sample(
            &store,
            &format!("strategy: '{strategy}', count: 40, seed: {SEED}, label: 'Person', edgeType: 'KNOWS'"),
        );
        assert_eq!(pairs.len(), 40, "{strategy}");
        let unique: HashSet<_> = pairs.iter().collect();
        assert_eq!(unique.len(), 40, "{strategy}: a repeated pair");
        for &(a, b) in &pairs {
            let (ia, ib) = (index(a), index(b));
            assert_ne!(ia, ib, "{strategy}: a node with itself");
            assert!(
                !adj[ia].contains(&ib),
                "{strategy}: ({a:?}, {b:?}) is an edge"
            );
        }
    }
}

#[test]
fn khop_pairs_are_within_k_hops_through_cypher() {
    let (store, ids) = fixture();
    let adj = knows(&store, &ids);
    let index = |id: NodeId| ids.iter().position(|&x| x == id).expect("a Person");
    for k in [2, 3] {
        let pairs = sample(
            &store,
            &format!("strategy: 'khop', k: {k}, count: 30, seed: {SEED}, label: 'Person', edgeType: 'KNOWS'"),
        );
        assert!(!pairs.is_empty());
        for (a, b) in pairs {
            let d = hops(&adj, index(a), index(b));
            assert!((2..=k).contains(&d), "k={k}: {d} hops");
        }
    }
}

#[test]
fn the_same_seed_gives_the_same_rows() {
    let (store, _) = fixture();
    let config = "strategy: 'degree', count: 25, seed: 3";
    assert_eq!(sample(&store, config), sample(&store, config));
    assert_ne!(
        sample(&store, config),
        sample(&store, "strategy: 'degree', count: 25, seed: 4")
    );
}

#[test]
fn the_label_and_edge_type_filters_decide_what_counts_as_an_edge() {
    let (store, ids) = fixture();
    let everyone: HashSet<NodeId> = ids.iter().copied().collect();
    let pairs = sample(
        &store,
        "count: 200, seed: 1, label: 'Person', edgeType: 'KNOWS'",
    );
    assert!(
        pairs
            .iter()
            .all(|(a, b)| everyone.contains(a) && everyone.contains(b)),
        "only Person nodes when the label filter says so"
    );
}

#[test]
fn a_bad_option_is_refused_with_what_to_pass_instead() {
    let (store, _) = fixture();
    let e = refusal(&store, "strategy: 'hard'");
    assert!(
        e.contains("unknown strategy `hard`") && e.contains("`khop`"),
        "{e}"
    );
    let e = refusal(&store, "count: 0");
    assert!(e.contains("`count` must be at least 1"), "{e}");
    let e = refusal(&store, "strategy: 'khop', k: 1");
    assert!(
        e.contains("`k` must be at least 2") && e.contains("edges"),
        "{e}"
    );
    let e = refusal(&store, "cnt: 10");
    assert!(e.contains("cnt"), "an unknown key is named: {e}");
}

#[test]
fn the_default_is_a_hundred_random_negatives() {
    let (store, _) = fixture();
    assert_eq!(sample(&store, "seed: 1").len(), 100);
}
