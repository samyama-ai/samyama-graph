//! `shortestPath` and `allShortestPaths` (#516).
//!
//! The search was rewritten from "enqueue every walk, carrying its whole path"
//! to "level BFS building a predecessor DAG, then backtrack". Two things about
//! the old version make the correctness tests here worth more than a timing
//! check:
//!
//! * for `allShortestPaths` it never populated the visited set, so it explored
//!   walks rather than paths — the results happened to come out right because
//!   a walk that revisits a node cannot have the shortest length, but nothing
//!   in the code said so;
//! * the new version returns *all* predecessors at the shortest distance,
//!   which is a different mechanism for producing the same answer, and
//!   "different mechanism, same answer" is what needs pinning.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn named(store: &mut GraphStore, name: &str) -> NodeId {
    let id = store.create_node("N");
    let _ = store.set_node_property(
        "default",
        id,
        "name".to_string(),
        PropertyValue::String(name.to_string()),
    );
    id
}

/// A diamond with two equal-length routes plus one longer detour:
///
/// ```text
///   A -> B -> D        (length 2)
///   A -> C -> D        (length 2)
///   A -> E -> F -> D   (length 3, never shortest)
/// ```
fn diamond() -> GraphStore {
    let mut store = GraphStore::new();
    let a = named(&mut store, "A");
    let b = named(&mut store, "B");
    let c = named(&mut store, "C");
    let d = named(&mut store, "D");
    let e = named(&mut store, "E");
    let f = named(&mut store, "F");
    for (from, to) in [(a, b), (b, d), (a, c), (c, d), (a, e), (e, f), (f, d)] {
        store.create_edge(from, to, "LINK").unwrap();
    }
    store
}

fn lengths(store: &GraphStore, cypher: &str) -> Vec<i64> {
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    batch
        .records
        .iter()
        .map(|r| match r.get("len") {
            Some(Value::Property(PropertyValue::Integer(n))) => *n,
            other => panic!("expected a length, got {other:?}"),
        })
        .collect()
}

#[test]
fn shortest_path_returns_one_path_of_the_minimum_length() {
    let store = diamond();
    let out = lengths(
        &store,
        "MATCH p = shortestPath((a:N)-[:LINK*]->(d:N)) WHERE a.name = \"A\" AND d.name = \"D\" \
         RETURN length(p) AS len",
    );
    assert_eq!(out, vec![2], "one path, and the short one: {out:?}");
}

#[test]
fn all_shortest_paths_returns_every_minimum_length_path() {
    let store = diamond();
    let out = lengths(
        &store,
        "MATCH p = allShortestPaths((a:N)-[:LINK*]->(d:N)) WHERE a.name = \"A\" AND d.name = \"D\" \
         RETURN length(p) AS len",
    );
    assert_eq!(out.len(), 2, "A->B->D and A->C->D: {out:?}");
    assert!(out.iter().all(|&n| n == 2), "the 3-hop detour is not shortest: {out:?}");
}

#[test]
fn the_longer_route_is_never_returned() {
    // The predecessor DAG must only record parents at the shortest distance.
    // Recording a parent at distance+1 would surface A->E->F->D here.
    let store = diamond();
    let out = lengths(
        &store,
        "MATCH p = allShortestPaths((a:N)-[:LINK*]->(d:N)) WHERE a.name = \"A\" AND d.name = \"D\" \
         RETURN length(p) AS len",
    );
    assert!(!out.contains(&3), "{out:?}");
}

#[test]
fn a_source_equal_to_the_target_is_a_zero_length_path() {
    let store = diamond();
    let out = lengths(
        &store,
        "MATCH p = shortestPath((a:N)-[:LINK*]->(b:N)) WHERE a.name = \"A\" AND b.name = \"A\" \
         RETURN length(p) AS len",
    );
    assert_eq!(out, vec![0]);
}

#[test]
fn an_unreachable_target_returns_nothing() {
    let mut store = diamond();
    let island = named(&mut store, "Z");
    let _ = island;
    let out = lengths(
        &store,
        "MATCH p = shortestPath((a:N)-[:LINK*]->(z:N)) WHERE a.name = \"A\" AND z.name = \"Z\" \
         RETURN length(p) AS len",
    );
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn direction_is_honoured() {
    // D cannot reach A following LINK forwards.
    let store = diamond();
    let forward = lengths(
        &store,
        "MATCH p = shortestPath((d:N)-[:LINK*]->(a:N)) WHERE d.name = \"D\" AND a.name = \"A\" \
         RETURN length(p) AS len",
    );
    assert!(forward.is_empty(), "{forward:?}");

    // Undirected, it can.
    let undirected = lengths(
        &store,
        "MATCH p = shortestPath((d:N)-[:LINK*]-(a:N)) WHERE d.name = \"D\" AND a.name = \"A\" \
         RETURN length(p) AS len",
    );
    assert_eq!(undirected, vec![2]);
}

#[test]
fn the_edge_type_filter_is_honoured() {
    let mut store = GraphStore::new();
    let a = named(&mut store, "A");
    let b = named(&mut store, "B");
    let c = named(&mut store, "C");
    // A short route of the wrong type, a longer one of the right type.
    store.create_edge(a, c, "OTHER").unwrap();
    store.create_edge(a, b, "LINK").unwrap();
    store.create_edge(b, c, "LINK").unwrap();

    let out = lengths(
        &store,
        "MATCH p = shortestPath((a:N)-[:LINK*]->(c:N)) WHERE a.name = \"A\" AND c.name = \"C\" \
         RETURN length(p) AS len",
    );
    assert_eq!(out, vec![2], "the 1-hop OTHER edge must not be followed: {out:?}");
}

#[test]
fn an_unknown_edge_type_finds_no_path() {
    let store = diamond();
    let out = lengths(
        &store,
        "MATCH p = shortestPath((a:N)-[:NO_SUCH_TYPE*]->(d:N)) WHERE a.name = \"A\" AND d.name = \"D\" \
         RETURN length(p) AS len",
    );
    assert!(out.is_empty(), "an unknown type must match nothing: {out:?}");
}

#[test]
fn all_shortest_paths_counts_every_route_through_a_wide_layer() {
    // Three parallel middles: A -> {M1,M2,M3} -> Z is three shortest paths.
    // A predecessor DAG that kept only the first parent would return one.
    let mut store = GraphStore::new();
    let a = named(&mut store, "A");
    let z = named(&mut store, "Z");
    for i in 0..3 {
        let m = named(&mut store, &format!("M{i}"));
        store.create_edge(a, m, "LINK").unwrap();
        store.create_edge(m, z, "LINK").unwrap();
    }
    let out = lengths(
        &store,
        "MATCH p = allShortestPaths((a:N)-[:LINK*]->(z:N)) WHERE a.name = \"A\" AND z.name = \"Z\" \
         RETURN length(p) AS len",
    );
    assert_eq!(out.len(), 3, "{out:?}");
    assert!(out.iter().all(|&n| n == 2));
}

#[test]
fn a_wide_graph_three_hops_apart_finishes_quickly() {
    // The shape that timed out: a high-degree graph where the endpoints are
    // three hops apart. Enumerating walks makes this ~degree³; the DAG makes
    // it one pass plus the paths returned.
    const N: usize = 4000;
    const DEGREE: usize = 30;
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..N)
        .map(|i| {
            let id = store.create_node("N");
            let _ = store.set_node_property(
                "default",
                id,
                "seq".to_string(),
                PropertyValue::Integer(i as i64),
            );
            id
        })
        .collect();
    for (i, &src) in ids.iter().enumerate() {
        for d in 0..DEGREE {
            let x = (i as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add((d as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9));
            let x = x ^ (x >> 31);
            let tgt = ids[(x % N as u64) as usize];
            if tgt != src {
                let _ = store.create_edge(src, tgt, "KNOWS");
            }
        }
        // Noise of a type the query does not want, as a real graph has.
        for d in 0..40 {
            let x = (i as u64).wrapping_mul(0x2545_F491_4F6C_DD1D).wrapping_add(d as u64);
            let tgt = ids[(x % N as u64) as usize];
            if tgt != src {
                let _ = store.create_edge(src, tgt, "NOISE");
            }
        }
    }

    // Anchored by inline properties, which is the form LDBC IC14 uses. The
    // `WHERE id(a) = …` form is 1000x slower because the planner does not use
    // the predicate to anchor the source — a separate defect, filed as #538,
    // and not what this test is about.
    for (label, cypher) in [
        (
            "shortestPath",
            "MATCH p = shortestPath((a:N {seq: 1})-[:KNOWS*]-(b:N {seq: 3500})) RETURN length(p) AS len",
        ),
        (
            "allShortestPaths",
            "MATCH p = allShortestPaths((a:N {seq: 1})-[:KNOWS*]-(b:N {seq: 3500})) RETURN length(p) AS len",
        ),
    ] {
        let started = std::time::Instant::now();
        let query = parse_query(cypher).unwrap();
        let batch = QueryExecutor::new(&store).execute(&query).unwrap();
        let elapsed = started.elapsed();

        assert!(!batch.records.is_empty(), "{label}: the graph is dense enough to be connected");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "{label} took {elapsed:?} on a 4,000-node graph — this is the shape that timed out"
        );
    }
}
