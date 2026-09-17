//! A cyclic pattern's middle hop may prune, and must prune nothing real.
//!
//! `MATCH (a)-[:R]-(b)-[:R]-(c)-[:R]-(a)` builds a row for every `c` in `N(b)`
//! and then discards the ones with no edge back to `a`. On LDBC BI-17 at SF1
//! that is ~3.2M rows built to keep 387,573 triangles. The planner now tells
//! the middle expand which variable the pattern closes onto, and the expand
//! rejects a candidate during the adjacency walk when it does not neighbour
//! that node (#1082).
//!
//! The optimisation is a *filter*, so the only way it can be wrong is by
//! rejecting something. Every test here is therefore built the same way: run
//! the pattern, and compare against the answer the same graph gives with the
//! optimisation unable to apply. A test that only asserts a count would pass
//! on a plan that prunes everything and a plan that prunes nothing equally
//! well on the wrong fixture.

use samyama::graph::{GraphStore, NodeId};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn count(store: &GraphStore, cypher: &str) -> i64 {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse {cypher}: {e:?}"));
    let out = QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("execute {cypher}: {e:?}"));
    match out.records.first().and_then(|r| r.values().next()) {
        Some(Value::Property(samyama::graph::PropertyValue::Integer(i))) => *i,
        other => panic!("expected an integer count, got {other:?} for {cypher}"),
    }
}

fn rows(store: &GraphStore, cypher: &str) -> usize {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("parse {cypher}: {e:?}"));
    QueryExecutor::new(store)
        .execute(&q)
        .unwrap_or_else(|e| panic!("execute {cypher}: {e:?}"))
        .records
        .len()
}

/// Enough nodes that the type index builds — the prune only applies once it
/// has, so a small fixture would test the fallback path and report success.
///
/// A ring of `n` nodes with every node also joined to its second neighbour,
/// which makes `n` triangles and a large number of two-hop pairs that are
/// *not* triangles. Those are what the prune must reject and the answer must
/// not lose.
fn ring(n: usize) -> (GraphStore, Vec<NodeId>) {
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..n).map(|_| store.create_node("N")).collect();
    for i in 0..n {
        store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
        store.create_edge(ns[i], ns[(i + 2) % n], "R").unwrap();
    }
    (store, ns)
}

const TRI: &str = "MATCH (a:N)-[:R]-(b:N)-[:R]-(c:N)-[:R]-(a) RETURN count(a) AS n";

/// Every triangle in a ring-plus-chord graph, counted six times — once per
/// ordering of its three nodes, which is what an unordered pattern does.
#[test]
fn a_triangle_pattern_finds_every_triangle() {
    for n in [600usize, 900] {
        let (store, _) = ring(n);
        // i, i+1, i+2 is a triangle for every i: the two ring edges and the chord.
        assert_eq!(count(&store, TRI), (n * 6) as i64, "ring of {n}");
    }
}

/// The same answer whether or not the pattern can prune.
///
/// `(a)-[:R]-(b)-[:R]-(c)` with a separate closing `MATCH` is the same
/// question written so the look-ahead cannot see the close. If the two
/// disagree, the prune has removed a real answer.
#[test]
fn pruning_and_not_pruning_agree() {
    let (store, _) = ring(700);
    let pruned = count(&store, TRI);
    let unpruned = count(
        &store,
        "MATCH (a:N)-[:R]-(b:N)-[:R]-(c:N) WITH a, b, c MATCH (c)-[:R]-(a) RETURN count(a) AS n",
    );
    assert_eq!(pruned, unpruned, "the prune changed the answer");
    assert!(pruned > 0, "the fixture produced no triangles, so neither plan was tested");
}

/// A graph with no triangles at all must answer zero, not "whatever survived".
///
/// An even ring with only its ring edges has plenty of two-hop paths and no
/// closing edge for any of them, so a prune that is too *weak* still answers
/// zero here and a prune that is too strong cannot be distinguished. Its job
/// is to catch the opposite failure from the tests above: a plan that answers
/// a triangle where there is none.
#[test]
fn a_triangle_free_graph_answers_zero() {
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..800).map(|_| store.create_node("N")).collect();
    for i in 0..ns.len() {
        store.create_edge(ns[i], ns[(i + 1) % ns.len()], "R").unwrap();
    }
    assert_eq!(count(&store, TRI), 0);
    // ...and the two-hop prefix is not empty, so the query really did walk.
    assert!(rows(&store, "MATCH (a:N)-[:R]-(b:N)-[:R]-(c:N) RETURN a") > 1000);
}

/// The prune must not fire when the closing hop is a *different* edge type.
///
/// `(a)-[:R]-(b)-[:R]-(c)-[:S]-(a)` closes over `:S`, so membership of `N_R(a)`
/// is the wrong question and asking it would reject real answers. The planner
/// requires the two segments to share their types; this is the fixture that
/// notices if that condition is dropped.
#[test]
fn a_different_closing_type_is_not_pruned_against_the_wrong_index() {
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..700).map(|_| store.create_node("N")).collect();
    let n = ns.len();
    for i in 0..n {
        store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
        store.create_edge(ns[i], ns[(i + 2) % n], "R").unwrap();
        // The closing type joins i to i+2 only, and only as `:S`.
        store.create_edge(ns[i], ns[(i + 2) % n], "S").unwrap();
    }
    let mixed = count(
        &store,
        "MATCH (a:N)-[:R]-(b:N)-[:R]-(c:N)-[:S]-(a) RETURN count(a) AS n",
    );
    let same = count(
        &store,
        "MATCH (a:N)-[:R]-(b:N)-[:R]-(c:N) WITH a, b, c MATCH (c)-[:S]-(a) RETURN count(a) AS n",
    );
    assert_eq!(mixed, same, "the mixed-type close lost answers");
    assert!(mixed > 0, "no `:S` close matched, so nothing was tested");
}

/// A directed close must not be pruned by an undirected membership test.
///
/// `N(a)` in the index is two lists, out and in. "Neighbours of `a`" is their
/// union, which is the right answer for `-[:R]-` and too permissive for
/// `-[:R]->`; being too permissive cannot lose a row, but the planner refuses
/// the case anyway rather than relying on that. This asserts the answer, which
/// is what matters either way.
#[test]
fn a_directed_close_answers_the_same_as_a_separate_match() {
    // A *directed* triangle needs the third edge pointing back, which the
    // undirected `ring` fixture does not have: its chords run i -> i+2, so
    // i -> i+1 -> i+2 never closes. The first version of this test used it and
    // compared 0 against 0 — two plans agreeing that there is nothing to find.
    // The "nothing was tested" guard below is what noticed.
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..700).map(|_| store.create_node("N")).collect();
    let n = ns.len();
    for i in 0..n {
        store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
        store.create_edge(ns[(i + 2) % n], ns[i], "R").unwrap();
    }
    let a = count(
        &store,
        "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)-[:R]->(a) RETURN count(a) AS n",
    );
    let b = count(
        &store,
        "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) WITH a, b, c MATCH (c)-[:R]->(a) RETURN count(a) AS n",
    );
    assert_eq!(a, b);
    assert!(a > 0, "no directed triangle matched, so nothing was tested");
}

/// A directed close must search the *right* half of the index.
///
/// `(c)-[:R]->(a)` is proved by `a`'s **incoming** list, not its outgoing one.
/// A graph where the two disagree is what makes that assertable: here every
/// node has out-neighbours it is not an in-neighbour of, so searching the wrong
/// list rejects rows that should survive and the count comes out short.
///
/// Verified by mutation: swapping the two arms of the direction mapping in
/// `ExpandOperator` turns this test red while every other test in the file
/// stays green. The undirected tests cannot catch it — they search both lists,
/// so the two arms are indistinguishable there.
#[test]
fn a_directed_close_searches_the_incoming_list_not_the_outgoing_one() {
    let mut store = GraphStore::new();
    let n = 700usize;
    let ns: Vec<NodeId> = (0..n).map(|_| store.create_node("N")).collect();
    // Directed triangles i -> i+1 -> i+2 -> i, and nothing symmetric: the
    // out-neighbourhood and in-neighbourhood of every node are disjoint.
    for i in 0..n {
        store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
        store.create_edge(ns[(i + 2) % n], ns[i], "R").unwrap();
    }
    let pruned = count(
        &store,
        "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N)-[:R]->(a) RETURN count(a) AS n",
    );
    let unpruned = count(
        &store,
        "MATCH (a:N)-[:R]->(b:N)-[:R]->(c:N) WITH a, b, c MATCH (c)-[:R]->(a) RETURN count(a) AS n",
    );
    assert_eq!(pruned, unpruned, "the directed prune lost answers");
    assert!(pruned > 0, "no directed triangle matched, so nothing was tested");
    // And the graph really is asymmetric, or searching either list would do.
    let reversed = count(
        &store,
        "MATCH (a:N)<-[:R]-(b:N)<-[:R]-(c:N)<-[:R]-(a) RETURN count(a) AS n",
    );
    assert_eq!(reversed, pruned, "the reversed pattern is the same cycle read backwards");
}

/// Relationship isomorphism still holds: the three hops must be three edges.
///
/// A two-node graph joined by one edge has a two-hop walk `a-b-a` only if the
/// edge may be reused, which openCypher forbids within a clause. The prune
/// asks "does `a` neighbour `a`" for those rows and answers yes, so if it were
/// allowed to *replace* the closing hop rather than precede it, this would
/// start matching.
#[test]
fn the_three_hops_must_be_three_distinct_edges() {
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..700).map(|_| store.create_node("N")).collect();
    // A ring, so the type index builds, plus one isolated pair.
    for i in 0..ns.len() {
        store.create_edge(ns[i], ns[(i + 1) % ns.len()], "R").unwrap();
    }
    let x = store.create_node("X");
    let y = store.create_node("X");
    store.create_edge(x, y, "R").unwrap();
    assert_eq!(
        count(&store, "MATCH (a:X)-[:R]-(b:X)-[:R]-(c:X)-[:R]-(a) RETURN count(a) AS n"),
        0,
        "one edge cannot be walked three times"
    );
}

// ---------------------------------------------------------------------------
// The membership test walks the closing node's list with a cursor that only
// moves forward (#1082). That is valid because `TypeAdjacency` sorts each
// node's entries by `(target, edge)`, and it is wrong the moment something
// it is asked about arrives out of order or twice. These fixtures are the
// shapes where that happens: parallel edges (one target, several edge ids),
// self-loops (a node in its own list, and in both directions), and a close
// over both directions at once (two sorted lists consulted per candidate).
//
// Each asserts against the same graph answered without the prune, so a cursor
// that skipped a match fails here rather than silently returning fewer rows.
// ---------------------------------------------------------------------------

/// The unpruned answer to the same question: the close as a separate MATCH,
/// which the planner cannot fold into the walk.
fn unpruned(store: &GraphStore) -> i64 {
    count(
        store,
        "MATCH (a:N)-[:R]-(b:N)-[:R]-(c:N) WITH a, b, c MATCH (c)-[:R]-(a) RETURN count(a) AS n",
    )
}

#[test]
fn parallel_edges_between_the_same_pair_are_all_found() {
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..600).map(|_| store.create_node("N")).collect();
    let n = ns.len();
    for i in 0..n {
        store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
        store.create_edge(ns[i], ns[(i + 2) % n], "R").unwrap();
        // A second edge to the same target: the entry repeats, with a
        // different edge id, and a cursor that advances past the first would
        // lose every match through the second.
        store.create_edge(ns[i], ns[(i + 2) % n], "R").unwrap();
    }
    assert_eq!(count(&store, TRI), unpruned(&store), "parallel edges lost matches");
    assert!(count(&store, TRI) > 0, "the fixture produced no triangles");
}

#[test]
fn self_loops_do_not_move_the_cursor_past_a_real_neighbour() {
    // A self-loop puts a node in its own list, in both directions, which is
    // where a forward-only cursor can overshoot the neighbour that follows it.
    //
    // The reference here is the *same ring without the loops*, not the close
    // as a second MATCH: relationship isomorphism does not span two clauses, so
    // the split form lets the loop be reused as the closing edge and counts
    // 8,400 where the pattern counts 3,600. The invariant that does hold is
    // that a self-loop cannot complete a triangle of three distinct edges over
    // these nodes, so adding loops must not change the count.
    let ring_with = |loops: bool| {
        let mut store = GraphStore::new();
        let ns: Vec<NodeId> = (0..600).map(|_| store.create_node("N")).collect();
        let n = ns.len();
        for i in 0..n {
            if loops {
                store.create_edge(ns[i], ns[i], "R").unwrap();
            }
            store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
            store.create_edge(ns[i], ns[(i + 2) % n], "R").unwrap();
        }
        store
    };
    let with_loops = count(&ring_with(true), TRI);
    assert_eq!(with_loops, count(&ring_with(false), TRI), "self-loops changed the answer");
    assert!(with_loops > 0, "the fixture produced no triangles");
}

#[test]
fn a_close_over_both_directions_consults_both_lists() {
    // Every edge is created in one direction only, and the pattern is
    // undirected, so a triangle's closing edge may sit in either of the
    // closing node's two lists. Both cursors have to be maintained.
    let mut store = GraphStore::new();
    let ns: Vec<NodeId> = (0..600).map(|_| store.create_node("N")).collect();
    let n = ns.len();
    for i in 0..n {
        if i % 2 == 0 {
            store.create_edge(ns[i], ns[(i + 1) % n], "R").unwrap();
            store.create_edge(ns[(i + 2) % n], ns[i], "R").unwrap();
        } else {
            store.create_edge(ns[(i + 1) % n], ns[i], "R").unwrap();
            store.create_edge(ns[i], ns[(i + 2) % n], "R").unwrap();
        }
    }
    assert_eq!(count(&store, TRI), unpruned(&store), "a close over both lists lost matches");
    assert!(count(&store, TRI) > 0, "the fixture produced no triangles");
}
