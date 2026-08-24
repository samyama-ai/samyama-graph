//! Variable-length expansion answers the same questions after #520's rewrite.
//!
//! The traversal stopped materialising every incident edge with an owned
//! `EdgeType` and now filters on the interned type id inside the walk. That is
//! a change to *what gets skipped and when*, so the risks are all
//! correctness ones:
//!
//! * the type filter must still match exactly the types asked for, including
//!   a type the graph has never seen (matches nothing) and no filter at all
//!   (matches everything);
//! * direction must still be honoured, including the `Both` case that reads
//!   the outgoing and incoming adjacency separately;
//! * hop bounds, deduplication by node, and named-path reconstruction must be
//!   unchanged;
//! * output order must be unchanged, because discovery and emission are now
//!   separated by a level rather than interleaved.

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::{QueryExecutor, Value};
use samyama::query::parser::parse_query;

/// A → B → C → D in a line, plus a parallel `OTHER`-typed chain A → X → Y
/// and one reverse edge D → A, so direction and type both matter.
fn line_graph() -> GraphStore {
    let mut store = GraphStore::new();
    let mut named = |store: &mut GraphStore, name: &str| {
        let id = store.create_node("N");
        let _ = store.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let a = named(&mut store, "A");
    let b = named(&mut store, "B");
    let c = named(&mut store, "C");
    let d = named(&mut store, "D");
    let x = named(&mut store, "X");
    let y = named(&mut store, "Y");

    store.create_edge(a, b, "LINK").unwrap();
    store.create_edge(b, c, "LINK").unwrap();
    store.create_edge(c, d, "LINK").unwrap();
    store.create_edge(d, a, "BACK").unwrap();
    store.create_edge(a, x, "OTHER").unwrap();
    store.create_edge(x, y, "OTHER").unwrap();
    store
}

fn names(store: &GraphStore, cypher: &str) -> Vec<String> {
    let query = parse_query(cypher).expect("query should parse");
    let batch = QueryExecutor::new(store).execute(&query).expect("query should run");
    batch
        .records
        .iter()
        .map(|r| match r.get("n") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[test]
fn the_type_filter_excludes_other_types() {
    let store = line_graph();
    // Outgoing LINK only: A reaches B, C, D. Never X or Y, which are OTHER.
    let mut out = names(
        &store,
        "MATCH (a:N)-[:LINK*1..3]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    out.sort();
    assert_eq!(out, vec!["B", "C", "D"]);
}

#[test]
fn no_type_filter_follows_every_type() {
    let store = line_graph();
    let mut out = names(
        &store,
        "MATCH (a:N)-[*1..2]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    out.sort();
    // A -> B -> C, and A -> X -> Y.
    assert_eq!(out, vec!["B", "C", "X", "Y"]);
}

#[test]
fn a_type_the_graph_has_never_seen_matches_nothing() {
    // The interned-id lookup returns None for an unknown type. That must mean
    // "matches nothing", not "matches everything" — an empty filter is the
    // wildcard, so collapsing the two would silently follow every edge.
    let store = line_graph();
    let out = names(
        &store,
        "MATCH (a:N)-[:NO_SUCH_TYPE*1..3]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    assert!(out.is_empty(), "an unknown edge type matched something: {out:?}");
}

#[test]
fn direction_is_honoured() {
    let store = line_graph();

    // Outgoing from D: only the BACK edge to A.
    let out = names(
        &store,
        "MATCH (d:N)-[:BACK*1..1]->(f:N) WHERE d.name = \"D\" RETURN f.name AS n",
    );
    assert_eq!(out, vec!["A"]);

    // Incoming to D over LINK: C.
    let out = names(
        &store,
        "MATCH (d:N)<-[:LINK*1..1]-(f:N) WHERE d.name = \"D\" RETURN f.name AS n",
    );
    assert_eq!(out, vec!["C"]);
}

#[test]
fn an_undirected_pattern_reads_both_adjacencies() {
    // The `Both` case visits the outgoing and incoming adjacency in two
    // separate walks now, so a node reachable only backwards must still appear.
    let store = line_graph();
    let mut out = names(
        &store,
        "MATCH (c:N)-[:LINK*1..1]-(f:N) WHERE c.name = \"C\" RETURN f.name AS n",
    );
    out.sort();
    assert_eq!(out, vec!["B", "D"], "C links forward to D and backward from B");
}

#[test]
fn hop_bounds_are_respected() {
    let store = line_graph();

    let out = names(
        &store,
        "MATCH (a:N)-[:LINK*1..1]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    assert_eq!(out, vec!["B"]);

    let mut out = names(
        &store,
        "MATCH (a:N)-[:LINK*2..3]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    out.sort();
    assert_eq!(out, vec!["C", "D"], "the minimum hop count excludes B");
}

#[test]
fn a_node_reachable_by_two_paths_is_returned_twice() {
    // A diamond: two distinct paths reach D. The BFS deduplicates by node, so
    // D appears once whichever path found it first.
    let mut store = GraphStore::new();
    let mk = |s: &mut GraphStore, name: &str| {
        let id = s.create_node("N");
        let _ = s.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };
    let a = mk(&mut store, "A");
    let b = mk(&mut store, "B");
    let c = mk(&mut store, "C");
    let d = mk(&mut store, "D");
    store.create_edge(a, b, "E").unwrap();
    store.create_edge(a, c, "E").unwrap();
    store.create_edge(b, d, "E").unwrap();
    store.create_edge(c, d, "E").unwrap();

    // A diamond: A->B->D and A->C->D. Those are **two distinct paths** to `D`,
    // so a var-length pattern without `DISTINCT` yields `D` twice.
    //
    // This test previously asserted `D` exactly once, which is reachability
    // semantics rather than path semantics, and is the defect #710 describes.
    // The reference settles it: TCK `Match7[12] Variable length optional
    // relationships` expects
    //
    //     | (:A {num: 42}) | (:B {num: 46}) | (:B {num: 46}) | (:C) |
    //
    // with `(:B)` twice — once at one hop and once via a self-loop at two. That
    // scenario only passes with trail semantics, and TCK went 1082 -> 1083 when
    // this engine adopted them.
    let out = names(
        &store,
        "MATCH (a:N)-[:E*1..3]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    assert_eq!(
        out.iter().filter(|n| *n == "D").count(),
        2,
        "D is reached by A-B-D and A-C-D, which are two paths: {out:?}"
    );
    let mut sorted = out.clone();
    sorted.sort();
    assert_eq!(sorted, vec!["B", "C", "D", "D"]);

    // And `DISTINCT` collapses them, which is how every LDBC var-length query
    // is written and why the planner can keep the cheap walk for those (#710).
    let deduped = names(
        &store,
        "MATCH (a:N)-[:E*1..3]->(f:N) WHERE a.name = \"A\" RETURN DISTINCT f.name AS n",
    );
    assert_eq!(deduped, vec!["B", "C", "D"]);
}

#[test]
fn results_come_back_in_breadth_first_order() {
    // Discovery and emission are separated by a level now rather than
    // interleaved. The order must be unchanged: level by level, and within a
    // level in discovery order.
    let store = line_graph();
    let out = names(
        &store,
        "MATCH (a:N)-[:LINK*1..3]->(f:N) WHERE a.name = \"A\" RETURN f.name AS n",
    );
    assert_eq!(out, vec!["B", "C", "D"], "B before C before D");
}

#[test]
fn a_named_path_still_reconstructs() {
    let store = line_graph();
    let query = parse_query(
        "MATCH p = (a:N)-[:LINK*1..3]->(f:N) WHERE a.name = \"A\" AND f.name = \"D\" \
         RETURN length(p) AS n",
    )
    .expect("query should parse");
    let batch = QueryExecutor::new(&store).execute(&query).expect("query should run");
    assert_eq!(batch.records.len(), 1);
    match batch.records[0].get("n") {
        Some(Value::Property(PropertyValue::Integer(len))) => {
            assert_eq!(*len, 3, "A -> B -> C -> D is three hops")
        }
        other => panic!("expected a length, got {other:?}"),
    }
}

#[test]
fn a_node_with_no_matching_edges_returns_nothing() {
    let store = line_graph();
    let out = names(
        &store,
        "MATCH (y:N)-[:LINK*1..3]->(f:N) WHERE y.name = \"Y\" RETURN f.name AS n",
    );
    assert!(out.is_empty(), "{out:?}");
}

#[test]
fn a_large_traversal_reaches_the_same_set_as_a_hand_computed_bfs() {
    // The unit-sized graphs above cannot catch an error that only appears once
    // the frontier is large. This builds a graph whose reachable set is
    // computable outside the engine and compares.
    const N: usize = 2000;
    let mut store = GraphStore::new();
    let ids: Vec<_> = (0..N).map(|_| store.create_node("N")).collect();
    for (i, &src) in ids.iter().enumerate() {
        // Each node links to i+1 and i+7, modulo N, plus a decoy of another type.
        store.create_edge(src, ids[(i + 1) % N], "E").unwrap();
        store.create_edge(src, ids[(i + 7) % N], "E").unwrap();
        store.create_edge(src, ids[(i + 3) % N], "DECOY").unwrap();
    }

    // Hand BFS over the same edge set, three hops, following only "E".
    let mut reachable = std::collections::HashSet::new();
    let mut frontier = vec![0usize];
    let mut seen: std::collections::HashSet<usize> = [0].into_iter().collect();
    for _ in 0..3 {
        let mut next = Vec::new();
        for &cur in &frontier {
            for step in [1usize, 7] {
                let nb = (cur + step) % N;
                if seen.insert(nb) {
                    reachable.insert(nb);
                    next.push(nb);
                }
            }
        }
        frontier = next;
    }

    let query = parse_query("MATCH (a:N)-[:E*1..3]->(f:N) WHERE id(a) = 1 RETURN id(f) AS n")
        .expect("query should parse");
    let batch = QueryExecutor::new(&store).execute(&query).expect("query should run");

    // Compare the reachable **set**, not the row count. A node reachable by
    // more than one path yields more than one row — two paths to the same node
    // are two matches (#710, TCK `Match7[12]`) — and on this graph, where every
    // node has two outgoing `E` edges, most of them are. What the hand BFS
    // computes is which nodes are reachable, so that is what to compare.
    let mut got: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for r in &batch.records {
        if let Some(Value::Property(PropertyValue::Integer(id))) = r.get("n") {
            got.insert(*id as usize);
        }
    }
    // The hand BFS works in *indices*; the engine returns node **ids**. The
    // original assertion compared only `len()`, so this never had to be right —
    // comparing the values themselves exposed an off-by-one that was always
    // there.
    let mut expected: Vec<usize> = reachable
        .iter()
        .map(|&i| ids[i].as_u64() as usize)
        .collect();
    let mut actual: Vec<usize> = got.iter().copied().collect();
    expected.sort();
    actual.sort();
    assert_eq!(
        actual, expected,
        "engine reached {} distinct nodes, hand BFS reached {}",
        actual.len(),
        expected.len()
    );
    assert!(
        batch.records.len() >= got.len(),
        "rows ({}) cannot be fewer than distinct nodes ({})",
        batch.records.len(),
        got.len()
    );
}

#[test]
fn a_directed_variable_length_segment_survives_anchor_reversal() {
    // Anchor selection may start a path at its far end and traverse a
    // variable-length segment against the written direction (#328). That is
    // sound — `(a)-[:R*1..2]->(b)` read from `b` is `(b)<-[:R*1..2]-(a)`, the
    // same relation — but only if the direction is actually reversed rather
    // than dropped. Dropping it would make the pattern undirected and admit
    // rows that do not satisfy it.
    let mut store = GraphStore::new();
    let mk = |s: &mut GraphStore, label: &str, name: &str| {
        let id = s.create_node(label);
        let _ = s.set_node_property(
            "default",
            id,
            "name".to_string(),
            PropertyValue::String(name.to_string()),
        );
        id
    };

    // One Tag, many People — so the cheapest anchor is the tag end and the
    // planner is forced to traverse the KNOWS segment backwards.
    let tag = mk(&mut store, "Tag", "T");
    let people: Vec<_> = (0..200).map(|i| mk(&mut store, "Person", &format!("p{i}"))).collect();

    // Reachable: p0 -KNOWS-> p1 -WROTE-> post0 -HAS_TAG-> T
    let post0 = mk(&mut store, "Post", "post0");
    store.create_edge(people[0], people[1], "KNOWS").unwrap();
    store.create_edge(people[1], post0, "WROTE").unwrap();
    store.create_edge(post0, tag, "HAS_TAG").unwrap();

    // Decoy: p5 wrote post1, which carries the tag — but nobody KNOWS p5, so
    // no `(p)-[:KNOWS*1..2]->(p5)` exists and post1 must not appear.
    let post1 = mk(&mut store, "Post", "post1");
    store.create_edge(people[5], post1, "WROTE").unwrap();
    store.create_edge(post1, tag, "HAS_TAG").unwrap();

    let query = parse_query(
        "MATCH (p:Person)-[:KNOWS*1..2]->(f:Person)-[:WROTE]->(m:Post)-[:HAS_TAG]->(t:Tag) \
         WHERE t.name = \"T\" RETURN m.name AS n",
    )
    .unwrap();
    let batch = QueryExecutor::new(&store).execute(&query).unwrap();
    let mut got: Vec<String> = batch
        .records
        .iter()
        .map(|r| match r.get("n") {
            Some(Value::Property(PropertyValue::String(s))) => s.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    got.sort();
    got.dedup();
    assert_eq!(got, vec!["post0".to_string()], "the decoy has no inbound KNOWS path");
}
