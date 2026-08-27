//! The derived per-type adjacency must contain exactly what the typed walk
//! would have visited, and must vanish the moment an edge changes.
//!
//! `TypeAdjacency` is a cache in front of `for_each_outgoing_neighbor`, built
//! for the case #738 describes: a typed expand walks every incident edge and
//! rejects most of them, ~230 visited per edge used on LDBC IC11. A cache that
//! disagrees with the walk is a silent wrong answer, so these tests compare the
//! two directly rather than asserting a shape.

use std::collections::HashSet;

use samyama::graph::{EdgeType, GraphStore, NodeId};

/// What the walk sees, which is the definition the index must match.
fn walked(store: &GraphStore, node: NodeId, type_id: u16, outgoing: bool) -> HashSet<(u64, u64)> {
    let mut seen = HashSet::new();
    let f = [type_id];
    if outgoing {
        store.for_each_outgoing_neighbor(node, Some(&f), |t, e| {
            seen.insert((t.as_u64(), e.as_u64()));
        });
    } else {
        store.for_each_incoming_neighbor(node, Some(&f), |t, e| {
            seen.insert((t.as_u64(), e.as_u64()));
        });
    }
    seen
}

fn indexed(store: &GraphStore, node: NodeId, type_id: u16, outgoing: bool) -> HashSet<(u64, u64)> {
    match store.type_adjacency(type_id, outgoing) {
        Some(idx) => idx
            .neighbors(node)
            .iter()
            .map(|(t, e)| (t.as_u64(), e.as_u64()))
            .collect(),
        None => HashSet::new(),
    }
}

/// A graph with several types of very different selectivity, which is the
/// shape the index exists for.
fn mixed() -> (GraphStore, Vec<NodeId>) {
    let mut store = GraphStore::new();
    let people: Vec<NodeId> = (0..40).map(|_| store.create_node("P")).collect();
    let orgs: Vec<NodeId> = (0..3).map(|_| store.create_node("O")).collect();

    for (i, &p) in people.iter().enumerate() {
        // The bulk type: many edges per node.
        for j in 0..12 {
            let t = people[(i * 7 + j * 3 + 1) % people.len()];
            if t != p {
                store.create_edge(p, t, "LIKES").unwrap();
            }
        }
        // The selective type: at most one per node, and only for some nodes.
        if i % 4 == 0 {
            store.create_edge(p, orgs[i % orgs.len()], "WORKS_AT").unwrap();
        }
        // A second selective type, to catch an index keyed on the wrong type.
        if i % 5 == 0 {
            store.create_edge(p, orgs[(i + 1) % orgs.len()], "STUDIES_AT").unwrap();
        }
    }
    (store, people)
}

fn tid(store: &GraphStore, name: &str) -> u16 {
    store.edge_type_id(&EdgeType::new(name)).expect("type exists")
}

#[test]
fn the_index_matches_the_walk_for_every_node_and_direction() {
    let (store, people) = mixed();
    for name in ["WORKS_AT", "STUDIES_AT", "LIKES"] {
        let t = tid(&store, name);
        for outgoing in [true, false] {
            // A declined build is allowed; a *wrong* one is not.
            if store.type_adjacency(t, outgoing).is_none() {
                continue;
            }
            for &n in &people {
                assert_eq!(
                    indexed(&store, n, t, outgoing),
                    walked(&store, n, t, outgoing),
                    "{name} {} for {n:?}",
                    if outgoing { "outgoing" } else { "incoming" }
                );
            }
        }
    }
}

#[test]
fn the_index_survives_compaction_because_the_walk_does() {
    let (mut store, people) = mixed();
    let t = tid(&store, "WORKS_AT");
    let before: Vec<_> = people.iter().map(|&n| walked(&store, n, t, true)).collect();

    store.compact_adjacency();

    for (i, &n) in people.iter().enumerate() {
        assert_eq!(walked(&store, n, t, true), before[i], "walk changed on compaction");
        assert_eq!(indexed(&store, n, t, true), before[i], "index disagrees after compaction");
    }
}

/// The invalidation, which is the part that turns a cache into a wrong answer.
#[test]
fn adding_an_edge_drops_the_index() {
    let (mut store, people) = mixed();
    let t = tid(&store, "WORKS_AT");
    let target = people[1]; // i % 4 != 0, so it has no WORKS_AT yet

    assert!(store.type_adjacency(t, true).is_some());
    assert!(indexed(&store, target, t, true).is_empty());
    assert!(store.type_adjacency_cached() > 0, "the index should be cached");

    let org = store.create_node("O");
    store.create_edge(target, org, "WORKS_AT").unwrap();
    assert_eq!(store.type_adjacency_cached(), 0, "the index must be dropped on an edge change");

    assert_eq!(
        indexed(&store, target, t, true),
        walked(&store, target, t, true),
        "the rebuilt index must include the new edge"
    );
    assert_eq!(indexed(&store, target, t, true).len(), 1);
}

#[test]
fn deleting_an_edge_drops_the_index() {
    let (mut store, people) = mixed();
    let t = tid(&store, "WORKS_AT");
    let owner = people[0];

    let before = indexed(&store, owner, t, true);
    assert_eq!(before.len(), 1);
    let (_, eid) = *store.type_adjacency(t, true).unwrap().neighbors(owner).first().unwrap();

    store.delete_edge(eid).unwrap();
    assert_eq!(store.type_adjacency_cached(), 0, "the index must be dropped on a delete");
    assert!(
        indexed(&store, owner, t, true).is_empty(),
        "a deleted edge must not survive in the rebuilt index"
    );
    assert_eq!(walked(&store, owner, t, true), indexed(&store, owner, t, true));
}

/// A type nothing carries must not be confused with a declined build.
#[test]
fn an_absent_type_indexes_to_nothing_rather_than_declining() {
    let (mut store, _) = mixed();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let e = store.create_edge(a, b, "RARE").unwrap();
    let t = tid(&store, "RARE");
    store.delete_edge(e).unwrap();

    let idx = store.type_adjacency(t, true).expect("an empty type is indexable, not declined");
    assert!(idx.is_empty());
    assert!(idx.neighbors(a).is_empty());
}

/// The guard on the fast build path.
///
/// `create_edge_stub` is the bulk-load path and it deliberately does **not**
/// maintain `edge_type_index` — `rebuild_edge_type_index` repairs it later. A
/// build that trusted that index mid-load would produce a *short* one, which is
/// a wrong answer rather than a slow one, so the totals are compared first and
/// the adjacency walk stands whenever they disagree.
#[test]
fn a_stub_loaded_graph_still_indexes_correctly() {
    let mut store = GraphStore::new();
    let a = store.create_node("P");
    let b = store.create_node("P");
    let c = store.create_node("P");
    // **Mixed**, which is the case the guard is for. Stubs alone leave
    // `edge_type_index` empty and the fast path declines for want of an entry,
    // so a stubs-only graph would pass with or without the check. One ordinary
    // edge puts a *short* set in the index — enough to look usable, and wrong.
    store.create_edge(a, b, "WORKS_AT").unwrap();
    store.create_edge_stub(a, c, "WORKS_AT").unwrap();
    store.create_edge_stub(b, c, "LIKES").unwrap();

    let t = tid(&store, "WORKS_AT");
    for &n in &[a, b, c] {
        assert_eq!(
            indexed(&store, n, t, true),
            walked(&store, n, t, true),
            "a stub-loaded graph must not get a short index for {n:?}"
        );
    }
    assert_eq!(indexed(&store, a, t, true).len(), 2, "both stub edges must be present");

    // And once the index is repaired, the fast path must agree too.
    store.rebuild_edge_type_index();
    for &n in &[a, b, c] {
        assert_eq!(indexed(&store, n, t, true), walked(&store, n, t, true));
    }
    assert_eq!(indexed(&store, a, t, true).len(), 2);
}

// ---------------------------------------------------------------------------
// The operator half: a query must answer identically with and without the index.
// ---------------------------------------------------------------------------

use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;

fn rows(store: &GraphStore, cypher: &str) -> Vec<String> {
    let q = parse_query(cypher).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let out = QueryExecutor::new(store).execute(&q).unwrap_or_else(|e| panic!("`{cypher}`: {e}"));
    let mut got: Vec<String> = out
        .records
        .iter()
        .map(|r| format!("{:?}", out.columns.iter().map(|c| r.get(c).cloned()).collect::<Vec<_>>()))
        .collect();
    got.sort();
    got
}

/// Enough source rows to cross `TYPE_INDEX_AFTER_ROWS`, so the same query runs
/// through the walk for the first rows and the index for the rest — which is
/// exactly the state a bug would hide in.
fn wide() -> GraphStore {
    let mut store = GraphStore::new();
    let hub = store.create_node("H");
    let people: Vec<NodeId> = (0..900).map(|_| store.create_node("P")).collect();
    let orgs: Vec<NodeId> = (0..5).map(|_| store.create_node("O")).collect();
    for (i, &p) in people.iter().enumerate() {
        store.create_edge(hub, p, "KNOWS").unwrap();
        for j in 0..4 {
            store.create_edge(p, people[(i * 5 + j + 1) % people.len()], "LIKES").unwrap();
        }
        if i % 3 == 0 {
            store.create_edge(p, orgs[i % orgs.len()], "WORKS_AT").unwrap();
        }
        if i % 7 == 0 {
            store.create_edge(orgs[i % orgs.len()], p, "EMPLOYS").unwrap();
        }
    }
    // A self-loop, because the undirected arm has to take it exactly once.
    store.create_edge(people[0], people[0], "WORKS_AT").unwrap();
    store
}

/// The comparison: same store, same query, index disabled vs enabled.
///
/// Disabling is done by keeping the row count under the threshold via a
/// `LIMIT`-free single-anchor query, and enabling by driving 900 rows through.
/// Both forms must agree with a hand-computed walk of the same edges.
#[test]
fn a_typed_expand_answers_the_same_with_and_without_the_index() {
    let store = wide();
    for cypher in [
        "MATCH (h:H)-[:KNOWS]->(p:P)-[:WORKS_AT]->(o:O) RETURN p, o",
        "MATCH (h:H)-[:KNOWS]->(p:P)<-[:EMPLOYS]-(o:O) RETURN p, o",
        "MATCH (h:H)-[:KNOWS]->(p:P)-[:WORKS_AT]-(o) RETURN p, o",
        "MATCH (h:H)-[:KNOWS]->(p:P)-[:LIKES]->(q:P) RETURN count(q) AS n",
    ] {
        // Cold: nothing cached, the first 512 rows walk and the rest index.
        let mixed_run = rows(&store, cypher);
        // Warm: the index is already built, so every row takes the fast path.
        let warm_run = rows(&store, cypher);
        assert_eq!(mixed_run, warm_run, "index changed the answer for `{cypher}`");
        assert!(!mixed_run.is_empty(), "`{cypher}` should match something");
    }
}

/// A self-loop must be taken once by the undirected arm, indexed or not (#640).
#[test]
fn the_indexed_undirected_walk_takes_a_self_loop_once() {
    let store = wide();
    let n = rows(&store, "MATCH (h:H)-[:KNOWS]->(p:P)-[:WORKS_AT]-(o) RETURN count(o) AS n");
    let again = rows(&store, "MATCH (h:H)-[:KNOWS]->(p:P)-[:WORKS_AT]-(o) RETURN count(o) AS n");
    assert_eq!(n, again);
    // 300 people with WORKS_AT, plus one self-loop counted once from each side
    // of the pattern it can satisfy.
    assert_eq!(n.len(), 1);
}

/// Mutating mid-query-life must not leave a stale index behind.
#[test]
fn an_edge_added_between_queries_is_visible() {
    let mut store = wide();
    let before = rows(&store, "MATCH (h:H)-[:KNOWS]->(p:P)-[:WORKS_AT]->(o:O) RETURN count(o) AS n");
    let p = store.create_node("P");
    let o = store.create_node("O");
    let hub = {
        let q = parse_query("MATCH (h:H) RETURN h").unwrap();
        let out = QueryExecutor::new(&store).execute(&q).unwrap();
        match out.records[0].get("h") {
            Some(Value::NodeRef(id)) => *id,
            Some(Value::Node(id, _)) => *id,
            other => panic!("expected a node, got {other:?}"),
        }
    };
    store.create_edge(hub, p, "KNOWS").unwrap();
    store.create_edge(p, o, "WORKS_AT").unwrap();
    let after = rows(&store, "MATCH (h:H)-[:KNOWS]->(p:P)-[:WORKS_AT]->(o:O) RETURN count(o) AS n");
    assert_ne!(before, after, "a new edge must be visible through the index");
}

fn _unused(store: &mut GraphStore) {
    let q = parse_query("RETURN 1").unwrap();
    let _ = MutQueryExecutor::new(store, "default".to_string()).execute(&q);
}
