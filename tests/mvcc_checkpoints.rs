//! Checkpoints, diff and rollback over the MVCC undo log (#2093).
//!
//! The contract an agent harness relies on: take a checkpoint, let a turn
//! write whatever it writes, then either keep it or put the graph back exactly
//! as it was. "Exactly" is checked by fingerprinting every observable part of
//! the store -- nodes, labels, properties, relationships, both adjacency
//! directions, the label and type indexes, the counts -- before and after.
//!
//! No wall-clock assertion: the issue's "< 5 ms" is a hypothesis for the
//! benchmark (`benches/checkpoint_rollback_benchmark.rs`), not a property of
//! the code.

use std::collections::{BTreeMap, BTreeSet};

use samyama::graph::{
    EdgeId, EdgeType, EntityRef, GraphDiff, GraphError, GraphStore, IsolationLevel, Label,
    LabelChange, NodeId, PropertyChange, PropertyValue,
};

const T: &str = "default";

/// Properties as sorted `(key, Debug of value)` pairs.
type Props = Vec<(String, String)>;
/// Labels and properties of a node.
type NodePrint = (Vec<String>, Props);
/// Source, target, type and properties of a relationship.
type EdgePrint = (u64, u64, String, Props);

/// Everything a reader can observe, in a form two stores can be compared by.
#[derive(Debug, PartialEq)]
struct Fingerprint {
    nodes: BTreeMap<u64, NodePrint>,
    edges: BTreeMap<u64, EdgePrint>,
    outgoing: BTreeMap<u64, Vec<(u64, u64)>>,
    incoming: BTreeMap<u64, Vec<(u64, u64)>>,
    by_label: BTreeMap<String, Vec<u64>>,
    by_type: BTreeMap<String, Vec<u64>>,
    node_count: usize,
    edge_count: usize,
}

fn props(map: &samyama::graph::PropertyMap) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = map
        .iter()
        .map(|(k, v)| (k.clone(), format!("{v:?}")))
        .collect();
    v.sort();
    v
}

const LABELS: [&str; 4] = ["A", "B", "C", "D"];
const TYPES: [&str; 3] = ["R", "S", "U"];

fn fingerprint(store: &GraphStore) -> Fingerprint {
    let mut nodes = BTreeMap::new();
    let mut outgoing = BTreeMap::new();
    let mut incoming = BTreeMap::new();
    for node in store.all_nodes() {
        let id = node.id;
        let mut labels: Vec<String> = node.labels.iter().map(|l| l.as_str().to_string()).collect();
        labels.sort();
        let full = store.node_properties_full(id);
        nodes.insert(id.as_u64(), (labels, props(&full)));
        let mut out: Vec<(u64, u64)> = store
            .get_outgoing_edge_targets(id)
            .iter()
            .map(|(e, _, t, _)| (t.as_u64(), e.as_u64()))
            .collect();
        out.sort();
        outgoing.insert(id.as_u64(), out);
        let mut inc: Vec<(u64, u64)> = store
            .get_incoming_edge_sources(id)
            .iter()
            .map(|(e, s, _, _)| (s.as_u64(), e.as_u64()))
            .collect();
        inc.sort();
        incoming.insert(id.as_u64(), inc);
    }
    let mut edges = BTreeMap::new();
    for edge in store.all_edges() {
        edges.insert(
            edge.id.as_u64(),
            (
                edge.source.as_u64(),
                edge.target.as_u64(),
                edge.edge_type.as_str().to_string(),
                props(&edge.properties),
            ),
        );
    }
    let mut by_label = BTreeMap::new();
    for label in LABELS {
        let mut ids: Vec<u64> = store
            .get_nodes_by_label(&Label::new(label))
            .iter()
            .map(|n| n.id.as_u64())
            .collect();
        ids.sort();
        assert_eq!(
            ids.len(),
            store.label_node_count(&Label::new(label)),
            "label index count for {label}"
        );
        by_label.insert(label.to_string(), ids);
    }
    let mut by_type = BTreeMap::new();
    for ty in TYPES {
        let mut ids: Vec<u64> = store
            .get_edges_by_type(&EdgeType::new(ty))
            .iter()
            .map(|e| e.id.as_u64())
            .collect();
        ids.sort();
        by_type.insert(ty.to_string(), ids);
    }
    Fingerprint {
        nodes,
        edges,
        outgoing,
        incoming,
        by_label,
        by_type,
        node_count: store.node_count(),
        edge_count: store.edge_count(),
    }
}

/// A deterministic generator, so a failure reproduces.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn live_nodes(store: &GraphStore) -> Vec<NodeId> {
    let mut ids: Vec<NodeId> = store.all_nodes().iter().map(|n| n.id).collect();
    ids.sort();
    ids
}

fn live_edges(store: &GraphStore) -> Vec<EdgeId> {
    let mut ids: Vec<EdgeId> = store.all_edges().iter().map(|e| e.id).collect();
    ids.sort();
    ids
}

fn value(rng: &mut Lcg) -> PropertyValue {
    match rng.below(3) {
        0 => PropertyValue::Integer(rng.below(1000) as i64),
        1 => PropertyValue::String(format!("s{}", rng.below(1000))),
        _ => PropertyValue::Boolean(rng.below(2) == 0),
    }
}

/// A base graph of `nodes` nodes and `edges` relationships with properties.
fn base_graph(rng: &mut Lcg, nodes: usize, edges: usize) -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::new();
    for i in 0..nodes {
        let id = store.create_node(LABELS[i % LABELS.len()]);
        store
            .set_node_property(T, id, "k0", PropertyValue::Integer(i as i64))
            .unwrap();
        store
            .set_node_property(T, id, format!("k{}", 1 + rng.below(3)), value(rng))
            .unwrap();
        ids.push(id);
    }
    for _ in 0..edges {
        let (a, b) = (ids[rng.below(ids.len())], ids[rng.below(ids.len())]);
        let e = store
            .create_edge(a, b, TYPES[rng.below(TYPES.len())])
            .unwrap();
        store.set_edge_property(e, "w", value(rng)).unwrap();
    }
    store
}

/// One random mutation of any kind the issue names: node, relationship,
/// property and label writes, creations and deletions. Returns whether it
/// changed anything.
fn mutate(store: &mut GraphStore, rng: &mut Lcg) -> bool {
    let nodes = live_nodes(store);
    let edges = live_edges(store);
    let key = format!("k{}", rng.below(4));
    match rng.below(10) {
        0 => {
            let id = store.create_node(LABELS[rng.below(LABELS.len())]);
            store.set_node_property(T, id, "k0", value(rng)).unwrap();
            true
        }
        1 | 2 if !nodes.is_empty() => {
            let id = nodes[rng.below(nodes.len())];
            store.set_node_property(T, id, key, value(rng)).unwrap();
            true
        }
        3 if !nodes.is_empty() => {
            store.remove_node_property(nodes[rng.below(nodes.len())], &key);
            true
        }
        4 if !nodes.is_empty() => {
            let id = nodes[rng.below(nodes.len())];
            let label = LABELS[rng.below(LABELS.len())];
            if rng.below(2) == 0 {
                store.add_label_to_node(T, id, label).unwrap();
            } else {
                store
                    .remove_label_from_node(id, &Label::new(label))
                    .unwrap();
            }
            true
        }
        5 | 6 if !nodes.is_empty() => {
            let (a, b) = (nodes[rng.below(nodes.len())], nodes[rng.below(nodes.len())]);
            let e = store
                .create_edge(a, b, TYPES[rng.below(TYPES.len())])
                .unwrap();
            if rng.below(2) == 0 {
                store.set_edge_property(e, "w", value(rng)).unwrap();
            }
            true
        }
        7 if !edges.is_empty() => {
            let e = edges[rng.below(edges.len())];
            if rng.below(3) == 0 {
                store.remove_edge_property(e, "w");
            } else {
                store
                    .set_edge_property(e, format!("w{}", rng.below(2)), value(rng))
                    .unwrap();
            }
            true
        }
        8 if !edges.is_empty() => {
            store.delete_edge(edges[rng.below(edges.len())]).unwrap();
            true
        }
        // Rarer than the rest, so the graph does not drain away.
        9 if nodes.len() > 10 && rng.below(3) == 0 => {
            store.delete_node(T, nodes[rng.below(nodes.len())]).unwrap();
            true
        }
        _ => false,
    }
}

#[test]
fn a_thousand_mutations_roll_back_to_an_identical_graph() {
    let mut rng = Lcg(2093);
    let mut store = base_graph(&mut rng, 60, 120);
    let before = fingerprint(&store);

    let v0 = store.checkpoint().unwrap();
    let mut applied = 0;
    while applied < 1000 {
        if mutate(&mut store, &mut rng) {
            applied += 1;
        }
    }
    assert_ne!(
        fingerprint(&store),
        before,
        "a thousand mutations changed nothing; the test exercises nothing"
    );

    let stats = store.rollback_to(v0).unwrap();
    assert!(stats.failures.is_empty(), "{:?}", stats.failures);
    assert!(
        stats.nodes_removed > 0 && stats.nodes_restored > 0,
        "{stats:?}"
    );
    assert!(
        stats.edges_removed > 0 && stats.edges_restored > 0,
        "{stats:?}"
    );
    assert!(
        stats.properties_reverted > 0 && stats.labels_reverted > 0,
        "{stats:?}"
    );
    assert_eq!(fingerprint(&store), before);
    assert!(
        store.pinned_checkpoints().is_empty(),
        "a rollback releases its checkpoint"
    );

    // And the store takes writes afterwards.
    let n = store.create_node("A");
    store
        .set_node_property(T, n, "k0", PropertyValue::Integer(1))
        .unwrap();
    assert_eq!(store.node_count(), before.node_count + 1);
}

#[test]
fn rollback_through_cypher_statements_is_identical_too() {
    // The path an embedded client takes: every statement is its own session
    // transaction at its own version, and `gc_auto` runs after each one.
    let engine = samyama::query::QueryEngine::new();
    let mut store = GraphStore::new();
    for q in [
        "CREATE (:A {name: 'a', n: 1})-[:R {w: 1}]->(:B {name: 'b'})",
        "CREATE (:C {name: 'c'})",
        "MATCH (a:A), (c:C) CREATE (a)-[:S]->(c)",
    ] {
        engine.execute_mut(q, &mut store, T).expect(q);
    }
    let before = fingerprint(&store);
    let v = store.checkpoint().unwrap();
    for q in [
        "MATCH (a:A) SET a.n = 2, a.extra = true",
        "MATCH (b:B) SET b:D",
        "MATCH (b:B) REMOVE b:B",
        "MATCH (c:C) DETACH DELETE c",
        "MATCH ()-[r:R]->() SET r.w = 9",
        "CREATE (:A {name: 'new'})",
        "MATCH (a:A {name: 'a'}), (n:A {name: 'new'}) CREATE (n)-[:U]->(a)",
    ] {
        engine.execute_mut(q, &mut store, T).expect(q);
    }
    let stats = store.rollback_to(v).unwrap();
    assert!(stats.failures.is_empty(), "{:?}", stats.failures);
    assert_eq!(fingerprint(&store), before);
}

#[test]
fn the_diff_lists_exactly_what_changed_in_a_fixed_order() {
    let mut store = GraphStore::new();
    let a = store.create_node("Person");
    store
        .set_node_property(T, a, "age", PropertyValue::Integer(1))
        .unwrap();
    let b = store.create_node("Person");
    store
        .set_node_property(T, b, "name", PropertyValue::String("b".into()))
        .unwrap();
    let e1 = store.create_edge(a, b, "KNOWS").unwrap();
    store
        .set_edge_property(e1, "w", PropertyValue::Integer(1))
        .unwrap();
    let e2 = store.create_edge(b, a, "KNOWS").unwrap();

    let v = store.checkpoint().unwrap();
    store
        .set_node_property(T, a, "city", PropertyValue::String("x".into()))
        .unwrap();
    store
        .set_node_property(T, a, "age", PropertyValue::Integer(2))
        .unwrap();
    store.remove_node_property(b, "name");
    store.add_label_to_node(T, a, "Admin").unwrap();
    store
        .remove_label_from_node(b, &Label::new("Person"))
        .unwrap();
    let c = store.create_node("Person");
    store
        .set_node_property(T, c, "age", PropertyValue::Integer(7))
        .unwrap();
    let e3 = store.create_edge(a, c, "KNOWS").unwrap();
    store
        .set_edge_property(e1, "w", PropertyValue::Integer(5))
        .unwrap();
    store.delete_edge(e2).unwrap();
    // Set and set back: not a change.
    store
        .set_node_property(T, b, "tmp", PropertyValue::Integer(1))
        .unwrap();
    store.remove_node_property(b, "tmp");
    // Created and deleted since: not in the diff either.
    let gone = store.create_node("Person");
    store.delete_node(T, gone).unwrap();

    let diff = store.diff_since(v).unwrap();
    let expected = GraphDiff {
        from_version: v,
        to_version: diff.to_version,
        nodes_added: vec![c],
        nodes_deleted: vec![],
        edges_added: vec![e3],
        edges_deleted: vec![e2],
        property_changes: vec![
            PropertyChange {
                entity: EntityRef::Node(a),
                key: "age".into(),
                old: Some(PropertyValue::Integer(1)),
                new: Some(PropertyValue::Integer(2)),
            },
            PropertyChange {
                entity: EntityRef::Node(a),
                key: "city".into(),
                old: None,
                new: Some(PropertyValue::String("x".into())),
            },
            PropertyChange {
                entity: EntityRef::Node(b),
                key: "name".into(),
                old: Some(PropertyValue::String("b".into())),
                new: None,
            },
            PropertyChange {
                entity: EntityRef::Edge(e1),
                key: "w".into(),
                old: Some(PropertyValue::Integer(1)),
                new: Some(PropertyValue::Integer(5)),
            },
        ],
        label_changes: vec![
            LabelChange {
                node: a,
                label: Label::new("Admin"),
                added: true,
            },
            LabelChange {
                node: b,
                label: Label::new("Person"),
                added: false,
            },
        ],
    };
    assert_eq!(diff, expected);
    assert!(diff.to_version > v);
    // Reading it changes nothing, so it reads the same twice.
    assert_eq!(store.diff_since(v).unwrap(), diff);
}

#[test]
fn a_fresh_checkpoint_has_an_empty_diff() {
    let mut store = base_graph(&mut Lcg(1), 5, 5);
    let v = store.checkpoint().unwrap();
    assert!(store.diff_since(v).unwrap().is_empty());
    let stats = store.rollback_to(v).unwrap();
    assert_eq!(
        stats.nodes_removed + stats.nodes_restored + stats.properties_reverted,
        0
    );
}

#[test]
fn garbage_collection_keeps_what_a_pinned_checkpoint_needs() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(1))
        .unwrap();
    let doomed = store.create_node("N");
    store
        .set_node_property(T, doomed, "p", PropertyValue::Integer(10))
        .unwrap();

    let v = store.checkpoint().unwrap();
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(2))
        .unwrap();
    store
        .set_node_property(T, doomed, "p", PropertyValue::Integer(11))
        .unwrap();
    store.delete_node(T, doomed).unwrap();
    store.begin_session_transaction().unwrap();
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(3))
        .unwrap();
    store.commit_session_transaction().unwrap();

    // Asked to drop everything: it must stop at the checkpoint.
    store.gc_versions(u64::MAX);

    let at_v = store
        .get_node_at_version(n, v)
        .expect("n existed at the checkpoint");
    assert_eq!(
        at_v.properties.get("p"),
        Some(&PropertyValue::Integer(1)),
        "gc pruned a pinned version"
    );
    let diff = store.diff_since(v).unwrap();
    assert_eq!(diff.nodes_deleted, vec![doomed]);
    assert_eq!(diff.property_changes.len(), 1);
    assert_eq!(
        diff.property_changes[0].old,
        Some(PropertyValue::Integer(1))
    );

    let stats = store.rollback_to(v).unwrap();
    assert!(stats.failures.is_empty(), "{:?}", stats.failures);
    // Through the reader's path: `get_node` is the arena node, without the
    // properties that live in columns.
    assert_eq!(
        store.node_properties_full(n).get("p"),
        Some(&PropertyValue::Integer(1))
    );
    // Restored with the value it had at the checkpoint, not the one it was
    // deleted with.
    assert_eq!(
        store.node_properties_full(doomed).get("p"),
        Some(&PropertyValue::Integer(10))
    );
}

#[test]
fn a_released_checkpoint_no_longer_holds_back_garbage_collection() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(1))
        .unwrap();
    let v = store.checkpoint().unwrap();
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(2))
        .unwrap();

    assert!(store.release_checkpoint(v));
    assert!(!store.release_checkpoint(v), "released twice");
    store.gc_versions(u64::MAX);
    // With nothing pinned, the version before the write is gone.
    assert_eq!(
        store
            .get_node_at_version(n, v)
            .and_then(|n| n.properties.get("p").cloned()),
        Some(PropertyValue::Integer(2)),
    );
    assert_eq!(store.diff_since(v), Err(GraphError::CheckpointNotPinned(v)));
    assert_eq!(
        store.rollback_to(v),
        Err(GraphError::CheckpointNotPinned(v))
    );
}

#[test]
fn no_id_is_reused_while_a_checkpoint_is_pinned() {
    let mut store = GraphStore::new();
    let keep = store.create_node("N");
    let freed = store.create_node("N");
    let freed_edge = store.create_edge(keep, keep, "R").unwrap();
    store.delete_edge(freed_edge).unwrap();
    store.delete_node(T, freed).unwrap();

    let v = store.checkpoint().unwrap();
    let victim = store.create_node("N");
    store.delete_node(T, victim).unwrap();
    let fresh = store.create_node("N");
    let fresh_edge = store.create_edge(keep, fresh, "R").unwrap();
    assert_ne!(fresh, freed);
    assert_ne!(
        fresh, victim,
        "a node deleted under a checkpoint gave its id to another"
    );
    assert_ne!(fresh_edge, freed_edge);

    store.rollback_to(v).unwrap();
    // Unpinned: the free list is used again.
    let reused = store.create_node("N");
    assert!(
        reused == freed || reused == victim || reused == fresh,
        "free ids are reused once nothing is pinned"
    );
}

#[test]
fn checkpoints_nest_and_each_one_rolls_back_to_its_own_state() {
    let mut rng = Lcg(7);
    let mut store = base_graph(&mut rng, 30, 40);
    let at_outer = fingerprint(&store);
    let outer = store.checkpoint().unwrap();
    for _ in 0..200 {
        mutate(&mut store, &mut rng);
    }
    let at_inner = fingerprint(&store);
    let inner = store.checkpoint().unwrap();
    for _ in 0..200 {
        mutate(&mut store, &mut rng);
    }

    // The inner one first, and the outer one is still valid afterwards.
    store.rollback_to(inner).unwrap();
    assert_eq!(fingerprint(&store), at_inner);
    assert_eq!(store.pinned_checkpoints(), vec![outer]);
    for _ in 0..100 {
        mutate(&mut store, &mut rng);
    }
    let stats = store.rollback_to(outer).unwrap();
    assert!(stats.failures.is_empty(), "{:?}", stats.failures);
    assert_eq!(fingerprint(&store), at_outer);
}

#[test]
fn rolling_back_to_the_outer_checkpoint_first_leaves_the_inner_one_consistent() {
    let mut rng = Lcg(11);
    let mut store = base_graph(&mut rng, 30, 40);
    let at_outer = fingerprint(&store);
    let outer = store.checkpoint().unwrap();
    for _ in 0..150 {
        mutate(&mut store, &mut rng);
    }
    let at_inner = fingerprint(&store);
    let inner = store.checkpoint().unwrap();
    for _ in 0..150 {
        mutate(&mut store, &mut rng);
    }

    store.rollback_to(outer).unwrap();
    assert_eq!(fingerprint(&store), at_outer);
    // The rollback was a write at a new version, so the inner checkpoint still
    // names a true past state, and can be gone back to.
    assert_eq!(store.pinned_checkpoints(), vec![inner]);
    let stats = store.rollback_to(inner).unwrap();
    assert!(stats.failures.is_empty(), "{:?}", stats.failures);
    assert_eq!(fingerprint(&store), at_inner);
}

#[test]
fn a_rollback_over_another_writers_commit_is_refused_with_the_diff() {
    let mut store = GraphStore::new();
    let mine = store.create_node("N");
    let theirs = store.create_node("N");
    store
        .set_node_property(T, theirs, "p", PropertyValue::Integer(1))
        .unwrap();

    let v = store.checkpoint().unwrap();
    store
        .set_node_property(T, mine, "p", PropertyValue::Integer(5))
        .unwrap();
    let txn = store.begin_transaction(IsolationLevel::ReadCommitted);
    store
        .txn_set_node_property(txn, theirs, "p", PropertyValue::Integer(2))
        .unwrap();
    store.commit_transaction(txn).unwrap();

    let before = fingerprint(&store);
    let err = store.rollback_to(v).unwrap_err();
    let GraphError::RollbackConflict(conflict) = &err else {
        panic!("expected a conflict, got {err:?}")
    };
    assert_eq!(conflict.checkpoint, v);
    assert_eq!(conflict.entities, vec![EntityRef::Node(theirs)]);
    assert_eq!(conflict.diff, store.diff_since(v).unwrap());
    let keys: BTreeSet<(EntityRef, String)> = conflict
        .diff
        .property_changes
        .iter()
        .map(|c| (c.entity, c.key.clone()))
        .collect();
    assert!(keys.contains(&(EntityRef::Node(mine), "p".to_string())));
    assert!(keys.contains(&(EntityRef::Node(theirs), "p".to_string())));
    assert!(err.to_string().contains("refused"), "{err}");

    // Nothing changed, and the checkpoint is still held.
    assert_eq!(fingerprint(&store), before);
    assert_eq!(store.pinned_checkpoints(), vec![v]);
}

#[test]
fn a_checkpoint_is_refused_inside_a_session_transaction() {
    let mut store = GraphStore::new();
    store.begin_session_transaction().unwrap();
    assert!(matches!(
        store.checkpoint(),
        Err(GraphError::CheckpointRefused(_))
    ));
    store.commit_session_transaction().unwrap();

    let v = store.checkpoint().unwrap();
    store.begin_session_transaction().unwrap();
    assert!(matches!(
        store.rollback_to(v),
        Err(GraphError::CheckpointRefused(_))
    ));
    store.rollback_session_transaction().unwrap();
    assert!(store.rollback_to(v).is_ok());
}

#[test]
fn a_session_rollback_leaves_nothing_for_the_checkpoint_to_undo() {
    let mut store = GraphStore::new();
    let n = store.create_node("N");
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(1))
        .unwrap();
    let v = store.checkpoint().unwrap();

    store.begin_session_transaction().unwrap();
    store
        .set_node_property(T, n, "p", PropertyValue::Integer(2))
        .unwrap();
    store.create_node("M");
    store.delete_node(T, n).unwrap();
    store.rollback_session_transaction().unwrap();

    assert!(
        store.diff_since(v).unwrap().is_empty(),
        "{:?}",
        store.diff_since(v)
    );
}

#[test]
fn an_unknown_checkpoint_is_an_error_not_a_guess() {
    let mut store = GraphStore::new();
    assert_eq!(
        store.diff_since(42),
        Err(GraphError::CheckpointNotPinned(42))
    );
    assert_eq!(
        store.rollback_to(42),
        Err(GraphError::CheckpointNotPinned(42))
    );
    assert!(!store.release_checkpoint(42));
}

#[test]
fn consecutive_checkpoints_pin_distinct_versions() {
    let mut store = GraphStore::new();
    let v1 = store.checkpoint().unwrap();
    // Nothing written in between, and still two versions: a checkpoint
    // advances the version, so each one is released on its own.
    let v2 = store.checkpoint().unwrap();
    assert!(v2 > v1);
    assert_eq!(store.pinned_checkpoints(), vec![v1, v2]);
    assert!(store.release_checkpoint(v1));
    assert_eq!(store.pinned_checkpoints(), vec![v2]);
    assert!(store.release_checkpoint(v2));
    assert!(store.pinned_checkpoints().is_empty());
}
