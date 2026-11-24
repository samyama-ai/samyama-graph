//! `agent.memory.remember` / `recall` / `record_decision` / `inspect_trace`
//! (#2099, Samyama Smriti Phase 1).
//!
//! - remember, then recall what was remembered, from Cypher;
//! - a single-valued relation closes the fact it contradicts and keeps it:
//!   default recall sees the new fact, `temporal_as_of` sees the old one, and
//!   a multi-valued relation is never closed;
//! - decision traces recorded and inspected from Cypher;
//! - persistence: what `remember` writes survives a restart under the tenant
//!   that wrote it, and a write that cannot be persisted leaves nothing.
//!
//! The persistence tests share the process-global degraded flag
//! (`persistence::health`), so they run one at a time.

use samyama::agent::smriti::{
    remember, remember_in, EntityInput, EpisodeInput, RelationInput, RememberInput,
};
use samyama::graph::{Edge, GraphStore, Node, NodeId, PropertyValue};
use samyama::persistence::{health, PersistedWriter, PersistenceManager};
use samyama::query::{QueryEngine, RecordBatch};
use std::sync::Arc;
use tokio::sync::RwLock;

const T: &str = "default";

fn write(engine: &QueryEngine, s: &mut GraphStore, q: &str) -> RecordBatch {
    engine
        .execute_mut(q, s, T)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn read(engine: &QueryEngine, s: &GraphStore, q: &str) -> RecordBatch {
    engine.execute(q, s).unwrap_or_else(|e| panic!("{q}: {e}"))
}

fn int(batch: &RecordBatch, row: usize, column: &str) -> i64 {
    match batch.records[row].get(column).and_then(|v| v.as_property()) {
        Some(PropertyValue::Integer(i)) => *i,
        other => panic!("{column} was {other:?}"),
    }
}

fn text(batch: &RecordBatch, row: usize, column: &str) -> String {
    match batch.records[row].get(column).and_then(|v| v.as_property()) {
        Some(PropertyValue::String(s)) => s.clone(),
        other => panic!("{column} was {other:?}"),
    }
}

fn ints(batch: &RecordBatch, column: &str) -> Vec<i64> {
    (0..batch.records.len())
        .map(|row| int(batch, row, column))
        .collect()
}

/// `CALL agent.memory.remember` for "Alice lives in `city`", from `from`.
fn lives_in(city: &str, from: i64, single_valued: bool) -> String {
    format!(
        "CALL agent.memory.remember('s-{city}', \
           {{content: 'Alice said she lives in {city} now', observed_at: {from}, user_id: 'u1'}}, \
           [{{name: 'Alice', kind: 'Person'}}, '{city}'], \
           [{{subject: 'Alice', relation: 'lives_in', object: '{city}', \
              single_valued: {single_valued}}}], \
           null) \
         YIELD episode_id, superseded_edges \
         RETURN episode_id, superseded_edges"
    )
}

fn superseded(batch: &RecordBatch) -> Vec<i64> {
    match batch.records[0].get("superseded_edges") {
        Some(samyama::query::executor::Value::List(items)) => items
            .iter()
            .map(|v| match v.as_property() {
                Some(PropertyValue::Integer(i)) => *i,
                other => panic!("{other:?}"),
            })
            .collect(),
        Some(samyama::query::executor::Value::Property(PropertyValue::Array(items))) => items
            .iter()
            .map(|v| match v {
                PropertyValue::Integer(i) => *i,
                other => panic!("{other:?}"),
            })
            .collect(),
        other => panic!("superseded_edges was {other:?}"),
    }
}

/// The objects of the `lives_in` facts `recall` returns.
fn recalled_cities(engine: &QueryEngine, s: &GraphStore, as_of: &str) -> Vec<String> {
    let q = format!(
        "CALL agent.memory.recall('Alice lives in', null, null, {as_of}, 20, 0) \
         YIELD node, memory_tier \
         WHERE node.relation = 'lives_in' \
         RETURN node.object AS city, memory_tier ORDER BY city"
    );
    let r = read(engine, s, &q);
    (0..r.records.len())
        .map(|row| {
            assert_eq!(text(&r, row, "memory_tier"), "semantic");
            text(&r, row, "city")
        })
        .collect()
}

#[test]
fn what_is_remembered_is_recalled() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();
    let r = write(
        &engine,
        &mut s,
        "CALL agent.memory.remember('session-1', 'The patient is allergic to penicillin', \
           ['patient', 'penicillin'], \
           [{subject: 'patient', relation: 'allergic_to', object: 'penicillin'}], \
           [1.0, 0.0, 0.0]) \
         YIELD episode_id RETURN episode_id",
    );
    let episode = int(&r, 0, "episode_id");
    write(
        &engine,
        &mut s,
        "CALL agent.memory.remember('session-2', 'The weather was sunny', [], [], [0.0, 1.0, 0.0])",
    );

    // By text: the episode, its fact and both entities; the unrelated episode
    // matches nothing.
    let r = read(
        &engine,
        &s,
        "CALL agent.memory.recall('allergic penicillin', null, null, null, 10, 1) \
         YIELD node, score, memory_tier, provenance_episode \
         RETURN id(node) AS id, labels(node)[0] AS label, score, memory_tier, \
                id(provenance_episode) AS from",
    );
    let labels: Vec<String> = (0..r.records.len()).map(|i| text(&r, i, "label")).collect();
    assert!(labels.contains(&"AgentEpisode".to_string()), "{labels:?}");
    assert!(labels.contains(&"AgentFact".to_string()), "{labels:?}");
    assert!(labels.contains(&"AgentEntity".to_string()), "{labels:?}");
    assert!(ints(&r, "id").contains(&episode));
    // Every row comes from the one episode that said it.
    assert!(
        ints(&r, "from").iter().all(|&from| from == episode),
        "{r:?}"
    );

    // By embedding: the nearest episode first.
    let r = read(
        &engine,
        &s,
        "CALL agent.memory.recall(null, [0.9, 0.1, 0.0], null, null, 1, 0) \
         YIELD node, memory_tier RETURN id(node) AS id, memory_tier",
    );
    assert_eq!(ints(&r, "id"), vec![episode]);
    assert_eq!(text(&r, 0, "memory_tier"), "episodic");

    // A session filter hides the other session.
    let r = read(
        &engine,
        &s,
        "CALL agent.memory.recall(null, [0.9, 0.1, 0.0], 'session-2', null, 10, 0) \
         YIELD node RETURN node.session_id AS session",
    );
    assert!(
        r.records.len() == 1 && text(&r, 0, "session") == "session-2",
        "{r:?}"
    );
}

#[test]
fn superseding_keeps_history_and_as_of_returns_the_old_fact() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();
    let first = write(&engine, &mut s, &lives_in("Paris", 1_000, true));
    assert!(superseded(&first).is_empty());
    let second = write(&engine, &mut s, &lives_in("Berlin", 2_000, true));
    let closed = superseded(&second);
    assert_eq!(closed.len(), 1, "{second:?}");

    // The old fact is kept, closed at the new one's start.
    let r = read(
        &engine,
        &s,
        &format!(
            "MATCH (old:AgentFact) WHERE id(old) = {} \
             MATCH (new:AgentFact)-[:SUPERSEDED]->(old) \
             RETURN old.object AS was, old.valid_to AS until, old.status AS status, \
                    new.object AS now",
            closed[0]
        ),
    );
    assert_eq!(r.records.len(), 1);
    assert_eq!(text(&r, 0, "was"), "Paris");
    assert_eq!(int(&r, 0, "until"), 2_000);
    assert_eq!(text(&r, 0, "status"), "superseded");
    assert_eq!(text(&r, 0, "now"), "Berlin");

    assert_eq!(recalled_cities(&engine, &s, "null"), vec!["Berlin"]);
    assert_eq!(recalled_cities(&engine, &s, "1500"), vec!["Paris"]);
    assert_eq!(recalled_cities(&engine, &s, "2000"), vec!["Berlin"]);
    assert!(recalled_cities(&engine, &s, "500").is_empty());
}

#[test]
fn a_multi_valued_relation_is_never_superseded() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();
    write(&engine, &mut s, &lives_in("Paris", 1_000, false));
    let second = write(&engine, &mut s, &lives_in("Berlin", 2_000, false));
    assert!(superseded(&second).is_empty());
    assert_eq!(
        recalled_cities(&engine, &s, "null"),
        vec!["Berlin", "Paris"]
    );
}

#[test]
fn remember_is_refused_on_the_read_path_and_bad_input_writes_nothing() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();
    assert!(engine.execute(&lives_in("Paris", 1, true), &s).is_err());
    assert_eq!(s.node_count(), 0);

    // The relation names an unknown key: refused, nothing written.
    let err = engine
        .execute_mut(
            "CALL agent.memory.remember('s', 'x', [], [{subject: 'a', relation: 'r', obj: 'b'}])",
            &mut s,
            T,
        )
        .unwrap_err()
        .to_string();
    assert!(err.contains("unknown key `obj`"), "{err}");
    assert_eq!(s.node_count(), 0);
}

#[test]
fn decisions_are_recorded_and_inspected_from_cypher() {
    let engine = QueryEngine::new();
    let mut s = GraphStore::new();
    let evidence = s.create_node("Observation");
    let first = write(
        &engine,
        &mut s,
        &format!(
            "CALL agent.memory.record_decision('s1', null, 'search', 'need evidence', [{}], \
               'found one', 'trace-a') \
             YIELD decision_node_id, trace_id RETURN decision_node_id, trace_id",
            evidence.as_u64()
        ),
    );
    assert_eq!(text(&first, 0, "trace_id"), "trace-a");
    let parent = int(&first, 0, "decision_node_id");
    let second = write(
        &engine,
        &mut s,
        &format!(
            "CALL agent.memory.record_decision('s1', {parent}, 'answer', 'use it', [], 'answered') \
             YIELD decision_node_id, trace_id RETURN decision_node_id, trace_id"
        ),
    );
    // A step with a parent joins the parent's trace.
    assert_eq!(text(&second, 0, "trace_id"), "trace-a");
    let child = int(&second, 0, "decision_node_id");

    let r = read(
        &engine,
        &s,
        "CALL agent.memory.inspect_trace('trace-a') \
         YIELD step_order, decision_node, retrieved_context_nodes, tool_calls, causal_edges \
         RETURN step_order, id(decision_node) AS id, size(retrieved_context_nodes) AS context, \
                tool_calls[0].action AS action, size(causal_edges) AS causes, \
                causal_edges[0].to AS cause \
         ORDER BY step_order",
    );
    assert_eq!(ints(&r, "step_order"), vec![0, 1]);
    assert_eq!(ints(&r, "id"), vec![parent, child]);
    assert_eq!(ints(&r, "context"), vec![1, 0]);
    assert_eq!(text(&r, 0, "action"), "search");
    assert_eq!(ints(&r, "causes"), vec![0, 1]);
    assert_eq!(int(&r, 1, "cause"), parent);

    // The same trace by a Cypher path query.
    let r = read(
        &engine,
        &s,
        "MATCH (d:AgentDecision {trace_id: 'trace-a'})-[:CAUSED_BY]->(p:AgentDecision) \
         RETURN id(d) AS d, id(p) AS p",
    );
    assert_eq!((int(&r, 0, "d"), int(&r, 0, "p")), (child, parent));
}

// ------------------------------------------------------------ persistence --

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().await;
    health::reset_for_test();
    guard
}

const TENANT: &str = "tenant-a";

fn open(dir: &std::path::Path) -> Arc<PersistenceManager> {
    let pm = PersistenceManager::new(dir).expect("persistence");
    pm.tenants()
        .create_tenant(TENANT.to_string(), TENANT.to_string(), None)
        .ok();
    Arc::new(pm)
}

/// Drop the manager, reopen the directory and read back what `tenant` holds.
fn restart(
    dir: &std::path::Path,
    pm: Arc<PersistenceManager>,
    tenant: &str,
) -> (Vec<Node>, Vec<Edge>) {
    pm.checkpoint().unwrap();
    let pm =
        Arc::try_unwrap(pm).unwrap_or_else(|_| panic!("a handle to the manager is still alive"));
    drop(pm);
    let pm = open(dir);
    pm.recover(tenant).expect("recover")
}

fn labelled<'a>(nodes: &'a [Node], label: &str) -> Vec<&'a Node> {
    nodes
        .iter()
        .filter(|n| n.labels.iter().any(|l| l.as_str() == label))
        .collect()
}

fn moved(city: &str, from: i64) -> RememberInput {
    RememberInput {
        episode: EpisodeInput {
            session_id: "s1".into(),
            content: format!("Alice moved to {city}"),
            observed_at: Some(from),
            ..EpisodeInput::default()
        },
        entities: vec![EntityInput {
            name: "Alice".into(),
            ..EntityInput::default()
        }],
        relations: vec![RelationInput {
            subject: "Alice".into(),
            relation: "lives_in".into(),
            object: city.into(),
            single_valued: true,
            ..RelationInput::default()
        }],
        embedding: Some(vec![0.5, 0.5]),
    }
}

#[tokio::test]
async fn memory_survives_a_restart_under_its_own_tenant() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path());
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let writer = PersistedWriter::new(store.clone(), Some(pm.clone()));

    let paris = remember(&writer, TENANT, moved("Paris", 1_000))
        .await
        .expect("remember");
    // The Cypher procedure, run as an in-process write.
    let engine = QueryEngine::new();
    writer
        .write(TENANT, |s| {
            engine
                .execute_mut(
                    "CALL agent.memory.remember('s1', {content: 'Alice moved to Berlin', \
                       observed_at: 2000}, ['Alice'], \
                       [{subject: 'Alice', relation: 'lives_in', object: 'Berlin', \
                         single_valued: true}])",
                    s,
                    TENANT,
                )
                .map_err(|e| e.to_string())
        })
        .await
        .expect("remember from Cypher");
    drop(writer);

    let (default_nodes, _) = pm.recover("default").expect("recover default");
    assert!(default_nodes.is_empty(), "memory persisted under default");

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    let episodes = labelled(&nodes, "AgentEpisode");
    assert_eq!(episodes.len(), 2, "{nodes:?}");
    let first = episodes
        .iter()
        .find(|n| n.id == paris.episode_id)
        .expect("the first episode");
    assert_eq!(
        first.properties.get("embedding"),
        Some(&PropertyValue::Vector(vec![0.5, 0.5])),
        "the embedding was not persisted"
    );
    assert_eq!(labelled(&nodes, "AgentEntity").len(), 3, "{nodes:?}");

    // The superseding reached disk too: Paris is closed, Berlin is active.
    let facts = labelled(&nodes, "AgentFact");
    assert_eq!(facts.len(), 2, "{nodes:?}");
    let fact = |city: &str| {
        *facts
            .iter()
            .find(|n| n.properties.get("object") == Some(&PropertyValue::String(city.into())))
            .unwrap()
    };
    assert_eq!(
        fact("Paris").properties.get("valid_to"),
        Some(&PropertyValue::Integer(2_000))
    );
    assert_eq!(
        fact("Paris").properties.get("status"),
        Some(&PropertyValue::String("superseded".into()))
    );
    assert_eq!(fact("Berlin").properties.get("valid_to"), None);
    assert!(edges.iter().any(|e| e.edge_type.as_str() == "SUPERSEDED"
        && e.source == fact("Berlin").id
        && e.target == fact("Paris").id));
}

#[tokio::test]
async fn a_remember_that_cannot_be_persisted_leaves_nothing() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path());
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let writer = PersistedWriter::new(store.clone(), Some(pm.clone()));

    pm.fail_next_apply_for_test();
    let err = remember(&writer, TENANT, moved("Paris", 1_000))
        .await
        .unwrap_err();
    assert!(err.contains("injected failure"), "{err}");
    {
        let g = store.read().await;
        assert_eq!(
            (g.node_count(), g.edge_count()),
            (0, 0),
            "rolled back in memory"
        );
        assert!(g.session_transaction_version().is_none());
    }
    assert!(!health::is_degraded(), "{:?}", health::reason());
    drop(writer);

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    assert!(nodes.is_empty() && edges.is_empty(), "{nodes:?} {edges:?}");
}

/// The body is one unit: a remember that fails after its first rows -- here,
/// an entity standing for a node that is gone by the time it is linked --
/// leaves none of them, in memory or on disk.
#[tokio::test]
async fn a_remember_that_fails_part_way_leaves_nothing() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path());
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let writer = PersistedWriter::new(store.clone(), Some(pm.clone()));

    let err = writer
        .write(TENANT, |s| {
            remember_in(s, TENANT, &moved("Paris", 1_000), 5)?;
            let mut bad = moved("Rome", 2_000);
            bad.entities.push(EntityInput {
                name: "Ghost".into(),
                node_id: Some(NodeId::new(999_999)),
                ..EntityInput::default()
            });
            remember_in(s, TENANT, &bad, 6)
        })
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");
    assert_eq!(store.read().await.node_count(), 0);
    drop(writer);

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    assert!(nodes.is_empty() && edges.is_empty(), "{nodes:?} {edges:?}");
}
