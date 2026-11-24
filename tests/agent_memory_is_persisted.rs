//! Agent memory reaches disk under the tenant that wrote it (#2241).
//!
//! `PlanExecutor` used to write its `(:AgentQuestion)-[:USED_TOOL]->(:AgentTool)`
//! telemetry straight into the `GraphStore`. Nothing persisted it, so it was
//! either lost on restart or saved by the next unrelated write, under that
//! write's tenant. It now goes through `PersistedWriter`, which does what the
//! RESP and HTTP query paths do and commits the run as one transaction.
//!
//! Every test ends in a real restart: the manager is dropped, a new one opens
//! the same directory and the tenant is recovered from RocksDB.
//!
//! The tests share the process-global degraded flag (`persistence::health`),
//! so they run one at a time, and each starts from a clear flag.

use samyama::agent::memory::{record_decision, DecisionInput};
use samyama::agent::{AgentError, AgentResult, PlanExecutor, Tool, ToolCall, ToolPlan};
use samyama::graph::{Edge, GraphStore, Node, PropertyValue};
use samyama::persistence::{
    health, PersistedWriteError, PersistedWriter, PersistenceManager, ResourceQuotas,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// A tokio mutex because the guard is held across `.await`.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = SERIAL.lock().await;
    health::reset_for_test();
    guard
}

const TENANT: &str = "tenant-a";

struct Echo;

#[async_trait::async_trait]
impl Tool for Echo {
    fn name(&self) -> &str {
        "echo"
    }
    fn description(&self) -> &str {
        "echo args"
    }
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({})
    }
    async fn execute(&self, args: serde_json::Value) -> AgentResult<serde_json::Value> {
        Ok(serde_json::json!({ "records": [args] }))
    }
}

fn open(dir: &std::path::Path, quotas: Option<ResourceQuotas>) -> Arc<PersistenceManager> {
    let pm = PersistenceManager::new(dir).expect("persistence");
    // `.ok()`: on a reopen the tenant may already be registered.
    pm.tenants()
        .create_tenant(TENANT.to_string(), TENANT.to_string(), quotas)
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
    // RocksDB holds an exclusive lock on the directory: every handle has to go
    // before the reopen, or this is not a restart.
    let pm =
        Arc::try_unwrap(pm).unwrap_or_else(|_| panic!("a handle to the manager is still alive"));
    drop(pm);
    let pm = open(dir, None);
    pm.recover(tenant).expect("recover")
}

fn executor(writer: PersistedWriter, tenant: &str) -> PlanExecutor {
    let mut tools: HashMap<String, Arc<dyn Tool>> = HashMap::new();
    tools.insert("echo".into(), Arc::new(Echo));
    PlanExecutor::new(tools, writer, tenant)
}

fn plan() -> ToolPlan {
    ToolPlan {
        calls: vec![
            ToolCall {
                tool: "echo".into(),
                args: serde_json::json!({ "n": 1 }),
                parallel_with_prev: false,
            },
            ToolCall {
                tool: "echo".into(),
                args: serde_json::json!({ "n": 2 }),
                parallel_with_prev: false,
            },
        ],
    }
}

fn labelled<'a>(nodes: &'a [Node], label: &str) -> Vec<&'a Node> {
    nodes
        .iter()
        .filter(|n| n.labels.iter().any(|l| l.as_str() == label))
        .collect()
}

#[tokio::test]
async fn plan_executor_telemetry_survives_a_restart_under_its_own_tenant() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path(), None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let exec = executor(
        PersistedWriter::new(store.clone(), Some(pm.clone())),
        TENANT,
    );

    let run = exec.execute("what is echoed?", &plan()).await.expect("run");
    assert!(
        store.read().await.write_log_enabled(),
        "the writer did not journal, so nothing was persisted"
    );
    drop(exec);

    // Not under `default`, which is where a write that drained the log next
    // would have put it.
    let (default_nodes, default_edges) = {
        let (n, e) = pm.recover("default").expect("recover default");
        (n.len(), e.len())
    };
    assert_eq!(
        (default_nodes, default_edges),
        (0, 0),
        "telemetry persisted under the default tenant"
    );

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    let questions = labelled(&nodes, "AgentQuestion");
    assert_eq!(questions.len(), 1, "{nodes:?}");
    assert_eq!(
        questions[0].properties.get("qid"),
        Some(&PropertyValue::String(run.question_id.clone()))
    );
    assert_eq!(
        questions[0].properties.get("text"),
        Some(&PropertyValue::String("what is echoed?".into()))
    );
    let tools = labelled(&nodes, "AgentTool");
    assert_eq!(tools.len(), 1, "{nodes:?}");
    assert_eq!(
        tools[0].properties.get("tid"),
        Some(&PropertyValue::String("echo".into()))
    );

    assert_eq!(edges.len(), 2, "{edges:?}");
    let mut slots: Vec<i64> = edges
        .iter()
        .map(|e| {
            assert_eq!(e.edge_type.as_str(), "USED_TOOL");
            assert_eq!((e.source, e.target), (questions[0].id, tools[0].id));
            assert!(e.properties.contains_key("latency_ms"), "{e:?}");
            match e.properties.get("slot") {
                Some(PropertyValue::Integer(i)) => *i,
                other => panic!("slot is {other:?}"),
            }
        })
        .collect();
    slots.sort_unstable();
    assert_eq!(slots, vec![0, 1]);
}

/// The other half of the old bug: rows left in the log were saved by the next
/// write that drained it, under that write's tenant. The telemetry write now
/// drains its own, so the next write persists only itself.
#[tokio::test]
async fn the_next_write_does_not_carry_telemetry_into_its_tenant() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path(), None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let writer = PersistedWriter::new(store.clone(), Some(pm.clone()));
    executor(writer.clone(), TENANT)
        .execute("q", &plan())
        .await
        .expect("run");

    writer
        .write("default", |store| {
            store.admit_node()?;
            store.create_node("Unrelated");
            Ok::<_, samyama::graph::GraphError>(())
        })
        .await
        .expect("unrelated write");
    drop(writer);

    let (nodes, edges) = restart(dir.path(), pm, "default");
    assert_eq!(nodes.len(), 1, "{nodes:?}");
    assert!(labelled(&nodes, "Unrelated").len() == 1, "{nodes:?}");
    assert!(edges.is_empty(), "{edges:?}");
}

#[tokio::test]
async fn telemetry_that_cannot_be_persisted_is_an_error_and_leaves_nothing() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path(), None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let exec = executor(
        PersistedWriter::new(store.clone(), Some(pm.clone())),
        TENANT,
    );

    pm.fail_next_apply_for_test();
    let err = exec.execute("lost?", &plan()).await.unwrap_err();
    assert!(
        matches!(err, AgentError::ExecutionError(ref m) if m.contains("injected failure")),
        "{err:?}"
    );
    drop(exec);
    {
        let g = store.read().await;
        assert_eq!(
            (g.node_count(), g.edge_count()),
            (0, 0),
            "the rolled-back run is still in memory"
        );
        assert!(
            g.session_transaction_version().is_none(),
            "the transaction was left open"
        );
    }
    // Rolled back and the disk repaired, so memory and disk agree: as for a
    // refused COMMIT, there is no divergence to stop writes over.
    assert!(!health::is_degraded(), "{:?}", health::reason());

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    assert!(nodes.is_empty() && edges.is_empty(), "{nodes:?} {edges:?}");
}

/// Admission is the tenant's quota, checked before a row exists (#1483): a run
/// whose telemetry does not fit is refused whole.
#[tokio::test]
async fn telemetry_over_the_tenant_quota_is_refused_whole() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    // A Question and a Tool are two nodes.
    let quotas = ResourceQuotas {
        max_nodes: Some(1),
        ..ResourceQuotas::default()
    };
    let pm = open(dir.path(), Some(quotas));
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let exec = executor(
        PersistedWriter::new(store.clone(), Some(pm.clone())),
        TENANT,
    );

    let err = exec.execute("too big", &plan()).await.unwrap_err();
    assert!(
        matches!(err, AgentError::ExecutionError(ref m) if m.to_lowercase().contains("quota")),
        "{err:?}"
    );
    drop(exec);
    assert_eq!(
        store.read().await.node_count(),
        0,
        "the Question created before the refusal was kept"
    );
    assert!(!health::is_degraded(), "{:?}", health::reason());

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    assert!(nodes.is_empty() && edges.is_empty(), "{nodes:?} {edges:?}");
}

#[tokio::test]
async fn a_degraded_process_refuses_memory_writes_before_they_start() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path(), None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let writer = PersistedWriter::new(store.clone(), Some(pm));

    health::mark_degraded("No space left on device");
    let outcome = writer
        .write(TENANT, |store| {
            store.create_node("Never");
            Ok::<_, String>(())
        })
        .await;
    health::reset_for_test();

    match outcome {
        Err(PersistedWriteError::Refused(m)) => assert!(m.contains("No space left"), "{m}"),
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(store.read().await.node_count(), 0);
}

#[tokio::test]
async fn a_recorded_decision_survives_a_restart_under_its_tenant() {
    let _serial = serial().await;
    let dir = tempfile::tempdir().unwrap();
    let pm = open(dir.path(), None);
    let store = Arc::new(RwLock::new(GraphStore::new()));
    let writer = PersistedWriter::new(store.clone(), Some(pm.clone()));

    let evidence = writer
        .write(TENANT, |store| {
            store.admit_node()?;
            Ok::<_, samyama::graph::GraphError>(store.create_node("Observation"))
        })
        .await
        .expect("evidence");
    let step = record_decision(
        &writer,
        TENANT,
        DecisionInput {
            session_id: "session-a",
            trace_id: Some("trace-a"),
            parent_step_id: None,
            action: "search",
            rationale: "need evidence",
            evidence_node_ids: &[evidence],
            outcome: "found one",
        },
    )
    .await
    .expect("decision");
    drop(writer);

    let (nodes, edges) = restart(dir.path(), pm, TENANT);
    let decisions = labelled(&nodes, "AgentDecision");
    assert_eq!(decisions.len(), 1, "{nodes:?}");
    assert_eq!(decisions[0].id, step.node_id);
    assert_eq!(
        decisions[0].properties.get("trace_id"),
        Some(&PropertyValue::String("trace-a".into()))
    );
    assert_eq!(edges.len(), 1, "{edges:?}");
    assert_eq!(edges[0].edge_type.as_str(), "EVIDENCED_BY");
    assert_eq!((edges[0].source, edges[0].target), (step.node_id, evidence));
}
