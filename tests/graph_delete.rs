//! What `GRAPH.DELETE` actually deletes (#1110), and what the tenant counter says
//! afterwards (#1113).
//!
//! The handler used to parse the graph name, throw it away, call `GraphStore::clear()`
//! and return `OK`. Nothing on disk was touched, so a restart brought the graph back —
//! and `clear()` rewinds the id counters, so the writes that followed were persisted
//! over ids that still belonged to the deleted graph.

use samyama::graph::GraphStore;
use samyama::persistence::PersistenceManager;
use samyama::protocol::{CommandHandler, RespValue};
use std::sync::Arc;
use tokio::sync::RwLock;

fn cmd(parts: &[&str]) -> RespValue {
    RespValue::Array(
        parts
            .iter()
            .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
            .collect(),
    )
}

struct Server {
    _dir: tempfile::TempDir,
    pm: Arc<PersistenceManager>,
    handler: CommandHandler,
    store: Arc<RwLock<GraphStore>>,
}

impl Server {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let pm = Arc::new(PersistenceManager::new(dir.path()).unwrap());
        pm.tenants()
            .create_tenant("default".into(), "default".into(), None)
            .ok();
        Server {
            handler: CommandHandler::new(Some(Arc::clone(&pm))),
            store: Arc::new(RwLock::new(GraphStore::new())),
            pm,
            _dir: dir,
        }
    }

    async fn query(&self, q: &str) -> RespValue {
        self.handler
            .handle_command(&cmd(&["GRAPH.QUERY", "default", q]), &self.store)
            .await
    }

    async fn delete(&self, graph: &str) -> RespValue {
        self.handler
            .handle_command(&cmd(&["GRAPH.DELETE", graph]), &self.store)
            .await
    }

    /// What a restart would load.
    fn after_restart(&self) -> Vec<samyama::graph::Node> {
        self.pm.checkpoint().unwrap();
        self.pm.recover("default").unwrap().0
    }
}

/// The reproduction. Three nodes, a delete, then one node: a restart used to find
/// **three** — the new node at id 1, and the deleted graph's rows still sitting at
/// ids 2 and 3, with the row formerly at id 1 silently overwritten by an unrelated
/// entity. Neither the old graph nor the new one, and no error anywhere.
#[tokio::test]
async fn a_deleted_graph_does_not_come_back_around_the_next_write() {
    let s = Server::new();
    s.query("CREATE (:Before {tag: \"a\"}), (:Before {tag: \"b\"}), (:Before {tag: \"c\"})")
        .await;
    assert_eq!(s.after_restart().len(), 3, "setup did not persist");

    assert_eq!(s.delete("default").await, RespValue::SimpleString("OK".into()));
    assert_eq!(s.store.read().await.node_count(), 0, "memory not cleared");
    assert!(s.after_restart().is_empty(), "the delete did not reach disk");

    s.query("CREATE (:After {tag: \"second\"})").await;

    let nodes = s.after_restart();
    assert_eq!(
        nodes.len(),
        1,
        "a restart found {} nodes: {:?}",
        nodes.len(),
        nodes
            .iter()
            .map(|n| n.labels.iter().map(|l| l.as_str().to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
    assert!(
        nodes[0].labels.iter().any(|l| l.as_str() == "After"),
        "the surviving node is from the deleted graph: {:?}",
        nodes[0]
    );
}

/// The name was parsed into `_graph_name` and discarded, and then the whole store was
/// cleared — so deleting a graph this build does not serve emptied the one it does.
/// `GRAPH.QUERY` already refuses an unknown name; a destructive command has more
/// reason to.
#[tokio::test]
async fn deleting_a_graph_that_does_not_exist_does_not_empty_the_one_that_does() {
    let s = Server::new();
    s.query("CREATE (:Keep {tag: \"a\"})").await;

    let response = s.delete("analytics").await;
    assert!(
        matches!(response, RespValue::Error(_)),
        "deleting an unknown graph returned {response:?}"
    );
    assert_eq!(s.store.read().await.node_count(), 1, "memory was cleared anyway");
    assert_eq!(s.after_restart().len(), 1, "disk was cleared anyway");
}

/// #1113: `recover()` used to add the graph's whole size to the tenant counter on
/// every call, so reading the row count twice doubled it. The counter gates
/// `check_quota`, so the drift ends in a graph that has not grown refusing writes.
#[tokio::test]
async fn reading_the_graph_twice_does_not_double_its_recorded_size() {
    let s = Server::new();
    s.query("CREATE (:A {t: 1}), (:A {t: 2}), (:A {t: 3})").await;

    let count = || s.pm.tenants().get_usage("default").unwrap().node_count;
    assert_eq!(count(), 3);
    for _ in 0..3 {
        s.pm.recover("default").unwrap();
        assert_eq!(count(), 3, "a recover changed the count of an unchanged graph");
    }
}

/// A drop sets the counter to the truth rather than subtracting a remembered number
/// from it, so it lands on zero however far it had drifted beforehand.
#[tokio::test]
async fn a_drop_leaves_the_counter_at_zero() {
    let s = Server::new();
    s.query("CREATE (:A {t: 1}), (:A {t: 2})").await;
    s.pm.tenants().increment_usage("default", "nodes", 99).unwrap();

    s.delete("default").await;
    assert_eq!(s.pm.tenants().get_usage("default").unwrap().node_count, 0);
}
