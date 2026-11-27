//! Change data capture: a durable, per-tenant feed of committed changes (#2025).
//!
//! ## Where the events come from
//!
//! From the persist itself. Every committed write reaches disk through
//! [`super::PersistenceManager::apply_mutations`]: auto-commit statements over
//! RESP and HTTP, `GRAPH.COMMIT` and `/api/tx/:id/commit`, in-process writers
//! (`PersistedWriter`), imports and the retention sweeper. A rolled-back
//! transaction never gets there -- its write log is dropped -- so it emits
//! nothing, without the feed having to know what a transaction is.
//!
//! The write log carries ids, not changes. What changed is the difference
//! between the stored row, read in the batched existence probe that
//! `apply_mutations` already makes, and the entity as it is in memory now. So
//! one statement that sets a property five times is one `property_set`, with
//! the value from before the statement and the value after it: the feed
//! describes committed states, as the disk does, not each step between them.
//!
//! ## Why the events are persisted rather than derived from the logical WAL
//!
//! The logical WAL (`wal.rs`) is not the recovery source (#1592): it can be
//! deleted without losing data, checkpoints truncate it, its records carry the
//! whole property map with no before-image, no timestamp and no commit
//! boundary. A feed derived from it would be lossy exactly where it is needed.
//!
//! Instead each event is written into the **same RocksDB write batch** as the
//! rows it describes. RocksDB is what a restart reads, and a batch lands whole
//! or not at all, so after a crash the feed and the data agree: there is no
//! committed row without its event and no event for a row that did not land.
//! The keys live in the `default` column family; see `storage.rs`.
//!
//! ## Offsets, transactions and retention
//!
//! Offsets count from 1 per tenant and never go back, across restarts and
//! across `GRAPH.DELETE`. Every event of one persist shares a `tx` number,
//! also per tenant, and the last one carries `"commit": true`. The feed keeps
//! the newest `retention` events per tenant and trims the rest in the same
//! batch; a reader asking for something older is told the oldest offset still
//! held rather than handed a gap.
//!
//! The feed is off unless a retention is set (`--cdc-retention`): it costs a
//! decode of each touched row and a second write per changed entity, and a
//! deployment that does not read it should not pay that.

use crate::graph::{Edge, Node, PropertyMap};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Events a reader gets when it does not say how many.
pub const DEFAULT_LIMIT: usize = 100;
/// Most events one read returns.
pub const MAX_LIMIT: usize = 1000;

/// Where a tenant's feed stands. Persisted beside its events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedMeta {
    /// The offset the next event gets.
    pub next_offset: u64,
    /// The oldest offset still held. Equal to `next_offset` when none is.
    pub first_offset: u64,
    /// The transaction number the next persist gets.
    pub next_tx: u64,
}

impl Default for FeedMeta {
    fn default() -> Self {
        FeedMeta {
            next_offset: 1,
            first_offset: 1,
            next_tx: 1,
        }
    }
}

/// One read of a tenant's feed.
#[derive(Debug, Clone)]
pub struct ChangePage {
    /// The events, oldest first.
    pub events: Vec<Value>,
    /// The oldest offset still held; events below it were trimmed.
    pub oldest: u64,
    /// The newest offset written, 0 when none ever was.
    pub latest: u64,
}

/// Why a read of the feed returned no page.
#[derive(Debug, thiserror::Error)]
pub enum ChangeReadError {
    /// The feed is off on this server.
    #[error("change data capture is off; start the server with --cdc-retention <events>")]
    Disabled,
    /// Events after `after` were trimmed; the reader has missed some.
    #[error(
        "events after offset {after} have been trimmed; the oldest held is {oldest}. \
         Re-read the graph and resume from offset {latest}"
    )]
    Trimmed {
        after: u64,
        oldest: u64,
        latest: u64,
    },
    /// Storage could not be read.
    #[error("{0}")]
    Storage(String),
}

/// The feed's process-wide state: whether it is on, and each tenant's
/// position as last committed.
#[derive(Default)]
pub struct ChangeFeed {
    /// Events kept per tenant; 0 is off.
    retention: AtomicU64,
    /// Positions as last committed, loaded from storage on first use.
    meta: Mutex<HashMap<String, FeedMeta>>,
    /// Woken after every persist that wrote events, for long-polling readers.
    notify: tokio::sync::Notify,
}

impl ChangeFeed {
    /// Events kept per tenant, 0 when the feed is off.
    pub fn retention(&self) -> u64 {
        self.retention.load(Ordering::Relaxed)
    }

    /// Turn the feed on with this many events kept per tenant, or off with 0.
    pub fn set_retention(&self, events: u64) {
        self.retention.store(events, Ordering::Relaxed);
    }

    /// Woken after every persist that wrote events.
    pub fn notify(&self) -> &tokio::sync::Notify {
        &self.notify
    }

    /// The committed position for `tenant`, read through to `load` once.
    pub(crate) fn meta_or_load<E>(
        &self,
        tenant: &str,
        load: impl FnOnce() -> Result<FeedMeta, E>,
    ) -> Result<FeedMeta, E> {
        if let Some(m) = self.meta.lock().unwrap().get(tenant) {
            return Ok(*m);
        }
        let m = load()?;
        Ok(*self
            .meta
            .lock()
            .unwrap()
            .entry(tenant.to_string())
            .or_insert(m))
    }

    /// Record a position that has reached disk.
    pub(crate) fn committed(&self, tenant: &str, meta: FeedMeta) {
        self.meta.lock().unwrap().insert(tenant.to_string(), meta);
    }
}

/// The events of one persist, as they are staged: offsets and the tx number
/// are assigned here, and the position is moved only once the batch holding
/// them commits.
pub(crate) struct TxCursor {
    pub tenant: String,
    /// The position including what is staged.
    pub meta: FeedMeta,
    /// The position on disk.
    pub committed: FeedMeta,
    pub retention: u64,
    /// This persist's tx number, taken with its first event.
    pub tx: Option<u64>,
    /// Milliseconds since the epoch, one for the whole persist.
    pub ts: i64,
    /// The last event staged, re-written with `"commit": true` once the
    /// persist completes.
    pub last_event: Option<(u64, Value)>,
    /// Whether any event has reached disk.
    pub wrote: bool,
}

impl TxCursor {
    pub fn new(tenant: &str, meta: FeedMeta, retention: u64) -> Self {
        TxCursor {
            tenant: tenant.to_string(),
            meta,
            committed: meta,
            retention,
            tx: None,
            ts: chrono::Utc::now().timestamp_millis(),
            last_event: None,
            wrote: false,
        }
    }

    /// Stamp `body` with its offset, tx, tenant and time; returns the offset
    /// and the event.
    pub fn stamp(&mut self, body: Value) -> (u64, Value) {
        let tx = *self.tx.get_or_insert_with(|| {
            let tx = self.meta.next_tx;
            self.meta.next_tx += 1;
            tx
        });
        let offset = self.meta.next_offset;
        self.meta.next_offset += 1;
        let mut event = Map::new();
        event.insert("offset".into(), json!(offset));
        event.insert("tx".into(), json!(tx));
        event.insert("tenant".into(), json!(self.tenant));
        event.insert("ts".into(), json!(self.ts));
        if let Value::Object(fields) = body {
            event.extend(fields);
        }
        let event = Value::Object(event);
        self.last_event = Some((offset, event.clone()));
        (offset, event)
    }

    /// The last event of the persist, marked as such, to be staged again over
    /// the unmarked copy.
    pub fn commit_marker(&self) -> Option<(u64, Value)> {
        let (offset, mut event) = self.last_event.clone()?;
        event["commit"] = Value::Bool(true);
        Some((offset, event))
    }

    /// The trim this position needs to stay within retention, as
    /// `(from, to)`, and the position after it.
    pub fn trim(&mut self) -> Option<(u64, u64)> {
        let held = self.meta.next_offset - self.meta.first_offset;
        if self.retention == 0 || held <= self.retention {
            return None;
        }
        let from = self.meta.first_offset;
        let to = self.meta.next_offset - self.retention;
        self.meta.first_offset = to;
        Some((from, to))
    }
}

fn props_json(props: &PropertyMap) -> Value {
    let mut keys: Vec<&String> = props.keys().collect();
    keys.sort();
    Value::Object(
        keys.into_iter()
            .map(|k| (k.clone(), props[k].to_json()))
            .collect(),
    )
}

fn labels_json(node: &Node) -> Value {
    let mut labels: Vec<&str> = node.labels.iter().map(|l| l.as_str()).collect();
    labels.sort_unstable();
    json!(labels)
}

/// `property_set` and `property_removed` events between two property maps,
/// in key order.
fn property_events(entity: &str, id: u64, before: &PropertyMap, after: &PropertyMap) -> Vec<Value> {
    let keys: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    let mut out = Vec::new();
    for key in keys {
        match (before.get(key), after.get(key)) {
            (old, Some(new)) if old != Some(new) => out.push(json!({
                "op": "property_set",
                "entity": entity,
                "id": id,
                "key": key,
                "value": new.to_json(),
                "old": old.map(|v| v.to_json()).unwrap_or(Value::Null),
            })),
            (Some(old), None) => out.push(json!({
                "op": "property_removed",
                "entity": entity,
                "id": id,
                "key": key,
                "old": old.to_json(),
            })),
            _ => {}
        }
    }
    out
}

/// The events that take a node from `before` (its stored row, `None` when it
/// had none) to `after` (as it is now, `None` when deleted).
pub fn node_events(id: u64, before: Option<&Node>, after: Option<&Node>) -> Vec<Value> {
    match (before, after) {
        (None, Some(n)) => vec![json!({
            "op": "node_created",
            "id": id,
            "labels": labels_json(n),
            "properties": props_json(&n.properties),
        })],
        (Some(b), None) => vec![json!({
            "op": "node_deleted",
            "id": id,
            "labels": labels_json(b),
            "properties": props_json(&b.properties),
        })],
        (Some(b), Some(a)) => {
            let mut out = Vec::new();
            let old: BTreeSet<&str> = b.labels.iter().map(|l| l.as_str()).collect();
            let new: BTreeSet<&str> = a.labels.iter().map(|l| l.as_str()).collect();
            for label in new.difference(&old) {
                out.push(json!({ "op": "label_added", "id": id, "label": label }));
            }
            for label in old.difference(&new) {
                out.push(json!({ "op": "label_removed", "id": id, "label": label }));
            }
            out.extend(property_events("node", id, &b.properties, &a.properties));
            out
        }
        (None, None) => Vec::new(),
    }
}

fn edge_json(op: &str, e: &Edge) -> Value {
    json!({
        "op": op,
        "id": e.id.as_u64(),
        "type": e.edge_type.as_str(),
        "source": e.source.as_u64(),
        "target": e.target.as_u64(),
        "properties": props_json(&e.properties),
    })
}

/// [`node_events`] for an edge. An edge's endpoints and type do not change,
/// so only its properties can.
pub fn edge_events(id: u64, before: Option<&Edge>, after: Option<&Edge>) -> Vec<Value> {
    match (before, after) {
        (None, Some(e)) => vec![edge_json("edge_created", e)],
        (Some(e), None) => vec![edge_json("edge_deleted", e)],
        (Some(b), Some(a)) => property_events("edge", id, &b.properties, &a.properties),
        (None, None) => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::{Label, NodeId, PropertyValue};

    fn node(labels: &[&str], props: &[(&str, i64)]) -> Node {
        let mut n = Node::new(NodeId::new(7), Label::new(labels[0]));
        for l in &labels[1..] {
            n.labels.insert(Label::new(*l));
        }
        for (k, v) in props {
            n.properties
                .insert((*k).to_string(), PropertyValue::Integer(*v));
        }
        n
    }

    fn ops(events: &[Value]) -> Vec<String> {
        events
            .iter()
            .map(|e| e["op"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn an_unchanged_entity_has_no_events() {
        let n = node(&["A"], &[("x", 1)]);
        assert!(node_events(7, Some(&n), Some(&n)).is_empty());
    }

    #[test]
    fn a_diff_names_each_label_and_property_once_in_order() {
        let b = node(&["A", "B"], &[("x", 1), ("y", 2)]);
        let a = node(&["A", "C"], &[("x", 5), ("z", 3)]);
        let ev = node_events(7, Some(&b), Some(&a));
        assert_eq!(
            ops(&ev),
            [
                "label_added",
                "label_removed",
                "property_set",
                "property_removed",
                "property_set"
            ]
        );
        assert_eq!(ev[2]["key"], "x");
        assert_eq!(ev[2]["old"], 1);
        assert_eq!(ev[2]["value"], 5);
        assert_eq!(ev[3]["key"], "y");
        assert_eq!(ev[4]["old"], Value::Null);
    }

    #[test]
    fn the_cursor_numbers_events_and_trims_to_retention() {
        let mut c = TxCursor::new("t", FeedMeta::default(), 2);
        let (o1, e1) = c.stamp(json!({ "op": "x" }));
        let (o2, _) = c.stamp(json!({ "op": "y" }));
        let (o3, e3) = c.stamp(json!({ "op": "z" }));
        assert_eq!((o1, o2, o3), (1, 2, 3));
        assert_eq!(e1["tx"], e3["tx"], "one persist is one transaction");
        assert_eq!(c.trim(), Some((1, 2)));
        assert_eq!(c.meta.first_offset, 2);
        assert_eq!(c.trim(), None);
    }
}
