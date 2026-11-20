//! Checkpoints, structural diff and rollback over the MVCC undo log (#2093,
//! Samyama Smriti Phase 0).
//!
//! An agent harness that uses the graph as working memory takes a checkpoint
//! before a turn, reads what the turn changed with `diff_since`, and throws a
//! failed branch away with `rollback_to`. None of it copies the store.
//!
//! # How it works
//!
//! `checkpoint` pins the current version and advances it, so every later write
//! lands at a newer version and records what it replaced in the undo logs
//! (`node_history`, `edge_history`, #1200). A pinned version stops
//! `gc_versions` from pruning that history.
//!
//! The undo logs cannot rebuild a deletion, and they say which entities a
//! version touched only by scanning all of them. So while any checkpoint is
//! pinned the store also keeps, in [`Checkpoints`]:
//!
//! * a **touch log**: `(version, id)` for every recorded write, birth and
//!   deletion, in version order. The entities changed since `v` are the
//!   entries after `v`, found by binary search, so a diff or a rollback costs
//!   O(mutations since `v`), not O(graph).
//! * a **tomb** per deletion: the entity whole as it was deleted, with the
//!   version it was born at. Its history stays in the undo log, and no id is
//!   reused while a checkpoint is pinned, so the entity as it was at `v` is the
//!   tomb with the history between `v` and the deletion undone.
//!
//! Both are dropped back to the oldest pinned version when a checkpoint is
//! released, and cleared when none is left.
//!
//! # Decisions (the Smriti spec, §3 and §11)
//!
//! * **Not durable.** Checkpoints serve work inside one process and a restart
//!   discards them. Undo after a restart is logical, by run id (#2132).
//! * **A rollback is a write.** It goes through the ordinary journalled
//!   setters at a new version, never by moving the version back, so the
//!   history stays true for every other pinned checkpoint and a reader at an
//!   older version, and a store with the write log on journals it like any
//!   statement. `PersistenceManager::rollback_to` is the persisted wrapper.
//! * **Refused on conflict.** If a transaction committed through
//!   `commit_transaction` changed an entity the rollback would undo, the
//!   rollback is refused and the error carries the diff, so it never silently
//!   undoes someone else's work.
//! * **Pinned until released or rolled back to.** `rollback_to(v)` releases
//!   `v`; a caller that wants to retry from the same point takes a new
//!   checkpoint. Checkpoints newer or older than `v` stay valid.
//!
//! # What it does not see
//!
//! Writes that bypass the undo log: `get_node_mut`, `get_edge_properties_mut`
//! beyond the one map it records, and the bulk-load column setters
//! (`set_column_property`). They are the paths that already bypass MVCC.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::Serialize;

use super::{EdgeUndo, GraphError, GraphResult, GraphStore, NodeUndo};
use crate::graph::event::Mutation;
use crate::graph::{Edge, EdgeId, Label, Node, NodeId, PropertyMap, PropertyValue};

/// A node or a relationship, by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum EntityRef {
    Node(NodeId),
    Edge(EdgeId),
}

impl std::fmt::Display for EntityRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntityRef::Node(id) => write!(f, "node {}", id.as_u64()),
            EntityRef::Edge(id) => write!(f, "relationship {}", id.as_u64()),
        }
    }
}

/// One property whose value differs between a checkpoint and now. `None` is an
/// absent key.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PropertyChange {
    pub entity: EntityRef,
    pub key: String,
    pub old: Option<PropertyValue>,
    pub new: Option<PropertyValue>,
}

/// A label a node gained (`added`) or lost since a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LabelChange {
    pub node: NodeId,
    pub label: Label,
    pub added: bool,
}

/// What changed between a checkpoint and the current version (`diff_since`).
///
/// A **net** diff: a node created and deleted since the checkpoint is not in
/// it, and a property set and set back is not either. Property and label
/// changes are listed only for entities that exist at both ends; an added or
/// deleted entity is listed by id.
///
/// Deterministic: every list is in id order, nodes before relationships in
/// `property_changes`, and keys in byte order within one entity.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct GraphDiff {
    /// The checkpoint.
    pub from_version: u64,
    /// The store's version when the diff was taken.
    pub to_version: u64,
    pub nodes_added: Vec<NodeId>,
    pub nodes_deleted: Vec<NodeId>,
    pub edges_added: Vec<EdgeId>,
    pub edges_deleted: Vec<EdgeId>,
    pub property_changes: Vec<PropertyChange>,
    pub label_changes: Vec<LabelChange>,
}

impl GraphDiff {
    /// Nothing changed.
    pub fn is_empty(&self) -> bool {
        self.nodes_added.is_empty()
            && self.nodes_deleted.is_empty()
            && self.edges_added.is_empty()
            && self.edges_deleted.is_empty()
            && self.property_changes.is_empty()
            && self.label_changes.is_empty()
    }
}

/// What `rollback_to` did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RollbackStats {
    /// The checkpoint rolled back to. It is released.
    pub checkpoint: u64,
    /// The version the rollback was written at.
    pub version: u64,
    /// Created since the checkpoint, now deleted.
    pub nodes_removed: usize,
    pub edges_removed: usize,
    /// Deleted since the checkpoint, now back under the same id.
    pub nodes_restored: usize,
    pub edges_restored: usize,
    /// Keys put back on entities that existed at both ends.
    pub properties_reverted: usize,
    /// Labels put back or taken off.
    pub labels_reverted: usize,
    /// Steps the store refused. Empty on every rollback whose checkpoint state
    /// was itself valid; the rollback carries on past one, so the store ends
    /// as close to the checkpoint as it can.
    pub failures: Vec<String>,
}

/// Why a rollback was refused: entities another writer committed changes to
/// since the checkpoint, and the whole diff the rollback would have undone.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RollbackConflict {
    pub checkpoint: u64,
    /// In id order, nodes first.
    pub entities: Vec<EntityRef>,
    pub diff: GraphDiff,
}

impl std::fmt::Display for RollbackConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rollback to checkpoint {} refused: {} entit{} changed by another writer's \
             committed transaction since it was taken",
            self.checkpoint,
            self.entities.len(),
            if self.entities.len() == 1 {
                "y was"
            } else {
                "ies were"
            },
        )?;
        let shown: Vec<String> = self
            .entities
            .iter()
            .take(5)
            .map(|e| e.to_string())
            .collect();
        write!(f, " ({}", shown.join(", "))?;
        if self.entities.len() > shown.len() {
            write!(f, ", and {} more", self.entities.len() - shown.len())?;
        }
        write!(f, ")")
    }
}

/// A node deleted while a checkpoint was pinned.
#[derive(Debug, Clone)]
struct NodeTomb {
    /// The version it was deleted at.
    at: u64,
    /// The version it was born at; `None` for a node as old as the history.
    born: Option<u64>,
    /// Labels and every property, as deleted.
    node: Node,
}

/// A relationship deleted while a checkpoint was pinned.
#[derive(Debug, Clone)]
struct EdgeTomb {
    at: u64,
    born: Option<u64>,
    edge: Edge,
}

/// Pinned checkpoints, and what has changed since the oldest of them.
#[derive(Debug, Default)]
pub(super) struct Checkpoints {
    /// Pinned version -> how many holders.
    pins: BTreeMap<u64, usize>,
    /// `(version, id)` per recorded write, birth or deletion, in version order.
    touched_nodes: Vec<(u64, NodeId)>,
    touched_edges: Vec<(u64, EdgeId)>,
    /// In version order.
    node_tombs: Vec<NodeTomb>,
    edge_tombs: Vec<EdgeTomb>,
}

impl Checkpoints {
    pub(super) fn is_pinned(&self) -> bool {
        !self.pins.is_empty()
    }

    /// The oldest pinned version: the one garbage collection must not pass.
    pub(super) fn oldest(&self) -> Option<u64> {
        self.pins.keys().next().copied()
    }

    /// A freed id to hand out again, unless a checkpoint is pinned. A reused id
    /// would make a deleted entity and a new one the same id, and the rollback
    /// could restore only one of them.
    pub(super) fn reuse(&self, free: &mut Vec<u64>) -> Option<u64> {
        if self.is_pinned() {
            None
        } else {
            free.pop()
        }
    }

    pub(super) fn touch_node(&mut self, at: u64, id: NodeId) {
        if self.is_pinned() {
            self.touched_nodes.push((at, id));
        }
    }

    pub(super) fn touch_edge(&mut self, at: u64, id: EdgeId) {
        if self.is_pinned() {
            self.touched_edges.push((at, id));
        }
    }

    pub(super) fn bury_node(&mut self, at: u64, born: Option<u64>, node: Node) {
        if self.is_pinned() {
            self.touched_nodes.push((at, node.id));
            self.node_tombs.push(NodeTomb { at, born, node });
        }
    }

    pub(super) fn bury_edge(&mut self, at: u64, born: Option<u64>, edge: Edge) {
        if self.is_pinned() {
            self.touched_edges.push((at, edge.id));
            self.edge_tombs.push(EdgeTomb { at, born, edge });
        }
    }

    /// Drop what was recorded at `version`, whose writes were rolled back by a
    /// session transaction ROLLBACK.
    pub(super) fn forget_version(&mut self, version: u64) {
        self.touched_nodes.retain(|(at, _)| *at != version);
        self.touched_edges.retain(|(at, _)| *at != version);
        self.node_tombs.retain(|t| t.at != version);
        self.edge_tombs.retain(|t| t.at != version);
    }

    /// Drop everything at or before `floor`, the oldest pinned version, or
    /// everything if none is pinned. Returns the ids of the tombs dropped.
    fn trim(&mut self, floor: Option<u64>) -> (Vec<NodeId>, Vec<EdgeId>) {
        let Some(floor) = floor else {
            let nodes = self.node_tombs.drain(..).map(|t| t.node.id).collect();
            let edges = self.edge_tombs.drain(..).map(|t| t.edge.id).collect();
            self.touched_nodes = Vec::new();
            self.touched_edges = Vec::new();
            return (nodes, edges);
        };
        let n = self.touched_nodes.partition_point(|(at, _)| *at <= floor);
        self.touched_nodes.drain(..n);
        let n = self.touched_edges.partition_point(|(at, _)| *at <= floor);
        self.touched_edges.drain(..n);
        let n = self.node_tombs.partition_point(|t| t.at <= floor);
        let nodes = self.node_tombs.drain(..n).map(|t| t.node.id).collect();
        let n = self.edge_tombs.partition_point(|t| t.at <= floor);
        let edges = self.edge_tombs.drain(..n).map(|t| t.edge.id).collect();
        (nodes, edges)
    }
}

/// One entity as it was at a checkpoint and as it is now.
struct Change<T> {
    before: Option<T>,
    after: Option<T>,
}

/// The keys whose values differ between two maps, in byte order.
fn changed_keys(before: &PropertyMap, after: &PropertyMap) -> Vec<String> {
    let keys: BTreeSet<&String> = before.keys().chain(after.keys()).collect();
    keys.into_iter()
        .filter(|k| before.get(*k) != after.get(*k))
        .cloned()
        .collect()
}

fn sorted_labels(labels: &HashSet<Label>) -> Vec<Label> {
    let mut v: Vec<Label> = labels.iter().cloned().collect();
    v.sort();
    v
}

fn node_differs(c: &Change<Node>) -> bool {
    match (&c.before, &c.after) {
        (Some(b), Some(a)) => {
            b.labels != a.labels || !changed_keys(&b.properties, &a.properties).is_empty()
        }
        (None, None) => false,
        _ => true,
    }
}

fn edge_differs(c: &Change<Edge>) -> bool {
    match (&c.before, &c.after) {
        (Some(b), Some(a)) => !changed_keys(&b.properties, &a.properties).is_empty(),
        (None, None) => false,
        _ => true,
    }
}

fn build_diff(
    from_version: u64,
    to_version: u64,
    nodes: &BTreeMap<NodeId, Change<Node>>,
    edges: &BTreeMap<EdgeId, Change<Edge>>,
) -> GraphDiff {
    let mut diff = GraphDiff {
        from_version,
        to_version,
        ..GraphDiff::default()
    };
    for (&id, c) in nodes {
        match (&c.before, &c.after) {
            (None, Some(_)) => diff.nodes_added.push(id),
            (Some(_), None) => diff.nodes_deleted.push(id),
            (Some(b), Some(a)) => {
                for key in changed_keys(&b.properties, &a.properties) {
                    diff.property_changes.push(PropertyChange {
                        entity: EntityRef::Node(id),
                        old: b.properties.get(&key).cloned(),
                        new: a.properties.get(&key).cloned(),
                        key,
                    });
                }
                for label in sorted_labels(&b.labels) {
                    if !a.labels.contains(&label) {
                        diff.label_changes.push(LabelChange {
                            node: id,
                            label,
                            added: false,
                        });
                    }
                }
                for label in sorted_labels(&a.labels) {
                    if !b.labels.contains(&label) {
                        diff.label_changes.push(LabelChange {
                            node: id,
                            label,
                            added: true,
                        });
                    }
                }
            }
            (None, None) => {}
        }
    }
    for (&id, c) in edges {
        match (&c.before, &c.after) {
            (None, Some(_)) => diff.edges_added.push(id),
            (Some(_), None) => diff.edges_deleted.push(id),
            (Some(b), Some(a)) => {
                for key in changed_keys(&b.properties, &a.properties) {
                    diff.property_changes.push(PropertyChange {
                        entity: EntityRef::Edge(id),
                        old: b.properties.get(&key).cloned(),
                        new: a.properties.get(&key).cloned(),
                        key,
                    });
                }
            }
            (None, None) => {}
        }
    }
    diff
}

impl GraphStore {
    /// Pin the current version and return it (#2093).
    ///
    /// O(1): nothing is copied. The version advances, so every later write is
    /// recorded against the checkpoint, and `gc_versions` keeps the history
    /// after it until it is released (`release_checkpoint`) or rolled back to
    /// (`rollback_to`).
    ///
    /// Refused inside an open session transaction: its statements all write at
    /// one version, so a checkpoint in the middle could not separate the
    /// writes before it from the writes after.
    ///
    /// Checkpoints live in memory only, and a restart discards them.
    pub fn checkpoint(&mut self) -> GraphResult<u64> {
        if self.session_txn.is_some() {
            return Err(GraphError::CheckpointRefused(
                "a checkpoint cannot be taken inside an open session transaction".to_string(),
            ));
        }
        let version = self.current_version;
        *self.checkpoints.pins.entry(version).or_insert(0) += 1;
        self.current_version += 1;
        Ok(version)
    }

    /// Release a checkpoint, keeping every change made since. Returns whether
    /// `version` was pinned.
    pub fn release_checkpoint(&mut self, version: u64) -> bool {
        let Some(holders) = self.checkpoints.pins.get_mut(&version) else {
            return false;
        };
        *holders -= 1;
        if *holders == 0 {
            self.checkpoints.pins.remove(&version);
        }
        self.trim_checkpoint_log();
        true
    }

    /// The pinned checkpoint versions, oldest first.
    pub fn pinned_checkpoints(&self) -> Vec<u64> {
        self.checkpoints.pins.keys().copied().collect()
    }

    /// What changed since checkpoint `version`, which must be pinned (#2093).
    ///
    /// Reads the touch log after `version` and rebuilds each entity it names
    /// at the checkpoint from the undo log, so it costs O(mutations since
    /// `version`). See [`GraphDiff`] for what it lists and in what order.
    pub fn diff_since(&self, version: u64) -> GraphResult<GraphDiff> {
        self.require_pinned(version)?;
        let nodes = self.node_changes(version);
        let edges = self.edge_changes(version);
        Ok(build_diff(version, self.current_version, &nodes, &edges))
    }

    /// Put the store back as it was at checkpoint `version`, which must be
    /// pinned, and release it (#2093).
    ///
    /// O(mutations since `version`): only the entities in the touch log after
    /// it are visited. The rollback is itself a write, at a new version,
    /// through the ordinary setters, so the indexes, the catalog and the write
    /// log see it like any other; with the write log on, persisting what it
    /// journals makes the rollback durable (`PersistenceManager::rollback_to`).
    ///
    /// Refused, changing nothing, if a transaction committed through
    /// `commit_transaction` since the checkpoint changed an entity the
    /// rollback would undo: the error is `GraphError::RollbackConflict`, with
    /// the diff. Also refused inside an open session transaction.
    pub fn rollback_to(&mut self, version: u64) -> GraphResult<RollbackStats> {
        self.require_pinned(version)?;
        if self.session_txn.is_some() {
            return Err(GraphError::CheckpointRefused(
                "a rollback cannot run inside an open session transaction".to_string(),
            ));
        }
        let nodes = self.node_changes(version);
        let edges = self.edge_changes(version);

        let mut conflicts: Vec<EntityRef> = Vec::new();
        for (id, c) in &nodes {
            if self
                .node_last_commit
                .get(id)
                .is_some_and(|&at| at > version)
                && node_differs(c)
            {
                conflicts.push(EntityRef::Node(*id));
            }
        }
        for (id, c) in &edges {
            if self
                .edge_last_commit
                .get(id)
                .is_some_and(|&at| at > version)
                && edge_differs(c)
            {
                conflicts.push(EntityRef::Edge(*id));
            }
        }
        if !conflicts.is_empty() {
            let diff = build_diff(version, self.current_version, &nodes, &edges);
            return Err(GraphError::RollbackConflict(Box::new(RollbackConflict {
                checkpoint: version,
                entities: conflicts,
                diff,
            })));
        }

        self.bump_epoch();
        self.invalidate_statistics_cache();
        self.current_version += 1;
        let mut stats = RollbackStats {
            checkpoint: version,
            version: self.current_version,
            ..RollbackStats::default()
        };

        // Created since: relationships first, so deleting a node cascades to
        // nothing a later step still needs.
        for (&id, c) in &edges {
            if c.before.is_none() && c.after.is_some() {
                match self.delete_edge(id) {
                    Ok(_) => stats.edges_removed += 1,
                    Err(e) => stats
                        .failures
                        .push(format!("delete relationship {}: {e}", id.as_u64())),
                }
            }
        }
        for (&id, c) in &nodes {
            if c.before.is_none() && c.after.is_some() {
                match self.delete_node("default", id) {
                    Ok(_) => stats.nodes_removed += 1,
                    Err(e) => stats
                        .failures
                        .push(format!("delete node {}: {e}", id.as_u64())),
                }
            }
        }

        // Changed since: every differing key comes off before any goes back
        // on, so a unique value that moved between two nodes is never held by
        // both at once.
        for (&id, c) in &nodes {
            let (Some(b), Some(a)) = (&c.before, &c.after) else {
                continue;
            };
            for key in changed_keys(&b.properties, &a.properties) {
                if a.properties.contains_key(&key) {
                    self.remove_node_property(id, &key);
                }
            }
            for label in sorted_labels(&a.labels) {
                if !b.labels.contains(&label) {
                    match self.remove_label_from_node(id, &label) {
                        Ok(_) => stats.labels_reverted += 1,
                        Err(e) => stats
                            .failures
                            .push(format!("remove label from node {}: {e}", id.as_u64())),
                    }
                }
            }
        }
        for (&id, c) in &edges {
            let (Some(b), Some(a)) = (&c.before, &c.after) else {
                continue;
            };
            for key in changed_keys(&b.properties, &a.properties) {
                if a.properties.contains_key(&key) {
                    self.remove_edge_property(id, &key);
                }
            }
        }

        // Deleted since: back under the same id, which no one has taken.
        for c in nodes.values() {
            if let (Some(b), None) = (&c.before, &c.after) {
                self.restore_node(b, &mut stats);
                stats.nodes_restored += 1;
            }
        }

        for (&id, c) in &nodes {
            let (Some(b), Some(a)) = (&c.before, &c.after) else {
                continue;
            };
            for label in sorted_labels(&b.labels) {
                if !a.labels.contains(&label) {
                    match self.add_label_to_node_inner("default", id, label, false) {
                        Ok(()) => stats.labels_reverted += 1,
                        Err(e) => stats
                            .failures
                            .push(format!("add label to node {}: {e}", id.as_u64())),
                    }
                }
            }
            for key in changed_keys(&b.properties, &a.properties) {
                stats.properties_reverted += 1;
                if let Some(value) = b.properties.get(&key) {
                    if let Err(e) =
                        self.set_node_property("default", id, key.clone(), value.clone())
                    {
                        stats
                            .failures
                            .push(format!("set {key} on node {}: {e}", id.as_u64()));
                    }
                }
            }
        }
        for (&id, c) in &edges {
            let (Some(b), Some(a)) = (&c.before, &c.after) else {
                continue;
            };
            for key in changed_keys(&b.properties, &a.properties) {
                stats.properties_reverted += 1;
                if let Some(value) = b.properties.get(&key) {
                    self.set_edge_property_sparse(id, key, value.clone());
                }
            }
        }

        // Relationships last: both their ends exist again by now.
        for c in edges.values() {
            if let (Some(b), None) = (&c.before, &c.after) {
                match self.restore_edge(b) {
                    Ok(()) => stats.edges_restored += 1,
                    Err(e) => stats
                        .failures
                        .push(format!("restore relationship {}: {e}", b.id.as_u64())),
                }
            }
        }

        self.release_checkpoint(version);
        Ok(stats)
    }

    fn require_pinned(&self, version: u64) -> GraphResult<()> {
        if self.checkpoints.pins.contains_key(&version) {
            Ok(())
        } else {
            Err(GraphError::CheckpointNotPinned(version))
        }
    }

    /// Every node touched after `version`, as it was then and as it is now.
    fn node_changes(&self, version: u64) -> BTreeMap<NodeId, Change<Node>> {
        let cp = &self.checkpoints;
        let start = cp.touched_nodes.partition_point(|(at, _)| *at <= version);
        let ids: BTreeSet<NodeId> = cp.touched_nodes[start..]
            .iter()
            .map(|(_, id)| *id)
            .collect();
        // A node deleted after `version` is rebuilt from its first deletion
        // after it: until then it was one node, continuously.
        let start = cp.node_tombs.partition_point(|t| t.at <= version);
        let mut first_tomb: HashMap<NodeId, &NodeTomb> = HashMap::new();
        for tomb in &cp.node_tombs[start..] {
            first_tomb.entry(tomb.node.id).or_insert(tomb);
        }
        let mut out = BTreeMap::new();
        for id in ids {
            let before = match first_tomb.get(&id) {
                Some(tomb) => self.node_from_tomb(tomb, version),
                None => self.get_node_at_version(id, version),
            };
            let after = self.node_materialized(id);
            if before.is_some() || after.is_some() {
                out.insert(id, Change { before, after });
            }
        }
        out
    }

    fn edge_changes(&self, version: u64) -> BTreeMap<EdgeId, Change<Edge>> {
        let cp = &self.checkpoints;
        let start = cp.touched_edges.partition_point(|(at, _)| *at <= version);
        let ids: BTreeSet<EdgeId> = cp.touched_edges[start..]
            .iter()
            .map(|(_, id)| *id)
            .collect();
        let start = cp.edge_tombs.partition_point(|t| t.at <= version);
        let mut first_tomb: HashMap<EdgeId, &EdgeTomb> = HashMap::new();
        for tomb in &cp.edge_tombs[start..] {
            first_tomb.entry(tomb.edge.id).or_insert(tomb);
        }
        let mut out = BTreeMap::new();
        for id in ids {
            let before = match first_tomb.get(&id) {
                Some(tomb) => self.edge_from_tomb(tomb, version),
                None => self.get_edge_at_version(id, version),
            };
            let after = self.get_edge(id);
            if before.is_some() || after.is_some() {
                out.insert(id, Change { before, after });
            }
        }
        out
    }

    /// A deleted node as it was at `version`: the tomb, with every write
    /// recorded between `version` and the deletion undone, newest first.
    /// Entries after the deletion belong to a later restoration of the id.
    fn node_from_tomb(&self, tomb: &NodeTomb, version: u64) -> Option<Node> {
        if tomb.born.unwrap_or(Self::FIRST_VERSION) > version {
            return None;
        }
        let mut node = tomb.node.clone();
        if let Some(history) = self.node_history.get(&node.id) {
            let entries = history
                .undo
                .iter()
                .rev()
                .skip_while(|e| e.at() > tomb.at)
                .take_while(|e| e.at() > version);
            for entry in entries {
                match entry {
                    NodeUndo::Property {
                        key,
                        old: Some(old),
                        ..
                    } => {
                        node.properties.insert(key.clone(), old.clone());
                    }
                    NodeUndo::Property { key, old: None, .. } => {
                        node.properties.remove(key);
                    }
                    NodeUndo::Label {
                        label, had: true, ..
                    } => {
                        node.labels.insert(label.clone());
                    }
                    NodeUndo::Label {
                        label, had: false, ..
                    } => {
                        node.labels.remove(label);
                    }
                }
            }
        }
        Some(node)
    }

    fn edge_from_tomb(&self, tomb: &EdgeTomb, version: u64) -> Option<Edge> {
        if tomb.born.unwrap_or(Self::FIRST_VERSION) > version {
            return None;
        }
        let mut edge = tomb.edge.clone();
        if let Some(history) = self.edge_history.get(&edge.id) {
            let entries = history
                .undo
                .iter()
                .rev()
                .skip_while(|e| e.at() > tomb.at)
                .take_while(|e| e.at() > version);
            for entry in entries {
                match entry {
                    EdgeUndo::Property {
                        key,
                        old: Some(old),
                        ..
                    } => {
                        edge.properties.insert(key.clone(), old.clone());
                    }
                    EdgeUndo::Property { key, old: None, .. } => {
                        edge.properties.remove(key);
                    }
                    EdgeUndo::Map { old, .. } => edge.properties = old.clone(),
                }
            }
        }
        Some(edge)
    }

    /// Put a deleted node back under its own id, with its labels and
    /// properties, through the paths a write takes, so every index and the
    /// write log learn of it. It is born again at the current version: the
    /// history before its deletion describes a node that a read between the
    /// deletion and now must not see.
    fn restore_node(&mut self, node: &Node, stats: &mut RollbackStats) {
        let id = node.id;
        let at = self.current_version;
        let mut shell = Node::with_labels(id, node.labels.iter().cloned());
        shell.version = at;
        shell.created_at = node.created_at;
        shell.updated_at = node.updated_at;
        self.insert_recovered_node(shell);
        self.invalidate_label_bits();
        for label in &node.labels {
            self.catalog.on_label_added(label);
        }
        self.free_node_ids.retain(|&free| free != id.as_u64());
        self.node_history.entry(id).or_default().born = Some(at);
        self.checkpoints.touch_node(at, id);
        self.journal(Mutation::NodeUpserted(id));
        let mut keys: Vec<&String> = node.properties.keys().collect();
        keys.sort();
        for key in keys {
            let value = node.properties[key].clone();
            if let Err(e) = self.set_node_property("default", id, key.clone(), value) {
                stats
                    .failures
                    .push(format!("set {key} on restored node {}: {e}", id.as_u64()));
            }
        }
    }

    /// Put a deleted relationship back under its own id. One still listed in
    /// a frozen adjacency segment is only un-tombstoned: inserting it into
    /// the write buffer as well would list it twice.
    fn restore_edge(&mut self, edge: &Edge) -> GraphResult<()> {
        let id = edge.id;
        let idx = id.as_u64() as usize;
        let (source, target) = (edge.source, edge.target);
        if !self.has_node(source) {
            return Err(GraphError::InvalidEdgeSource(source));
        }
        if !self.has_node(target) {
            return Err(GraphError::InvalidEdgeTarget(target));
        }
        let frozen = idx < self.edge_endpoints.len()
            && idx < self.edge_type_ids.len()
            && self
                .frozen_outgoing
                .neighbors_collected(source.as_u64() as usize)
                .iter()
                .any(|&(_, e)| e == id);
        if frozen {
            self.bump_epoch();
            self.edge_endpoints[idx] = (source, target);
            let type_id = self.intern_edge_type(&edge.edge_type);
            self.edge_type_ids[idx] = type_id;
            self.edge_type_index
                .entry(edge.edge_type.clone())
                .or_default()
                .insert(id);
            self.frozen_dead_edges = self.frozen_dead_edges.saturating_sub(1);
        } else {
            let mut shell = edge.clone();
            shell.properties = PropertyMap::new();
            self.insert_recovered_edge(shell)?;
        }
        self.invalidate_statistics_cache();
        self.invalidate_type_adj();
        self.invalidate_hierarchies_for_edge_type(&edge.edge_type);
        let src_labels: Vec<Label> = self
            .get_node(source)
            .map(|n| n.labels.iter().cloned().collect())
            .unwrap_or_default();
        let tgt_labels: Vec<Label> = self
            .get_node(target)
            .map(|n| n.labels.iter().cloned().collect())
            .unwrap_or_default();
        self.catalog
            .on_edge_created(source, &src_labels, &edge.edge_type, target, &tgt_labels);
        self.free_edge_ids.retain(|&free| free != id.as_u64());
        let at = self.current_version;
        self.edge_history.entry(id).or_default().born = Some(at);
        self.checkpoints.touch_edge(at, id);
        self.journal(Mutation::EdgeUpserted(id));
        let mut keys: Vec<&String> = edge.properties.keys().collect();
        keys.sort();
        for key in keys {
            self.set_edge_property_sparse(id, key.clone(), edge.properties[key].clone());
        }
        Ok(())
    }

    /// Drop the checkpoint log back to the oldest pinned version, and the
    /// history of every entity that died before it and stayed dead: no pinned
    /// checkpoint can need it, and its id may now be reused.
    fn trim_checkpoint_log(&mut self) {
        let floor = self.checkpoints.oldest();
        let (nodes, edges) = self.checkpoints.trim(floor);
        let entombed_nodes: HashSet<NodeId> = self
            .checkpoints
            .node_tombs
            .iter()
            .map(|t| t.node.id)
            .collect();
        for id in nodes {
            if !self.has_node(id) && !entombed_nodes.contains(&id) {
                self.node_history.remove(&id);
            }
        }
        let entombed_edges: HashSet<EdgeId> = self
            .checkpoints
            .edge_tombs
            .iter()
            .map(|t| t.edge.id)
            .collect();
        for id in edges {
            if !self.has_edge(id) && !entombed_edges.contains(&id) {
                self.edge_history.remove(&id);
            }
        }
    }
}
