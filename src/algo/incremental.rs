//! Incremental PageRank, WCC and degree centrality on a live graph (ALGO-14,
//! #1697).
//!
//! `CALL algo.pageRank({incremental: true})` -- and the same key on `algo.wcc`
//! and `algo.degree` -- keeps the algorithm's state with the graph between
//! calls. The first call builds it from the whole graph, as a plain call
//! would. Each later call reads what was added since from the store's
//! [`StructureLog`](crate::graph::structure_log::StructureLog), applies those
//! nodes and relationships as deltas and answers from the updated state, so
//! one new relationship costs about what it changes rather than a full run.
//!
//! The state is rebuilt from scratch, never patched, when:
//!
//! * anything was deleted, or a node's labels changed, since the last call
//!   (the log's rewrite count moved) -- deletions are not applied as deltas;
//! * more additions arrived than the log holds;
//! * after the deltas, the state's node count, or for an unfiltered call its
//!   relationship count, disagrees with the store's. Every write path is
//!   meant to reach the log; this check is what makes one that does not cost
//!   a rebuild instead of a wrong answer.
//!
//! A rebuild gives the same answer a plain call would, so none of these is an
//! error. [`IncrementalStates::status`] says which of them last happened.
//!
//! One state is kept per algorithm and filter -- `label`, `edgeType` and, for
//! PageRank, `damping` -- and at most [`MAX_KEPT_STATES`] per graph, the
//! least recently used going first. A state holds a copy of the graph's
//! adjacency (PageRank) or a word per node (WCC, degree), so the bound is a
//! bound on memory.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use samyama_graph_algorithms::{
    IncrementalDegree, IncrementalPageRank, IncrementalPageRankError, IncrementalWcc, NodeId as AlgoNodeId,
    Progress, Stopped,
};
use serde::Serialize;

use crate::graph::structure_log::{Addition, CatchUp, LogPosition};
use crate::graph::{EdgeType, GraphStore, Label};

/// The most states one graph keeps. Each PageRank state copies the graph's
/// adjacency, so this bounds the memory a caller can make the engine hold by
/// varying filters, at a few times the graph's own edge arrays.
pub const MAX_KEPT_STATES: usize = 8;

/// Which algorithm a state is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Algorithm {
    PageRank,
    Wcc,
    Degree,
}

impl Algorithm {
    /// The name it is called by.
    pub fn call_name(self) -> &'static str {
        match self {
            Self::PageRank => "algo.pageRank",
            Self::Wcc => "algo.wcc",
            Self::Degree => "algo.degree",
        }
    }
}

/// What a state is the answer to: one per algorithm and set of arguments that
/// changes the answer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateKey {
    pub algorithm: Algorithm,
    pub label: Option<String>,
    pub edge_type: Option<String>,
    /// PageRank's damping, by bit pattern so the key can be hashed; 0 for the
    /// others.
    damping_bits: u64,
}

impl StateKey {
    pub fn new(algorithm: Algorithm, label: Option<&str>, edge_type: Option<&str>) -> Self {
        Self { algorithm, label: label.map(str::to_string), edge_type: edge_type.map(str::to_string), damping_bits: 0 }
    }

    pub fn page_rank(label: Option<&str>, edge_type: Option<&str>, damping: f64) -> Self {
        Self { damping_bits: damping.to_bits(), ..Self::new(Algorithm::PageRank, label, edge_type) }
    }
}

/// How a call got its answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum Refresh {
    /// No state was kept for these arguments; it was built from the graph.
    Built,
    /// The kept state took the additions since the last call.
    Updated { nodes: usize, relationships: usize },
    /// The kept state could not be brought up to date and was rebuilt.
    Rebuilt { reason: RebuildReason },
}

/// Why a kept state was rebuilt rather than updated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RebuildReason {
    /// Something was deleted, or a node's labels changed.
    Rewritten,
    /// More was added than the log holds.
    Dropped,
    /// After the deltas, the state's counts disagreed with the store's.
    CountMismatch,
}

impl From<CatchUp> for RebuildReason {
    fn from(c: CatchUp) -> Self {
        match c {
            CatchUp::Rewritten => Self::Rewritten,
            CatchUp::Dropped => Self::Dropped,
        }
    }
}

/// A kept state, as [`IncrementalStates::status`] reports it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StateStatus {
    pub algorithm: Algorithm,
    pub label: Option<String>,
    pub edge_type: Option<String>,
    pub nodes: usize,
    pub calls: u64,
    pub rebuilds: u64,
    pub last: Refresh,
}

enum Kept {
    PageRank(IncrementalPageRank),
    Wcc(IncrementalWcc),
    Degree(IncrementalDegree),
}

impl Kept {
    fn add_node(&mut self, id: AlgoNodeId) -> bool {
        match self {
            Self::PageRank(s) => s.add_node(id),
            Self::Wcc(s) => s.add_node(id),
            Self::Degree(s) => s.add_node(id),
        }
    }

    fn add_edge(&mut self, source: AlgoNodeId, target: AlgoNodeId) -> bool {
        match self {
            Self::PageRank(s) => s.add_edge(source, target),
            Self::Wcc(s) => s.add_edge(source, target),
            Self::Degree(s) => s.add_edge(source, target),
        }
    }

    fn node_count(&self) -> usize {
        match self {
            Self::PageRank(s) => s.node_count(),
            Self::Wcc(s) => s.node_count(),
            Self::Degree(s) => s.node_count(),
        }
    }
}

struct Saved {
    kept: Kept,
    at: LogPosition,
    /// Relationships the state holds, for the unfiltered count check.
    relationships: usize,
    calls: u64,
    rebuilds: u64,
    last: Refresh,
    last_used: u64,
}

type Slot = Arc<Mutex<Option<Saved>>>;

/// Every kept state of one graph. Lives in its `GraphStore`.
#[derive(Default)]
pub struct IncrementalStates {
    /// Held only to find or add a slot; a slot's own lock is held for the
    /// call, so a long first build of one state does not block another.
    slots: Mutex<HashMap<StateKey, Slot>>,
    clock: AtomicU64,
}

impl std::fmt::Debug for IncrementalStates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncrementalStates").field("kept", &lock(&self.slots).len()).finish()
    }
}

/// A poisoned lock means a call panicked part-way. The slot is then cleared
/// by the caller, which rebuilds, so the data behind the poison is never
/// trusted -- see [`IncrementalStates::slot`].
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Why an incremental call failed.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IncrementalError {
    /// Stopped by its progress hook -- the deadline -- while building the
    /// graph view.
    ViewStopped(Stopped),
    /// Stopped while solving from scratch.
    RunStopped(Stopped),
    /// PageRank's damping is outside `(0, 1)`.
    Damping(f64),
}

impl std::fmt::Display for IncrementalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ViewStopped(s) | Self::RunStopped(s) => s.fmt(f),
            Self::Damping(d) => write!(f, "damping must be above 0 and below 1, got {d}"),
        }
    }
}

impl std::error::Error for IncrementalError {}

impl IncrementalStates {
    /// The kept states, for diagnostics and tests.
    pub fn status(&self) -> Vec<StateStatus> {
        let slots: Vec<(StateKey, Slot)> = lock(&self.slots).iter().map(|(k, s)| (k.clone(), s.clone())).collect();
        let mut out: Vec<StateStatus> = slots
            .into_iter()
            .filter_map(|(key, slot)| {
                let guard = lock(&slot);
                guard.as_ref().map(|saved| StateStatus {
                    algorithm: key.algorithm,
                    label: key.label.clone(),
                    edge_type: key.edge_type.clone(),
                    nodes: saved.kept.node_count(),
                    calls: saved.calls,
                    rebuilds: saved.rebuilds,
                    last: saved.last,
                })
            })
            .collect();
        out.sort_by(|a, b| {
            (a.algorithm.call_name(), &a.label, &a.edge_type).cmp(&(b.algorithm.call_name(), &b.label, &b.edge_type))
        });
        out
    }

    /// The slot for `key`, made if missing, evicting the least recently used
    /// when the graph already keeps [`MAX_KEPT_STATES`].
    fn slot(&self, key: &StateKey) -> Slot {
        let mut slots = lock(&self.slots);
        if let Some(slot) = slots.get(key) {
            return slot.clone();
        }
        if slots.len() >= MAX_KEPT_STATES {
            // A slot whose lock is held is in use; its `last_used` is recent
            // in any case, so reading it as "now" keeps it.
            let now = self.clock.load(Ordering::Relaxed);
            let oldest = slots
                .iter()
                .min_by_key(|(_, slot)| match slot.try_lock() {
                    Ok(guard) => guard.as_ref().map_or(0, |s| s.last_used),
                    Err(_) => now,
                })
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest {
                slots.remove(&k);
            }
        }
        let slot: Slot = Arc::default();
        slots.insert(key.clone(), slot.clone());
        slot
    }
}

/// Does node `id` belong in a state filtered to `label`?
fn in_filter(store: &GraphStore, id: crate::graph::NodeId, label: Option<&Label>) -> bool {
    match (store.get_node(id), label) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some(node), Some(l)) => node.has_label(l),
    }
}

/// Apply the additions since `saved.at`, or say why they cannot be.
fn catch_up(store: &GraphStore, key: &StateKey, saved: &mut Saved) -> Result<Refresh, RebuildReason> {
    let label = key.label.as_deref().map(Label::new);
    let edge_type = key.edge_type.as_deref().map(EdgeType::new);
    let (mut nodes, mut relationships) = (0, 0);
    for addition in store.structure_log().since(saved.at)? {
        match addition {
            Addition::Node(id) => {
                if in_filter(store, id, label.as_ref()) && saved.kept.add_node(id.as_u64()) {
                    nodes += 1;
                }
            }
            Addition::Edge(id) => {
                let Some(edge) = store.get_edge(id) else { continue };
                if edge_type.as_ref().is_some_and(|t| edge.edge_type != *t) {
                    continue;
                }
                if saved.kept.add_edge(edge.source.as_u64(), edge.target.as_u64()) {
                    relationships += 1;
                }
            }
        }
    }
    saved.relationships += relationships;
    let expected_nodes = match &label {
        Some(l) => store.label_node_count(l),
        None => store.node_count(),
    };
    let unfiltered = label.is_none() && edge_type.is_none();
    if saved.kept.node_count() != expected_nodes || (unfiltered && saved.relationships != store.edge_count()) {
        return Err(RebuildReason::CountMismatch);
    }
    saved.at = store.structure_log().position();
    Ok(Refresh::Updated { nodes, relationships })
}

/// Run `read` on the state for `key`, brought up to date with `store` first.
///
/// `build` makes a state from a fresh view; `view_progress` and
/// `run_progress` are handed the view build and the first solve.
fn with_state<T>(
    store: &GraphStore,
    key: &StateKey,
    view_progress: &dyn Progress,
    run_progress: &dyn Progress,
    build: impl Fn(&crate::algo::GraphView, &dyn Progress) -> Result<Kept, IncrementalError>,
    read: impl FnOnce(&mut Kept) -> T,
) -> Result<(T, Refresh), IncrementalError> {
    let states = store.incremental_states();
    // Before reading the position: an addition after this point is recorded,
    // and everything before it is in the view built below.
    store.structure_log().start_recording();
    let slot = states.slot(key);
    // A poisoned slot held a state a panicking call may have half-updated:
    // drop it and rebuild.
    let mut guard = match slot.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            let mut g = poisoned.into_inner();
            *g = None;
            g
        }
    };

    let refresh = match guard.as_mut().map(|saved| catch_up(store, key, saved)) {
        Some(Ok(updated)) => updated,
        previous => {
            let at = store.structure_log().position();
            let view = crate::algo::build_view_with(
                store,
                key.label.as_deref(),
                key.edge_type.as_deref(),
                None,
                view_progress,
            )
            .map_err(IncrementalError::ViewStopped)?;
            let kept = build(&view, run_progress)?;
            let (calls, rebuilds, last) = match (&previous, guard.as_ref()) {
                (Some(Err(reason)), Some(old)) => (old.calls, old.rebuilds + 1, Refresh::Rebuilt { reason: *reason }),
                _ => (0, 0, Refresh::Built),
            };
            *guard = Some(Saved { kept, at, relationships: view.out_targets.len(), calls, rebuilds, last, last_used: 0 });
            last
        }
    };
    let saved = guard.as_mut().expect("a state was just kept or updated");
    saved.calls += 1;
    saved.last = refresh;
    saved.last_used = states.clock.fetch_add(1, Ordering::Relaxed) + 1;
    Ok((read(&mut saved.kept), refresh))
}

/// PageRank scores, summing to 1, for every node the filter admits.
pub fn page_rank(
    store: &GraphStore,
    label: Option<&str>,
    edge_type: Option<&str>,
    damping: f64,
    view_progress: &dyn Progress,
    run_progress: &dyn Progress,
) -> Result<(Vec<(AlgoNodeId, f64)>, Refresh), IncrementalError> {
    // Refused before a state is made, so a bad damping keeps nothing.
    if !(damping > 0.0 && damping < 1.0) {
        return Err(IncrementalError::Damping(damping));
    }
    let key = StateKey::page_rank(label, edge_type, damping);
    with_state(
        store,
        &key,
        view_progress,
        run_progress,
        |view, progress| match IncrementalPageRank::from_view(view, damping, progress) {
            Ok(s) => Ok(Kept::PageRank(s)),
            Err(IncrementalPageRankError::Damping(d)) => Err(IncrementalError::Damping(d)),
            Err(IncrementalPageRankError::Stopped(s)) => Err(IncrementalError::RunStopped(s)),
        },
        |kept| match kept {
            Kept::PageRank(s) => s.scores(),
            _ => unreachable!("a PageRank key holds a PageRank state"),
        },
    )
}

/// Every node the filter admits and its component id.
pub fn wcc(
    store: &GraphStore,
    label: Option<&str>,
    edge_type: Option<&str>,
    view_progress: &dyn Progress,
) -> Result<(Vec<(AlgoNodeId, usize)>, Refresh), IncrementalError> {
    let key = StateKey::new(Algorithm::Wcc, label, edge_type);
    with_state(
        store,
        &key,
        view_progress,
        &samyama_graph_algorithms::NoProgress,
        |view, _| Ok(Kept::Wcc(IncrementalWcc::from_view(view))),
        |kept| match kept {
            Kept::Wcc(s) => s.components(),
            _ => unreachable!("a WCC key holds a WCC state"),
        },
    )
}

/// Degree centrality for every node the filter admits.
pub fn degree(
    store: &GraphStore,
    label: Option<&str>,
    edge_type: Option<&str>,
    view_progress: &dyn Progress,
) -> Result<(Vec<(AlgoNodeId, f64)>, Refresh), IncrementalError> {
    let key = StateKey::new(Algorithm::Degree, label, edge_type);
    with_state(
        store,
        &key,
        view_progress,
        &samyama_graph_algorithms::NoProgress,
        |view, _| Ok(Kept::Degree(IncrementalDegree::from_view(view))),
        |kept| match kept {
            Kept::Degree(s) => s.scores(),
            _ => unreachable!("a degree key holds a degree state"),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::algo::NoProgress;
    use crate::graph::NodeId;

    fn ring(n: usize) -> (GraphStore, Vec<NodeId>) {
        let mut s = GraphStore::new();
        let ids: Vec<NodeId> = (0..n).map(|_| s.create_node("P")).collect();
        for i in 0..n {
            s.create_edge(ids[i], ids[(i + 1) % n], "L").unwrap();
        }
        (s, ids)
    }

    fn pr(s: &GraphStore) -> (HashMap<u64, f64>, Refresh) {
        let (scores, how) = page_rank(s, None, None, 0.85, &NoProgress, &NoProgress).unwrap();
        (scores.into_iter().collect(), how)
    }

    /// The batch PageRank, run to its fixed point.
    fn batch_pr(s: &GraphStore) -> HashMap<u64, f64> {
        let view = crate::algo::build_view(s, None, None, None);
        let config = crate::algo::PageRankConfig { iterations: 1000, tolerance: 0.0, ..Default::default() };
        crate::algo::page_rank(&view, config)
    }

    fn l1(a: &HashMap<u64, f64>, b: &HashMap<u64, f64>) -> f64 {
        assert_eq!(a.len(), b.len());
        a.iter().map(|(k, v)| (v - b[k]).abs()).sum()
    }

    /// The crate's stated bound for a graph with no dangling nodes.
    const BOUND: f64 = 2.0 * samyama_graph_algorithms::RESIDUAL_TOLERANCE / (1.0 - 0.85);

    #[test]
    fn first_call_builds_later_calls_update_and_both_match_a_batch_run() {
        let (mut s, ids) = ring(50);
        let (first, how) = pr(&s);
        assert_eq!(how, Refresh::Built);
        assert!(l1(&first, &batch_pr(&s)) <= BOUND);

        s.create_edge(ids[3], ids[30], "L").unwrap();
        let extra = s.create_node("P");
        s.create_edge(extra, ids[0], "L").unwrap();
        s.create_edge(ids[7], extra, "L").unwrap();
        let (second, how) = pr(&s);
        assert_eq!(how, Refresh::Updated { nodes: 1, relationships: 3 });
        assert!(l1(&second, &batch_pr(&s)) <= BOUND, "updated scores drifted from a batch run");

        let (_, how) = pr(&s);
        assert_eq!(how, Refresh::Updated { nodes: 0, relationships: 0 }, "an unchanged graph did work");
    }

    #[test]
    fn a_deletion_rebuilds_and_the_answer_is_still_right() {
        let (mut s, ids) = ring(30);
        pr(&s);
        let e = s.create_edge(ids[1], ids[20], "L").unwrap();
        s.delete_edge(e).unwrap();
        let (scores, how) = pr(&s);
        assert_eq!(how, Refresh::Rebuilt { reason: RebuildReason::Rewritten });
        assert!(l1(&scores, &batch_pr(&s)) <= BOUND);
        let status = s.incremental_states().status();
        assert_eq!((status[0].calls, status[0].rebuilds), (2, 1), "{status:?}");
    }

    #[test]
    fn a_label_change_rebuilds_a_label_filtered_state() {
        let (mut s, ids) = ring(10);
        let filtered = |s: &GraphStore| wcc(s, Some("Q"), None, &NoProgress).unwrap();
        s.add_label_to_node("default", ids[0], "Q").unwrap();
        let (rows, how) = filtered(&s);
        assert_eq!((rows.len(), how), (1, Refresh::Built));
        s.add_label_to_node("default", ids[1], "Q").unwrap();
        let (rows, how) = filtered(&s);
        assert_eq!(how, Refresh::Rebuilt { reason: RebuildReason::Rewritten });
        // 0 -> 1 is in the ring, so the two Q nodes are one component.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1, rows[1].1);
    }

    #[test]
    fn a_write_the_log_missed_is_caught_by_the_count_check() {
        let (mut s, ids) = ring(10);
        degree(&s, None, None, &NoProgress).unwrap();
        s.create_edge(ids[2], ids[6], "L").unwrap();
        // Every store path that adds a relationship logs it. One that did not
        // would look like this: the saved position already past the addition,
        // so the state never hears of it.
        {
            let slot = s.incremental_states().slot(&StateKey::new(Algorithm::Degree, None, None));
            let mut guard = slot.lock().unwrap();
            guard.as_mut().unwrap().at = s.structure_log().position();
        }
        let (rows, how) = degree(&s, None, None, &NoProgress).unwrap();
        assert_eq!(how, Refresh::Rebuilt { reason: RebuildReason::CountMismatch });
        let view = crate::algo::build_view(&s, None, None, None);
        let batch = crate::algo::degree_centrality(&view, true);
        let got: HashMap<u64, f64> = rows.into_iter().collect();
        for (i, id) in view.index_to_node.iter().enumerate() {
            assert_eq!(got[id].to_bits(), batch[i].to_bits(), "node {id}");
        }
    }

    #[test]
    fn a_node_the_log_missed_is_caught_by_the_count_check() {
        // A node with no relationships changes no relationship count, so only
        // the node count can notice it was missed.
        let (mut s, _) = ring(10);
        degree(&s, None, None, &NoProgress).unwrap();
        s.create_node("P");
        {
            let slot = s.incremental_states().slot(&StateKey::new(Algorithm::Degree, None, None));
            let mut guard = slot.lock().unwrap();
            guard.as_mut().unwrap().at = s.structure_log().position();
        }
        let (rows, how) = degree(&s, None, None, &NoProgress).unwrap();
        assert_eq!(how, Refresh::Rebuilt { reason: RebuildReason::CountMismatch });
        assert_eq!(rows.len(), 11, "the missed node is not in the answer");
    }

    #[test]
    fn filters_and_damping_keep_separate_states() {
        let (mut s, ids) = ring(6);
        s.create_edge(ids[0], ids[3], "M").unwrap();
        page_rank(&s, None, None, 0.85, &NoProgress, &NoProgress).unwrap();
        page_rank(&s, None, None, 0.5, &NoProgress, &NoProgress).unwrap();
        page_rank(&s, None, Some("M"), 0.85, &NoProgress, &NoProgress).unwrap();
        let (rows, _) = wcc(&s, None, Some("M"), &NoProgress).unwrap();
        // Only 0 -- 3 under `M`: four singletons and one pair.
        let mut ids_by_component: HashMap<usize, usize> = HashMap::new();
        for (_, c) in rows {
            *ids_by_component.entry(c).or_default() += 1;
        }
        assert_eq!(ids_by_component.len(), 5);
        assert_eq!(s.incremental_states().status().len(), 4);
    }

    #[test]
    fn an_edge_type_filter_ignores_other_relationships() {
        let (mut s, ids) = ring(6);
        wcc(&s, None, Some("M"), &NoProgress).unwrap();
        s.create_edge(ids[0], ids[3], "L").unwrap();
        s.create_edge(ids[1], ids[4], "M").unwrap();
        let (rows, how) = wcc(&s, None, Some("M"), &NoProgress).unwrap();
        assert_eq!(how, Refresh::Updated { nodes: 0, relationships: 1 });
        let c: HashMap<u64, usize> = rows.into_iter().collect();
        assert_eq!(c[&ids[1].as_u64()], c[&ids[4].as_u64()]);
        assert_ne!(c[&ids[0].as_u64()], c[&ids[3].as_u64()]);
    }

    #[test]
    fn at_most_the_bound_of_states_is_kept_and_the_oldest_goes() {
        let (s, _) = ring(4);
        for i in 0..=MAX_KEPT_STATES {
            let damping = 0.5 + i as f64 / 100.0;
            page_rank(&s, None, None, damping, &NoProgress, &NoProgress).unwrap();
        }
        let kept = lock(&s.incremental_states().slots);
        assert_eq!(kept.len(), MAX_KEPT_STATES);
        assert!(!kept.contains_key(&StateKey::page_rank(None, None, 0.5)), "the oldest state was kept");
    }

    #[test]
    fn bad_damping_is_refused_and_keeps_nothing() {
        let (s, _) = ring(4);
        for d in [0.0, 1.0, 2.0, f64::NAN] {
            let err = page_rank(&s, None, None, d, &NoProgress, &NoProgress).unwrap_err();
            assert!(matches!(err, IncrementalError::Damping(_)), "{err:?}");
        }
        assert!(s.incremental_states().status().is_empty());
    }

    #[test]
    fn a_stopped_first_build_keeps_nothing_and_says_where_it_stopped() {
        struct Never;
        impl Progress for Never {
            fn step(&self, _: usize, _: usize) -> bool {
                false
            }
        }
        let (s, _) = ring(4);
        let err = page_rank(&s, None, None, 0.85, &Never, &NoProgress).unwrap_err();
        assert!(matches!(err, IncrementalError::ViewStopped(_)), "{err:?}");
        let err = page_rank(&s, None, None, 0.85, &NoProgress, &Never).unwrap_err();
        assert!(matches!(err, IncrementalError::RunStopped(Stopped { done: 0, .. })), "{err:?}");
        assert!(s.incremental_states().status().is_empty(), "a stopped build was kept");
    }
}
