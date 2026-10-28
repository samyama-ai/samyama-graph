//! Named graph projections (ALGO-05, #1681).
//!
//! Every `CALL algo.*` builds a compact, integer-indexed copy of the graph (a
//! [`GraphView`]) for the algorithm to run on, and drops it when the call ends.
//! Running PageRank, then Louvain, then WCC over the same subgraph builds that
//! copy three times. A **projection** builds it once, under a name, and keeps it:
//!
//! ```cypher
//! CREATE GRAPH PROJECTION social NODES :Person RELATIONSHIPS :KNOWS
//! CALL algo.pageRank({graph: 'social'}) YIELD node, score
//! REFRESH GRAPH PROJECTION social
//! SHOW GRAPH PROJECTIONS
//! DROP GRAPH PROJECTION social
//! ```
//!
//! # What a projection is, exactly
//!
//! A [`ProjectionSpec`] names at most one node label, one relationship type and
//! one weight property: the three things `crate::algo::build_view` takes, and
//! nothing it does not. The view is built by that same function, so an
//! algorithm on a projection sees exactly the graph a plain call with the same
//! `label`, `edgeType` and weight would see.
//!
//! # A snapshot, not a live view
//!
//! A projection is the graph as it was when it was built or last refreshed.
//! Writes after that do not reach it; [`ProjectionInfo::stale`] says when the
//! data may have changed since (any data change counts, inside the projection
//! or not), and `REFRESH` rebuilds it. A node deleted since the
//! build is skipped by the algorithms that look it up, as it would be on any
//! view built just before the delete.
//!
//! # Memory, and what is not here yet
//!
//! Each projection reports its resident bytes ([`GraphView::heap_bytes`]),
//! listed by `SHOW GRAPH PROJECTIONS` and in the store's index memory report.
//! The bytes are **reported, not capped**: a memory budget is a later
//! requirement (ALGO-12). Projections live in memory only and are not written
//! to disk, so a restart forgets them, as it forgets hierarchy indexes.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::algo::GraphView;
use crate::graph::GraphStore;

/// Which part of the graph a projection holds.
///
/// `None` means "every": every node, every relationship type, no weight.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionSpec {
    /// Only nodes with this label, and relationships between them.
    pub node_label: Option<String>,
    /// Only relationships of this type.
    pub edge_type: Option<String>,
    /// A numeric relationship property used as the weight; a relationship
    /// without a numeric value for it weighs 1.0, as in every weighted view.
    pub weight_property: Option<String>,
}

/// A built projection: its spec, its view, and when it was built.
pub struct GraphProjection {
    /// The name it was created under.
    pub name: String,
    /// What it holds.
    pub spec: ProjectionSpec,
    /// The view algorithms run on. Shared, so a long algorithm keeps the view
    /// it started on even if the projection is refreshed or dropped meanwhile.
    pub view: Arc<GraphView>,
    /// `GraphStore::data_changes` when the view was built.
    pub built_at_change: u64,
    /// `view.heap_bytes()`, computed once at build time.
    pub bytes: usize,
}

/// What `SHOW GRAPH PROJECTIONS` reports for one projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionInfo {
    pub name: String,
    pub spec: ProjectionSpec,
    pub nodes: usize,
    pub relationships: usize,
    pub bytes: usize,
    /// Whether a node, relationship, label or property may have changed
    /// since it was built. Never false after a change; it can be true after a
    /// write that touched only data outside the projection.
    pub stale: bool,
}

/// Why a projection statement was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionError {
    /// `CREATE` of a name already in use, without `IF NOT EXISTS`.
    AlreadyExists(String),
    /// A name that no projection has.
    NotFound(String),
    /// A name with nothing in it.
    EmptyName,
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProjectionError::AlreadyExists(name) => write!(
                f,
                "a graph projection named `{name}` already exists; drop it first, \
                 refresh it, or add IF NOT EXISTS"
            ),
            ProjectionError::NotFound(name) => write!(
                f,
                "there is no graph projection named `{name}`; SHOW GRAPH PROJECTIONS lists them"
            ),
            ProjectionError::EmptyName => f.write_str("a graph projection needs a non-empty name"),
        }
    }
}

impl std::error::Error for ProjectionError {}

/// Outcome of a `CREATE` that may have found the name taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Created {
    /// A new projection was built.
    Built(ProjectionInfo),
    /// `IF NOT EXISTS` found one under this name and left it as it was.
    AlreadyThere(ProjectionInfo),
}

/// The named projections of one store.
#[derive(Default)]
pub struct GraphProjections {
    /// Name-keyed, a `BTreeMap` so `SHOW` lists them in a stable order.
    entries: RwLock<BTreeMap<String, Arc<GraphProjection>>>,
    /// Moves on with every create, refresh and drop.
    ///
    /// The query result cache keys on it, alongside the data epoch: an answer
    /// computed on a projection is not served once that projection has been
    /// rebuilt or replaced. A counter of its own rather than a bump of the data
    /// epoch, because no data changed, and the epoch also says whether a
    /// snapshot's recorded sources are current.
    generation: AtomicU64,
}

impl fmt::Debug for GraphProjections {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `GraphView` has no `Debug`, and the views would be noise anyway.
        f.debug_struct("GraphProjections")
            .field("names", &self.read().keys().collect::<Vec<_>>())
            .field("generation", &self.generation())
            .finish()
    }
}

impl GraphProjections {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// The registry, for reading.
    ///
    /// A poisoned lock is recovered rather than propagated: the map is only
    /// ever changed by a single `insert` or `remove`, which cannot leave it
    /// half-updated, so the data behind a poisoned lock is still consistent.
    fn read(&self) -> RwLockReadGuard<'_, BTreeMap<String, Arc<GraphProjection>>> {
        self.entries.read().unwrap_or_else(|e| e.into_inner())
    }

    /// The registry, for writing. See [`GraphProjections::read`] on poisoning.
    fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Arc<GraphProjection>>> {
        self.entries.write().unwrap_or_else(|e| e.into_inner())
    }

    /// The current generation. See the field.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    fn changed(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn checked_name(name: &str) -> Result<&str, ProjectionError> {
        if name.trim().is_empty() {
            Err(ProjectionError::EmptyName)
        } else {
            Ok(name)
        }
    }

    /// Build the view for `spec` from `store` as it is now.
    fn build(store: &GraphStore, name: &str, spec: ProjectionSpec) -> GraphProjection {
        let view = crate::algo::build_view(
            store,
            spec.node_label.as_deref(),
            spec.edge_type.as_deref(),
            spec.weight_property.as_deref(),
        );
        GraphProjection {
            name: name.to_string(),
            spec,
            bytes: view.heap_bytes(),
            view: Arc::new(view),
            built_at_change: store.data_changes(),
        }
    }

    fn info(projection: &GraphProjection, data_changes: u64) -> ProjectionInfo {
        ProjectionInfo {
            name: projection.name.clone(),
            spec: projection.spec.clone(),
            nodes: projection.view.node_count,
            relationships: projection.view.edge_count(),
            bytes: projection.bytes,
            stale: projection.built_at_change != data_changes,
        }
    }

    /// `CREATE GRAPH PROJECTION`.
    ///
    /// With `if_not_exists`, a name already in use is left as it is and
    /// reported, even when its spec differs: `IF NOT EXISTS` means "do nothing
    /// if it is there", and the returned spec shows what is there.
    pub fn create(
        &self,
        store: &GraphStore,
        name: &str,
        spec: ProjectionSpec,
        if_not_exists: bool,
    ) -> Result<Created, ProjectionError> {
        let name = Self::checked_name(name)?;
        if let Some(existing) = self.read().get(name) {
            return if if_not_exists {
                Ok(Created::AlreadyThere(Self::info(
                    existing,
                    store.data_changes(),
                )))
            } else {
                Err(ProjectionError::AlreadyExists(name.to_string()))
            };
        }
        // Built outside the lock: a large view takes a while, and other
        // statements may read the registry meanwhile.
        let built = Arc::new(Self::build(store, name, spec));
        let mut entries = self.write();
        // Statements that write run one at a time against `&mut GraphStore`,
        // so no other CREATE can take the name between the check above and
        // here; checked again anyway, because a registry should not rely on
        // its caller's locking for its own invariant.
        if entries.contains_key(name) {
            return Err(ProjectionError::AlreadyExists(name.to_string()));
        }
        let info = Self::info(&built, store.data_changes());
        entries.insert(name.to_string(), built);
        drop(entries);
        self.changed();
        Ok(Created::Built(info))
    }

    /// `REFRESH GRAPH PROJECTION`: rebuild from the data as it is now, with the
    /// spec it was created with.
    pub fn refresh(
        &self,
        store: &GraphStore,
        name: &str,
    ) -> Result<ProjectionInfo, ProjectionError> {
        let name = Self::checked_name(name)?;
        let spec = self
            .read()
            .get(name)
            .map(|p| p.spec.clone())
            .ok_or_else(|| ProjectionError::NotFound(name.to_string()))?;
        let rebuilt = Arc::new(Self::build(store, name, spec));
        let info = Self::info(&rebuilt, store.data_changes());
        self.write().insert(name.to_string(), rebuilt);
        self.changed();
        Ok(info)
    }

    /// `DROP GRAPH PROJECTION`. Returns whether one was dropped; with
    /// `if_exists`, a missing name is not an error.
    pub fn drop_projection(&self, name: &str, if_exists: bool) -> Result<bool, ProjectionError> {
        let name = Self::checked_name(name)?;
        let removed = self.write().remove(name).is_some();
        if removed {
            self.changed();
            Ok(true)
        } else if if_exists {
            Ok(false)
        } else {
            Err(ProjectionError::NotFound(name.to_string()))
        }
    }

    /// The projection under `name`, if any.
    pub fn get(&self, name: &str) -> Option<Arc<GraphProjection>> {
        self.read().get(name).cloned()
    }

    /// Every projection, by name, with staleness measured against
    /// `data_changes` (`GraphStore::data_changes`).
    pub fn list(&self, data_changes: u64) -> Vec<ProjectionInfo> {
        self.read()
            .values()
            .map(|p| Self::info(p, data_changes))
            .collect()
    }

    /// Whether there are none.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::PropertyValue;

    const PERSON: &str = "Person";
    const KNOWS: &str = "KNOWS";
    const COST: &str = "cost";
    const NAME: &str = "social";

    /// Three people who know each other in a chain, and one robot no one knows.
    fn store() -> GraphStore {
        let mut s = GraphStore::new();
        let a = s.create_node(PERSON);
        let b = s.create_node(PERSON);
        let c = s.create_node(PERSON);
        s.create_node("Robot");
        for (from, to, cost) in [(a, b, 2.0), (b, c, 3.0)] {
            let e = s.create_edge(from, to, KNOWS).unwrap();
            s.set_edge_property(e, COST, PropertyValue::Float(cost))
                .unwrap();
        }
        s
    }

    fn people_who_know() -> ProjectionSpec {
        ProjectionSpec {
            node_label: Some(PERSON.into()),
            edge_type: Some(KNOWS.into()),
            weight_property: None,
        }
    }

    fn built(created: Created) -> ProjectionInfo {
        match created {
            Created::Built(info) => info,
            other => panic!("expected a new projection, got {other:?}"),
        }
    }

    #[test]
    fn create_builds_the_view_the_spec_names() {
        let s = store();
        let projections = GraphProjections::new();
        let info = built(
            projections
                .create(&s, NAME, people_who_know(), false)
                .unwrap(),
        );
        assert_eq!((info.nodes, info.relationships), (3, 2));
        assert!(info.bytes > 0);
        assert!(!info.stale);
        // Every node and relationship when the spec names nothing.
        let all = built(
            projections
                .create(&s, "all", ProjectionSpec::default(), false)
                .unwrap(),
        );
        assert_eq!(all.nodes, 4);
    }

    #[test]
    fn a_projection_holds_the_same_graph_as_a_plain_view() {
        let s = store();
        let projections = GraphProjections::new();
        let spec = ProjectionSpec {
            weight_property: Some(COST.into()),
            ..people_who_know()
        };
        projections.create(&s, NAME, spec, false).unwrap();
        let view = projections.get(NAME).unwrap().view.clone();
        let direct = crate::algo::build_view(&s, Some(PERSON), Some(KNOWS), Some(COST));
        assert_eq!(view.index_to_node, direct.index_to_node);
        assert_eq!(view.out_targets, direct.out_targets);
        assert_eq!(view.weights, direct.weights);
    }

    #[test]
    fn a_taken_name_is_refused_unless_if_not_exists() {
        let s = store();
        let projections = GraphProjections::new();
        projections
            .create(&s, NAME, people_who_know(), false)
            .unwrap();
        let err = projections
            .create(&s, NAME, ProjectionSpec::default(), false)
            .unwrap_err();
        assert_eq!(err, ProjectionError::AlreadyExists(NAME.into()));
        assert!(err.to_string().contains("IF NOT EXISTS"), "{err}");
        // IF NOT EXISTS keeps the one that is there, spec and all.
        match projections
            .create(&s, NAME, ProjectionSpec::default(), true)
            .unwrap()
        {
            Created::AlreadyThere(info) => assert_eq!(info.spec, people_who_know()),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn writes_make_it_stale_and_refresh_brings_it_up_to_date() {
        let mut s = store();
        let projections = GraphProjections::new();
        projections
            .create(&s, NAME, people_who_know(), false)
            .unwrap();
        let d = s.create_node(PERSON);
        assert!(
            projections.list(s.data_changes())[0].stale,
            "a new node is not reflected"
        );
        assert_eq!(
            projections.list(s.data_changes())[0].nodes,
            3,
            "a snapshot, not a live view"
        );
        let _ = d;
        let info = projections.refresh(&s, NAME).unwrap();
        assert_eq!(info.nodes, 4);
        assert!(!info.stale);
    }

    #[test]
    fn a_view_in_use_survives_a_refresh_or_a_drop() {
        let s = store();
        let projections = GraphProjections::new();
        projections
            .create(&s, NAME, people_who_know(), false)
            .unwrap();
        let in_use = projections.get(NAME).unwrap().view.clone();
        projections.refresh(&s, NAME).unwrap();
        projections.drop_projection(NAME, false).unwrap();
        assert_eq!(in_use.node_count, 3, "the running algorithm keeps its view");
    }

    #[test]
    fn drop_removes_it_and_if_exists_tolerates_a_missing_name() {
        let s = store();
        let projections = GraphProjections::new();
        projections
            .create(&s, NAME, people_who_know(), false)
            .unwrap();
        assert_eq!(projections.drop_projection(NAME, false), Ok(true));
        assert!(projections.get(NAME).is_none());
        assert!(projections.is_empty());
        assert_eq!(
            projections.drop_projection(NAME, false),
            Err(ProjectionError::NotFound(NAME.into()))
        );
        assert_eq!(projections.drop_projection(NAME, true), Ok(false));
    }

    #[test]
    fn refresh_of_a_missing_name_is_refused() {
        let s = store();
        let err = GraphProjections::new().refresh(&s, NAME).unwrap_err();
        assert_eq!(err, ProjectionError::NotFound(NAME.into()));
        assert!(err.to_string().contains("SHOW GRAPH PROJECTIONS"), "{err}");
    }

    #[test]
    fn an_empty_name_is_refused_everywhere() {
        let s = store();
        let projections = GraphProjections::new();
        for name in ["", "   "] {
            assert_eq!(
                projections.create(&s, name, ProjectionSpec::default(), false),
                Err(ProjectionError::EmptyName)
            );
            assert_eq!(
                projections.refresh(&s, name),
                Err(ProjectionError::EmptyName)
            );
            assert_eq!(
                projections.drop_projection(name, true),
                Err(ProjectionError::EmptyName)
            );
        }
    }

    #[test]
    fn the_generation_moves_on_every_change_and_only_then() {
        let s = store();
        let projections = GraphProjections::new();
        let start = projections.generation();
        projections
            .create(&s, NAME, people_who_know(), false)
            .unwrap();
        let after_create = projections.generation();
        assert!(after_create > start);
        // Nothing changed: no move.
        projections
            .create(&s, NAME, people_who_know(), true)
            .unwrap();
        let _ = projections.drop_projection("missing", true);
        let _ = projections.create(&s, NAME, people_who_know(), false);
        assert_eq!(projections.generation(), after_create);
        projections.refresh(&s, NAME).unwrap();
        let after_refresh = projections.generation();
        assert!(after_refresh > after_create);
        projections.drop_projection(NAME, false).unwrap();
        assert!(projections.generation() > after_refresh);
    }

    #[test]
    fn list_is_in_name_order() {
        let s = store();
        let projections = GraphProjections::new();
        for name in ["zeta", "alpha", "mid"] {
            projections
                .create(&s, name, ProjectionSpec::default(), false)
                .unwrap();
        }
        let names: Vec<String> = projections
            .list(s.data_changes())
            .into_iter()
            .map(|p| p.name)
            .collect();
        assert_eq!(names, ["alpha", "mid", "zeta"]);
    }

    #[test]
    fn debug_lists_names_without_the_views() {
        let s = store();
        let projections = GraphProjections::new();
        projections
            .create(&s, NAME, ProjectionSpec::default(), false)
            .unwrap();
        let shown = format!("{projections:?}");
        assert!(shown.contains(NAME), "{shown}");
    }
}
