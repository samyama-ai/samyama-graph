//! Hierarchy index registry — declaration, lifecycle, staleness (ADR-035 §6).
//!
//! OEH is a **static** index: it is built from a snapshot of the covering relation and does
//! not maintain itself under writes. Rather than pretend otherwise, the manager tracks
//! staleness explicitly:
//!
//! - Mutating an edge whose type is part of a hierarchy's covering relation marks that
//!   hierarchy stale. So does writing the declared measure property.
//! - A stale index is invisible to the planner by default, so a query silently falls back
//!   to variable-length expansion and returns a *correct* answer rather than a fast wrong
//!   one. `EXPLAIN` reports the fallback and its reason.
//! - `REBUILD` rebuilds from the current graph.
//!
//! Declining is a first-class outcome too: `create` on a high-width DAG stores the entry
//! with a diagnostic and no index, so `SHOW HIERARCHY INDEXES` can explain why the planner
//! is not using it (see the honest-scope section of ADR-035).

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use crate::graph::types::{EdgeType, Label, NodeId};
use crate::graph::{GraphStore, PropertyValue};

use super::monoid::{RollupOp, RollupValue};
use super::oeh::{Encoding, OehIndex};
use super::poset::{HierarchyError, HierarchyResult, Poset};

/// Which node property a roll-up aggregates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasureSpec {
    /// Optional label restriction; `None` means "any node in the hierarchy".
    pub label: Option<Label>,
    /// Property name.
    pub property: String,
}

/// A hierarchy index declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HierarchySpec {
    /// Index name, unique per store.
    pub name: String,
    /// Edge types forming the covering relation.
    pub edge_types: Vec<EdgeType>,
    /// Whether stored edges point `parent -> child` instead of `child -> parent`.
    pub reverse: bool,
    /// Optional measure for roll-up. COUNT works without one.
    pub measure: Option<MeasureSpec>,
    /// Monoids to build range structures for.
    pub ops: Vec<RollupOp>,
}

impl HierarchySpec {
    /// A minimal declaration: covering relation only, COUNT roll-up available for free.
    pub fn new(name: impl Into<String>, edge_types: Vec<EdgeType>) -> Self {
        HierarchySpec {
            name: name.into(),
            edge_types,
            reverse: false,
            measure: None,
            ops: vec![RollupOp::Count],
        }
    }

    /// Declare a measure and the monoids to support over it.
    pub fn with_measure(
        mut self,
        label: Option<Label>,
        property: impl Into<String>,
        ops: Vec<RollupOp>,
    ) -> Self {
        self.measure = Some(MeasureSpec {
            label,
            property: property.into(),
        });
        self.ops = ops;
        self
    }
}

/// Why the planner is not using an index right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unusable {
    /// The structural probe declined this poset; a 2-hop index is the right substrate.
    Declined(String),
    /// The graph changed under the index.
    Stale,
}

/// One registered hierarchy.
#[derive(Debug)]
pub struct HierarchyEntry {
    /// The declaration.
    pub spec: HierarchySpec,
    /// The built index; `None` when the probe declined.
    pub index: Option<OehIndex>,
    /// Set when the probe declined, carrying the diagnostic.
    pub declined: Option<String>,
    /// Chain width measured by the probe when it declined. Kept because it is the whole
    /// reason for the decline: reporting "n/a" here would hide the one number that tells a
    /// user how far out of regime their poset is.
    pub declined_width: Option<usize>,
    /// Whether the graph has changed since the build.
    pub stale: bool,
    /// Nodes covered at build time.
    pub nodes: usize,
    /// Covering edges at build time.
    pub edges: usize,
}

impl HierarchyEntry {
    /// Is this index usable by the planner (built, in regime, and fresh)?
    pub fn usable(&self) -> bool {
        self.index.is_some() && !self.stale && self.declined.is_none()
    }

    /// Why it is not usable, if it is not.
    pub fn unusable_reason(&self) -> Option<Unusable> {
        if let Some(d) = &self.declined {
            return Some(Unusable::Declined(d.clone()));
        }
        if self.stale {
            return Some(Unusable::Stale);
        }
        None
    }
}

/// A row of `SHOW HIERARCHY INDEXES`.
#[derive(Debug, Clone, PartialEq)]
pub struct HierarchyInfo {
    /// Index name.
    pub name: String,
    /// Covering-relation edge types.
    pub edge_types: Vec<String>,
    /// Selected encoding, or `None` when declined.
    pub encoding: Option<&'static str>,
    /// Node count of the poset.
    pub nodes: usize,
    /// Covering-edge count.
    pub edges: usize,
    /// Chain width, in chain mode.
    pub width: Option<usize>,
    /// Declared measure property.
    pub measure: Option<String>,
    /// Monoids with a built range structure.
    pub ops: Vec<&'static str>,
    /// Approximate resident size.
    pub bytes: usize,
    /// Bytes held by the order embedding alone — the like-for-like comparison against a
    /// 2-hop index, which answers subsumption and has no roll-up analogue.
    pub structural_bytes: usize,
    /// Bytes held by the roll-up range structures.
    pub rollup_bytes: usize,
    /// Whether the graph changed under the index.
    pub stale: bool,
    /// Decline diagnostic, if any.
    pub declined: Option<String>,
}

/// Registry of hierarchy indexes for one store.
#[derive(Debug, Default)]
pub struct HierarchyIndexManager {
    /// Name-keyed registry.
    ///
    /// A `BTreeMap` rather than a `HashMap` because iteration order matters: when several
    /// hierarchies could answer the same question the choice must be reproducible, not
    /// dependent on hash seeding. Getting that from the container is also what makes the
    /// hot path cheap — `usable_containing` runs once per row inside `subsumes()`, and it
    /// previously collected every key into a `Vec` and sorted it on each call.
    entries: RwLock<BTreeMap<String, Arc<RwLock<HierarchyEntry>>>>,
}

impl HierarchyIndexManager {
    /// Empty registry.
    pub fn new() -> Self {
        HierarchyIndexManager {
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    /// Declare and build a hierarchy index.
    ///
    /// A declined build is **not** an error: the entry is registered with its diagnostic so
    /// `SHOW` can explain the planner's behaviour, and `Ok(info)` is returned with
    /// `encoding: None`. A cycle in the covering relation *is* an error — that is a data
    /// bug that would make every roll-up wrong.
    pub fn create(
        &self,
        store: &GraphStore,
        spec: HierarchySpec,
    ) -> HierarchyResult<HierarchyInfo> {
        {
            let entries = self.entries.read().unwrap();
            if entries.contains_key(&spec.name) {
                return Err(HierarchyError::DuplicateIndex(spec.name.clone()));
            }
        }
        let entry = Self::build_entry(store, spec)?;
        let name = entry.spec.name.clone();
        let info = Self::info_for(&entry);
        self.entries
            .write()
            .unwrap()
            .insert(name, Arc::new(RwLock::new(entry)));
        Ok(info)
    }

    fn build_entry(store: &GraphStore, spec: HierarchySpec) -> HierarchyResult<HierarchyEntry> {
        let poset = Poset::from_store(store, &spec.edge_types, spec.reverse)?;
        let (nodes, edges) = (poset.n(), poset.m());
        match OehIndex::build(poset) {
            Ok(mut index) => {
                if let Some(measure) = &spec.measure {
                    let values = read_measure(store, index.poset(), measure);
                    index.set_measure(values, &spec.ops);
                }
                Ok(HierarchyEntry {
                    spec,
                    index: Some(index),
                    declined: None,
                    declined_width: None,
                    stale: false,
                    nodes,
                    edges,
                })
            }
            Err(e @ HierarchyError::WidthTooHigh { .. }) => {
                let width = match e {
                    HierarchyError::WidthTooHigh { width, .. } => Some(width),
                    _ => None,
                };
                Ok(HierarchyEntry {
                    spec,
                    index: None,
                    declined: Some(e.to_string()),
                    declined_width: width,
                    stale: false,
                    nodes,
                    edges,
                })
            }
            Err(other) => Err(other),
        }
    }

    /// Rebuild an index from the current graph, clearing staleness.
    pub fn rebuild(&self, store: &GraphStore, name: &str) -> HierarchyResult<HierarchyInfo> {
        let spec = {
            let entries = self.entries.read().unwrap();
            let entry = entries
                .get(name)
                .ok_or_else(|| HierarchyError::NoSuchIndex(name.to_string()))?;
            let guard = entry.read().unwrap();
            guard.spec.clone()
        };
        let rebuilt = Self::build_entry(store, spec)?;
        let info = Self::info_for(&rebuilt);
        let entries = self.entries.read().unwrap();
        if let Some(slot) = entries.get(name) {
            *slot.write().unwrap() = rebuilt;
        }
        Ok(info)
    }

    /// Drop an index.
    pub fn drop_index(&self, name: &str) -> HierarchyResult<()> {
        self.entries
            .write()
            .unwrap()
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| HierarchyError::NoSuchIndex(name.to_string()))
    }

    /// Fetch an entry by name, stale or not.
    pub fn get(&self, name: &str) -> Option<Arc<RwLock<HierarchyEntry>>> {
        self.entries.read().unwrap().get(name).cloned()
    }

    /// The planner's lookup: a **usable** index whose covering relation includes
    /// `edge_type`.
    ///
    /// Returning `None` for a stale or declined index is the whole point — the query then
    /// plans a variable-length expansion and stays correct.
    pub fn usable_for_edge_type(
        &self,
        edge_type: &EdgeType,
    ) -> Option<Arc<RwLock<HierarchyEntry>>> {
        // Iteration is name-ordered, so the choice among several covering hierarchies is
        // deterministic without a sort.
        let entries = self.entries.read().unwrap();
        entries
            .values()
            .find(|e| {
                let g = e.read().unwrap();
                g.usable() && g.spec.edge_types.iter().any(|t| t == edge_type)
            })
            .map(Arc::clone)
    }

    /// Any entry covering `edge_type`, usable or not — for `EXPLAIN` to report why a
    /// rewrite did not fire.
    pub fn any_for_edge_type(&self, edge_type: &EdgeType) -> Option<Arc<RwLock<HierarchyEntry>>> {
        let entries = self.entries.read().unwrap();
        entries
            .values()
            .find(|e| {
                e.read()
                    .unwrap()
                    .spec
                    .edge_types
                    .iter()
                    .any(|t| t == edge_type)
            })
            .map(Arc::clone)
    }

    /// A usable index by name.
    pub fn usable_named(&self, name: &str) -> Option<Arc<RwLock<HierarchyEntry>>> {
        let e = self.get(name)?;
        let ok = e.read().unwrap().usable();
        if ok {
            Some(e)
        } else {
            None
        }
    }

    /// The first usable index (name-sorted, for determinism) whose poset contains every
    /// node in `ids`.
    ///
    /// This is what lets `subsumes(x, y)` work without the caller naming an index: in a
    /// graph with one ontology hierarchy — the common case — there is nothing to choose.
    /// When several hierarchies could answer, the name order makes the choice reproducible
    /// rather than dependent on hash iteration order.
    pub fn usable_containing(&self, ids: &[NodeId]) -> Option<Arc<RwLock<HierarchyEntry>>> {
        let entries = self.entries.read().unwrap();
        for e in entries.values() {
            let g = e.read().unwrap();
            if !g.usable() {
                continue;
            }
            if let Some(idx) = &g.index {
                if ids.iter().all(|&id| idx.poset().idx(id).is_some()) {
                    drop(g);
                    return Some(Arc::clone(e));
                }
            }
        }
        None
    }

    /// Mark every hierarchy built on `edge_type` stale. Called from edge writes.
    pub fn mark_stale_for_edge_type(&self, edge_type: &EdgeType) {
        let entries = self.entries.read().unwrap();
        for entry in entries.values() {
            let mut g = entry.write().unwrap();
            if g.spec.edge_types.iter().any(|t| t == edge_type) {
                g.stale = true;
            }
        }
    }

    /// Apply a measure write in place where possible, falling back to invalidation.
    ///
    /// A write to the declared measure changes a value, not the shape of the poset, so the
    /// order embedding stays valid and the range structures can absorb the change in
    /// O(log n). Marking the index stale for this — as the first implementation did — meant
    /// a full rebuild to reflect one number, which on a 2.9M-node taxonomy is nine seconds
    /// (#351).
    ///
    /// Returns the number of indexes updated in place. Any hierarchy that cannot take the
    /// update — no measure attached yet, or the node is outside its poset — is marked stale
    /// instead, so correctness never depends on the fast path succeeding.
    pub fn update_measure(&self, node: NodeId, property: &str, value: &PropertyValue) -> usize {
        let entries = self.entries.read().unwrap();
        let mut updated = 0usize;
        for e in entries.values() {
            let mut g = e.write().unwrap();
            if g.spec
                .measure
                .as_ref()
                .is_none_or(|m| m.property != property)
            {
                continue;
            }
            let rollup = to_rollup_value(value);
            let applied = match g.index.as_mut() {
                Some(idx) => idx.update_measure(node, rollup),
                None => false,
            };
            if applied {
                updated += 1;
            } else {
                g.stale = true;
            }
        }
        updated
    }

    /// Mark every hierarchy whose declared measure is `property` stale. Called from node
    /// property writes.
    pub fn mark_stale_for_property(&self, property: &str) {
        let entries = self.entries.read().unwrap();
        for entry in entries.values() {
            let mut g = entry.write().unwrap();
            if g.spec
                .measure
                .as_ref()
                .is_some_and(|m| m.property == property)
            {
                g.stale = true;
            }
        }
    }

    /// Is anything registered? Lets hot write paths skip the staleness scan entirely.
    pub fn is_empty(&self) -> bool {
        self.entries.read().unwrap().is_empty()
    }

    /// Is *any* registered index usable — present, built and not stale?
    ///
    /// The difference from `is_empty` is what a hierarchy function should say
    /// when it cannot find an index covering its arguments. "These two nodes
    /// are in no declared hierarchy" is a legitimate `false`; "there is no
    /// declared hierarchy at all, or every one of them is stale" is a question
    /// the engine cannot answer, and answering `false` to it is a guess that
    /// looks like a result (#721).
    pub fn any_usable(&self) -> bool {
        self.entries.read().unwrap().values().any(|e| e.read().unwrap().usable())
    }

    /// All registered indexes, name-sorted.
    pub fn list(&self) -> Vec<HierarchyInfo> {
        let entries = self.entries.read().unwrap();
        // BTreeMap iteration is already name-ordered.
        entries
            .values()
            .map(|e| Self::info_for(&e.read().unwrap()))
            .collect()
    }

    fn info_for(entry: &HierarchyEntry) -> HierarchyInfo {
        let (encoding, width, bytes, structural_bytes, rollup_bytes, ops) = match &entry.index {
            Some(idx) => (
                Some(idx.encoding().name()),
                idx.width(),
                idx.size_bytes(),
                idx.structural_bytes(),
                idx.rollup_bytes(),
                entry
                    .spec
                    .ops
                    .iter()
                    .filter(|op| idx.has_rollup(**op))
                    .map(|op| op.name())
                    .collect(),
            ),
            None => (None, entry.declined_width, 0, 0, 0, Vec::new()),
        };
        HierarchyInfo {
            name: entry.spec.name.clone(),
            edge_types: entry
                .spec
                .edge_types
                .iter()
                .map(|t| t.as_str().to_string())
                .collect(),
            encoding,
            nodes: entry.nodes,
            edges: entry.edges,
            width,
            measure: entry.spec.measure.as_ref().map(|m| m.property.clone()),
            ops,
            bytes,
            structural_bytes,
            rollup_bytes,
            stale: entry.stale,
            declined: entry.declined.clone(),
        }
    }
}

/// Read the declared measure for every node of the poset.
///
/// **Columnar first.** After a snapshot import `node.properties` is empty and the values
/// live only in the columnar store (ADR-021), so reading the sparse property map alone
/// would give a silently-zero roll-up on every imported graph — the exact failure mode this
/// ordering exists to prevent. The sparse map is consulted only as a fallback for nodes
/// created through the non-columnar path.
pub fn read_measure(
    store: &GraphStore,
    poset: &Poset,
    measure: &MeasureSpec,
) -> Vec<Option<RollupValue>> {
    poset
        .node_ids()
        .iter()
        .map(|&id| read_node_measure(store, id, measure))
        .collect()
}

fn read_node_measure(store: &GraphStore, id: NodeId, measure: &MeasureSpec) -> Option<RollupValue> {
    if let Some(label) = &measure.label {
        let node = store.get_node(id)?;
        if !node.labels.iter().any(|l| l == label) {
            return None;
        }
    }
    let columnar = store
        .node_columns
        .get_property(id.as_u64() as usize, &measure.property);
    let value = match columnar {
        PropertyValue::Null => store
            .get_node(id)
            .and_then(|n| n.get_property(&measure.property).cloned())
            .unwrap_or(PropertyValue::Null),
        v => v,
    };
    to_rollup_value(&value)
}

/// Numeric properties become measures; everything else contributes nothing.
fn to_rollup_value(v: &PropertyValue) -> Option<RollupValue> {
    match v {
        PropertyValue::Integer(i) => Some(RollupValue::Int(*i as i128)),
        PropertyValue::Float(f) => Some(RollupValue::Float(*f)),
        PropertyValue::Boolean(b) => Some(RollupValue::Int(if *b { 1 } else { 0 })),
        _ => None,
    }
}

/// Encoding name for a built entry, or `"declined"`.
pub fn encoding_name(entry: &HierarchyEntry) -> &'static str {
    match &entry.index {
        Some(i) => i.encoding().name(),
        None => "declined",
    }
}

/// Convenience: the encoding an entry uses, if it built one.
pub fn entry_encoding(entry: &HierarchyEntry) -> Option<Encoding> {
    entry.index.as_ref().map(|i| i.encoding())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;

    /// A three-level ATC-shaped drug hierarchy with enrollment-style measures.
    fn drug_store() -> (GraphStore, Vec<NodeId>) {
        let mut store = GraphStore::new();
        let root = store.create_node("Class");
        let mut ids = vec![root];
        for c in 0..3 {
            let mid = store.create_node("Class");
            store.create_edge(mid, root, "IS_A").unwrap();
            ids.push(mid);
            for l in 0..3 {
                let leaf = store.create_node("Drug");
                store.create_edge(leaf, mid, "IS_A").unwrap();
                if let Some(n) = store.get_node_mut(leaf) {
                    n.set_property("units", (c * 3 + l + 1) as i64);
                }
                store.set_column_property(
                    leaf,
                    "units",
                    PropertyValue::Integer((c * 3 + l + 1) as i64),
                );
                ids.push(leaf);
            }
        }
        (store, ids)
    }

    fn spec_with_measure() -> HierarchySpec {
        HierarchySpec::new("atc", vec![EdgeType::new("IS_A")]).with_measure(
            None,
            "units",
            vec![RollupOp::Sum, RollupOp::Max, RollupOp::Count],
        )
    }

    #[test]
    fn create_builds_and_reports() {
        let (store, _) = drug_store();
        let mgr = HierarchyIndexManager::new();
        let info = mgr.create(&store, spec_with_measure()).unwrap();
        assert_eq!(info.encoding, Some("nested-set"));
        assert_eq!(info.nodes, 13);
        assert_eq!(info.edges, 12);
        assert!(!info.stale);
        assert!(info.ops.contains(&"sum"));
        assert!(info.bytes > 0);
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let (store, _) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();
        let err = mgr.create(&store, spec_with_measure()).unwrap_err();
        assert!(matches!(err, HierarchyError::DuplicateIndex(_)));
    }

    #[test]
    fn rollup_reads_the_measure_from_the_columnar_store() {
        // The sum of 1..=9 over the leaves; the columnar path is the one that must work,
        // because an imported snapshot has nothing in the sparse property map.
        let (store, ids) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();
        let entry = mgr.get("atc").unwrap();
        let guard = entry.read().unwrap();
        let idx = guard.index.as_ref().unwrap();
        assert_eq!(
            idx.rollup_id(ids[0], RollupOp::Sum),
            Some(RollupValue::Int(45))
        );
        assert_eq!(
            idx.rollup_id(ids[0], RollupOp::Max),
            Some(RollupValue::Int(9))
        );
        assert_eq!(
            idx.rollup_id(ids[0], RollupOp::Count),
            Some(RollupValue::Int(13))
        );
    }

    #[test]
    fn measure_read_prefers_columnar_over_sparse_map() {
        // Deliberate divergence: sparse map says 100, column says 7. An imported graph only
        // ever has the column, so the column must win.
        let mut store = GraphStore::new();
        let root = store.create_node("Class");
        let leaf = store.create_node("Class");
        store.create_edge(leaf, root, "IS_A").unwrap();
        if let Some(n) = store.get_node_mut(leaf) {
            n.set_property("units", 100i64);
        }
        store.set_column_property(leaf, "units", PropertyValue::Integer(7));
        let mgr = HierarchyIndexManager::new();
        mgr.create(
            &store,
            HierarchySpec::new("h", vec![EdgeType::new("IS_A")]).with_measure(
                None,
                "units",
                vec![RollupOp::Sum],
            ),
        )
        .unwrap();
        let entry = mgr.get("h").unwrap();
        let guard = entry.read().unwrap();
        assert_eq!(
            guard.index.as_ref().unwrap().rollup_id(root, RollupOp::Sum),
            Some(RollupValue::Int(7))
        );
    }

    #[test]
    fn editing_the_covering_relation_marks_the_index_stale_and_hides_it() {
        let (store, _) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();
        let et = EdgeType::new("IS_A");
        assert!(mgr.usable_for_edge_type(&et).is_some());

        mgr.mark_stale_for_edge_type(&et);
        assert!(
            mgr.usable_for_edge_type(&et).is_none(),
            "a stale index must be invisible to the planner"
        );
        assert!(
            mgr.any_for_edge_type(&et).is_some(),
            "but EXPLAIN must still be able to say why"
        );
        let entry = mgr.any_for_edge_type(&et).unwrap();
        assert_eq!(
            entry.read().unwrap().unusable_reason(),
            Some(Unusable::Stale)
        );
    }

    #[test]
    fn unrelated_edge_types_do_not_invalidate() {
        let (store, _) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();
        mgr.mark_stale_for_edge_type(&EdgeType::new("TREATS"));
        assert!(mgr.usable_for_edge_type(&EdgeType::new("IS_A")).is_some());
    }

    #[test]
    fn writing_the_measure_property_marks_stale() {
        let (store, _) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();
        mgr.mark_stale_for_property("name");
        assert!(mgr.usable_for_edge_type(&EdgeType::new("IS_A")).is_some());
        mgr.mark_stale_for_property("units");
        assert!(mgr.usable_for_edge_type(&EdgeType::new("IS_A")).is_none());
    }

    #[test]
    fn rebuild_clears_staleness_and_picks_up_new_data() {
        let (mut store, ids) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();

        // add a new leaf under the first mid-level class
        let extra = store.create_node("Drug");
        store.create_edge(extra, ids[1], "IS_A").unwrap();
        store.set_column_property(extra, "units", PropertyValue::Integer(100));
        mgr.mark_stale_for_edge_type(&EdgeType::new("IS_A"));
        assert!(mgr.usable_for_edge_type(&EdgeType::new("IS_A")).is_none());

        let info = mgr.rebuild(&store, "atc").unwrap();
        assert!(!info.stale);
        assert_eq!(info.nodes, 14);
        let entry = mgr.get("atc").unwrap();
        let guard = entry.read().unwrap();
        assert_eq!(
            guard
                .index
                .as_ref()
                .unwrap()
                .rollup_id(ids[0], RollupOp::Sum),
            Some(RollupValue::Int(145))
        );
    }

    #[test]
    fn declined_hierarchy_registers_with_a_diagnostic_and_is_not_used() {
        // Wide bipartite DAG — the Gene Ontology regime.
        let mut store = GraphStore::new();
        let roots: Vec<NodeId> = (0..3).map(|_| store.create_node("Term")).collect();
        for i in 0..400usize {
            let leaf = store.create_node("Term");
            store.create_edge(leaf, roots[i % 3], "PART_OF").unwrap();
            store
                .create_edge(leaf, roots[(i + 1) % 3], "PART_OF")
                .unwrap();
        }
        let mgr = HierarchyIndexManager::new();
        let info = mgr
            .create(
                &store,
                HierarchySpec::new("go", vec![EdgeType::new("PART_OF")]),
            )
            .unwrap();
        assert_eq!(info.encoding, None);
        let declined = info.declined.expect("a decline must carry a diagnostic");
        assert!(
            declined.contains("2-hop"),
            "diagnostic names the alternative: {declined}"
        );
        assert!(mgr
            .usable_for_edge_type(&EdgeType::new("PART_OF"))
            .is_none());
    }

    #[test]
    fn cyclic_covering_relation_is_an_error_not_a_decline() {
        let mut store = GraphStore::new();
        let a = store.create_node("T");
        let b = store.create_node("T");
        store.create_edge(a, b, "IS_A").unwrap();
        store.create_edge(b, a, "IS_A").unwrap();
        let mgr = HierarchyIndexManager::new();
        let err = mgr
            .create(
                &store,
                HierarchySpec::new("bad", vec![EdgeType::new("IS_A")]),
            )
            .unwrap_err();
        assert!(matches!(err, HierarchyError::NotAcyclic { .. }));
    }

    #[test]
    fn reverse_orientation_builds_the_same_hierarchy() {
        // parent -[:HAS_CHILD]-> child is the same poset read the other way round.
        let mut store = GraphStore::new();
        let root = store.create_node("T");
        let kid = store.create_node("T");
        store.create_edge(root, kid, "HAS_CHILD").unwrap();
        let mgr = HierarchyIndexManager::new();
        let mut spec = HierarchySpec::new("h", vec![EdgeType::new("HAS_CHILD")]);
        spec.reverse = true;
        mgr.create(&store, spec).unwrap();
        let entry = mgr.get("h").unwrap();
        let guard = entry.read().unwrap();
        let idx = guard.index.as_ref().unwrap();
        assert_eq!(idx.subsumes_ids(kid, root), Some(true));
        assert_eq!(idx.subsumes_ids(root, kid), Some(false));
    }

    #[test]
    fn store_write_paths_invalidate_through_graph_store() {
        // End-to-end staleness: the index is registered on the store, and an ordinary
        // `create_edge` on the covering relation must take it out of service without
        // anyone calling the manager directly.
        let (mut store, _) = drug_store();
        let mgr = Arc::clone(&store.hierarchy_index);
        mgr.create(&store, spec_with_measure()).unwrap();
        let et = EdgeType::new("IS_A");
        assert!(store.hierarchy_index.usable_for_edge_type(&et).is_some());

        let extra = store.create_node("Drug");
        let root = store.node_ids_by_label(&Label::new("Class"), Some(1))[0];
        store.create_edge(extra, root, "IS_A").unwrap();
        assert!(
            store.hierarchy_index.usable_for_edge_type(&et).is_none(),
            "create_edge on the covering relation must mark the hierarchy stale"
        );
    }

    #[test]
    fn a_measure_write_updates_the_index_in_place_instead_of_invalidating_it() {
        // Only the value changed, not the shape of the poset, so the index absorbs it and
        // stays usable — and the roll-up must reflect the new number immediately (#351).
        let (mut store, ids) = drug_store();
        let mgr = Arc::clone(&store.hierarchy_index);
        mgr.create(&store, spec_with_measure()).unwrap();
        let et = EdgeType::new("IS_A");
        let root = ids[0];

        let before = {
            let e = mgr.get("atc").unwrap();
            let g = e.read().unwrap();
            g.index
                .as_ref()
                .unwrap()
                .rollup_id(root, RollupOp::Sum)
                .unwrap()
        };
        assert_eq!(before, RollupValue::Int(45), "units 1..=9");

        // one leaf goes from 9 to 109: the total should rise by exactly 100
        store.set_column_property(ids[12], "units", PropertyValue::Integer(109));

        assert!(
            mgr.usable_for_edge_type(&et).is_some(),
            "a measure write must not take the index out of service"
        );
        let after = {
            let e = mgr.get("atc").unwrap();
            let g = e.read().unwrap();
            assert!(!g.stale, "the index is still fresh");
            g.index
                .as_ref()
                .unwrap()
                .rollup_id(root, RollupOp::Sum)
                .unwrap()
        };
        assert_eq!(
            after,
            RollupValue::Int(145),
            "roll-up reflects the write without a rebuild"
        );
    }

    #[test]
    fn an_unrelated_property_write_touches_nothing() {
        let (mut store, ids) = drug_store();
        let mgr = Arc::clone(&store.hierarchy_index);
        mgr.create(&store, spec_with_measure()).unwrap();
        store.set_column_property(ids[2], "colour", PropertyValue::Integer(1));
        let e = mgr.get("atc").unwrap();
        let g = e.read().unwrap();
        assert!(!g.stale);
        assert_eq!(
            g.index.as_ref().unwrap().rollup_id(ids[0], RollupOp::Sum),
            Some(RollupValue::Int(45))
        );
    }

    #[test]
    fn drop_and_list() {
        let (store, _) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(&store, spec_with_measure()).unwrap();
        assert_eq!(mgr.list().len(), 1);
        assert!(!mgr.is_empty());
        mgr.drop_index("atc").unwrap();
        assert!(mgr.is_empty());
        assert!(matches!(
            mgr.drop_index("atc"),
            Err(HierarchyError::NoSuchIndex(_))
        ));
    }

    #[test]
    fn count_rollup_works_without_a_declared_measure() {
        let (store, ids) = drug_store();
        let mgr = HierarchyIndexManager::new();
        mgr.create(
            &store,
            HierarchySpec::new("atc", vec![EdgeType::new("IS_A")]),
        )
        .unwrap();
        let entry = mgr.get("atc").unwrap();
        let guard = entry.read().unwrap();
        assert_eq!(
            guard
                .index
                .as_ref()
                .unwrap()
                .rollup_id(ids[0], RollupOp::Count),
            Some(RollupValue::Int(13))
        );
    }
}
