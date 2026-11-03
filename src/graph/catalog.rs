//! Triple-level statistics for cost-based query optimization (ADR-015)
//!
//! GraphCatalog provides fine-grained statistics at the (source_label, edge_type, target_label)
//! level, enabling the graph-native planner to accurately estimate cardinalities and choose
//! optimal traversal directions.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use super::types::{Label, EdgeType, NodeId};

/// Which endpoint of a triple a degree is counted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DegreeSide {
    /// Out-degree: the node is the edge's source.
    Source,
    /// In-degree: the node is the edge's target.
    Target,
}

/// A triple pattern representing a (source_label, edge_type, target_label) combination
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriplePattern {
    pub source_label: Label,
    pub edge_type: EdgeType,
    pub target_label: Label,
}

impl TriplePattern {
    pub fn new(source_label: impl Into<Label>, edge_type: impl Into<EdgeType>, target_label: impl Into<Label>) -> Self {
        TriplePattern {
            source_label: source_label.into(),
            edge_type: edge_type.into(),
            target_label: target_label.into(),
        }
    }
}

/// Statistics for a single triple pattern
/// A triple pattern's three parts, so the catalog maps can be probed with
/// borrowed labels. Building an owned `TriplePattern` to look one up cloned
/// three `String`s on every edge insert, for an entry that exists after the
/// first edge of each shape (#491).
trait TripleKey {
    fn parts(&self) -> (&str, &str, &str);
}

impl TripleKey for TriplePattern {
    fn parts(&self) -> (&str, &str, &str) {
        (self.source_label.as_str(), self.edge_type.as_str(), self.target_label.as_str())
    }
}

impl TripleKey for (&Label, &EdgeType, &Label) {
    fn parts(&self) -> (&str, &str, &str) {
        (self.0.as_str(), self.1.as_str(), self.2.as_str())
    }
}

impl<'a> Borrow<dyn TripleKey + 'a> for TriplePattern {
    fn borrow(&self) -> &(dyn TripleKey + 'a) {
        self
    }
}

// One hash for the owned and the borrowed form: a map finds a key only if both
// hash the same.
impl Hash for TriplePattern {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl Hash for dyn TripleKey + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl PartialEq for dyn TripleKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn TripleKey + '_ {}

#[derive(Debug, Clone)]
pub struct TripleStats {
    /// Number of edges matching this triple pattern
    pub count: usize,
    /// Average outgoing degree from source nodes for this pattern
    pub avg_out_degree: f64,
    /// Average incoming degree to target nodes for this pattern
    pub avg_in_degree: f64,
    /// Number of distinct source nodes
    pub distinct_sources: usize,
    /// Number of distinct target nodes
    pub distinct_targets: usize,
    /// Maximum outgoing degree for this pattern
    pub max_out_degree: usize,
}

impl TripleStats {
    fn new() -> Self {
        TripleStats {
            count: 0,
            avg_out_degree: 0.0,
            avg_in_degree: 0.0,
            distinct_sources: 0,
            distinct_targets: 0,
            max_out_degree: 0,
        }
    }
}

/// Incrementally maintained graph catalog for cost-based optimization.
///
/// Extends (does not replace) the existing GraphStatistics.
/// New planner uses GraphCatalog; old planner path keeps working.
#[derive(Debug, Clone)]
pub struct GraphCatalog {
    /// Node count per label
    pub label_counts: HashMap<Label, usize>,
    /// Statistics per triple pattern (source_label, edge_type, target_label)
    triple_stats: HashMap<TriplePattern, TripleStats>,
    /// Per-triple per-source outgoing degree: triple_pattern -> source_node -> out_degree
    source_degrees: HashMap<TriplePattern, HashMap<NodeId, usize>>,
    /// Per-triple per-target incoming degree: triple_pattern -> target_node -> in_degree
    target_degrees: HashMap<TriplePattern, HashMap<NodeId, usize>>,
    /// Monotonically increasing version for cache invalidation
    pub generation: u64,
    /// Edges this catalog has been told about, one per `on_edge_created`.
    ///
    /// Compared against the store's own edge count to notice the edges it was
    /// *not* told about: `create_edge_stub` skips `on_edge_created` entirely,
    /// so a bulk-loaded graph has degree maps that are short rather than
    /// merely stale. See [`Self::degrees_are_exact_for`].
    edges_counted: usize,
    /// Edge types for which at least one edge was filed under a number of
    /// triples other than exactly one.
    ///
    /// The degree maps are keyed by `(source label, type, target label)`, so an
    /// edge between two single-label nodes is filed once and a node's degree is
    /// the sum over the matching triples. An edge whose endpoints carry two
    /// labels each is filed four times and would be counted four times; an
    /// edge between unlabelled nodes is filed nowhere and would be counted
    /// never. Either way the sum is not the degree, so the type is listed here
    /// and the shortcut declines it.
    non_unit_label_types: std::collections::HashSet<EdgeType>,
    /// Set by anything that can leave a filed degree under the wrong key, and
    /// cleared only by a full recompute. See [`Self::note_degrees_may_be_stale`].
    degrees_stale: bool,
}

impl GraphCatalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        GraphCatalog {
            label_counts: HashMap::new(),
            triple_stats: HashMap::new(),
            source_degrees: HashMap::new(),
            target_degrees: HashMap::new(),
            generation: 0,
            edges_counted: 0,
            non_unit_label_types: std::collections::HashSet::new(),
            // An empty catalog describes an empty graph exactly.
            degrees_stale: false,
        }
    }

    /// Notify the catalog that a label was added to a node
    pub fn on_label_added(&mut self, label: &Label) {
        *self.label_counts.entry(label.clone()).or_insert(0) += 1;
        self.generation += 1;
    }

    /// Notify the catalog that a label was removed from a node (e.g., node deleted)
    pub fn on_label_removed(&mut self, label: &Label) {
        if let Some(count) = self.label_counts.get_mut(label) {
            *count = count.saturating_sub(1);
        }
        self.generation += 1;
    }

    /// Notify the catalog that an edge was created
    ///
    /// For a multi-label source and multi-label target, this generates
    /// one triple entry per (src_label, edge_type, tgt_label) combination.
    /// Generic over the label collections so a caller holding a `HashSet<Label>`
    /// -- which `Node` does -- can pass it directly. `GraphStore::create_edge`
    /// previously collected two `Vec<Label>` per edge purely to satisfy a
    /// `&[Label]` parameter, cloning every label `String` on every insert
    /// (#491). `&[Label]` still coerces, so existing callers are unchanged.
    pub fn on_edge_created<'a, S, T>(
        &mut self,
        source_id: NodeId,
        src_labels: S,
        edge_type: &EdgeType,
        target_id: NodeId,
        tgt_labels: T,
    ) where
        S: IntoIterator<Item = &'a Label>,
        // Clone because the inner loop re-iterates the targets for every
        // source. `&[Label]` and `&HashSet<Label>` are both references, so the
        // clone is a pointer copy and costs nothing.
        T: IntoIterator<Item = &'a Label> + Clone,
    {
        // How many triples this one edge was filed under. Exactly one is the
        // only case in which summing a node's per-triple degrees gives its
        // degree, so anything else disqualifies the type (#304).
        let mut filings = 0usize;
        for src_label in src_labels {
            for tgt_label in tgt_labels.clone() {
                filings += 1;
                // Probe by reference; build the owned key only to insert one.
                let parts = (src_label, edge_type, tgt_label);
                let key: &dyn TripleKey = &parts;
                let owned = || TriplePattern::new(src_label.clone(), edge_type.clone(), tgt_label.clone());

                // `entry(k)` takes the key by value, so an `entry` per map cloned
                // three Strings on every call whether or not the entry already
                // existed -- and after the first few edges of a given (label,
                // type, label) shape it always does. `get_mut` by the borrowed
                // key first; `entry(owned())` only on the genuinely-new path.
                let src_degree_val = match self.source_degrees.get_mut(key) {
                    Some(m) => {
                        let d = m.entry(source_id).or_insert(0);
                        *d += 1;
                        *d
                    }
                    None => {
                        let d = self
                            .source_degrees
                            .entry(owned())
                            .or_default()
                            .entry(source_id)
                            .or_insert(0);
                        *d += 1;
                        *d
                    }
                };
                let new_src_degree = src_degree_val;

                match self.target_degrees.get_mut(key) {
                    Some(m) => {
                        *m.entry(target_id).or_insert(0) += 1;
                    }
                    None => {
                        *self
                            .target_degrees
                            .entry(owned())
                            .or_default()
                            .entry(target_id)
                            .or_insert(0) += 1;
                    }
                }

                let stats = match self.triple_stats.get_mut(key) {
                    Some(s) => s,
                    None => self
                        .triple_stats
                        .entry(owned())
                        .or_insert_with(TripleStats::new),
                };
                stats.count += 1;

                // Recompute distinct sources/targets from degree maps
                let src_map = self.source_degrees.get(key).unwrap();
                let tgt_map = self.target_degrees.get(key).unwrap();
                stats.distinct_sources = src_map.len();
                stats.distinct_targets = tgt_map.len();

                // Recompute averages
                stats.avg_out_degree = stats.count as f64 / stats.distinct_sources as f64;
                stats.avg_in_degree = stats.count as f64 / stats.distinct_targets as f64;

                // Update max out degree
                if new_src_degree > stats.max_out_degree {
                    stats.max_out_degree = new_src_degree;
                }
            }
        }
        self.edges_counted += 1;
        if filings != 1 {
            self.non_unit_label_types.insert(edge_type.clone());
        }
        self.generation += 1;
    }

    /// Mark the degree maps unfit to be read as answers.
    ///
    /// Called for the changes that can leave a degree filed under a key that no
    /// longer describes the edge: a label added to or removed from a node that
    /// already has edges, a node deleted (which removes its labels *before* its
    /// edges, so `on_edge_deleted` sees labels that are already gone), and the
    /// stub edge path, which does not reach this catalog at all.
    ///
    /// Sticky. `GraphStore::rebuild_catalog` builds a fresh catalog and is the
    /// only repair, which is what `finish_bulk_load` calls.
    pub fn note_degrees_may_be_stale(&mut self) {
        self.degrees_stale = true;
    }

    /// Whether the per-node degree maps for `edge_type` can be read as the
    /// answer to a degree question rather than as an estimate.
    ///
    /// `total_edges` is the store's own edge count, which the caller already
    /// holds; passing it keeps this a pure function of the catalog and avoids
    /// reading the two from different states.
    ///
    /// Three ways the maps can be wrong, and a read has to rule out all three:
    ///
    /// * **short** — `create_edge_stub` never calls `on_edge_created`, so a
    ///   mid-bulk-load catalog is missing whole edges. `edges_counted` against
    ///   the store's count catches it, the same question PR #1806 asks of
    ///   `edge_type_counts`.
    /// * **mis-keyed by label** — an edge filed under two triples or none,
    ///   recorded per type in `non_unit_label_types`.
    /// * **stale** — a label or a deletion moved a node after its degrees were
    ///   filed; see [`Self::note_degrees_may_be_stale`].
    ///
    /// A `false` means "walk the adjacency", which is slower and right. A fast
    /// wrong answer is worse than a slow one.
    pub fn degrees_are_exact_for(&self, total_edges: usize, edge_type: &EdgeType) -> bool {
        !self.degrees_stale
            && self.edges_counted == total_edges
            && !self.non_unit_label_types.contains(edge_type)
    }

    /// The degree maps that answer one pattern, as read-only borrows.
    ///
    /// `side` picks out-degree (`Source`) or in-degree (`Target`); `source_label`
    /// and `target_label` are the pattern's endpoint labels, `None` meaning
    /// unconstrained. The returned maps are exactly the triples the pattern can
    /// match, so a caller sums a node's entry across them and applies no
    /// further filter.
    ///
    /// `pub(crate)`: the maps themselves stay private — this is the narrowest
    /// accessor an operator in this crate can use, and the neighbouring
    /// `all_triple_stats` hands out a whole map by comparison. Only meaningful
    /// when [`Self::degrees_are_exact_for`] says yes for `edge_type`.
    pub(crate) fn degree_maps_for(
        &self,
        edge_type: &EdgeType,
        source_label: Option<&Label>,
        target_label: Option<&Label>,
        side: DegreeSide,
    ) -> Vec<&HashMap<NodeId, usize>> {
        let maps = match side {
            DegreeSide::Source => &self.source_degrees,
            DegreeSide::Target => &self.target_degrees,
        };
        maps.iter()
            .filter(|(pattern, _)| {
                pattern.edge_type.as_str() == edge_type.as_str()
                    && source_label.is_none_or(|l| pattern.source_label.as_str() == l.as_str())
                    && target_label.is_none_or(|l| pattern.target_label.as_str() == l.as_str())
            })
            .map(|(_, degrees)| degrees)
            .collect()
    }

    /// Notify the catalog that an edge was deleted
    pub fn on_edge_deleted(
        &mut self,
        source_id: NodeId,
        src_labels: &[Label],
        edge_type: &EdgeType,
        target_id: NodeId,
        tgt_labels: &[Label],
    ) {
        self.edges_counted = self.edges_counted.saturating_sub(1);
        // The labels passed here are the endpoints' labels *now*, and
        // `delete_node` strips a node's labels before deleting its edges — so
        // the decrement below can miss the triple the increment used, leaving a
        // degree that no longer has an edge behind it. Rather than reason about
        // every order a deletion can arrive in, a deletion ends the exactness
        // claim until a rebuild.
        self.degrees_stale = true;
        for src_label in src_labels {
            for tgt_label in tgt_labels {
                let pattern = TriplePattern::new(src_label.clone(), edge_type.clone(), tgt_label.clone());

                // Update source degree tracking
                if let Some(src_map) = self.source_degrees.get_mut(&pattern) {
                    if let Some(degree) = src_map.get_mut(&source_id) {
                        *degree = degree.saturating_sub(1);
                        if *degree == 0 {
                            src_map.remove(&source_id);
                        }
                    }
                }

                // Update target degree tracking
                if let Some(tgt_map) = self.target_degrees.get_mut(&pattern) {
                    if let Some(degree) = tgt_map.get_mut(&target_id) {
                        *degree = degree.saturating_sub(1);
                        if *degree == 0 {
                            tgt_map.remove(&target_id);
                        }
                    }
                }

                // Update triple stats
                if let Some(stats) = self.triple_stats.get_mut(&pattern) {
                    stats.count = stats.count.saturating_sub(1);

                    let src_map = self.source_degrees.get(&pattern);
                    let tgt_map = self.target_degrees.get(&pattern);

                    stats.distinct_sources = src_map.map(|m| m.len()).unwrap_or(0);
                    stats.distinct_targets = tgt_map.map(|m| m.len()).unwrap_or(0);

                    if stats.distinct_sources > 0 {
                        stats.avg_out_degree = stats.count as f64 / stats.distinct_sources as f64;
                    } else {
                        stats.avg_out_degree = 0.0;
                    }
                    if stats.distinct_targets > 0 {
                        stats.avg_in_degree = stats.count as f64 / stats.distinct_targets as f64;
                    } else {
                        stats.avg_in_degree = 0.0;
                    }

                    // Recompute max_out_degree (need full scan of source degrees)
                    stats.max_out_degree = src_map
                        .map(|m| m.values().copied().max().unwrap_or(0))
                        .unwrap_or(0);

                    // Remove empty triple stats
                    if stats.count == 0 {
                        self.triple_stats.remove(&pattern);
                        self.source_degrees.remove(&pattern);
                        self.target_degrees.remove(&pattern);
                    }
                }
            }
        }
        self.generation += 1;
    }

    /// Get triple stats for a specific pattern
    pub fn get_triple_stats(&self, pattern: &TriplePattern) -> Option<&TripleStats> {
        self.triple_stats.get(pattern)
    }

    /// Get all triple stats
    pub fn all_triple_stats(&self) -> &HashMap<TriplePattern, TripleStats> {
        &self.triple_stats
    }

    /// Estimate the number of rows from a label scan
    pub fn estimate_label_scan(&self, label: &Label) -> f64 {
        self.label_counts.get(label).copied().unwrap_or(0) as f64
    }

    /// Estimate cardinality after an outgoing expand from source_label via edge_type
    pub fn estimate_expand_out(&self, source_label: &Label, edge_type: &EdgeType) -> f64 {
        // Sum avg_out_degree across all target labels for this (source, edge_type)
        let mut total_degree = 0.0;
        let mut found = false;
        for (pattern, stats) in &self.triple_stats {
            if &pattern.source_label == source_label && &pattern.edge_type == edge_type {
                total_degree += stats.avg_out_degree;
                found = true;
            }
        }
        if found { total_degree } else { 1.0 } // default: assume 1 edge per node
    }

    /// Estimate cardinality after an incoming expand to target_label via edge_type
    pub fn estimate_expand_in(&self, target_label: &Label, edge_type: &EdgeType) -> f64 {
        let mut total_degree = 0.0;
        let mut found = false;
        for (pattern, stats) in &self.triple_stats {
            if &pattern.target_label == target_label && &pattern.edge_type == edge_type {
                total_degree += stats.avg_in_degree;
                found = true;
            }
        }
        if found { total_degree } else { 1.0 }
    }

    /// Estimate the probability that an edge exists between a specific source and target
    pub fn estimate_edge_existence(
        &self,
        source_label: &Label,
        edge_type: &EdgeType,
        target_label: &Label,
    ) -> f64 {
        let pattern = TriplePattern::new(source_label.clone(), edge_type.clone(), target_label.clone());
        match self.triple_stats.get(&pattern) {
            Some(stats) => {
                let source_count = self.label_counts.get(source_label).copied().unwrap_or(1) as f64;
                let target_count = self.label_counts.get(target_label).copied().unwrap_or(1) as f64;
                let possible_pairs = source_count * target_count;
                if possible_pairs > 0.0 {
                    (stats.count as f64 / possible_pairs).min(1.0)
                } else {
                    0.0
                }
            }
            None => 0.0,
        }
    }

    /// Recompute all catalog statistics from scratch using the graph store.
    ///
    /// Dispatches to [`Self::recompute_full_parallel`] once the arena is large
    /// enough for the split to pay for itself, and to
    /// [`Self::recompute_full_serial`] below that. The two produce identical
    /// catalogs — see `parallel_recompute_matches_serial` — and the serial one
    /// is the reference, so a doubt about the parallel pass is settled by
    /// comparing against it rather than by reasoning about it.
    ///
    /// # Why this is parallel at all
    ///
    /// `finish_bulk_load` is 1,380 s of a 4,219 s federation load and this
    /// phase is ~72% of that, on one core of 96 (#1824). The work is per-node
    /// and per-edge accumulation into counts, which splits cleanly by node
    /// slot: each source node is visited by exactly one task, so out-degrees
    /// never collide, and in-degrees and counts merge by integer addition —
    /// associative, commutative, and exact, which is what makes the result
    /// independent of thread count (`CH-DETERM-THREADS`).
    pub fn recompute_full(store: &super::store::GraphStore) -> Self {
        // Below this the fold/merge costs more than the scan saves, and the
        // whole pass is sub-millisecond anyway.
        const PARALLEL_MIN_SLOTS: usize = 64 * 1024;
        if store.node_slot_count() >= PARALLEL_MIN_SLOTS && rayon::current_num_threads() > 1 {
            Self::recompute_full_parallel(store)
        } else {
            Self::recompute_full_serial(store)
        }
    }

    /// The reference recompute: one thread, through `on_edge_created`.
    ///
    /// Kept as written rather than reimplemented, so it is an oracle for the
    /// parallel pass and not a second thing to doubt.
    pub(crate) fn recompute_full_serial(store: &super::store::GraphStore) -> Self {
        let mut catalog = GraphCatalog::new();

        // Recompute label counts
        for node in store.all_nodes() {
            for label in &node.labels {
                *catalog.label_counts.entry(label.clone()).or_insert(0) += 1;
            }
        }

        // Recompute triple stats by iterating all edges
        for node in store.all_nodes() {
            let outgoing = store.get_outgoing_edge_targets(node.id);
            for (_, _, target_id, edge_type) in &outgoing {
                if let Some(target_node) = store.get_node(*target_id) {
                    // Borrowed, not collected: `on_edge_created` is generic over
                    // `IntoIterator<Item = &Label>` since #493, so a full rebuild
                    // over 176M edges no longer allocates two Vecs and clones a
                    // String per edge just to satisfy a `&[Label]` parameter.
                    catalog.on_edge_created(
                        node.id,
                        &node.labels,
                        &edge_type,
                        *target_id,
                        &target_node.labels,
                    );
                    // Undo the generation bump from on_edge_created (we'll set it at the end)
                    catalog.generation -= 1;
                }
            }
        }

        catalog.generation = 1;
        catalog
    }

    /// The same recompute, split over rayon's pool.
    ///
    /// Three things make this both fast and deterministic:
    ///
    /// * **Split by node slot, not by edge.** A slot range owns every outgoing
    ///   edge of every node in it, so each source node's out-degree is
    ///   accumulated by exactly one task. In-degrees and triple counts are
    ///   touched by several tasks and merged by addition.
    /// * **Integer accumulators, floats last.** `avg_out_degree` and
    ///   `avg_in_degree` are divisions of two merged integers computed once at
    ///   the end, so no float is ever summed across tasks and the result
    ///   cannot depend on the merge order.
    /// * **No per-node allocation.** Labels and edge types are interned to
    ///   integers inside the task, the triple key is a `(u32, u16, u32)`
    ///   `Copy` tuple, and the two per-node buffers are reused across nodes.
    ///   A `String` is cloned once per distinct label and per distinct type in
    ///   the whole catalog, not once per edge — #520 removed a per-edge type
    ///   clone from the expansion path and #1457 records a naive `par_iter`
    ///   putting it back.
    pub(crate) fn recompute_full_parallel(store: &super::store::GraphStore) -> Self {
        use rayon::prelude::*;

        let slots = store.node_slot_count();
        // Enough chunks for work stealing to even out a skewed degree
        // distribution, few enough that the merge tree stays shallow.
        let chunk = (slots / (rayon::current_num_threads() * 8).max(1)).max(8 * 1024);
        let ranges: Vec<(usize, usize)> = (0..slots)
            .step_by(chunk)
            .map(|start| (start, (start + chunk).min(slots)))
            .collect();

        // `fold` gives one accumulator per rayon job split rather than one per
        // node, which is what keeps this from allocating per edge.
        let shards: Vec<Shard> = ranges
            .par_iter()
            .fold(Shard::new, |mut shard, &(start, end)| {
                shard.scan_slots(store, start, end);
                shard
            })
            .collect();

        // Regroup, then merge per triple rather than pairwise over shards.
        //
        // A `reduce` tree looks like the obvious merge and caps the speedup at
        // about 2x: its last merge is one thread combining two half-size
        // shards, which is half the total work. Grouping each triple's
        // accumulators together first makes every triple's merge independent,
        // so the merge scales with the number of triples instead. Measured at
        // 4M nodes / 8M edges: 0.768 s with the reduce tree, which is where
        // the 2x ceiling shows up.
        let (global, grouped) = Shard::regroup(shards);
        let merged: Vec<(TripleKeyIds, TripleAcc)> = grouped
            .into_par_iter()
            .map(|(key, accs)| (key, TripleAcc::merge_all(accs)))
            .collect();

        global.into_catalog(store, merged)
    }

    /// Format catalog as human-readable text for EXPLAIN output
    pub fn format(&self) -> String {
        let mut result = String::new();
        result.push_str("Triple Statistics:\n");

        let mut patterns: Vec<_> = self.triple_stats.iter().collect();
        patterns.sort_by(|a, b| b.1.count.cmp(&a.1.count));

        for (pattern, stats) in patterns {
            result.push_str(&format!(
                "  (:{})--[:{}]-->(:{}) count={}, avg_out={:.1}, avg_in={:.1}, max_out={}, sources={}, targets={}\n",
                pattern.source_label.as_str(),
                pattern.edge_type.as_str(),
                pattern.target_label.as_str(),
                stats.count,
                stats.avg_out_degree,
                stats.avg_in_degree,
                stats.max_out_degree,
                stats.distinct_sources,
                stats.distinct_targets,
            ));
        }

        result
    }

    /// Reset catalog to empty state
    pub fn clear(&mut self) {
        self.label_counts.clear();
        self.triple_stats.clear();
        self.source_degrees.clear();
        self.target_degrees.clear();
        self.generation = 0;
    }
}

/// A triple keyed by interned ids: (source label, edge type, target label).
///
/// The point of the whole key being `Copy` integers is that hashing it costs
/// 8 bytes rather than three `String`s.
type TripleKeyIds = (u32, u16, u32);

/// One triple's accumulated counters inside a single task.
#[derive(Default)]
struct TripleAcc {
    count: usize,
    source_degrees: HashMap<NodeId, usize>,
    target_degrees: HashMap<NodeId, usize>,
}

impl TripleAcc {
    /// Combine every task's view of one triple.
    ///
    /// Counts add and degrees add per node, so the result is the same whatever
    /// order the accumulators arrive in — which is what lets the caller hand
    /// them over from an unordered parallel iterator.
    fn merge_all(mut accs: Vec<TripleAcc>) -> TripleAcc {
        // Fold into the biggest one: the cost is one hash insert per entry
        // moved, so moving the fewest entries is the cheapest order.
        let biggest = accs
            .iter()
            .enumerate()
            .max_by_key(|(_, a)| a.source_degrees.len() + a.target_degrees.len())
            .map(|(i, _)| i)
            .unwrap_or(0);
        if accs.is_empty() {
            return TripleAcc::default();
        }
        let mut base = accs.swap_remove(biggest);
        for acc in accs {
            base.count += acc.count;
            // Out-degrees cannot actually collide — a source node lives in one
            // slot range — but adding is correct whether they do or not.
            for (node, degree) in acc.source_degrees {
                *base.source_degrees.entry(node).or_insert(0) += degree;
            }
            for (node, degree) in acc.target_degrees {
                *base.target_degrees.entry(node).or_insert(0) += degree;
            }
        }
        base
    }
}

/// One task's private accumulator for [`GraphCatalog::recompute_full_parallel`].
///
/// Labels are interned to task-local `u32`s and edge types carry the store's
/// own `u16` id, so the triple key is a `Copy` 8-byte tuple rather than three
/// `String`s. That is the difference between hashing ~40 bytes of text five
/// times per edge and hashing 8 bytes once.
struct Shard<'a> {
    /// `&str` borrowed from the store, so interning allocates nothing.
    label_ids: HashMap<&'a str, u32>,
    labels: Vec<&'a Label>,
    /// Node count per local label id, parallel to `labels`.
    label_counts: Vec<usize>,
    triples: HashMap<TripleKeyIds, TripleAcc>,
    edges_counted: usize,
    non_unit_types: std::collections::HashSet<u16>,
}

impl<'a> Shard<'a> {
    fn new() -> Self {
        Shard {
            label_ids: HashMap::new(),
            labels: Vec::new(),
            label_counts: Vec::new(),
            triples: HashMap::new(),
            edges_counted: 0,
            non_unit_types: std::collections::HashSet::new(),
        }
    }

    /// The task-local id for a label, interning it on first sight.
    ///
    /// Keyed by `&str` borrowed from the store, so this allocates nothing; a
    /// `Label` is cloned once per distinct label at the very end.
    #[inline]
    fn intern(&mut self, label: &'a Label) -> u32 {
        if let Some(&id) = self.label_ids.get(label.as_str()) {
            return id;
        }
        let id = self.labels.len() as u32;
        self.labels.push(label);
        self.label_counts.push(0);
        self.label_ids.insert(label.as_str(), id);
        id
    }

    /// Scan node slots `start..end`, which own every outgoing edge of every
    /// node in them and so every out-degree those nodes have.
    ///
    /// The three buffers are declared once and cleared per node: a `Vec` per
    /// node would be 328M allocations on the federation.
    fn scan_slots(&mut self, store: &'a super::store::GraphStore, start: usize, end: usize) {
        let mut src_labels: Vec<u32> = Vec::new();
        let mut tgt_labels: Vec<u32> = Vec::new();
        let mut edges: Vec<(NodeId, u16)> = Vec::new();

        for idx in start..end {
            // Every version in the slot, which is what `all_nodes()` yields.
            for node in store.node_slot_versions(idx) {
                src_labels.clear();
                for label in &node.labels {
                    let id = self.intern(label);
                    self.label_counts[id as usize] += 1;
                    src_labels.push(id);
                }

                edges.clear();
                store.for_each_outgoing_neighbor(node.id, None, |target, eid| {
                    // Both checks mirror `get_outgoing_edge_targets_owned`,
                    // which resolves the type through `get_edge_type` and
                    // silently drops an edge it cannot name.
                    if let Some(type_id) = store.edge_type_id_at(eid) {
                        if store.edge_type_name_at(type_id).is_some() {
                            edges.push((target, type_id));
                        }
                    }
                });

                for &(target, type_id) in &edges {
                    // The serial pass skips an edge whose target row is gone.
                    let Some(target_node) = store.get_node(target) else {
                        continue;
                    };
                    tgt_labels.clear();
                    for label in &target_node.labels {
                        let id = self.intern(label);
                        tgt_labels.push(id);
                    }

                    for &s in &src_labels {
                        for &t in &tgt_labels {
                            let acc = self.triples.entry((s, type_id, t)).or_default();
                            acc.count += 1;
                            *acc.source_degrees.entry(node.id).or_insert(0) += 1;
                            *acc.target_degrees.entry(target).or_insert(0) += 1;
                        }
                    }

                    self.edges_counted += 1;
                    // Exactly one filing is the only case where summing a
                    // node's per-triple degrees gives its degree (#304).
                    if src_labels.len() * tgt_labels.len() != 1 {
                        self.non_unit_types.insert(type_id);
                    }
                }
            }
        }
    }

    /// Collapse the shards' task-local label numbering into one, and group
    /// every shard's view of each triple together.
    ///
    /// Serial, and deliberately so: it is O(shards x distinct triples), with
    /// no per-node or per-edge work at all — the accumulators move by pointer.
    /// The expensive part, merging the degree maps, is left to the caller to
    /// do per triple in parallel.
    fn regroup(shards: Vec<Self>) -> (Self, HashMap<TripleKeyIds, Vec<TripleAcc>>) {
        let mut global = Shard::new();
        let mut grouped: HashMap<TripleKeyIds, Vec<TripleAcc>> = HashMap::new();
        for shard in shards {
            // A shard's label ids are its own: translate them into the
            // global numbering before the keys can be compared.
            let remap: Vec<u32> = shard.labels.iter().copied().map(|l| global.intern(l)).collect();
            for (local, count) in shard.label_counts.iter().enumerate() {
                global.label_counts[remap[local] as usize] += count;
            }
            for ((s, type_id, t), acc) in shard.triples {
                grouped
                    .entry((remap[s as usize], type_id, remap[t as usize]))
                    .or_default()
                    .push(acc);
            }
            global.edges_counted += shard.edges_counted;
            global.non_unit_types.extend(shard.non_unit_types);
        }
        (global, grouped)
    }

    /// Turn the merged counters into a catalog, resolving names once per
    /// distinct label and per distinct edge type.
    ///
    /// The two averages are computed here, from integers that are already
    /// final, so no float is ever accumulated across tasks.
    fn into_catalog(
        self,
        store: &super::store::GraphStore,
        triples: Vec<(TripleKeyIds, TripleAcc)>,
    ) -> GraphCatalog {
        let Shard { labels, label_counts, edges_counted, non_unit_types, .. } = self;

        let mut catalog = GraphCatalog::new();
        for (local, &count) in label_counts.iter().enumerate() {
            // A label interned only as an edge target is counted by whichever
            // shard scanned its node's slot; a zero here would be a label on
            // no node, which the serial pass also omits.
            if count > 0 {
                catalog.label_counts.insert(labels[local].clone(), count);
            }
        }

        let mut type_names: HashMap<u16, EdgeType> = HashMap::new();
        for ((s, type_id, t), acc) in triples {
            let edge_type = match type_names.get(&type_id) {
                Some(name) => name.clone(),
                None => {
                    let Some(name) = store.edge_type_name_at(type_id) else { continue };
                    type_names.insert(type_id, name.clone());
                    name.clone()
                }
            };
            let distinct_sources = acc.source_degrees.len();
            let distinct_targets = acc.target_degrees.len();
            let max_out_degree = acc.source_degrees.values().copied().max().unwrap_or(0);
            let pattern = TriplePattern::new(
                labels[s as usize].clone(),
                edge_type,
                labels[t as usize].clone(),
            );
            catalog.triple_stats.insert(
                pattern.clone(),
                TripleStats {
                    count: acc.count,
                    avg_out_degree: acc.count as f64 / distinct_sources as f64,
                    avg_in_degree: acc.count as f64 / distinct_targets as f64,
                    distinct_sources,
                    distinct_targets,
                    max_out_degree,
                },
            );
            catalog.source_degrees.insert(pattern.clone(), acc.source_degrees);
            catalog.target_degrees.insert(pattern, acc.target_degrees);
        }

        for type_id in non_unit_types {
            if let Some(name) = store.edge_type_name_at(type_id) {
                catalog.non_unit_label_types.insert(name.clone());
            }
        }
        catalog.edges_counted = edges_counted;
        catalog.generation = 1;
        catalog
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::slice::from_ref;

    // ---- TDD: Tests written first, then implementation verified ----

    /// A graph with enough shape to tell the two recomputes apart if they
    /// differ: several labels, several edge types, multi-label nodes (an edge
    /// filed under four triples), unlabelled nodes (filed under none),
    /// self-loops, parallel edges, and a skewed degree distribution so the
    /// slot chunks carry unequal work.
    ///
    /// Above 8,192 slots so the parallel pass really splits (see
    /// `recompute_full_parallel`'s chunk size) — a one-chunk graph would prove
    /// nothing about merging.
    fn many_shapes_store() -> crate::graph::store::GraphStore {
        use crate::graph::store::GraphStore;
        let mut store = GraphStore::new();
        let labels = ["Person", "Company", "City", "Product", "Tag"];
        let types = ["KNOWS", "WORKS_AT", "LIVES_IN", "RATED"];
        let n = 30_000usize;

        let ids: Vec<_> = (0..n)
            .map(|i| {
                if i >= n - 40 {
                    // Two labels, and only in the last few slots. Every edge
                    // touching one of these is filed under two or four
                    // triples, which disqualifies its type from the degree
                    // shortcut -- and because they sit in one slot range,
                    // exactly one task learns that. A merge that forgets to
                    // union `non_unit_types` therefore loses it, rather than
                    // being covered up by every task having seen it.
                    store.create_node_with_labels([
                        Label::new(labels[i % 5]),
                        Label::new(labels[(i + 2) % 5]),
                    ])
                } else if i % 11 == 1 {
                    // No labels at all: filed under no triple.
                    store.create_node_with_labels(Vec::<Label>::new())
                } else {
                    store.create_node(labels[i % 5])
                }
            })
            .collect();

        // A type carried only by the multi-label tail, so it is the one type
        // whose disqualification lives in a single task's accumulator.
        for i in (n - 40)..n {
            store.create_edge(ids[i], ids[(i + 3) % n], "MULTI").unwrap();
        }

        for i in 0..n {
            let t = types[i % 4];
            store.create_edge(ids[i], ids[(i * 7 + 1) % n], t).unwrap();
            // A skewed fan-out: the first few nodes carry many edges.
            if i < 50 {
                for j in 0..200 {
                    store.create_edge(ids[i], ids[(i + j * 13) % n], types[j % 4]).unwrap();
                }
            }
            if i % 1_000 == 0 {
                // Self-loop and a parallel edge of the same type.
                store.create_edge(ids[i], ids[i], "KNOWS").unwrap();
                store.create_edge(ids[i], ids[(i + 1) % n], types[i % 4]).unwrap();
                store.create_edge(ids[i], ids[(i + 1) % n], types[i % 4]).unwrap();
            }
        }
        store
    }

    /// Equal in every field, with the two averages compared bit for bit
    /// rather than within a tolerance: a tolerance would hide exactly the
    /// order-dependence this test exists to rule out.
    fn assert_catalogs_equal(expected: &GraphCatalog, actual: &GraphCatalog, what: &str) {
        assert_eq!(expected.label_counts, actual.label_counts, "label_counts ({what})");
        assert_eq!(
            expected.triple_stats.len(),
            actual.triple_stats.len(),
            "triple count ({what})"
        );
        for (pattern, want) in &expected.triple_stats {
            let got = actual
                .triple_stats
                .get(pattern)
                .unwrap_or_else(|| panic!("missing triple {pattern:?} ({what})"));
            assert_eq!(want.count, got.count, "count {pattern:?} ({what})");
            assert_eq!(want.distinct_sources, got.distinct_sources, "sources {pattern:?} ({what})");
            assert_eq!(want.distinct_targets, got.distinct_targets, "targets {pattern:?} ({what})");
            assert_eq!(want.max_out_degree, got.max_out_degree, "max_out {pattern:?} ({what})");
            assert_eq!(
                want.avg_out_degree.to_bits(),
                got.avg_out_degree.to_bits(),
                "avg_out {pattern:?} ({what})"
            );
            assert_eq!(
                want.avg_in_degree.to_bits(),
                got.avg_in_degree.to_bits(),
                "avg_in {pattern:?} ({what})"
            );
        }
        assert_eq!(expected.source_degrees, actual.source_degrees, "source_degrees ({what})");
        assert_eq!(expected.target_degrees, actual.target_degrees, "target_degrees ({what})");
        assert_eq!(expected.edges_counted, actual.edges_counted, "edges_counted ({what})");
        assert_eq!(
            expected.non_unit_label_types, actual.non_unit_label_types,
            "non_unit_label_types ({what})"
        );
        assert_eq!(expected.generation, actual.generation, "generation ({what})");
    }

    #[test]
    fn parallel_recompute_matches_serial() {
        let store = many_shapes_store();
        let serial = GraphCatalog::recompute_full_serial(&store);
        // Not an empty check in disguise: the graph has to have produced
        // triples, degrees and a disqualified type for the comparison to mean
        // anything.
        assert!(serial.triple_stats.len() >= 20, "triples: {}", serial.triple_stats.len());
        assert!(!serial.non_unit_label_types.is_empty(), "no multi-label edge was filed");
        assert!(serial.edges_counted > 30_000, "edges: {}", serial.edges_counted);

        let parallel = GraphCatalog::recompute_full_parallel(&store);
        assert_catalogs_equal(&serial, &parallel, "default pool");
    }

    /// `CH-DETERM-THREADS`: the counts may not depend on how many threads ran.
    ///
    /// Driven through explicit rayon pools rather than `RAYON_NUM_THREADS`,
    /// because the env var is read once per process and every other test in
    /// this binary shares that process.
    #[test]
    fn parallel_recompute_is_independent_of_thread_count() {
        let store = many_shapes_store();
        let serial = GraphCatalog::recompute_full_serial(&store);
        for threads in [1usize, 2, 3, 5, 8, 16] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build()
                .expect("rayon pool");
            let parallel = pool.install(|| GraphCatalog::recompute_full_parallel(&store));
            assert_catalogs_equal(&serial, &parallel, &format!("{threads} threads"));
        }
    }

    /// The dispatcher itself: above `PARALLEL_MIN_SLOTS`, `recompute_full`
    /// must still agree with the serial pass. The two tests above call the
    /// parallel pass directly, so without this one nothing checks that the
    /// path production takes is the path that was checked (#1467's class).
    #[test]
    fn recompute_full_above_the_parallel_threshold_matches_serial() {
        use crate::graph::store::GraphStore;
        let mut store = GraphStore::new();
        // More than 64 Ki slots, so the dispatcher chooses the parallel pass.
        let n = 70_000usize;
        let ids: Vec<_> = (0..n)
            .map(|i| store.create_node(if i % 3 == 0 { "Person" } else { "Company" }))
            .collect();
        for i in 0..n {
            store.create_edge(ids[i], ids[(i * 7 + 1) % n], if i % 2 == 0 { "KNOWS" } else { "RATED" }).unwrap();
        }
        let serial = GraphCatalog::recompute_full_serial(&store);
        let dispatched = GraphCatalog::recompute_full(&store);
        assert_catalogs_equal(&serial, &dispatched, "recompute_full");
    }

    #[test]
    fn parallel_recompute_handles_an_empty_store() {
        let store = crate::graph::store::GraphStore::new();
        let serial = GraphCatalog::recompute_full_serial(&store);
        let parallel = GraphCatalog::recompute_full_parallel(&store);
        assert_catalogs_equal(&serial, &parallel, "empty");
        assert!(parallel.label_counts.is_empty());
    }

    #[test]
    fn test_empty_catalog() {
        let catalog = GraphCatalog::new();
        assert!(catalog.triple_stats.is_empty());
        assert!(catalog.label_counts.is_empty());
        assert_eq!(catalog.generation, 0);
        assert_eq!(catalog.estimate_label_scan(&Label::new("Person")), 0.0);
    }

    #[test]
    fn test_label_tracking() {
        let mut catalog = GraphCatalog::new();
        catalog.on_label_added(&Label::new("Person"));
        catalog.on_label_added(&Label::new("Person"));
        catalog.on_label_added(&Label::new("Company"));

        assert_eq!(catalog.estimate_label_scan(&Label::new("Person")), 2.0);
        assert_eq!(catalog.estimate_label_scan(&Label::new("Company")), 1.0);
        assert_eq!(catalog.estimate_label_scan(&Label::new("Unknown")), 0.0);

        catalog.on_label_removed(&Label::new("Person"));
        assert_eq!(catalog.estimate_label_scan(&Label::new("Person")), 1.0);
    }

    #[test]
    fn test_single_triple() {
        let mut catalog = GraphCatalog::new();

        let src = NodeId::new(1);
        let tgt = NodeId::new(2);
        catalog.on_edge_created(
            src,
            &[Label::new("Person")],
            &EdgeType::new("KNOWS"),
            tgt,
            &[Label::new("Person")],
        );

        let pattern = TriplePattern::new("Person", "KNOWS", "Person");
        let stats = catalog.get_triple_stats(&pattern).unwrap();

        assert_eq!(stats.count, 1);
        assert_eq!(stats.distinct_sources, 1);
        assert_eq!(stats.distinct_targets, 1);
        assert_eq!(stats.avg_out_degree, 1.0);
        assert_eq!(stats.avg_in_degree, 1.0);
        assert_eq!(stats.max_out_degree, 1);
    }

    #[test]
    fn test_incremental_edge_created() {
        let mut catalog = GraphCatalog::new();

        // Person 1 knows Person 2 and Person 3
        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        let p3 = NodeId::new(3);

        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p3, &[Label::new("Person")]);

        let pattern = TriplePattern::new("Person", "KNOWS", "Person");
        let stats = catalog.get_triple_stats(&pattern).unwrap();

        assert_eq!(stats.count, 2);
        assert_eq!(stats.distinct_sources, 1); // only p1 is source
        assert_eq!(stats.distinct_targets, 2); // p2 and p3 are targets
        assert_eq!(stats.avg_out_degree, 2.0); // 2 edges / 1 source
        assert_eq!(stats.avg_in_degree, 1.0);  // 2 edges / 2 targets
        assert_eq!(stats.max_out_degree, 2);
    }

    #[test]
    fn test_incremental_edge_deleted() {
        let mut catalog = GraphCatalog::new();

        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        let p3 = NodeId::new(3);

        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p3, &[Label::new("Person")]);

        // Delete one edge
        catalog.on_edge_deleted(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p3, &[Label::new("Person")]);

        let pattern = TriplePattern::new("Person", "KNOWS", "Person");
        let stats = catalog.get_triple_stats(&pattern).unwrap();

        assert_eq!(stats.count, 1);
        assert_eq!(stats.distinct_sources, 1);
        assert_eq!(stats.distinct_targets, 1); // p3 removed
        assert_eq!(stats.avg_out_degree, 1.0);
        assert_eq!(stats.max_out_degree, 1);
    }

    #[test]
    fn test_edge_deleted_removes_empty_pattern() {
        let mut catalog = GraphCatalog::new();

        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);

        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        catalog.on_edge_deleted(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);

        let pattern = TriplePattern::new("Person", "KNOWS", "Person");
        assert!(catalog.get_triple_stats(&pattern).is_none());
    }

    #[test]
    fn test_multi_label_nodes() {
        let mut catalog = GraphCatalog::new();

        // Source has labels [Person, Employee], target has labels [Person, Manager]
        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);

        catalog.on_edge_created(
            p1,
            &[Label::new("Person"), Label::new("Employee")],
            &EdgeType::new("REPORTS_TO"),
            p2,
            &[Label::new("Person"), Label::new("Manager")],
        );

        // Should create 4 triple entries: Person->Person, Person->Manager, Employee->Person, Employee->Manager
        assert_eq!(catalog.triple_stats.len(), 4);

        let pp = TriplePattern::new("Person", "REPORTS_TO", "Person");
        assert!(catalog.get_triple_stats(&pp).is_some());

        let pm = TriplePattern::new("Person", "REPORTS_TO", "Manager");
        assert!(catalog.get_triple_stats(&pm).is_some());

        let ep = TriplePattern::new("Employee", "REPORTS_TO", "Person");
        assert!(catalog.get_triple_stats(&ep).is_some());

        let em = TriplePattern::new("Employee", "REPORTS_TO", "Manager");
        assert!(catalog.get_triple_stats(&em).is_some());
    }

    #[test]
    fn test_max_out_degree_tracking() {
        let mut catalog = GraphCatalog::new();

        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        let p3 = NodeId::new(3);
        let p4 = NodeId::new(4);

        // p1 knows p2, p3, p4 (degree 3)
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p3, &[Label::new("Person")]);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p4, &[Label::new("Person")]);

        // p2 knows p3 (degree 1)
        catalog.on_edge_created(p2, &[Label::new("Person")], &EdgeType::new("KNOWS"), p3, &[Label::new("Person")]);

        let pattern = TriplePattern::new("Person", "KNOWS", "Person");
        let stats = catalog.get_triple_stats(&pattern).unwrap();

        assert_eq!(stats.max_out_degree, 3);
        assert_eq!(stats.count, 4);
        assert_eq!(stats.distinct_sources, 2); // p1 and p2
    }

    #[test]
    fn test_estimate_expand_out() {
        let mut catalog = GraphCatalog::new();

        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        let c1 = NodeId::new(3);

        // Each Person knows 2 Persons
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        catalog.on_edge_created(p2, &[Label::new("Person")], &EdgeType::new("KNOWS"), p1, &[Label::new("Person")]);

        // Each Person works at 1 Company
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("WORKS_AT"), c1, &[Label::new("Company")]);

        let knows_degree = catalog.estimate_expand_out(&Label::new("Person"), &EdgeType::new("KNOWS"));
        assert_eq!(knows_degree, 1.0); // 2 edges / 2 sources = 1.0

        let works_degree = catalog.estimate_expand_out(&Label::new("Person"), &EdgeType::new("WORKS_AT"));
        assert_eq!(works_degree, 1.0); // 1 edge / 1 source = 1.0
    }

    #[test]
    fn test_estimate_expand_in() {
        let mut catalog = GraphCatalog::new();

        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        let p3 = NodeId::new(3);
        let c1 = NodeId::new(4);

        // 3 Person nodes WORKS_AT 1 Company
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("WORKS_AT"), c1, &[Label::new("Company")]);
        catalog.on_edge_created(p2, &[Label::new("Person")], &EdgeType::new("WORKS_AT"), c1, &[Label::new("Company")]);
        catalog.on_edge_created(p3, &[Label::new("Person")], &EdgeType::new("WORKS_AT"), c1, &[Label::new("Company")]);

        let in_degree = catalog.estimate_expand_in(&Label::new("Company"), &EdgeType::new("WORKS_AT"));
        assert_eq!(in_degree, 3.0); // 3 edges / 1 target = 3.0
    }

    #[test]
    fn test_estimate_edge_existence() {
        let mut catalog = GraphCatalog::new();

        // 2 Person nodes, 1 Company
        catalog.on_label_added(&Label::new("Person"));
        catalog.on_label_added(&Label::new("Person"));
        catalog.on_label_added(&Label::new("Company"));

        let p1 = NodeId::new(1);
        let c1 = NodeId::new(3);

        // 1 of 2 persons works at the company
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("WORKS_AT"), c1, &[Label::new("Company")]);

        let prob = catalog.estimate_edge_existence(
            &Label::new("Person"),
            &EdgeType::new("WORKS_AT"),
            &Label::new("Company"),
        );
        // 1 edge / (2 persons * 1 company) = 0.5
        assert!((prob - 0.5).abs() < 1e-10);

        // Non-existent pattern
        let prob = catalog.estimate_edge_existence(
            &Label::new("Company"),
            &EdgeType::new("KNOWS"),
            &Label::new("Person"),
        );
        assert_eq!(prob, 0.0);
    }

    #[test]
    fn test_generation_increments() {
        let mut catalog = GraphCatalog::new();
        assert_eq!(catalog.generation, 0);

        catalog.on_label_added(&Label::new("Person"));
        assert_eq!(catalog.generation, 1);

        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        assert_eq!(catalog.generation, 2);

        catalog.on_edge_deleted(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);
        assert_eq!(catalog.generation, 3);
    }

    #[test]
    fn test_format_output() {
        let mut catalog = GraphCatalog::new();
        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);

        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);

        let output = catalog.format();
        assert!(output.contains("Triple Statistics:"));
        assert!(output.contains("Person"));
        assert!(output.contains("KNOWS"));
        assert!(output.contains("count=1"));
    }

    #[test]
    fn test_clear() {
        let mut catalog = GraphCatalog::new();
        catalog.on_label_added(&Label::new("Person"));
        let p1 = NodeId::new(1);
        let p2 = NodeId::new(2);
        catalog.on_edge_created(p1, &[Label::new("Person")], &EdgeType::new("KNOWS"), p2, &[Label::new("Person")]);

        catalog.clear();

        assert!(catalog.label_counts.is_empty());
        assert!(catalog.triple_stats.is_empty());
        assert_eq!(catalog.generation, 0);
    }

    #[test]
    fn test_recompute_full_matches_incremental() {
        use crate::graph::store::GraphStore;

        let mut store = GraphStore::new();
        let p1 = store.create_node("Person");
        let p2 = store.create_node("Person");
        let p3 = store.create_node("Person");
        let c1 = store.create_node("Company");

        store.create_edge(p1, p2, "KNOWS").unwrap();
        store.create_edge(p2, p3, "KNOWS").unwrap();
        store.create_edge(p1, c1, "WORKS_AT").unwrap();

        let recomputed = GraphCatalog::recompute_full(&store);

        // Should have 2 triple patterns: Person-KNOWS->Person, Person-WORKS_AT->Company
        assert_eq!(recomputed.all_triple_stats().len(), 2);

        let knows = TriplePattern::new("Person", "KNOWS", "Person");
        let knows_stats = recomputed.get_triple_stats(&knows).unwrap();
        assert_eq!(knows_stats.count, 2);

        let works = TriplePattern::new("Person", "WORKS_AT", "Company");
        let works_stats = recomputed.get_triple_stats(&works).unwrap();
        assert_eq!(works_stats.count, 1);

        // Verify label counts
        assert_eq!(recomputed.label_counts.get(&Label::new("Person")).copied().unwrap_or(0), 3);
        assert_eq!(recomputed.label_counts.get(&Label::new("Company")).copied().unwrap_or(0), 1);
    }

    #[test]
    fn test_catalog_maintained_on_create_delete_edge() {
        use crate::graph::store::GraphStore;

        let mut store = GraphStore::new();
        let p1 = store.create_node("Person");
        let p2 = store.create_node("Person");

        let edge_id = store.create_edge(p1, p2, "KNOWS").unwrap();

        // Catalog should reflect the edge
        let pattern = TriplePattern::new("Person", "KNOWS", "Person");
        let stats = store.catalog().get_triple_stats(&pattern).unwrap();
        assert_eq!(stats.count, 1);

        // Delete the edge
        store.delete_edge(edge_id).unwrap();

        // Catalog should be empty
        assert!(store.catalog().get_triple_stats(&pattern).is_none());
    }

    #[test]
    fn test_estimate_expand_unknown_returns_default() {
        let catalog = GraphCatalog::new();

        // Unknown patterns should return 1.0 (default assumption)
        let out = catalog.estimate_expand_out(&Label::new("Unknown"), &EdgeType::new("UNKNOWN"));
        assert_eq!(out, 1.0);

        let in_d = catalog.estimate_expand_in(&Label::new("Unknown"), &EdgeType::new("UNKNOWN"));
        assert_eq!(in_d, 1.0);
    }

    #[test]
    fn test_asymmetric_graph_statistics() {
        // Model the "100 Companies vs 1M Persons" scenario from ADR-015
        let mut catalog = GraphCatalog::new();

        // Simulate: 10 persons each working at 1 company
        for i in 0..10 {
            catalog.on_edge_created(
                NodeId::new(i),
                &[Label::new("Person")],
                &EdgeType::new("WORKS_AT"),
                NodeId::new(100), // all work at same company
                &[Label::new("Company")],
            );
        }

        // Outgoing from Person via WORKS_AT: avg_out = 1.0 (each person -> 1 company)
        let out = catalog.estimate_expand_out(&Label::new("Person"), &EdgeType::new("WORKS_AT"));
        assert_eq!(out, 1.0);

        // Incoming to Company via WORKS_AT: avg_in = 10.0 (one company <- 10 persons)
        let in_d = catalog.estimate_expand_in(&Label::new("Company"), &EdgeType::new("WORKS_AT"));
        assert_eq!(in_d, 10.0);

        // This tells the planner: starting from Company and expanding incoming is 10x more expensive
        // than starting from Person and expanding outgoing
    }

    #[test]
    fn deleting_one_of_several_edges_keeps_the_endpoint_degrees() {
        let mut catalog = GraphCatalog::new();
        let (p, c) = (Label::new("Person"), Label::new("Company"));
        let works = EdgeType::new("WORKS_AT");
        // One person at two companies, and a second person at the first.
        catalog.on_edge_created(
            NodeId::new(1),
            &[p.clone()],
            &works,
            NodeId::new(10),
            &[c.clone()],
        );
        catalog.on_edge_created(
            NodeId::new(1),
            &[p.clone()],
            &works,
            NodeId::new(11),
            &[c.clone()],
        );
        catalog.on_edge_created(
            NodeId::new(2),
            &[p.clone()],
            &works,
            NodeId::new(10),
            &[c.clone()],
        );

        catalog.on_edge_deleted(
            NodeId::new(1),
            &[p.clone()],
            &works,
            NodeId::new(10),
            &[c.clone()],
        );
        let stats = catalog
            .get_triple_stats(&TriplePattern::new(p.clone(), works.clone(), c.clone()))
            .expect("two edges remain");
        assert_eq!(stats.count, 2);
        assert_eq!(stats.distinct_sources, 2, "person 1 still has an edge");
        assert_eq!(stats.distinct_targets, 2, "company 10 still has an edge");
        assert_eq!(stats.max_out_degree, 1);
        assert_eq!(stats.avg_out_degree, 1.0);
        assert_eq!(stats.avg_in_degree, 1.0);
    }

    #[test]
    fn edge_existence_is_zero_when_a_label_count_has_dropped_to_zero() {
        let mut catalog = GraphCatalog::new();
        let (a, b) = (Label::new("A"), Label::new("B"));
        let r = EdgeType::new("R");
        catalog.on_label_added(&a);
        catalog.on_label_added(&b);
        catalog.on_edge_created(
            NodeId::new(1),
            &[a.clone()],
            &r,
            NodeId::new(2),
            &[b.clone()],
        );
        assert_eq!(catalog.estimate_edge_existence(&a, &r, &b), 1.0);
        catalog.on_label_removed(&a);
        assert_eq!(catalog.estimate_edge_existence(&a, &r, &b), 0.0);
        assert_eq!(
            catalog.estimate_edge_existence(&b, &r, &a),
            0.0,
            "no such triple"
        );
    }

    #[test]
    fn deleting_an_edge_the_catalog_never_saw_changes_only_the_generation() {
        let mut catalog = GraphCatalog::new();
        let (a, b) = (Label::new("A"), Label::new("B"));
        let r = EdgeType::new("R");
        let before = catalog.generation;
        catalog.on_edge_deleted(
            NodeId::new(1),
            &[a.clone()],
            &r,
            NodeId::new(2),
            &[b.clone()],
        );
        assert!(catalog.triple_stats.is_empty());
        assert_eq!(catalog.generation, before + 1);

        // A known pattern but an endpoint it has no degree for.
        catalog.on_edge_created(
            NodeId::new(1),
            &[a.clone()],
            &r,
            NodeId::new(2),
            &[b.clone()],
        );
        catalog.on_edge_created(
            NodeId::new(1),
            &[a.clone()],
            &r,
            NodeId::new(3),
            &[b.clone()],
        );
        catalog.on_edge_deleted(
            NodeId::new(7),
            &[a.clone()],
            &r,
            NodeId::new(8),
            &[b.clone()],
        );
        let stats = catalog
            .get_triple_stats(&TriplePattern::new(a, r, b))
            .unwrap();
        assert_eq!(stats.count, 1, "the count is decremented regardless");
        assert_eq!(stats.distinct_sources, 1);
    }

    // ---------------------------------------------------------------- #304

    /// The exactness guard, asked of the catalog alone.
    ///
    /// Three separate ways the degree maps can be unfit, and each has to say
    /// no on its own: a short count, a mis-keyed edge, a later mutation.
    #[test]
    fn degrees_are_exact_only_when_every_edge_is_filed_once() {
        let a = Label::new("A");
        let b = Label::new("B");
        let t = EdgeType::new("T");
        let n = |i: u64| NodeId::new(i);

        let mut c = GraphCatalog::new();
        // Vacuously exact: no edges, no graph.
        assert!(c.degrees_are_exact_for(0, &t));
        // And not exact against a store that has an edge the catalog never saw
        // -- the `create_edge_stub` case.
        assert!(!c.degrees_are_exact_for(1, &t));

        c.on_edge_created(n(1), from_ref(&a), &t, n(2), from_ref(&b));
        assert!(c.degrees_are_exact_for(1, &t));

        // A two-label endpoint files the edge twice, so a per-node sum over
        // the matching triples would double it.
        let mut two = GraphCatalog::new();
        two.on_edge_created(n(1), from_ref(&a), &t, n(2), &[a.clone(), b.clone()]);
        assert!(!two.degrees_are_exact_for(1, &t));
        // Only that type is disqualified.
        assert!(two.degrees_are_exact_for(1, &EdgeType::new("U")));

        // An unlabelled endpoint files it nowhere.
        let mut none = GraphCatalog::new();
        none.on_edge_created(n(1), &[] as &[Label], &t, n(2), from_ref(&b));
        assert!(!none.degrees_are_exact_for(1, &t));

        // A deletion ends the claim until a rebuild, and so does a label
        // change on a connected node.
        let mut deleted = c.clone();
        deleted.on_edge_deleted(n(1), from_ref(&a), &t, n(2), from_ref(&b));
        assert!(!deleted.degrees_are_exact_for(0, &t));
        let mut churned = c.clone();
        churned.note_degrees_may_be_stale();
        assert!(!churned.degrees_are_exact_for(1, &t));
    }

    /// The accessor hands back exactly the triples a pattern can match, so a
    /// caller sums and applies no further filter.
    #[test]
    fn degree_maps_are_selected_by_the_patterns_labels() {
        let article = Label::new("Article");
        let blog = Label::new("Blog");
        let cites = EdgeType::new("CITES");
        let n = |i: u64| NodeId::new(i);

        let mut c = GraphCatalog::new();
        // Two articles and a blog all cite node 1.
        c.on_edge_created(n(2), from_ref(&article), &cites, n(1), from_ref(&article));
        c.on_edge_created(n(3), from_ref(&article), &cites, n(1), from_ref(&article));
        c.on_edge_created(n(4), from_ref(&blog), &cites, n(1), from_ref(&article));

        let sum = |maps: Vec<&HashMap<NodeId, usize>>, node: NodeId| -> usize {
            maps.iter().filter_map(|m| m.get(&node)).sum()
        };

        // In-degree of node 1, far end unconstrained.
        assert_eq!(
            sum(
                c.degree_maps_for(&cites, None, Some(&article), DegreeSide::Target),
                n(1)
            ),
            3
        );
        // Far end an :Article only.
        assert_eq!(
            sum(
                c.degree_maps_for(&cites, Some(&article), Some(&article), DegreeSide::Target),
                n(1)
            ),
            2
        );
        // A label nothing carries selects no triple at all, not every triple.
        assert!(c
            .degree_maps_for(&cites, Some(&Label::new("Nope")), None, DegreeSide::Target)
            .is_empty());
        // Out-degree is the other map, and node 1 has none.
        assert_eq!(
            sum(
                c.degree_maps_for(&cites, Some(&article), None, DegreeSide::Source),
                n(1)
            ),
            0
        );
        assert_eq!(
            sum(
                c.degree_maps_for(&cites, Some(&article), None, DegreeSide::Source),
                n(2)
            ),
            1
        );
        // Another edge type shares no triple with this one.
        assert!(c
            .degree_maps_for(&EdgeType::new("QUOTES"), None, None, DegreeSide::Target)
            .is_empty());
    }
}
