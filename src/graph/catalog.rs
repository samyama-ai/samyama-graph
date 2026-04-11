//! Triple-level statistics for cost-based query optimization (ADR-015)
//!
//! GraphCatalog provides fine-grained statistics at the (source_label, edge_type, target_label)
//! level, enabling the graph-native planner to accurately estimate cardinalities and choose
//! optimal traversal directions.

use std::collections::HashMap;
use super::types::{Label, EdgeType, NodeId};

/// A triple pattern representing a (source_label, edge_type, target_label) combination
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    pub fn on_edge_created(
        &mut self,
        source_id: NodeId,
        src_labels: &[Label],
        edge_type: &EdgeType,
        target_id: NodeId,
        tgt_labels: &[Label],
    ) {
        for src_label in src_labels {
            for tgt_label in tgt_labels {
                let pattern = TriplePattern::new(src_label.clone(), edge_type.clone(), tgt_label.clone());

                // Update source degree tracking
                let src_degree = self.source_degrees
                    .entry(pattern.clone())
                    .or_default()
                    .entry(source_id)
                    .or_insert(0);
                *src_degree += 1;
                let new_src_degree = *src_degree;

                // Update target degree tracking
                let tgt_degree = self.target_degrees
                    .entry(pattern.clone())
                    .or_default()
                    .entry(target_id)
                    .or_insert(0);
                *tgt_degree += 1;

                // Update triple stats
                let stats = self.triple_stats.entry(pattern.clone()).or_insert_with(TripleStats::new);
                stats.count += 1;

                // Recompute distinct sources/targets from degree maps
                let src_map = self.source_degrees.get(&pattern).unwrap();
                let tgt_map = self.target_degrees.get(&pattern).unwrap();
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
        self.generation += 1;
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
    /// Used for consistency checks — result should match incrementally maintained stats.
    pub fn recompute_full(store: &super::store::GraphStore) -> Self {
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
                    let src_labels: Vec<Label> = node.labels.iter().cloned().collect();
                    let tgt_labels: Vec<Label> = target_node.labels.iter().cloned().collect();
                    catalog.on_edge_created(
                        node.id,
                        &src_labels,
                        &edge_type,
                        *target_id,
                        &tgt_labels,
                    );
                    // Undo the generation bump from on_edge_created (we'll set it at the end)
                    catalog.generation -= 1;
                }
            }
        }

        catalog.generation = 1;
        catalog
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

#[cfg(test)]
mod tests {
    use super::*;

    // ---- TDD: Tests written first, then implementation verified ----

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
}
