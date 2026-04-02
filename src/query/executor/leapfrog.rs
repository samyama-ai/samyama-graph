//! LeapFrog TrieJoin — Worst-Case Optimal Join for cyclic graph patterns.
//!
//! Implements the LeapFrog algorithm from Veldhuizen (2012) / Ngo et al. (2018).
//! Used for triangle queries, clique detection, and cyclic pattern matching
//! where traditional binary joins are sub-optimal.
//!
//! The key insight: instead of materializing intermediate join results,
//! LeapFrog intersects sorted adjacency lists simultaneously using
//! a seek-based iterator protocol. For k sorted lists of total size N,
//! intersection takes O(N × k) in the worst case but O(min-list) in practice.
//!
//! Prerequisites:
//! - Sorted adjacency lists (guaranteed by CSR FrozenAdjacency + sorted Vec-of-Vec)
//! - Binary search support (FrozenAdjacency::find_neighbor already exists)

use crate::graph::types::{NodeId, EdgeId};
use crate::graph::store::GraphStore;
use crate::query::ast::Direction;
use super::operator::{PhysicalOperator, OperatorBox, OperatorDescription};
use super::record::{Record, Value};
use super::ExecutionResult;

/// A seekable iterator over sorted (NodeId, EdgeId) pairs.
/// Supports `seek(target)` for O(log n) jumping — the core of LeapFrog.
#[derive(Debug)]
pub struct AdjacencyIterator<'a> {
    /// Sorted neighbor entries (from CSR or write buffer)
    entries: &'a [(NodeId, EdgeId)],
    /// Current position in the entries slice
    pos: usize,
}

impl<'a> AdjacencyIterator<'a> {
    /// Create an iterator over a sorted adjacency slice
    pub fn new(entries: &'a [(NodeId, EdgeId)]) -> Self {
        Self { entries, pos: 0 }
    }

    /// Current node ID (None if at end)
    #[inline]
    pub fn key(&self) -> Option<NodeId> {
        self.entries.get(self.pos).map(|&(nid, _)| nid)
    }

    /// Current (NodeId, EdgeId) pair
    #[inline]
    pub fn current(&self) -> Option<(NodeId, EdgeId)> {
        self.entries.get(self.pos).copied()
    }

    /// Advance to next entry
    #[inline]
    pub fn next(&mut self) {
        if self.pos < self.entries.len() {
            self.pos += 1;
        }
    }

    /// Seek to the first entry with NodeId >= target.
    /// O(log n) via binary search on the sorted entries.
    #[inline]
    pub fn seek(&mut self, target: NodeId) {
        // Binary search from current position (not from 0 — entries before pos are already passed)
        let remaining = &self.entries[self.pos..];
        match remaining.binary_search_by_key(&target, |&(nid, _)| nid) {
            Ok(offset) => self.pos += offset,
            Err(offset) => self.pos += offset,
        }
    }

    /// Is the iterator exhausted?
    #[inline]
    pub fn at_end(&self) -> bool {
        self.pos >= self.entries.len()
    }

    /// Number of remaining entries
    #[inline]
    pub fn remaining(&self) -> usize {
        self.entries.len() - self.pos
    }

    /// Reset to beginning
    pub fn reset(&mut self) {
        self.pos = 0;
    }
}

/// LeapFrog Join: intersect multiple sorted iterators.
/// Finds all NodeIds that appear in ALL iterators simultaneously.
///
/// Algorithm:
/// 1. Sort iterators by current key (smallest first)
/// 2. If all keys match → emit match, advance first iterator
/// 3. Otherwise → smallest iterator seeks to largest's key
/// 4. Repeat until any iterator is exhausted
pub struct LeapFrogJoin<'a> {
    /// The participating iterators, maintained in sorted order by current key
    iters: Vec<AdjacencyIterator<'a>>,
    /// Whether the join has been initialized
    initialized: bool,
}

impl<'a> LeapFrogJoin<'a> {
    /// Create a new LeapFrog join over multiple sorted adjacency lists.
    /// Each list is a node's sorted neighbor list.
    pub fn new(iters: Vec<AdjacencyIterator<'a>>) -> Self {
        Self {
            iters,
            initialized: false,
        }
    }

    /// Initialize the join — sort iterators by their current key.
    fn init(&mut self) {
        let original_count = self.iters.len();
        // Remove exhausted iterators
        self.iters.retain(|it| !it.at_end());
        // If any iterator was removed (empty), intersection is empty
        if self.iters.len() < original_count {
            self.iters.clear(); // force empty result
        }
        // Sort by current key
        self.iters.sort_by_key(|it| it.key().unwrap_or(NodeId::new(u64::MAX)));
        self.initialized = true;
    }

    /// Find the next matching NodeId (appears in all iterators).
    /// Returns None when no more matches exist.
    pub fn next_match(&mut self) -> Option<NodeId> {
        if !self.initialized {
            self.init();
        }

        if self.iters.is_empty() {
            return None;
        }
        if self.iters.len() == 1 {
            let result = self.iters[0].key();
            self.iters[0].next();
            return result;
        }

        let k = self.iters.len();

        loop {
            // Check if any iterator is exhausted
            if self.iters.iter().any(|it| it.at_end()) {
                return None;
            }

            let min_key = self.iters[0].key()?;
            let max_key = self.iters[k - 1].key()?;

            if min_key == max_key {
                // All iterators agree — this is a match!
                // Advance the first iterator for the next call
                self.iters[0].next();
                // Re-sort (rotate: move first to correct position)
                if k > 1 && !self.iters[0].at_end() {
                    let new_key = self.iters[0].key().unwrap_or(NodeId::new(u64::MAX));
                    // Find insertion point
                    let insert_pos = self.iters[1..].partition_point(|it| {
                        it.key().unwrap_or(NodeId::new(u64::MAX)) < new_key
                    });
                    if insert_pos > 0 {
                        self.iters[..=insert_pos].rotate_left(1);
                    }
                }
                return Some(min_key);
            } else {
                // Smallest iterator seeks to largest's key
                self.iters[0].seek(max_key);
                // Re-sort after seek
                if !self.iters[0].at_end() {
                    let new_key = self.iters[0].key().unwrap_or(NodeId::new(u64::MAX));
                    let insert_pos = self.iters[1..].partition_point(|it| {
                        it.key().unwrap_or(NodeId::new(u64::MAX)) < new_key
                    });
                    if insert_pos > 0 {
                        self.iters[..=insert_pos].rotate_left(1);
                    }
                }
            }
        }
    }

    /// Collect all matches into a Vec.
    pub fn collect_all(&mut self) -> Vec<NodeId> {
        let mut results = Vec::new();
        while let Some(nid) = self.next_match() {
            results.push(nid);
        }
        results
    }
}

/// Count triangles in the graph using LeapFrog intersection.
/// Triangle: (a)->(b)->(c)->(a)
/// For each edge (a,b), count |N_out(b) ∩ N_in(a)| = number of c values.
pub fn count_triangles_leapfrog(store: &GraphStore) -> u64 {
    let mut count = 0u64;
    let node_count = store.node_count();

    for a_idx in 0..node_count {
        let a = NodeId::new((a_idx + 1) as u64);

        // Get sorted outgoing neighbors of a
        let a_out = store.frozen_outgoing_neighbors(a.as_u64() as usize);
        // Get write buffer outgoing for a
        let a_out_buf = store.get_outgoing_neighbor_slice(a);

        // For each outgoing neighbor b of a
        for &(b, _) in a_out.iter().chain(a_out_buf.iter()) {
            // Get sorted outgoing neighbors of b
            let b_out = store.frozen_outgoing_neighbors(b.as_u64() as usize);
            let b_out_buf = store.get_outgoing_neighbor_slice(b);

            // Get sorted incoming neighbors of a (these are nodes that point TO a)
            let a_in = store.frozen_incoming_neighbors(a.as_u64() as usize);
            let a_in_buf = store.get_incoming_neighbor_slice(a);

            // Intersect N_out(b) ∩ N_in(a) to find c values
            // Use LeapFrog on the two sorted lists
            let mut iter_b_out = AdjacencyIterator::new(&b_out);
            let mut iter_a_in = AdjacencyIterator::new(&a_in);

            // Also check write buffers
            // For now, use the frozen tier only (write buffer is unsorted)
            let mut join = LeapFrogJoin::new(vec![iter_b_out, iter_a_in]);
            count += join.collect_all().len() as u64;
        }
    }

    count
}

// ============================================================
// TrieJoinOperator — Physical operator for WCO cyclic joins
// ============================================================

/// A constraint for the TrieJoinOperator: which adjacency list to intersect.
#[derive(Debug, Clone)]
pub struct PhysicalTrieConstraint {
    /// The already-bound variable whose neighbor list we use
    pub bound_var: String,
    /// Outgoing or Incoming adjacency list
    pub direction: Direction,
    /// Optional edge type filter
    pub edge_types: Vec<String>,
    /// Optional edge variable to bind
    pub edge_var: Option<String>,
}

/// Worst-Case Optimal Join operator using LeapFrog intersection.
///
/// For each input record with some variables already bound, finds all values
/// of `target_var` that satisfy ALL adjacency constraints simultaneously.
/// Uses sorted-list intersection (LeapFrog) instead of nested-loop checking.
///
/// Example: triangle (a)->(b)->(c)->(a)
///   Input binds: {a, b}
///   Constraints: [c ∈ N_out(b), c ∈ N_in(a)]
///   LeapFrog intersects both lists → emits one record per valid c
pub struct TrieJoinOperator {
    input: OperatorBox,
    target_var: String,
    constraints: Vec<PhysicalTrieConstraint>,
    /// Buffered matches for the current input record
    current_record: Option<Record>,
    current_matches: Vec<NodeId>,
    /// Neighbor lists kept alive for edge variable resolution
    current_neighbor_lists: Vec<Vec<(NodeId, EdgeId)>>,
    match_index: usize,
}

impl TrieJoinOperator {
    pub fn new(
        input: OperatorBox,
        target_var: String,
        constraints: Vec<PhysicalTrieConstraint>,
    ) -> Self {
        Self {
            input,
            target_var,
            constraints,
            current_record: None,
            current_matches: Vec::new(),
            current_neighbor_lists: Vec::new(),
            match_index: 0,
        }
    }

    /// Build sorted neighbor lists for each constraint given the current record.
    fn build_neighbor_lists(&self, record: &Record, store: &GraphStore) -> Vec<Vec<(NodeId, EdgeId)>> {
        let mut lists = Vec::with_capacity(self.constraints.len());

        for constraint in &self.constraints {
            let bound_id = match record.get(&constraint.bound_var).and_then(|v| v.node_id()) {
                Some(id) => id,
                None => {
                    lists.push(Vec::new());
                    continue;
                }
            };

            let idx = bound_id.as_u64() as usize;

            // Get frozen (CSR) neighbors — already sorted
            let mut neighbors = match constraint.direction {
                Direction::Outgoing => store.frozen_outgoing_neighbors(idx),
                Direction::Incoming => store.frozen_incoming_neighbors(idx),
                Direction::Both => {
                    let mut out = store.frozen_outgoing_neighbors(idx);
                    let inc = store.frozen_incoming_neighbors(idx);
                    out.extend(inc);
                    out.sort_by_key(|&(nid, _)| nid);
                    out.dedup_by_key(|entry| entry.0);
                    out
                }
            };

            // Also include write buffer neighbors
            let buf = match constraint.direction {
                Direction::Outgoing => store.get_outgoing_neighbor_slice(bound_id),
                Direction::Incoming => store.get_incoming_neighbor_slice(bound_id),
                Direction::Both => &[], // handled above
            };
            if !buf.is_empty() {
                neighbors.extend_from_slice(buf);
                neighbors.sort_by_key(|&(nid, _)| nid);
                neighbors.dedup_by_key(|entry| entry.0);
            }

            // Filter by edge type if specified
            if !constraint.edge_types.is_empty() {
                neighbors.retain(|&(_, eid)| {
                    if let Some(et) = store.get_edge_type(eid) {
                        constraint.edge_types.iter().any(|t| t == et.as_str())
                    } else {
                        false
                    }
                });
            }

            lists.push(neighbors);
        }

        lists
    }
}

impl PhysicalOperator for TrieJoinOperator {
    fn next(&mut self, store: &GraphStore) -> ExecutionResult<Option<Record>> {
        loop {
            // Emit buffered matches
            if self.match_index < self.current_matches.len() {
                let matched_node = self.current_matches[self.match_index];
                self.match_index += 1;

                let mut record = self.current_record.as_ref().unwrap().clone();
                record.bind(self.target_var.clone(), Value::NodeRef(matched_node));

                // Bind edge variables by looking up EdgeId from neighbor lists
                for (i, constraint) in self.constraints.iter().enumerate() {
                    if let Some(ref edge_var) = constraint.edge_var {
                        let list = &self.current_neighbor_lists[i];
                        if let Ok(pos) = list.binary_search_by_key(&matched_node, |&(nid, _)| nid) {
                            let (_, edge_id) = list[pos];
                            if let Some(et) = store.get_edge_type(edge_id) {
                                let bound_id = record.get(&constraint.bound_var)
                                    .and_then(|v| v.node_id())
                                    .unwrap_or(NodeId::new(0));
                                let (src, tgt) = match constraint.direction {
                                    Direction::Outgoing => (bound_id, matched_node),
                                    Direction::Incoming => (matched_node, bound_id),
                                    Direction::Both => (bound_id, matched_node),
                                };
                                record.bind(edge_var.clone(), Value::EdgeRef(edge_id, src, tgt, et));
                            }
                        }
                    }
                }

                return Ok(Some(record));
            }

            // Pull next input record
            let record = match self.input.next(store)? {
                Some(r) => r,
                None => return Ok(None),
            };

            // Build neighbor lists and run LeapFrog intersection
            let neighbor_lists = self.build_neighbor_lists(&record, store);

            // If any constraint has an empty list, no matches possible
            if neighbor_lists.iter().any(|l| l.is_empty()) {
                self.current_record = Some(record);
                self.current_matches.clear();
                self.current_neighbor_lists = neighbor_lists;
                self.match_index = 0;
                continue;
            }

            let matches = {
                let iters: Vec<AdjacencyIterator> = neighbor_lists.iter()
                    .map(|list| AdjacencyIterator::new(list.as_slice()))
                    .collect();
                let mut join = LeapFrogJoin::new(iters);
                join.collect_all()
            };

            self.current_neighbor_lists = neighbor_lists;
            self.current_matches = matches;
            self.match_index = 0;
            self.current_record = Some(record);
        }
    }

    fn reset(&mut self) {
        self.input.reset();
        self.current_record = None;
        self.current_matches.clear();
        self.current_neighbor_lists.clear();
        self.match_index = 0;
    }

    fn describe(&self) -> OperatorDescription {
        let constraints_str: Vec<String> = self.constraints.iter().map(|c| {
            let dir = match c.direction {
                Direction::Outgoing => format!("N_out({})", c.bound_var),
                Direction::Incoming => format!("N_in({})", c.bound_var),
                Direction::Both => format!("N_both({})", c.bound_var),
            };
            if c.edge_types.is_empty() { dir } else {
                format!("{}[:{}]", dir, c.edge_types.join("|"))
            }
        }).collect();

        OperatorDescription {
            name: "TrieJoin".to_string(),
            details: format!("{} ∈ {}", self.target_var, constraints_str.join(" ∩ ")),
            children: vec![self.input.describe()],
        }
    }
}

// ============================================================
// TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entries(node_ids: &[u64]) -> Vec<(NodeId, EdgeId)> {
        node_ids.iter().map(|&id| (NodeId::new(id), EdgeId::new(id * 100))).collect()
    }

    #[test]
    fn test_adjacency_iterator_basic() {
        let entries = make_entries(&[1, 3, 5, 7, 9]);
        let mut it = AdjacencyIterator::new(&entries);

        assert_eq!(it.key(), Some(NodeId::new(1)));
        it.next();
        assert_eq!(it.key(), Some(NodeId::new(3)));
        it.next();
        assert_eq!(it.key(), Some(NodeId::new(5)));
        assert!(!it.at_end());
        assert_eq!(it.remaining(), 3);
    }

    #[test]
    fn test_adjacency_iterator_seek() {
        let entries = make_entries(&[1, 3, 5, 7, 9, 11, 13]);
        let mut it = AdjacencyIterator::new(&entries);

        it.seek(NodeId::new(5));
        assert_eq!(it.key(), Some(NodeId::new(5)));

        it.seek(NodeId::new(8));
        assert_eq!(it.key(), Some(NodeId::new(9)));

        it.seek(NodeId::new(20));
        assert!(it.at_end());
    }

    #[test]
    fn test_adjacency_iterator_seek_from_position() {
        let entries = make_entries(&[1, 3, 5, 7, 9]);
        let mut it = AdjacencyIterator::new(&entries);

        it.next(); // at 3
        it.next(); // at 5
        it.seek(NodeId::new(7)); // should find 7, not restart from 1
        assert_eq!(it.key(), Some(NodeId::new(7)));
    }

    #[test]
    fn test_leapfrog_join_two_lists() {
        let a = make_entries(&[1, 3, 5, 7, 9]);
        let b = make_entries(&[2, 3, 5, 8, 9, 10]);

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b]);

        let matches = join.collect_all();
        let match_ids: Vec<u64> = matches.iter().map(|n| n.as_u64()).collect();
        assert_eq!(match_ids, vec![3, 5, 9]);
    }

    #[test]
    fn test_leapfrog_join_three_lists() {
        let a = make_entries(&[1, 2, 3, 5, 7, 9]);
        let b = make_entries(&[2, 3, 5, 8, 9, 10]);
        let c = make_entries(&[3, 4, 5, 6, 9]);

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let iter_c = AdjacencyIterator::new(&c);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b, iter_c]);

        let matches = join.collect_all();
        let match_ids: Vec<u64> = matches.iter().map(|n| n.as_u64()).collect();
        assert_eq!(match_ids, vec![3, 5, 9]);
    }

    #[test]
    fn test_leapfrog_join_no_overlap() {
        let a = make_entries(&[1, 3, 5]);
        let b = make_entries(&[2, 4, 6]);

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b]);

        assert!(join.collect_all().is_empty());
    }

    #[test]
    fn test_leapfrog_join_single_match() {
        let a = make_entries(&[1, 5, 100]);
        let b = make_entries(&[5, 200]);

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b]);

        let matches = join.collect_all();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], NodeId::new(5));
    }

    #[test]
    fn test_leapfrog_join_empty_list() {
        let a = make_entries(&[1, 3, 5]);
        let b: Vec<(NodeId, EdgeId)> = vec![];

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b]);

        assert!(join.collect_all().is_empty());
    }

    #[test]
    fn test_leapfrog_join_identical_lists() {
        let a = make_entries(&[1, 2, 3, 4, 5]);
        let b = make_entries(&[1, 2, 3, 4, 5]);

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b]);

        let matches = join.collect_all();
        assert_eq!(matches.len(), 5);
    }

    #[test]
    fn test_leapfrog_join_large_gap() {
        let a = make_entries(&[1, 1_000_000]);
        let b = make_entries(&[500_000, 1_000_000, 2_000_000]);

        let iter_a = AdjacencyIterator::new(&a);
        let iter_b = AdjacencyIterator::new(&b);
        let mut join = LeapFrogJoin::new(vec![iter_a, iter_b]);

        let matches = join.collect_all();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0], NodeId::new(1_000_000));
    }

    // ========================================================
    // TrieJoinOperator integration tests
    // ========================================================

    use super::super::operator::NodeScanOperator;
    use super::super::operator::ExpandOperator;
    use crate::graph::types::Label;

    /// Build a triangle graph: a->b, b->c, c->a
    fn build_triangle_graph() -> GraphStore {
        let mut g = GraphStore::new();
        let a = g.create_node("Node");
        let b = g.create_node("Node");
        let c = g.create_node("Node");
        g.create_edge(a, b, "EDGE").unwrap();
        g.create_edge(b, c, "EDGE").unwrap();
        g.create_edge(c, a, "EDGE").unwrap();
        // Compact to CSR so frozen neighbors are available
        g.compact_adjacency();
        g
    }

    /// Build a graph with 2 triangles sharing an edge
    fn build_double_triangle_graph() -> GraphStore {
        let mut g = GraphStore::new();
        let a = g.create_node("Node"); // 1
        let b = g.create_node("Node"); // 2
        let c = g.create_node("Node"); // 3
        let d = g.create_node("Node"); // 4
        // Triangle 1: a->b->c->a
        g.create_edge(a, b, "EDGE").unwrap();
        g.create_edge(b, c, "EDGE").unwrap();
        g.create_edge(c, a, "EDGE").unwrap();
        // Triangle 2: a->b->d->a
        g.create_edge(b, d, "EDGE").unwrap();
        g.create_edge(d, a, "EDGE").unwrap();
        g.compact_adjacency();
        g
    }

    #[test]
    fn test_trie_join_operator_triangle() {
        let store = build_triangle_graph();
        // Triangle: (a)->(b)->(c)->(a)
        // Input: scan all "Node" as a, expand to b
        // TrieJoin: find c where c ∈ N_out(b) ∩ N_in(a)

        let scan = Box::new(NodeScanOperator::new("a".to_string(), vec![Label::new("Node")]));
        let expand = Box::new(ExpandOperator::new(
            scan,
            "a".to_string(),
            "b".to_string(),
            None,
            vec!["EDGE".to_string()],
            crate::query::ast::Direction::Outgoing,
        ));

        let mut trie_join = TrieJoinOperator::new(
            expand,
            "c".to_string(),
            vec![
                PhysicalTrieConstraint {
                    bound_var: "b".to_string(),
                    direction: Direction::Outgoing,
                    edge_types: vec![],
                    edge_var: None,
                },
                PhysicalTrieConstraint {
                    bound_var: "a".to_string(),
                    direction: Direction::Incoming,
                    edge_types: vec![],
                    edge_var: None,
                },
            ],
        );

        // Collect all triangle results
        let mut results = Vec::new();
        while let Some(record) = trie_join.next(&store).unwrap() {
            let a = record.get("a").unwrap().node_id().unwrap();
            let b = record.get("b").unwrap().node_id().unwrap();
            let c = record.get("c").unwrap().node_id().unwrap();
            results.push((a.as_u64(), b.as_u64(), c.as_u64()));
        }

        // Single triangle: 3 nodes × 1 rotation each = 3 triangles
        // (1,2,3), (2,3,1), (3,1,2)
        assert_eq!(results.len(), 3, "Expected 3 triangle instances, got {:?}", results);

        // All three rotations should be present
        assert!(results.contains(&(1, 2, 3)));
        assert!(results.contains(&(2, 3, 1)));
        assert!(results.contains(&(3, 1, 2)));
    }

    #[test]
    fn test_trie_join_operator_double_triangle() {
        let store = build_double_triangle_graph();
        // Two triangles sharing edge a->b:
        // T1: (1)->(2)->(3)->(1)
        // T2: (1)->(2)->(4)->(1)

        let scan = Box::new(NodeScanOperator::new("a".to_string(), vec![Label::new("Node")]));
        let expand = Box::new(ExpandOperator::new(
            scan,
            "a".to_string(),
            "b".to_string(),
            None,
            vec!["EDGE".to_string()],
            crate::query::ast::Direction::Outgoing,
        ));

        let mut trie_join = TrieJoinOperator::new(
            expand,
            "c".to_string(),
            vec![
                PhysicalTrieConstraint {
                    bound_var: "b".to_string(),
                    direction: Direction::Outgoing,
                    edge_types: vec![],
                    edge_var: None,
                },
                PhysicalTrieConstraint {
                    bound_var: "a".to_string(),
                    direction: Direction::Incoming,
                    edge_types: vec![],
                    edge_var: None,
                },
            ],
        );

        let mut count = 0;
        while let Some(_record) = trie_join.next(&store).unwrap() {
            count += 1;
        }

        // 2 triangles × 3 rotations = 6 total triangle instances
        assert_eq!(count, 6, "Expected 6 triangle instances in double-triangle graph");
    }

    #[test]
    fn test_trie_join_operator_no_triangles() {
        // Chain graph: a->b->c (no cycle)
        let mut g = GraphStore::new();
        let a = g.create_node("Node");
        let b = g.create_node("Node");
        let c = g.create_node("Node");
        g.create_edge(a, b, "EDGE").unwrap();
        g.create_edge(b, c, "EDGE").unwrap();
        g.compact_adjacency();

        let scan = Box::new(NodeScanOperator::new("a".to_string(), vec![Label::new("Node")]));
        let expand = Box::new(ExpandOperator::new(
            scan,
            "a".to_string(),
            "b".to_string(),
            None,
            vec!["EDGE".to_string()],
            crate::query::ast::Direction::Outgoing,
        ));

        let mut trie_join = TrieJoinOperator::new(
            expand,
            "c".to_string(),
            vec![
                PhysicalTrieConstraint {
                    bound_var: "b".to_string(),
                    direction: Direction::Outgoing,
                    edge_types: vec![],
                    edge_var: None,
                },
                PhysicalTrieConstraint {
                    bound_var: "a".to_string(),
                    direction: Direction::Incoming,
                    edge_types: vec![],
                    edge_var: None,
                },
            ],
        );

        let mut count = 0;
        while let Some(_) = trie_join.next(&g).unwrap() {
            count += 1;
        }
        assert_eq!(count, 0, "Chain graph should have no triangles");
    }

    #[test]
    fn test_trie_join_describe() {
        let scan = Box::new(NodeScanOperator::new("a".to_string(), vec![]));
        let tj = TrieJoinOperator::new(
            scan,
            "c".to_string(),
            vec![
                PhysicalTrieConstraint {
                    bound_var: "b".to_string(),
                    direction: Direction::Outgoing,
                    edge_types: vec![],
                    edge_var: None,
                },
                PhysicalTrieConstraint {
                    bound_var: "a".to_string(),
                    direction: Direction::Incoming,
                    edge_types: vec!["KNOWS".to_string()],
                    edge_var: None,
                },
            ],
        );

        let desc = tj.describe();
        assert_eq!(desc.name, "TrieJoin");
        assert!(desc.details.contains("N_out(b)"), "Should show N_out(b): {}", desc.details);
        assert!(desc.details.contains("N_in(a)"), "Should show N_in(a): {}", desc.details);
        assert!(desc.details.contains("KNOWS"), "Should show edge type: {}", desc.details);
    }
}
