//! The MVCC version chain for a node, with the common single-version case
//! stored inline instead of on the heap (#477, `PERF-10`).
//!
//! ## Why this exists
//!
//! `GraphStore` held nodes as `Vec<Vec<Node>>`: the outer index is the node id,
//! the inner `Vec` is that node's MVCC version chain. The footprint harness
//! (`benches/memory_footprint.rs`) measured the consequence at 100k nodes:
//! **748 bytes for a bare node whose `Node` struct is 128 bytes** by
//! `size_of`. One of the three allocations behind that gap is a heap `Vec`
//! holding exactly one element.
//!
//! It holds exactly one element for every node that has never been updated,
//! which is every node in a freshly loaded graph and the overwhelming majority
//! in most graphs after that. The chain grows past one only on a write in a
//! *later* transaction than the one that created the node.
//!
//! So the common case pays a `Vec` header (24 B in the outer vector), a heap
//! allocation of 128 B for the single `Node`, and the allocator's own header
//! and rounding on top of it -- to store one value whose size is known.
//!
//! ## What this changes
//!
//! [`NodeVersions`] holds the first version inline and only allocates when a
//! second one appears:
//!
//! ```text
//!                     bytes in the outer Vec   heap allocations
//!   Vec<Node>, len 1              24               1  (128 B + header)
//!   NodeVersions::One            136               0
//! ```
//!
//! A one-version node therefore costs 136 B instead of ~168 B, and one fewer
//! trip to the allocator. The allocation count is the figure that matters
//! twice: it is a large part of the per-node byte overhead (`PERF-10`) and it
//! is a hard ceiling on insert throughput (`PERF-14`, #491).
//!
//! ## What this deliberately does not change
//!
//! The semantics are the `Vec`'s, exactly. Versions stay in push order, `last`
//! is the newest, `pop` removes it, and a chain that grows past one element
//! becomes an ordinary `Vec` with no further special cases. The API below is
//! the subset of `Vec`'s that `GraphStore` uses, with the same names and the
//! same meanings, so a call site reads the same before and after.
//!
//! The trade is that an *empty* slot -- an id never allocated, or one whose
//! node was deleted -- grows from 24 B to 136 B. That is a real cost on a
//! sparse id space. Node ids are assigned densely from a counter and freed ids
//! are reused (`free_node_ids`), so in practice the slot vector is dense; the
//! `Empty` variant exists for the gaps a deletion leaves behind, not as a
//! common case.

use super::node::Node;

/// A node's MVCC version chain: ordered oldest → newest, as `Vec<Node>` was.
///
/// See the module docs for why the one-element case is special-cased.
#[derive(Debug, Clone, Default)]
pub enum NodeVersions {
    /// No node at this id: never allocated, or deleted.
    #[default]
    Empty,
    /// Exactly one version, stored inline. The common case.
    One(Node),
    /// Two or more versions. Never constructed with fewer than two elements
    /// by `push`, but `drain`/`pop` may leave it holding one or zero — the
    /// accessors below do not depend on which variant a given length uses.
    Many(Vec<Node>),
}

impl NodeVersions {
    /// Number of versions in the chain.
    pub fn len(&self) -> usize {
        match self {
            NodeVersions::Empty => 0,
            NodeVersions::One(_) => 1,
            NodeVersions::Many(v) => v.len(),
        }
    }

    /// True when the chain holds no versions — i.e. there is no node here.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The newest version, or `None` if there is no node at this id.
    pub fn last(&self) -> Option<&Node> {
        match self {
            NodeVersions::Empty => None,
            NodeVersions::One(n) => Some(n),
            NodeVersions::Many(v) => v.last(),
        }
    }

    /// The newest version, mutably.
    pub fn last_mut(&mut self) -> Option<&mut Node> {
        match self {
            NodeVersions::Empty => None,
            NodeVersions::One(n) => Some(n),
            NodeVersions::Many(v) => v.last_mut(),
        }
    }

    /// Append a version, making it the newest.
    ///
    /// The first push stores inline; the second promotes to a heap `Vec` sized
    /// for exactly the two versions it now holds, rather than `Vec`'s default
    /// growth to four.
    pub fn push(&mut self, node: Node) {
        match self {
            NodeVersions::Empty => *self = NodeVersions::One(node),
            NodeVersions::One(_) => {
                let NodeVersions::One(first) = std::mem::take(self) else {
                    unreachable!("matched One immediately above")
                };
                let mut v = Vec::with_capacity(2);
                v.push(first);
                v.push(node);
                *self = NodeVersions::Many(v);
            }
            NodeVersions::Many(v) => v.push(node),
        }
    }

    /// Remove and return the newest version.
    pub fn pop(&mut self) -> Option<Node> {
        match self {
            NodeVersions::Empty => None,
            NodeVersions::One(_) => {
                let NodeVersions::One(only) = std::mem::take(self) else {
                    unreachable!("matched One immediately above")
                };
                Some(only)
            }
            NodeVersions::Many(v) => v.pop(),
        }
    }

    /// Versions oldest → newest. `DoubleEndedIterator`, so `.rev()` walks
    /// newest → oldest the way the version lookup does.
    pub fn iter(&self) -> std::slice::Iter<'_, Node> {
        self.as_slice().iter()
    }

    /// Remove the first `n` versions, keeping the rest in order.
    ///
    /// Named for the `Vec::drain(..n)` it replaces at the one call site that
    /// used it (version GC). Unlike `drain` it returns nothing: no caller
    /// wanted the removed values, and returning them would mean either
    /// allocating or handing back a borrow that pins the chain.
    pub fn drop_first(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        match self {
            NodeVersions::Empty => {}
            NodeVersions::One(_) => *self = NodeVersions::Empty,
            NodeVersions::Many(v) => {
                if n >= v.len() {
                    *self = NodeVersions::Empty;
                } else {
                    v.drain(..n);
                }
            }
        }
    }

    /// The chain as a contiguous slice, oldest → newest.
    pub fn as_slice(&self) -> &[Node] {
        match self {
            NodeVersions::Empty => &[],
            NodeVersions::One(n) => std::slice::from_ref(n),
            NodeVersions::Many(v) => v.as_slice(),
        }
    }
}

impl<'a> IntoIterator for &'a NodeVersions {
    type Item = &'a Node;
    type IntoIter = std::slice::Iter<'a, Node>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::types::NodeId;

    fn node(version: u64) -> Node {
        let mut n = Node::new(NodeId::new(7), "Person");
        n.version = version;
        n
    }

    #[test]
    fn empty_chain_reads_as_absent() {
        let chain = NodeVersions::default();
        assert_eq!(chain.len(), 0);
        assert!(chain.is_empty());
        assert!(chain.last().is_none());
        assert!(chain.as_slice().is_empty());
        assert_eq!(chain.iter().count(), 0);
    }

    #[test]
    fn one_version_stays_inline() {
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        assert!(matches!(chain, NodeVersions::One(_)));
        assert_eq!(chain.len(), 1);
        assert!(!chain.is_empty());
        assert_eq!(chain.last().unwrap().version, 1);
        assert_eq!(chain.as_slice().len(), 1);
    }

    #[test]
    fn second_version_promotes_and_keeps_order() {
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        chain.push(node(5));
        assert!(matches!(chain, NodeVersions::Many(_)));
        let versions: Vec<u64> = chain.iter().map(|n| n.version).collect();
        assert_eq!(versions, vec![1, 5], "push order is oldest -> newest");
        assert_eq!(chain.last().unwrap().version, 5);
    }

    #[test]
    fn reverse_iteration_finds_the_newest_visible_version() {
        // This is exactly what `get_node_at_version` does.
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        chain.push(node(4));
        chain.push(node(9));

        let visible_at = |v: u64| chain.iter().rev().find(|n| n.version <= v).map(|n| n.version);
        assert_eq!(visible_at(0), None);
        assert_eq!(visible_at(1), Some(1));
        assert_eq!(visible_at(3), Some(1));
        assert_eq!(visible_at(4), Some(4));
        assert_eq!(visible_at(100), Some(9));
    }

    #[test]
    fn pop_returns_newest_and_empties() {
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        chain.push(node(2));
        assert_eq!(chain.pop().unwrap().version, 2);
        assert_eq!(chain.len(), 1);
        assert_eq!(chain.pop().unwrap().version, 1);
        assert!(chain.is_empty());
        assert!(chain.pop().is_none());
    }

    #[test]
    fn pop_on_inline_single_returns_it() {
        let mut chain = NodeVersions::default();
        chain.push(node(3));
        assert_eq!(chain.pop().unwrap().version, 3);
        assert!(chain.is_empty());
    }

    #[test]
    fn last_mut_writes_through_in_both_representations() {
        let mut inline = NodeVersions::default();
        inline.push(node(1));
        inline.last_mut().unwrap().version = 42;
        assert_eq!(inline.last().unwrap().version, 42);

        let mut heap = NodeVersions::default();
        heap.push(node(1));
        heap.push(node(2));
        heap.last_mut().unwrap().version = 42;
        assert_eq!(heap.last().unwrap().version, 42);
        assert_eq!(heap.iter().next().unwrap().version, 1, "older version untouched");
    }

    #[test]
    fn drop_first_prunes_the_oldest_versions() {
        let mut chain = NodeVersions::default();
        for v in [1, 2, 3, 4] {
            chain.push(node(v));
        }
        chain.drop_first(2);
        let versions: Vec<u64> = chain.iter().map(|n| n.version).collect();
        assert_eq!(versions, vec![3, 4]);
    }

    #[test]
    fn drop_first_of_zero_is_a_no_op() {
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        chain.drop_first(0);
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn drop_first_past_the_end_empties() {
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        chain.push(node(2));
        chain.drop_first(9);
        assert!(chain.is_empty());

        let mut inline = NodeVersions::default();
        inline.push(node(1));
        inline.drop_first(1);
        assert!(inline.is_empty());
    }

    #[test]
    fn a_reused_slot_starts_empty_again() {
        // `free_node_ids` hands a deleted id back to the next create; the slot
        // it lands in must read as absent in between.
        let mut chain = NodeVersions::default();
        chain.push(node(1));
        chain.pop();
        assert!(chain.last().is_none());
        chain.push(node(2));
        assert_eq!(chain.len(), 1);
        assert_eq!(chain.last().unwrap().version, 2);
    }

    #[test]
    fn the_inline_case_costs_no_more_than_the_node_plus_a_tag() {
        // The whole point of the type. If `Node` grows a heap-free field this
        // stays true; if the enum ever gains a variant larger than `Node` it
        // does not, and the saving is gone.
        assert_eq!(
            std::mem::size_of::<NodeVersions>(),
            std::mem::size_of::<Node>() + std::mem::size_of::<usize>(),
        );
    }
}
