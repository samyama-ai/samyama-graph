//! What was added to a graph's structure since a given point (ALGO-14, #1697).
//!
//! An incremental algorithm keeps its answer between calls and needs to know
//! what changed since: the nodes and relationships created, and whether
//! anything was taken away. `data_changes` says only *that* the graph
//! changed, and the durability write log (`take_write_log`) is drained by its
//! one consumer each statement, so neither can tell a later reader what it
//! missed.
//!
//! The log keeps two things:
//!
//! * the ids of nodes and relationships added, in order -- but only once some
//!   reader has asked for them ([`StructureLog::start_recording`]), so a store
//!   nobody runs an incremental algorithm on pays one branch per write;
//! * a count of **rewrites**: anything that removes structure or changes which
//!   label a node has. It is kept always, because it is one integer, and a
//!   reader that sees it move rebuilds from scratch. Deletions are not
//!   replayed: undoing a union-find merge or a PageRank push is a rebuild
//!   anyway.
//!
//! Ids are recorded rather than payloads. A reader looks the id up in the
//! store when it applies the change; an id that no longer resolves was
//! deleted, which also counted as a rewrite, so the reader is rebuilding.
//!
//! The log is bounded by [`MAX_RECORDED_ADDITIONS`]. When it fills, the older
//! half is dropped and a reader whose position fell off the front rebuilds --
//! slower, never wrong.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

use super::types::{EdgeId, NodeId};

/// The most additions held. At 16 bytes each this is 16 MiB; a reader that
/// falls this far behind would apply a million changes one by one, which
/// costs more than the rebuild it is told to do instead.
pub const MAX_RECORDED_ADDITIONS: usize = 1 << 20;

/// One addition to a graph's structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Addition {
    Node(NodeId),
    Edge(EdgeId),
}

/// Where a reader is in the log: the next addition it has not seen, and the
/// rewrite count it last saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogPosition {
    next: u64,
    rewrites: u64,
}

/// Why a reader cannot catch up from its position, and must rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUp {
    /// Something was removed or relabelled since the position was taken.
    Rewritten,
    /// The additions since the position were dropped to bound the log.
    Dropped,
}

#[derive(Debug, Default)]
pub struct StructureLog {
    /// Set from a read path, so atomic; written to only under `&mut`.
    recording: AtomicBool,
    /// Sequence number of `additions[0]`.
    first: u64,
    additions: VecDeque<Addition>,
    rewrites: u64,
}

impl StructureLog {
    /// Begin recording additions. Idempotent. A reader calls this before it
    /// takes its first [`position`](Self::position), so nothing it needs is
    /// missed: additions before that point are in the view it built.
    pub fn start_recording(&self) {
        self.recording.store(true, Ordering::Relaxed);
    }

    pub fn is_recording(&self) -> bool {
        self.recording.load(Ordering::Relaxed)
    }

    pub fn position(&self) -> LogPosition {
        LogPosition {
            next: self.first + self.additions.len() as u64,
            rewrites: self.rewrites,
        }
    }

    pub(crate) fn node_added(&mut self, id: NodeId) {
        self.record(Addition::Node(id));
    }

    pub(crate) fn edge_added(&mut self, id: EdgeId) {
        self.record(Addition::Edge(id));
    }

    /// Something was removed, or a node's labels changed.
    pub(crate) fn rewritten(&mut self) {
        self.rewrites += 1;
    }

    fn record(&mut self, addition: Addition) {
        if !self.is_recording() {
            return;
        }
        if self.additions.len() >= MAX_RECORDED_ADDITIONS {
            let drop = MAX_RECORDED_ADDITIONS / 2;
            self.additions.drain(..drop);
            self.first += drop as u64;
        }
        self.additions.push_back(addition);
    }

    /// The additions since `from`, oldest first, or why there is no such list.
    pub fn since(&self, from: LogPosition) -> Result<impl Iterator<Item = Addition> + '_, CatchUp> {
        if from.rewrites != self.rewrites {
            return Err(CatchUp::Rewritten);
        }
        if from.next < self.first {
            return Err(CatchUp::Dropped);
        }
        let skip = (from.next - self.first) as usize;
        Ok(self.additions.iter().skip(skip).copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(i: u64) -> NodeId {
        NodeId::new(i)
    }

    #[test]
    fn nothing_is_recorded_until_a_reader_asks() {
        let mut log = StructureLog::default();
        log.node_added(n(1));
        assert!(!log.is_recording());
        let at = log.position();
        log.start_recording();
        log.node_added(n(2));
        log.edge_added(EdgeId::new(7));
        assert_eq!(
            log.since(at).unwrap().collect::<Vec<_>>(),
            vec![Addition::Node(n(2)), Addition::Edge(EdgeId::new(7))]
        );
        assert_eq!(log.since(log.position()).unwrap().count(), 0);
    }

    #[test]
    fn a_rewrite_sends_every_earlier_reader_to_rebuild() {
        let mut log = StructureLog::default();
        log.start_recording();
        let at = log.position();
        log.node_added(n(1));
        log.rewritten();
        assert_eq!(log.since(at).err(), Some(CatchUp::Rewritten));
        // A position taken after the rewrite is fine.
        let after = log.position();
        log.node_added(n(2));
        assert_eq!(
            log.since(after).unwrap().collect::<Vec<_>>(),
            vec![Addition::Node(n(2))]
        );
    }

    #[test]
    fn rewrites_are_counted_even_when_not_recording() {
        let mut log = StructureLog::default();
        let at = log.position();
        log.rewritten();
        assert_eq!(log.since(at).err(), Some(CatchUp::Rewritten));
    }

    #[test]
    fn a_reader_behind_the_bound_is_told_to_rebuild_and_a_recent_one_is_not() {
        let mut log = StructureLog::default();
        log.start_recording();
        let oldest = log.position();
        for i in 0..MAX_RECORDED_ADDITIONS as u64 {
            log.node_added(n(i));
        }
        let recent = log.position();
        log.node_added(n(u64::MAX));
        assert_eq!(log.since(oldest).err(), Some(CatchUp::Dropped));
        assert_eq!(
            log.since(recent).unwrap().collect::<Vec<_>>(),
            vec![Addition::Node(n(u64::MAX))]
        );
        assert!(log.additions.len() <= MAX_RECORDED_ADDITIONS);
    }
}
