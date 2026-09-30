//! What an index *is*, separated from what it contains (#1477).
//!
//! The three index structures each keep their own registry — `IndexManager`,
//! `FullTextIndexes`, `VectorIndexManager` — and none of them had an on-disk
//! form. The rows came back from RocksDB after a restart and the definitions did
//! not, so `SHOW INDEXES` was empty on a data directory that had three indexes
//! in it, a B-tree predicate replanned as a scan, a full-text search errored, and
//! a vector search returned an empty result with no error at all.
//!
//! This is the shared description the three registries can be read into and
//! written back from. It carries only the *declaration* — never the contents.
//! Contents are rebuilt from the rows on recovery, which is the one thing that
//! cannot go stale: a persisted posting list can disagree with the rows it
//! describes, and a definition cannot.
//!
//! Serialised with bincode into the `indices` column family of the same RocksDB
//! database the rows live in, so an index definition is exactly as durable as the
//! nodes it indexes, and a `DROP` that reaches the rows reaches the catalog in
//! the same write path.

use serde::{Deserialize, Serialize};

/// One index declaration, in the form its registry needs to be rebuilt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexDefinition {
    /// `CREATE INDEX ON :Label(property)`. A composite index is N of these, which
    /// is how `CompositeCreateIndexOperator` already builds it.
    Property { label: String, property: String },
    /// `CREATE CONSTRAINT FOR (n:Label) REQUIRE n.property IS UNIQUE`.
    ///
    /// Carried separately from `Property` even though the constraint also
    /// creates one, because the two registries are separate and only the
    /// constraint one is consulted on write. A constraint that did not survive
    /// a restart is not a slower query — it is a duplicate that gets in.
    UniqueConstraint { label: String, property: String },
    /// `CREATE FULLTEXT INDEX name FOR (n:Label) ON (n.property)`. The name is
    /// part of the definition: `db.index.fulltext.queryNodes` addresses an index
    /// by name and by nothing else.
    FullText { name: String, label: String, property: String },
    /// `CREATE VECTOR INDEX name FOR (n:Label) ON (n.property)`.
    ///
    /// `dimensions` and `metric` must be carried because an index rebuilt at the
    /// wrong dimension silently skips every vector, and one rebuilt under the
    /// wrong metric ranks differently. `quantization` is carried because NDS-09
    /// makes it a choice the caller states in the DDL, and a restart that
    /// promoted an fp16 index to full precision would quietly double its memory.
    Vector {
        name: Option<String>,
        label: String,
        property: String,
        dimensions: usize,
        metric: crate::vector::DistanceMetric,
        quantization: crate::vector::index::Quantization,
    },
}

/// Every index declaration a tenant has, as one record.
///
/// A snapshot of what exists, not a log of what was created. A log would have to
/// replay drops to stay correct, and a dropped index that comes back on the next
/// restart is a worse failure than one that never persisted: the rows it indexed
/// may have been deleted in between.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IndexCatalog {
    pub definitions: Vec<IndexDefinition>,
}

impl IndexCatalog {
    pub fn is_empty(&self) -> bool {
        self.definitions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.definitions.len()
    }
}

/// What a restore actually rebuilt, per structure.
///
/// Returned rather than logged so a caller can say "3 definitions, 3 rebuilt"
/// instead of "3 definitions". The counts are of definitions re-declared and
/// populated, not of entries written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestoredIndexes {
    pub property: usize,
    pub unique: usize,
    pub fulltext: usize,
    pub vector: usize,
    /// Definitions that could not be rebuilt — a vector index whose registry
    /// refused the declaration, for instance. Counted so a partial restore is
    /// visible instead of being read as a smaller catalog.
    pub failed: usize,
}

impl RestoredIndexes {
    pub fn total(&self) -> usize {
        self.property + self.unique + self.fulltext + self.vector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_len_and_emptiness() {
        let mut cat = IndexCatalog::default();
        assert!(cat.is_empty());
        assert_eq!(cat.len(), 0);
        cat.definitions.push(IndexDefinition::Property {
            label: "A".into(),
            property: "p".into(),
        });
        assert!(!cat.is_empty());
        assert_eq!(cat.len(), 1);
    }

    #[test]
    fn restored_total_excludes_failures() {
        let r = RestoredIndexes {
            property: 1,
            unique: 2,
            fulltext: 3,
            vector: 4,
            failed: 5,
        };
        assert_eq!(r.total(), 10);
    }
}
