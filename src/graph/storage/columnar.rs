//! Columnar storage implementation for node and edge properties.
//!
//! Sparse hash-map-based columns: only entries that exist are stored.
//! No memory waste for multi-label graphs where properties differ per label.
//! E.g., "title" only stored for Article nodes, not for Author/MeSH/Chemical nodes.
//!
//! # Cost of a read, and what it is not
//!
//! A property read is **two hash lookups**: the property name to a column, then
//! the row index to a value. Measured over a million integer reads in id order
//! (#531):
//!
//! | access | ns |
//! |---|---:|
//! | `get_property` with `std::collections::HashMap` | 222.4 |
//! | `get_property` with `FxHashMap` (this) | see the bench |
//! | a dense `Vec<i64>` index | 1.3 |
//!
//! The hash function is the part this module can cheaply control, and that is
//! what `FxHashMap` addresses here — `std`'s `SipHash` is a DoS-resistant hash
//! being asked to hash a `usize`, which is not the threat model of a row index.
//!
//! **Most of the remaining cost is not the hash.** At a million rows the inner
//! table is tens of megabytes and the hash scatters access, so a scan in id
//! order — the friendliest order there is — still misses cache on nearly every
//! row. Fixing that means a dense array with a presence bitmap for the columns
//! that are dense enough to deserve one, which is #531's Phase 2 and a real
//! design task: the sparsity argument above is genuine, and a dense array over
//! a rare property in a 66M-node graph would be worse than what is here.
//!
//! So: this is the cheap half, it is measured, and it does not close #531.

use crate::graph::PropertyValue;
use crate::graph::types::NodeId;
use rustc_hash::FxHashMap;
use std::collections::HashMap;

/// How one column stores its values.
///
/// A column is dense whenever dense is *smaller*; see [`dense_is_smaller`].
/// That rule serves both goals at once -- reads become an indexed load rather
/// than a hash lookup (`PERF-05`), and the store never grows (`PERF-10`,
/// `SLT-3`, currently red) -- which is why the decision is computed rather
/// than tuned.
#[derive(Debug, Clone)]
pub enum ColumnData<T> {
    /// Scattered rows: a hash map from row index to value.
    Sparse(FxHashMap<usize, T>),
    /// A contiguous band of rows: `values[idx - base]`, with a presence bit
    /// per slot so an absent row is distinguishable from a stored default.
    ///
    /// `base` is not decoration. Node indices run from a counter, but a
    /// *label's* rows sit in a contiguous band inside that range -- LDBC loads
    /// Places, Organisations, Tags, Persons, Forums, Posts, then Comments, so
    /// `Post.content` is 1.19M consecutive indices starting above 1.1M. Without
    /// a base, a dense array for it would allocate the whole unused prefix.
    Dense {
        base: usize,
        values: Vec<T>,
        /// One bit per slot, `present[i / 64] & (1 << (i % 64))`.
        present: Vec<u64>,
        /// How many slots are present.
        ///
        /// Kept rather than counted: `len()` is consulted on every write that
        /// lands outside the span, and popcounting the bitmap there would make
        /// loading a column quadratic in its length.
        count: usize,
    },
}

/// Is a dense array smaller than a hash map for this shape?
///
/// `hashbrown` stores `(usize, T)` plus one control byte per bucket at a load
/// factor of 7/8, so a sparse entry costs `(8 + size_of::<T>() + 1) * 8/7`.
/// A dense slot costs `size_of::<T>()` plus one presence bit, whether or not
/// the row is present -- so the comparison is per *span*, not per entry.
///
/// The break-even fill factor therefore depends on the element size, and a
/// single global threshold would either waste memory on `String` columns or
/// leave speed unclaimed on `bool` ones:
///
/// | T | sparse B/entry | dense B/slot | break-even fill |
/// |---|---:|---:|---:|
/// | `bool` | 11.4 | 1.125 | 0.10 |
/// | `i64`, `f64` | 19.4 | 8.125 | 0.42 |
/// | `String` | 37.7 | 24.125 | 0.64 |
///
/// String *contents* are heap-allocated either way and cancel out; only the
/// 24-byte `String` header is counted here.
pub fn dense_is_smaller(span: usize, entries: usize, elem_bytes: usize) -> bool {
    if span == 0 || entries == 0 {
        return false;
    }
    // Scaled by 8 throughout to stay in integer arithmetic: a floating-point
    // threshold in a storage decision invites a platform-dependent layout.
    let dense_bits = span.saturating_mul(elem_bytes.saturating_mul(8) + 1);
    let sparse_bits = entries
        .saturating_mul((8 + elem_bytes + 1) * 8 * 8)
        / 7;
    dense_bits < sparse_bits
}

impl<T: Clone + Default> ColumnData<T> {
    fn new() -> Self {
        ColumnData::Sparse(FxHashMap::default())
    }

    fn get(&self, idx: usize) -> Option<&T> {
        match self {
            ColumnData::Sparse(m) => m.get(&idx),
            ColumnData::Dense { base, values, present, .. } => {
                let slot = idx.checked_sub(*base)?;
                if slot >= values.len() || !bit(present, slot) {
                    return None;
                }
                Some(&values[slot])
            }
        }
    }

    fn has(&self, idx: usize) -> bool {
        self.get(idx).is_some()
    }

    fn len(&self) -> usize {
        match self {
            ColumnData::Sparse(m) => m.len(),
            ColumnData::Dense { count, .. } => *count,
        }
    }

    fn remove(&mut self, idx: usize) {
        match self {
            ColumnData::Sparse(m) => {
                m.remove(&idx);
            }
            ColumnData::Dense { base, values, present, count } => {
                // Clear the presence bit and the value. Node ids are recycled
                // through a free list, so a deleted node's slot goes to the
                // next `create_node`; leaving the old value behind is how
                // deleted data reappeared on new nodes (#364). Clearing the
                // bit alone would be enough for `get`, but not for anything
                // that walks `values` directly.
                if let Some(slot) = idx.checked_sub(*base) {
                    if slot < values.len() && bit(present, slot) {
                        clear_bit(present, slot);
                        values[slot] = T::default();
                        *count -= 1;
                    }
                }
            }
        }
    }

    fn set(&mut self, idx: usize, value: T) {
        match self {
            ColumnData::Sparse(m) => {
                m.insert(idx, value);
                self.maybe_promote();
            }
            ColumnData::Dense { base, values, present, count } => {
                if idx >= *base && idx - *base < values.len() {
                    let slot = idx - *base;
                    if !bit(present, slot) {
                        set_bit(present, slot);
                        *count += 1;
                    }
                    values[slot] = value;
                    return;
                }

                // Outside the span. Extend if the wider span is still smaller
                // than a map would be; otherwise fall back to sparse rather
                // than allocate a range for one far-away row. One rule decides
                // both directions, so there is no second policy to keep
                // consistent with the first.
                let entries = *count + 1;
                let elem = std::mem::size_of::<T>();

                if idx >= *base {
                    // Growing upward -- the common case, and the one a load
                    // does once per row. `Vec::resize` reserves geometrically,
                    // so repeatedly extending by one is amortised O(1);
                    // rebuilding into a fresh exactly-sized Vec each time (as
                    // an earlier draft did) makes a 1M-row load quadratic.
                    let new_span = idx - *base + 1;
                    if dense_is_smaller(new_span, entries, elem) {
                        values.resize(new_span, T::default());
                        present.resize(new_span.div_ceil(64), 0);
                        let slot = idx - *base;
                        set_bit(present, slot);
                        values[slot] = value;
                        *count += 1;
                        return;
                    }
                } else {
                    // Below the base. Rare -- properties arrive in id order --
                    // and it needs every slot shifted, so it rebuilds.
                    let new_base = idx;
                    let new_span = *base + values.len() - new_base;
                    if dense_is_smaller(new_span, entries, elem) {
                        self.rebase(new_base, new_span);
                        let ColumnData::Dense { base, values, present, count } = self else {
                            unreachable!("rebase leaves the column dense")
                        };
                        let slot = idx - *base;
                        set_bit(present, slot);
                        values[slot] = value;
                        *count += 1;
                        return;
                    }
                }

                self.demote_to_sparse();
                let ColumnData::Sparse(m) = self else {
                    unreachable!("demote leaves the column sparse")
                };
                m.insert(idx, value);
            }
        }
    }

    /// Shift a dense column down to a lower base.
    ///
    /// Every slot moves, so this rebuilds. Only reached when a row arrives
    /// below everything already stored, which properties loaded in id order
    /// never do.
    fn rebase(&mut self, new_base: usize, new_span: usize) {
        let ColumnData::Dense { base, values, present, count } = self else {
            return;
        };
        let shift = *base - new_base;
        let mut next_values = vec![T::default(); new_span];
        let mut next_present = vec![0u64; new_span.div_ceil(64)];
        for slot in 0..values.len() {
            if bit(present, slot) {
                next_values[slot + shift] = values[slot].clone();
                set_bit(&mut next_present, slot + shift);
            }
        }
        *self = ColumnData::Dense {
            base: new_base,
            values: next_values,
            present: next_present,
            count: *count,
        };
    }

    fn demote_to_sparse(&mut self) {
        let ColumnData::Dense { base, values, present, .. } = self else {
            return;
        };
        let mut m = FxHashMap::default();
        for slot in 0..values.len() {
            if bit(present, slot) {
                m.insert(*base + slot, values[slot].clone());
            }
        }
        *self = ColumnData::Sparse(m);
    }

    /// Visit every present `(row index, value)` pair. Used when a column has
    /// to give up its typed representation for one it cannot hold.
    fn for_each(&self, mut visit: impl FnMut(usize, &T)) {
        match self {
            ColumnData::Sparse(m) => {
                for (idx, value) in m {
                    visit(*idx, value);
                }
            }
            ColumnData::Dense { base, values, present, .. } => {
                for slot in 0..values.len() {
                    if bit(present, slot) {
                        visit(base + slot, &values[slot]);
                    }
                }
            }
        }
    }

    /// Consider promoting a sparse column to dense.
    ///
    /// Deciding needs the index range, which costs a scan of the map, so it is
    /// only considered when `len` crosses a power of two at or above
    /// [`PROMOTE_MIN_ENTRIES`]. That is amortised O(1) per insert -- checking
    /// on every insert would make loading a column O(n^2) -- and a column that
    /// is going to be dense becomes dense early in a load rather than at the
    /// end of one.
    fn maybe_promote(&mut self) {
        let ColumnData::Sparse(m) = self else { return };
        let len = m.len();
        if len < PROMOTE_MIN_ENTRIES || !len.is_power_of_two() {
            return;
        }
        let (Some(&min), Some(&max)) = (m.keys().min(), m.keys().max()) else {
            return;
        };
        let span = max - min + 1;
        if !dense_is_smaller(span, len, std::mem::size_of::<T>()) {
            return;
        }
        let mut values = vec![T::default(); span];
        let mut present = vec![0u64; span.div_ceil(64)];
        for (&idx, value) in m.iter() {
            let slot = idx - min;
            values[slot] = value.clone();
            set_bit(&mut present, slot);
        }
        *self = ColumnData::Dense { base: min, values, present, count: len };
    }
}

/// Below this, the map is small enough that the representation does not
/// matter and the scan to decide would cost more than it saves.
const PROMOTE_MIN_ENTRIES: usize = 1024;

#[inline]
fn bit(words: &[u64], slot: usize) -> bool {
    words
        .get(slot / 64)
        .is_some_and(|w| w & (1u64 << (slot % 64)) != 0)
}

#[inline]
fn set_bit(words: &mut [u64], slot: usize) {
    if let Some(w) = words.get_mut(slot / 64) {
        *w |= 1u64 << (slot % 64);
    }
}

#[inline]
fn clear_bit(words: &mut [u64], slot: usize) {
    if let Some(w) = words.get_mut(slot / 64) {
        *w &= !(1u64 << (slot % 64));
    }
}

/// A single property column. Only rows that are explicitly set are readable;
/// everything else reads as [`PropertyValue::Null`].
///
/// The Null distinction is load-bearing rather than cosmetic: `resolve_property`
/// treats a Null from this store as "not here, try row storage", so a dense
/// column returning `T::default()` for an absent slot would turn a missing
/// property into `0` or `""` -- a wrong answer rather than a slow one.
#[derive(Debug, Clone)]
pub enum Column {
    Int(ColumnData<i64>),
    Float(ColumnData<f64>),
    String(ColumnData<String>),
    Bool(ColumnData<bool>),
    /// Anything the typed columns cannot hold, kept as whole values.
    ///
    /// Two things land here:
    ///
    /// * **variants with no typed column** — `DateTime`, `Array`, `Map`,
    ///   `Vector`, `Duration`. Before this they were dropped on the way in and
    ///   survived only because row storage kept a second copy of every
    ///   property. That made the duplication load-bearing rather than
    ///   redundant, and blocked removing it (#545).
    /// * **a property whose type is not consistent across rows** — one node
    ///   with `score: 5` and another with `score: "high"`. The typed column
    ///   silently discarded the mismatch; now the column promotes and keeps
    ///   both.
    ///
    /// Deliberately a plain map rather than a `ColumnData`: these are the rare
    /// and irregular values, a dense array of `PropertyValue` would be 56
    /// bytes a slot, and nothing scans them.
    Other(FxHashMap<usize, PropertyValue>),
}

impl Column {
    pub fn new_int() -> Self { Column::Int(ColumnData::new()) }
    pub fn new_float() -> Self { Column::Float(ColumnData::new()) }
    pub fn new_string() -> Self { Column::String(ColumnData::new()) }
    pub fn new_bool() -> Self { Column::Bool(ColumnData::new()) }

    /// A column that can hold every `PropertyValue`, chosen from the first
    /// value written to it.
    pub fn for_value(value: &PropertyValue) -> Self {
        match value {
            PropertyValue::Integer(_) => Column::new_int(),
            PropertyValue::Float(_) => Column::new_float(),
            PropertyValue::String(_) => Column::new_string(),
            PropertyValue::Boolean(_) => Column::new_bool(),
            _ => Column::Other(FxHashMap::default()),
        }
    }

    pub fn set(&mut self, idx: usize, value: PropertyValue) {
        match (&mut *self, value) {
            (Column::Int(m), PropertyValue::Integer(val)) => m.set(idx, val),
            (Column::Float(m), PropertyValue::Float(val)) => m.set(idx, val),
            (Column::String(m), PropertyValue::String(val)) => m.set(idx, val),
            (Column::Bool(m), PropertyValue::Boolean(val)) => m.set(idx, val),
            (Column::Other(m), value) => {
                m.insert(idx, value);
            }
            // A value the typed column cannot hold. Promote the whole column
            // rather than drop it: silently discarding here is what made row
            // storage load-bearing, because the value was only readable from
            // the copy kept there (#545).
            (_, value) => {
                self.promote_to_other();
                if let Column::Other(m) = self {
                    m.insert(idx, value);
                }
            }
        }
    }

    /// Move every value into an untyped map, preserving what is already here.
    fn promote_to_other(&mut self) {
        if matches!(self, Column::Other(_)) {
            return;
        }
        let mut spilled: FxHashMap<usize, PropertyValue> = FxHashMap::default();
        match self {
            Column::Int(m) => m.for_each(|idx, v| { spilled.insert(idx, PropertyValue::Integer(*v)); }),
            Column::Float(m) => m.for_each(|idx, v| { spilled.insert(idx, PropertyValue::Float(*v)); }),
            Column::Bool(m) => m.for_each(|idx, v| { spilled.insert(idx, PropertyValue::Boolean(*v)); }),
            Column::String(m) => m.for_each(|idx, v| { spilled.insert(idx, PropertyValue::String(v.clone())); }),
            Column::Other(_) => unreachable!("checked above"),
        }
        *self = Column::Other(spilled);
    }

    /// Remove this row's value from the column, if present.
    pub fn remove(&mut self, idx: usize) {
        match self {
            Column::Int(m) => m.remove(idx),
            Column::Float(m) => m.remove(idx),
            Column::String(m) => m.remove(idx),
            Column::Bool(m) => m.remove(idx),
            Column::Other(m) => {
                m.remove(&idx);
            }
        }
    }

    pub fn get(&self, idx: usize) -> PropertyValue {
        match self {
            Column::Int(m) => m.get(idx).map(|&v| PropertyValue::Integer(v)).unwrap_or(PropertyValue::Null),
            Column::Float(m) => m.get(idx).map(|&v| PropertyValue::Float(v)).unwrap_or(PropertyValue::Null),
            Column::Bool(m) => m.get(idx).map(|&v| PropertyValue::Boolean(v)).unwrap_or(PropertyValue::Null),
            Column::String(m) => m.get(idx).map(|s| PropertyValue::String(s.clone())).unwrap_or(PropertyValue::Null),
            Column::Other(m) => m.get(&idx).cloned().unwrap_or(PropertyValue::Null),
        }
    }

    /// Check if a value exists at the given index.
    pub fn has(&self, idx: usize) -> bool {
        match self {
            Column::Int(m) => m.has(idx),
            Column::Float(m) => m.has(idx),
            Column::String(m) => m.has(idx),
            Column::Bool(m) => m.has(idx),
            Column::Other(m) => m.contains_key(&idx),
        }
    }

    /// Number of entries in this column.
    pub fn len(&self) -> usize {
        match self {
            Column::Int(m) => m.len(),
            Column::Float(m) => m.len(),
            Column::String(m) => m.len(),
            Column::Bool(m) => m.len(),
            Column::Other(m) => m.len(),
        }
    }

    /// Whether this column is stored densely. Diagnostics and tests only --
    /// no caller should behave differently based on it.
    pub fn is_dense(&self) -> bool {
        match self {
            Column::Int(m) => matches!(m, ColumnData::Dense { .. }),
            Column::Float(m) => matches!(m, ColumnData::Dense { .. }),
            Column::String(m) => matches!(m, ColumnData::Dense { .. }),
            Column::Bool(m) => matches!(m, ColumnData::Dense { .. }),
            // Irregular values are never worth a dense array.
            Column::Other(_) => false,
        }
    }
}

/// Manages multiple property columns.
#[derive(Debug, Default, Clone)]
pub struct ColumnStore {
    /// Mapping from property key -> Column.
    ///
    /// Also `FxHashMap`: property names are short, the set of them is small
    /// and fixed after load, and this lookup happens on every property read.
    columns: FxHashMap<String, Column>,
}

impl ColumnStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_property(&mut self, idx: usize, key: &str, value: PropertyValue) {
        if let Some(col) = self.columns.get_mut(key) {
            col.set(idx, value);
        } else {
            // Every `PropertyValue` gets a column. Returning early for the
            // ones without a typed representation is what made row storage
            // load-bearing: the value was readable only from the copy kept
            // there, so the duplication could not be removed (#545).
            let mut col = Column::for_value(&value);
            col.set(idx, value);
            self.columns.insert(key.to_string(), col);
        }
    }

    /// Drop every property stored for one row.
    ///
    /// Node ids are recycled through a free list, so a deleted node's slot is handed to the
    /// next `create_node`. Without this the new node inherits whatever the previous
    /// occupant left in each column — values from deleted data reappearing on new data
    /// (#364). Deletion has to clear the columns as well as the sparse map.
    pub fn clear_row(&mut self, idx: usize) {
        for col in self.columns.values_mut() {
            col.remove(idx);
        }
    }

    pub fn get_property(&self, idx: usize, key: &str) -> PropertyValue {
        self.columns.get(key).map(|col| col.get(idx)).unwrap_or(PropertyValue::Null)
    }

    /// Optimized batch read for a single property
    pub fn get_column(&self, key: &str) -> Option<&Column> {
        self.columns.get(key)
    }

    /// Get all property keys that have a non-null value for a given node index.
    /// Used by `keys()` function to discover column-store-only properties.
    pub fn get_property_keys(&self, idx: usize) -> Vec<String> {
        self.columns.iter()
            .filter(|(_, col)| col.has(idx))
            .map(|(key, _)| key.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Enough consecutive rows to cross `PROMOTE_MIN_ENTRIES` at a power of
    /// two, which is when promotion is considered.
    const DENSE_N: usize = 2048;

    fn dense_int_column(base: usize) -> Column {
        let mut col = Column::new_int();
        for i in 0..DENSE_N {
            col.set(base + i, PropertyValue::Integer(i as i64));
        }
        col
    }

    #[test]
    fn break_even_matches_the_documented_fill_factors() {
        // The table in `dense_is_smaller`'s docs is the contract. If these
        // move, the doc is wrong and a footprint claim built on it is too.
        let span = 100_000;

        // i64: break-even ~0.42
        assert!(!dense_is_smaller(span, (span as f64 * 0.35) as usize, 8));
        assert!(dense_is_smaller(span, (span as f64 * 0.50) as usize, 8));

        // String header (24 B): break-even ~0.64
        assert!(!dense_is_smaller(span, (span as f64 * 0.55) as usize, 24));
        assert!(dense_is_smaller(span, (span as f64 * 0.75) as usize, 24));

        // bool: break-even ~0.10, so a fairly sparse bool column still wins
        assert!(dense_is_smaller(span, (span as f64 * 0.20) as usize, 1));
        assert!(!dense_is_smaller(span, (span as f64 * 0.05) as usize, 1));
    }

    #[test]
    fn break_even_is_never_true_for_an_empty_or_degenerate_column() {
        assert!(!dense_is_smaller(0, 0, 8));
        assert!(!dense_is_smaller(1000, 0, 8));
        assert!(!dense_is_smaller(0, 1000, 8));
    }

    #[test]
    fn break_even_does_not_overflow_on_an_absurd_span() {
        // One property written at a very high index. The arithmetic must not
        // wrap into "dense is smaller" and allocate the universe.
        assert!(!dense_is_smaller(usize::MAX, 1, 8));
        assert!(!dense_is_smaller(usize::MAX / 2, 1000, 24));
    }

    #[test]
    fn a_contiguous_run_becomes_dense_and_reads_back_identically() {
        let col = dense_int_column(0);
        assert!(col.is_dense(), "a fully packed run should be stored densely");
        assert_eq!(col.len(), DENSE_N);
        for i in 0..DENSE_N {
            assert_eq!(col.get(i), PropertyValue::Integer(i as i64), "row {i}");
        }
    }

    #[test]
    fn a_dense_column_offset_from_zero_keeps_its_base() {
        // `Post.content` in LDBC starts above index 1.1M. A dense array that
        // allocated from zero would waste the whole prefix.
        let base = 1_100_000;
        let col = dense_int_column(base);
        assert!(col.is_dense());
        assert_eq!(col.get(base), PropertyValue::Integer(0));
        assert_eq!(col.get(base + DENSE_N - 1), PropertyValue::Integer(DENSE_N as i64 - 1));
        assert_eq!(col.get(0), PropertyValue::Null, "nothing below the base");
        assert_eq!(col.get(base - 1), PropertyValue::Null);
        assert_eq!(col.get(base + DENSE_N), PropertyValue::Null);
    }

    #[test]
    fn an_absent_slot_in_a_dense_column_reads_null_not_a_default() {
        // The distinction `resolve_property` depends on: a Null means "not
        // here, try row storage". Returning `T::default()` would turn a
        // missing property into 0 or "" -- a wrong answer, not a slow one.
        let mut col = Column::new_int();
        for i in 0..DENSE_N {
            if i % 3 != 1 {
                col.set(i, PropertyValue::Integer(i as i64));
            }
        }
        for i in 0..DENSE_N {
            if i % 3 == 1 {
                assert_eq!(col.get(i), PropertyValue::Null, "row {i} was never set");
            } else {
                assert_eq!(col.get(i), PropertyValue::Integer(i as i64), "row {i}");
            }
        }

        let mut strings = Column::new_string();
        for i in 0..DENSE_N {
            if i % 2 == 0 {
                strings.set(i, PropertyValue::String(format!("v{i}")));
            }
        }
        assert_eq!(strings.get(1), PropertyValue::Null, "not an empty string");
        assert_eq!(strings.get(0), PropertyValue::String("v0".to_string()));
    }

    #[test]
    fn a_zero_value_is_still_present() {
        // The inverse of the test above: a stored 0 must not read as absent.
        let mut col = Column::new_int();
        for i in 0..DENSE_N {
            col.set(i, PropertyValue::Integer(0));
        }
        assert!(col.is_dense());
        assert_eq!(col.get(5), PropertyValue::Integer(0));
        assert!(col.has(5));
        assert_eq!(col.len(), DENSE_N);
    }

    #[test]
    fn a_sparse_column_stays_sparse() {
        // One property on one node in fifty must not allocate a slot per node.
        let mut col = Column::new_int();
        for i in 0..DENSE_N {
            col.set(i * 50, PropertyValue::Integer(i as i64));
        }
        assert!(!col.is_dense(), "a 1-in-50 column should not go dense");
        assert_eq!(col.len(), DENSE_N);
        assert_eq!(col.get(50), PropertyValue::Integer(1));
        assert_eq!(col.get(51), PropertyValue::Null);
    }

    #[test]
    fn one_far_away_row_demotes_rather_than_allocating_the_span() {
        // The pathology the growth rule exists to prevent.
        let mut col = dense_int_column(0);
        assert!(col.is_dense());
        col.set(50_000_000, PropertyValue::Integer(7));
        assert!(!col.is_dense(), "extending to 50M slots for one row is the wrong trade");
        // And nothing was lost on the way out.
        assert_eq!(col.get(50_000_000), PropertyValue::Integer(7));
        assert_eq!(col.get(0), PropertyValue::Integer(0));
        assert_eq!(col.get(DENSE_N - 1), PropertyValue::Integer(DENSE_N as i64 - 1));
        assert_eq!(col.len(), DENSE_N + 1);
    }

    #[test]
    fn a_dense_column_grows_upward_when_the_span_still_pays() {
        let mut col = dense_int_column(0);
        col.set(DENSE_N, PropertyValue::Integer(-1));
        assert!(col.is_dense(), "one row past the end is still dense");
        assert_eq!(col.get(DENSE_N), PropertyValue::Integer(-1));
        assert_eq!(col.get(0), PropertyValue::Integer(0));
        assert_eq!(col.len(), DENSE_N + 1);
    }

    #[test]
    fn a_dense_column_grows_downward_below_its_base() {
        let base = 10_000;
        let mut col = dense_int_column(base);
        col.set(base - 1, PropertyValue::Integer(-1));
        assert!(col.is_dense());
        assert_eq!(col.get(base - 1), PropertyValue::Integer(-1));
        assert_eq!(col.get(base), PropertyValue::Integer(0), "the old base survived the shift");
        assert_eq!(col.get(base + DENSE_N - 1), PropertyValue::Integer(DENSE_N as i64 - 1));
        assert_eq!(col.get(base - 2), PropertyValue::Null);
        assert_eq!(col.len(), DENSE_N + 1);
    }

    #[test]
    fn removing_from_a_dense_column_clears_the_row() {
        // Node ids are recycled through a free list, so a deleted node's slot
        // goes to the next create_node. A value left behind reappears on new
        // data (#364).
        let mut col = dense_int_column(0);
        col.remove(10);
        assert_eq!(col.get(10), PropertyValue::Null);
        assert!(!col.has(10));
        assert_eq!(col.len(), DENSE_N - 1);
        assert_eq!(col.get(9), PropertyValue::Integer(9), "neighbours untouched");
        assert_eq!(col.get(11), PropertyValue::Integer(11));

        // And the slot is reusable.
        col.set(10, PropertyValue::Integer(999));
        assert_eq!(col.get(10), PropertyValue::Integer(999));
        assert_eq!(col.len(), DENSE_N);
    }

    #[test]
    fn removing_outside_the_span_is_a_no_op() {
        let mut col = dense_int_column(1000);
        col.remove(0);
        col.remove(usize::MAX);
        assert_eq!(col.len(), DENSE_N);
    }

    #[test]
    fn overwriting_a_row_does_not_double_count_it() {
        let mut col = dense_int_column(0);
        col.set(5, PropertyValue::Integer(42));
        assert_eq!(col.get(5), PropertyValue::Integer(42));
        assert_eq!(col.len(), DENSE_N);
    }

    #[test]
    fn every_column_type_round_trips_when_dense() {
        let mut floats = Column::new_float();
        let mut strings = Column::new_string();
        let mut bools = Column::new_bool();
        for i in 0..DENSE_N {
            floats.set(i, PropertyValue::Float(i as f64 * 0.25));
            strings.set(i, PropertyValue::String(format!("s{i}")));
            bools.set(i, PropertyValue::Boolean(i % 2 == 0));
        }
        assert!(floats.is_dense() && strings.is_dense() && bools.is_dense());
        assert_eq!(floats.get(7), PropertyValue::Float(1.75));
        assert_eq!(strings.get(7), PropertyValue::String("s7".to_string()));
        assert_eq!(bools.get(7), PropertyValue::Boolean(false));
        assert_eq!(bools.get(8), PropertyValue::Boolean(true));
        assert_eq!(floats.get(DENSE_N), PropertyValue::Null);
    }

    #[test]
    fn a_type_mismatch_promotes_the_column_instead_of_dropping_the_value() {
        // This used to drop the mismatched value on the floor. It survived
        // only because row storage kept a second copy of every property, which
        // is what made that duplication impossible to remove (#545).
        let mut col = Column::new_int();
        col.set(0, PropertyValue::Integer(1));
        col.set(1, PropertyValue::String("high".to_string()));

        assert_eq!(col.get(1), PropertyValue::String("high".to_string()));
        assert_eq!(col.get(0), PropertyValue::Integer(1), "the earlier value survived promotion");
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn promotion_carries_a_dense_column_across_intact() {
        // The promotion path has to read whichever representation the column
        // was using. A dense column's values live in an array behind a
        // presence bitmap, not in a map.
        let mut col = dense_int_column(0);
        assert!(col.is_dense());
        col.set(DENSE_N + 1, PropertyValue::String("odd one out".to_string()));

        assert!(!col.is_dense(), "an untyped column is never dense");
        assert_eq!(col.len(), DENSE_N + 1);
        assert_eq!(col.get(0), PropertyValue::Integer(0));
        assert_eq!(col.get(DENSE_N - 1), PropertyValue::Integer(DENSE_N as i64 - 1));
        assert_eq!(col.get(DENSE_N + 1), PropertyValue::String("odd one out".to_string()));
        assert_eq!(col.get(DENSE_N), PropertyValue::Null, "the gap is still a gap");
    }

    #[test]
    fn every_property_value_variant_round_trips() {
        // The point of the spill column. Before it, five of these were
        // dropped on the way in and readable only from row storage.
        let cases = vec![
            PropertyValue::Integer(7),
            PropertyValue::Float(1.5),
            PropertyValue::String("s".to_string()),
            PropertyValue::Boolean(true),
            PropertyValue::DateTime(1_700_000_000_000),
            PropertyValue::Array(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]),
            PropertyValue::Vector(vec![0.5, 0.25]),
            PropertyValue::Duration { months: 1, days: 2, seconds: 3, nanos: 4 },
        ];
        for value in cases {
            let mut store = ColumnStore::new();
            store.set_property(3, "p", value.clone());
            assert_eq!(store.get_property(3, "p"), value, "round trip failed for {value:?}");
            assert_eq!(store.get_property(4, "p"), PropertyValue::Null);
        }

        let mut store = ColumnStore::new();
        let mut map = std::collections::HashMap::new();
        map.insert("k".to_string(), PropertyValue::Integer(1));
        store.set_property(0, "m", PropertyValue::Map(map.clone()));
        assert_eq!(store.get_property(0, "m"), PropertyValue::Map(map));
    }

    #[test]
    fn an_untyped_column_still_clears_a_recycled_row() {
        // #364 applies to the spill column too: a deleted node's slot goes to
        // the next create_node.
        let mut store = ColumnStore::new();
        store.set_property(5, "tags", PropertyValue::Array(vec![PropertyValue::Integer(1)]));
        assert!(store.get_property(5, "tags") != PropertyValue::Null);
        store.clear_row(5);
        assert_eq!(store.get_property(5, "tags"), PropertyValue::Null);
    }

    #[test]
    fn get_property_keys_sees_untyped_columns() {
        let mut store = ColumnStore::new();
        store.set_property(1, "when", PropertyValue::DateTime(1));
        store.set_property(1, "n", PropertyValue::Integer(2));
        let mut keys = store.get_property_keys(1);
        keys.sort();
        assert_eq!(keys, vec!["n".to_string(), "when".to_string()]);
    }

    #[test]
    fn test_sparse_column_string() {
        let mut col = Column::new_string();
        // Set at index 1_000_000 — should NOT allocate 1M entries
        col.set(1_000_000, PropertyValue::String("hello".to_string()));
        assert_eq!(col.get(1_000_000), PropertyValue::String("hello".to_string()));
        assert_eq!(col.get(0), PropertyValue::Null);
        assert_eq!(col.get(999_999), PropertyValue::Null);
        assert_eq!(col.len(), 1);
    }

    #[test]
    fn test_sparse_column_int() {
        let mut col = Column::new_int();
        col.set(42, PropertyValue::Integer(100));
        col.set(99, PropertyValue::Integer(200));
        assert_eq!(col.get(42), PropertyValue::Integer(100));
        assert_eq!(col.get(99), PropertyValue::Integer(200));
        assert_eq!(col.get(50), PropertyValue::Null);
        assert_eq!(col.len(), 2);
    }

    #[test]
    fn test_column_store_sparse() {
        let mut store = ColumnStore::new();
        // Set property at high index — should not waste memory
        store.set_property(10_000_000, "name", PropertyValue::String("test".to_string()));
        assert_eq!(
            store.get_property(10_000_000, "name"),
            PropertyValue::String("test".to_string())
        );
        assert_eq!(store.get_property(0, "name"), PropertyValue::Null);
        assert_eq!(store.get_property(10_000_000, "other"), PropertyValue::Null);
    }

    #[test]
    fn test_get_property_keys() {
        let mut store = ColumnStore::new();
        store.set_property(5, "name", PropertyValue::String("Alice".to_string()));
        store.set_property(5, "age", PropertyValue::Integer(30));
        store.set_property(10, "name", PropertyValue::String("Bob".to_string()));

        let keys5 = store.get_property_keys(5);
        assert!(keys5.contains(&"name".to_string()));
        assert!(keys5.contains(&"age".to_string()));
        assert_eq!(keys5.len(), 2);

        let keys10 = store.get_property_keys(10);
        assert_eq!(keys10, vec!["name".to_string()]);

        let keys99 = store.get_property_keys(99);
        assert!(keys99.is_empty());
    }
}
