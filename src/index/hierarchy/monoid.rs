//! Monoids and the range structures that answer a roll-up from the index.
//!
//! A roll-up folds a per-node measure over `{y} ∪ descendants(y)` with a commutative
//! monoid. Which range structure can answer that fold depends on whether the monoid has an
//! **inverse**:
//!
//! | Op | Invertible | Structure | Query |
//! |---|---|---|---|
//! | `SUM`, `COUNT` | yes | [`Fenwick`] | O(log n) — `prefix(hi+1) − prefix(lo)` |
//! | `MIN`, `MAX` | no | [`SparseTable`] | O(1) — overlapping power-of-two blocks |
//!
//! Using a Fenwick difference for MIN/MAX would be silently wrong: there is no value to
//! subtract. The sparse table costs O(n log n) build and space; we pay it because the
//! engine calls roll-up in a loop (top-k over subtrees), where an O(subtree) scan per
//! candidate would put the O(subtree) cost we just removed straight back in.

use std::fmt;

/// A numeric measure carried through a roll-up.
///
/// Integers accumulate in `i128` rather than `i64` so that a sum over a million-node
/// subtree cannot silently wrap. Exactness is the headline correctness property of this
/// index — the reference implementation matches TimescaleDB continuous aggregates to the
/// unit — so an accumulator that can overflow is not acceptable.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RollupValue {
    /// Exact integer accumulation.
    Int(i128),
    /// Floating-point accumulation.
    Float(f64),
    /// The fold saw no contributing node (identity for MIN/MAX over an empty set).
    Null,
}

impl fmt::Display for RollupValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RollupValue::Int(v) => write!(f, "{v}"),
            RollupValue::Float(v) => write!(f, "{v}"),
            RollupValue::Null => write!(f, "NULL"),
        }
    }
}

impl RollupValue {
    /// Numeric view for comparison and float-domain arithmetic.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            RollupValue::Int(v) => Some(*v as f64),
            RollupValue::Float(v) => Some(*v),
            RollupValue::Null => None,
        }
    }
}

/// The supported roll-up monoids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RollupOp {
    /// Sum of the measure.
    Sum,
    /// Count of nodes carrying a measure (measure pre-mapped to 1).
    Count,
    /// Minimum measure.
    Min,
    /// Maximum measure.
    Max,
}

impl RollupOp {
    /// Parse from Cypher surface syntax (`sum`, `count`, `min`, `max`).
    pub fn parse(s: &str) -> Option<RollupOp> {
        match s.to_ascii_lowercase().as_str() {
            "sum" => Some(RollupOp::Sum),
            "count" => Some(RollupOp::Count),
            "min" => Some(RollupOp::Min),
            "max" => Some(RollupOp::Max),
            _ => None,
        }
    }

    /// Surface name.
    pub fn name(&self) -> &'static str {
        match self {
            RollupOp::Sum => "sum",
            RollupOp::Count => "count",
            RollupOp::Min => "min",
            RollupOp::Max => "max",
        }
    }

    /// Does this monoid have an inverse? Only invertible monoids may use a Fenwick
    /// prefix-difference to answer a range fold.
    pub fn is_invertible(&self) -> bool {
        matches!(self, RollupOp::Sum | RollupOp::Count)
    }

    /// The monoid identity.
    pub fn identity(&self) -> RollupValue {
        match self {
            RollupOp::Sum | RollupOp::Count => RollupValue::Int(0),
            RollupOp::Min | RollupOp::Max => RollupValue::Null,
        }
    }

    /// Fold two values.
    ///
    /// Integer inputs stay in the integer domain (exact); a float on either side promotes
    /// the fold to `f64`. `Null` is the identity for MIN/MAX and absorbing-free for SUM.
    pub fn combine(&self, a: RollupValue, b: RollupValue) -> RollupValue {
        use RollupValue::*;
        match (self, a, b) {
            (_, Null, x) => x,
            (_, x, Null) => x,
            (RollupOp::Sum | RollupOp::Count, Int(x), Int(y)) => Int(x + y),
            (RollupOp::Sum | RollupOp::Count, x, y) => {
                Float(x.as_f64().unwrap_or(0.0) + y.as_f64().unwrap_or(0.0))
            }
            (RollupOp::Min, Int(x), Int(y)) => Int(x.min(y)),
            (RollupOp::Max, Int(x), Int(y)) => Int(x.max(y)),
            (RollupOp::Min, x, y) => {
                let (xf, yf) = (x.as_f64().unwrap_or(0.0), y.as_f64().unwrap_or(0.0));
                if xf <= yf {
                    x
                } else {
                    y
                }
            }
            (RollupOp::Max, x, y) => {
                let (xf, yf) = (x.as_f64().unwrap_or(0.0), y.as_f64().unwrap_or(0.0));
                if xf >= yf {
                    x
                } else {
                    y
                }
            }
        }
    }
}

/// Fenwick (binary indexed) tree over `i128` and `f64` measures.
///
/// Only valid for invertible monoids: a range fold is answered by differencing two
/// prefixes. Build is O(n), query O(log n), space one accumulator per position.
#[derive(Debug, Clone)]
pub enum Fenwick {
    /// Exact integer accumulation.
    Int(Vec<i128>),
    /// Float accumulation.
    Float(Vec<f64>),
}

impl Fenwick {
    /// Build from per-position measures (position = in-rank).
    pub fn build(values: &[RollupValue]) -> Fenwick {
        let any_float = values.iter().any(|v| matches!(v, RollupValue::Float(_)));
        if any_float {
            let mut tree = vec![0.0f64; values.len() + 1];
            for (i, v) in values.iter().enumerate() {
                let delta = v.as_f64().unwrap_or(0.0);
                let mut j = i + 1;
                while j <= values.len() {
                    tree[j] += delta;
                    j += j & j.wrapping_neg();
                }
            }
            Fenwick::Float(tree)
        } else {
            let mut tree = vec![0i128; values.len() + 1];
            for (i, v) in values.iter().enumerate() {
                let delta = match v {
                    RollupValue::Int(x) => *x,
                    _ => 0,
                };
                let mut j = i + 1;
                while j <= values.len() {
                    tree[j] += delta;
                    j += j & j.wrapping_neg();
                }
            }
            Fenwick::Int(tree)
        }
    }

    fn len(&self) -> usize {
        match self {
            Fenwick::Int(t) => t.len() - 1,
            Fenwick::Float(t) => t.len() - 1,
        }
    }

    /// Add `delta` at `pos`, in O(log n).
    ///
    /// A Fenwick tree updates in place because SUM has an inverse — the caller supplies the
    /// difference and every covering node absorbs it. This is what lets a measure write
    /// avoid invalidating the whole index (#351).
    ///
    /// A Float delta promotes an Int tree to a Float one first, as `build` does when any
    /// measure is a Float; casting the delta instead would drop its fraction (#1568).
    /// Every node holds a sum of measures, so converting node by node keeps the tree valid.
    pub fn add(&mut self, pos: usize, delta: RollupValue) {
        if let (Fenwick::Int(t), RollupValue::Float(_)) = (&*self, &delta) {
            *self = Fenwick::Float(t.iter().map(|&x| x as f64).collect());
        }
        match self {
            Fenwick::Int(t) => {
                let d = match delta {
                    RollupValue::Int(v) => v,
                    RollupValue::Float(_) => unreachable!("promoted to a Float tree above"),
                    RollupValue::Null => return,
                };
                let n = t.len() - 1;
                let mut j = pos + 1;
                while j <= n {
                    t[j] += d;
                    j += j & j.wrapping_neg();
                }
            }
            Fenwick::Float(t) => {
                let d = delta.as_f64().unwrap_or(0.0);
                let n = t.len() - 1;
                let mut j = pos + 1;
                while j <= n {
                    t[j] += d;
                    j += j & j.wrapping_neg();
                }
            }
        }
    }

    /// Sum of positions `[0, i)`.
    fn prefix(&self, i: usize) -> RollupValue {
        let mut i = i.min(self.len());
        match self {
            Fenwick::Int(t) => {
                let mut acc: i128 = 0;
                while i > 0 {
                    acc += t[i];
                    i -= i & i.wrapping_neg();
                }
                RollupValue::Int(acc)
            }
            Fenwick::Float(t) => {
                let mut acc = 0.0f64;
                while i > 0 {
                    acc += t[i];
                    i -= i & i.wrapping_neg();
                }
                RollupValue::Float(acc)
            }
        }
    }

    /// Sum of the inclusive position range `[lo, hi]`.
    pub fn range(&self, lo: usize, hi: usize) -> RollupValue {
        if hi < lo {
            return RollupValue::Int(0);
        }
        match (self.prefix(hi + 1), self.prefix(lo)) {
            (RollupValue::Int(a), RollupValue::Int(b)) => RollupValue::Int(a - b),
            (a, b) => RollupValue::Float(a.as_f64().unwrap_or(0.0) - b.as_f64().unwrap_or(0.0)),
        }
    }

    /// Approximate heap size in bytes, for index-size reporting.
    pub fn size_bytes(&self) -> usize {
        match self {
            Fenwick::Int(t) => t.len() * std::mem::size_of::<i128>(),
            Fenwick::Float(t) => t.len() * std::mem::size_of::<f64>(),
        }
    }
}

/// Segment tree over a non-invertible monoid (MIN / MAX).
///
/// A range fold is the combination of O(log n) disjoint canonical nodes. The alternative —
/// a sparse table of overlapping power-of-two blocks — answers in O(1) but costs
/// **O(n log n)** space, and that space dominated the index: on a 9,331-node ontology the
/// MIN and MAX tables came to 7.46 MB against 112 KB for the order embedding itself
/// (#352). A segment tree is O(n) — `2n` cells — trading O(1) for O(log n) on a query that
/// was already measured in tens of nanoseconds.
///
/// It also supports point update in O(log n), which a sparse table does not. Incremental
/// maintenance (#351) will need exactly that, so this is the structure that can grow into
/// it rather than being replaced again.
///
/// Disjointness is why this works for *any* commutative monoid, not just idempotent ones:
/// no element is folded twice, so MIN/MAX and (were it ever wanted) SUM are equally safe.
#[derive(Debug, Clone)]
pub struct SegmentTree {
    /// `tree[1]` is the root; leaves live at `[size, size + n)`.
    tree: Vec<RollupValue>,
    op: RollupOp,
    size: usize,
    n: usize,
}

impl SegmentTree {
    /// Build for `op` over per-position measures.
    pub fn build(values: &[RollupValue], op: RollupOp) -> SegmentTree {
        let n = values.len();
        let size = n.max(1).next_power_of_two();
        let mut tree = vec![op.identity(); 2 * size];
        tree[size..size + n].clone_from_slice(values);
        for i in (1..size).rev() {
            tree[i] = op.combine(tree[2 * i], tree[2 * i + 1]);
        }
        SegmentTree { tree, op, size, n }
    }

    /// Fold the inclusive position range `[lo, hi]`.
    pub fn range(&self, lo: usize, hi: usize) -> RollupValue {
        if hi < lo || lo >= self.n {
            return self.op.identity();
        }
        let mut acc = self.op.identity();
        let mut l = lo + self.size;
        let mut r = hi.min(self.n - 1) + self.size + 1;
        while l < r {
            if l & 1 == 1 {
                acc = self.op.combine(acc, self.tree[l]);
                l += 1;
            }
            if r & 1 == 1 {
                r -= 1;
                acc = self.op.combine(acc, self.tree[r]);
            }
            l >>= 1;
            r >>= 1;
        }
        acc
    }

    /// Set the value at `pos` and refold the path to the root, in O(log n).
    ///
    /// Unused by the static index today; present because it is what incremental
    /// maintenance (#351) needs and it costs nothing to expose.
    pub fn set(&mut self, pos: usize, value: RollupValue) {
        if pos >= self.n {
            return;
        }
        let mut i = pos + self.size;
        self.tree[i] = value;
        while i > 1 {
            i >>= 1;
            self.tree[i] = self.op.combine(self.tree[2 * i], self.tree[2 * i + 1]);
        }
    }

    /// Approximate heap size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.tree.len() * std::mem::size_of::<RollupValue>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ints(v: &[i128]) -> Vec<RollupValue> {
        v.iter().map(|&x| RollupValue::Int(x)).collect()
    }

    #[test]
    fn ops_report_invertibility_correctly() {
        assert!(RollupOp::Sum.is_invertible());
        assert!(RollupOp::Count.is_invertible());
        assert!(!RollupOp::Min.is_invertible());
        assert!(!RollupOp::Max.is_invertible());
    }

    #[test]
    fn fenwick_matches_a_naive_range_sum() {
        let vals = ints(&[3, 1, 4, 1, 5, 9, 2, 6, 5, 3]);
        let f = Fenwick::build(&vals);
        for lo in 0..vals.len() {
            for hi in lo..vals.len() {
                let naive: i128 = vals[lo..=hi]
                    .iter()
                    .map(|v| match v {
                        RollupValue::Int(x) => *x,
                        _ => 0,
                    })
                    .sum();
                assert_eq!(
                    f.range(lo, hi),
                    RollupValue::Int(naive),
                    "range {lo}..={hi}"
                );
            }
        }
    }

    #[test]
    fn fenwick_sum_is_exact_beyond_i64() {
        // A per-node measure near i64::MAX summed over three nodes overflows i64 but not
        // the i128 accumulator. Exactness is the property being defended here.
        let big = i64::MAX as i128;
        let vals = ints(&[big, big, big]);
        let f = Fenwick::build(&vals);
        assert_eq!(f.range(0, 2), RollupValue::Int(big * 3));
    }

    #[test]
    fn fenwick_handles_floats() {
        let vals = vec![
            RollupValue::Float(1.5),
            RollupValue::Float(2.25),
            RollupValue::Float(0.25),
        ];
        let f = Fenwick::build(&vals);
        match f.range(0, 2) {
            RollupValue::Float(v) => assert!((v - 4.0).abs() < 1e-9),
            other => panic!("expected float, got {other:?}"),
        }
    }

    #[test]
    fn segment_tree_matches_naive_min_and_max() {
        let vals = ints(&[3, 1, 4, 1, 5, 9, 2, 6]);
        for op in [RollupOp::Min, RollupOp::Max] {
            let st = SegmentTree::build(&vals, op);
            for lo in 0..vals.len() {
                for hi in lo..vals.len() {
                    let mut naive = op.identity();
                    for v in &vals[lo..=hi] {
                        naive = op.combine(naive, *v);
                    }
                    assert_eq!(st.range(lo, hi), naive, "{op:?} over {lo}..={hi}");
                }
            }
        }
    }

    #[test]
    fn empty_range_returns_identity() {
        let vals = ints(&[1, 2, 3]);
        let st = SegmentTree::build(&vals, RollupOp::Min);
        assert_eq!(st.range(2, 1), RollupValue::Null);
        let f = Fenwick::build(&vals);
        assert_eq!(f.range(2, 1), RollupValue::Int(0));
    }

    #[test]
    fn null_is_the_identity_for_min_and_max() {
        assert_eq!(
            RollupOp::Min.combine(RollupValue::Null, RollupValue::Int(7)),
            RollupValue::Int(7)
        );
        assert_eq!(
            RollupOp::Max.combine(RollupValue::Int(7), RollupValue::Null),
            RollupValue::Int(7)
        );
    }

    #[test]
    fn single_element_table() {
        let vals = ints(&[42]);
        let st = SegmentTree::build(&vals, RollupOp::Max);
        assert_eq!(st.range(0, 0), RollupValue::Int(42));
    }

    #[test]
    fn segment_tree_matches_naive_on_a_non_power_of_two_length() {
        // The padding to a power of two must not leak identity elements into a range.
        let vals = ints(&[5, 3, 9, 1, 7]);
        for op in [RollupOp::Min, RollupOp::Max] {
            let st = SegmentTree::build(&vals, op);
            for lo in 0..vals.len() {
                for hi in lo..vals.len() {
                    let mut naive = op.identity();
                    for v in &vals[lo..=hi] {
                        naive = op.combine(naive, *v);
                    }
                    assert_eq!(st.range(lo, hi), naive, "{op:?} over {lo}..={hi}");
                }
            }
        }
    }

    #[test]
    fn segment_tree_point_update_refolds_the_path() {
        let vals = ints(&[5, 3, 9, 1, 7]);
        let mut st = SegmentTree::build(&vals, RollupOp::Min);
        assert_eq!(st.range(0, 4), RollupValue::Int(1));
        st.set(3, RollupValue::Int(100));
        assert_eq!(
            st.range(0, 4),
            RollupValue::Int(3),
            "the old minimum is gone"
        );
        st.set(0, RollupValue::Int(-2));
        assert_eq!(st.range(0, 4), RollupValue::Int(-2));
        assert_eq!(
            st.range(1, 2),
            RollupValue::Int(3),
            "untouched ranges are unaffected"
        );
    }

    #[test]
    fn segment_tree_is_linear_where_a_sparse_table_was_n_log_n() {
        // The point of #352: 2*next_power_of_two(n) cells rather than n*log2(n).
        let vals = ints(&vec![1i128; 4096]);
        let st = SegmentTree::build(&vals, RollupOp::Max);
        let cells = st.size_bytes() / std::mem::size_of::<RollupValue>();
        assert_eq!(cells, 8192, "2n for a power-of-two length");
        // a sparse table over the same input would hold ~n*log2(n) = 4096*13 = 53,248
        assert!(cells * 6 < 4096 * 13, "at least 6x smaller: {cells}");
    }

    #[test]
    fn rollup_value_display_and_numeric_view() {
        assert_eq!(RollupValue::Int(3).to_string(), "3");
        assert_eq!(RollupValue::Float(1.5).to_string(), "1.5");
        assert_eq!(RollupValue::Null.to_string(), "NULL");
        assert_eq!(RollupValue::Int(3).as_f64(), Some(3.0));
        assert_eq!(RollupValue::Float(2.5).as_f64(), Some(2.5));
        assert_eq!(RollupValue::Null.as_f64(), None);
    }

    #[test]
    fn op_names_parse_back() {
        for op in [RollupOp::Sum, RollupOp::Count, RollupOp::Min, RollupOp::Max] {
            assert_eq!(RollupOp::parse(op.name()), Some(op));
        }
        assert_eq!(RollupOp::parse("MAX"), Some(RollupOp::Max));
        assert_eq!(RollupOp::parse("avg"), None);
    }

    #[test]
    fn combine_promotes_mixed_domains() {
        use RollupValue::*;
        assert_eq!(RollupOp::Sum.combine(Int(1), Float(0.5)), Float(1.5));
        assert_eq!(RollupOp::Count.combine(Float(1.0), Int(2)), Float(3.0));
        assert_eq!(RollupOp::Min.combine(Float(2.0), Int(1)), Int(1));
        assert_eq!(RollupOp::Min.combine(Int(1), Float(2.0)), Int(1));
        assert_eq!(RollupOp::Max.combine(Float(2.5), Int(1)), Float(2.5));
        assert_eq!(RollupOp::Max.combine(Int(1), Float(2.5)), Float(2.5));
        assert_eq!(RollupOp::Max.combine(Null, Int(4)), Int(4));
        assert_eq!(RollupOp::Min.combine(Int(4), Null), Int(4));
    }

    #[test]
    fn fenwick_point_updates_in_both_domains() {
        use RollupValue::*;
        let mut f = Fenwick::build(&[Int(1), Null, Int(3)]);
        assert_eq!(f.range(0, 2), Int(4), "null contributes nothing");
        f.add(1, Int(5));
        assert_eq!(f.range(0, 2), Int(9));
        f.add(1, Null);
        assert_eq!(f.range(0, 2), Int(9), "a null delta is ignored");
        assert_eq!(f.range(2, 1), Int(0), "an inverted range is empty");
        assert_eq!(f.size_bytes(), 4 * std::mem::size_of::<i128>());

        let mut g = Fenwick::build(&[Float(0.5), Int(1)]);
        g.add(1, Int(2));
        assert_eq!(g.range(0, 1), Float(3.5));
        g.add(0, Null);
        assert_eq!(g.range(0, 0), Float(0.5));
        assert_eq!(g.size_bytes(), 3 * std::mem::size_of::<f64>());
    }

    #[test]
    fn segment_tree_set_and_out_of_range_queries() {
        use RollupValue::*;
        let mut t = SegmentTree::build(&ints(&[5, 3, 9]), RollupOp::Min);
        t.set(10, Int(0));
        assert_eq!(t.range(0, 2), Int(3), "an out-of-range set is ignored");
        t.set(1, Int(7));
        assert_eq!(t.range(0, 2), Int(5));
        assert_eq!(t.range(5, 6), Null, "a range past the end is the identity");
        assert_eq!(t.range(0, 99), Int(5), "the upper bound is clamped");
        assert!(t.size_bytes() >= 8 * std::mem::size_of::<RollupValue>() / 2);
    }
}
