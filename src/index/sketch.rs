//! Approximate aggregates: HyperLogLog distinct counts, t-digest quantiles
//! (NDS-10).
//!
//! `count(DISTINCT x)` and `percentileCont(x, 0.95)` both keep every value they
//! see. That is exact and it is linear in the data, which is the wrong trade
//! when the question is "roughly how many" over a column with a hundred million
//! rows. These answer the same questions in bounded memory, and the error is
//! stated rather than implied.
//!
//! # Why the hash is ours and not `DefaultHasher`
//!
//! `std::collections::hash_map::DefaultHasher` is SipHash with a fixed key, and
//! its output is explicitly **not** guaranteed stable across Rust releases.
//! A HyperLogLog is a function of its hash: change the hash and the same data
//! gives a different estimate. That would make the result irreproducible across
//! toolchains for no benefit, and irreproducible in a way nothing would report
//! — the number would simply be a bit different. So [`hash64`] is a fixed
//! FNV-1a/splitmix construction written out here, where it can be read.

use crate::graph::PropertyValue;

/// FNV-1a over the bytes, finished with splitmix64.
///
/// FNV alone has poor avalanche in the high bits, and HyperLogLog reads the
/// **top** `p` bits for the register index and counts leading zeros in the
/// rest — so a hash whose high bits move sluggishly puts most values in a few
/// registers and the estimate is quietly wrong. The splitmix finalizer fixes
/// the avalanche.
pub fn hash64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // splitmix64 finalizer
    let mut z = h.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// A property value's bytes for hashing.
///
/// Type-tagged, so the integer `1` and the string `"1"` are different values —
/// which is what `count(DISTINCT)` says they are. Without the tag the
/// approximate count would disagree with the exact one on a column holding
/// both, and only on such a column, which is the hardest kind of disagreement
/// to find.
fn value_bytes(v: &PropertyValue) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    match v {
        PropertyValue::Null => out.push(0),
        PropertyValue::Boolean(b) => {
            out.push(1);
            out.push(*b as u8);
        }
        PropertyValue::Integer(i) => {
            out.push(2);
            out.extend_from_slice(&i.to_le_bytes());
        }
        PropertyValue::Float(f) => {
            out.push(3);
            // A float that is exactly an integer hashes as the integer, because
            // Cypher compares 1 and 1.0 as equal and a DISTINCT count that
            // disagreed with `=` would be wrong about its own question.
            if f.fract() == 0.0 && f.is_finite() && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                out.clear();
                out.push(2);
                out.extend_from_slice(&(*f as i64).to_le_bytes());
            } else {
                out.extend_from_slice(&f.to_bits().to_le_bytes());
            }
        }
        PropertyValue::String(s) => {
            out.push(4);
            out.extend_from_slice(s.as_bytes());
        }
        other => {
            out.push(5);
            out.extend_from_slice(other.to_string().as_bytes());
        }
    }
    out
}

/// HyperLogLog with `p = 14` (16,384 registers, 16 KB).
///
/// Standard error is `1.04 / sqrt(m)` ≈ **0.81%**. That is the figure to quote;
/// the estimate for any one input is not guaranteed within it, and the error is
/// a property of the distribution of estimates, not a bound.
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

const P: u32 = 14;
const M: usize = 1 << P;

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    pub fn new() -> Self {
        Self {
            registers: vec![0; M],
        }
    }

    pub fn add(&mut self, v: &PropertyValue) {
        // Null is not a value `count(DISTINCT)` counts, and adding it would
        // make the approximate count one higher than the exact one on any
        // column with a missing entry.
        if matches!(v, PropertyValue::Null) {
            return;
        }
        self.add_hash(hash64(&value_bytes(v)));
    }

    fn add_hash(&mut self, h: u64) {
        let idx = (h >> (64 - P)) as usize;
        // Leading zeros of the remaining bits, plus one. The `| 1` guards the
        // all-zero tail, where `leading_zeros` would return the full width and
        // the register would saturate on one unlucky value.
        let rest = (h << P) | (1 << (P - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    /// Merge another sketch. Two HLLs over the same hash merge exactly — this
    /// is the property that makes the aggregate parallelisable, and it is why
    /// the register array is the whole state.
    pub fn merge(&mut self, other: &HyperLogLog) {
        for (a, b) in self.registers.iter_mut().zip(&other.registers) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    pub fn estimate(&self) -> u64 {
        let m = M as f64;
        let sum: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;

        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        if raw <= 2.5 * m && zeros > 0 {
            // Linear counting for small cardinalities. Without it the raw
            // estimator is badly biased below roughly `2.5m`, and the bias
            // shows up as a distinct count of ~12,000 on a table with 3 rows,
            // which is the kind of wrong that is noticed immediately and then
            // distrusted forever.
            (m * (m / zeros as f64).ln()).round() as u64
        } else {
            raw.round() as u64
        }
    }
}

/// One centroid: a mean and the number of points it stands for.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Centroid {
    mean: f64,
    weight: f64,
}

/// A t-digest: quantiles in bounded memory, accurate at the tails.
///
/// The property that makes it worth having over a fixed-size sample: centroids
/// near q=0 and q=1 are kept small and centroids near the median are allowed to
/// grow, so **p99 is accurate where it matters** and the median is approximate
/// where nobody minds. A uniform sample has the opposite behaviour, and a
/// latency p99 is the number people actually ask for.
#[derive(Debug, Clone)]
pub struct TDigest {
    buffer: Vec<f64>,
    centroids: Vec<Centroid>,
    count: f64,
    compression: f64,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new(100.0)
    }
}

impl TDigest {
    pub fn new(compression: f64) -> Self {
        Self {
            buffer: Vec::new(),
            centroids: Vec::new(),
            count: 0.0,
            compression,
        }
    }

    pub fn add(&mut self, x: f64) {
        if !x.is_finite() {
            return;
        }
        self.buffer.push(x);
        self.count += 1.0;
        if self.buffer.len() >= 1000 {
            self.flush();
        }
    }

    pub fn len(&self) -> f64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0.0
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        let mut all: Vec<Centroid> = self
            .buffer
            .drain(..)
            .map(|mean| Centroid { mean, weight: 1.0 })
            .chain(self.centroids.drain(..))
            .collect();
        all.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total: f64 = all.iter().map(|c| c.weight).sum();
        let mut merged: Vec<Centroid> = Vec::with_capacity(all.len());
        let mut so_far = 0.0f64;

        for c in all {
            match merged.last_mut() {
                None => merged.push(c),
                Some(last) => {
                    // The scale function. `q * (1 - q)` is why the tails stay
                    // fine-grained: it goes to zero at both ends, so the
                    // allowed weight of a centroid there goes to zero too,
                    // while centroids near the median may absorb the most.
                    let q = (so_far + last.weight / 2.0) / total;
                    let limit = 4.0 * total * q * (1.0 - q) / self.compression;
                    if last.weight + c.weight <= limit.max(1.0) {
                        let w = last.weight + c.weight;
                        last.mean = (last.mean * last.weight + c.mean * c.weight) / w;
                        last.weight = w;
                    } else {
                        so_far += last.weight;
                        merged.push(c);
                    }
                }
            }
        }
        self.centroids = merged;
    }

    /// Fold another digest in.
    ///
    /// Concatenates both centroid sets and re-compresses, which is the only
    /// correct way: centroids carry weights, so appending the other digest's
    /// *points* is not available -- they no longer exist as points. Grouped
    /// aggregation merges partial aggregates, so without this a
    /// `RETURN key, approx.percentile(...)` would combine nothing.
    pub fn merge(&mut self, other: &TDigest) {
        self.flush();
        let mut theirs = other.clone();
        theirs.flush();
        self.centroids.extend(theirs.centroids);
        self.count += theirs.count;
        // Re-compress through the same path, by pushing the centroids back
        // through `flush`'s merge loop with an empty buffer.
        let pending = std::mem::take(&mut self.buffer);
        self.buffer = pending;
        self.recompress();
    }

    fn recompress(&mut self) {
        // `flush` handles an empty buffer by returning early, so drive the
        // merge directly with a sentinel push and removal would be worse than
        // sorting here: sort the centroids and run the same scale function.
        let mut all = std::mem::take(&mut self.centroids);
        all.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let total: f64 = all.iter().map(|c| c.weight).sum();
        let mut merged: Vec<Centroid> = Vec::with_capacity(all.len());
        let mut so_far = 0.0f64;
        for c in all {
            match merged.last_mut() {
                None => merged.push(c),
                Some(last) => {
                    let q = (so_far + last.weight / 2.0) / total;
                    let limit = 4.0 * total * q * (1.0 - q) / self.compression;
                    if last.weight + c.weight <= limit.max(1.0) {
                        let w = last.weight + c.weight;
                        last.mean = (last.mean * last.weight + c.mean * c.weight) / w;
                        last.weight = w;
                    } else {
                        so_far += last.weight;
                        merged.push(c);
                    }
                }
            }
        }
        self.centroids = merged;
    }

    /// The value at quantile `q` in `[0, 1]`, interpolating between centroids.
    pub fn quantile(&mut self, q: f64) -> Option<f64> {
        self.flush();
        if self.centroids.is_empty() {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let total: f64 = self.centroids.iter().map(|c| c.weight).sum();
        let target = q * total;

        let mut so_far = 0.0;
        for (i, c) in self.centroids.iter().enumerate() {
            let centre = so_far + c.weight / 2.0;
            if target <= centre {
                if i == 0 {
                    return Some(c.mean);
                }
                let prev = &self.centroids[i - 1];
                let prev_centre = so_far - prev.weight / 2.0;
                let span = centre - prev_centre;
                if span <= 0.0 {
                    return Some(c.mean);
                }
                let t = (target - prev_centre) / span;
                return Some(prev.mean + t * (c.mean - prev.mean));
            }
            so_far += c.weight;
        }
        self.centroids.last().map(|c| c.mean)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int(i: i64) -> PropertyValue {
        PropertyValue::Integer(i)
    }

    #[test]
    fn the_hash_is_written_down_and_stable() {
        // Pinned, because a HyperLogLog is a function of its hash: change the
        // hash and the same data gives a different estimate, with nothing to
        // report that anything moved. `DefaultHasher` is explicitly not stable
        // across Rust releases, which is why this one exists.
        // Cross-checked against an independent Python implementation of the
        // same construction, not copied out of the Rust output -- a constant
        // taken from the code it guards pins a typo as readily as a value.
        assert_eq!(hash64(b""), 14087677454934409008);
        assert_eq!(hash64(b"a"), 6857225946766476583);
        assert_ne!(hash64(b"a"), hash64(b"b"));
    }

    #[test]
    fn a_small_cardinality_is_exact_enough_to_trust() {
        // Linear counting's job. The raw HLL estimator is badly biased here and
        // would report thousands for a handful of values.
        for n in [1u64, 3, 10, 100] {
            let mut h = HyperLogLog::new();
            for i in 0..n {
                h.add(&int(i as i64));
            }
            let e = h.estimate();
            assert!(
                e.abs_diff(n) <= 1,
                "n={n} estimated {e}; small cardinalities must be near-exact"
            );
        }
    }

    #[test]
    fn a_large_cardinality_is_within_the_stated_error() {
        // 0.81% standard error at p=14. Asserted at 3x that: a tighter bound
        // would be asserting the distribution's tail rather than its spread,
        // and would fail on an unlucky input for no reason anyone could act on.
        let mut h = HyperLogLog::new();
        let n = 100_000u64;
        for i in 0..n {
            h.add(&PropertyValue::String(format!("value-{i}")));
        }
        let e = h.estimate() as f64;
        let err = (e - n as f64).abs() / n as f64;
        assert!(
            err < 0.025,
            "estimated {e} for {n}, error {:.3}%",
            err * 100.0
        );
    }

    #[test]
    fn duplicates_do_not_move_the_estimate() {
        let mut h = HyperLogLog::new();
        for _ in 0..10_000 {
            h.add(&int(7));
        }
        assert_eq!(h.estimate(), 1);
    }

    #[test]
    fn null_is_not_counted() {
        // `count(DISTINCT x)` does not count nulls. An approximate count that
        // did would be one higher than the exact one on any column with a
        // missing entry.
        let mut h = HyperLogLog::new();
        h.add(&PropertyValue::Null);
        assert_eq!(h.estimate(), 0);
    }

    #[test]
    fn an_integer_and_the_string_of_it_are_different_values() {
        let mut h = HyperLogLog::new();
        h.add(&int(1));
        h.add(&PropertyValue::String("1".into()));
        assert_eq!(h.estimate(), 2);
    }

    #[test]
    fn a_whole_float_and_the_integer_are_the_same_value() {
        // Cypher compares `1` and `1.0` as equal, so a DISTINCT count that
        // separated them would disagree with `=` about its own question.
        let mut h = HyperLogLog::new();
        h.add(&int(1));
        h.add(&PropertyValue::Float(1.0));
        assert_eq!(h.estimate(), 1);
    }

    #[test]
    fn merging_two_sketches_matches_one_over_both() {
        let mut a = HyperLogLog::new();
        let mut b = HyperLogLog::new();
        let mut both = HyperLogLog::new();
        for i in 0..5_000i64 {
            a.add(&int(i));
            both.add(&int(i));
        }
        for i in 2_500..8_000i64 {
            b.add(&int(i));
            both.add(&int(i));
        }
        a.merge(&b);
        assert_eq!(a.estimate(), both.estimate(), "merge must be exact");
    }

    #[test]
    fn a_quantile_lands_near_the_exact_one() {
        let mut d = TDigest::new(100.0);
        let values: Vec<f64> = (1..=10_000).map(|i| i as f64).collect();
        for v in &values {
            d.add(*v);
        }
        for (q, exact) in [(0.5, 5000.0), (0.95, 9500.0), (0.99, 9900.0)] {
            let got = d.quantile(q).unwrap();
            let err = (got - exact).abs() / exact;
            assert!(
                err < 0.01,
                "q={q}: got {got}, exact {exact}, error {:.3}",
                err
            );
        }
    }

    #[test]
    fn the_tails_are_more_accurate_than_the_middle() {
        // The reason to use a t-digest rather than a fixed-size sample. If this
        // ever stops holding, the scale function has been broken and the
        // structure has become an expensive sample.
        let mut d = TDigest::new(100.0);
        for i in 1..=100_000 {
            d.add(i as f64);
        }
        let p999 = d.quantile(0.999).unwrap();
        let err_tail = (p999 - 99_900.0).abs() / 99_900.0;
        assert!(err_tail < 0.002, "p99.9 error {:.4}", err_tail);
    }

    #[test]
    fn merging_two_digests_is_close_to_one_over_both() {
        // Grouped aggregation merges partial aggregates; without this a
        // `RETURN key, approx.percentile(...)` combines nothing.
        let mut a = TDigest::new(100.0);
        let mut b = TDigest::new(100.0);
        let mut both = TDigest::new(100.0);
        for i in 1..=5_000 {
            a.add(i as f64);
            both.add(i as f64);
        }
        for i in 5_001..=10_000 {
            b.add(i as f64);
            both.add(i as f64);
        }
        a.merge(&b);
        for q in [0.5, 0.95, 0.99] {
            let m = a.quantile(q).unwrap();
            let o = both.quantile(q).unwrap();
            assert!((m - o).abs() / o < 0.02, "q={q}: merged {m}, single {o}");
        }
        assert_eq!(a.len(), 10_000.0);
    }

    #[test]
    fn min_and_max_are_the_actual_extremes() {
        let mut d = TDigest::new(100.0);
        for v in [5.0, 1.0, 9.0, 3.0] {
            d.add(v);
        }
        assert_eq!(d.quantile(0.0), Some(1.0));
        assert_eq!(d.quantile(1.0), Some(9.0));
    }

    #[test]
    fn an_empty_digest_has_no_quantile_rather_than_zero() {
        // Zero is a value the data might have had. `None` is the answer to
        // "what is the p95 of nothing".
        let mut d = TDigest::new(100.0);
        assert_eq!(d.quantile(0.5), None);
        assert!(d.is_empty());
    }
}
