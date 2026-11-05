//! f32 distance kernels with runtime dispatch (#1976).
//!
//! Three reductions cover every metric the index supports: the dot product
//! (inner product), the squared L2 distance, and the triple
//! `(a.b, a.a, b.b)` that cosine needs. Each has a scalar version, which is
//! the reference, and a SIMD version chosen once per process:
//!
//! - **x86_64**: AVX2 with FMA, when `is_x86_feature_detected!` reports both.
//!   A CPU without them gets the scalar kernels.
//! - **aarch64**: NEON. It is part of the aarch64 baseline, so there is no
//!   runtime check; the `cfg(target_feature = "neon")` guard only matters for
//!   a target built with NEON explicitly turned off.
//! - **anything else**: scalar.
//!
//! The SIMD kernels add in a different order from the scalar loop (several
//! lanes and accumulators, then a horizontal sum), so they agree with it to
//! rounding, not bit for bit. Inputs shorter than one SIMD block go through
//! the scalar tail and give exactly the scalar answer.
//!
//! Slices of different lengths are compared over the shorter one, as
//! `zip` did in the loops these replace.

use std::sync::OnceLock;

/// Scalar reference kernels. These are the loops the index used before #1976.
pub mod scalar {
    /// `sum(a[i] * b[i])`.
    pub fn dot(a: &[f32], b: &[f32]) -> f32 {
        let mut s = 0.0f32;
        for (x, y) in a.iter().zip(b) {
            s += x * y;
        }
        s
    }

    /// `sum((a[i] - b[i])^2)`.
    pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        let mut s = 0.0f32;
        for (x, y) in a.iter().zip(b) {
            let d = x - y;
            s += d * d;
        }
        s
    }

    /// `(a.b, a.a, b.b)` in one pass.
    pub fn cosine_parts(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
        for (x, y) in a.iter().zip(b) {
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
        (dot, na, nb)
    }
}

#[cfg(target_arch = "x86_64")]
mod avx2 {
    use std::arch::x86_64::*;

    /// Sum of the eight lanes.
    #[inline]
    #[target_feature(enable = "avx2,fma")]
    unsafe fn hsum(v: __m256) -> f32 {
        let lo = _mm256_castps256_ps128(v);
        let hi = _mm256_extractf128_ps(v, 1);
        let s = _mm_add_ps(lo, hi);
        let s = _mm_add_ps(s, _mm_movehl_ps(s, s));
        let s = _mm_add_ss(s, _mm_shuffle_ps(s, s, 0b01));
        _mm_cvtss_f32(s)
    }

    /// # Safety
    /// The CPU must support AVX2 and FMA.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn dot(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut i = 0;
        // SAFETY (all loads below): `i + 16 <= n` or `i + 8 <= n`, and `n` is
        // no longer than either slice, so every load is in bounds. The loads
        // are unaligned (`loadu`), so alignment does not matter.
        while i + 16 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            acc1 = _mm256_fmadd_ps(
                _mm256_loadu_ps(pa.add(i + 8)),
                _mm256_loadu_ps(pb.add(i + 8)),
                acc1,
            );
            i += 16;
        }
        if i + 8 <= n {
            acc0 = _mm256_fmadd_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)), acc0);
            i += 8;
        }
        let mut s = hsum(_mm256_add_ps(acc0, acc1));
        s += super::scalar::dot(&a[i..n], &b[i..n]);
        s
    }

    /// # Safety
    /// The CPU must support AVX2 and FMA.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();
        let mut i = 0;
        // SAFETY: as in `dot`; every load is within the first `n` elements.
        while i + 16 <= n {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
            let d1 = _mm256_sub_ps(
                _mm256_loadu_ps(pa.add(i + 8)),
                _mm256_loadu_ps(pb.add(i + 8)),
            );
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);
            acc1 = _mm256_fmadd_ps(d1, d1, acc1);
            i += 16;
        }
        if i + 8 <= n {
            let d0 = _mm256_sub_ps(_mm256_loadu_ps(pa.add(i)), _mm256_loadu_ps(pb.add(i)));
            acc0 = _mm256_fmadd_ps(d0, d0, acc0);
            i += 8;
        }
        let mut s = hsum(_mm256_add_ps(acc0, acc1));
        s += super::scalar::l2_sq(&a[i..n], &b[i..n]);
        s
    }

    /// # Safety
    /// The CPU must support AVX2 and FMA.
    #[target_feature(enable = "avx2,fma")]
    pub unsafe fn cosine_parts(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut dot = _mm256_setzero_ps();
        let mut na = _mm256_setzero_ps();
        let mut nb = _mm256_setzero_ps();
        let mut i = 0;
        // SAFETY: as in `dot`; every load is within the first `n` elements.
        while i + 8 <= n {
            let x = _mm256_loadu_ps(pa.add(i));
            let y = _mm256_loadu_ps(pb.add(i));
            dot = _mm256_fmadd_ps(x, y, dot);
            na = _mm256_fmadd_ps(x, x, na);
            nb = _mm256_fmadd_ps(y, y, nb);
            i += 8;
        }
        let (td, ta, tb) = super::scalar::cosine_parts(&a[i..n], &b[i..n]);
        (hsum(dot) + td, hsum(na) + ta, hsum(nb) + tb)
    }
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
mod neon {
    use std::arch::aarch64::*;

    /// # Safety
    /// NEON must be available; it is whenever this module is compiled.
    #[inline]
    pub unsafe fn dot(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = vdupq_n_f32(0.0);
        let mut acc1 = vdupq_n_f32(0.0);
        let mut acc2 = vdupq_n_f32(0.0);
        let mut acc3 = vdupq_n_f32(0.0);
        let mut i = 0;
        // SAFETY (all loads below): `i + 16 <= n` or `i + 4 <= n`, and `n` is
        // no longer than either slice, so every load is in bounds. `vld1q_f32`
        // has no alignment requirement beyond that of `f32`.
        while i + 16 <= n {
            acc0 = vfmaq_f32(acc0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
            acc1 = vfmaq_f32(acc1, vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
            acc2 = vfmaq_f32(acc2, vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
            acc3 = vfmaq_f32(acc3, vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
            i += 16;
        }
        while i + 4 <= n {
            acc0 = vfmaq_f32(acc0, vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
            i += 4;
        }
        let acc = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
        vaddvq_f32(acc) + super::scalar::dot(&a[i..n], &b[i..n])
    }

    /// # Safety
    /// NEON must be available; it is whenever this module is compiled.
    #[inline]
    pub unsafe fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let mut acc0 = vdupq_n_f32(0.0);
        let mut acc1 = vdupq_n_f32(0.0);
        let mut acc2 = vdupq_n_f32(0.0);
        let mut acc3 = vdupq_n_f32(0.0);
        let mut i = 0;
        // SAFETY: as in `dot`; every load is within the first `n` elements.
        while i + 16 <= n {
            let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
            let d1 = vsubq_f32(vld1q_f32(pa.add(i + 4)), vld1q_f32(pb.add(i + 4)));
            let d2 = vsubq_f32(vld1q_f32(pa.add(i + 8)), vld1q_f32(pb.add(i + 8)));
            let d3 = vsubq_f32(vld1q_f32(pa.add(i + 12)), vld1q_f32(pb.add(i + 12)));
            acc0 = vfmaq_f32(acc0, d0, d0);
            acc1 = vfmaq_f32(acc1, d1, d1);
            acc2 = vfmaq_f32(acc2, d2, d2);
            acc3 = vfmaq_f32(acc3, d3, d3);
            i += 16;
        }
        while i + 4 <= n {
            let d0 = vsubq_f32(vld1q_f32(pa.add(i)), vld1q_f32(pb.add(i)));
            acc0 = vfmaq_f32(acc0, d0, d0);
            i += 4;
        }
        let acc = vaddq_f32(vaddq_f32(acc0, acc1), vaddq_f32(acc2, acc3));
        vaddvq_f32(acc) + super::scalar::l2_sq(&a[i..n], &b[i..n])
    }

    /// # Safety
    /// NEON must be available; it is whenever this module is compiled.
    #[inline]
    pub unsafe fn cosine_parts(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        let n = a.len().min(b.len());
        let (pa, pb) = (a.as_ptr(), b.as_ptr());
        let (mut d0, mut d1) = (vdupq_n_f32(0.0), vdupq_n_f32(0.0));
        let (mut a0, mut a1) = (vdupq_n_f32(0.0), vdupq_n_f32(0.0));
        let (mut b0, mut b1) = (vdupq_n_f32(0.0), vdupq_n_f32(0.0));
        let mut i = 0;
        // SAFETY: as in `dot`; every load is within the first `n` elements.
        while i + 8 <= n {
            let x0 = vld1q_f32(pa.add(i));
            let y0 = vld1q_f32(pb.add(i));
            let x1 = vld1q_f32(pa.add(i + 4));
            let y1 = vld1q_f32(pb.add(i + 4));
            d0 = vfmaq_f32(d0, x0, y0);
            d1 = vfmaq_f32(d1, x1, y1);
            a0 = vfmaq_f32(a0, x0, x0);
            a1 = vfmaq_f32(a1, x1, x1);
            b0 = vfmaq_f32(b0, y0, y0);
            b1 = vfmaq_f32(b1, y1, y1);
            i += 8;
        }
        if i + 4 <= n {
            let x0 = vld1q_f32(pa.add(i));
            let y0 = vld1q_f32(pb.add(i));
            d0 = vfmaq_f32(d0, x0, y0);
            a0 = vfmaq_f32(a0, x0, x0);
            b0 = vfmaq_f32(b0, y0, y0);
            i += 4;
        }
        let (td, ta, tb) = super::scalar::cosine_parts(&a[i..n], &b[i..n]);
        (
            vaddvq_f32(vaddq_f32(d0, d1)) + td,
            vaddvq_f32(vaddq_f32(a0, a1)) + ta,
            vaddvq_f32(vaddq_f32(b0, b1)) + tb,
        )
    }
}

/// A reduction to one value: `dot` or `l2_sq`.
pub type Reduce = fn(&[f32], &[f32]) -> f32;
/// The cosine reduction, `(a.b, a.a, b.b)`.
pub type Parts = fn(&[f32], &[f32]) -> (f32, f32, f32);

/// The kernel set chosen for this process.
#[derive(Clone, Copy)]
struct Kernels {
    name: &'static str,
    dot: Reduce,
    l2_sq: Reduce,
    cosine_parts: Parts,
}

const SCALAR: Kernels = Kernels {
    name: "scalar",
    dot: scalar::dot,
    l2_sq: scalar::l2_sq,
    cosine_parts: scalar::cosine_parts,
};

#[cfg(target_arch = "x86_64")]
fn avx2_kernels() -> Option<Kernels> {
    if !(is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma")) {
        return None;
    }
    // SAFETY (the three wrappers): they are reachable only through the
    // `Kernels` returned here, which exists only after the check above found
    // AVX2 and FMA on this CPU.
    fn dot(a: &[f32], b: &[f32]) -> f32 {
        unsafe { avx2::dot(a, b) }
    }
    fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        unsafe { avx2::l2_sq(a, b) }
    }
    fn cosine_parts(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        unsafe { avx2::cosine_parts(a, b) }
    }
    Some(Kernels {
        name: "avx2+fma",
        dot,
        l2_sq,
        cosine_parts,
    })
}

#[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
fn neon_kernels() -> Option<Kernels> {
    // SAFETY (the three wrappers): this function is compiled only when the
    // target has NEON enabled, so the NEON instructions are available.
    fn dot(a: &[f32], b: &[f32]) -> f32 {
        unsafe { neon::dot(a, b) }
    }
    fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
        unsafe { neon::l2_sq(a, b) }
    }
    fn cosine_parts(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
        unsafe { neon::cosine_parts(a, b) }
    }
    Some(Kernels {
        name: "neon",
        dot,
        l2_sq,
        cosine_parts,
    })
}

/// The SIMD kernel set for this CPU, if there is one.
fn simd_kernels() -> Option<Kernels> {
    #[cfg(target_arch = "x86_64")]
    {
        avx2_kernels()
    }
    #[cfg(all(target_arch = "aarch64", target_feature = "neon"))]
    {
        neon_kernels()
    }
    #[cfg(not(any(
        target_arch = "x86_64",
        all(target_arch = "aarch64", target_feature = "neon")
    )))]
    {
        None
    }
}

fn kernels() -> &'static Kernels {
    static K: OnceLock<Kernels> = OnceLock::new();
    K.get_or_init(|| simd_kernels().unwrap_or(SCALAR))
}

/// Which kernels this process uses: `"avx2+fma"`, `"neon"` or `"scalar"`.
pub fn active_kernels() -> &'static str {
    kernels().name
}

/// `sum(a[i] * b[i])`, with the fastest kernel this CPU has.
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    (kernels().dot)(a, b)
}

/// `sum((a[i] - b[i])^2)`, with the fastest kernel this CPU has.
#[inline]
pub fn l2_sq(a: &[f32], b: &[f32]) -> f32 {
    (kernels().l2_sq)(a, b)
}

/// `(a.b, a.a, b.b)`, with the fastest kernel this CPU has.
#[inline]
pub fn cosine_parts(a: &[f32], b: &[f32]) -> (f32, f32, f32) {
    (kernels().cosine_parts)(a, b)
}

/// The SIMD kernels themselves, bypassing dispatch, or `None` where this CPU
/// has none. For the microbenchmark and the agreement tests, which must
/// compare SIMD with scalar rather than the dispatcher with itself.
pub mod simd {
    use super::{Parts, Reduce};

    /// The name, `dot`, `l2_sq` and `cosine_parts` of the SIMD set.
    pub fn kernels() -> Option<(&'static str, Reduce, Reduce, Parts)> {
        super::simd_kernels().map(|k| (k.name, k.dot, k.l2_sq, k.cosine_parts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic values in [-1, 1) from a 64-bit LCG, so a failure
    /// reproduces without a seed in the output.
    fn vector(len: usize, seed: u64) -> Vec<f32> {
        let mut s = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (0..len)
            .map(|_| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    const LENGTHS: &[usize] = &[
        0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 95, 96, 97, 767, 768, 769, 1535, 1536,
        1537, 1543,
    ];

    /// Agreement to rounding. The sums differ only in order, so the error
    /// grows with the sum of the absolute terms, not with the result: a
    /// bound relative to the result fails on sums that cancel to near zero.
    fn close(simd: f32, scalar: f32, abs_terms: f32) {
        let tol = 1e-5 * abs_terms.max(1.0);
        assert!(
            (simd - scalar).abs() <= tol,
            "simd {simd} vs scalar {scalar} (tolerance {tol})"
        );
    }

    fn abs_dot(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x * y).abs()).sum()
    }

    fn check(name: &str, a: &[f32], b: &[f32], k: (Reduce, Reduce, Parts)) {
        let (dot, l2, cos) = k;
        let n = a.len();
        close(dot(a, b), scalar::dot(a, b), abs_dot(a, b));
        close(l2(a, b), scalar::l2_sq(a, b), scalar::l2_sq(a, b));
        let (d, na, nb) = cos(a, b);
        let (sd, sna, snb) = scalar::cosine_parts(a, b);
        close(d, sd, abs_dot(a, b));
        close(na, sna, sna);
        close(nb, snb, snb);
        if n < 4 {
            // Shorter than any SIMD block: the scalar tail alone.
            assert_eq!(dot(a, b), scalar::dot(a, b), "{name} len {n}");
        }
    }

    #[test]
    fn simd_kernels_match_scalar_on_awkward_lengths() {
        let Some((name, dot, l2, cos)) = simd::kernels() else {
            // No SIMD on this CPU; the dispatcher is the scalar set.
            assert_eq!(active_kernels(), "scalar");
            return;
        };
        assert_eq!(active_kernels(), name);
        for &len in LENGTHS {
            for seed in 0..4u64 {
                let a = vector(len, seed * 2 + 1);
                let b = vector(len, seed * 2 + 2);
                check(name, &a, &b, (dot, l2, cos));
                // A vector against itself: L2 must be exactly 0.
                assert_eq!(l2(&a, &a), 0.0, "{name} len {len}");
            }
        }
    }

    #[test]
    fn dispatched_kernels_match_scalar() {
        for &len in LENGTHS {
            let a = vector(len, 11);
            let b = vector(len, 12);
            check("dispatch", &a, &b, (dot, l2_sq, cosine_parts));
        }
    }

    #[test]
    fn zero_vectors_give_zero_norms() {
        let Some((_, _, _, cos)) = simd::kernels() else {
            return;
        };
        for &len in LENGTHS {
            let z = vec![0.0f32; len];
            let a = vector(len, 7);
            let (d, nz, na) = cos(&z, &a);
            assert_eq!(d, 0.0, "len {len}");
            assert_eq!(nz, 0.0, "len {len}");
            close(na, scalar::cosine_parts(&a, &a).1, na);
            assert_eq!(cos(&z, &z), (0.0, 0.0, 0.0), "len {len}");
        }
    }

    #[test]
    fn unequal_lengths_use_the_shorter() {
        let a = vector(100, 3);
        let b = vector(37, 4);
        close(
            dot(&a, &b),
            scalar::dot(&a[..37], &b),
            abs_dot(&a[..37], &b),
        );
        close(
            l2_sq(&a, &b),
            scalar::l2_sq(&a[..37], &b),
            scalar::l2_sq(&a[..37], &b),
        );
    }

    /// The index's f32 distance runs on these kernels; its f16 distance is
    /// still the scalar loop. On values f16 holds exactly the two see the
    /// same numbers, so they must agree to rounding for every metric.
    #[test]
    fn index_distance_uses_kernels_that_agree_with_the_scalar_path() {
        use crate::vector::index::MetricDistance;
        use crate::vector::DistanceMetric;
        use half::f16;
        use hnsw_rs::prelude::Distance;
        for metric in [
            DistanceMetric::Cosine,
            DistanceMetric::L2,
            DistanceMetric::InnerProduct,
        ] {
            let m = MetricDistance(metric);
            for &len in &[1usize, 7, 96, 768, 1536] {
                let to16 = |v: Vec<f32>| -> Vec<f16> { v.into_iter().map(f16::from_f32).collect() };
                let (a16, b16) = (to16(vector(len, 21)), to16(vector(len, 22)));
                let a: Vec<f32> = a16.iter().map(|x| x.to_f32()).collect();
                let b: Vec<f32> = b16.iter().map(|x| x.to_f32()).collect();
                let (simd, scalar) = (m.eval(&a, &b), m.eval(&a16, &b16));
                assert!(
                    (simd - scalar).abs() <= 1e-4 * scalar.abs().max(1.0),
                    "{metric:?} len {len}: {simd} vs {scalar}"
                );
            }
            // Zero vectors: cosine's defined fallback, not a NaN.
            let z = vec![0.0f32; 96];
            assert!(m.eval(&z, &vector(96, 5)).is_finite(), "{metric:?}");
        }
    }

    #[test]
    fn unaligned_slices_agree() {
        // Offsets into one allocation, so the loads start off any SIMD
        // alignment boundary.
        let big = vector(1600, 9);
        let other = vector(1600, 10);
        for off in 0..8 {
            let a = &big[off..off + 768];
            let b = &other[(7 - off)..(7 - off) + 768];
            check("unaligned", a, b, (dot, l2_sq, cosine_parts));
        }
    }
}
