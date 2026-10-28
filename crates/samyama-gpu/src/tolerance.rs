//! How closely each kernel must agree with the CPU (HW-09, #1682).
//!
//! A GPU kernel does not give bit-identical results. It works in `f32`, and a
//! parallel reduction adds in a different order than a CPU loop. So "equal to
//! the CPU" needs a number, and these are those numbers.
//!
//! `samyama-graph-algorithms/src/gpu_parity_tests.rs` holds every kernel to its
//! tolerance against a CPU reference, on a real device. Each tolerance is
//! documented with the error it allows for and what the parity test measured
//! (on Apple M-series Metal, the device available when they were set). A
//! tolerance only ever loosens with a reason written next to it.
//!
//! | Kernel | Compared | Tolerance |
//! |---|---|---|
//! | `gpu_page_rank` | each node's score | [`GRAPH_SCORE_ABS`], absolute |
//! | `gpu_lcc` | each node's coefficient, and the average | [`GRAPH_SCORE_ABS`], absolute |
//! | `gpu_count_triangles` | the count | exact |
//! | `gpu_cdlp` | every node is labelled | no label equality: ties may break differently |
//! | `gpu_sum_f64` | the sum | [`SUM_REL`] × the sum of absolute values |
//! | `gpu_argsort_f64` | the values in sorted order | exact, on the `f32` keys the kernel sorts |
//! | `gpu_batch_cosine` | each distance | [`DISTANCE_ABS`], absolute |
//! | `gpu_batch_inner_product` | each distance | [`DISTANCE_ABS`], absolute |
//! | `gpu_pca` | mean, eigenvalues, components | [`PCA_MEAN_ABS`], [`PCA_EIGENVALUE_REL`], [`PCA_COMPONENT_ALIGNMENT`] |

/// PageRank scores and clustering coefficients: six decimal places, the LDBC
/// Graphalytics comparison precision.
pub const GRAPH_SCORE_ABS: f64 = 1e-6;

/// The sum, as a fraction of the sum of absolute values.
///
/// The kernel rounds each input to `f32` (half an ulp, 2^-24 relative) and
/// adds in a 256-wide workgroup tree (8 levels) before the CPU adds the
/// partial sums in `f64`, so the worst case is about 9 × 2^-24 ≈ 5e-7.
/// Measuring against `sum(|x|)` rather than `|sum(x)|` keeps the bound
/// meaningful when positive and negative values cancel. Measured: 6.2e-11.
pub const SUM_REL: f64 = 1e-6;

/// Cosine and inner-product distances, for vectors of unit scale.
///
/// Each distance is a dot product accumulated in `f32`, one term at a time, so
/// the worst case grows with the dimension: 128 × 2^-24 ≈ 7.6e-6 at the
/// dimension the parity test uses. Measured: 1.3e-7 (cosine), 2.0e-7 (inner
/// product).
pub const DISTANCE_ABS: f64 = 1e-5;

/// The per-feature mean PCA centres by, absolute, for features whose values are
/// at most a few units. Measured: 1.1e-7.
pub const PCA_MEAN_ABS: f64 = 1e-5;

/// Each eigenvalue (variance explained), relative to the CPU's. The GPU builds
/// its covariance matrix in `f32`. Measured: 1.8e-6.
pub const PCA_EIGENVALUE_REL: f64 = 1e-4;

/// Each component, as `1 - |cos θ|` between the GPU and CPU directions.
///
/// The sign of an eigenvector is arbitrary, so the components are compared by
/// the angle between them, not element by element. Measured: 2.9e-13; the
/// bound leaves room for eigenvalue gaps narrower than the test's.
pub const PCA_COMPONENT_ALIGNMENT: f64 = 1e-6;
