//! Principal Component Analysis (PCA)
//!
//! Implements dimensionality reduction for node feature matrices.
//!
//! Two solvers are available:
//! - **Randomized SVD** (default): Halko-Martinsson-Tropp algorithm. O(n·d·k),
//!   numerically stable, automatic orthogonality. Industry standard (scikit-learn,
//!   cuML, Spark MLlib).
//! - **Power Iteration** (legacy): Extract one eigenvector at a time from the
//!   covariance matrix, then deflate and repeat. With Gram-Schmidt
//!   re-orthogonalization for stability.

use ndarray::{Array1, Array2, Axis};
use rand::Rng;

/// Solver strategy for PCA.
#[derive(Debug, Clone)]
pub enum PcaSolver {
    /// Automatically select solver based on data size:
    /// Randomized if n > 500 and k < 0.8 * min(n, d), else PowerIteration.
    Auto,
    /// Randomized SVD (Halko-Martinsson-Tropp).
    Randomized {
        /// Extra columns for accuracy (default: 10).
        n_oversamples: usize,
        /// Subspace power iterations for stability (default: 4).
        n_power_iters: usize,
    },
    /// Legacy power iteration with deflation.
    PowerIteration,
}

impl Default for PcaSolver {
    fn default() -> Self {
        PcaSolver::Auto
    }
}

/// PCA configuration
pub struct PcaConfig {
    /// Number of principal components to extract
    pub n_components: usize,
    /// Maximum power iterations per component (only used for PowerIteration solver)
    pub max_iterations: usize,
    /// Convergence tolerance for power iteration (only used for PowerIteration solver)
    pub tolerance: f64,
    /// Subtract column means before PCA (default: true)
    pub center: bool,
    /// Divide by column std dev before PCA (default: false)
    pub scale: bool,
    /// Solver strategy (default: Auto)
    pub solver: PcaSolver,
    /// Seed for the randomized solver's projection matrix.
    ///
    /// The randomized solver draws a random projection, so without a fixed
    /// seed the same data gives a different answer every call. That is not a
    /// wrong answer -- randomized SVD is approximate by construction and the
    /// spread sits inside its error budget -- but it is not reproducible, and
    /// ALGO-11 asks for reproducible. Defaults to a constant so determinism is
    /// what a caller gets without asking; set it to vary the draw deliberately.
    pub seed: u64,
}

/// The default projection seed. Any value would do; it is fixed so that two
/// runs agree.
pub const DEFAULT_PCA_SEED: u64 = 0x5A_4D_59_4D_41; // "SAMYA"


impl Default for PcaConfig {
    fn default() -> Self {
        Self {
            n_components: 2,
            max_iterations: 100,
            tolerance: 1e-6,
            center: true,
            scale: false,
            solver: PcaSolver::Auto,
            seed: DEFAULT_PCA_SEED,
        }
    }
}

/// PCA result containing components and explained variance
pub struct PcaResult {
    /// Principal component vectors (n_components x n_features), row-major
    pub components: Vec<Vec<f64>>,
    /// Variance explained by each component (eigenvalues)
    pub explained_variance: Vec<f64>,
    /// Proportion of total variance explained by each component
    pub explained_variance_ratio: Vec<f64>,
    /// Feature means used for centering (needed to project new data)
    pub mean: Vec<f64>,
    /// Feature standard deviations (if scaling was used)
    pub std_dev: Vec<f64>,
    /// Number of samples in the input data
    pub n_samples: usize,
    /// Number of features in the input data
    pub n_features: usize,
    /// Number of iterations used (last component for PowerIteration, power iters for Randomized)
    pub iterations_used: usize,
}

impl PcaResult {
    /// Project multiple data points into the reduced PCA space.
    ///
    /// Uses ndarray matrix multiplication for efficiency.
    pub fn transform(&self, data: &[Vec<f64>]) -> Vec<Vec<f64>> {
        let n = data.len();
        if n == 0 || self.components.is_empty() {
            return vec![];
        }
        let d = self.n_features;
        let k = self.components.len();

        // Build component matrix (k × d)
        let comp_flat: Vec<f64> = self.components.iter().flat_map(|r| r.iter().copied()).collect();
        let comp_mat = Array2::from_shape_vec((k, d), comp_flat).unwrap();

        // Build centered data matrix (n × d)
        let data_flat: Vec<f64> = data.iter().flat_map(|row| {
            row.iter().enumerate().map(|(j, &val)| {
                let mut v = val - self.mean[j];
                if self.std_dev[j] > 0.0 && self.std_dev[j] != 1.0 {
                    v /= self.std_dev[j];
                }
                v
            })
        }).collect();
        let data_mat = Array2::from_shape_vec((n, d), data_flat).unwrap();

        // projected = data_mat @ comp_mat^T → (n × k)
        let projected = data_mat.dot(&comp_mat.t());

        // Convert back to Vec<Vec<f64>>
        projected.rows().into_iter()
            .map(|row| row.to_vec())
            .collect()
    }

    /// Project a single data point into the reduced PCA space.
    pub fn transform_one(&self, point: &[f64]) -> Vec<f64> {
        let k = self.components.len();
        let d = self.n_features;
        let mut result = vec![0.0; k];

        for (c, component) in self.components.iter().enumerate() {
            let mut dot = 0.0;
            for j in 0..d {
                let mut val = point[j] - self.mean[j];
                if self.std_dev[j] > 0.0 && self.std_dev[j] != 1.0 {
                    val /= self.std_dev[j];
                }
                dot += val * component[j];
            }
            result[c] = dot;
        }
        result
    }
}

/// Run PCA on a feature matrix.
///
/// # Arguments
/// - `data`: slice of rows, each row is a feature vector of equal length
/// - `config`: PCA parameters (including solver selection)
///
/// # Panics
/// Panics if `data` is empty or rows have inconsistent lengths.
pub fn pca(data: &[Vec<f64>], config: PcaConfig) -> PcaResult {
    let n = data.len();
    assert!(n > 0, "PCA requires at least one data point");
    let d = data[0].len();
    assert!(d > 0, "PCA requires at least one feature");

    let k = config.n_components.min(d).min(n);

    // Build ndarray matrix from flat data (cache-friendly)
    let flat: Vec<f64> = data.iter().flat_map(|row| {
        assert_eq!(row.len(), d, "All rows must have the same number of features");
        row.iter().copied()
    }).collect();
    let mut mat = Array2::from_shape_vec((n, d), flat).unwrap();

    // Compute column means and center using ndarray operations
    let mut mean = vec![0.0; d];
    if config.center {
        let mean_arr = mat.mean_axis(Axis(0)).unwrap();
        mat -= &mean_arr;
        mean = mean_arr.to_vec();
    }

    // Compute column std devs and scale
    let mut std_dev = vec![1.0; d];
    if config.scale {
        for j in 0..d {
            let col = mat.column(j);
            let ss: f64 = col.iter().map(|x| x * x).sum();
            let s = (ss / (n.max(2) - 1) as f64).sqrt();
            std_dev[j] = s;
            if s > 0.0 {
                mat.column_mut(j).mapv_inplace(|x| x / s);
            }
        }
    }

    // Decide solver
    let use_randomized = match &config.solver {
        PcaSolver::Auto => {
            let min_dim = n.min(d);
            n > 500 && (k as f64) < 0.8 * min_dim as f64
        }
        PcaSolver::Randomized { .. } => true,
        PcaSolver::PowerIteration => false,
    };

    let (n_oversamples, n_power_iters) = match &config.solver {
        PcaSolver::Randomized { n_oversamples, n_power_iters } => (*n_oversamples, *n_power_iters),
        _ => (10, 4),
    };

    if use_randomized {
        // ── Randomized SVD path ──
        let (components, singular_values) =
            randomized_svd(&mat, k, n_oversamples, n_power_iters, config.seed);

        // Eigenvalues (variance) = σ² / (n-1)
        let denom = if n > 1 { (n - 1) as f64 } else { 1.0 };
        let eigenvalues: Vec<f64> = singular_values.iter().map(|&s| s * s / denom).collect();

        // Total variance from covariance diagonal
        let total_variance = compute_total_variance(&mat, denom);

        let explained_variance_ratio: Vec<f64> = eigenvalues
            .iter()
            .map(|&ev| if total_variance > 0.0 { ev / total_variance } else { 0.0 })
            .collect();

        PcaResult {
            components,
            explained_variance: eigenvalues,
            explained_variance_ratio,
            mean,
            std_dev,
            n_samples: n,
            n_features: d,
            iterations_used: n_power_iters,
        }
    } else {
        // ── Power iteration with deflation path ──
        let xt = mat.t();
        let denom = if n > 1 { (n - 1) as f64 } else { 1.0 };
        let mut cov = xt.dot(&mat) / denom;

        // Capture total variance BEFORE deflation
        let total_variance: f64 = (0..d).map(|i| cov[[i, i]]).sum();

        let mut components: Vec<Vec<f64>> = Vec::with_capacity(k);
        let mut eigenvalues: Vec<f64> = Vec::with_capacity(k);
        let mut last_iters = 0;

        for _ in 0..k {
            let (eigvec, _eigval, iters) = power_iteration(&cov, config.max_iterations, config.tolerance);
            last_iters = iters;

            // Gram-Schmidt re-orthogonalization against previous components
            let mut new_vec = eigvec;
            for prev in &components {
                let dot: f64 = new_vec.iter().zip(prev).map(|(a, b)| a * b).sum();
                for (v, p) in new_vec.iter_mut().zip(prev) {
                    *v -= dot * p;
                }
            }
            // Re-normalize after orthogonalization
            let norm: f64 = new_vec.iter().map(|x| x * x).sum::<f64>().sqrt();
            if norm > 1e-15 {
                for v in &mut new_vec {
                    *v /= norm;
                }
            }

            // Recompute eigenvalue after orthogonalization: v^T C v
            let v_arr = Array1::from(new_vec.clone());
            let cv = cov.dot(&v_arr);
            let eigval = v_arr.dot(&cv);

            // Deflate: C = C - lambda * v * v^T
            for r in 0..d {
                for c in 0..d {
                    cov[[r, c]] -= eigval * new_vec[r] * new_vec[c];
                }
            }

            components.push(new_vec);
            eigenvalues.push(eigval);
        }

        let explained_variance_ratio: Vec<f64> = eigenvalues
            .iter()
            .map(|&ev| if total_variance > 0.0 { ev / total_variance } else { 0.0 })
            .collect();

        PcaResult {
            components,
            explained_variance: eigenvalues,
            explained_variance_ratio,
            mean,
            std_dev,
            n_samples: n,
            n_features: d,
            iterations_used: last_iters,
        }
    }
}

/// Compute total variance from centered data matrix: sum of column variances.
fn compute_total_variance(mat: &Array2<f64>, denom: f64) -> f64 {
    let d = mat.ncols();
    let mut total = 0.0;
    for j in 0..d {
        let col = mat.column(j);
        let ss: f64 = col.iter().map(|x| x * x).sum();
        total += ss / denom;
    }
    total
}

// ============================================================
// Randomized SVD (Halko-Martinsson-Tropp)
// ============================================================

/// Randomized SVD: find top-k singular vectors of a centered data matrix.
///
/// Algorithm (Algorithm 5.1 from Halko et al. 2011):
/// 1. Generate random Gaussian matrix Omega (d × (k+p))
/// 2. Y = X @ Omega
/// 3. Power iteration for stability: Y = (X @ X^T)^q @ Y, with QR between steps
/// 4. QR factorization of Y → Q
/// 5. B = Q^T @ X (small: (k+p) × d)
/// 6. SVD of B via eigendecomposition of B @ B^T
/// 7. Return top-k right singular vectors and singular values
fn randomized_svd(
    mat: &Array2<f64>,       // n × d, already centered
    k: usize,                // number of components
    n_oversamples: usize,    // oversampling parameter
    n_power_iters: usize,    // power iterations for accuracy
    seed: u64,               // fixes the projection so two runs agree
) -> (Vec<Vec<f64>>, Vec<f64>) {
    let n = mat.nrows();
    let d = mat.ncols();
    let l = (k + n_oversamples).min(n).min(d); // sketch width

    // Stage 1: Random projection
    // Seeded, not `thread_rng()`. `Auto` only reaches this branch above 500
    // rows, so an unseeded draw is deterministic in every small fixture and
    // nondeterministic in production -- the tests cannot see it.
    let mut rng = <rand::rngs::StdRng as rand::SeedableRng>::seed_from_u64(seed);
    let omega_flat: Vec<f64> = (0..d * l).map(|_| rng.sample::<f64, _>(rand::distributions::Standard)).collect();
    let omega = Array2::from_shape_vec((d, l), omega_flat).unwrap();

    // Y = X @ Omega  (n × l)
    let mut y = mat.dot(&omega);

    // Power iteration for stability: repeat q times
    // Y = X @ (X^T @ Y), then QR factorize Y
    for _ in 0..n_power_iters {
        // QR factorize Y to maintain numerical stability
        qr_modified_gram_schmidt(&mut y);
        // Y = X @ (X^T @ Y)
        let xty = mat.t().dot(&y); // d × l
        y = mat.dot(&xty);         // n × l
    }

    // Final QR to get orthonormal basis Q
    qr_modified_gram_schmidt(&mut y);
    let q = y; // n × l, orthonormal columns

    // Stage 2: Form small matrix B = Q^T @ X  (l × d)
    let b = q.t().dot(mat);

    // SVD of B via eigendecomposition of B @ B^T  (l × l, small)
    let bbt = b.dot(&b.t());
    let l_actual = bbt.nrows();

    // Eigen-decompose the small l×l matrix using power iteration
    // (we need all l eigenvectors, but we only keep top k)
    let (eigvecs_left, eigvals) = symmetric_eigen(&bbt, l_actual);

    // Right singular vectors of B: V_i = B^T @ U_i / σ_i
    // Components are the right singular vectors of X (rows of V^T)
    let mut components = Vec::with_capacity(k);
    let mut singular_values = Vec::with_capacity(k);

    for i in 0..k.min(l_actual) {
        let sigma_sq = eigvals[i];
        if sigma_sq < 1e-30 {
            break;
        }
        let sigma = sigma_sq.sqrt();
        singular_values.push(sigma);

        // u_i = eigvecs_left column i
        let u_i = eigvecs_left.column(i);
        // v_i = B^T @ u_i / sigma
        let v_i = b.t().dot(&u_i) / sigma;
        components.push(v_i.to_vec());
    }

    (components, singular_values)
}

/// Symmetric eigendecomposition of a small matrix using power iteration + deflation.
///
/// Returns (eigenvectors as columns of Array2, eigenvalues sorted descending).
fn symmetric_eigen(mat: &Array2<f64>, k: usize) -> (Array2<f64>, Vec<f64>) {
    let d = mat.nrows();
    let mut deflated = mat.clone();
    let mut eigvecs = Array2::<f64>::zeros((d, k));
    let mut eigvals = Vec::with_capacity(k);

    for i in 0..k {
        let (vec, _val, _) = power_iteration(&deflated, 200, 1e-12);

        // Gram-Schmidt against previous eigenvectors
        let mut v = Array1::from(vec);
        for j in 0..i {
            let prev = eigvecs.column(j);
            let dot = prev.dot(&v);
            v -= &(&prev * dot);
        }
        let norm = v.dot(&v).sqrt();
        if norm > 1e-15 {
            v /= norm;
        }

        // Recompute eigenvalue after orthogonalization
        let av = deflated.dot(&v);
        let val = v.dot(&av);

        // Deflate
        for r in 0..d {
            for c in 0..d {
                deflated[[r, c]] -= val * v[r] * v[c];
            }
        }

        eigvecs.column_mut(i).assign(&v);
        eigvals.push(val);
    }

    (eigvecs, eigvals)
}

// ============================================================
// QR factorization (Modified Gram-Schmidt)
// ============================================================

/// Modified Gram-Schmidt QR factorization in-place.
///
/// Replaces columns of `mat` with orthonormal vectors spanning the same space.
fn qr_modified_gram_schmidt(mat: &mut Array2<f64>) {
    let ncols = mat.ncols();
    for j in 0..ncols {
        let mut col_j = mat.column(j).to_owned();
        for i in 0..j {
            let col_i = mat.column(i).to_owned();
            let r = col_i.dot(&col_j);
            col_j -= &(&col_i * r);
        }
        let norm = col_j.dot(&col_j).sqrt();
        if norm > 1e-15 {
            col_j /= norm;
        }
        mat.column_mut(j).assign(&col_j);
    }
}

// ============================================================
// Power iteration (used by both solvers and internal eigen)
// ============================================================

/// Power iteration: find the dominant eigenvector of a symmetric matrix.
///
/// Returns (eigenvector, eigenvalue, iterations_used).
fn power_iteration(
    matrix: &Array2<f64>,
    max_iters: usize,
    tolerance: f64,
) -> (Vec<f64>, f64, usize) {
    let d = matrix.nrows();

    // Initialize with a vector that has some structure to avoid degenerate starts
    let mut v = Array1::<f64>::zeros(d);
    for i in 0..d {
        v[i] = ((i + 1) as f64).sqrt();
    }
    let norm = v.dot(&v).sqrt();
    if norm > 0.0 {
        v /= norm;
    }

    let mut iters = 0;
    for iter in 0..max_iters {
        iters = iter + 1;

        // w = C * v
        let w = matrix.dot(&v);

        // Normalize
        let w_norm = w.dot(&w).sqrt();
        if w_norm < 1e-15 {
            // Matrix has zero eigenvalue in this direction
            break;
        }
        let v_new = &w / w_norm;

        // Check convergence: |v_new - v| or |v_new + v| (sign may flip)
        let diff_pos: f64 = v_new.iter().zip(v.iter()).map(|(a, b)| (a - b).powi(2)).sum();
        let diff_neg: f64 = v_new.iter().zip(v.iter()).map(|(a, b)| (a + b).powi(2)).sum();
        let diff = diff_pos.min(diff_neg).sqrt();

        v = v_new;

        if diff < tolerance {
            break;
        }
    }

    // Eigenvalue: v^T C v
    let cv = matrix.dot(&v);
    let eigenvalue = v.dot(&cv);

    (v.to_vec(), eigenvalue, iters)
}

#[cfg(test)]
mod determinism_tests {
    use super::*;

    /// Deterministic input: any spread in the output is the solver's, not the
    /// data's.
    fn data(n: usize, d: usize) -> Vec<Vec<f64>> {
        (0..n)
            .map(|i| (0..d).map(|j| ((i * 31 + j * 17) % 97) as f64 + (i as f64) * 0.01).collect())
            .collect()
    }

    fn first_component(n: usize, seed: u64) -> Vec<f64> {
        let cfg = PcaConfig { n_components: 3, seed, ..Default::default() };
        pca(&data(n, 20), cfg).components[0].clone()
    }

    /// The row count matters. `Auto` only reaches the randomized solver above
    /// 500 rows, so this test is worthless at the fixture sizes used elsewhere
    /// in this file -- which is exactly why the bug survived: every existing
    /// PCA test was under the threshold and passed.
    #[test]
    fn randomized_solver_gives_the_same_answer_twice() {
        assert!(600 > 500, "n must cross the Auto threshold or this tests power iteration");
        assert_eq!(first_component(600, DEFAULT_PCA_SEED), first_component(600, DEFAULT_PCA_SEED));
    }

    /// Without this, the test above would still pass if `seed` were dropped on
    /// the floor and the projection hard-coded. It asserts the seed is load
    /// bearing, not merely present.
    #[test]
    fn a_different_seed_draws_a_different_projection() {
        let a = first_component(600, DEFAULT_PCA_SEED);
        let b = first_component(600, DEFAULT_PCA_SEED ^ 0xFFFF);
        assert_ne!(a, b, "seed is ignored: two seeds gave byte-identical output");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pca_basic() {
        // 2D data with clear primary direction along x=y
        let data: Vec<Vec<f64>> = (0..100)
            .map(|i| {
                let x = i as f64;
                let y = x + (i as f64 * 0.1).sin() * 2.0; // strong correlation
                vec![x, y]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 2,
            ..Default::default()
        });

        assert_eq!(result.components.len(), 2);
        assert_eq!(result.explained_variance.len(), 2);

        // First component should capture most variance
        assert!(result.explained_variance_ratio[0] > 0.9,
            "First component should explain >90% variance, got {}",
            result.explained_variance_ratio[0]);

        // First component should be roughly along [1, 1] / sqrt(2) direction
        let c0 = &result.components[0];
        let angle = (c0[0].abs() - c0[1].abs()).abs();
        assert!(angle < 0.2, "First component should be near diagonal, got {:?}", c0);
    }

    #[test]
    fn test_pca_identity() {
        // When n_components == n_features, explained_variance_ratio sums to ~1.0
        let data: Vec<Vec<f64>> = (0..50)
            .map(|i| {
                vec![
                    i as f64,
                    (i as f64 * 0.5).sin() * 10.0,
                    (i * i) as f64 % 17.0,
                ]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 3,
            ..Default::default()
        });

        let total: f64 = result.explained_variance_ratio.iter().sum();
        assert!((total - 1.0).abs() < 0.01,
            "Total explained variance ratio should be ~1.0, got {}", total);
    }

    #[test]
    fn test_pca_explained_variance_ratios_sum() {
        let data: Vec<Vec<f64>> = (0..200)
            .map(|i| {
                let x = i as f64 * 0.1;
                vec![x, x * 2.0 + 1.0, x.sin(), x.cos(), (x * 0.3).exp().min(100.0)]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 5,
            ..Default::default()
        });

        let total: f64 = result.explained_variance_ratio.iter().sum();
        assert!((total - 1.0).abs() < 0.05,
            "Ratios should sum to ~1.0, got {}", total);

        // Ratios should be in descending order
        for i in 1..result.explained_variance_ratio.len() {
            assert!(result.explained_variance_ratio[i] <= result.explained_variance_ratio[i - 1] + 1e-10,
                "Ratios should be descending");
        }
    }

    #[test]
    fn test_pca_transform() {
        // Create correlated 3D data
        let data: Vec<Vec<f64>> = (0..100)
            .map(|i| {
                let x = i as f64;
                vec![x, x * 1.5 + 3.0, x * 0.8 - 2.0]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 1,
            ..Default::default()
        });

        // Project and check that reconstructing from 1 component preserves most info
        let projected = result.transform(&data);
        assert_eq!(projected.len(), 100);
        assert_eq!(projected[0].len(), 1);

        // First component should explain nearly all variance (perfectly correlated data)
        assert!(result.explained_variance_ratio[0] > 0.99,
            "Should explain >99% variance for perfectly correlated data, got {}",
            result.explained_variance_ratio[0]);
    }

    #[test]
    fn test_pca_centering() {
        // Data with large offset should give same components as centered data
        let offset = 1000.0;
        let data: Vec<Vec<f64>> = (0..50)
            .map(|i| vec![i as f64 + offset, (i as f64 * 2.0) + offset])
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 2,
            center: true,
            ..Default::default()
        });

        // Mean should be approximately offset + 24.5 for x, offset + 49 for y
        assert!((result.mean[0] - (offset + 24.5)).abs() < 0.01);
        assert!((result.mean[1] - (offset + 49.0)).abs() < 0.01);

        // Components should still capture the correlation direction
        assert!(result.explained_variance_ratio[0] > 0.99);
    }

    #[test]
    fn test_pca_convergence() {
        // Simple data that should converge quickly
        let data: Vec<Vec<f64>> = (0..20)
            .map(|i| vec![i as f64, 0.0])
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 1,
            max_iterations: 100,
            tolerance: 1e-10,
            solver: PcaSolver::PowerIteration,
            ..Default::default()
        });

        // Should converge well before 100 iterations for such simple data
        assert!(result.iterations_used < 50,
            "Should converge quickly, used {} iterations", result.iterations_used);

        // First component should be [1, 0] (all variance along x)
        let c0 = &result.components[0];
        assert!(c0[0].abs() > 0.99, "Should be along x axis, got {:?}", c0);
        assert!(c0[1].abs() < 0.1, "Should have near-zero y component, got {:?}", c0);
    }

    #[test]
    fn test_pca_scaling() {
        // Two features with very different scales
        let data: Vec<Vec<f64>> = (0..100)
            .map(|i| vec![i as f64, i as f64 * 1000.0])
            .collect();

        // Without scaling, second feature dominates
        let result_no_scale = pca(&data, PcaConfig {
            n_components: 2,
            scale: false,
            ..Default::default()
        });
        // First component should align with second feature (larger variance)
        assert!(result_no_scale.components[0][1].abs() > result_no_scale.components[0][0].abs());

        // With scaling, features are treated equally
        let result_scaled = pca(&data, PcaConfig {
            n_components: 2,
            scale: true,
            ..Default::default()
        });
        // Both features should contribute roughly equally to first component
        let ratio = result_scaled.components[0][0].abs() / result_scaled.components[0][1].abs();
        assert!(ratio > 0.5 && ratio < 2.0,
            "Scaled components should be balanced, ratio = {}", ratio);
    }

    // ── New tests for randomized SVD ──

    #[test]
    fn test_pca_randomized_basic() {
        // Generate data large enough to trigger randomized solver via Auto
        let data: Vec<Vec<f64>> = (0..600)
            .map(|i| {
                let x = i as f64;
                vec![x, x * 2.0 + 1.0, x.sin() * 10.0, (x * 0.01).cos() * 5.0]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 2,
            solver: PcaSolver::Randomized { n_oversamples: 10, n_power_iters: 4 },
            ..Default::default()
        });

        assert_eq!(result.components.len(), 2);
        // First component should capture most variance (x and 2x dominate)
        assert!(result.explained_variance_ratio[0] > 0.8,
            "Randomized SVD: first component should explain >80% variance, got {}",
            result.explained_variance_ratio[0]);
    }

    #[test]
    fn test_pca_randomized_orthogonality() {
        // Verify components from randomized SVD are orthogonal
        let data: Vec<Vec<f64>> = (0..600)
            .map(|i| {
                let x = i as f64 * 0.1;
                vec![x, x * 2.0 + 1.0, x.sin() * 10.0, x.cos() * 5.0, (x * 0.3).exp().min(100.0)]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 4,
            solver: PcaSolver::Randomized { n_oversamples: 10, n_power_iters: 4 },
            ..Default::default()
        });

        // Check pairwise orthogonality
        for i in 0..result.components.len() {
            for j in (i + 1)..result.components.len() {
                let dot: f64 = result.components[i].iter()
                    .zip(result.components[j].iter())
                    .map(|(a, b)| a * b)
                    .sum();
                assert!(dot.abs() < 0.05,
                    "Components {} and {} should be orthogonal, dot product = {}", i, j, dot);
            }
        }
    }

    #[test]
    fn test_pca_auto_selects_randomized() {
        // n=600, d=4, k=2 → k < 0.8 * min(600,4) = 3.2 → randomized
        let data: Vec<Vec<f64>> = (0..600)
            .map(|i| {
                let x = i as f64;
                vec![x, x * 2.0, x.sin(), x.cos()]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 2,
            solver: PcaSolver::Auto,
            ..Default::default()
        });

        // iterations_used should be the n_power_iters (4) for randomized, not >4
        // (Power iteration would use many more iterations)
        assert!(result.iterations_used <= 10,
            "Auto should select randomized (iters={})", result.iterations_used);
        assert_eq!(result.components.len(), 2);
    }

    #[test]
    fn test_pca_solver_backward_compat() {
        // Verify PowerIteration still works identically to before
        let data: Vec<Vec<f64>> = (0..100)
            .map(|i| {
                let x = i as f64;
                vec![x, x * 1.5 + 3.0, x * 0.8 - 2.0]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 2,
            solver: PcaSolver::PowerIteration,
            ..Default::default()
        });

        assert_eq!(result.components.len(), 2);
        assert!(result.explained_variance_ratio[0] > 0.99,
            "PowerIteration should explain >99% on correlated data, got {}",
            result.explained_variance_ratio[0]);

        // Verify orthogonality of components (improved by Gram-Schmidt)
        let dot: f64 = result.components[0].iter()
            .zip(result.components[1].iter())
            .map(|(a, b)| a * b)
            .sum();
        assert!(dot.abs() < 0.01,
            "PowerIteration components should be orthogonal, dot = {}", dot);
    }

    #[test]
    fn test_pca_randomized_variance_sum() {
        // Variance ratios from randomized SVD should sum close to 1.0 when k = d
        let data: Vec<Vec<f64>> = (0..600)
            .map(|i| {
                let x = i as f64 * 0.1;
                vec![x, x.sin() * 10.0, (x * 0.5).cos() * 5.0]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 3,
            solver: PcaSolver::Randomized { n_oversamples: 10, n_power_iters: 4 },
            ..Default::default()
        });

        let total: f64 = result.explained_variance_ratio.iter().sum();
        assert!((total - 1.0).abs() < 0.1,
            "Randomized SVD ratios should sum close to 1.0, got {}", total);
    }

    #[test]
    fn test_pca_transform_batch() {
        // Verify that batch transform matches individual transform_one calls
        let data: Vec<Vec<f64>> = (0..100)
            .map(|i| {
                let x = i as f64;
                vec![x, x * 2.0 + 1.0, x.sin() * 3.0]
            })
            .collect();

        let result = pca(&data, PcaConfig {
            n_components: 2,
            ..Default::default()
        });

        let batch = result.transform(&data);
        for (i, row) in data.iter().enumerate() {
            let single = result.transform_one(row);
            for (j, (&b, &s)) in batch[i].iter().zip(single.iter()).enumerate() {
                assert!((b - s).abs() < 1e-10,
                    "Batch[{}][{}]={} != single={}", i, j, b, s);
            }
        }
    }
}
