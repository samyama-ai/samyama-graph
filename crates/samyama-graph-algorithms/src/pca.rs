//! Principal Component Analysis (PCA) via power iteration
//!
//! Implements dimensionality reduction for node feature matrices.
//! Uses the deflation method: extract one eigenvector at a time from the
//! covariance matrix using power iteration, then deflate and repeat.

use ndarray::{Array1, Array2};

/// PCA configuration
pub struct PcaConfig {
    /// Number of principal components to extract
    pub n_components: usize,
    /// Maximum power iterations per component (default: 100)
    pub max_iterations: usize,
    /// Convergence tolerance for power iteration (default: 1e-6)
    pub tolerance: f64,
    /// Subtract column means before PCA (default: true)
    pub center: bool,
    /// Divide by column std dev before PCA (default: false)
    pub scale: bool,
}

impl Default for PcaConfig {
    fn default() -> Self {
        Self {
            n_components: 2,
            max_iterations: 100,
            tolerance: 1e-6,
            center: true,
            scale: false,
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
    /// Number of power iterations used for the last component
    pub iterations_used: usize,
}

impl PcaResult {
    /// Project multiple data points into the reduced PCA space.
    ///
    /// Each input row is centered (and optionally scaled) using the stored
    /// mean/std_dev, then multiplied by the component matrix.
    pub fn transform(&self, data: &[Vec<f64>]) -> Vec<Vec<f64>> {
        data.iter().map(|row| self.transform_one(row)).collect()
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
                if self.std_dev[j] > 0.0 {
                    val /= self.std_dev[j];
                }
                dot += val * component[j];
            }
            result[c] = dot;
        }
        result
    }
}

/// Run PCA on a feature matrix using power iteration with deflation.
///
/// # Arguments
/// - `data`: slice of rows, each row is a feature vector of equal length
/// - `config`: PCA parameters
///
/// # Panics
/// Panics if `data` is empty or rows have inconsistent lengths.
pub fn pca(data: &[Vec<f64>], config: PcaConfig) -> PcaResult {
    let n = data.len();
    assert!(n > 0, "PCA requires at least one data point");
    let d = data[0].len();
    assert!(d > 0, "PCA requires at least one feature");

    let k = config.n_components.min(d).min(n);

    // Build ndarray matrix
    let mut mat = Array2::<f64>::zeros((n, d));
    for (i, row) in data.iter().enumerate() {
        assert_eq!(row.len(), d, "All rows must have the same number of features");
        for (j, &val) in row.iter().enumerate() {
            mat[[i, j]] = val;
        }
    }

    // Compute column means
    let mut mean = vec![0.0; d];
    if config.center {
        for j in 0..d {
            let mut s = 0.0;
            for i in 0..n {
                s += mat[[i, j]];
            }
            mean[j] = s / n as f64;
        }
        // Center the data
        for i in 0..n {
            for j in 0..d {
                mat[[i, j]] -= mean[j];
            }
        }
    }

    // Compute column std devs and scale
    let mut std_dev = vec![1.0; d];
    if config.scale {
        for j in 0..d {
            let mut ss = 0.0;
            for i in 0..n {
                ss += mat[[i, j]] * mat[[i, j]];
            }
            let s = (ss / (n.max(2) - 1) as f64).sqrt();
            std_dev[j] = s;
            if s > 0.0 {
                for i in 0..n {
                    mat[[i, j]] /= s;
                }
            }
        }
    }

    // Compute covariance matrix: C = X^T X / (n-1)
    let xt = mat.t();
    let denom = if n > 1 { (n - 1) as f64 } else { 1.0 };
    let mut cov = xt.dot(&mat) / denom;

    // Power iteration with deflation
    let mut components: Vec<Vec<f64>> = Vec::with_capacity(k);
    let mut eigenvalues: Vec<f64> = Vec::with_capacity(k);
    let mut last_iters = 0;

    for _ in 0..k {
        let (eigvec, eigval, iters) = power_iteration(&cov, config.max_iterations, config.tolerance);
        last_iters = iters;

        // Deflate: C = C - lambda * v * v^T
        for r in 0..d {
            for c in 0..d {
                cov[[r, c]] -= eigval * eigvec[r] * eigvec[c];
            }
        }

        components.push(eigvec);
        eigenvalues.push(eigval);
    }

    // Compute explained variance ratios
    let total_variance: f64 = eigenvalues.iter().sum::<f64>()
        + (0..d).map(|i| cov[[i, i]]).sum::<f64>(); // remaining diagonal = remaining eigenvalues
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
}
