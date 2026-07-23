//! GPU-accelerated PCA via power iteration
//!
//! Multi-stage kernel pipeline:
//! 1. Compute column-wise means (pca_mean.wgsl)
//! 2. Center data in-place (pca_center.wgsl)
//! 3. Compute covariance matrix (pca_covariance.wgsl) — tiled for cache efficiency
//! 4. Power iteration per component (pca_power_iter_norm.wgsl) with fused normalize
//! 5. Deflation on CPU (covariance matrix is small: features x features)
//!
//! Optimizations over v0.5.11:
//! - Pre-allocated ping-pong buffers (no per-iteration allocation)
//! - Batched iterations with periodic convergence checks (~20 dispatches vs ~400)
//! - Fused mat-vec + normalize shader (eliminates CPU normalization round-trip)
//! - Tiled covariance shader (improved GPU occupancy)

use crate::buffer::{
    create_empty_buffer_rw, create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer,
    dispatch_compute, download_f32,
};
use crate::context::GpuContext;
use crate::error::GpuError;

/// Create a storage buffer that can be read, written, and copied into.
/// Needed for ping-pong iteration where we copy updated vectors back.
fn create_rw_copy_dst_buffer(ctx: &GpuContext, label: &str, data: &[u8]) -> wgpu::Buffer {
    use wgpu::util::DeviceExt;
    ctx.device
        .create_buffer_init(&wgpu::util::BufferInitDescriptor {
            label: Some(label),
            contents: data,
            usage: wgpu::BufferUsages::STORAGE
                | wgpu::BufferUsages::COPY_SRC
                | wgpu::BufferUsages::COPY_DST,
        })
}

const MEAN_SHADER: &str = include_str!("shaders/pca_mean.wgsl");
const CENTER_SHADER: &str = include_str!("shaders/pca_center.wgsl");
const COV_SHADER: &str = include_str!("shaders/pca_covariance.wgsl");
const POWER_ITER_NORM_SHADER: &str = include_str!("shaders/pca_power_iter_norm.wgsl");
const WORKGROUP_SIZE: u32 = 256;

/// GPU PCA configuration
pub struct GpuPcaConfig {
    pub n_components: usize,
    pub max_iterations: usize,
    pub tolerance: f64,
    /// How often to download vectors to check convergence (default: 10)
    pub check_interval: usize,
}

/// GPU PCA result
pub struct GpuPcaResult {
    /// Principal component vectors (n_components x n_features)
    pub components: Vec<Vec<f32>>,
    /// Eigenvalues (variance explained per component)
    pub eigenvalues: Vec<f32>,
    /// Mean vector (n_features)
    pub mean: Vec<f32>,
    pub n_samples: usize,
    pub n_features: usize,
    pub iterations_used: usize,
}

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct MeanParams {
    n_samples: u32,
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
}

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct CenterParams {
    n_samples: u32,
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
}

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct CovParams {
    n_samples: u32,
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
}

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct PowerIterNormParams {
    n_features: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

/// Run PCA on the GPU
///
/// # Arguments
/// - `data`: flat row-major f32 array of shape n_samples x n_features
/// - `n_samples`: number of data points
/// - `n_features`: number of features per data point
pub fn gpu_pca(
    ctx: &GpuContext,
    data: &[f32],
    n_samples: usize,
    n_features: usize,
    config: &GpuPcaConfig,
) -> Result<GpuPcaResult, GpuError> {
    assert_eq!(data.len(), n_samples * n_features);

    if n_samples == 0 || n_features == 0 {
        return Ok(GpuPcaResult {
            components: vec![],
            eigenvalues: vec![],
            mean: vec![],
            n_samples,
            n_features,
            iterations_used: 0,
        });
    }

    let k = config.n_components.min(n_features).min(n_samples);
    let check_interval = if config.check_interval > 0 {
        config.check_interval
    } else {
        10
    };

    // Check buffer size limits
    let max_buf = ctx.device.limits().max_buffer_size as usize;
    let data_size = data.len() * std::mem::size_of::<f32>();
    if data_size > max_buf {
        return Err(GpuError::DataTooLarge {
            requested: data_size,
            available: max_buf,
        });
    }

    // ── Stage 1: Upload data and compute means ──
    let data_buf = create_storage_buffer_rw(ctx, "pca_data", bytemuck::cast_slice(data));
    let mean_buf = create_empty_buffer_rw(ctx, "pca_mean", (n_features * 4) as u64);

    let mean_params = MeanParams {
        n_samples: n_samples as u32,
        n_features: n_features as u32,
        _pad1: 0,
        _pad2: 0,
    };
    let mean_params_buf =
        create_uniform_buffer(ctx, "pca_mean_params", bytemuck::bytes_of(&mean_params));

    let mean_pipeline = ctx.create_compute_pipeline("pca_mean", MEAN_SHADER, "compute_mean");
    let mean_bg_layout = mean_pipeline.get_bind_group_layout(0);
    let mean_bg = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("pca_mean_bg"),
        layout: &mean_bg_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: data_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: mean_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: mean_params_buf.as_entire_binding(),
            },
        ],
    });
    // One workgroup per feature
    dispatch_compute(ctx, &mean_pipeline, &mean_bg, n_features as u32);

    // Download means for the result
    let mean_vec = download_f32(ctx, &mean_buf, n_features)?;

    // ── Stage 2: Center data in-place on GPU ──
    let center_params = CenterParams {
        n_samples: n_samples as u32,
        n_features: n_features as u32,
        _pad1: 0,
        _pad2: 0,
    };
    let center_params_buf =
        create_uniform_buffer(ctx, "pca_center_params", bytemuck::bytes_of(&center_params));

    let center_pipeline = ctx.create_compute_pipeline("pca_center", CENTER_SHADER, "center_data");
    let center_bg_layout = center_pipeline.get_bind_group_layout(0);
    let center_bg = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("pca_center_bg"),
        layout: &center_bg_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: data_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: mean_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: center_params_buf.as_entire_binding(),
            },
        ],
    });
    let total_elements = (n_samples * n_features) as u32;
    let center_wg = (total_elements + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;
    dispatch_compute(ctx, &center_pipeline, &center_bg, center_wg);

    // ── Stage 3: Compute covariance matrix (tiled shader) ──
    let cov_size = n_features * n_features;
    let cov_buf = create_empty_buffer_rw(ctx, "pca_cov", (cov_size * 4) as u64);

    let cov_params = CovParams {
        n_samples: n_samples as u32,
        n_features: n_features as u32,
        _pad1: 0,
        _pad2: 0,
    };
    let cov_params_buf =
        create_uniform_buffer(ctx, "pca_cov_params", bytemuck::bytes_of(&cov_params));

    let cov_pipeline = ctx.create_compute_pipeline("pca_cov", COV_SHADER, "compute_covariance");
    let cov_bg_layout = cov_pipeline.get_bind_group_layout(0);
    let cov_bg = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("pca_cov_bg"),
        layout: &cov_bg_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: data_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: cov_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: cov_params_buf.as_entire_binding(),
            },
        ],
    });
    let cov_wg = (cov_size as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;
    dispatch_compute(ctx, &cov_pipeline, &cov_bg, cov_wg);

    // Download covariance matrix to CPU for deflation
    // The covariance matrix is features x features (small), so doing deflation
    // on the CPU is efficient and avoids complex GPU synchronization.
    let mut cov_data = download_f32(ctx, &cov_buf, cov_size)?;

    // ── Stage 4: Power iteration with fused normalize shader ──
    // Pre-create the pipeline once (shared across all components)
    let pi_norm_params = PowerIterNormParams {
        n_features: n_features as u32,
        _pad1: 0,
        _pad2: 0,
        _pad3: 0,
    };
    let pi_norm_params_buf =
        create_uniform_buffer(ctx, "pca_pin_params", bytemuck::bytes_of(&pi_norm_params));
    let pi_norm_pipeline =
        ctx.create_compute_pipeline("pca_pi_norm", POWER_ITER_NORM_SHADER, "power_iter_norm");
    let pi_wg = (n_features as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;

    // Pre-allocate ping-pong buffers ONCE (reused across components)
    let initial_v: Vec<f32> = {
        let mut v: Vec<f32> = (0..n_features).map(|i| ((i + 1) as f32).sqrt()).collect();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        v
    };
    let zeros = vec![0.0f32; n_features];
    let v_a = create_rw_copy_dst_buffer(ctx, "pca_v_a", bytemuck::cast_slice(&initial_v));
    let v_b = create_rw_copy_dst_buffer(ctx, "pca_v_b", bytemuck::cast_slice(&zeros));

    let mut components: Vec<Vec<f32>> = Vec::with_capacity(k);
    let mut eigenvalues: Vec<f32> = Vec::with_capacity(k);
    let mut last_iters = 0usize;

    for _comp in 0..k {
        // Upload current covariance matrix for this component
        let cov_gpu = create_storage_buffer(ctx, "pca_cov_iter", bytemuck::cast_slice(&cov_data));

        // Reset v_a to initial vector for each component
        ctx.queue
            .write_buffer(&v_a, 0, bytemuck::cast_slice(&initial_v));

        // Create bind groups for ping-pong: A→B and B→A
        let pi_bg_layout = pi_norm_pipeline.get_bind_group_layout(0);

        let bg_a_to_b = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("pca_pi_a2b"),
            layout: &pi_bg_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: cov_gpu.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: v_a.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: v_b.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: pi_norm_params_buf.as_entire_binding(),
                },
            ],
        });

        let bg_b_to_a = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("pca_pi_b2a"),
            layout: &pi_bg_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: cov_gpu.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: v_b.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: v_a.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: pi_norm_params_buf.as_entire_binding(),
                },
            ],
        });

        let mut iters = 0usize;
        let mut converged = false;

        // Batched iterations: dispatch `check_interval` iterations, then check convergence
        while iters < config.max_iterations && !converged {
            let batch_end = (iters + check_interval).min(config.max_iterations);
            let batch_size = batch_end - iters;

            // Dispatch a batch of iterations without CPU round-trips
            for step in 0..batch_size {
                let iter_idx = iters + step;
                if iter_idx % 2 == 0 {
                    dispatch_compute(ctx, &pi_norm_pipeline, &bg_a_to_b, pi_wg);
                } else {
                    dispatch_compute(ctx, &pi_norm_pipeline, &bg_b_to_a, pi_wg);
                }
            }
            iters = batch_end;

            // Download current vector to check convergence
            if config.tolerance > 0.0 {
                // Result is in v_b if iters is even, v_a if odd (due to ping-pong)
                let (current_buf, prev_buf) = if iters % 2 == 0 {
                    (&v_a, &v_b)
                } else {
                    (&v_b, &v_a)
                };

                let v_new = download_f32(ctx, current_buf, n_features)?;
                let v_old = download_f32(ctx, prev_buf, n_features)?;

                let w_norm: f32 = v_new.iter().map(|x| x * x).sum::<f32>().sqrt();
                if w_norm < 1e-15 {
                    converged = true;
                } else {
                    let diff_pos: f32 = v_new
                        .iter()
                        .zip(v_old.iter())
                        .map(|(a, b)| (a - b).powi(2))
                        .sum();
                    let diff_neg: f32 = v_new
                        .iter()
                        .zip(v_old.iter())
                        .map(|(a, b)| (a + b).powi(2))
                        .sum();
                    let diff = diff_pos.min(diff_neg).sqrt();
                    if (diff as f64) < config.tolerance {
                        converged = true;
                    }
                }
            }
        }

        last_iters = iters;

        // Download final eigenvector (from whichever buffer holds the result)
        let eigvec = if iters % 2 == 0 {
            download_f32(ctx, &v_a, n_features)?
        } else {
            download_f32(ctx, &v_b, n_features)?
        };

        // Compute eigenvalue: v^T @ C @ v (on CPU, covariance is small)
        let mut cv = vec![0.0f32; n_features];
        for r in 0..n_features {
            for c in 0..n_features {
                cv[r] += cov_data[r * n_features + c] * eigvec[c];
            }
        }
        let eigenvalue: f32 = eigvec.iter().zip(cv.iter()).map(|(a, b)| a * b).sum();

        // Deflate covariance matrix on CPU: C = C - lambda * v @ v^T
        for r in 0..n_features {
            for c in 0..n_features {
                cov_data[r * n_features + c] -= eigenvalue * eigvec[r] * eigvec[c];
            }
        }

        components.push(eigvec);
        eigenvalues.push(eigenvalue);
    }

    Ok(GpuPcaResult {
        components,
        eigenvalues,
        mean: mean_vec,
        n_samples,
        n_features,
        iterations_used: last_iters,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_pca_basic() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        // 100 points in 3D with clear primary direction along x=y=z
        let n = 100;
        let d = 3;
        let mut data = Vec::with_capacity(n * d);
        for i in 0..n {
            let x = i as f32;
            data.push(x);
            data.push(x * 1.5 + 3.0);
            data.push(x * 0.8 - 2.0);
        }

        let config = GpuPcaConfig {
            n_components: 2,
            max_iterations: 100,
            tolerance: 1e-6,
            check_interval: 10,
        };

        let result = gpu_pca(&ctx, &data, n, d, &config).unwrap();
        assert_eq!(result.components.len(), 2);
        assert_eq!(result.eigenvalues.len(), 2);

        // First eigenvalue should be much larger than second (nearly collinear data)
        assert!(
            result.eigenvalues[0] > result.eigenvalues[1] * 10.0,
            "First eigenvalue should dominate: {} vs {}",
            result.eigenvalues[0],
            result.eigenvalues[1]
        );
    }

    #[test]
    fn test_gpu_pca_empty() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        let config = GpuPcaConfig {
            n_components: 2,
            max_iterations: 100,
            tolerance: 1e-6,
            check_interval: 10,
        };

        let result = gpu_pca(&ctx, &[], 0, 0, &config).unwrap();
        assert!(result.components.is_empty());
    }

    #[test]
    fn test_gpu_pca_convergence_batched() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        // Simple data that should converge quickly with batched checks
        let n = 50;
        let d = 2;
        let mut data = Vec::with_capacity(n * d);
        for i in 0..n {
            let x = i as f32;
            data.push(x);
            data.push(0.0); // all variance along first axis
        }

        let config = GpuPcaConfig {
            n_components: 1,
            max_iterations: 100,
            tolerance: 1e-6,
            check_interval: 5, // check every 5 iterations
        };

        let result = gpu_pca(&ctx, &data, n, d, &config).unwrap();
        assert_eq!(result.components.len(), 1);
        // Should converge in well under 100 iterations
        assert!(
            result.iterations_used <= 50,
            "Should converge quickly with batched checks, used {} iterations",
            result.iterations_used
        );
    }
}
