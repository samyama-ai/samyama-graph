//! GPU-accelerated PageRank
//!
//! Dispatches PageRank iterations as wgpu compute shaders.
//! Each iteration is a single kernel launch with one thread per node.
//! Accepts raw CSR arrays to avoid circular dependency on samyama-graph-algorithms.

use crate::buffer::{
    self, create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer, download_f32,
};
use crate::error::GpuError;

const SHADER_SOURCE: &str = include_str!("shaders/pagerank.wgsl");
const WORKGROUP_SIZE: u32 = 256;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct PagerankParams {
    node_count: u32,
    damping: f32,
    base_score: f32,
    _padding: u32,
}

/// PageRank configuration for GPU
pub struct GpuPageRankConfig {
    pub damping_factor: f64,
    pub iterations: usize,
    /// Convergence tolerance (0.0 = use fixed iteration count)
    pub tolerance: f64,
    /// How often to check convergence (every N iterations)
    pub check_interval: usize,
}

/// Run PageRank on the GPU using raw CSR data
///
/// Returns f32 scores indexed by dense node index (0..node_count).
/// When the `cuda` feature is enabled, tries CUDA first for NVIDIA GPUs.
pub fn gpu_page_rank(
    node_count: usize,
    in_offsets: &[usize],
    in_sources: &[usize],
    out_offsets: &[usize],
    config: &GpuPageRankConfig,
) -> Result<Vec<f64>, GpuError> {
    if node_count == 0 {
        return Ok(Vec::new());
    }

    // Use CUDA if runtime selected it as the active backend
    #[cfg(feature = "cuda")]
    if let Some(rt) = crate::runtime::GpuRuntime::get() {
        if let Some(cuda_ctx) = rt.cuda() {
            match crate::cuda::pagerank::cuda_page_rank(
                cuda_ctx,
                node_count,
                in_offsets,
                in_sources,
                out_offsets,
                config,
            ) {
                Ok(scores) => {
                    tracing::debug!("PageRank: {} backend ({} nodes)", rt.backend, node_count);
                    return Ok(scores);
                }
                Err(e) => {
                    tracing::warn!("CUDA PageRank failed, falling back to wgpu: {}", e);
                }
            }
        }
    }

    // wgpu fallback: source the wgpu context from the runtime (F1 fix). None on
    // headless/CUDA-only hosts -> the caller falls back to CPU. `init()` is idempotent.
    let ctx = match crate::runtime::GpuRuntime::init().wgpu() {
        Some(c) => c,
        None => return Err(GpuError::NoAdapter),
    };

    // Check buffer sizes against GPU limits
    let max_buf = ctx.device.limits().max_buffer_size as usize;
    let largest_buf = in_sources.len() * std::mem::size_of::<u32>();
    if largest_buf > max_buf {
        return Err(GpuError::DataTooLarge {
            requested: largest_buf,
            available: max_buf,
        });
    }

    // Convert to u32 for GPU
    let in_offsets_u32: Vec<u32> = in_offsets.iter().map(|&x| x as u32).collect();
    let in_sources_u32: Vec<u32> = in_sources.iter().map(|&x| x as u32).collect();
    let out_degrees_u32: Vec<u32> = (0..node_count)
        .map(|i| (out_offsets[i + 1] - out_offsets[i]) as u32)
        .collect();

    let in_offsets_buf =
        create_storage_buffer(ctx, "pr_in_offsets", bytemuck::cast_slice(&in_offsets_u32));
    let in_sources_buf =
        create_storage_buffer(ctx, "pr_in_sources", bytemuck::cast_slice(&in_sources_u32));
    let out_degrees_buf = create_storage_buffer(
        ctx,
        "pr_out_degrees",
        bytemuck::cast_slice(&out_degrees_u32),
    );

    // Initialize scores (1/N per LDBC Graphalytics spec)
    let initial_score = 1.0 / node_count as f32;
    let initial_scores: Vec<f32> = vec![initial_score; node_count];
    let scores_buf =
        create_storage_buffer_rw(ctx, "scores_a", bytemuck::cast_slice(&initial_scores));
    let next_scores_buf =
        create_storage_buffer_rw(ctx, "scores_b", bytemuck::cast_slice(&initial_scores));

    let params = PagerankParams {
        node_count: node_count as u32,
        damping: config.damping_factor as f32,
        base_score: (1.0 - config.damping_factor) as f32 / node_count as f32,
        _padding: 0,
    };
    let params_buf = create_uniform_buffer(ctx, "pr_params", bytemuck::bytes_of(&params));

    let pipeline = ctx.create_compute_pipeline("pagerank", SHADER_SOURCE, "pagerank_iter");
    let workgroup_count = (node_count as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;

    let check_interval = if config.check_interval > 0 {
        config.check_interval
    } else {
        10
    };
    let use_tolerance = config.tolerance > 0.0;
    let mut actual_iters = 0usize;

    for iter in 0..config.iterations {
        let (read_buf, write_buf) = if iter % 2 == 0 {
            (&scores_buf, &next_scores_buf)
        } else {
            (&next_scores_buf, &scores_buf)
        };

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("pr_bind_group"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: in_offsets_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: in_sources_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: out_degrees_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: read_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 4,
                    resource: write_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 5,
                    resource: params_buf.as_entire_binding(),
                },
            ],
        });

        buffer::dispatch_compute(ctx, &pipeline, &bind_group, workgroup_count);
        actual_iters = iter + 1;

        // Tolerance-based convergence check (download scores to CPU periodically)
        if use_tolerance && (iter + 1) % check_interval == 0 {
            let cur_scores = download_f32(ctx, write_buf, node_count)?;
            let prev_scores = download_f32(ctx, read_buf, node_count)?;
            let total_diff: f64 = cur_scores
                .iter()
                .zip(prev_scores.iter())
                .map(|(a, b)| (*a as f64 - *b as f64).abs())
                .sum();
            if total_diff < config.tolerance {
                break;
            }
        }
    }

    let final_buf = if actual_iters % 2 == 0 {
        &scores_buf
    } else {
        &next_scores_buf
    };
    let scores_f32 = download_f32(ctx, final_buf, node_count)?;

    Ok(scores_f32.into_iter().map(|s| s as f64).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_pagerank_small() {
        if !crate::gpu_available() {
            println!("No GPU, skipping test");
            return;
        }

        // 3-node chain: 0 -> 1 -> 2
        let out_offsets = vec![0, 1, 2, 2]; // node 0: [1], node 1: [2], node 2: []
        let in_offsets = vec![0, 0, 1, 2]; // node 0: [], node 1: [0], node 2: [1]
        let in_sources = vec![0, 1];

        let config = GpuPageRankConfig {
            damping_factor: 0.85,
            iterations: 20,
            tolerance: 0.0,
            check_interval: 0,
        };
        let scores =
            gpu_page_rank(3, &in_offsets, &in_sources, &out_offsets, &config).unwrap();

        assert_eq!(scores.len(), 3);
        // Node 2 (sink) should have highest score
        assert!(scores[2] > scores[0], "Sink node should have higher score");
    }
}
