//! CUDA-accelerated PageRank

use super::{csr_to_device, download_f32, upload_f32, CudaGpuContext};
use crate::error::GpuError;
use crate::pagerank::GpuPageRankConfig;
use cudarc::driver::{LaunchAsync, LaunchConfig};

const BLOCK_SIZE: u32 = 256;

/// Run PageRank on CUDA using native NVIDIA kernels
pub fn cuda_page_rank(
    ctx: &CudaGpuContext,
    node_count: usize,
    in_offsets: &[usize],
    in_sources: &[usize],
    out_offsets: &[usize],
    config: &GpuPageRankConfig,
) -> Result<Vec<f64>, GpuError> {
    if node_count == 0 {
        return Ok(Vec::new());
    }

    let dev = &ctx.dev;

    // Convert to u32 for GPU
    let in_offsets_u32: Vec<u32> = in_offsets.iter().map(|&x| x as u32).collect();
    let in_sources_u32: Vec<u32> = in_sources.iter().map(|&x| x as u32).collect();
    let out_degrees_u32: Vec<u32> = (0..node_count)
        .map(|i| (out_offsets[i + 1] - out_offsets[i]) as u32)
        .collect();

    // Move CSR to the device — managed (no copy) under SAMYAMA_GPU_UM, else explicit copy.
    let d_in_offsets = csr_to_device(dev, &in_offsets_u32)?;
    let d_in_sources = csr_to_device(dev, &in_sources_u32)?;
    let d_out_degrees = csr_to_device(dev, &out_degrees_u32)?;

    // Initialize scores (1/N)
    let initial_score = 1.0f32 / node_count as f32;
    let initial_scores = vec![initial_score; node_count];
    let mut d_scores = upload_f32(dev, &initial_scores)?;
    let mut d_next_scores = upload_f32(dev, &initial_scores)?;

    let n = node_count as u32;
    let damping = config.damping_factor as f32;
    let base_score = (1.0 - config.damping_factor) as f32 / node_count as f32;
    let grid = CudaGpuContext::grid_size(n);
    let cfg = LaunchConfig {
        grid_dim: (grid, 1, 1),
        block_dim: (BLOCK_SIZE, 1, 1),
        shared_mem_bytes: 0,
    };

    let func = dev
        .get_func("pagerank", "pagerank_iter")
        .ok_or_else(|| GpuError::ShaderError("pagerank_iter not found".into()))?;

    let check_interval = if config.check_interval > 0 {
        config.check_interval
    } else {
        10
    };
    let use_tolerance = config.tolerance > 0.0;
    let mut actual_iters = 0usize;

    for iter in 0..config.iterations {
        let (read, write) = if iter % 2 == 0 {
            (&d_scores, &mut d_next_scores)
        } else {
            (&d_next_scores, &mut d_scores)
        };

        unsafe {
            func.clone().launch(
                cfg,
                (
                    &d_in_offsets,
                    &d_in_sources,
                    &d_out_degrees,
                    read,
                    write,
                    n,
                    damping,
                    base_score,
                ),
            )
        }
        .map_err(|e| GpuError::ShaderError(format!("pagerank launch: {}", e)))?;

        actual_iters = iter + 1;

        if use_tolerance && actual_iters % check_interval == 0 {
            let cur = download_f32(
                dev,
                if iter % 2 == 0 {
                    &d_next_scores
                } else {
                    &d_scores
                },
            )?;
            let prev = download_f32(
                dev,
                if iter % 2 == 0 {
                    &d_scores
                } else {
                    &d_next_scores
                },
            )?;
            let diff: f64 = cur
                .iter()
                .zip(prev.iter())
                .map(|(a, b)| (*a as f64 - *b as f64).abs())
                .sum();
            if diff < config.tolerance {
                break;
            }
        }
    }

    let final_scores = if actual_iters % 2 == 0 {
        download_f32(dev, &d_scores)?
    } else {
        download_f32(dev, &d_next_scores)?
    };

    Ok(final_scores.into_iter().map(|s| s as f64).collect())
}
