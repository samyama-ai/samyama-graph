//! GPU-accelerated Local Clustering Coefficient (LCC)
//!
//! Per-node kernel: enumerate neighbor pairs and check adjacency
//! via binary search in sorted CSR. Accepts raw CSR arrays.
//! Supports both directed and undirected modes.

use crate::buffer::{
    self, create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer, download_f32,
};
use crate::error::GpuError;

const SHADER_SOURCE: &str = include_str!("shaders/lcc.wgsl");
const WORKGROUP_SIZE: u32 = 256;
/// Maximum undirected degree for GPU LCC. Nodes with higher degree cause
/// O(d²) shader loops that exceed the GPU compute timeout (~60s on Apple
/// Silicon). When any node exceeds this, we fall back to CPU.
const MAX_GPU_DEGREE: u32 = 4096;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct LccParams {
    node_count: u32,
    directed: u32,
    _pad2: u32,
    _pad3: u32,
}

/// GPU LCC result: coefficients indexed by dense node index
pub struct GpuLccResult {
    /// Coefficients indexed by dense node index (0..node_count)
    pub coefficients: Vec<f64>,
    /// Global average
    pub average: f64,
}

/// Compute LCC on the GPU using raw CSR data
///
/// When `directed` is false, builds undirected sorted adjacency internally
/// for binary search on GPU (original behavior).
///
/// When `directed` is true, uses directed outgoing edges for triangle
/// counting, dividing by d*(d-1) instead of d*(d-1)/2.
pub fn gpu_lcc(
    node_count: usize,
    out_offsets: &[usize],
    out_targets: &[usize],
    in_offsets: &[usize],
    in_sources: &[usize],
    directed: bool,
) -> Result<GpuLccResult, GpuError> {
    if node_count == 0 {
        return Ok(GpuLccResult {
            coefficients: Vec::new(),
            average: 0.0,
        });
    }

    // Try CUDA first
    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        // Build merged sorted adjacency for CUDA
        let mut merged_offsets: Vec<u32> = Vec::with_capacity(node_count + 1);
        let mut merged_targets: Vec<u32> = Vec::new();
        merged_offsets.push(0);
        for i in 0..node_count {
            let mut neighbors = std::collections::BTreeSet::new();
            for idx in out_offsets[i]..out_offsets[i + 1] {
                neighbors.insert(out_targets[idx] as u32);
            }
            for idx in in_offsets[i]..in_offsets[i + 1] {
                neighbors.insert(in_sources[idx] as u32);
            }
            for n in &neighbors {
                merged_targets.push(*n);
            }
            merged_offsets.push(merged_targets.len() as u32);
        }
        match crate::cuda::lcc::cuda_lcc(
            cuda_ctx,
            &merged_offsets,
            &merged_targets,
            node_count,
            directed,
        ) {
            Ok(coefficients) => {
                let avg = if coefficients.is_empty() {
                    0.0
                } else {
                    coefficients.iter().map(|c| *c as f64).sum::<f64>() / coefficients.len() as f64
                };
                tracing::debug!("LCC: used CUDA backend ({} nodes)", node_count);
                return Ok(GpuLccResult {
                    coefficients: coefficients.into_iter().map(|c| c as f64).collect(),
                    average: avg,
                });
            }
            Err(e) => tracing::warn!("CUDA LCC failed, falling back to wgpu: {}", e),
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
    let largest_buf =
        std::cmp::max(out_targets.len(), in_sources.len()) * std::mem::size_of::<u32>();
    if largest_buf > max_buf {
        return Err(GpuError::DataTooLarge {
            requested: largest_buf,
            available: max_buf,
        });
    }

    // Build undirected sorted adjacency (used for neighbor enumeration in both modes)
    let mut sorted_offsets: Vec<u32> = Vec::with_capacity(node_count + 1);
    let mut sorted_targets: Vec<u32> = Vec::new();

    sorted_offsets.push(0);
    for i in 0..node_count {
        let mut neighbors: Vec<u32> = Vec::new();
        let out_start = out_offsets[i];
        let out_end = out_offsets[i + 1];
        for idx in out_start..out_end {
            if out_targets[idx] != i {
                neighbors.push(out_targets[idx] as u32);
            }
        }
        let in_start = in_offsets[i];
        let in_end = in_offsets[i + 1];
        for idx in in_start..in_end {
            if in_sources[idx] != i {
                neighbors.push(in_sources[idx] as u32);
            }
        }
        neighbors.sort();
        neighbors.dedup();
        sorted_targets.extend(&neighbors);
        sorted_offsets.push(sorted_targets.len() as u32);
    }

    // Check max degree — O(d²) shader loop times out on high-degree nodes
    let max_degree = sorted_offsets
        .windows(2)
        .map(|w| w[1] - w[0])
        .max()
        .unwrap_or(0);
    if max_degree > MAX_GPU_DEGREE {
        return Err(GpuError::DataTooLarge {
            requested: max_degree as usize,
            available: MAX_GPU_DEGREE as usize,
        });
    }

    // Check derived buffer sizes against GPU limits
    let sorted_targets_bytes = sorted_targets.len() * std::mem::size_of::<u32>();
    if sorted_targets_bytes > max_buf {
        return Err(GpuError::DataTooLarge {
            requested: sorted_targets_bytes,
            available: max_buf,
        });
    }

    // Handle edge case
    if sorted_targets.is_empty() {
        sorted_targets.push(0);
    }

    // Build sorted outgoing adjacency for directed mode
    let (dir_out_offsets, dir_out_targets) = if directed {
        let mut d_offsets: Vec<u32> = Vec::with_capacity(node_count + 1);
        let mut d_targets: Vec<u32> = Vec::new();
        d_offsets.push(0);
        for i in 0..node_count {
            let start = out_offsets[i];
            let end = out_offsets[i + 1];
            let mut outs: Vec<u32> = (start..end)
                .filter(|&idx| out_targets[idx] != i)
                .map(|idx| out_targets[idx] as u32)
                .collect();
            outs.sort();
            d_targets.extend(&outs);
            d_offsets.push(d_targets.len() as u32);
        }
        if d_targets.is_empty() {
            d_targets.push(0);
        }
        (d_offsets, d_targets)
    } else {
        // Dummy buffers for undirected mode (shader won't use them)
        (vec![0u32; node_count + 1], vec![0u32])
    };

    let offsets_buf =
        create_storage_buffer(ctx, "lcc_offsets", bytemuck::cast_slice(&sorted_offsets));
    let targets_buf =
        create_storage_buffer(ctx, "lcc_targets", bytemuck::cast_slice(&sorted_targets));

    let zeros: Vec<f32> = vec![0.0; node_count];
    let coefficients_buf =
        create_storage_buffer_rw(ctx, "lcc_coefficients", bytemuck::cast_slice(&zeros));

    let params = LccParams {
        node_count: node_count as u32,
        directed: if directed { 1 } else { 0 },
        _pad2: 0,
        _pad3: 0,
    };
    let params_buf = create_uniform_buffer(ctx, "lcc_params", bytemuck::bytes_of(&params));

    let dir_out_offsets_buf = create_storage_buffer(
        ctx,
        "lcc_dir_out_offsets",
        bytemuck::cast_slice(&dir_out_offsets),
    );
    let dir_out_targets_buf = create_storage_buffer(
        ctx,
        "lcc_dir_out_targets",
        bytemuck::cast_slice(&dir_out_targets),
    );

    let pipeline = ctx.create_compute_pipeline("lcc", SHADER_SOURCE, "compute_lcc");
    let workgroup_count = (node_count as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;

    let bind_group_layout = pipeline.get_bind_group_layout(0);
    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("lcc_bind_group"),
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: offsets_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: targets_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: coefficients_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: params_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 4,
                resource: dir_out_offsets_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 5,
                resource: dir_out_targets_buf.as_entire_binding(),
            },
        ],
    });

    buffer::dispatch_compute(ctx, &pipeline, &bind_group, workgroup_count);

    let coeffs_f32 = download_f32(ctx, &coefficients_buf, node_count)?;

    let mut sum = 0.0;
    let coefficients: Vec<f64> = coeffs_f32
        .iter()
        .map(|&cc| {
            let cc_f64 = cc as f64;
            sum += cc_f64;
            cc_f64
        })
        .collect();

    let average = if node_count > 0 {
        sum / node_count as f64
    } else {
        0.0
    };

    Ok(GpuLccResult {
        coefficients,
        average,
    })
}
