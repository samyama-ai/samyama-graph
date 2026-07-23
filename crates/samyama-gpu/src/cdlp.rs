//! GPU-accelerated CDLP (Community Detection via Label Propagation)
//!
//! Synchronous label propagation on GPU. Each iteration is a kernel launch
//! where each thread processes one node. Accepts raw CSR arrays.

use crate::buffer::{
    self, create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer, download_u32,
};
use crate::error::GpuError;

const SHADER_SOURCE: &str = include_str!("shaders/cdlp.wgsl");
const WORKGROUP_SIZE: u32 = 256;
/// Maximum total degree (out+in) for GPU CDLP. The shader has O(d²)
/// complexity per node, which exceeds GPU compute timeout on high-degree
/// nodes. When any node exceeds this threshold, we fall back to CPU.
const MAX_GPU_DEGREE: u32 = 4096;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct CdlpParams {
    node_count: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

/// GPU CDLP result: labels indexed by dense node index
pub struct GpuCdlpResult {
    /// Labels indexed by dense node index (0..node_count)
    pub labels: Vec<u32>,
    /// Number of iterations performed
    pub iterations: usize,
}

/// Run CDLP on the GPU using raw CSR data
///
/// `initial_labels` should contain the actual vertex IDs (as u32) for each
/// dense index. This ensures tie-breaking matches the LDBC spec (smallest
/// vertex ID wins). Pass `None` to use dense indices (0..node_count).
pub fn gpu_cdlp(
    node_count: usize,
    out_offsets: &[usize],
    out_targets: &[usize],
    in_offsets: &[usize],
    in_sources: &[usize],
    max_iterations: usize,
    initial_labels: Option<&[u32]>,
) -> Result<GpuCdlpResult, GpuError> {
    if node_count == 0 {
        return Ok(GpuCdlpResult {
            labels: Vec::new(),
            iterations: 0,
        });
    }

    // Try CUDA first
    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        // Build merged adjacency for CUDA
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
        match crate::cuda::cdlp::cuda_cdlp(
            cuda_ctx,
            &merged_offsets,
            &merged_targets,
            node_count,
            max_iterations,
        ) {
            Ok(labels) => {
                tracing::debug!("CDLP: used CUDA backend ({} nodes)", node_count);
                return Ok(GpuCdlpResult {
                    labels,
                    iterations: max_iterations,
                });
            }
            Err(e) => tracing::warn!("CUDA CDLP failed, falling back to wgpu: {}", e),
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

    // Check max total degree — O(d²) shader loop times out on high-degree nodes
    let max_degree = (0..node_count)
        .map(|i| {
            let out_deg = out_offsets[i + 1] - out_offsets[i];
            let in_deg = in_offsets[i + 1] - in_offsets[i];
            (out_deg + in_deg) as u32
        })
        .max()
        .unwrap_or(0);
    if max_degree > MAX_GPU_DEGREE {
        return Err(GpuError::DataTooLarge {
            requested: max_degree as usize,
            available: MAX_GPU_DEGREE as usize,
        });
    }

    // Upload CSR
    let csr = crate::buffer::upload_csr(
        ctx,
        node_count,
        out_offsets,
        out_targets,
        in_offsets,
        in_sources,
    );

    // Initialize labels: use provided vertex IDs or fall back to dense indices
    let default_labels: Vec<u32>;
    let init_labels = match initial_labels {
        Some(labels) => labels,
        None => {
            default_labels = (0..node_count as u32).collect();
            &default_labels
        }
    };
    let labels_buf_a = create_storage_buffer_rw(ctx, "labels_a", bytemuck::cast_slice(init_labels));
    let labels_buf_b = create_storage_buffer_rw(ctx, "labels_b", bytemuck::cast_slice(init_labels));

    let params = CdlpParams {
        node_count: node_count as u32,
        _pad1: 0,
        _pad2: 0,
        _pad3: 0,
    };
    let params_buf = create_uniform_buffer(ctx, "cdlp_params", bytemuck::bytes_of(&params));

    let pipeline = ctx.create_compute_pipeline("cdlp", SHADER_SOURCE, "cdlp_iter");
    let workgroup_count = (node_count as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;

    let mut iterations = 0;

    for iter in 0..max_iterations {
        iterations += 1;

        let (read_buf, write_buf) = if iter % 2 == 0 {
            (&labels_buf_a, &labels_buf_b)
        } else {
            (&labels_buf_b, &labels_buf_a)
        };

        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("cdlp_bind_group"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: csr.out_offsets.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: csr.out_targets.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: csr.in_offsets.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: csr.in_sources.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 4,
                    resource: read_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 5,
                    resource: write_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 6,
                    resource: params_buf.as_entire_binding(),
                },
            ],
        });

        buffer::dispatch_compute(ctx, &pipeline, &bind_group, workgroup_count);

        // Convergence check every 5 iterations
        if (iter + 1) % 5 == 0 || iter + 1 == max_iterations {
            let old = download_u32(ctx, read_buf, node_count)?;
            let new = download_u32(ctx, write_buf, node_count)?;
            if old == new {
                break;
            }
        }
    }

    let final_buf = if iterations % 2 == 0 {
        &labels_buf_a
    } else {
        &labels_buf_b
    };
    let labels = download_u32(ctx, final_buf, node_count)?;

    Ok(GpuCdlpResult { labels, iterations })
}
