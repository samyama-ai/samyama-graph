//! GPU-accelerated triangle counting
//!
//! Edge-centric parallel algorithm: one thread per edge,
//! counts common neighbors via sorted merge intersection.
//! Accepts raw CSR arrays.

use crate::buffer::{
    self, create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer, download_u32,
};
use crate::error::GpuError;

const SHADER_SOURCE: &str = include_str!("shaders/triangles.wgsl");
const WORKGROUP_SIZE: u32 = 256;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct TriangleParams {
    edge_count: u32,
    _pad1: u32,
    _pad2: u32,
    _pad3: u32,
}

/// Count triangles using the GPU (edge-centric algorithm)
///
/// Takes raw CSR data. Builds undirected sorted adjacency internally.
pub fn gpu_count_triangles(
    node_count: usize,
    out_offsets: &[usize],
    out_targets: &[usize],
    in_offsets: &[usize],
    in_sources: &[usize],
) -> Result<usize, GpuError> {
    if node_count == 0 || out_targets.is_empty() {
        return Ok(0);
    }

    // Build undirected sorted adjacency
    let mut sorted_offsets: Vec<u32> = Vec::with_capacity(node_count + 1);
    let mut sorted_targets: Vec<u32> = Vec::new();

    sorted_offsets.push(0);
    for i in 0..node_count {
        let mut neighbors: Vec<u32> = Vec::new();
        let out_start = out_offsets[i];
        let out_end = out_offsets[i + 1];
        for idx in out_start..out_end {
            neighbors.push(out_targets[idx] as u32);
        }
        let in_start = in_offsets[i];
        let in_end = in_offsets[i + 1];
        for idx in in_start..in_end {
            neighbors.push(in_sources[idx] as u32);
        }
        neighbors.sort();
        neighbors.dedup();
        sorted_targets.extend(&neighbors);
        sorted_offsets.push(sorted_targets.len() as u32);
    }

    // Build edge list (u < v only)
    let mut edge_src: Vec<u32> = Vec::new();
    let mut edge_dst: Vec<u32> = Vec::new();
    for u in 0..node_count {
        let start = sorted_offsets[u] as usize;
        let end = sorted_offsets[u + 1] as usize;
        for idx in start..end {
            let v = sorted_targets[idx];
            if (u as u32) < v {
                edge_src.push(u as u32);
                edge_dst.push(v);
            }
        }
    }

    let edge_count = edge_src.len();

    // Try CUDA first (native NVIDIA path) — adjacency already built
    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        match crate::cuda::topology::cuda_count_triangles(
            cuda_ctx,
            &edge_src,
            &edge_dst,
            &sorted_offsets,
            &sorted_targets,
        ) {
            Ok(count) => {
                tracing::debug!("Triangles: used CUDA backend ({} edges)", edge_count);
                return Ok(count as usize);
            }
            Err(e) => {
                tracing::warn!("CUDA triangles failed, falling back to wgpu: {}", e);
            }
        }
    }

    if edge_count == 0 {
        return Ok(0);
    }

    // wgpu fallback: source the wgpu context from the runtime (F1 fix). None on
    // headless/CUDA-only hosts -> the caller falls back to CPU. `init()` is idempotent.
    let ctx = match crate::runtime::GpuRuntime::init().wgpu() {
        Some(c) => c,
        None => return Err(GpuError::NoAdapter),
    };

    let edge_src_buf = create_storage_buffer(ctx, "edge_src", bytemuck::cast_slice(&edge_src));
    let edge_dst_buf = create_storage_buffer(ctx, "edge_dst", bytemuck::cast_slice(&edge_dst));
    let offsets_buf =
        create_storage_buffer(ctx, "sorted_offsets", bytemuck::cast_slice(&sorted_offsets));

    // Pad sorted_targets if empty
    let targets_data = if sorted_targets.is_empty() {
        vec![0u32]
    } else {
        sorted_targets
    };
    let targets_buf =
        create_storage_buffer(ctx, "sorted_targets", bytemuck::cast_slice(&targets_data));

    let counter_data: [u32; 1] = [0];
    let counter_buf =
        create_storage_buffer_rw(ctx, "triangle_count", bytemuck::cast_slice(&counter_data));

    let params = TriangleParams {
        edge_count: edge_count as u32,
        _pad1: 0,
        _pad2: 0,
        _pad3: 0,
    };
    let params_buf = create_uniform_buffer(ctx, "tri_params", bytemuck::bytes_of(&params));

    let pipeline = ctx.create_compute_pipeline("triangles", SHADER_SOURCE, "count_triangles");

    let bind_group_layout = pipeline.get_bind_group_layout(0);
    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("tri_bind_group"),
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: edge_src_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: edge_dst_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: offsets_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: targets_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 4,
                resource: counter_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 5,
                resource: params_buf.as_entire_binding(),
            },
        ],
    });

    let workgroup_count = (edge_count as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;
    buffer::dispatch_compute(ctx, &pipeline, &bind_group, workgroup_count);

    let result = download_u32(ctx, &counter_buf, 1)?;
    Ok(result[0] as usize)
}
