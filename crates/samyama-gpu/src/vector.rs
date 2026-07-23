//! GPU-accelerated vector distance computation
//!
//! Batch distance evaluation for cosine and inner product metrics.
//! Used to accelerate vector search re-ranking and brute-force kNN.

use crate::buffer::{
    create_storage_buffer, create_storage_buffer_rw, create_uniform_buffer, dispatch_compute,
    download_f32,
};
use crate::context::GpuContext;
use crate::error::GpuError;

const COSINE_SHADER: &str = include_str!("shaders/cosine_distance.wgsl");
const INNER_PRODUCT_SHADER: &str = include_str!("shaders/inner_product.wgsl");
const WORKGROUP_SIZE: u32 = 256;

#[repr(C)]
#[derive(Copy, Clone, bytemuck::Pod, bytemuck::Zeroable)]
struct VectorParams {
    dimensions: u32,
    candidate_count: u32,
    _pad1: u32,
    _pad2: u32,
}

/// Compute batch cosine distances on GPU
///
/// Returns a Vec<f32> of distances, one per candidate.
pub fn gpu_batch_cosine(
    ctx: &GpuContext,
    query: &[f32],
    candidates: &[f32], // Flat array: K * dimensions
    dimensions: usize,
) -> Result<Vec<f32>, GpuError> {
    let candidate_count = candidates.len() / dimensions;
    if candidate_count == 0 {
        return Ok(Vec::new());
    }

    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        match crate::cuda::vector::cuda_batch_cosine(cuda_ctx, query, candidates, dimensions) {
            Ok(distances) => return Ok(distances),
            Err(e) => tracing::warn!("CUDA cosine failed, falling back to wgpu: {}", e),
        }
    }

    gpu_batch_distance(
        ctx,
        query,
        candidates,
        dimensions,
        candidate_count,
        COSINE_SHADER,
        "cosine_batch",
    )
}

/// Compute batch inner product distances on GPU
pub fn gpu_batch_inner_product(
    ctx: &GpuContext,
    query: &[f32],
    candidates: &[f32],
    dimensions: usize,
) -> Result<Vec<f32>, GpuError> {
    let candidate_count = candidates.len() / dimensions;
    if candidate_count == 0 {
        return Ok(Vec::new());
    }

    #[cfg(feature = "cuda")]
    if let Some(cuda_ctx) = crate::runtime::GpuRuntime::get().and_then(|rt| rt.cuda()) {
        match crate::cuda::vector::cuda_batch_inner_product(cuda_ctx, query, candidates, dimensions)
        {
            Ok(distances) => return Ok(distances),
            Err(e) => tracing::warn!("CUDA inner product failed, falling back to wgpu: {}", e),
        }
    }

    gpu_batch_distance(
        ctx,
        query,
        candidates,
        dimensions,
        candidate_count,
        INNER_PRODUCT_SHADER,
        "inner_product_batch",
    )
}

fn gpu_batch_distance(
    ctx: &GpuContext,
    query: &[f32],
    candidates: &[f32],
    dimensions: usize,
    candidate_count: usize,
    shader_source: &str,
    entry_point: &str,
) -> Result<Vec<f32>, GpuError> {
    let query_buf = create_storage_buffer(ctx, "query", bytemuck::cast_slice(query));
    let candidates_buf = create_storage_buffer(ctx, "candidates", bytemuck::cast_slice(candidates));

    let distances_init: Vec<f32> = vec![0.0; candidate_count];
    let distances_buf =
        create_storage_buffer_rw(ctx, "distances", bytemuck::cast_slice(&distances_init));

    let params = VectorParams {
        dimensions: dimensions as u32,
        candidate_count: candidate_count as u32,
        _pad1: 0,
        _pad2: 0,
    };
    let params_buf = create_uniform_buffer(ctx, "vec_params", bytemuck::bytes_of(&params));

    let pipeline = ctx.create_compute_pipeline("vector_distance", shader_source, entry_point);

    let bind_group_layout = pipeline.get_bind_group_layout(0);
    let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("vec_bind_group"),
        layout: &bind_group_layout,
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: query_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: candidates_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: distances_buf.as_entire_binding(),
            },
            wgpu::BindGroupEntry {
                binding: 3,
                resource: params_buf.as_entire_binding(),
            },
        ],
    });

    let workgroup_count = (candidate_count as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;
    dispatch_compute(ctx, &pipeline, &bind_group, workgroup_count);

    download_f32(ctx, &distances_buf, candidate_count)
}

/// Distance metric for a resident vector index.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum VectorMetric {
    /// Cosine distance (1 - cosine similarity).
    Cosine,
    /// Inner-product distance (negated dot product).
    InnerProduct,
}

/// A vector corpus uploaded **once** to the GPU and reused across many queries.
///
/// The one-shot [`gpu_batch_cosine`] re-uploads the whole candidate matrix on
/// every call, so for a single query it is transfer-bound and can lose to a
/// SIMD CPU working from resident RAM. For repeated search over a *fixed*
/// corpus (kNN, re-ranking, RAG retrieval), build a `GpuVectorIndex` once and
/// call [`query`](GpuVectorIndex::query) per probe: the candidate matrix, the
/// compute pipeline, and the bind group are all cached on-device, so only the
/// (tiny) query vector crosses the PCIe bus each time.
pub struct GpuVectorIndex {
    pipeline: wgpu::ComputePipeline,
    bind_group: wgpu::BindGroup,
    query_buf: wgpu::Buffer,
    distances_buf: wgpu::Buffer,
    dimensions: usize,
    candidate_count: usize,
    metric: VectorMetric,
    // Held to keep the resident GPU allocations alive for the index's lifetime.
    _candidates_buf: wgpu::Buffer,
    _params_buf: wgpu::Buffer,
}

impl GpuVectorIndex {
    /// Upload `candidates` (a flat `K * dimensions` row-major matrix) to the GPU
    /// and build a reusable index for the given `metric`.
    pub fn new(
        ctx: &GpuContext,
        candidates: &[f32],
        dimensions: usize,
        metric: VectorMetric,
    ) -> Result<Self, GpuError> {
        use wgpu::util::DeviceExt;

        if dimensions == 0 {
            return Err(GpuError::DataTooLarge {
                requested: 0,
                available: 0,
            });
        }
        let candidate_count = candidates.len() / dimensions;
        if candidate_count == 0 {
            return Err(GpuError::DataTooLarge {
                requested: candidates.len(),
                available: dimensions,
            });
        }

        let (shader_source, entry_point) = match metric {
            VectorMetric::Cosine => (COSINE_SHADER, "cosine_batch"),
            VectorMetric::InnerProduct => (INNER_PRODUCT_SHADER, "inner_product_batch"),
        };

        // Resident, read-only candidate matrix (uploaded once).
        let candidates_buf =
            create_storage_buffer(ctx, "candidates_resident", bytemuck::cast_slice(candidates));

        // Writable query buffer, refreshed per probe via `queue.write_buffer`.
        let query_init = vec![0u8; dimensions * std::mem::size_of::<f32>()];
        let query_buf = ctx
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("query_resident"),
                contents: &query_init,
                usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_DST,
            });

        // Reused output buffer.
        let distances_init: Vec<f32> = vec![0.0; candidate_count];
        let distances_buf =
            create_storage_buffer_rw(ctx, "distances_resident", bytemuck::cast_slice(&distances_init));

        let params = VectorParams {
            dimensions: dimensions as u32,
            candidate_count: candidate_count as u32,
            _pad1: 0,
            _pad2: 0,
        };
        let params_buf = create_uniform_buffer(ctx, "vec_params", bytemuck::bytes_of(&params));

        let pipeline =
            ctx.create_compute_pipeline("vector_distance_resident", shader_source, entry_point);
        let bind_group_layout = pipeline.get_bind_group_layout(0);
        let bind_group = ctx.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("vec_bind_group_resident"),
            layout: &bind_group_layout,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: query_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: candidates_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: distances_buf.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: params_buf.as_entire_binding(),
                },
            ],
        });

        Ok(Self {
            pipeline,
            bind_group,
            query_buf,
            distances_buf,
            dimensions,
            candidate_count,
            metric,
            _candidates_buf: candidates_buf,
            _params_buf: params_buf,
        })
    }

    /// Score a single `query` (length must equal `dimensions`) against the
    /// resident corpus, returning one distance per candidate.
    pub fn query(&self, ctx: &GpuContext, query: &[f32]) -> Result<Vec<f32>, GpuError> {
        if query.len() != self.dimensions {
            return Err(GpuError::DataTooLarge {
                requested: query.len(),
                available: self.dimensions,
            });
        }
        // Stream only the query vector to the device; the corpus stays resident.
        ctx.queue
            .write_buffer(&self.query_buf, 0, bytemuck::cast_slice(query));

        let workgroup_count =
            (self.candidate_count as u32 + WORKGROUP_SIZE - 1) / WORKGROUP_SIZE;
        dispatch_compute(ctx, &self.pipeline, &self.bind_group, workgroup_count);

        download_f32(ctx, &self.distances_buf, self.candidate_count)
    }

    /// Number of candidate vectors resident on the GPU.
    pub fn candidate_count(&self) -> usize {
        self.candidate_count
    }

    /// Vector dimensionality of the resident corpus.
    pub fn dimensions(&self) -> usize {
        self.dimensions
    }

    /// The distance metric this index was built for.
    pub fn metric(&self) -> VectorMetric {
        self.metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gpu_cosine_distance() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        let query = vec![1.0f32, 0.0, 0.0];
        // 3 candidates: same direction, orthogonal, opposite
        let candidates = vec![
            1.0, 0.0, 0.0, // Same: distance ~0
            0.0, 1.0, 0.0, // Orthogonal: distance ~1
            -1.0, 0.0, 0.0, // Opposite: distance ~2
        ];

        let distances = gpu_batch_cosine(&ctx, &query, &candidates, 3).unwrap();
        assert_eq!(distances.len(), 3);
        assert!(
            distances[0].abs() < 0.01,
            "Same direction should be ~0, got {}",
            distances[0]
        );
        assert!(
            (distances[1] - 1.0).abs() < 0.01,
            "Orthogonal should be ~1, got {}",
            distances[1]
        );
        assert!(
            (distances[2] - 2.0).abs() < 0.01,
            "Opposite should be ~2, got {}",
            distances[2]
        );
    }

    #[test]
    fn test_resident_index_matches_one_shot() {
        let ctx = match GpuContext::new() {
            Ok(ctx) => ctx,
            Err(_) => {
                println!("No GPU, skipping test");
                return;
            }
        };

        let dims = 3;
        let candidates = vec![
            1.0f32, 0.0, 0.0, // same
            0.0, 1.0, 0.0, // orthogonal
            -1.0, 0.0, 0.0, // opposite
        ];

        let index = GpuVectorIndex::new(&ctx, &candidates, dims, VectorMetric::Cosine).unwrap();
        assert_eq!(index.candidate_count(), 3);
        assert_eq!(index.dimensions(), 3);

        // Two different queries reusing the same resident corpus.
        let q1 = vec![1.0f32, 0.0, 0.0];
        let d1 = index.query(&ctx, &q1).unwrap();
        let one_shot = gpu_batch_cosine(&ctx, &q1, &candidates, dims).unwrap();
        assert_eq!(d1.len(), 3);
        for (a, b) in d1.iter().zip(one_shot.iter()) {
            assert!((a - b).abs() < 1e-4, "resident {} vs one-shot {}", a, b);
        }

        // A second probe must reflect the new query, not a stale corpus upload.
        let q2 = vec![0.0f32, 1.0, 0.0];
        let d2 = index.query(&ctx, &q2).unwrap();
        assert!(d2[1].abs() < 0.01, "query aligned with candidate 1, got {}", d2[1]);

        // Dimension mismatch is rejected.
        assert!(index.query(&ctx, &[1.0, 0.0]).is_err());
    }
}
