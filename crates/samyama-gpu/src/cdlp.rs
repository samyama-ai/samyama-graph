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

/// The neighbour **multiset** CDLP counts labels over: every incident edge
/// contributes, in both directions, with no deduplication.
///
/// The multiset is the whole algorithm. Label propagation gives a node the most
/// frequent label among its neighbours, so how many times a neighbour is
/// counted decides which label wins. The CPU implementation counts each
/// successor and each predecessor separately:
///
/// ```ignore
/// for &neighbor in view.successors(idx)   { *counts.entry(labels[neighbor]).or_insert(0) += 1; }
/// for &neighbor in view.predecessors(idx) { *counts.entry(labels[neighbor]).or_insert(0) += 1; }
/// ```
///
/// so a **reciprocal** neighbour votes twice and parallel edges vote once each.
/// That is also what the wgpu shader sees, because it is handed the four raw
/// CSR arrays. The CUDA path used to merge them through a `BTreeSet` first,
/// which collapsed every one of those repeats into a single vote.
///
/// On three nodes with one reciprocal edge -- `0<->2` and `1->0`, labels
/// starting at node ids -- the two disagree from the first iteration and
/// converge to different communities:
///
/// ```text
/// CPU + wgpu (multiset)   final labels = [2, 0, 0]
/// CUDA       (deduped)    final labels = [1, 0, 0]
/// ```
///
/// Deliberately **not** `crate::lcc::undirected_adjacency`, which dedups, drops
/// self-loops and sorts. LCC's kernel binary-searches a neighbour *set*;
/// CDLP's kernel counts over a range and needs the multiset. Two different
/// questions -- unifying them would break one.
pub(crate) fn incident_multiset(
    node_count: usize,
    out_offsets: &[usize],
    out_targets: &[usize],
    in_offsets: &[usize],
    in_sources: &[usize],
) -> (Vec<u32>, Vec<u32>) {
    let mut offsets: Vec<u32> = Vec::with_capacity(node_count + 1);
    let mut targets: Vec<u32> = Vec::new();
    offsets.push(0);
    for i in 0..node_count {
        for idx in out_offsets[i]..out_offsets[i + 1] {
            targets.push(out_targets[idx] as u32);
        }
        for idx in in_offsets[i]..in_offsets[i + 1] {
            targets.push(in_sources[idx] as u32);
        }
        offsets.push(targets.len() as u32);
    }
    (offsets, targets)
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
        let (merged_offsets, merged_targets) = incident_multiset(
            node_count, out_offsets, out_targets, in_offsets, in_sources,
        );
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

#[cfg(test)]
mod multiset_tests {
    use super::incident_multiset;

    fn csr(rows: &[&[usize]]) -> (Vec<usize>, Vec<usize>) {
        let mut offsets = vec![0usize];
        let mut flat = Vec::new();
        for r in rows {
            flat.extend_from_slice(r);
            offsets.push(flat.len());
        }
        (offsets, flat)
    }

    fn votes(offsets: &[u32], targets: &[u32], node: usize) -> Vec<u32> {
        let mut v = targets[offsets[node] as usize..offsets[node + 1] as usize].to_vec();
        v.sort_unstable();
        v
    }

    /// A reciprocal neighbour votes twice. The CUDA path used to merge through
    /// a `BTreeSet`, which gave it one vote and changed which label won -- see
    /// `incident_multiset` for the three-node graph where CPU/wgpu converge to
    /// `[2, 0, 0]` and the deduped version to `[1, 0, 0]`.
    #[test]
    fn a_reciprocal_neighbour_votes_twice() {
        // 0 -> 2, 2 -> 0 (reciprocal), 1 -> 0
        let (out_offsets, out_targets) = csr(&[&[2], &[0], &[0]]);
        let (in_offsets, in_sources) = csr(&[&[1, 2], &[], &[0]]);

        let (offsets, targets) =
            incident_multiset(3, &out_offsets, &out_targets, &in_offsets, &in_sources);

        assert_eq!(
            votes(&offsets, &targets, 0),
            vec![1, 2, 2],
            "node 2 is both a successor and a predecessor of node 0, so it votes twice"
        );
    }

    /// Parallel edges vote once each, for the same reason.
    #[test]
    fn parallel_edges_each_get_a_vote() {
        // Two distinct 0 -> 1 edges.
        let (out_offsets, out_targets) = csr(&[&[1, 1], &[]]);
        let (in_offsets, in_sources) = csr(&[&[], &[0, 0]]);

        let (offsets, targets) =
            incident_multiset(2, &out_offsets, &out_targets, &in_offsets, &in_sources);

        assert_eq!(votes(&offsets, &targets, 0), vec![1, 1]);
        assert_eq!(votes(&offsets, &targets, 1), vec![0, 0]);
    }

    /// Every incident edge is represented exactly once per direction, so the
    /// flat length is the total degree and the offsets stay monotonic.
    #[test]
    fn total_length_is_the_sum_of_both_degrees() {
        let (out_offsets, out_targets) = csr(&[&[1, 2], &[2], &[]]);
        let (in_offsets, in_sources) = csr(&[&[], &[0], &[0, 1]]);

        let (offsets, targets) =
            incident_multiset(3, &out_offsets, &out_targets, &in_offsets, &in_sources);

        assert_eq!(targets.len(), out_targets.len() + in_sources.len());
        assert!(offsets.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(*offsets.last().unwrap() as usize, targets.len());
    }
}
