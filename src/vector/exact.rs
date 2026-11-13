//! Which device scores an exact (brute-force) vector scan (#1977).
//!
//! The exact scan in [`VectorIndex`](super::VectorIndex) answers small
//! indexes, filtered plans that pass few nodes (#1969), deletion-heavy
//! indexes (#1968) and every fallback from the graph search. Built with
//! `--features gpu`, an unquantized cosine or inner-product scan that is big
//! enough is scored by the `samyama-gpu` batch kernels
//! (`gpu_batch_cosine`, `gpu_batch_inner_product`) instead of the CPU.
//!
//! The GPU is used only when all of these hold, and the CPU answers
//! otherwise, with no error and no change in the result:
//!
//! - the build has the `gpu` feature (without it this module never touches a
//!   device, and the scan is the CPU loop it always was);
//! - a GPU adapter is present and `SAMYAMA_GPU` is not `off` / `0` / `false`
//!   (the crate-wide kill switch, [`samyama_gpu::GpuContext::is_enabled`]);
//! - the index stores f32 values (an f16 index is scored at f16 on the CPU,
//!   which the kernels do not reproduce);
//! - the metric is cosine or inner product (there is no L2 kernel);
//! - the work, candidates x dimensions, is at least [`gpu_min_work`];
//! - the kernel returns one distance per candidate without an error or a
//!   panic.
//!
//! ## The threshold
//!
//! The batch kernels upload the whole candidate matrix on every call, so a
//! single query is transfer-bound: the host must first gather the candidate
//! rows into one contiguous buffer, which reads as much memory as the SIMD
//! CPU scan (#1976) does to finish the job. The GPU can only win once the
//! scan is large enough that dispatch and read-back latency are amortised and
//! the device's arithmetic outruns the host's. [`GPU_MIN_WORK`] is set at
//! 2^24 multiply-adds (64 MiB of f32 candidates, e.g. 128K vectors at 128
//! dimensions), well above the size at which the transfer alone dominates.
//! It is a conservative guess, not a measurement: set
//! `SAMYAMA_GPU_VECTOR_MIN_WORK` to tune it on a given host. A corpus kept
//! resident on the device (`samyama_gpu::GpuVectorIndex`) is the change that
//! removes the per-query upload; it is not wired here.
//!
//! ## Equality with the CPU
//!
//! The kernels accumulate in f32 one term at a time, so each distance agrees
//! with the CPU's SIMD sum to rounding (`samyama_gpu::tolerance::DISTANCE_ABS`
//! for unit-scale vectors), not bit for bit. The GPU distance is mapped to
//! the same rank the CPU computes (cosine clamped and floored exactly as
//! `cosine_rank` does; inner product through `inner_product_rank`), and the
//! caller sorts both with the same stable sort, so ties keep stored order on
//! either device. Two candidates whose distances differ by less than the
//! rounding may come back in the other order.
//!
//! ## Which backend answered
//!
//! [`backend_counts`] reports how many exact scans each device has answered
//! in this process.

use std::sync::atomic::{AtomicU64, Ordering};

use super::index::{DistanceMetric, StoredVector};

/// The device that scored an exact scan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExactBackend {
    /// The CPU loop (SIMD where the host has it).
    Cpu,
    /// A `samyama-gpu` batch distance kernel.
    Gpu,
}

impl ExactBackend {
    /// `"cpu"` or `"gpu"`.
    pub fn as_str(self) -> &'static str {
        match self {
            ExactBackend::Cpu => "cpu",
            ExactBackend::Gpu => "gpu",
        }
    }
}

/// Default minimum candidates x dimensions for a scan to go to the GPU.
/// See the module documentation for why it is this large.
pub const GPU_MIN_WORK: usize = 1 << 24;

/// The threshold in force: `SAMYAMA_GPU_VECTOR_MIN_WORK` when it is set to
/// a number, [`GPU_MIN_WORK`] otherwise. Read once per process.
pub fn gpu_min_work() -> usize {
    static MIN_WORK: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MIN_WORK.get_or_init(|| {
        std::env::var("SAMYAMA_GPU_VECTOR_MIN_WORK")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(GPU_MIN_WORK)
    })
}

static CPU_SCANS: AtomicU64 = AtomicU64::new(0);
static GPU_SCANS: AtomicU64 = AtomicU64::new(0);

/// Exact scans answered so far in this process, as `(cpu, gpu)`.
pub fn backend_counts() -> (u64, u64) {
    (
        CPU_SCANS.load(Ordering::Relaxed),
        GPU_SCANS.load(Ordering::Relaxed),
    )
}

pub(super) fn record(backend: ExactBackend) {
    let counter = match backend {
        ExactBackend::Cpu => &CPU_SCANS,
        ExactBackend::Gpu => &GPU_SCANS,
    };
    counter.fetch_add(1, Ordering::Relaxed);
}

/// The rank (as `MetricDistance` orders by) of each row, scored on the GPU,
/// or `None` when the CPU should score them. See the module documentation
/// for when that is.
#[cfg(feature = "gpu")]
pub(super) fn gpu_ranks(
    metric: DistanceMetric,
    query: &[f32],
    rows: &[&StoredVector],
    dimensions: usize,
    min_work: usize,
) -> Option<Vec<f32>> {
    use samyama_gpu::{GpuContext, GpuError};

    type Kernel = fn(&GpuContext, &[f32], &[f32], usize) -> Result<Vec<f32>, GpuError>;
    let kernel: Kernel = match metric {
        DistanceMetric::Cosine => samyama_gpu::gpu_batch_cosine,
        DistanceMetric::InnerProduct => samyama_gpu::gpu_batch_inner_product,
        DistanceMetric::L2 => return None,
    };
    if rows.is_empty()
        || dimensions == 0
        || query.len() != dimensions
        || rows.len().saturating_mul(dimensions) < min_work
    {
        return None;
    }
    let ctx = GpuContext::try_global()?;

    // wgpu's portable limit on one storage-buffer binding is 128 MiB; a
    // larger corpus is scored in chunks under it, so no adapter's limit is
    // exceeded whatever it reports.
    const MAX_CHUNK_BYTES: usize = 128 << 20;
    let per_chunk = (MAX_CHUNK_BYTES / (dimensions * std::mem::size_of::<f32>())).max(1);
    let mut flat: Vec<f32> = Vec::with_capacity(per_chunk.min(rows.len()) * dimensions);
    let mut ranks = Vec::with_capacity(rows.len());
    for chunk in rows.chunks(per_chunk) {
        flat.clear();
        for sv in chunk {
            if sv.vector.len() != dimensions {
                return None;
            }
            flat.extend_from_slice(&sv.vector);
        }
        // A wgpu validation failure panics rather than returning an error;
        // a query must not take the server down over it, so contain it and
        // let the CPU answer.
        let scored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            kernel(ctx, query, &flat, dimensions)
        }));
        match scored {
            Ok(Ok(distances)) if distances.len() == chunk.len() => {
                ranks.extend(distances.into_iter().map(|d| rank_of(metric, d)));
            }
            Ok(Ok(_)) => {
                tracing::warn!(
                    "GPU vector kernel returned the wrong number of distances; using the CPU"
                );
                return None;
            }
            Ok(Err(e)) => {
                tracing::warn!("GPU vector kernel failed, using the CPU: {e}");
                return None;
            }
            Err(_) => {
                tracing::warn!("GPU vector kernel panicked; using the CPU");
                return None;
            }
        }
    }
    Some(ranks)
}

/// Without the `gpu` feature the CPU scores every scan.
#[cfg(not(feature = "gpu"))]
pub(super) fn gpu_ranks(
    _metric: DistanceMetric,
    _query: &[f32],
    _rows: &[&StoredVector],
    _dimensions: usize,
    _min_work: usize,
) -> Option<Vec<f32>> {
    None
}

/// A kernel's distance as the rank `MetricDistance` gives the same pair.
///
/// Cosine: the kernel reports `1 - sim` unclamped; `cosine_rank` clamps the
/// similarity to [-1, 1] and floors the distance at 0, and so does this, in
/// the same order, so a NaN goes where the CPU sends it. Zero-norm vectors
/// are 1 on both. Inner product: the kernel reports `1 - dot`.
#[cfg(feature = "gpu")]
fn rank_of(metric: DistanceMetric, d: f32) -> f32 {
    match metric {
        DistanceMetric::Cosine => {
            let sim = (1.0 - d).clamp(-1.0, 1.0);
            (1.0 - sim).max(0.0)
        }
        DistanceMetric::InnerProduct => super::index::inner_product_rank(1.0 - d),
        DistanceMetric::L2 => d,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::NodeId;
    use crate::vector::VectorIndex;

    /// Deterministic values in [-1, 1).
    fn values(seed: u64, n: usize) -> Vec<f32> {
        let mut s = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (0..n)
            .map(|_| {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                ((s >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
            })
            .collect()
    }

    fn unit(mut v: Vec<f32>) -> Vec<f32> {
        let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if n > 0.0 {
            v.iter_mut().for_each(|x| *x /= n);
        }
        v
    }

    /// 300 vectors at 16 dimensions, with node 1000 an exact copy of node 7
    /// (a tie that must keep stored order) and node 1001 all zeros.
    fn index(metric: DistanceMetric) -> VectorIndex {
        let dims = 16;
        let mut index = VectorIndex::new(dims, metric);
        for i in 0..300u64 {
            index.add(NodeId::new(i), &unit(values(i, dims))).unwrap();
        }
        index
            .add(NodeId::new(1000), &unit(values(7, dims)))
            .unwrap();
        index.add(NodeId::new(1001), &vec![0.0; dims]).unwrap();
        index
    }

    #[test]
    fn a_huge_threshold_keeps_every_scan_on_the_cpu() {
        let index = index(DistanceMetric::Cosine);
        let q = unit(values(7, 16));
        let (_, backend) = index.brute_force_where_with(&q, 10, &|_| true, usize::MAX);
        assert_eq!(backend, ExactBackend::Cpu);
    }

    #[test]
    fn l2_has_no_kernel_and_answers_on_the_cpu() {
        let index = index(DistanceMetric::L2);
        let q = values(7, 16);
        let (_, backend) = index.brute_force_where_with(&q, 10, &|_| true, 0);
        assert_eq!(backend, ExactBackend::Cpu);
    }

    #[cfg(not(feature = "gpu"))]
    #[test]
    fn without_the_gpu_feature_no_scan_reaches_a_device() {
        let index = index(DistanceMetric::Cosine);
        let q = unit(values(7, 16));
        let (_, backend) = index.brute_force_where_with(&q, 10, &|_| true, 0);
        assert_eq!(backend, ExactBackend::Cpu);
    }

    /// The GPU path, forced with a threshold of 0, answers what the CPU path
    /// answers: the same distances within rounding, the same node at each
    /// rank unless two candidates are within rounding of each other, and the
    /// duplicate pair in stored order. Passes with a message on a host with
    /// no adapter, which is every CI runner.
    #[test]
    fn the_gpu_scan_answers_what_the_cpu_scan_answers() {
        const EPS: f32 = 1e-4;
        for metric in [DistanceMetric::Cosine, DistanceMetric::InnerProduct] {
            let index = index(metric);
            let keep_even = |id: NodeId| id.as_u64().is_multiple_of(2) || id.as_u64() >= 1000;
            for (seed, keep) in [
                (7u64, &(|_: NodeId| true) as &dyn Fn(NodeId) -> bool),
                (12345, &keep_even as &dyn Fn(NodeId) -> bool),
            ] {
                let q = unit(values(seed, 16));
                let (gpu, backend) = index.brute_force_where_with(&q, 25, keep, 0);
                if backend != ExactBackend::Gpu {
                    // The GPU lane sets this so a broken GPU path fails
                    // rather than skips (.github/workflows/gpu-ci.yml).
                    if cfg!(feature = "gpu")
                        && std::env::var("SAMYAMA_REQUIRE_GPU").as_deref() == Ok("1")
                    {
                        panic!("SAMYAMA_REQUIRE_GPU=1 but the exact scan answered on the CPU");
                    }
                    println!("no GPU adapter (or built without --features gpu); GPU-vs-CPU parity skipped");
                    return;
                }
                let (cpu, cpu_backend) = index.brute_force_where_with(&q, 25, keep, usize::MAX);
                assert_eq!(cpu_backend, ExactBackend::Cpu);
                assert_eq!(gpu.len(), cpu.len(), "{metric:?}");

                let cpu_all = index
                    .brute_force_where_with(&q, usize::MAX, keep, usize::MAX)
                    .0;
                let cpu_distance = |id: NodeId| {
                    cpu_all
                        .iter()
                        .find(|(n, _)| *n == id)
                        .map(|(_, d)| *d)
                        .unwrap()
                };
                for (i, ((gid, gd), (cid, cd))) in gpu.iter().zip(&cpu).enumerate() {
                    assert!(
                        (gd - cd).abs() < EPS,
                        "{metric:?} rank {i}: gpu {gd} vs cpu {cd}"
                    );
                    if gid != cid {
                        let d = cpu_distance(*gid);
                        assert!(
                            (d - cd).abs() < EPS,
                            "{metric:?} rank {i}: gpu {gid:?} at {d} is not a near-tie of cpu {cid:?} at {cd}"
                        );
                    }
                }
                if seed == 7 {
                    let pos = |id: u64| gpu.iter().position(|(n, _)| n.as_u64() == id);
                    if let (Some(a), Some(b)) = (pos(7), pos(1000)) {
                        assert!(a < b, "{metric:?}: an exact tie must keep stored order");
                    }
                }
            }
        }
    }
}
