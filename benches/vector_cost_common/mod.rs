//! The arithmetic of the vector cost profile (#1989): which quantizations it
//! profiles, the theoretical bytes per vector it attributes overhead against,
//! the recall-target operating point, dollars per million queries, and a
//! resident-memory sampler.
//!
//! Shared by `benches/vector_cost_profile.rs` and checked by
//! `tests/vector_cost_profile.rs`: a bench target cannot run its own tests,
//! and a cost figure is quoted, so the code that derives it is tested here.

#![allow(dead_code)]

use samyama::vector::index::Quantization;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

/// Every quantization the profile measures, with the name its rows carry.
///
/// Adding a quantization to the index is one line here. The name must be one
/// `Quantization::parse` accepts, which `tests/vector_cost_profile.rs` checks,
/// so `--quantization` can select the row by the name it is printed under.
pub const QUANTIZATIONS: &[(&str, Quantization)] =
    &[("f32", Quantization::None), ("fp16", Quantization::Fp16)];

/// The default instance price, in US dollars per hour.
///
/// AWS `r6i.xlarge` (4 vCPU, 32 GiB), Linux on-demand in `us-east-1`, as
/// listed at the time of writing. An assumption, not a measurement: pass
/// `--dollars-per-hour` with the price of the host the bench actually ran on
/// (spot or on-demand), or the dollar columns describe a machine nobody used.
pub const DEFAULT_DOLLARS_PER_HOUR: f64 = 0.252;
pub const DEFAULT_INSTANCE: &str = "AWS r6i.xlarge on-demand, us-east-1";

/// The bytes a vector needs in theory: its values at the quantization's width,
/// plus its layer-0 links as 4-byte ids (`dim·bytes + M·2·4`).
///
/// The floor any HNSW pays. What the index actually holds above it is
/// overhead, and the profile reports the ratio so the overhead is
/// attributable rather than folded into one figure.
pub fn theoretical_bytes_per_vector(dim: usize, bytes_per_value: usize, m: usize) -> usize {
    dim * bytes_per_value + m * 2 * 4
}

/// One point of an ef sweep, reduced to what the cost needs.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Sweep {
    pub ef: usize,
    pub recall: f64,
    pub qps: f64,
}

/// The point a deployment would run at to meet `target` recall: the fastest
/// point that reaches it. `None` when no point does — then there is no cost to
/// quote, because the target was not met at any width tried.
pub fn operating_point(points: &[Sweep], target: f64) -> Option<Sweep> {
    points
        .iter()
        .filter(|p| p.recall >= target && p.qps > 0.0)
        .copied()
        .max_by(|a, b| a.qps.total_cmp(&b.qps).then(b.ef.cmp(&a.ef)))
}

/// Hours of one host to answer `queries` at `qps` sustained.
pub fn hours_for(queries: f64, qps: f64) -> f64 {
    if qps <= 0.0 {
        f64::INFINITY
    } else {
        queries / qps / 3600.0
    }
}

/// Dollars to answer a million queries at `qps` on a host costing
/// `dollars_per_hour`. Infinite when nothing was answered.
pub fn dollars_per_million(qps: f64, dollars_per_hour: f64) -> f64 {
    hours_for(1e6, qps) * dollars_per_hour
}

/// Dollars of host time spent on `elapsed` of work, such as an index build.
pub fn dollars_for(elapsed: Duration, dollars_per_hour: f64) -> f64 {
    elapsed.as_secs_f64() / 3600.0 * dollars_per_hour
}

/// Samples resident memory on a background thread and keeps the maximum.
///
/// The issue asks for peak RSS sampled over the run rather than a single
/// reading at the end, which misses the transient peak of a build or a load.
/// `read` returns `None` where the platform cannot say (off Linux), and the
/// peak is then `None` too: unknown is reported as unknown.
pub struct RssSampler {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<Option<u64>>,
}

impl RssSampler {
    pub fn start(interval: Duration, read: fn() -> Option<u64>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::spawn(move || {
            let mut peak = read();
            // Sleep in short slices so `stop` returns promptly whatever the
            // interval.
            let slice = interval.min(Duration::from_millis(50));
            let mut waited = Duration::ZERO;
            while !flag.load(Ordering::Relaxed) {
                std::thread::sleep(slice);
                waited += slice;
                if waited >= interval {
                    waited = Duration::ZERO;
                    peak = max_known(peak, read());
                }
            }
            max_known(peak, read())
        });
        Self { stop, handle }
    }

    /// Stop sampling and return the largest reading, including one taken now.
    pub fn stop(self) -> Option<u64> {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap_or(None)
    }
}

fn max_known(a: Option<u64>, b: Option<u64>) -> Option<u64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, None) | (None, x) => x,
    }
}
