//! The arithmetic of the modern-embedding scale benchmark (#1984): the memory
//! estimate checked before a load, the operating point a cost is quoted at,
//! dollars per million queries, and the NDS-05 verdict.
//!
//! Shared by `benches/vector_scale_benchmark.rs` and checked by
//! `tests/vector_scale.rs`. A bench target cannot run its own tests, and every
//! figure here is one a reader will quote, so the code that derives it lives
//! where a test can reach it.

#![allow(dead_code)]

use std::io::Read;
use std::path::Path;

// ------------------------------------------------------------------- shape

/// Vectors and dimension of a `.fvecs` file, read from its first record's
/// header and the file length, without reading the vectors.
///
/// What lets the memory check run before a 30 GB load rather than after it.
/// A length that is not a whole number of records is refused: the file is cut
/// short or is not `.fvecs`, and either way the count would be wrong.
pub fn fvecs_shape(path: &Path) -> Result<(usize, usize), String> {
    let mut f = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let len = f
        .metadata()
        .map_err(|e| format!("{}: {e}", path.display()))?
        .len();
    let mut head = [0u8; 4];
    f.read_exact(&mut head)
        .map_err(|_| format!("{}: shorter than one record header", path.display()))?;
    let dim = i32::from_le_bytes(head);
    if dim <= 0 {
        return Err(format!(
            "{}: first record has dimension {dim}",
            path.display()
        ));
    }
    let n = shape_from_len(len, dim as usize).ok_or_else(|| {
        format!(
            "{}: {len} bytes is not a whole number of {dim}-d records",
            path.display()
        )
    })?;
    Ok((n, dim as usize))
}

/// Records in a `.fvecs` file of `len` bytes whose vectors have `dim`
/// dimensions, or `None` when the length does not divide evenly.
pub fn shape_from_len(len: u64, dim: usize) -> Option<usize> {
    let record = 4 + 4 * dim as u64;
    len.is_multiple_of(record)
        .then_some((len / record) as usize)
}

// ------------------------------------------------------------------ memory

/// Bytes per HNSW link as `VectorIndex::graph_bytes` counts them: the `Arc`
/// in the neighbour list and the `PointWithOrder` behind it.
pub const LINK_BYTES: usize = 40;

/// Bytes per node that are neither its vector nor its links: the node record,
/// its two small properties, and its entries in the store's and the index's
/// id maps. An allowance, not a measurement; it is small beside the vector
/// copies at 768 dimensions and above, which is where this check matters.
pub const NODE_OVERHEAD_BYTES: usize = 512;

/// Full-precision copies of every vector a bench run holds at its peak:
/// the base set read from disk, the node property, the HNSW graph's copy and
/// the index's own copy kept for persistence (`VectorIndex::vector_bytes`
/// counts the last two). A server holds three; the bench holds the base set
/// as well, to score answers against.
pub const BENCH_VECTOR_COPIES: usize = 4;

/// What loading `n` vectors of `dim` dimensions and building an HNSW index of
/// out-degree `m` over them is expected to need, in bytes.
///
/// Layer 0 holds up to `2·m` links per vector; the upper layers add about
/// `1/(m-1)` of that again, which is folded into the allowance.
pub fn estimate_bytes(n: usize, dim: usize, m: usize, copies: usize) -> u128 {
    let per_vector =
        (copies * dim * 4) as u128 + (2 * m * LINK_BYTES) as u128 + NODE_OVERHEAD_BYTES as u128;
    per_vector * n as u128
}

/// `MemAvailable` from the text of `/proc/meminfo`, in bytes.
pub fn parse_mem_available(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find(|l| l.starts_with("MemAvailable:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|kb| kb.parse::<u64>().ok())
        .map(|kb| kb * 1024)
}

/// Memory this host can give the run, in bytes; `None` where the platform
/// does not say (off Linux). Unknown is reported as unknown, not guessed.
pub fn available_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| parse_mem_available(&s))
}

/// Whether a run is expected to fit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fit {
    /// The estimate is within the available memory, with the headroom kept.
    Fits,
    /// It is not: the run would swap or be killed part way through a load
    /// that can take an hour, so it is refused before it starts.
    TooBig,
    /// The host did not say how much it has.
    Unknown,
}

/// Compare an estimate with what the host has, keeping `headroom` (a fraction
/// such as 0.1) of it free for the allocator, the page cache and the queries.
pub fn memory_fit(estimate: u128, available: Option<u64>, headroom: f64) -> Fit {
    match available {
        None => Fit::Unknown,
        Some(avail) => {
            let usable = avail as f64 * (1.0 - headroom.clamp(0.0, 1.0));
            if estimate as f64 <= usable {
                Fit::Fits
            } else {
                Fit::TooBig
            }
        }
    }
}

pub fn gib(bytes: u128) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

// ------------------------------------------------------------ operating point

/// One serial point of the ef sweep, with both recalls the report quotes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SweepPoint {
    pub ef: usize,
    pub recall_at_100: f64,
    pub recall_at_10: f64,
    /// Serial queries per second at k = 100.
    pub qps: f64,
}

/// The ef the concurrency sweep and the cost are measured at.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Operating {
    pub point: SweepPoint,
    /// Whether `point` reaches the recall target. When no point does, the
    /// highest-recall one is used and the report says the target was missed;
    /// a cost quoted at a recall nobody would deploy is labelled as such.
    pub meets_target: bool,
}

/// The fastest serial point whose recall@100 reaches `target`; failing that,
/// the point with the highest recall@100. `None` only for an empty sweep.
pub fn operating_point(points: &[SweepPoint], target: f64) -> Option<Operating> {
    let met = points
        .iter()
        .filter(|p| p.recall_at_100 >= target)
        .max_by(|a, b| a.qps.total_cmp(&b.qps).then(b.ef.cmp(&a.ef)));
    if let Some(p) = met {
        return Some(Operating {
            point: *p,
            meets_target: true,
        });
    }
    points
        .iter()
        .max_by(|a, b| {
            a.recall_at_100
                .total_cmp(&b.recall_at_100)
                .then(b.ef.cmp(&a.ef))
        })
        .map(|p| Operating {
            point: *p,
            meets_target: false,
        })
}

/// The highest throughput over a concurrency sweep, and the thread count it
/// was reached at. Points with failed queries are not eligible: a fast error
/// is not throughput.
pub fn max_qps(points: &[(usize, f64, usize)]) -> Option<(usize, f64)> {
    points
        .iter()
        .filter(|(_, qps, errors)| *errors == 0 && qps.is_finite() && *qps > 0.0)
        .max_by(|a, b| a.1.total_cmp(&b.1).then(b.0.cmp(&a.0)))
        .map(|(threads, qps, _)| (*threads, *qps))
}

// -------------------------------------------------------------------- cost

/// Dollars to answer a million queries at `qps` sustained on a host costing
/// `price_per_hour`. Infinite when nothing was answered.
pub fn dollars_per_million(qps: f64, price_per_hour: f64) -> f64 {
    if qps <= 0.0 || !qps.is_finite() {
        f64::INFINITY
    } else {
        1e6 / qps / 3600.0 * price_per_hour
    }
}

// ------------------------------------------------------------------ NDS-05

/// The NDS-05 target: recall@10 of at least this ...
pub const NDS05_RECALL_AT_10: f64 = 0.95;
/// ... at this many vectors.
pub const NDS05_VECTORS: usize = 10_000_000;

/// What a run says about NDS-05.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Nds05 {
    /// Measured at 10M or more, and recall@10 reached the bar.
    Met,
    /// Measured at 10M or more, and recall@10 fell short.
    NotMet,
    /// Measured below 10M. Whatever the recall, this is not evidence for
    /// the target; `bar_reached` says only what happened at this size.
    NotEvidence { bar_reached: bool },
}

pub fn nds05(vectors: usize, recall_at_10: f64) -> Nds05 {
    let reached = recall_at_10 >= NDS05_RECALL_AT_10;
    if vectors < NDS05_VECTORS {
        Nds05::NotEvidence {
            bar_reached: reached,
        }
    } else if reached {
        Nds05::Met
    } else {
        Nds05::NotMet
    }
}

/// The line the report prints, naming the size it was measured at.
pub fn nds05_line(vectors: usize, recall_at_10: f64, ef: usize) -> String {
    let at = format!("recall@10 {recall_at_10:.4} at {vectors} vectors (ef={ef})");
    match nds05(vectors, recall_at_10) {
        Nds05::Met => format!("NDS-05: MET -- {at}; target is >= {NDS05_RECALL_AT_10} at 10M."),
        Nds05::NotMet => {
            format!("NDS-05: NOT MET -- {at}; target is >= {NDS05_RECALL_AT_10} at 10M.")
        }
        Nds05::NotEvidence { bar_reached } => format!(
            "NDS-05: NOT EVIDENCE -- {at} {} the {NDS05_RECALL_AT_10} bar, but NDS-05 is stated at \
             {NDS05_VECTORS} vectors and this run had fewer.",
            if bar_reached { "reaches" } else { "misses" }
        ),
    }
}
