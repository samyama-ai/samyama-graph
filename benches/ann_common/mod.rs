//! The parts of the recall–QPS harness (#1983) that do not need a dataset:
//! the `.fvecs`/`.ivecs` readers, exact ground truth, recall@k, latency
//! percentiles and the sweep loop.
//!
//! Shared by `benches/ann_benchmark.rs`, which runs the sweep, and
//! `tests/ann_harness.rs`, which checks this file in CI. A bench target cannot
//! run its own tests, so the logic lives here rather than in the bench.

#![allow(dead_code)]

use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

/// Row-major vectors of one dimension.
#[derive(Clone, Debug, PartialEq)]
pub struct Vectors {
    pub dim: usize,
    pub data: Vec<f32>,
}

impl Vectors {
    pub fn len(&self) -> usize {
        self.data.len().checked_div(self.dim).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn row(&self, i: usize) -> &[f32] {
        &self.data[i * self.dim..(i + 1) * self.dim]
    }
}

/// Neighbour lists, one per query, as indexes into the base set.
#[derive(Clone, Debug, PartialEq)]
pub struct Neighbours {
    pub width: usize,
    pub ids: Vec<u32>,
}

impl Neighbours {
    pub fn len(&self) -> usize {
        self.ids.len().checked_div(self.width).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn row(&self, i: usize) -> &[u32] {
        &self.ids[i * self.width..(i + 1) * self.width]
    }
}

// ------------------------------------------------------------------ file I/O
//
// The TEXMEX layout used by SIFT/GIST and by ann-benchmarks' converters: each
// record is a little-endian `i32` dimension followed by that many 4-byte
// values (`f32` for `.fvecs`, `i32` for `.ivecs`). Every record must carry the
// same dimension; a file that does not is refused rather than read with the
// first record's width, which would misalign every vector after it.

fn read_records<T>(
    path: &Path,
    limit: Option<usize>,
    decode: impl Fn([u8; 4]) -> T,
) -> Result<(usize, Vec<T>), String> {
    let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut r = BufReader::with_capacity(1 << 20, file);
    let mut dim: Option<usize> = None;
    let mut out = Vec::new();
    let mut rows = 0usize;
    let mut head = [0u8; 4];
    loop {
        if limit.is_some_and(|l| rows >= l) {
            break;
        }
        match r.read_exact(&mut head) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("{}: {e}", path.display())),
        }
        let d = i32::from_le_bytes(head);
        if d <= 0 {
            return Err(format!(
                "{}: record {rows} has dimension {d}",
                path.display()
            ));
        }
        let d = d as usize;
        match dim {
            None => dim = Some(d),
            Some(first) if first != d => {
                return Err(format!(
                    "{}: record {rows} has dimension {d}, the first had {first}",
                    path.display()
                ))
            }
            Some(_) => {}
        }
        let mut buf = vec![0u8; d * 4];
        r.read_exact(&mut buf)
            .map_err(|_| format!("{}: record {rows} is cut short", path.display()))?;
        out.extend(
            buf.chunks_exact(4)
                .map(|c| decode([c[0], c[1], c[2], c[3]])),
        );
        rows += 1;
    }
    Ok((dim.unwrap_or(0), out))
}

/// Read up to `limit` vectors from a `.fvecs` file.
pub fn read_fvecs(path: &Path, limit: Option<usize>) -> Result<Vectors, String> {
    let (dim, data) = read_records(path, limit, f32::from_le_bytes)?;
    Ok(Vectors { dim, data })
}

/// Read up to `limit` neighbour lists from a `.ivecs` file.
pub fn read_ivecs(path: &Path, limit: Option<usize>) -> Result<Neighbours, String> {
    let (width, raw) = read_records(path, limit, i32::from_le_bytes)?;
    let ids = raw
        .into_iter()
        .map(|v| u32::try_from(v).map_err(|_| format!("{}: negative id {v}", path.display())))
        .collect::<Result<_, _>>()?;
    Ok(Neighbours { width, ids })
}

fn write_records(
    path: &Path,
    dim: usize,
    rows: impl Iterator<Item = Vec<[u8; 4]>>,
) -> Result<(), String> {
    let file = std::fs::File::create(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut w = BufWriter::new(file);
    for row in rows {
        w.write_all(&(dim as i32).to_le_bytes())
            .and_then(|_| row.iter().try_for_each(|b| w.write_all(b)))
            .map_err(|e| format!("{}: {e}", path.display()))?;
    }
    w.flush().map_err(|e| format!("{}: {e}", path.display()))
}

pub fn write_fvecs(path: &Path, v: &Vectors) -> Result<(), String> {
    write_records(
        path,
        v.dim,
        (0..v.len()).map(|i| v.row(i).iter().map(|x| x.to_le_bytes()).collect()),
    )
}

pub fn write_ivecs(path: &Path, n: &Neighbours) -> Result<(), String> {
    write_records(
        path,
        n.width,
        (0..n.len()).map(|i| n.row(i).iter().map(|x| (*x as i32).to_le_bytes()).collect()),
    )
}

// ------------------------------------------------------------ ground truth

/// The metric the dataset's ground truth was computed with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metric {
    L2,
    Cosine,
    Dot,
}

impl Metric {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "l2" | "euclidean" => Ok(Metric::L2),
            "cosine" | "angular" => Ok(Metric::Cosine),
            "dot" | "ip" | "inner" => Ok(Metric::Dot),
            other => Err(format!("unknown metric `{other}`; use l2, cosine or dot")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Metric::L2 => "l2",
            Metric::Cosine => "cosine",
            Metric::Dot => "dot",
        }
    }

    /// Smaller is nearer, for every metric.
    pub fn distance(self, a: &[f32], b: &[f32]) -> f32 {
        match self {
            Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
            Metric::Dot => -a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>(),
            Metric::Cosine => {
                let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
                for (x, y) in a.iter().zip(b) {
                    dot += x * y;
                    na += x * x;
                    nb += y * y;
                }
                let denom = na.sqrt() * nb.sqrt();
                if denom == 0.0 {
                    1.0
                } else {
                    1.0 - dot / denom
                }
            }
        }
    }
}

/// The exact `k` nearest base vectors of each query, by brute force.
///
/// Used when the dataset has no ground truth, and when `--limit` truncates the
/// base set: the published ground truth is for the full set, so its
/// neighbours may lie past the truncation and would count as misses.
pub fn exact_ground_truth(
    base: &Vectors,
    queries: &Vectors,
    k: usize,
    metric: Metric,
) -> Neighbours {
    use rayon::prelude::*;
    let k = k.min(base.len());
    let rows: Vec<Vec<u32>> = (0..queries.len())
        .into_par_iter()
        .map(|qi| {
            let q = queries.row(qi);
            let mut scored: Vec<(f32, u32)> = (0..base.len())
                .map(|bi| (metric.distance(q, base.row(bi)), bi as u32))
                .collect();
            if k < scored.len() {
                scored.select_nth_unstable_by(k, |a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
                scored.truncate(k);
            }
            scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
            scored.into_iter().map(|(_, id)| id).collect()
        })
        .collect();
    Neighbours {
        width: k,
        ids: rows.into_iter().flatten().collect(),
    }
}

/// Deterministic Gaussian-ish vectors in clusters, for a run without a dataset.
///
/// Uniform random vectors are a poor stand-in: in high dimension every point
/// is about equally far from every other, which makes recall look worse than
/// on any real embedding. Clustered points have the local structure HNSW is
/// built for.
pub fn synthetic(n: usize, dim: usize, seed: u64) -> Vectors {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let clusters = (n / 100).clamp(1, 256);
    let centres: Vec<Vec<f32>> = (0..clusters)
        .map(|_| (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect())
        .collect();
    let mut data = Vec::with_capacity(n * dim);
    for i in 0..n {
        let c = &centres[i % clusters];
        for x in c {
            // Sum of three uniforms: cheap, bell-shaped, no extra dependency.
            let noise: f32 = (0..3).map(|_| rng.gen_range(-0.1f32..0.1)).sum();
            data.push(x + noise);
        }
    }
    Vectors { dim, data }
}

// ------------------------------------------------------------------ metrics

/// The share of the true `k` nearest that `found` contains.
///
/// Set membership, not rank order: ann-benchmarks' recall@k. Only the first
/// `k` of each list count, so a search that returns more than `k` is not
/// credited for the extras, and one that returns fewer is charged for them.
pub fn recall_at_k(found: &[u32], truth: &[u32], k: usize) -> f64 {
    let k = k.min(truth.len());
    if k == 0 {
        return 1.0;
    }
    let truth: std::collections::HashSet<u32> = truth[..k].iter().copied().collect();
    let mut seen = std::collections::HashSet::new();
    let hits = found
        .iter()
        .take(k)
        .filter(|id| truth.contains(id) && seen.insert(**id))
        .count();
    hits as f64 / k as f64
}

/// Nearest-rank percentile of `sorted`, which must be sorted ascending.
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// One point on the recall–QPS curve.
#[derive(Clone, Debug)]
pub struct Point {
    pub ef: usize,
    pub threads: usize,
    pub recall: f64,
    pub qps: f64,
    pub p50: Duration,
    pub p99: Duration,
    pub mean: Duration,
    /// Queries whose search returned an error, counted as recall 0.
    pub errors: usize,
}

/// Run every query once through `search` on `threads` threads, and score the
/// answers against `truth`.
///
/// QPS is queries over wall-clock time for the whole set, so at more than one
/// thread it is throughput, not the inverse of latency. Queries are dealt out
/// round-robin so every thread gets a share of every part of the query set.
pub fn run_point<F>(
    search: &F,
    queries: &Vectors,
    truth: &Neighbours,
    k: usize,
    ef: usize,
    threads: usize,
) -> Point
where
    F: Fn(&[f32], usize, usize) -> Result<Vec<u32>, String> + Sync,
{
    let threads = threads.max(1);
    let n = queries.len();
    let started = Instant::now();
    // (query index, latency, answer or None on error), per thread.
    type Answered = Vec<(usize, Duration, Option<Vec<u32>>)>;
    let per_thread: Vec<Answered> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    (t..n)
                        .step_by(threads)
                        .map(|qi| {
                            let at = Instant::now();
                            let got = search(queries.row(qi), k, ef).ok();
                            (qi, at.elapsed(), got)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().expect("search thread panicked"))
            .collect()
    });
    let wall = started.elapsed();

    let mut latencies = Vec::with_capacity(n);
    let mut recall_sum = 0.0;
    let mut errors = 0;
    for (qi, took, got) in per_thread.into_iter().flatten() {
        latencies.push(took);
        match got {
            Some(ids) => recall_sum += recall_at_k(&ids, truth.row(qi), k),
            None => errors += 1,
        }
    }
    latencies.sort();
    let total: Duration = latencies.iter().sum();
    Point {
        ef,
        threads,
        recall: if n == 0 { 0.0 } else { recall_sum / n as f64 },
        qps: if wall.is_zero() {
            0.0
        } else {
            n as f64 / wall.as_secs_f64()
        },
        p50: percentile(&latencies, 50.0),
        p99: percentile(&latencies, 99.0),
        mean: if n == 0 {
            Duration::ZERO
        } else {
            total / n as u32
        },
        errors,
    }
}

/// The points no other point beats on both recall and QPS, by ascending recall.
pub fn pareto_frontier(points: &[Point]) -> Vec<Point> {
    let mut sorted: Vec<Point> = points.to_vec();
    sorted.sort_by(|a, b| b.qps.total_cmp(&a.qps).then(b.recall.total_cmp(&a.recall)));
    let mut frontier: Vec<Point> = Vec::new();
    let mut best_recall = f64::NEG_INFINITY;
    for p in sorted {
        if p.recall > best_recall {
            best_recall = p.recall;
            frontier.push(p);
        }
    }
    frontier
}

// ------------------------------------------------------------------- memory

/// Resident set size and its peak, in bytes, from `/proc/self/status`.
/// `None` off Linux: the harness reports memory as unknown rather than guess.
pub fn rss_bytes() -> (Option<u64>, Option<u64>) {
    let Ok(s) = std::fs::read_to_string("/proc/self/status") else {
        return (None, None);
    };
    let field = |name: &str| {
        s.lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|kb| kb.parse::<u64>().ok())
            .map(|kb| kb * 1024)
    };
    (field("VmRSS:"), field("VmHWM:"))
}

// ---------------------------------------------------------------- the store
//
// Both search paths read the same store. `Direct` calls the store's search, so
// it measures the index and nothing else. `Cypher` sends the same search as a
// `db.index.vector.queryNodes` query through `QueryEngine::execute_with_params`,
// the call HTTP `/api/query` and RESP `GRAPH.QUERY` make, so it adds parsing,
// planning and result materialisation but not the network. The competitor
// suite measures over the network; neither path here is comparable to it.

use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::{BoundParams, QueryEngine, Value};

pub const LABEL: &str = "Item";
pub const PROPERTY: &str = "embedding";

/// HNSW build parameters, as `CREATE VECTOR INDEX ... OPTIONS` takes them.
#[derive(Clone, Copy, Debug)]
pub struct Build {
    pub m: usize,
    pub ef_construction: usize,
}

/// A store holding `base`, and how long its index took to build.
pub struct Loaded {
    pub store: GraphStore,
    /// Node id -> index into the base set, for scoring against ground truth.
    pub position: std::collections::HashMap<NodeId, u32>,
    pub load: Duration,
    pub build: Duration,
}

pub fn samyama_metric(m: Metric) -> samyama::vector::DistanceMetric {
    match m {
        Metric::L2 => samyama::vector::DistanceMetric::L2,
        Metric::Cosine => samyama::vector::DistanceMetric::Cosine,
        Metric::Dot => samyama::vector::DistanceMetric::InnerProduct,
    }
}

/// Load every base vector as an `:Item` node, then build the index over them.
///
/// The order `CREATE VECTOR INDEX` after a bulk load runs: nodes first, then
/// the index registered and filled by `rebuild_vector_index`. Build time is
/// that second step only, so it is comparable to a competitor's index build
/// and not to its ingest.
pub fn load(base: &Vectors, metric: Metric, build: Build) -> Result<Loaded, String> {
    let mut store = GraphStore::new();
    let mut position = std::collections::HashMap::with_capacity(base.len());
    let started = Instant::now();
    for i in 0..base.len() {
        let mut props = std::collections::HashMap::new();
        props.insert("i".to_string(), PropertyValue::Integer(i as i64));
        props.insert(
            PROPERTY.to_string(),
            PropertyValue::Vector(base.row(i).to_vec()),
        );
        let id = store.create_node_with_properties("default", vec![Label::new(LABEL)], props);
        position.insert(id, i as u32);
    }
    let load = started.elapsed();

    let started = Instant::now();
    store
        .create_vector_index_with_params(
            None,
            LABEL,
            PROPERTY,
            base.dim,
            samyama_metric(metric),
            samyama::vector::index::Quantization::None,
            samyama::vector::HnswParams {
                m: build.m,
                ef_construction: build.ef_construction,
                ef: None,
            },
        )
        .map_err(|e| e.to_string())?;
    store.rebuild_vector_index();
    let build = started.elapsed();
    Ok(Loaded {
        store,
        position,
        load,
        build,
    })
}

/// Which code path a query takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchPath {
    Direct,
    Cypher,
}

impl SearchPath {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "direct" => Ok(SearchPath::Direct),
            "cypher" => Ok(SearchPath::Cypher),
            other => Err(format!("unknown path `{other}`; use direct or cypher")),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            SearchPath::Direct => "direct",
            SearchPath::Cypher => "cypher",
        }
    }
}

/// The store's own search at width `ef`, answered as base-set positions.
pub fn direct_search(l: &Loaded, q: &[f32], k: usize, ef: usize) -> Result<Vec<u32>, String> {
    let hits = l
        .store
        .vector_search_where(LABEL, PROPERTY, q, k, Some(ef), |_| true)
        .map_err(|e| e.to_string())?;
    hits.into_iter()
        .map(|(id, _)| {
            l.position
                .get(&id)
                .copied()
                .ok_or_else(|| format!("unknown node {id:?}"))
        })
        .collect()
}

/// The same search as a Cypher `queryNodes` call, the vector as `$q`.
///
/// `k` and `ef` are written into the query text rather than passed as
/// parameters: each sweep point then has one query string, which is how a
/// client would send it, and the plan cache sees one entry per point.
pub fn cypher_search(
    l: &Loaded,
    engine: &QueryEngine,
    q: &[f32],
    k: usize,
    ef: usize,
) -> Result<Vec<u32>, String> {
    let text = format!(
        "CALL db.index.vector.queryNodes('{LABEL}', '{PROPERTY}', $q, {k}, {{ef: {ef}}}) \
         YIELD node RETURN node.i AS i"
    );
    let mut params = BoundParams::new();
    params.insert("q".to_string(), PropertyValue::Vector(q.to_vec()));
    let batch = engine
        .execute_with_params(&text, &l.store, &params)
        .map_err(|e| e.to_string())?;
    batch
        .records
        .iter()
        .map(|r| match r.get("i") {
            Some(Value::Property(PropertyValue::Integer(i))) => Ok(*i as u32),
            other => Err(format!("row without an integer `i`: {other:?}")),
        })
        .collect()
}
