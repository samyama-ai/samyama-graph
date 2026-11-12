//! The parts of the streaming / churn benchmark (#1986) that do not print:
//! workload generation, the Big-ANN streaming runbook reader, and the loop
//! that applies a workload to a `VectorIndex` and scores it after every search
//! step.
//!
//! Shared by `benches/vector_churn_benchmark.rs`, which runs it and reports,
//! and `tests/vector_churn_harness.rs`, which checks it in CI. A bench target
//! cannot run its own tests, so the logic lives here rather than in the bench.
//!
//! Every module that includes this one must also include
//! `benches/ann_common/mod.rs` as `ann_common` at its crate root: the metric,
//! the vector container and the RSS reader come from there.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use samyama::graph::types::NodeId;
use samyama::vector::index::Quantization;
use samyama::vector::{HnswParams, VectorIndex};

use crate::ann_common::{self, Metric, Vectors};

// ------------------------------------------------------------------ steps

/// A run of tags or data rows: a contiguous range, as a runbook names them,
/// or an explicit list, as the generated churn picks them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ids {
    /// `start..end`, end exclusive.
    Range(u64, u64),
    List(Vec<u64>),
}

impl Ids {
    pub fn len(&self) -> usize {
        match self {
            Ids::Range(s, e) => e.saturating_sub(*s) as usize,
            Ids::List(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        match self {
            Ids::Range(s, e) => Box::new(*s..*e),
            Ids::List(v) => Box::new(v.iter().copied()),
        }
    }

    /// One past the largest id, or 0 when empty.
    pub fn end(&self) -> u64 {
        match self {
            Ids::Range(s, e) if e > s => *e,
            Ids::Range(..) => 0,
            Ids::List(v) => v.iter().max().map(|m| m + 1).unwrap_or(0),
        }
    }
}

/// One step of a workload. A tag is the id a vector is known by (the node id
/// in the index); a row is a position in the workload's data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Add new tags, the i-th with the i-th row's vector.
    Insert { tags: Ids, rows: Ids },
    /// Remove live tags.
    Delete { tags: Ids },
    /// Give live tags new vectors: an update in place.
    Replace { tags: Ids, rows: Ids },
    /// Score the index with the given query set.
    Search { queries: usize },
}

/// A workload: the vectors it draws from, its query sets, and its steps.
pub struct Workload {
    pub name: String,
    pub data: Vectors,
    pub queries: Vec<Vectors>,
    pub steps: Vec<Step>,
}

/// One past the largest data row any step reads.
pub fn rows_needed(steps: &[Step]) -> usize {
    steps
        .iter()
        .map(|s| match s {
            Step::Insert { rows, .. } | Step::Replace { rows, .. } => rows.end() as usize,
            _ => 0,
        })
        .max()
        .unwrap_or(0)
}

// --------------------------------------------------------- generated churn

/// Churn at random: `n` tags loaded, then `cycles` cycles that each touch
/// `per_cycle` distinct live tags picked at random. The first half of them
/// (rounded up) are deleted and replaced by new tags with new vectors; the
/// rest are given a new vector in place. The live count stays `n`.
///
/// Rows are consumed in order: `0..n` for the load, then one per touched tag,
/// so the workload reads `n + cycles * per_cycle` rows. Searches use query
/// set 0.
pub fn churn_steps(n: usize, cycles: usize, per_cycle: usize, seed: u64) -> Vec<Step> {
    let per_cycle = per_cycle.min(n);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut steps = vec![
        Step::Insert {
            tags: Ids::Range(0, n as u64),
            rows: Ids::Range(0, n as u64),
        },
        Step::Search { queries: 0 },
    ];
    let mut live: Vec<u64> = (0..n as u64).collect();
    let mut next_tag = n as u64;
    let mut next_row = n as u64;
    for _ in 0..cycles {
        let picked: Vec<usize> =
            rand::seq::index::sample(&mut rng, live.len(), per_cycle).into_vec();
        let deleted = per_cycle.div_ceil(2);
        let gone: Vec<u64> = picked[..deleted].iter().map(|i| live[*i]).collect();
        let updated: Vec<u64> = picked[deleted..].iter().map(|i| live[*i]).collect();
        let new_tags: Vec<u64> = (next_tag..next_tag + deleted as u64).collect();
        for (slot, tag) in picked[..deleted].iter().zip(&new_tags) {
            live[*slot] = *tag;
        }
        next_tag += deleted as u64;
        steps.push(Step::Delete {
            tags: Ids::List(gone),
        });
        steps.push(Step::Insert {
            tags: Ids::List(new_tags),
            rows: Ids::Range(next_row, next_row + deleted as u64),
        });
        next_row += deleted as u64;
        if !updated.is_empty() {
            let rows = Ids::Range(next_row, next_row + updated.len() as u64);
            next_row += updated.len() as u64;
            steps.push(Step::Replace {
                tags: Ids::List(updated),
                rows,
            });
        }
        steps.push(Step::Search { queries: 0 });
    }
    steps
}

/// A sliding window: `n` tags loaded, then `cycles` cycles that each delete
/// the `per_cycle` oldest tags and insert `per_cycle` new ones. Tag and row
/// are the same number throughout. The search after cycle `c` uses query set
/// `min(c, query_sets - 1)`.
///
/// Over data whose distribution moves with the row number this is the drift
/// runbook the issue asks for (research idea #31): inserts follow a moving
/// distribution and deletes remove the oldest items.
pub fn window_steps(n: usize, cycles: usize, per_cycle: usize, query_sets: usize) -> Vec<Step> {
    let last = query_sets.saturating_sub(1);
    let mut steps = vec![
        Step::Insert {
            tags: Ids::Range(0, n as u64),
            rows: Ids::Range(0, n as u64),
        },
        Step::Search { queries: 0 },
    ];
    let per = per_cycle.min(n) as u64;
    for c in 0..cycles as u64 {
        steps.push(Step::Delete {
            tags: Ids::Range(c * per, (c + 1) * per),
        });
        let start = n as u64 + c * per;
        steps.push(Step::Insert {
            tags: Ids::Range(start, start + per),
            rows: Ids::Range(start, start + per),
        });
        steps.push(Step::Search {
            queries: ((c + 1) as usize).min(last),
        });
    }
    steps
}

/// Clustered vectors whose cluster centres can move: a vector drawn at time
/// `t` comes from `centre + t * direction`, with the same small noise as
/// `ann_common::synthetic`. At `distance = 0` nothing moves.
pub struct Clusters {
    dim: usize,
    centres: Vec<Vec<f32>>,
    direction: Vec<f32>,
}

impl Clusters {
    pub fn new(dim: usize, clusters: usize, distance: f32, seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let centres = (0..clusters.max(1))
            .map(|_| (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect())
            .collect();
        let raw: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0f32..1.0)).collect();
        let norm = raw
            .iter()
            .map(|x| x * x)
            .sum::<f32>()
            .sqrt()
            .max(f32::EPSILON);
        let direction = raw.iter().map(|x| x / norm * distance).collect();
        Self {
            dim,
            centres,
            direction,
        }
    }

    pub fn sample(&self, rng: &mut StdRng, t: f32, out: &mut Vec<f32>) {
        let c = &self.centres[rng.gen_range(0..self.centres.len())];
        for (x, d) in c.iter().zip(&self.direction) {
            let noise: f32 = (0..3).map(|_| rng.gen_range(-0.1f32..0.1)).sum();
            out.push(x + t * d + noise);
        }
    }

    /// `n` vectors, the i-th drawn at time `t(i)`.
    pub fn draw(&self, n: usize, seed: u64, t: impl Fn(usize, &mut StdRng) -> f32) -> Vectors {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut data = Vec::with_capacity(n * self.dim);
        for i in 0..n {
            let at = t(i, &mut rng);
            self.sample(&mut rng, at, &mut data);
        }
        Vectors {
            dim: self.dim,
            data,
        }
    }
}

fn clusters_for(n: usize) -> usize {
    (n / 100).clamp(1, 256)
}

/// Random churn over a stationary clustered distribution.
pub fn synthetic_churn(
    n: usize,
    dim: usize,
    cycles: usize,
    per_cycle: usize,
    queries: usize,
    seed: u64,
) -> Workload {
    let steps = churn_steps(n, cycles, per_cycle, seed);
    let gen = Clusters::new(dim, clusters_for(n), 0.0, seed);
    Workload {
        name: format!("synthetic-churn-{n}x{dim}"),
        data: gen.draw(rows_needed(&steps), seed ^ 0xDA7A, |_, _| 0.0),
        queries: vec![gen.draw(queries, seed ^ 0xC0FFEE, |_, _| 0.0)],
        steps,
    }
}

/// A sliding window over a distribution that moves `distance` (in the units
/// of the cluster centres, which lie in [-1, 1] per dimension) from the first
/// row to the last. The queries after each cycle are drawn from the time span
/// of the vectors then live, so they ask about the data the index holds.
pub fn synthetic_drift(
    n: usize,
    dim: usize,
    cycles: usize,
    per_cycle: usize,
    queries: usize,
    distance: f32,
    seed: u64,
) -> Workload {
    let steps = window_steps(n, cycles, per_cycle, cycles + 1);
    let total = rows_needed(&steps).max(1);
    let gen = Clusters::new(dim, clusters_for(n), distance, seed);
    let time = |row: usize| row as f32 / total as f32;
    let data = gen.draw(total, seed ^ 0xDA7A, |i, _| time(i));
    let per = per_cycle.min(n);
    let query_sets = (0..=cycles)
        .map(|c| {
            let (from, to) = (time(c * per), time(c * per + n));
            gen.draw(queries, seed ^ 0xC0FFEE ^ c as u64, |_, rng| {
                rng.gen_range(from..=to)
            })
        })
        .collect();
    Workload {
        name: format!("synthetic-drift-{n}x{dim}"),
        data,
        queries: query_sets,
        steps,
    }
}

// ----------------------------------------------------------------- file I/O

/// Read up to `limit` vectors from `path`: Big-ANN's `.fbin` (a `u32` count
/// and a `u32` dimension, then the `f32` values row by row, little-endian),
/// or the TEXMEX `.fvecs` that `ann_common` reads for anything else.
pub fn read_vectors(path: &std::path::Path, limit: Option<usize>) -> Result<Vectors, String> {
    if path.extension().and_then(|e| e.to_str()) != Some("fbin") {
        return ann_common::read_fvecs(path, limit);
    }
    use std::io::Read;
    let err = |e: std::io::Error| format!("{}: {e}", path.display());
    let file = std::fs::File::open(path).map_err(err)?;
    let mut r = std::io::BufReader::with_capacity(1 << 20, file);
    let mut head = [0u8; 8];
    r.read_exact(&mut head)
        .map_err(|_| format!("{}: shorter than its 8-byte header", path.display()))?;
    let n = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let dim = u32::from_le_bytes([head[4], head[5], head[6], head[7]]) as usize;
    if dim == 0 {
        return Err(format!("{}: dimension 0", path.display()));
    }
    let rows = limit.map_or(n, |l| l.min(n));
    let mut buf = vec![0u8; rows * dim * 4];
    r.read_exact(&mut buf).map_err(|_| {
        format!(
            "{}: header promises {n} x {dim} values and the file is shorter",
            path.display()
        )
    })?;
    let data = buf
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    Ok(Vectors { dim, data })
}

// ----------------------------------------------------------------- runbooks

/// Read a Big-ANN 2023 streaming-track runbook (`neurips23/streaming/*.yaml`
/// in big-ann-benchmarks): a mapping from dataset name to numbered steps.
///
/// | operation | fields | meaning here |
/// |---|---|---|
/// | `insert` | `start`, `end` | rows `start..end`, each tagged with its row number |
/// | `delete` | `start`, `end` | tags `start..end` |
/// | `replace` | `tags_start`, `tags_end`, `ids_start`, `ids_end` | tags get the vectors of rows `ids_start..ids_end` |
/// | `search` | | score the query set |
///
/// Other keys of the dataset's mapping (`max_pts`, `gt_url`) are ignored.
/// `name` picks the dataset; it may be omitted when the file names only one.
/// Returns the dataset's name and its steps in step-number order.
pub fn parse_runbook(text: &str, name: Option<&str>) -> Result<(String, Vec<Step>), String> {
    let doc: serde_yaml::Value = serde_yaml::from_str(text).map_err(|e| format!("runbook: {e}"))?;
    let top = doc
        .as_mapping()
        .ok_or("runbook: expected a mapping from dataset name to steps")?;
    let names: Vec<String> = top
        .keys()
        .filter_map(|k| k.as_str().map(str::to_string))
        .collect();
    let (key, body) = match name {
        Some(n) => top
            .iter()
            .find(|(k, _)| k.as_str() == Some(n))
            .ok_or_else(|| format!("runbook has no dataset `{n}`; it has {}", names.join(", ")))?,
        None if top.len() == 1 => top.iter().next().expect("one entry"),
        None => {
            return Err(format!(
                "runbook has {} datasets ({}); name one with --runbook-name",
                top.len(),
                names.join(", ")
            ))
        }
    };
    let key = key.as_str().unwrap_or_default().to_string();
    let body = body
        .as_mapping()
        .ok_or_else(|| format!("runbook `{key}`: expected a mapping of steps"))?;

    let mut numbered: Vec<(u64, &serde_yaml::Mapping)> = Vec::new();
    for (k, v) in body {
        let number = match k {
            serde_yaml::Value::Number(n) => n.as_u64(),
            serde_yaml::Value::String(s) => s.parse::<u64>().ok(),
            _ => None,
        };
        let Some(number) = number else { continue };
        let step = v
            .as_mapping()
            .ok_or_else(|| format!("runbook `{key}` step {number}: expected a mapping"))?;
        numbered.push((number, step));
    }
    numbered.sort_by_key(|(n, _)| *n);
    if numbered.is_empty() {
        return Err(format!("runbook `{key}` has no numbered steps"));
    }

    let mut steps = Vec::with_capacity(numbered.len());
    for (number, m) in numbered {
        let field = |f: &str| -> Result<u64, String> {
            m.get(f).and_then(|v| v.as_u64()).ok_or_else(|| {
                format!("runbook `{key}` step {number}: `{f}` is missing or not a count")
            })
        };
        let range = |s: &str, e: &str| -> Result<Ids, String> {
            let (start, end) = (field(s)?, field(e)?);
            if end < start {
                return Err(format!(
                    "runbook `{key}` step {number}: `{e}` {end} is before `{s}` {start}"
                ));
            }
            Ok(Ids::Range(start, end))
        };
        let op = m
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("runbook `{key}` step {number}: no `operation`"))?;
        steps.push(match op {
            "insert" => {
                let r = range("start", "end")?;
                Step::Insert {
                    tags: r.clone(),
                    rows: r,
                }
            }
            "delete" => Step::Delete {
                tags: range("start", "end")?,
            },
            "replace" => {
                let tags = range("tags_start", "tags_end")?;
                let rows = range("ids_start", "ids_end")?;
                if tags.len() != rows.len() {
                    return Err(format!(
                        "runbook `{key}` step {number}: replaces {} tags with {} rows",
                        tags.len(),
                        rows.len()
                    ));
                }
                Step::Replace { tags, rows }
            }
            "search" => Step::Search { queries: 0 },
            other => {
                return Err(format!(
                    "runbook `{key}` step {number}: unknown operation `{other}`"
                ))
            }
        });
    }
    Ok((key, steps))
}

// ------------------------------------------------------------------- the run

/// How a workload is run and scored.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub metric: Metric,
    pub params: HnswParams,
    pub k: usize,
    /// Search width for every scored search.
    pub ef: usize,
    /// Build a fresh index of the live set at every `fresh_every`-th search
    /// step and score it too. 0 never does.
    pub fresh_every: usize,
    /// Run one unscored search after every `probe_every` writes, to measure
    /// search latency while writes are going on. 0 never does.
    pub probe_every: usize,
}

/// Latencies of one kind of operation since the previous search step.
#[derive(Clone, Debug, Default)]
pub struct Ops {
    pub latencies: Vec<Duration>,
}

impl Ops {
    pub fn count(&self) -> usize {
        self.latencies.len()
    }

    pub fn total(&self) -> Duration {
        self.latencies.iter().sum()
    }

    /// Operations per second of time spent in them. Not wall-clock
    /// throughput: the probes and the bookkeeping between operations are
    /// left out.
    pub fn per_second(&self) -> f64 {
        let t = self.total().as_secs_f64();
        if t == 0.0 {
            0.0
        } else {
            self.count() as f64 / t
        }
    }

    pub fn percentile(&self, p: f64) -> Duration {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        ann_common::percentile(&sorted, p)
    }

    /// The slowest one. A write that triggered a compaction shows here.
    pub fn max(&self) -> Duration {
        self.latencies.iter().max().copied().unwrap_or_default()
    }
}

/// What a search step measured.
#[derive(Clone, Debug)]
pub struct Report {
    /// Position of this step in the workload.
    pub step: usize,
    /// Which search step this is, from 0.
    pub search: usize,
    pub live: usize,
    /// Entries left in the graph that no longer answer for a tag.
    pub superseded: usize,
    /// Rebuilds the index did on its own since the start, at a fifth dead.
    pub compactions: usize,
    /// recall@k of the churned index against exact search over the live set.
    pub recall: f64,
    /// The same for an index built fresh from the live set, when built.
    pub fresh_recall: Option<f64>,
    pub fresh_build: Option<Duration>,
    /// The churned index answered by linear scan rather than by walking the
    /// graph, so its recall says nothing about the graph.
    pub scanned: bool,
    /// Latency of each scored search.
    pub search_ops: Ops,
    pub search_wall: Duration,
    /// Writes since the previous search step.
    pub inserts: Ops,
    pub deletes: Ops,
    pub updates: Ops,
    /// Unscored searches run between those writes.
    pub probes: Ops,
    /// `VectorIndex::resident_bytes`.
    pub resident_bytes: usize,
    /// Process RSS, Linux only.
    pub rss_bytes: Option<u64>,
}

impl Report {
    pub fn search_qps(&self) -> f64 {
        let t = self.search_wall.as_secs_f64();
        if t == 0.0 {
            0.0
        } else {
            self.search_ops.count() as f64 / t
        }
    }

    /// Entries in the graph that are dead, as a share of all entries.
    pub fn dead_share(&self) -> f64 {
        let entries = self.live + self.superseded;
        if entries == 0 {
            0.0
        } else {
            self.superseded as f64 / entries as f64
        }
    }
}

/// The exact `k` nearest live tags of each query.
pub fn exact_truth(
    data: &Vectors,
    live: &HashMap<u64, usize>,
    queries: &Vectors,
    k: usize,
    metric: Metric,
) -> Vec<HashSet<u64>> {
    use rayon::prelude::*;
    let entries: Vec<(u64, usize)> = live.iter().map(|(t, r)| (*t, *r)).collect();
    let k = k.min(entries.len());
    (0..queries.len())
        .into_par_iter()
        .map(|qi| {
            let q = queries.row(qi);
            let mut scored: Vec<(f32, u64)> = entries
                .iter()
                .map(|(tag, row)| (metric.distance(q, data.row(*row)), *tag))
                .collect();
            let order = |a: &(f32, u64), b: &(f32, u64)| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1));
            if k < scored.len() {
                scored.select_nth_unstable_by(k, order);
                scored.truncate(k);
            }
            scored.into_iter().map(|(_, tag)| tag).collect()
        })
        .collect()
}

/// Share of the true neighbours found, over all queries. Set membership, as
/// in ann-benchmarks; only the first `k` answers count.
pub fn recall(found: &[Vec<u64>], truth: &[HashSet<u64>], k: usize) -> f64 {
    let mut hits = 0usize;
    let mut want = 0usize;
    for (f, t) in found.iter().zip(truth) {
        want += t.len().min(k);
        let mut seen = HashSet::new();
        hits += f
            .iter()
            .take(k)
            .filter(|id| t.contains(id) && seen.insert(**id))
            .count();
    }
    if want == 0 {
        1.0
    } else {
        hits as f64 / want as f64
    }
}

fn new_index(dim: usize, c: &Config) -> VectorIndex {
    VectorIndex::with_params(
        dim,
        ann_common::samyama_metric(c.metric),
        Quantization::None,
        c.params,
    )
}

/// Run every query once, single-threaded; the answers as tags, and the
/// latency of each.
fn search_all(
    index: &VectorIndex,
    queries: &Vectors,
    c: &Config,
) -> Result<(Vec<Vec<u64>>, Ops, Duration), String> {
    let mut found = Vec::with_capacity(queries.len());
    let mut ops = Ops::default();
    let started = Instant::now();
    for qi in 0..queries.len() {
        let at = Instant::now();
        let hits = index
            .search_ef(queries.row(qi), c.k, Some(c.ef))
            .map_err(|e| format!("search: {e}"))?;
        ops.latencies.push(at.elapsed());
        found.push(hits.into_iter().map(|(id, _)| id.0).collect());
    }
    Ok((found, ops, started.elapsed()))
}

/// Whether `VectorIndex::search_ef` answers by linear scan in this state:
/// a small index always does, and one holding dead entries does once the
/// search width times `2 * m` reaches the live count. Mirrors the rule in
/// `src/vector/index.rs` so the report can say which path it measured.
pub fn answers_by_scan(live: usize, superseded: usize, c: &Config) -> bool {
    const EXACT_SEARCH_MAX: usize = 128;
    if live <= EXACT_SEARCH_MAX {
        return true;
    }
    let width = c.ef.max(c.k.min(live)).min(live + superseded);
    superseded > 0 && width.saturating_mul(c.params.m).saturating_mul(2) >= live
}

/// Apply `w` to a new index, calling `report` after every search step.
pub fn run(
    w: &Workload,
    c: &Config,
    mut report: impl FnMut(&Report),
) -> Result<Vec<Report>, String> {
    let dim = w.data.dim;
    if let Some(bad) = w.queries.iter().find(|q| q.dim != dim) {
        return Err(format!(
            "queries have {} dimensions and the data {dim}",
            bad.dim
        ));
    }
    let needed = rows_needed(&w.steps);
    if needed > w.data.len() {
        return Err(format!(
            "the workload reads {needed} rows and the data has {}",
            w.data.len()
        ));
    }

    let mut index = new_index(dim, c);
    let mut live: HashMap<u64, usize> = HashMap::new();
    let (mut inserts, mut deletes, mut updates, mut probes) = (
        Ops::default(),
        Ops::default(),
        Ops::default(),
        Ops::default(),
    );
    let mut compactions = 0usize;
    let mut writes = 0usize;
    let mut probe_set = 0usize;
    let mut probe_next = 0usize;
    let mut searches = 0usize;
    let mut reports = Vec::new();

    // One unscored search after every `probe_every` writes, cycling through
    // the query set the last search step used.
    let mut after_write =
        |index: &VectorIndex, probes: &mut Ops, probe_set: usize| -> Result<(), String> {
            writes += 1;
            if c.probe_every == 0 || !writes.is_multiple_of(c.probe_every) {
                return Ok(());
            }
            let qs = &w.queries[probe_set];
            if qs.is_empty() || index.is_empty() {
                return Ok(());
            }
            let q = qs.row(probe_next % qs.len());
            probe_next += 1;
            let at = Instant::now();
            index
                .search_ef(q, c.k, Some(c.ef))
                .map_err(|e| format!("search: {e}"))?;
            probes.latencies.push(at.elapsed());
            Ok(())
        };

    for (si, step) in w.steps.iter().enumerate() {
        match step {
            Step::Insert { tags, rows } => {
                if tags.len() != rows.len() {
                    return Err(format!(
                        "step {si}: {} tags for {} rows",
                        tags.len(),
                        rows.len()
                    ));
                }
                for (tag, row) in tags.iter().zip(rows.iter()) {
                    if live.contains_key(&tag) {
                        return Err(format!("step {si}: insert of tag {tag}, which is live"));
                    }
                    let v = w.data.row(row as usize).to_vec();
                    let at = Instant::now();
                    index
                        .add(NodeId(tag), &v)
                        .map_err(|e| format!("step {si}: insert: {e}"))?;
                    inserts.latencies.push(at.elapsed());
                    live.insert(tag, row as usize);
                    after_write(&index, &mut probes, probe_set)?;
                }
            }
            Step::Delete { tags } => {
                for tag in tags.iter() {
                    let before = index.superseded();
                    let at = Instant::now();
                    let was = index.remove(NodeId(tag));
                    deletes.latencies.push(at.elapsed());
                    if !was || live.remove(&tag).is_none() {
                        return Err(format!("step {si}: delete of tag {tag}, which is not live"));
                    }
                    if index.superseded() <= before {
                        compactions += 1;
                    }
                    after_write(&index, &mut probes, probe_set)?;
                }
            }
            Step::Replace { tags, rows } => {
                if tags.len() != rows.len() {
                    return Err(format!(
                        "step {si}: {} tags for {} rows",
                        tags.len(),
                        rows.len()
                    ));
                }
                for (tag, row) in tags.iter().zip(rows.iter()) {
                    if !live.contains_key(&tag) {
                        return Err(format!(
                            "step {si}: replace of tag {tag}, which is not live"
                        ));
                    }
                    let v = w.data.row(row as usize).to_vec();
                    let before = index.superseded();
                    let at = Instant::now();
                    index
                        .add(NodeId(tag), &v)
                        .map_err(|e| format!("step {si}: replace: {e}"))?;
                    updates.latencies.push(at.elapsed());
                    live.insert(tag, row as usize);
                    if index.superseded() <= before {
                        compactions += 1;
                    }
                    after_write(&index, &mut probes, probe_set)?;
                }
            }
            Step::Search { queries } => {
                let qs = w
                    .queries
                    .get(*queries)
                    .ok_or_else(|| format!("step {si}: no query set {queries}"))?;
                if index.len() != live.len() {
                    return Err(format!(
                        "step {si}: the index holds {} tags and the workload {}",
                        index.len(),
                        live.len()
                    ));
                }
                let truth = exact_truth(&w.data, &live, qs, c.k, c.metric);
                let (found, search_ops, search_wall) = search_all(&index, qs, c)?;
                if let Some(dead) = found.iter().flatten().find(|t| !live.contains_key(t)) {
                    return Err(format!(
                        "step {si}: search returned tag {dead}, which is not live"
                    ));
                }
                let churned = recall(&found, &truth, c.k);

                let (fresh_recall, fresh_build) =
                    if c.fresh_every > 0 && searches.is_multiple_of(c.fresh_every) {
                        let mut tags: Vec<(&u64, &usize)> = live.iter().collect();
                        tags.sort();
                        let at = Instant::now();
                        let mut fresh = new_index(dim, c);
                        for (tag, row) in tags {
                            fresh
                                .add(NodeId(*tag), &w.data.row(*row).to_vec())
                                .map_err(|e| format!("fresh build: {e}"))?;
                        }
                        let built = at.elapsed();
                        let (f, _, _) = search_all(&fresh, qs, c)?;
                        (Some(recall(&f, &truth, c.k)), Some(built))
                    } else {
                        (None, None)
                    };

                let r = Report {
                    step: si,
                    search: searches,
                    live: live.len(),
                    superseded: index.superseded(),
                    compactions,
                    recall: churned,
                    fresh_recall,
                    fresh_build,
                    scanned: answers_by_scan(live.len(), index.superseded(), c),
                    search_ops,
                    search_wall,
                    inserts: std::mem::take(&mut inserts),
                    deletes: std::mem::take(&mut deletes),
                    updates: std::mem::take(&mut updates),
                    probes: std::mem::take(&mut probes),
                    resident_bytes: index.resident_bytes(),
                    rss_bytes: ann_common::rss_bytes().0,
                };
                report(&r);
                reports.push(r);
                searches += 1;
                probe_set = *queries;
            }
        }
    }
    Ok(reports)
}
