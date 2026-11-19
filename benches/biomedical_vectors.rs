//! Vector search at biomedical scale (#1281).
//!
//! Two embedding levels, each measured per (layout, index kind, quantization):
//!
//! - **concept**: one vector per Drug, Disease, Gene, Protein, Target,
//!   Pathway, MeSHTerm and ClinicalCondition node, from short name + synonym
//!   text;
//! - **article**: one vector per article, from title + abstract.
//!
//! For each it reports build time; memory (the index's own vector and graph
//! bytes, resident bytes, and process RSS where the platform reports it); the
//! checkpoint's size on disk by file kind, its write time and the recovery
//! time; recall@k against exact search with p50/p99 latency over a sweep of
//! `ef` (HNSW) or `nprobe` (IVF); and the **recall delta of each
//! quantization against f32** at the same width, which is the accuracy
//! impact the issue asks for.
//!
//! Concept types are node labels in the biomedical graph, so every level is
//! also searched **filtered to one type** (k-NN among Drug nodes only, and so
//! on), with its own exact ground truth over that type. Two layouts are
//! compared: `shared`, one index on a common label with the type applied as a
//! filter, and `per-type`, one index per type label, where "any type" is the
//! merge of every type's answer.
//!
//! ```text
//! # No data: a seeded synthetic biomedical-shaped corpus (what the nightly runs).
//! cargo bench --bench biomedical_vectors
//!
//! # Larger synthetic run, one quantization per process for clean RSS.
//! cargo bench --bench biomedical_vectors -- --concepts 1000000 --articles 200000 \
//!     --quantization fp16 --json bio-fp16.json
//!
//! # Real embeddings, after scripts/prepare_biomedical_vectors.py:
//! cargo bench --bench biomedical_vectors -- --data data/biomedical-vectors
//! ```
//!
//! Options:
//!
//! | flag | default | |
//! |---|---|---|
//! | `--data DIR` | none (synthetic) | `<level>/base.fvecs`, `<level>/base_types.txt`, `<level>/query.fvecs`, optional `metric.txt` |
//! | `--levels` | `concept,article` | levels measured |
//! | `--concepts N`, `--articles N` | 10000, 3000 | synthetic base sizes |
//! | `--concept-dim`, `--article-dim` | 384, 768 | synthetic dimensions |
//! | `--queries N` | 200 | queries per level |
//! | `--limit N` | all | base vectors read per level from `--data` |
//! | `--seed N` | 1281 | synthetic corpus seed |
//! | `--metric` | cosine, or `metric.txt` | l2, cosine or dot |
//! | `--quantization` | every one this build has | comma list: `f32,fp16` today; `int8` once #2307 merges |
//! | `--kind` | `hnsw,ivf` | index kinds |
//! | `--layout` | `shared,per-type` | per-type is skipped for a one-type level |
//! | `--filter-types` | `Drug,Pathway` | type subsets; `all` = every type, `none` = no filtered rows |
//! | `--k` | 10 | recall@k |
//! | `--ef` | `16,32,64,128,256` | HNSW search widths |
//! | `--nprobe` | `4,8,16,32,64` | IVF lists probed |
//! | `--m`, `--ef-construction` | 16, 200 | HNSW build parameters |
//! | `--recall-target` | 0.95 | the summary row is the fastest width reaching it |
//! | `--rss-interval-ms` | 1000 | RSS sampling period |
//! | `--json FILE` | none | every row as JSON |
//!
//! Caveats, stated rather than hidden:
//!
//! - The synthetic corpus has the shape of a biomedical one (typed clusters,
//!   related types near each other, unit vectors at sentence-model
//!   dimensions) and none of its content. Its recall says how the index
//!   behaves on clustered data, not how a given model's embeddings search.
//! - Latency is serial (one client thread): p50/p99 are per query, and QPS
//!   is one thread's. Throughput across cores is `ann_benchmark`'s job.
//! - RSS comes from `/proc` and is unknown off Linux; a freed index does not
//!   always return its pages, so later rows in one process carry earlier
//!   rows' residue. Pass one `--quantization` and `--kind` per run to compare.
//! - In-process, no network: this engine's ceiling, not comparable to a
//!   system measured over its API.
//!
//! Run with `--release` on a quiet host and compare the calibration line
//! before comparing timings (#529).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[path = "common/bench_setup.rs"]
mod bench_setup;

#[path = "ann_common/mod.rs"]
mod ann_common;

#[path = "vector_cost_common/mod.rs"]
mod vector_cost_common;

#[path = "biomedical_vector_common/mod.rs"]
mod biomedical_vector_common;

use ann_common::{Metric, Neighbours, Vectors};
use biomedical_vector_common::{CheckpointBytes, Level, Measured};
use samyama::graph::NodeId;
use samyama::vector::index::Quantization;
use samyama::vector::work::Work;
use samyama::vector::{HnswParams, IndexKind, IvfParams, VectorIndex, VectorIndexManager};
use vector_cost_common::{Sweep, QUANTIZATIONS};

const PROPERTY: &str = "embedding";

/// The index kinds this bench builds, by the name `--kind` takes. IVF's
/// `nlist` is left to the index (`sqrt(n)`).
const KINDS: &[(&str, IndexKind)] = &[
    ("hnsw", IndexKind::Hnsw),
    (
        "ivf",
        IndexKind::Ivf(IvfParams {
            nlist: None,
            nprobe: None,
        }),
    ),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    /// One index on the level's common label; a type is a filter.
    Shared,
    /// One index per type label; "any type" merges every index's answer.
    PerType,
}

impl Layout {
    fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "shared" => Ok(Layout::Shared),
            "per-type" | "per_type" | "pertype" => Ok(Layout::PerType),
            other => Err(format!(
                "--layout: unknown `{other}`; use shared or per-type"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Layout::Shared => "shared",
            Layout::PerType => "per-type",
        }
    }
}

enum FilterTypes {
    All,
    Named(Vec<String>),
}

struct Args {
    data: Option<PathBuf>,
    levels: Vec<Level>,
    concepts: usize,
    articles: usize,
    concept_dim: usize,
    article_dim: usize,
    queries: usize,
    limit: Option<usize>,
    seed: u64,
    metric: Option<Metric>,
    quantizations: Vec<(&'static str, Quantization)>,
    kinds: Vec<(&'static str, IndexKind)>,
    layouts: Vec<Layout>,
    filter_types: FilterTypes,
    k: usize,
    ef: Vec<usize>,
    nprobe: Vec<usize>,
    m: usize,
    ef_construction: usize,
    recall_target: f64,
    rss_interval: Duration,
    json: Option<PathBuf>,
}

fn list(s: &str, flag: &str) -> Result<Vec<usize>, String> {
    s.split(',')
        .map(|v| {
            v.trim()
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("{flag}: `{v}` is not a positive integer"))
        })
        .collect()
}

fn one(s: &str, flag: &str) -> Result<usize, String> {
    match list(s, flag)?.as_slice() {
        [n] => Ok(*n),
        _ => Err(format!("{flag} takes one number")),
    }
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        data: None,
        levels: Level::ALL.to_vec(),
        concepts: 10_000,
        articles: 3_000,
        concept_dim: Level::Concept.default_dim(),
        article_dim: Level::Article.default_dim(),
        queries: 200,
        limit: None,
        seed: 1281,
        metric: None,
        quantizations: QUANTIZATIONS.to_vec(),
        kinds: KINDS.to_vec(),
        layouts: vec![Layout::Shared, Layout::PerType],
        filter_types: FilterTypes::Named(
            biomedical_vector_common::DEFAULT_FILTER_TYPES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        ),
        k: 10,
        ef: vec![16, 32, 64, 128, 256],
        nprobe: vec![4, 8, 16, 32, 64],
        m: 16,
        ef_construction: 200,
        recall_target: 0.95,
        rss_interval: Duration::from_secs(1),
        json: None,
    };
    // `cargo bench` passes `--bench` to every harness=false target.
    let mut it = std::env::args().skip(1).filter(|a| a != "--bench");
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--data" => a.data = Some(value()?.into()),
            "--levels" => {
                a.levels = value()?
                    .split(',')
                    .map(Level::parse)
                    .collect::<Result<_, _>>()?;
                a.levels.dedup();
            }
            "--concepts" => a.concepts = one(&value()?, "--concepts")?,
            "--articles" => a.articles = one(&value()?, "--articles")?,
            "--concept-dim" => a.concept_dim = one(&value()?, "--concept-dim")?,
            "--article-dim" => a.article_dim = one(&value()?, "--article-dim")?,
            "--queries" => a.queries = one(&value()?, "--queries")?,
            "--limit" => a.limit = Some(one(&value()?, "--limit")?),
            "--seed" => {
                let v = value()?;
                a.seed = v
                    .trim()
                    .parse()
                    .map_err(|_| format!("--seed: `{v}` is not an integer"))?;
            }
            "--metric" => a.metric = Some(Metric::parse(&value()?)?),
            "--quantization" => {
                a.quantizations = biomedical_vector_common::select(
                    &value()?,
                    QUANTIZATIONS,
                    "--quantization",
                    Quantization::parse,
                )?
            }
            "--kind" => {
                a.kinds = biomedical_vector_common::select(&value()?, KINDS, "--kind", |s| {
                    IndexKind::parse(s)
                })?
            }
            "--layout" => {
                a.layouts = value()?
                    .split(',')
                    .map(Layout::parse)
                    .collect::<Result<_, _>>()?;
                a.layouts.dedup();
            }
            "--filter-types" => {
                let v = value()?;
                a.filter_types = match v.trim() {
                    "all" => FilterTypes::All,
                    "none" => FilterTypes::Named(Vec::new()),
                    names => FilterTypes::Named(
                        names
                            .split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect(),
                    ),
                };
            }
            "--k" => a.k = one(&value()?, "--k")?,
            "--ef" => a.ef = list(&value()?, "--ef")?,
            "--nprobe" => a.nprobe = list(&value()?, "--nprobe")?,
            "--m" => a.m = one(&value()?, "--m")?,
            "--ef-construction" => a.ef_construction = one(&value()?, "--ef-construction")?,
            "--recall-target" => {
                let v = value()?;
                a.recall_target = v
                    .trim()
                    .parse::<f64>()
                    .ok()
                    .filter(|r| *r > 0.0 && *r <= 1.0)
                    .ok_or_else(|| format!("--recall-target is a fraction, got `{v}`"))?;
            }
            "--rss-interval-ms" => {
                a.rss_interval = Duration::from_millis(one(&value()?, "--rss-interval-ms")? as u64)
            }
            "--json" => a.json = Some(value()?.into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    HnswParams {
        m: a.m,
        ef_construction: a.ef_construction,
        ef: None,
    }
    .validate()?;
    for ef in &a.ef {
        samyama::vector::index::check_ef(*ef)?;
    }
    for p in &a.nprobe {
        IvfParams {
            nlist: None,
            nprobe: Some(*p),
        }
        .validate()?;
    }
    Ok(a)
}

/// One level's base vectors, their types, and its queries.
struct LevelData {
    level: Level,
    source: String,
    metric: Metric,
    base: Vectors,
    types: Vec<u16>,
    type_names: Vec<String>,
    queries: Vectors,
}

impl LevelData {
    fn type_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.type_names.len()];
        for t in &self.types {
            counts[*t as usize] += 1;
        }
        counts
    }
}

fn load_level(a: &Args, level: Level) -> Result<Option<LevelData>, String> {
    let Some(root) = &a.data else {
        let (n, dim) = match level {
            Level::Concept => (a.concepts, a.concept_dim),
            Level::Article => (a.articles, a.article_dim),
        };
        // Each level its own seed, so the two corpora are not one stream.
        let seed = a.seed.wrapping_mul(31).wrapping_add(level as u64);
        let (base, queries) = biomedical_vector_common::synthetic(level, n, a.queries, dim, seed);
        return Ok(Some(LevelData {
            level,
            source: format!("synthetic {n}x{dim}, seed {}", a.seed),
            metric: a.metric.unwrap_or(Metric::Cosine),
            base: Vectors {
                dim: base.dim,
                data: base.data,
            },
            types: base.types,
            type_names: base.type_names,
            queries: Vectors {
                dim: queries.dim,
                data: queries.data,
            },
        }));
    };
    let dir = root.join(level.name());
    if !dir.join("base.fvecs").exists() {
        println!(
            "{}: no base.fvecs in {}; level skipped",
            level.name(),
            dir.display()
        );
        return Ok(None);
    }
    let metric = match (a.metric, read_metric(root, &dir)?) {
        (Some(m), Some(f)) if m != f => {
            return Err(format!(
                "--metric {} disagrees with metric.txt ({})",
                m.name(),
                f.name()
            ))
        }
        (Some(m), _) | (None, Some(m)) => m,
        // Sentence-transformer embeddings are compared by cosine.
        (None, None) => Metric::Cosine,
    };
    let base = ann_common::read_fvecs(&dir.join("base.fvecs"), a.limit)?;
    let queries = ann_common::read_fvecs(&dir.join("query.fvecs"), Some(a.queries))?;
    if base.dim != queries.dim {
        return Err(format!(
            "{}: base vectors have {} dimensions and queries have {}",
            dir.display(),
            base.dim,
            queries.dim
        ));
    }
    // One type per vector of the whole file. `--limit` cuts the vectors, so
    // the types are checked against the file's vector count and then cut to
    // match: a short or long types file is misaligned either way.
    let types_path = dir.join("base_types.txt");
    let text = std::fs::read_to_string(&types_path)
        .map_err(|e| format!("{}: {e}", types_path.display()))?;
    let in_file = if a.limit.is_some() {
        // Each .fvecs record is a 4-byte dimension and `dim` f32 values.
        let bytes = std::fs::metadata(dir.join("base.fvecs"))
            .map_err(|e| format!("{}: {e}", dir.display()))?
            .len();
        (bytes / (4 + 4 * base.dim as u64)) as usize
    } else {
        base.len()
    };
    let names =
        biomedical_vector_common::parse_types(&text, in_file, &types_path.display().to_string())?;
    let (types, type_names) = biomedical_vector_common::intern(&names[..base.len()]);
    let model = std::fs::read_to_string(root.join("model.txt"))
        .map(|s| format!(", model {}", s.trim()))
        .unwrap_or_default();
    Ok(Some(LevelData {
        level,
        source: format!("{}{model}", dir.display()),
        metric,
        base,
        types,
        type_names,
        queries,
    }))
}

fn read_metric(root: &Path, dir: &Path) -> Result<Option<Metric>, String> {
    for p in [dir.join("metric.txt"), root.join("metric.txt")] {
        if let Ok(s) = std::fs::read_to_string(&p) {
            return Metric::parse(s.trim()).map(Some);
        }
    }
    Ok(None)
}

/// A set of vectors searched as one: every vector, or one type's.
struct Subset {
    name: String,
    /// `None` for every type.
    type_id: Option<u16>,
    size: usize,
    truth: Neighbours,
}

fn subsets(a: &Args, ld: &LevelData) -> Vec<Subset> {
    eprintln!(
        "{}: exact ground truth for {} queries over {} vectors ...",
        ld.level.name(),
        ld.queries.len(),
        ld.base.len()
    );
    let mut out = vec![Subset {
        name: "all".into(),
        type_id: None,
        size: ld.base.len(),
        truth: ann_common::exact_ground_truth(&ld.base, &ld.queries, a.k, ld.metric),
    }];
    // Filtering a one-type level by its only type is the unfiltered search.
    if ld.type_names.len() < 2 {
        return out;
    }
    let wanted: Vec<String> = match &a.filter_types {
        FilterTypes::All => ld.type_names.clone(),
        FilterTypes::Named(names) => names.clone(),
    };
    for name in wanted {
        let Some(t) = ld.type_names.iter().position(|n| *n == name) else {
            println!(
                "{}: type `{name}` has no vectors; its filtered rows are skipped",
                ld.level.name()
            );
            continue;
        };
        let t = t as u16;
        let positions = biomedical_vector_common::positions_of(&ld.types, t);
        let sub = Vectors {
            dim: ld.base.dim,
            data: biomedical_vector_common::gather(&ld.base.data, ld.base.dim, &positions),
        };
        let local = ann_common::exact_ground_truth(&sub, &ld.queries, a.k, ld.metric);
        out.push(Subset {
            name,
            type_id: Some(t),
            size: positions.len(),
            truth: Neighbours {
                width: local.width,
                ids: biomedical_vector_common::remap(&local.ids, &positions),
            },
        });
    }
    out
}

fn rss_now() -> Option<u64> {
    ann_common::rss_bytes().0
}

/// One built configuration: its build, memory and checkpoint figures.
struct Built {
    layout: Layout,
    kind: &'static str,
    quantization: &'static str,
    bytes_per_value: usize,
    indexes: usize,
    build: Duration,
    vector_bytes: usize,
    graph_bytes: usize,
    resident_bytes: usize,
    theoretical_per_vec: usize,
    rss_growth: Option<u64>,
    rss_peak: Option<u64>,
    checkpoint: Duration,
    disk: CheckpointBytes,
    recover: Duration,
    graphs_loaded: usize,
    rebuilt: usize,
    first_query: Duration,
    searches: Vec<SearchRow>,
}

/// One point of one subset's sweep.
struct SearchRow {
    subset: String,
    subset_size: usize,
    point: ann_common::Point,
    /// Share of queries that gave up on the index and scanned exactly.
    exact_fallback_share: f64,
    distances_per_query: f64,
}

type Search = Box<dyn Fn(&[f32], usize, usize) -> Result<Vec<u32>, String> + Sync>;

fn ids(hits: Vec<(NodeId, f32)>) -> Vec<u32> {
    hits.into_iter().map(|(id, _)| id.as_u64() as u32).collect()
}

/// The search a row measures. Every path takes the index's own lock per
/// query, as the server does.
fn searcher(
    layout: Layout,
    level: Level,
    manager: &Arc<VectorIndexManager>,
    handles: &[Arc<RwLock<VectorIndex>>],
    types: &Arc<Vec<u16>>,
    subset: Option<u16>,
) -> Search {
    match (layout, subset) {
        (Layout::Shared, None) => {
            let h = Arc::clone(&handles[0]);
            Box::new(move |q, k, w| {
                h.read()
                    .unwrap()
                    .search_ef(q, k, Some(w))
                    .map(ids)
                    .map_err(|e| e.to_string())
            })
        }
        (Layout::Shared, Some(t)) => {
            let m = Arc::clone(manager);
            let types = Arc::clone(types);
            let label = level.label();
            Box::new(move |q, k, w| {
                m.search_filtered_ef(label, PROPERTY, q, k, Some(w), |id| {
                    types.get(id.as_u64() as usize) == Some(&t)
                })
                .map(ids)
                .map_err(|e| e.to_string())
            })
        }
        (Layout::PerType, Some(t)) => {
            let h = Arc::clone(&handles[t as usize]);
            Box::new(move |q, k, w| {
                h.read()
                    .unwrap()
                    .search_ef(q, k, Some(w))
                    .map(ids)
                    .map_err(|e| e.to_string())
            })
        }
        (Layout::PerType, None) => {
            let hs: Vec<_> = handles.iter().map(Arc::clone).collect();
            Box::new(move |q, k, w| {
                let mut lists = Vec::with_capacity(hs.len());
                for h in &hs {
                    let hits = h
                        .read()
                        .unwrap()
                        .search_ef(q, k, Some(w))
                        .map_err(|e| e.to_string())?;
                    lists.push(
                        hits.into_iter()
                            .map(|(id, d)| (id.as_u64() as u32, d))
                            .collect(),
                    );
                }
                Ok(biomedical_vector_common::merge_top_k(&lists, k))
            })
        }
    }
}

fn measure(
    a: &Args,
    ld: &LevelData,
    subsets: &[Subset],
    layout: Layout,
    (kind_name, kind): (&'static str, IndexKind),
    (quant_name, quantization): (&'static str, Quantization),
) -> Result<Built, String> {
    let n = ld.base.len();
    let dim = ld.base.dim;
    let metric = ann_common::samyama_metric(ld.metric);
    let params = HnswParams {
        m: a.m,
        ef_construction: a.ef_construction,
        ef: None,
    };
    let labels: Vec<String> = match layout {
        Layout::Shared => vec![ld.level.label().to_string()],
        Layout::PerType => ld.type_names.clone(),
    };
    let rss_before = rss_now();
    let sampler = vector_cost_common::RssSampler::start(a.rss_interval, rss_now);

    // Build: the indexes declared, then filled one vector at a time and
    // trained once at the end, as a bulk load does.
    let manager = Arc::new(VectorIndexManager::new());
    let started = Instant::now();
    for label in &labels {
        manager
            .create_index_with_kind(
                None,
                label,
                PROPERTY,
                dim,
                metric,
                quantization,
                params,
                kind,
            )
            .map_err(|e| format!("create {label}: {e}"))?;
    }
    let handles: Vec<Arc<RwLock<VectorIndex>>> = labels
        .iter()
        .map(|l| {
            manager
                .get_index(l, PROPERTY)
                .ok_or(format!("no index {l}"))
        })
        .collect::<Result<_, _>>()?;
    {
        let mut guards: Vec<_> = handles.iter().map(|h| h.write().unwrap()).collect();
        for i in 0..n {
            let slot = match layout {
                Layout::Shared => 0,
                Layout::PerType => ld.types[i] as usize,
            };
            guards[slot]
                .add_untrained(NodeId::new(i as u64), ld.base.row(i))
                .map_err(|e| format!("add {i}: {e}"))?;
        }
        for g in &mut guards {
            g.train_if_due();
        }
    }
    let build = started.elapsed();
    let rss_built = rss_now();
    let (mut vector_bytes, mut graph_bytes, mut resident_bytes) = (0, 0, 0);
    for h in &handles {
        let g = h.read().unwrap();
        vector_bytes += g.vector_bytes();
        graph_bytes += g.graph_bytes();
        resident_bytes += g.resident_bytes();
    }

    // Checkpoint, then recover into a fresh manager and answer one query.
    let dir = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let path = dir.path().join("vectors");
    let started = Instant::now();
    manager
        .checkpoint_all(&path)
        .map_err(|e| format!("checkpoint: {e}"))?;
    let checkpoint = started.elapsed();
    let disk = biomedical_vector_common::checkpoint_bytes(&path)
        .map_err(|e| format!("checkpoint size: {e}"))?;
    let recovered = VectorIndexManager::new();
    let started = Instant::now();
    let report = recovered
        .recover_checkpoint(&path)
        .map_err(|e| format!("recover: {e}"))?;
    let recover = started.elapsed();
    let first_width = if kind.is_ivf() { a.nprobe[0] } else { a.ef[0] };
    let started = Instant::now();
    recovered
        .get_index(&labels[0], PROPERTY)
        .ok_or("recovered checkpoint has no index")?
        .read()
        .unwrap()
        .search_ef(ld.queries.row(0), a.k, Some(first_width))
        .map_err(|e| format!("first query: {e}"))?;
    let first_query = started.elapsed();
    drop(recovered);
    drop(dir);

    // Search sweep, serial, per subset.
    let widths = if kind.is_ivf() { &a.nprobe } else { &a.ef };
    let types = Arc::new(ld.types.clone());
    let mut searches = Vec::new();
    for s in subsets {
        let search = searcher(layout, ld.level, &manager, &handles, &types, s.type_id);
        // One untimed pass so the sweep does not pay for cold caches.
        let _ = ann_common::run_point(&search, &ld.queries, &s.truth, a.k, widths[0], 1);
        for &w in widths {
            let fallbacks = AtomicU64::new(0);
            let distances = AtomicU64::new(0);
            let counted = |q: &[f32], k: usize, w: usize| {
                let before = Work::now();
                let got = search(q, k, w);
                let did = Work::now().since(before);
                fallbacks.fetch_add(did.exact_fallbacks, Ordering::Relaxed);
                distances.fetch_add(did.distances, Ordering::Relaxed);
                got
            };
            let point = ann_common::run_point(&counted, &ld.queries, &s.truth, a.k, w, 1);
            let q = ld.queries.len() as u64;
            searches.push(SearchRow {
                subset: s.name.clone(),
                subset_size: s.size,
                point,
                exact_fallback_share: biomedical_vector_common::share(
                    fallbacks.load(Ordering::Relaxed),
                    q,
                ),
                distances_per_query: biomedical_vector_common::share(
                    distances.load(Ordering::Relaxed),
                    q,
                ),
            });
        }
    }
    drop(handles);
    drop(manager);
    let rss_peak = sampler.stop();

    let bytes_per_value = quantization.bytes_per_value();
    Ok(Built {
        layout,
        kind: kind_name,
        quantization: quant_name,
        bytes_per_value,
        indexes: labels.len(),
        build,
        vector_bytes,
        graph_bytes,
        resident_bytes,
        // An IVF index has no links: its floor is the values alone.
        theoretical_per_vec: if kind.is_ivf() {
            dim * bytes_per_value
        } else {
            vector_cost_common::theoretical_bytes_per_vector(dim, bytes_per_value, a.m)
        },
        rss_growth: match (rss_before, rss_built) {
            (Some(b), Some(after)) => Some(after.saturating_sub(b)),
            _ => None,
        },
        rss_peak,
        checkpoint,
        disk,
        recover,
        graphs_loaded: report.graphs_loaded,
        rebuilt: report.rebuilt,
        first_query,
        searches,
    })
}

fn group(level: Level, b: &Built, subset: &str) -> String {
    format!("{}/{}/{}/{}", level.name(), b.layout.name(), b.kind, subset)
}

fn measured(level: Level, built: &[Built]) -> Vec<Measured> {
    built
        .iter()
        .flat_map(|b| {
            b.searches.iter().map(move |s| Measured {
                group: group(level, b, &s.subset),
                quantization: b.quantization.to_string(),
                width: s.point.ef,
                recall: s.point.recall,
            })
        })
        .collect()
}

fn mb(b: Option<u64>) -> String {
    b.map(|b| format!("{:.1}", b as f64 / 1e6))
        .unwrap_or_else(|| "?".into())
}

fn delta(d: Option<f64>) -> String {
    d.map(|d| format!("{d:+.4}")).unwrap_or_else(|| "-".into())
}

fn print_level(a: &Args, ld: &LevelData, built: &[Built]) {
    let n = ld.base.len();
    println!("\n== {} level: {} ==", ld.level.name(), ld.source);
    let counts = ld.type_counts();
    let mix: Vec<String> = ld
        .type_names
        .iter()
        .zip(&counts)
        .map(|(t, c)| format!("{t} {c}"))
        .collect();
    println!(
        "{n} vectors x {} dims, {} queries, metric {}, k={}; types: {}",
        ld.base.dim,
        ld.queries.len(),
        ld.metric.name(),
        a.k,
        mix.join(", ")
    );

    println!(
        "\nBuild and memory (bytes per vector; theory = dim*bytes + M*2*4, values only for IVF)"
    );
    println!(
        "  {:<9} {:<5} {:<5} {:>3} {:>9} {:>8} {:>8} {:>9} {:>7} {:>11} {:>11}",
        "layout",
        "kind",
        "quant",
        "idx",
        "build s",
        "vectors",
        "graph",
        "resident",
        "theory",
        "RSS grow MB",
        "RSS peak MB"
    );
    for b in built {
        let per = |x: usize| biomedical_vector_common::per_vector(x as u64, n);
        println!(
            "  {:<9} {:<5} {:<5} {:>3} {:>9.3} {:>8.0} {:>8.0} {:>9.0} {:>7} {:>11} {:>11}",
            b.layout.name(),
            b.kind,
            b.quantization,
            b.indexes,
            b.build.as_secs_f64(),
            per(b.vector_bytes),
            per(b.graph_bytes),
            per(b.resident_bytes),
            b.theoretical_per_vec,
            mb(b.rss_growth),
            mb(b.rss_peak),
        );
    }

    println!("\nCheckpoint on disk (MB) and recovery; restart = recover + first query");
    println!(
        "  {:<9} {:<5} {:<5} {:>8} {:>7} {:>7} {:>9} {:>6} {:>9} {:>10} {:>9} {:>11}",
        "layout",
        "kind",
        "quant",
        "total",
        "lists",
        "graphs",
        "centroids",
        "B/vec",
        "write s",
        "recover s",
        "first ms",
        "graph/rebuilt"
    );
    for b in built {
        let d = &b.disk;
        println!(
            "  {:<9} {:<5} {:<5} {:>8} {:>7} {:>7} {:>9} {:>6.0} {:>9.3} {:>10.3} {:>9.3} {:>11}",
            b.layout.name(),
            b.kind,
            b.quantization,
            mb(Some(d.total())),
            mb(Some(d.vector_lists)),
            mb(Some(d.graphs)),
            mb(Some(d.centroids)),
            biomedical_vector_common::per_vector(d.total(), n),
            b.checkpoint.as_secs_f64(),
            b.recover.as_secs_f64(),
            b.first_query.as_secs_f64() * 1e3,
            format!("{}/{}", b.graphs_loaded, b.rebuilt),
        );
    }

    let rows = measured(ld.level, built);
    let deltas = biomedical_vector_common::recall_deltas(&rows);
    println!(
        "\nRecall@{} vs exact, serial latency; delta = recall minus f32's at the same width \
         (ef for hnsw, nprobe for ivf)",
        a.k
    );
    println!(
        "  {:<9} {:<5} {:<18} {:<5} {:>5} {:>8} {:>8} {:>9} {:>9} {:>8} {:>7} {:>9}",
        "layout",
        "kind",
        "subset (size)",
        "quant",
        "width",
        "recall",
        "delta",
        "p50 us",
        "p99 us",
        "QPS",
        "exact%",
        "dist/q"
    );
    let mut i = 0;
    for b in built {
        for s in &b.searches {
            let p = &s.point;
            println!(
                "  {:<9} {:<5} {:<18} {:<5} {:>5} {:>8.4} {:>8} {:>9.0} {:>9.0} {:>8.0} {:>6.1}% {:>9.0}",
                b.layout.name(),
                b.kind,
                format!("{} ({})", s.subset, s.subset_size),
                b.quantization,
                p.ef,
                p.recall,
                delta(deltas[i]),
                p.p50.as_secs_f64() * 1e6,
                p.p99.as_secs_f64() * 1e6,
                p.qps,
                s.exact_fallback_share * 100.0,
                s.distances_per_query,
            );
            i += 1;
        }
    }

    println!(
        "\nFastest width reaching recall >= {} (per layout, kind, subset, quantization)",
        a.recall_target
    );
    for b in built {
        let mut names: Vec<&str> = b.searches.iter().map(|s| s.subset.as_str()).collect();
        names.dedup();
        for name in names {
            let pts: Vec<&SearchRow> = b.searches.iter().filter(|s| s.subset == name).collect();
            let sweep: Vec<Sweep> = pts
                .iter()
                .map(|s| Sweep {
                    ef: s.point.ef,
                    recall: s.point.recall,
                    qps: s.point.qps,
                })
                .collect();
            let head = format!(
                "  {:<9} {:<5} {:<18} {:<5}",
                b.layout.name(),
                b.kind,
                name,
                b.quantization
            );
            match vector_cost_common::operating_point(&sweep, a.recall_target) {
                Some(op) => {
                    let s = pts
                        .iter()
                        .find(|s| s.point.ef == op.ef)
                        .expect("from the sweep");
                    println!(
                        "{head} width {:>5}  recall {:.4}  p50 {:.0} us  p99 {:.0} us",
                        op.ef,
                        op.recall,
                        s.point.p50.as_secs_f64() * 1e6,
                        s.point.p99.as_secs_f64() * 1e6
                    );
                }
                None => {
                    let best = sweep.iter().map(|p| p.recall).fold(0.0, f64::max);
                    println!("{head} not reached at any width tried (best {best:.4})");
                }
            }
        }
    }
}

fn json_level(ld: &LevelData, built: &[Built]) -> serde_json::Value {
    use serde_json::json;
    let n = ld.base.len();
    let rows = measured(ld.level, built);
    let deltas = biomedical_vector_common::recall_deltas(&rows);
    let mut i = 0;
    let configs: Vec<_> = built
        .iter()
        .map(|b| {
            let searches: Vec<_> = b
                .searches
                .iter()
                .map(|s| {
                    let p = &s.point;
                    let row = json!({
                        "subset": s.subset,
                        "subset_size": s.subset_size,
                        "width": p.ef,
                        "recall": p.recall,
                        "recall_delta_vs_f32": deltas[i],
                        "p50_us": p.p50.as_secs_f64() * 1e6,
                        "p99_us": p.p99.as_secs_f64() * 1e6,
                        "qps_serial": p.qps,
                        "errors": p.errors,
                        "exact_fallback_share": s.exact_fallback_share,
                        "distances_per_query": s.distances_per_query,
                    });
                    i += 1;
                    row
                })
                .collect();
            json!({
                "layout": b.layout.name(),
                "kind": b.kind,
                "quantization": b.quantization,
                "bytes_per_value": b.bytes_per_value,
                "indexes": b.indexes,
                "build_s": b.build.as_secs_f64(),
                "vector_bytes": b.vector_bytes,
                "graph_bytes": b.graph_bytes,
                "resident_bytes": b.resident_bytes,
                "resident_bytes_per_vector":
                    biomedical_vector_common::per_vector(b.resident_bytes as u64, n),
                "theoretical_bytes_per_vector": b.theoretical_per_vec,
                "rss_growth_bytes": b.rss_growth,
                "rss_peak_bytes": b.rss_peak,
                "checkpoint_s": b.checkpoint.as_secs_f64(),
                "checkpoint_bytes": {
                    "total": b.disk.total(),
                    "vector_lists": b.disk.vector_lists,
                    "graphs": b.disk.graphs,
                    "centroids": b.disk.centroids,
                    "other": b.disk.other,
                },
                "recover_s": b.recover.as_secs_f64(),
                "recovered_graphs_loaded": b.graphs_loaded,
                "recovered_rebuilt": b.rebuilt,
                "first_query_after_recover_s": b.first_query.as_secs_f64(),
                "searches": searches,
            })
        })
        .collect();
    json!({
        "level": ld.level.name(),
        "source": ld.source,
        "n": n,
        "dim": ld.base.dim,
        "queries": ld.queries.len(),
        "metric": ld.metric.name(),
        "types": ld.type_names.iter().zip(ld.type_counts())
            .map(|(t, c)| json!({"type": t, "count": c})).collect::<Vec<_>>(),
        "configs": configs,
    })
}

fn run(a: &Args, calibration: Duration) -> Result<(), String> {
    if rss_now().is_none() {
        println!("RSS: unknown on this platform (read from /proc); the RSS columns show `?`.");
    }
    let mut levels_json = Vec::new();
    let mut measured_any = false;
    for &level in &a.levels {
        let Some(ld) = load_level(a, level)? else {
            continue;
        };
        if ld.base.is_empty() || ld.queries.is_empty() {
            return Err(format!("{}: no base vectors or no queries", level.name()));
        }
        let subsets = subsets(a, &ld);
        let mut built = Vec::new();
        for &layout in &a.layouts {
            if layout == Layout::PerType && ld.type_names.len() < 2 {
                continue;
            }
            for &kind in &a.kinds {
                for &q in &a.quantizations {
                    eprintln!("{}: {} {} {} ...", level.name(), layout.name(), kind.0, q.0);
                    built.push(measure(a, &ld, &subsets, layout, kind, q)?);
                }
            }
        }
        print_level(a, &ld, &built);
        levels_json.push(json_level(&ld, &built));
        measured_any = true;
    }
    if !measured_any {
        return Err("no level had data to measure".into());
    }
    if a.quantizations.len() * a.kinds.len() > 1 {
        println!(
            "\n  Several configurations ran in one process: RSS rows after the first include\n  \
             what earlier ones left resident. Run one --quantization and --kind per process to compare RSS."
        );
    }
    if let Some(path) = &a.json {
        let doc = serde_json::json!({
            "system": "samyama-graph",
            "issue": 1281,
            "k": a.k,
            "m": a.m,
            "ef_construction": a.ef_construction,
            "recall_target": a.recall_target,
            "calibration_ms": calibration.as_secs_f64() * 1000.0,
            "levels": levels_json,
        });
        let text = serde_json::to_string_pretty(&doc).expect("JSON of plain values");
        std::fs::write(path, text).map_err(|e| format!("{}: {e}", path.display()))?;
        println!("\nWrote {}", path.display());
    }
    Ok(())
}

fn main() {
    bench_setup::init();
    let a = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("biomedical_vectors: {e}");
            std::process::exit(2);
        }
    };
    let calibration = bench_setup::report_calibration();
    if let Err(e) = run(&a, calibration) {
        eprintln!("biomedical_vectors: {e}");
        std::process::exit(1);
    }
    bench_setup::report_drift(calibration);
}
