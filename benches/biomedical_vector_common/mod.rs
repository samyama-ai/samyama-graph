//! The parts of the biomedical vector benchmark (#1281) that do not time
//! anything: the synthetic biomedical-shaped corpus, the prepared-directory
//! type files, type subsets and their ground-truth remapping, the merge of
//! per-type results, the recall delta of a quantization against f32, and
//! on-disk size accounting.
//!
//! Shared by `benches/biomedical_vectors.rs` and checked by
//! `tests/biomedical_vectors.rs`: a bench target cannot run its own tests,
//! and the recall delta is the figure the issue calls "accuracy impact", so
//! the code that derives it is tested here.

#![allow(dead_code)]

use std::path::Path;

/// The two embedding levels the issue asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Level {
    /// One vector per concept node, embedded from a short name + synonyms.
    Concept,
    /// One vector per article, embedded from a title + abstract.
    Article,
}

impl Level {
    pub const ALL: [Level; 2] = [Level::Concept, Level::Article];

    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "concept" | "concepts" => Ok(Level::Concept),
            "article" | "articles" => Ok(Level::Article),
            other => Err(format!("unknown level `{other}`; use concept or article")),
        }
    }

    /// The name of the level's subdirectory in a prepared directory, and of
    /// its rows.
    pub fn name(self) -> &'static str {
        match self {
            Level::Concept => "concept",
            Level::Article => "article",
        }
    }

    /// The label the shared-layout index is declared on.
    pub fn label(self) -> &'static str {
        match self {
            Level::Concept => "Concept",
            Level::Article => "Article",
        }
    }

    /// The dimension of the synthetic corpus when none is given: a small
    /// sentence-transformer for short concept text, a BERT-base model for
    /// abstracts.
    pub fn default_dim(self) -> usize {
        match self {
            Level::Concept => 384,
            Level::Article => 768,
        }
    }
}

/// The concept types of the biomedical graph, each a node label, with the
/// share of concept nodes the synthetic corpus gives it and the semantic
/// family it sits in. Types of one family are embedded near each other
/// (a gene and its protein, a drug and its target, a disease and its clinical
/// presentation), which is what makes a type filter non-trivial.
///
/// The shares are an assumption, not a census of any one source; with a
/// prepared directory the corpus's own counts are used.
pub const CONCEPT_TYPES: &[(&str, u32, usize)] = &[
    ("Gene", 24, 0),
    ("Protein", 20, 0),
    ("MeSHTerm", 16, 3),
    ("Drug", 12, 1),
    ("Disease", 10, 2),
    ("ClinicalCondition", 8, 2),
    ("Target", 6, 1),
    ("Pathway", 4, 0),
];

/// The article level has one node type.
pub const ARTICLE_TYPE: &str = "Article";

/// The type subsets measured with a filter when `--filter-types` is not
/// given: a mid-sized type and a small one, so one filter keeps about an
/// eighth of the index and the other a twenty-fifth.
pub const DEFAULT_FILTER_TYPES: &[&str] = &["Drug", "Pathway"];

/// Split `n` in proportion to `weights` by largest remainder, ties to the
/// earlier weight. The counts sum to `n` exactly.
pub fn apportion(n: usize, weights: &[u32]) -> Vec<usize> {
    let total: u64 = weights.iter().map(|w| *w as u64).sum();
    if total == 0 {
        return vec![0; weights.len()];
    }
    let mut counts: Vec<usize> = weights
        .iter()
        .map(|w| (n as u64 * *w as u64 / total) as usize)
        .collect();
    let mut rest: Vec<(u64, usize)> = weights
        .iter()
        .enumerate()
        .map(|(i, w)| (n as u64 * *w as u64 % total, i))
        .collect();
    rest.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let short = n - counts.iter().sum::<usize>();
    for (_, i) in rest.into_iter().take(short) {
        counts[i] += 1;
    }
    counts
}

/// Row-major vectors with one type per vector.
#[derive(Clone, Debug, PartialEq)]
pub struct Corpus {
    pub dim: usize,
    pub data: Vec<f32>,
    /// Index into `type_names`, one per vector.
    pub types: Vec<u16>,
    pub type_names: Vec<String>,
}

impl Corpus {
    pub fn len(&self) -> usize {
        self.types.len()
    }

    pub fn is_empty(&self) -> bool {
        self.types.is_empty()
    }

    pub fn row(&self, i: usize) -> &[f32] {
        &self.data[i * self.dim..(i + 1) * self.dim]
    }

    pub fn type_id(&self, name: &str) -> Option<u16> {
        self.type_names
            .iter()
            .position(|t| t == name)
            .map(|i| i as u16)
    }

    /// Vectors of each type, in `type_names` order.
    pub fn type_counts(&self) -> Vec<usize> {
        let mut counts = vec![0; self.type_names.len()];
        for t in &self.types {
            counts[*t as usize] += 1;
        }
        counts
    }
}

/// The generator behind a synthetic level: topic centroids by type. Base
/// vectors and queries are drawn from one model with different seeds, so the
/// queries ask about the same topics the base set covers without being copies
/// of base vectors.
#[derive(Clone, Debug)]
pub struct Model {
    pub dim: usize,
    pub type_names: Vec<String>,
    weights: Vec<u32>,
    /// Topic centroids of each type, unit length.
    topics: Vec<Vec<Vec<f32>>>,
    /// Norm of the noise added to a topic centroid before normalising.
    spread: f32,
}

type Rng = rand::rngs::StdRng;

fn rng(seed: u64) -> Rng {
    use rand::SeedableRng;
    Rng::seed_from_u64(seed)
}

/// A vector of norm about `scale`: each coordinate a sum of three uniforms,
/// bell-shaped like the generator in `ann_common`.
fn noise(rng: &mut Rng, dim: usize, scale: f32) -> Vec<f32> {
    use rand::Rng as _;
    let a = scale / (dim as f32).sqrt();
    (0..dim)
        .map(|_| (0..3).map(|_| rng.gen_range(-a..=a)).sum::<f32>())
        .collect()
}

/// `v` scaled to unit length; a zero vector is left as it is.
pub fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

fn plus(a: &[f32], b: &[f32]) -> Vec<f32> {
    a.iter().zip(b).map(|(x, y)| x + y).collect()
}

/// Vectors per topic the synthetic corpus aims for. Concept text is short
/// and specific, so its topics are small; abstracts spread wider.
const CONCEPTS_PER_TOPIC: usize = 50;
const ARTICLES_PER_TOPIC: usize = 200;
/// Topics per type are capped, so a large corpus gets denser topics rather
/// than an unbounded number of centroids.
const MAX_TOPICS_PER_TYPE: usize = 2048;

impl Model {
    /// The concept-level model for a corpus of about `n` vectors: four
    /// semantic families, each type a direction inside its family, each topic
    /// a direction inside its type.
    pub fn concept(n: usize, dim: usize, seed: u64) -> Self {
        let mut r = rng(seed);
        let families = 1 + CONCEPT_TYPES.iter().map(|t| t.2).max().unwrap_or(0);
        let family: Vec<Vec<f32>> = (0..families)
            .map(|_| normalize(noise(&mut r, dim, 1.0)))
            .collect();
        let weights: Vec<u32> = CONCEPT_TYPES.iter().map(|t| t.1).collect();
        let counts = apportion(n, &weights);
        let topics = CONCEPT_TYPES
            .iter()
            .zip(&counts)
            .map(|(&(_, _, f), &count)| {
                let centre = normalize(plus(&family[f], &noise(&mut r, dim, 0.6)));
                let topics = count
                    .div_ceil(CONCEPTS_PER_TOPIC)
                    .clamp(1, MAX_TOPICS_PER_TYPE);
                (0..topics)
                    .map(|_| normalize(plus(&centre, &noise(&mut r, dim, 0.5))))
                    .collect()
            })
            .collect();
        Model {
            dim,
            type_names: CONCEPT_TYPES.iter().map(|t| t.0.to_string()).collect(),
            weights,
            topics,
            spread: 0.35,
        }
    }

    /// The article-level model: one type, larger topics, wider spread.
    pub fn article(n: usize, dim: usize, seed: u64) -> Self {
        let mut r = rng(seed);
        let topics = n.div_ceil(ARTICLES_PER_TOPIC).clamp(1, MAX_TOPICS_PER_TYPE);
        let topics = vec![(0..topics)
            .map(|_| normalize(noise(&mut r, dim, 1.0)))
            .collect()];
        Model {
            dim,
            type_names: vec![ARTICLE_TYPE.to_string()],
            weights: vec![1],
            topics,
            spread: 0.6,
        }
    }

    pub fn for_level(level: Level, n: usize, dim: usize, seed: u64) -> Self {
        match level {
            Level::Concept => Self::concept(n, dim, seed),
            Level::Article => Self::article(n, dim, seed),
        }
    }

    /// `n` unit vectors, each type getting its share by [`apportion`], in a
    /// seeded shuffled order: a loader inserts one type at a time, but the
    /// bench should not measure an insertion order nobody chose.
    pub fn sample(&self, n: usize, seed: u64) -> Corpus {
        use rand::{seq::SliceRandom, Rng as _};
        let mut r = rng(seed);
        let counts = apportion(n, &self.weights);
        let mut types: Vec<u16> = counts
            .iter()
            .enumerate()
            .flat_map(|(t, c)| std::iter::repeat_n(t as u16, *c))
            .collect();
        types.shuffle(&mut r);
        let mut data = Vec::with_capacity(n * self.dim);
        for t in &types {
            let topics = &self.topics[*t as usize];
            let topic = &topics[r.gen_range(0..topics.len())];
            data.extend(normalize(plus(
                topic,
                &noise(&mut r, self.dim, self.spread),
            )));
        }
        Corpus {
            dim: self.dim,
            data,
            types,
            type_names: self.type_names.clone(),
        }
    }
}

/// The salt that separates a level's query seed from its base seed.
pub const QUERY_SALT: u64 = 0x1281_0000_0000_0001;

/// Base vectors and queries of a synthetic level.
pub fn synthetic(
    level: Level,
    n: usize,
    queries: usize,
    dim: usize,
    seed: u64,
) -> (Corpus, Corpus) {
    let model = Model::for_level(level, n, dim, seed);
    (
        model.sample(n, seed.wrapping_add(1)),
        model.sample(queries, seed ^ QUERY_SALT),
    )
}

// --------------------------------------------------------- prepared directory

/// Whether `name` can be a node label and an index file name: ASCII letters,
/// digits and `_`, starting with a letter. A checkpoint names its files after
/// the label, so a type with a separator in it must be refused before any
/// index is built from it.
pub fn valid_type_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Parse a types file: one type name per line, one line per vector, in the
/// order of the vectors. Blank lines are refused rather than skipped, since
/// skipping one would shift every type after it onto the wrong vector.
pub fn parse_types(text: &str, expected: usize, what: &str) -> Result<Vec<String>, String> {
    let names: Vec<String> = text.lines().map(|l| l.trim().to_string()).collect();
    for (i, name) in names.iter().enumerate() {
        if !valid_type_name(name) {
            return Err(format!(
                "{what}: line {}: `{name}` is not a type name (letters, digits, _)",
                i + 1
            ));
        }
    }
    if names.len() != expected {
        return Err(format!(
            "{what}: {} types for {expected} vectors",
            names.len()
        ));
    }
    Ok(names)
}

/// Type names as indices, the names sorted so the order does not depend on
/// which type the file happened to list first.
pub fn intern(names: &[String]) -> (Vec<u16>, Vec<String>) {
    let mut distinct: Vec<String> = names.to_vec();
    distinct.sort();
    distinct.dedup();
    let ids = names
        .iter()
        .map(|n| distinct.binary_search(n).expect("interned") as u16)
        .collect();
    (ids, distinct)
}

// ------------------------------------------------------- subsets and results

/// The positions of the vectors of type `t`, ascending.
pub fn positions_of(types: &[u16], t: u16) -> Vec<u32> {
    types
        .iter()
        .enumerate()
        .filter(|(_, x)| **x == t)
        .map(|(i, _)| i as u32)
        .collect()
}

/// The rows of `data` at `positions`, as one row-major block.
pub fn gather(data: &[f32], dim: usize, positions: &[u32]) -> Vec<f32> {
    let mut out = Vec::with_capacity(positions.len() * dim);
    for p in positions {
        let p = *p as usize;
        out.extend_from_slice(&data[p * dim..(p + 1) * dim]);
    }
    out
}

/// Ground truth computed over a subset names subset rows; the index answers
/// with corpus positions. Map each subset row back to its position.
pub fn remap(ids: &[u32], positions: &[u32]) -> Vec<u32> {
    ids.iter().map(|i| positions[*i as usize]).collect()
}

/// The `k` nearest of several indexes' answers, by distance then id — what
/// a search over one index per type returns for "any type". An id answered
/// by two lists is kept once, at its best distance.
pub fn merge_top_k(lists: &[Vec<(u32, f32)>], k: usize) -> Vec<u32> {
    let mut all: Vec<(u32, f32)> = lists.iter().flatten().copied().collect();
    all.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
    let mut seen = std::collections::HashSet::new();
    all.into_iter()
        .filter(|(id, _)| seen.insert(*id))
        .take(k)
        .map(|(id, _)| id)
        .collect()
}

// ------------------------------------------------------------ accuracy impact

/// The quantization every other is compared with.
pub const BASELINE: &str = "f32";

/// One measured recall, keyed by everything but the quantization: `group`
/// names the level, layout, subset and index kind; `width` is `ef` or
/// `nprobe`.
#[derive(Clone, Debug, PartialEq)]
pub struct Measured {
    pub group: String,
    pub quantization: String,
    pub width: usize,
    pub recall: f64,
}

/// Each row's recall minus the f32 row's at the same group and width: the
/// accuracy a quantization costs, with the graph, filter and search width
/// held fixed. `None` where no f32 row was measured, which is reported as
/// unknown rather than as zero.
pub fn recall_deltas(rows: &[Measured]) -> Vec<Option<f64>> {
    rows.iter()
        .map(|r| {
            rows.iter()
                .find(|b| b.quantization == BASELINE && b.group == r.group && b.width == r.width)
                .map(|b| r.recall - b.recall)
        })
        .collect()
}

/// Parse a comma list of quantization names against what this build offers,
/// `(name, value)` pairs. A name the build does not have is an error naming
/// the ones it does, so `--quantization int8` says what to use instead
/// rather than silently measuring something else.
pub fn select<'a, T>(
    s: &str,
    known: &'a [(&'a str, T)],
    flag: &str,
    parse: impl Fn(&str) -> Option<T>,
) -> Result<Vec<(&'a str, T)>, String>
where
    T: Copy + PartialEq,
{
    let mut out: Vec<(&str, T)> = Vec::new();
    for name in s.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let found = parse(name).and_then(|v| known.iter().find(|(_, k)| *k == v).copied());
        match found {
            Some(entry) => {
                if !out.iter().any(|(_, v)| *v == entry.1) {
                    out.push(entry);
                }
            }
            None => {
                let names: Vec<&str> = known.iter().map(|(n, _)| *n).collect();
                return Err(format!(
                    "{flag}: `{name}` is not available in this build; available: {}",
                    names.join(", ")
                ));
            }
        }
    }
    if out.is_empty() {
        return Err(format!("{flag}: empty list"));
    }
    Ok(out)
}

// ------------------------------------------------------------------ disk size

/// `bytes` per vector of `n`; zero vectors cost nothing per vector.
pub fn per_vector(bytes: u64, n: usize) -> f64 {
    if n == 0 {
        0.0
    } else {
        bytes as f64 / n as f64
    }
}

/// `part` as a share of `whole`, zero when there is nothing to share.
pub fn share(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64
    }
}

/// What a checkpoint's bytes are spent on, by file kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointBytes {
    /// Each index's vector list (`<label>_<property>.hnsw`), always f32.
    pub vector_lists: u64,
    /// HNSW graph files (`*.topology.ids`, `*.topology.hnsw.graph`,
    /// `*.topology.hnsw.data`), the data file at the index's precision.
    pub graphs: u64,
    /// IVF centroids (`*.partition.ivf`).
    pub centroids: u64,
    /// Manifest, metadata and the `CURRENT` pointer.
    pub other: u64,
}

impl CheckpointBytes {
    pub fn total(&self) -> u64 {
        self.vector_lists + self.graphs + self.centroids + self.other
    }

    /// Count one file by its name; see `docs/benchmarks/vector-checkpoints.md`
    /// for the names a generation holds.
    pub fn add(&mut self, file_name: &str, bytes: u64) {
        if file_name.contains(".topology.") {
            self.graphs += bytes;
        } else if file_name.ends_with(".partition.ivf") {
            self.centroids += bytes;
        } else if file_name.ends_with(".hnsw") {
            self.vector_lists += bytes;
        } else {
            self.other += bytes;
        }
    }
}

/// Every file under `path`, counted by kind.
pub fn checkpoint_bytes(path: &Path) -> std::io::Result<CheckpointBytes> {
    fn walk(path: &Path, into: &mut CheckpointBytes) -> std::io::Result<()> {
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let meta = std::fs::symlink_metadata(entry.path())?;
            if meta.is_dir() {
                walk(&entry.path(), into)?;
            } else if meta.is_file() {
                into.add(&entry.file_name().to_string_lossy(), meta.len());
            }
        }
        Ok(())
    }
    let mut bytes = CheckpointBytes::default();
    walk(path, &mut bytes)?;
    Ok(bytes)
}
