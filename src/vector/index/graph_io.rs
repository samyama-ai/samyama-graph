//! An index's HNSW graph, written beside its vector list so that a verified
//! checkpoint reloads without re-inserting a single vector (#1973).
//!
//! `hnsw_rs` dumps a graph as two files: `<base>.hnsw.graph` holds the layers,
//! every neighbour list with the distance to each neighbour, and the entry
//! point; `<base>.hnsw.data` holds the vectors at the index's own precision.
//! Reloading reads both back and links the neighbour lists from the stored
//! distances, so no distance is computed. What the graph does not know -- the
//! node behind each data id, and which entry is each node's current one
//! (#1968) -- goes in a sidecar, `<base>.ids`.
//!
//! The reader in `hnsw_rs` trusts its input: it asserts on a bad magic number
//! and exits the process on a short data file. It is reached only for files a
//! checkpoint manifest has verified byte for byte (see `checkpoint::verify`),
//! a panic it raises is contained, and every error is the caller's cue to
//! rebuild from the vector list instead. The files are a same-machine cache:
//! `hnsw_rs` writes native-endian `usize`s, so a sidecar from a different
//! word size or byte order is refused rather than read.

use super::{bits_key, Backend, StoredVector, VectorIndex};
use super::{DistanceMetric, HnswParams, MetricDistance, Quantization};
use super::{VectorError, VectorResult};
use half::f16;
use hnsw_rs::prelude::{AnnT, Hnsw, HnswIo, PointIndexation};
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

/// Bumped whenever the sidecar or the way the graph files are read changes.
const FORMAT: u32 = 1;

/// Suffixes of the files a graph dump adds to a checkpoint generation. None
/// can collide with a vector-list file, whose name ends in `.hnsw`.
pub(crate) const GRAPH_SUFFIXES: [&str; 3] = [
    ".topology.ids",
    ".topology.hnsw.graph",
    ".topology.hnsw.data",
];

#[derive(serde::Serialize, serde::Deserialize)]
struct Sidecar {
    format: u32,
    pointer_width: u32,
    little_endian: bool,
    dimensions: usize,
    metric: DistanceMetric,
    quantization: Quantization,
    m: usize,
    ef_construction: usize,
    /// The node behind each graph data id. Data ids are positions, so this
    /// includes superseded entries, which are still in the graph.
    node_ids: Vec<u64>,
    /// Each node's current position.
    current: Vec<(u64, usize)>,
}

fn bad(message: impl Into<String>) -> VectorError {
    VectorError::IndexError(message.into())
}

fn graph_base(name: &str) -> String {
    format!("{name}.topology")
}

fn sidecar_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}{}", GRAPH_SUFFIXES[0]))
}

/// The vectors the graph holds, by data id. Every id below `n` must appear
/// exactly once, at the index's dimension.
fn by_position<T: Clone + Send + Sync>(
    points: &PointIndexation<'_, T>,
    n: usize,
    dimensions: usize,
) -> Result<Vec<Vec<T>>, String> {
    let mut slots: Vec<Option<Vec<T>>> = vec![None; n];
    for point in points {
        let pos = point.get_origin_id();
        let v = point.get_v();
        if pos >= n || v.len() != dimensions || slots[pos].is_some() {
            return Err("graph entries do not match the sidecar".into());
        }
        slots[pos] = Some(v.to_vec());
    }
    slots
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| "graph is missing entries".into())
}

impl VectorIndex {
    /// Write the graph and its sidecar into `dir`, named after the index's
    /// vector-list file `name`. An empty index writes nothing: `hnsw_rs`
    /// cannot dump a graph with no entry point, and rebuilding an empty
    /// index costs nothing. A backend this format does not cover also
    /// writes nothing, and reloads by rebuilding.
    pub(crate) fn dump_graph(&self, dir: &Path, name: &str) -> VectorResult<()> {
        if self.entries() == 0 {
            return Ok(());
        }
        let base = graph_base(name);
        let dumped = catch_unwind(AssertUnwindSafe(|| match &self.backend {
            Backend::F32(h) => Some(h.file_dump(dir, &base).map_err(|e| e.to_string())),
            Backend::F16(h) => Some(h.file_dump(dir, &base).map_err(|e| e.to_string())),
            #[allow(unreachable_patterns)]
            _ => None,
        }));
        match dumped {
            Ok(Some(Ok(written))) if written == base => {}
            Ok(Some(Ok(written))) => {
                return Err(bad(format!("graph dumped as {written}, expected {base}")))
            }
            Ok(Some(Err(e))) => return Err(bad(format!("graph dump failed: {e}"))),
            Ok(None) => return Ok(()),
            Err(_) => return Err(bad("graph dump panicked")),
        }
        let sidecar = Sidecar {
            format: FORMAT,
            pointer_width: usize::BITS,
            little_endian: cfg!(target_endian = "little"),
            dimensions: self.dimensions,
            metric: self.metric,
            quantization: self.quantization,
            m: self.params.m,
            ef_construction: self.params.ef_construction,
            node_ids: (0..self.entries()).map(|pos| self.node_at(pos)).collect(),
            current: self
                .current
                .iter()
                .map(|(&node, &pos)| (node, pos))
                .collect(),
        };
        let bytes = bincode::serialize(&sidecar)
            .map_err(|e| bad(format!("graph sidecar serialization error: {e}")))?;
        std::fs::write(sidecar_path(dir, name), bytes)?;
        Ok(())
    }

    /// Rebuild the index from a graph [`dump_graph`](Self::dump_graph) wrote,
    /// with no distance computation and no insertion. Refuses -- so the
    /// caller rebuilds instead -- a graph written for another shape of index,
    /// precision, platform or format, and one whose files disagree with each
    /// other. Call only on files a checkpoint manifest has verified.
    pub(crate) fn load_graph(
        dir: &Path,
        name: &str,
        dimensions: usize,
        metric: DistanceMetric,
        quantization: Quantization,
        params: HnswParams,
    ) -> VectorResult<Self> {
        let bytes = std::fs::read(sidecar_path(dir, name))?;
        let s: Sidecar = bincode::deserialize(&bytes)
            .map_err(|e| bad(format!("graph sidecar deserialization error: {e}")))?;
        if s.format != FORMAT
            || s.pointer_width != usize::BITS
            || s.little_endian != cfg!(target_endian = "little")
        {
            return Err(bad("graph written by another format or platform"));
        }
        if s.dimensions != dimensions
            || s.metric != metric
            || s.quantization != quantization
            || s.m != params.m
            || s.ef_construction != params.ef_construction
        {
            return Err(bad("graph describes a different index"));
        }
        let n = s.node_ids.len();
        if n == 0 {
            return Err(bad("graph sidecar has no entries"));
        }
        let mut current = HashMap::with_capacity(s.current.len());
        for &(node, pos) in &s.current {
            if pos >= n || s.node_ids[pos] != node || current.insert(node, pos).is_some() {
                return Err(bad("graph sidecar names an inconsistent current entry"));
            }
        }

        // The loaded graph borrows its reader for the reader's lifetime,
        // which exists for memory-mapped data. Nothing is mapped here (the
        // default options read every vector into memory), but the index
        // holds a `'static` graph, so the reader -- a path, a name and a
        // counter -- is leaked once per load.
        let io: &'static HnswIo = Box::leak(Box::new(HnswIo::new(dir, &graph_base(name))));
        let mut index = Self::build(dimensions, metric, quantization, params, 0);
        let loaded = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
            let distance = MetricDistance(metric);
            match quantization {
                Quantization::None => {
                    let mut h: Hnsw<'static, f32, MetricDistance> = io
                        .load_hnsw_with_dist(distance)
                        .map_err(|e| e.to_string())?;
                    check_shape(
                        h.get_nb_point(),
                        h.get_max_nb_connection(),
                        h.get_ef_construction(),
                        n,
                        &params,
                    )?;
                    // Reloading turns this on; a built graph has it off, and
                    // later insertions should link the same way they did.
                    h.set_extend_candidates(false);
                    let vectors = by_position(h.get_point_indexation(), n, dimensions)?;
                    for (pos, vector) in vectors.into_iter().enumerate() {
                        let key = bits_key(&vector, |v: f32| v.to_bits() as u64);
                        index.exact.entry(key).or_default().push(pos);
                        index.stored_f32.push(StoredVector {
                            node_id: s.node_ids[pos],
                            vector,
                        });
                    }
                    index.backend = Backend::F32(h);
                }
                Quantization::Fp16 => {
                    let mut h: Hnsw<'static, f16, MetricDistance> = io
                        .load_hnsw_with_dist(distance)
                        .map_err(|e| e.to_string())?;
                    check_shape(
                        h.get_nb_point(),
                        h.get_max_nb_connection(),
                        h.get_ef_construction(),
                        n,
                        &params,
                    )?;
                    h.set_extend_candidates(false);
                    let vectors = by_position(h.get_point_indexation(), n, dimensions)?;
                    for (pos, vector) in vectors.into_iter().enumerate() {
                        let key = bits_key(&vector, |v: f16| v.to_bits() as u64);
                        index.exact.entry(key).or_default().push(pos);
                        index.stored_f16.push((s.node_ids[pos], vector));
                    }
                    index.backend = Backend::F16(h);
                }
                #[allow(unreachable_patterns)]
                _ => return Err("no graph format for this precision".into()),
            }
            Ok(())
        }));
        match loaded {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(bad(format!("graph reload failed: {e}"))),
            Err(_) => return Err(bad("graph reload panicked")),
        }
        // Every entry not current was superseded or removed, which is how
        // `add` and `remove` count them.
        index.superseded = n - current.len();
        index.current = current;
        Ok(index)
    }
}

/// The reloaded graph is the one the sidecar describes.
fn check_shape(
    points: usize,
    m: u8,
    ef_construction: usize,
    n: usize,
    params: &HnswParams,
) -> Result<(), String> {
    if points != n || m as usize != params.m || ef_construction != params.ef_construction {
        return Err("graph header disagrees with the sidecar".into());
    }
    Ok(())
}
