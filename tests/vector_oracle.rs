//! The vector index against an exact oracle, and under transformations that
//! must not change its answer (#1981).
//!
//! The metric bug (#1967) and the delete bug (#1968) both shipped with green
//! tests, because what was tested was one metric, one size, no deletes, no
//! filter and no restart. Every test here computes the right answer itself, by
//! brute force in `f64`, and compares the index with it by **node identity**:
//! which nodes come back, never the number reported as their score. Whether a
//! score is a distance or a similarity, and how it is computed, is the index's
//! business; the set of nearest nodes is not.
//!
//! Two kinds of check:
//!
//! - **Exact**, at 120 vectors. Up to 128 the index scans exactly, so the
//!   answer must equal the oracle's.
//! - **Recall**, at 1,000 and more, where the HNSW graph answers. The floor is
//!   0.90, as in `vector_recall_floor.rs` and for its reason: HNSW builds a
//!   different graph each time, so a floor at the median fails on a bad draw,
//!   while a wrong metric, a dead entry answering, or a lost vector drops
//!   recall far below it. Graph searches here use a width of a quarter of the
//!   corpus (see `each_metric_ranks_and_scores_as_an_exact_scan_does` for what
//!   the default width did to CI); these tests are about which question the
//!   index answers, not how wide it has to look.
//!
//! Relations checked:
//!
//! | relation | why it should hold |
//! |---|---|
//! | index = oracle, each metric | the index answers with the metric it was declared with |
//! | every stored vector finds itself (L2) | #1498, for the metric `vector_recall_floor.rs` does not cover |
//! | scaling vectors (cosine) | cosine ignores length |
//! | translating data and query (L2) | L2 depends on differences only |
//! | adding far-away points (L2) | a point farther than every current answer is never an answer |
//! | deleting nodes (cosine, ip) | the answer is the oracle's over the survivors |
//! | dump and load, with deletes | a restart changes nothing a query can see |
//! | `.sgsnap` export and import, with deletes | as above, through the snapshot path |
//! | filtered search (cosine, ip) | the answer is the oracle's over the passing nodes |
//!
//! The data is seeded, and avoids the two-lines-in-2-D shape on which the
//! greedy search is known to stall (see `vector_declared_metric.rs`).

use samyama::graph::PropertyValue;
use samyama::graph::{GraphStore, NodeId};
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::{DistanceMetric, VectorIndex};
use std::collections::HashSet;

const DIM: usize = 8;
const K: usize = 10;
const FLOOR: f64 = 0.90;
/// Below `EXACT_SEARCH_MAX` (128): the index scans exactly.
const SMALL: usize = 120;
/// Above it: the HNSW graph answers.
const LARGE: usize = 1_000;
const METRICS: [DistanceMetric; 3] = [
    DistanceMetric::Cosine,
    DistanceMetric::L2,
    DistanceMetric::InnerProduct,
];

// ---------------------------------------------------------------- data ----

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    /// Uniform in [-1, 1).
    fn next(&mut self) -> f32 {
        let x = &mut self.0;
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        ((*x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }
}

/// Points uniform in the cube [-1, 1)^DIM.
fn cube(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut r = Rng::new(seed);
    (0..n)
        .map(|_| (0..DIM).map(|_| r.next()).collect())
        .collect()
}

/// Points with lengths between 0.75 and 1.25: what embeddings for an inner
/// product index usually look like. Over arbitrary lengths the inner product
/// is not a distance and the "nearest" vector is mostly the longest one.
fn near_unit(n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut r = Rng::new(seed);
    (0..n)
        .map(|_| {
            let v: Vec<f32> = (0..DIM).map(|_| r.next()).collect();
            let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-6);
            let len = 1.0 + 0.25 * r.next();
            v.iter().map(|x| x / norm * len).collect()
        })
        .collect()
}

/// The data an index of `metric` is tested on.
fn data_for(metric: DistanceMetric, n: usize, seed: u64) -> Vec<Vec<f32>> {
    match metric {
        DistanceMetric::InnerProduct => near_unit(n, seed),
        _ => cube(n, seed),
    }
}

// -------------------------------------------------------------- oracle ----

/// Smaller is nearer, in f64, written here from the definitions.
fn oracle_distance(metric: DistanceMetric, a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum();
    match metric {
        DistanceMetric::L2 => a
            .iter()
            .zip(b)
            .map(|(x, y)| (*x as f64 - *y as f64).powi(2))
            .sum::<f64>()
            .sqrt(),
        DistanceMetric::Cosine => {
            let na: f64 = a.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
            let nb: f64 = b.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
            1.0 - dot / (na * nb)
        }
        DistanceMetric::InnerProduct => -dot,
    }
}

/// The exact `k` nearest ids to `q` among `(id, vector)` pairs.
fn oracle<'a>(
    metric: DistanceMetric,
    candidates: impl Iterator<Item = (u64, &'a Vec<f32>)>,
    q: &[f32],
    k: usize,
) -> HashSet<u64> {
    let mut d: Vec<(u64, f64)> = candidates
        .map(|(id, v)| (id, oracle_distance(metric, q, v)))
        .collect();
    d.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then(a.0.cmp(&b.0)));
    d.into_iter().take(k).map(|p| p.0).collect()
}

/// Oracle over all of `data`, ids being positions.
fn oracle_all(metric: DistanceMetric, data: &[Vec<f32>], q: &[f32], k: usize) -> HashSet<u64> {
    oracle(
        metric,
        data.iter().enumerate().map(|(i, v)| (i as u64, v)),
        q,
        k,
    )
}

// --------------------------------------------------------------- index ----

fn build(metric: DistanceMetric, data: &[Vec<f32>]) -> VectorIndex {
    let mut index = VectorIndex::new(DIM, metric);
    for (i, v) in data.iter().enumerate() {
        index.add(NodeId::new(i as u64), v).expect("add");
    }
    index
}

/// The ids the index returns, searched a quarter of the corpus wide.
fn search(index: &VectorIndex, q: &[f32], k: usize) -> Vec<u64> {
    let ef = (index.len() / 4).max(64);
    index
        .search_ef(q, k, Some(ef))
        .expect("search")
        .iter()
        .map(|p| p.0.as_u64())
        .collect()
}

/// Accumulates recall over many queries.
#[derive(Default)]
struct Recall {
    hit: usize,
    total: usize,
}

impl Recall {
    fn add(&mut self, got: &[u64], want: &HashSet<u64>) {
        let distinct: HashSet<u64> = got.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            got.len(),
            "a node was returned twice: {got:?}"
        );
        self.hit += got.iter().filter(|id| want.contains(id)).count();
        self.total += want.len();
    }
    fn check(&self, what: &str) {
        let r = self.hit as f64 / self.total.max(1) as f64;
        assert!(
            r >= FLOOR,
            "{what}: recall@{K} {r:.3} ({}/{}) is below {FLOOR}",
            self.hit,
            self.total
        );
    }
}

fn as_set(ids: &[u64]) -> HashSet<u64> {
    ids.iter().copied().collect()
}

// --------------------------------------------------------- the oracle ----

/// At 120 vectors the index scans exactly, so its answer is the oracle's.
#[test]
fn each_metric_equals_the_oracle_when_the_index_scans_exactly() {
    for metric in METRICS {
        let data = data_for(metric, SMALL, 11);
        let index = build(metric, &data);
        for q in data_for(metric, 20, 12) {
            let got = search(&index, &q, K);
            assert_eq!(got.len(), K, "{metric:?}: short result");
            assert_eq!(as_set(&got), oracle_all(metric, &data, &q, K), "{metric:?}");
        }
    }
}

/// Through the graph, each metric finds most of the oracle's answer. A graph
/// built or searched with the wrong metric (#1967) falls far below the floor.
#[test]
fn each_metric_keeps_recall_against_the_oracle_through_the_graph() {
    for metric in METRICS {
        let data = data_for(metric, LARGE, 21);
        let index = build(metric, &data);
        let mut recall = Recall::default();
        for q in data_for(metric, 30, 22) {
            let got = search(&index, &q, K);
            assert_eq!(got.len(), K, "{metric:?}: short result");
            recall.add(&got, &oracle_all(metric, &data, &q, K));
        }
        recall.check(&format!("{metric:?} n={LARGE}"));
    }
}

/// `vector_recall_floor.rs` checks this for cosine. Under L2 a stored vector
/// is at distance 0 from itself and nothing else is (the corpus has no
/// duplicates), so it must come first: #1498 for the other metric.
#[test]
fn under_l2_every_stored_vector_is_its_own_nearest_neighbour() {
    let data = cube(400, 31);
    let index = build(DistanceMetric::L2, &data);
    for (i, v) in data.iter().enumerate() {
        let got = search(&index, v, 1);
        assert_eq!(got, vec![i as u64], "querying with vector {i}");
    }
}

// --------------------------------------------------------- metamorphic ----

/// Each vector multiplied by its own positive factor, from 0.01 to 100, and
/// the query by another. Cosine ignores length, so the answer is the
/// unscaled oracle's.
#[test]
fn scaling_vectors_leaves_the_cosine_answer_unchanged() {
    let factor = |i: usize| 10f32.powf(((i * 7919) % 401) as f32 / 100.0 - 2.0);
    let scale = |v: &Vec<f32>, f: f32| -> Vec<f32> { v.iter().map(|x| x * f).collect() };
    for n in [SMALL, LARGE] {
        let data = cube(n, 41);
        let scaled: Vec<Vec<f32>> = data
            .iter()
            .enumerate()
            .map(|(i, v)| scale(v, factor(i)))
            .collect();
        let index = build(DistanceMetric::Cosine, &scaled);
        let mut recall = Recall::default();
        for (j, q) in cube(30, 42).iter().enumerate() {
            let want = oracle_all(DistanceMetric::Cosine, &data, q, K);
            let got = search(&index, &scale(q, factor(j + n)), K);
            if n == SMALL {
                assert_eq!(as_set(&got), want, "n={n}, query {j}");
            }
            recall.add(&got, &want);
        }
        recall.check(&format!("cosine, scaled, n={n}"));
    }
}

/// Data and query moved by the same offset. L2 depends on differences only,
/// so the answer is the untranslated oracle's.
#[test]
fn translating_data_and_query_leaves_the_l2_answer_unchanged() {
    let offset: Vec<f32> = (0..DIM).map(|d| 5.0 - 1.5 * d as f32).collect();
    let shift = |v: &Vec<f32>| -> Vec<f32> { v.iter().zip(&offset).map(|(x, o)| x + o).collect() };
    for n in [SMALL, LARGE] {
        let data = cube(n, 51);
        let moved: Vec<Vec<f32>> = data.iter().map(shift).collect();
        let index = build(DistanceMetric::L2, &moved);
        let mut recall = Recall::default();
        for (j, q) in cube(30, 52).iter().enumerate() {
            let want = oracle_all(DistanceMetric::L2, &data, q, K);
            let got = search(&index, &shift(q), K);
            if n == SMALL {
                assert_eq!(as_set(&got), want, "n={n}, query {j}");
            }
            recall.add(&got, &want);
        }
        recall.check(&format!("l2, translated, n={n}"));
    }
}

/// Points placed a thousand units away from the cube the queries are drawn
/// from are farther than every point in it, so they never enter a top-k and
/// the answer is the oracle's over the cube alone.
#[test]
fn adding_far_away_points_leaves_the_l2_answer_unchanged() {
    // (near, far): 100 + 20 stays an exact scan, 1,000 + 500 goes through the graph.
    for (near, far) in [(100usize, 20usize), (LARGE, 500)] {
        let data = cube(near, 61);
        let mut index = build(DistanceMetric::L2, &data);
        for (i, mut v) in cube(far, 62).into_iter().enumerate() {
            v[i % DIM] += if i % 2 == 0 { 1_000.0 } else { -1_000.0 };
            index.add(NodeId::new((near + i) as u64), &v).expect("add");
        }
        let mut recall = Recall::default();
        for (j, q) in cube(30, 63).iter().enumerate() {
            let want = oracle_all(DistanceMetric::L2, &data, q, K);
            let got = search(&index, q, K);
            assert!(
                got.iter().all(|id| (*id as usize) < near),
                "near={near}: a far point was returned: {got:?}"
            );
            if near + far <= 128 {
                assert_eq!(as_set(&got), want, "near={near}, query {j}");
            }
            recall.add(&got, &want);
        }
        recall.check(&format!("l2, {far} far points added to {near}"));
    }
}

/// `vector_deletes.rs` checks this for L2. A third of the nodes deleted, the
/// answer is the oracle's over the survivors, and no deleted node returns.
#[test]
fn deleting_nodes_leaves_the_survivors_answer_for_cosine_and_inner_product() {
    for metric in [DistanceMetric::Cosine, DistanceMetric::InnerProduct] {
        for n in [SMALL + 60, 1_500] {
            let data = data_for(metric, n, 71);
            let mut index = build(metric, &data);
            let deleted = |i: u64| i % 3 == 1;
            for i in (0..n as u64).filter(|i| deleted(*i)) {
                assert!(
                    index.remove(NodeId::new(i)),
                    "{metric:?}: {i} was not indexed"
                );
            }
            let mut recall = Recall::default();
            for (j, q) in data_for(metric, 30, 72).iter().enumerate() {
                let want = oracle(
                    metric,
                    data.iter()
                        .enumerate()
                        .map(|(i, v)| (i as u64, v))
                        .filter(|(i, _)| !deleted(*i)),
                    q,
                    K,
                );
                let got = search(&index, q, K);
                assert_eq!(got.len(), K, "{metric:?} n={n}: short result");
                assert!(
                    got.iter().all(|id| !deleted(*id)),
                    "{metric:?} n={n}: a deleted node was returned: {got:?}"
                );
                if index.len() <= 128 {
                    assert_eq!(as_set(&got), want, "{metric:?} n={n}, query {j}");
                }
                recall.add(&got, &want);
            }
            recall.check(&format!("{metric:?}, a third deleted, n={n}"));
        }
    }
}

/// `dump` and `load` are what a restart does to an index. With a share of the
/// nodes deleted first, the reloaded index answers as the oracle over the
/// survivors does: exactly at a size it scans, within the floor above it, and
/// never with a deleted node.
#[test]
fn a_dump_and_load_round_trip_changes_no_answer() {
    let dir = tempfile::tempdir().expect("tempdir");
    for metric in METRICS {
        // 160 with a quarter deleted leaves 120, which the index scans.
        for n in [SMALL + 40, LARGE] {
            let data = data_for(metric, n, 81);
            let mut index = build(metric, &data);
            let deleted = |i: u64| i % 4 == 0;
            for i in (0..n as u64).filter(|i| deleted(*i)) {
                index.remove(NodeId::new(i));
            }
            let path = dir.path().join(format!("{metric:?}-{n}.idx"));
            index.dump(&path).expect("dump");
            let loaded = VectorIndex::load(&path, DIM, metric).expect("load");
            assert_eq!(loaded.len(), index.len(), "{metric:?} n={n}: size changed");
            assert_eq!(loaded.metric(), metric);

            let mut before = Recall::default();
            let mut after = Recall::default();
            for (j, q) in data_for(metric, 25, 82).iter().enumerate() {
                let want = oracle(
                    metric,
                    data.iter()
                        .enumerate()
                        .map(|(i, v)| (i as u64, v))
                        .filter(|(i, _)| !deleted(*i)),
                    q,
                    K,
                );
                let a = search(&index, q, K);
                let b = search(&loaded, q, K);
                assert!(
                    b.iter().all(|id| !deleted(*id)),
                    "{metric:?} n={n}: a deleted node came back after reload: {b:?}"
                );
                if loaded.len() <= 128 {
                    assert_eq!(as_set(&a), as_set(&b), "{metric:?} n={n}, query {j}");
                    assert_eq!(as_set(&b), want, "{metric:?} n={n}, query {j}");
                }
                before.add(&a, &want);
                after.add(&b, &want);
            }
            before.check(&format!("{metric:?} n={n}, before reload"));
            after.check(&format!("{metric:?} n={n}, after reload"));
        }
    }
}

// ------------------------------------------------- through the engine ----

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

fn ids(s: &GraphStore, q: &str) -> Vec<u64> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::Integer(i))) => *i as u64,
            other => panic!("{other:?}"),
        })
        .collect()
}

fn similarity(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::L2 => "l2",
        DistanceMetric::InnerProduct => "inner_product",
    }
}

fn literal(v: &[f32]) -> String {
    // Fixed-point: `{:?}` writes a value below 1e-5 in exponent form.
    let parts: Vec<String> = v.iter().map(|x| format!("{x:.9}")).collect();
    format!("[{}]", parts.join(","))
}

/// `:Emb` nodes with `id` = position in `data`, `cat = id % 10` and `v`,
/// under an index named `emb` declared with `metric`.
fn store(metric: DistanceMetric, data: &[Vec<f32>]) -> GraphStore {
    let mut s = GraphStore::new();
    write(
        &mut s,
        &format!(
            "CREATE VECTOR INDEX emb FOR (n:Emb) ON (n.v) \
             OPTIONS {{dimensions: {DIM}, similarity: '{}'}}",
            similarity(metric)
        ),
    );
    for chunk in (0..data.len()).collect::<Vec<_>>().chunks(250) {
        let nodes: Vec<String> = chunk
            .iter()
            .map(|&i| format!("(:Emb {{id:{i}, cat:{}, v:{}}})", i % 10, literal(&data[i])))
            .collect();
        write(&mut s, &format!("CREATE {}", nodes.join(", ")));
    }
    s
}

/// The ids `queryNodes` returns for `q`, under an optional WHERE on `node`.
/// Nothing is ordered by score: what the score means is not this test's
/// concern.
fn query_nodes(s: &GraphStore, q: &[f32], k: usize, filter: Option<&str>) -> Vec<u64> {
    let filter = filter.map(|f| format!("WHERE {f} ")).unwrap_or_default();
    ids(
        s,
        &format!(
            "CALL db.index.vector.queryNodes('emb', {k}, {}) YIELD node {filter}\
             RETURN node.id AS id",
            literal(q)
        ),
    )
}

/// A `.sgsnap` export and import is the other restart. Nodes get new ids on
/// import, so identity here is the `id` property. With a share deleted before
/// the export, the imported store answers as the oracle over the survivors.
#[test]
fn a_snapshot_round_trip_changes_no_answer() {
    for metric in [DistanceMetric::Cosine, DistanceMetric::L2] {
        for n in [SMALL + 30, LARGE] {
            let data = data_for(metric, n, 91);
            let mut src = store(metric, &data);
            write(&mut src, "MATCH (n:Emb) WHERE n.id % 5 = 2 DETACH DELETE n");
            let deleted = |i: u64| i % 5 == 2;
            let live = (0..n as u64).filter(|i| !deleted(*i)).count();

            let mut bytes = Vec::new();
            samyama::snapshot::export_tenant(&src, &mut bytes).expect("export");
            let mut dst = GraphStore::new();
            samyama::snapshot::import_tenant(&mut dst, &bytes[..]).expect("import");

            let mut before = Recall::default();
            let mut after = Recall::default();
            for (j, q) in data_for(metric, 20, 92).iter().enumerate() {
                let want = oracle(
                    metric,
                    data.iter()
                        .enumerate()
                        .map(|(i, v)| (i as u64, v))
                        .filter(|(i, _)| !deleted(*i)),
                    q,
                    K,
                );
                let a = query_nodes(&src, q, K, None);
                let b = query_nodes(&dst, q, K, None);
                assert_eq!(b.len(), K, "{metric:?} n={n}: short result after import");
                assert!(
                    b.iter().all(|id| !deleted(*id)),
                    "{metric:?} n={n}: a deleted node came back after import: {b:?}"
                );
                if live <= 128 {
                    assert_eq!(as_set(&a), as_set(&b), "{metric:?} n={n}, query {j}");
                    assert_eq!(as_set(&b), want, "{metric:?} n={n}, query {j}");
                }
                before.add(&a, &want);
                after.add(&b, &want);
            }
            before.check(&format!("{metric:?} n={n}, before export"));
            after.check(&format!("{metric:?} n={n}, after import"));
        }
    }
}

/// `vector_filtered_search.rs` checks this for L2. Under cosine and inner
/// product, every row passes the WHERE, and the rows are the oracle's over the
/// nodes that pass it: one in ten, and one in two.
#[test]
fn a_filtered_search_is_the_oracle_over_the_passing_nodes() {
    let n = 1_500;
    for metric in [DistanceMetric::Cosine, DistanceMetric::InnerProduct] {
        let data = data_for(metric, n, 101);
        let s = store(metric, &data);
        for (filter, pass) in [
            ("node.cat = 3", (|i: u64| i % 10 == 3) as fn(u64) -> bool),
            ("node.cat < 5", |i: u64| i % 10 < 5),
        ] {
            let mut recall = Recall::default();
            for q in data_for(metric, 20, 102) {
                let want = oracle(
                    metric,
                    data.iter()
                        .enumerate()
                        .map(|(i, v)| (i as u64, v))
                        .filter(|(i, _)| pass(*i)),
                    &q,
                    K,
                );
                let got = query_nodes(&s, &q, K, Some(filter));
                assert_eq!(got.len(), K, "{metric:?} {filter}: short result");
                assert!(
                    got.iter().all(|id| pass(*id)),
                    "{metric:?} {filter}: a row fails the WHERE: {got:?}"
                );
                recall.add(&got, &want);
            }
            recall.check(&format!("{metric:?} WHERE {filter}"));
        }
    }
}
