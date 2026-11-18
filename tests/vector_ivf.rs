//! The IVF vector index kind (#1975).
//!
//! `CREATE VECTOR INDEX ... OPTIONS {kind: 'ivf'}` partitions the vectors
//! into `nlist` lists by seeded k-means and scores, exactly, the `nprobe`
//! lists whose centroids are nearest the query. These tests pin the DDL and
//! its refusals, that recall rises with `nprobe` and is good at a moderate
//! one, that probing every list is the exact scan, that removed and replaced
//! vectors are never returned, that filtered search comes back full, and that
//! a checkpoint or a catalog round trip gives the same index back. Nothing
//! here reads a clock.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::index::catalog::IndexDefinition;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::index::Quantization;
use samyama::vector::{
    DistanceMetric, HnswParams, IndexKind, IvfParams, VectorIndex, VectorIndexManager,
};

fn write(s: &mut GraphStore, q: &str) {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    MutQueryExecutor::new(s, "default".into())
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
}

/// The error a statement fails with, from the parser or the executor.
fn refused(s: &mut GraphStore, q: &str) -> String {
    let query = match parse_query(q) {
        Ok(query) => query,
        Err(e) => return format!("{e:?}"),
    };
    match MutQueryExecutor::new(s, "default".into()).execute(&query) {
        Ok(_) => panic!("{q}: accepted"),
        Err(e) => format!("{e:?}"),
    }
}

fn column(s: &GraphStore, q: &str, col: &str) -> Vec<String> {
    let query = parse_query(q).unwrap_or_else(|e| panic!("{q}: {e:?}"));
    let batch = QueryExecutor::new(s)
        .execute(&query)
        .unwrap_or_else(|e| panic!("{q}: {e:?}"));
    batch
        .records
        .iter()
        .map(|r| match r.get(col) {
            Some(Value::Property(PropertyValue::String(v))) => v.clone(),
            Some(Value::Property(PropertyValue::Integer(i))) => i.to_string(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// A deterministic stream of values in [-1, 1).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
    }
}

/// Deterministic uniform points in `dim` dimensions.
fn points(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ seed);
    (0..n)
        .map(|_| (0..dim).map(|_| rng.next()).collect())
        .collect()
}

/// Deterministic points around `centres` centres, the shape embeddings
/// usually have and the one IVF is built for. The centres depend only on
/// their number and `dim`, so a different `seed` draws other points -- the
/// queries -- around the same centres.
fn clustered(n: usize, dim: usize, centres: usize, seed: u64) -> Vec<Vec<f32>> {
    let middles = points(centres, dim, 0xC0FFEE);
    let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ seed.wrapping_mul(0x9E37_79B9));
    (0..n)
        .map(|i| {
            middles[i % centres]
                .iter()
                .map(|c| c + 0.15 * rng.next())
                .collect()
        })
        .collect()
}

fn ivf(nlist: Option<usize>, nprobe: Option<usize>) -> IndexKind {
    IndexKind::Ivf(IvfParams { nlist, nprobe })
}

fn filled(data: &[Vec<f32>], metric: DistanceMetric, kind: IndexKind) -> VectorIndex {
    let dim = data[0].len();
    let mut index =
        VectorIndex::with_kind(dim, metric, Quantization::None, HnswParams::default(), kind);
    for (i, v) in data.iter().enumerate() {
        index.add(NodeId::new(i as u64), v).unwrap();
    }
    index
}

fn ids(r: &[(NodeId, f32)]) -> Vec<u64> {
    r.iter().map(|(n, _)| n.as_u64()).collect()
}

#[test]
fn the_ddl_selects_ivf_and_show_indexes_reports_it() {
    let mut s = GraphStore::new();
    for (i, p) in clustered(600, 4, 12, 0).iter().enumerate() {
        write(
            &mut s,
            &format!(
                "CREATE (:Doc {{id: {i}, v: [{}, {}, {}, {}]}})",
                p[0], p[1], p[2], p[3]
            ),
        );
    }
    write(
        &mut s,
        "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.v) \
         OPTIONS {dimensions: 4, similarity: 'l2', kind: 'ivf', nlist: 16, nprobe: 4}",
    );
    write(
        &mut s,
        "CREATE VECTOR INDEX FOR (n:Other) ON (n.v) OPTIONS {dimensions: 4, kind: 'hnsw'}",
    );
    let mut shown = column(&s, "SHOW INDEXES", "type");
    shown.sort();
    assert_eq!(
        shown,
        vec![
            "VECTOR[kind=ivf, nlist=16, nprobe=4, lists=16]",
            "VECTOR[m=16, ef_construction=200, ef=auto]",
        ]
    );
    let index = s.vector_index.get_index("Doc", "v").unwrap();
    assert_eq!(index.read().unwrap().kind(), ivf(Some(16), Some(4)));
    assert_eq!(index.read().unwrap().len(), 600, "the rebuild filled it");
    assert_eq!(index.read().unwrap().ivf_lists(), 16, "and trained it");

    // Per query, `nprobe` is the search width, in either call form, and
    // EXPLAIN names the kind.
    let answers: Vec<Vec<String>> = [
        "CALL db.index.vector.queryNodes('Doc', 'v', [0.1, 0.2, 0.3, 0.4], 5, {nprobe: 16}) \
         YIELD node RETURN node.id AS id",
        "CALL db.index.vector.queryNodes('docs', 5, [0.1, 0.2, 0.3, 0.4], {nprobe: 16}) \
         YIELD node RETURN node.id AS id",
    ]
    .iter()
    .map(|q| column(&s, q, "id"))
    .collect();
    assert_eq!(answers[0].len(), 5);
    assert_eq!(answers[0], answers[1]);
    let plan = column(
        &s,
        "EXPLAIN CALL db.index.vector.queryNodes('Doc', 'v', [0.1, 0.2, 0.3, 0.4], 5, {nprobe: 8}) \
         YIELD node RETURN node.id AS id",
        "plan",
    )
    .join("\n");
    assert!(plan.contains("index=ivf"), "{plan}");
    assert!(plan.contains("nprobe=8"), "{plan}");
    let plan = column(
        &s,
        "EXPLAIN CALL db.index.vector.queryNodes('Other', 'v', [0.1, 0.2, 0.3, 0.4], 5) \
         YIELD node RETURN node.id AS id",
        "plan",
    )
    .join("\n");
    assert!(plan.contains("index=hnsw"), "{plan}");

    for (options, says) in [
        ("{nprobe: 0}", "`nprobe` must be a positive integer"),
        ("{nprobe: 100000}", "`nprobe` must be between"),
        ("{ef: 10, nprobe: 10}", "not both"),
    ] {
        let err = refused(
            &mut s,
            &format!(
                "CALL db.index.vector.queryNodes('Doc', 'v', [0.1, 0.2, 0.3, 0.4], 5, {options}) \
                 YIELD node RETURN node"
            ),
        );
        assert!(err.contains(says), "{options}: {err}");
    }
}

#[test]
fn unknown_kinds_and_another_kinds_options_are_refused() {
    let mut s = GraphStore::new();
    for (options, says) in [
        ("kind: 'diskann'", "unknown `kind`"),
        ("kind: 3", "`kind` must be a string"),
        (
            "nlist: 16",
            "`nlist` does not apply to a `kind: 'hnsw'` index",
        ),
        ("kind: 'hnsw', nprobe: 4", "`nprobe` does not apply"),
        (
            "kind: 'ivf', m: 16",
            "`m` does not apply to a `kind: 'ivf'` index",
        ),
        ("kind: 'ivf', ef: 40", "`ef` does not apply"),
        (
            "kind: 'ivf', nlist: 0",
            "`nlist` must be a positive integer",
        ),
        ("kind: 'ivf', nlist: 1000000", "`nlist` must be between"),
        (
            "kind: 'ivf', nprobe: 'x'",
            "`nprobe` must be a positive integer",
        ),
    ] {
        let err = refused(
            &mut s,
            &format!("CREATE VECTOR INDEX FOR (n:D) ON (n.v) OPTIONS {{dimensions: 4, {options}}}"),
        );
        assert!(err.contains(says), "{options}: {err}");
    }
    assert!(
        s.vector_index.get_index("D", "v").is_none(),
        "nothing was created"
    );
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// Recall@10 against the true neighbours rises with `nprobe`, is good at a
/// moderate one, and is perfect once every list is probed.
#[test]
fn recall_rises_with_nprobe_and_is_good_at_a_moderate_one() {
    let data = clustered(6_000, 16, 60, 1);
    let index = filled(&data, DistanceMetric::L2, ivf(Some(64), None));
    assert_eq!(index.ivf_lists(), 64);
    let queries = clustered(60, 16, 60, 2);
    let recall = |nprobe: usize| {
        let mut hits = 0;
        for q in &queries {
            let mut want: Vec<(usize, f32)> = data
                .iter()
                .enumerate()
                .map(|(i, v)| (i, l2(q, v)))
                .collect();
            want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let want: Vec<u64> = want.iter().take(10).map(|p| p.0 as u64).collect();
            let got = index.search_ef(q, 10, Some(nprobe)).unwrap();
            assert_eq!(got.len(), 10);
            hits += got
                .iter()
                .filter(|(id, _)| want.contains(&id.as_u64()))
                .count();
        }
        hits as f64 / (10 * queries.len()) as f64
    };
    let (one, moderate, all) = (recall(1), recall(12), recall(64));
    assert!(
        moderate >= 0.9,
        "recall@10 at nprobe=12 of 64: {moderate:.3}"
    );
    assert!(
        one <= moderate,
        "nprobe=1 gave {one:.3}, 12 gave {moderate:.3}"
    );
    assert_eq!(all, 1.0, "probing every list is exact");
}

/// Probing every list scores every vector, so the answer -- ids, order and
/// distances -- is the exact scan's, for each metric.
#[test]
fn nprobe_equal_to_nlist_is_the_exact_scan() {
    let data = points(2_000, 8, 3);
    for metric in [
        DistanceMetric::L2,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let index = filled(&data, metric, ivf(Some(32), Some(2)));
        assert_eq!(index.ivf_lists(), 32);
        for q in points(25, 8, 4) {
            let got = index.search_ef(&q, 10, Some(32)).unwrap();
            let want = index.search_exact_where(&q, 10, &|_| true).unwrap();
            assert_eq!(ids(&got), ids(&want), "{metric:?}");
            for ((_, a), (_, b)) in got.iter().zip(&want) {
                assert!((a - b).abs() <= 1e-6, "{metric:?}: {a} vs {b}");
            }
        }
        // A stored vector is its own nearest, even at nprobe = 1. Not so
        // for inner product, under which a longer vector can outscore it.
        if metric != DistanceMetric::InnerProduct {
            let got = index.search_ef(&data[17], 1, Some(1)).unwrap();
            assert_eq!(ids(&got), vec![17], "{metric:?}");
        }
    }
}

/// Too few vectors to train on: the index scans, exactly.
#[test]
fn an_untrained_index_is_exact() {
    let data = points(100, 8, 5);
    let index = filled(&data, DistanceMetric::L2, ivf(None, None));
    assert_eq!(index.ivf_lists(), 0);
    let q = &points(1, 8, 6)[0];
    let got = index.search_ef(q, 5, None).unwrap();
    let want = index.search_exact_where(q, 5, &|_| true).unwrap();
    assert_eq!(ids(&got), ids(&want));
}

#[test]
fn removed_and_replaced_vectors_are_not_returned() {
    let data = clustered(3_000, 8, 30, 7);
    let mut index = filled(&data, DistanceMetric::L2, ivf(Some(32), Some(4)));
    // Remove a few, and move a few far away: below the share that compacts,
    // so their old entries are still in the lists.
    for i in 0..40u64 {
        assert!(index.remove(NodeId::new(i)));
    }
    let far = vec![50.0f32; 8];
    for i in 40..60u64 {
        index.add(NodeId::new(i), &far).unwrap();
    }
    assert_eq!(index.len(), 2_960);
    for (i, v) in data.iter().enumerate().take(60) {
        let got = index.search_ef(v, 10, Some(32)).unwrap();
        let got = ids(&got);
        assert!(
            !got.iter().any(|&id| id < 60),
            "query {i} returned a removed or moved vector: {got:?}"
        );
    }
    let got = index.search_ef(&far, 20, Some(1)).unwrap();
    let mut got = ids(&got);
    got.sort_unstable();
    assert_eq!(
        got,
        (40..60).collect::<Vec<_>>(),
        "found at their new place"
    );

    // Past the share that compacts, the index is rebuilt and still answers.
    for i in 60..1_000u64 {
        index.remove(NodeId::new(i));
    }
    assert_eq!(index.len(), 2_020);
    assert!(index.ivf_lists() > 0);
    let q = &data[1_500];
    let got = index.search_ef(q, 10, Some(32)).unwrap();
    let want = index.search_exact_where(q, 10, &|_| true).unwrap();
    assert_eq!(ids(&got), ids(&want));
}

/// A filter is applied while the lists are scanned, and the probe widens
/// until `k` pass, so a selective filter still gets `k`.
#[test]
fn filtered_search_comes_back_full() {
    let data = clustered(4_000, 8, 40, 8);
    let m = VectorIndexManager::new();
    m.create_index_with_kind(
        None,
        "Doc",
        "v",
        8,
        DistanceMetric::L2,
        Quantization::None,
        HnswParams::default(),
        ivf(Some(64), Some(2)),
    )
    .unwrap();
    for (i, v) in data.iter().enumerate() {
        m.add_vector("Doc", "v", NodeId::new(i as u64), v).unwrap();
    }
    let index = m.get_index("Doc", "v").unwrap();
    for q in clustered(10, 8, 40, 9) {
        // One node in 97 passes.
        let keep = |id: NodeId| id.as_u64().is_multiple_of(97);
        let got = m.search_filtered("Doc", "v", &q, 10, keep).unwrap();
        assert_eq!(got.len(), 10);
        assert!(got.iter().all(|(id, _)| keep(*id)));
        // At nprobe = nlist it is the exact filtered scan.
        let got = m
            .search_filtered_ef("Doc", "v", &q, 10, Some(64), keep)
            .unwrap();
        let want = index
            .read()
            .unwrap()
            .search_exact_where(&q, 10, &keep)
            .unwrap();
        assert_eq!(ids(&got), ids(&want));
    }
}

fn manager_with(data: &[Vec<f32>], quantization: Quantization) -> VectorIndexManager {
    let m = VectorIndexManager::new();
    m.create_index_with_kind(
        Some("emb"),
        "Doc",
        "v",
        data[0].len(),
        DistanceMetric::Cosine,
        quantization,
        HnswParams::default(),
        ivf(Some(24), Some(3)),
    )
    .unwrap();
    for (i, v) in data.iter().enumerate() {
        m.add_vector("Doc", "v", NodeId::new(i as u64), v).unwrap();
    }
    // Some churn left in the lists.
    for i in 0..20u64 {
        m.add_vector(
            "Doc",
            "v",
            NodeId::new(i),
            &data[data.len() - 1 - i as usize],
        )
        .unwrap();
    }
    m.get_index("Doc", "v")
        .unwrap()
        .write()
        .unwrap()
        .remove(NodeId::new(25));
    m
}

/// A checkpoint carries the centroids, and recovery assigns each vector to
/// them again rather than re-running k-means: the same lists, so the same
/// answers at the same `nprobe`.
#[test]
fn a_checkpoint_reloads_to_the_same_answers() {
    let data = clustered(2_500, 12, 25, 10);
    for quantization in [Quantization::None, Quantization::Fp16] {
        let m = manager_with(&data, quantization);
        let dir = tempfile::tempdir().unwrap();
        m.checkpoint_all(dir.path()).unwrap();

        let back = VectorIndexManager::new();
        let report = back.recover_checkpoint(dir.path()).unwrap();
        assert_eq!(
            report.graphs_loaded, 1,
            "{quantization:?}: centroids reused"
        );
        assert_eq!(report.rebuilt, 0, "{quantization:?}");

        let (a, b) = (
            m.get_index("Doc", "v").unwrap(),
            back.get_index("Doc", "v").unwrap(),
        );
        let (a, b) = (a.read().unwrap(), b.read().unwrap());
        assert_eq!(b.kind(), ivf(Some(24), Some(3)));
        assert_eq!(b.len(), a.len());
        assert_eq!(b.ivf_lists(), 24);
        for q in clustered(30, 12, 25, 11) {
            for nprobe in [None, Some(1), Some(6)] {
                let x = a.search_ef(&q, 10, nprobe).unwrap();
                let y = b.search_ef(&q, 10, nprobe).unwrap();
                assert_eq!(ids(&x), ids(&y), "{quantization:?} nprobe={nprobe:?}");
                for ((_, dx), (_, dy)) in x.iter().zip(&y) {
                    assert!((dx - dy).abs() <= 1e-6, "{dx} vs {dy}");
                }
            }
        }
        assert_eq!(back.resolve_name("emb"), m.resolve_name("emb"));
    }
}

/// The kind is in the catalog that is persisted and put in snapshots, as a
/// record of its own, and comes back on restore.
#[test]
fn the_kind_survives_a_catalog_round_trip() {
    let docs = |s: &mut GraphStore| {
        for (i, p) in clustered(400, 4, 8, 12).iter().enumerate() {
            write(
                s,
                &format!(
                    "CREATE (:Doc {{id: {i}, v: [{}, {}, {}, {}]}})",
                    p[0], p[1], p[2], p[3]
                ),
            );
        }
    };
    let mut s = GraphStore::new();
    docs(&mut s);
    write(
        &mut s,
        "CREATE VECTOR INDEX FOR (n:Doc) ON (n.v) OPTIONS {dimensions: 4, kind: 'ivf', nlist: 8}",
    );
    let catalog = s.index_catalog();
    assert!(catalog.definitions.iter().any(|d| matches!(
        d,
        IndexDefinition::VectorKind {
            kind: IndexKind::Ivf(IvfParams {
                nlist: Some(8),
                nprobe: None
            }),
            ..
        }
    )));

    // Through bincode, as the RocksDB catalog stores it.
    let bytes = bincode::serialize(&catalog).unwrap();
    let back: samyama::index::catalog::IndexCatalog = bincode::deserialize(&bytes).unwrap();

    let mut restored = GraphStore::new();
    docs(&mut restored);
    let report = restored.restore_index_catalog(&back);
    assert_eq!(report.failed, 0);
    assert_eq!(
        column(&restored, "SHOW INDEXES", "type"),
        vec!["VECTOR[kind=ivf, nlist=8, nprobe=auto, lists=8]"]
    );
    let index = restored.vector_index.get_index("Doc", "v").unwrap();
    assert_eq!(index.read().unwrap().len(), 400);

    // An HNSW index writes no record, so its catalog is the one an older
    // version wrote and reads.
    let mut plain = GraphStore::new();
    write(
        &mut plain,
        "CREATE VECTOR INDEX FOR (n:Doc) ON (n.v) OPTIONS {dimensions: 4}",
    );
    assert!(!plain
        .index_catalog()
        .definitions
        .iter()
        .any(|d| matches!(d, IndexDefinition::VectorKind { .. })));
}
