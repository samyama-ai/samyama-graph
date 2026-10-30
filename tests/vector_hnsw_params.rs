//! HNSW parameters a caller can set (#1971).
//!
//! `m = 16`, `ef_construction = 200` and a search width of `max(2k, 64)` were
//! fixed, so every caller sat at one point on the recall-latency curve and no
//! benchmark that sweeps `ef` could measure this engine. The graph was also
//! sized for 100,000 points whatever it held. These tests pin the DDL options,
//! the per-query `ef`, that the parameters survive a rebuild and a catalog
//! round trip, and that a wider search finds more of the true neighbours.

use samyama::graph::{GraphStore, NodeId, PropertyValue};
use samyama::index::catalog::IndexDefinition;
use samyama::query::executor::{MutQueryExecutor, QueryExecutor, Value};
use samyama::query::parser::parse_query;
use samyama::vector::index::Quantization;
use samyama::vector::{DistanceMetric, HnswParams, VectorIndex};

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

/// Deterministic points in `dim` dimensions.
fn points(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ seed;
    (0..n)
        .map(|_| {
            (0..dim)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    ((x >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
                })
                .collect()
        })
        .collect()
}

fn with_docs(n: usize) -> GraphStore {
    let mut s = GraphStore::new();
    for (i, p) in points(n, 4, 0).iter().enumerate() {
        write(
            &mut s,
            &format!(
                "CREATE (:Doc {{id: {i}, v: [{}, {}, {}, {}]}})",
                p[0], p[1], p[2], p[3]
            ),
        );
    }
    s
}

#[test]
fn the_ddl_sets_the_parameters_and_show_indexes_reports_them() {
    // Data first, then the DDL: the index is filled by the rebuild, which
    // must keep the parameters it was declared with.
    let mut s = with_docs(200);
    write(
        &mut s,
        "CREATE VECTOR INDEX tuned FOR (n:Doc) ON (n.v) \
         OPTIONS {dimensions: 4, similarity: 'l2', m: 32, ef_construction: 400, ef: 120}",
    );
    write(
        &mut s,
        "CREATE VECTOR INDEX FOR (n:Other) ON (n.v) OPTIONS {dimensions: 4}",
    );
    let mut shown = column(&s, "SHOW INDEXES", "type");
    shown.sort();
    assert_eq!(
        shown,
        vec![
            "VECTOR[m=16, ef_construction=200, ef=auto]",
            "VECTOR[m=32, ef_construction=400, ef=120]",
        ]
    );
    let index = s.vector_index.get_index("Doc", "v").unwrap();
    assert_eq!(index.read().unwrap().len(), 200, "the rebuild filled it");
    assert_eq!(
        index.read().unwrap().params(),
        HnswParams {
            m: 32,
            ef_construction: 400,
            ef: Some(120)
        }
    );
}

#[test]
fn out_of_range_or_mistyped_parameters_are_refused() {
    let mut s = GraphStore::new();
    for (options, says) in [
        ("m: 0", "`m` must be a positive integer"),
        ("m: 1", "`m` must be between 2 and 128"),
        ("m: 256", "`m` must be between 2 and 128"),
        (
            "ef_construction: 0",
            "`ef_construction` must be a positive integer",
        ),
        (
            "ef_construction: 100001",
            "`ef_construction` must be between",
        ),
        ("ef: 'wide'", "`ef` must be a positive integer"),
        ("ef: 1000000", "`ef` must be between"),
        ("ef_search: 50", "unknown option `ef_search`"),
    ] {
        let err = refused(
            &mut s,
            &format!("CREATE VECTOR INDEX FOR (n:D) ON (n.v) OPTIONS {{dimensions: 4, {options}}}"),
        );
        assert!(err.contains(says), "{options}: {err}");
    }
    assert!(
        s.vector_index.list_indices().is_empty(),
        "nothing was created"
    );
}

#[test]
fn a_query_takes_its_own_ef_in_either_call_form() {
    let mut s = with_docs(300);
    write(
        &mut s,
        "CREATE VECTOR INDEX docs FOR (n:Doc) ON (n.v) OPTIONS {dimensions: 4, similarity: 'l2'}",
    );
    let answers: Vec<Vec<String>> = [
        "CALL db.index.vector.queryNodes('Doc', 'v', [0.1, 0.2, 0.3, 0.4], 5, {ef: 300}) \
         YIELD node RETURN node.id AS id",
        "CALL db.index.vector.queryNodes('docs', 5, [0.1, 0.2, 0.3, 0.4], {ef: 300}) \
         YIELD node RETURN node.id AS id",
    ]
    .iter()
    .map(|q| column(&s, q, "id"))
    .collect();
    assert_eq!(answers[0].len(), 5);
    assert_eq!(answers[0], answers[1], "the two call forms search alike");
    let plan = column(
        &s,
        "EXPLAIN CALL db.index.vector.queryNodes('Doc', 'v', [0.1, 0.2, 0.3, 0.4], 5, {ef: 40}) \
         YIELD node RETURN node.id AS id",
        "plan",
    )
    .join("\n");
    assert!(plan.contains("ef=40"), "{plan}");

    for (options, says) in [
        ("{ef: 0}", "`ef` must be a positive integer"),
        ("{ef: 'x'}", "`ef` must be a positive integer"),
        ("{ef: 1000000}", "`ef` must be between"),
        ("{k: 3}", "unknown option `k`"),
        ("200", "options must be a map"),
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

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

/// The knob does what it is for: a wider search finds more of the true
/// neighbours, and a wide one finds nearly all of them.
#[test]
fn a_wider_search_has_higher_recall() {
    let data = points(4_000, 16, 1);
    let params = HnswParams {
        m: 8,
        ef_construction: 40,
        ef: None,
    };
    let mut index = VectorIndex::with_params(16, DistanceMetric::L2, Quantization::None, params);
    for (i, v) in data.iter().enumerate() {
        index.add(NodeId::new(i as u64), v).unwrap();
    }
    let queries = points(50, 16, 2);
    let recall = |ef: usize| {
        let mut hits = 0;
        for q in &queries {
            let mut want: Vec<(usize, f32)> = data
                .iter()
                .enumerate()
                .map(|(i, v)| (i, l2(q, v)))
                .collect();
            want.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let want: Vec<u64> = want.iter().take(10).map(|p| p.0 as u64).collect();
            let got = index.search_ef(q, 10, Some(ef)).unwrap();
            assert_eq!(got.len(), 10);
            hits += got
                .iter()
                .filter(|(id, _)| want.contains(&id.as_u64()))
                .count();
        }
        hits as f64 / (10 * queries.len()) as f64
    };
    let narrow = recall(10);
    let wide = recall(400);
    assert!(wide >= 0.95, "recall@10 at ef=400: {wide:.3}");
    assert!(
        wide > narrow,
        "ef=10 gave {narrow:.3}, ef=400 gave {wide:.3}"
    );
}

#[test]
fn changing_m_rebuilds_and_changing_ef_does_not_lose_entries() {
    let data = points(500, 8, 3);
    let mut index = VectorIndex::new(8, DistanceMetric::Cosine);
    for (i, v) in data.iter().enumerate() {
        index.add(NodeId::new(i as u64), v).unwrap();
    }
    index.set_params(HnswParams {
        m: 24,
        ef_construction: 100,
        ef: None,
    });
    assert_eq!(index.len(), 500);
    index.set_params(HnswParams {
        m: 24,
        ef_construction: 100,
        ef: Some(50),
    });
    assert_eq!(index.len(), 500);
    assert_eq!(index.params().ef, Some(50));
    let got = index.search(&data[7], 1).unwrap();
    assert_eq!(got[0].0, NodeId::new(7));
}

/// The parameters are in the catalog that is persisted and put in snapshots,
/// as a record of their own, and come back on restore.
#[test]
fn the_parameters_survive_a_catalog_round_trip() {
    let mut s = with_docs(150);
    write(
        &mut s,
        "CREATE VECTOR INDEX tuned FOR (n:Doc) ON (n.v) \
         OPTIONS {dimensions: 4, m: 12, ef_construction: 64, ef: 90}",
    );
    let catalog = s.index_catalog();
    assert!(catalog.definitions.iter().any(|d| matches!(
        d,
        IndexDefinition::VectorParams {
            m: 12,
            ef_construction: 64,
            ef: Some(90),
            ..
        }
    )));

    // Through bincode, as the RocksDB catalog stores it.
    let bytes = bincode::serialize(&catalog).unwrap();
    let back: samyama::index::catalog::IndexCatalog = bincode::deserialize(&bytes).unwrap();

    let mut restored = with_docs(150);
    let report = restored.restore_index_catalog(&back);
    assert_eq!(report.failed, 0);
    assert_eq!(
        column(&restored, "SHOW INDEXES", "type"),
        vec!["VECTOR[m=12, ef_construction=64, ef=90]"]
    );
    let index = restored.vector_index.get_index("Doc", "v").unwrap();
    assert_eq!(index.read().unwrap().len(), 150);

    // An index on the defaults writes no record, so its catalog is the one
    // an older version wrote and reads.
    let mut plain = GraphStore::new();
    write(
        &mut plain,
        "CREATE VECTOR INDEX FOR (n:Doc) ON (n.v) OPTIONS {dimensions: 4}",
    );
    assert!(!plain
        .index_catalog()
        .definitions
        .iter()
        .any(|d| matches!(d, IndexDefinition::VectorParams { .. })));
}
