//! fp16 vector quantization (NDS-09, #1385).
//!
//! NDS-09 asks for "multiple named vectors per node, fp16/int8 quantization,
//! dimension declared in schema". Two of the three worked; quantization did not
//! exist, in the DDL or under it.
//!
//! # Why the memory has to actually drop
//!
//! `OPTIONS {quantization: "fp16"}` accepted and ignored would be worse than
//! the refusal it replaced: the caller would believe their vectors were half
//! the size. So the index holds `f16` in the HNSW graph **and** in the copy
//! kept for persistence. `vector_bytes()` reports both, and the case below
//! asserts the ratio rather than trusting the type.
//!
//! # Why recall is compared, not asserted
//!
//! How much recall fp16 costs is a property of the corpus, not a constant. The
//! case below builds the same vectors twice and compares, so it measures the
//! quantization rather than the day. Measured while writing it, three runs
//! each: at 64 dimensions f32 gave 0.968/0.976/0.969 and fp16 0.983/0.983/0.961;
//! at 384, f32 0.913/0.927/0.924 and fp16 0.920/0.915/0.921. Indistinguishable
//! either way -- the index's own layer randomness is larger than the effect.

use samyama::graph::types::NodeId;
use samyama::graph::GraphStore;
use samyama::query::executor::MutQueryExecutor;
use samyama::query::parser::parse_query;
use samyama::vector::index::{DistanceMetric, Quantization, VectorIndex};
use std::collections::HashSet;

const DIM: usize = 64;
const N: usize = 2000;
const K: usize = 10;
const QUERIES: usize = 100;

fn lcg(seed: &mut u64) -> f32 {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    ((*seed >> 33) as f32 / (1u64 << 31) as f32) - 1.0
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    dot / (na.sqrt() * nb.sqrt() + 1e-12)
}

fn corpus(n: usize, dim: usize) -> Vec<Vec<f32>> {
    let mut seed = 42u64;
    (0..n).map(|_| (0..dim).map(|_| lcg(&mut seed)).collect()).collect()
}

fn build(vectors: &[Vec<f32>], q: Quantization) -> VectorIndex {
    let mut idx = VectorIndex::with_quantization(DIM, DistanceMetric::Cosine, q);
    for (i, v) in vectors.iter().enumerate() {
        idx.add(NodeId(i as u64), v).expect("add");
    }
    idx
}

fn recall(idx: &VectorIndex, vectors: &[Vec<f32>]) -> f64 {
    let mut qseed = 7u64;
    let (mut hit, mut total) = (0usize, 0usize);
    for _ in 0..QUERIES {
        let query: Vec<f32> = (0..DIM).map(|_| lcg(&mut qseed)).collect();
        let mut exact: Vec<(usize, f32)> =
            vectors.iter().enumerate().map(|(i, v)| (i, cosine(&query, v))).collect();
        exact.sort_by(|a, b| b.1.partial_cmp(&a.1).expect("no NaNs"));
        let truth: HashSet<usize> = exact.iter().take(K).map(|(i, _)| *i).collect();
        let got = idx.search(&query, K).expect("search");
        hit += got.iter().filter(|(id, _)| truth.contains(&(id.0 as usize))).count();
        total += K;
    }
    hit as f64 / total as f64
}

#[test]
fn fp16_halves_the_bytes_the_index_holds() {
    let vectors = corpus(N, DIM);
    let f32_idx = build(&vectors, Quantization::None);
    let f16_idx = build(&vectors, Quantization::Fp16);

    assert_eq!(f32_idx.len(), f16_idx.len(), "both hold every vector");
    assert_eq!(
        f32_idx.vector_bytes(),
        2 * f16_idx.vector_bytes(),
        "fp16 must be exactly half: {} against {}",
        f32_idx.vector_bytes(),
        f16_idx.vector_bytes()
    );
}

#[test]
fn fp16_recall_tracks_full_precision_on_the_same_corpus() {
    // Both indexes see the same vectors and the same queries, so the
    // comparison is of the quantization and not of the corpus. The tolerance
    // is wide because HNSW's layer assignment is random and its own run-to-run
    // spread is larger than the quantization effect -- a tight bound here would
    // be a flaky test measuring the weather.
    let vectors = corpus(N, DIM);
    let full = recall(&build(&vectors, Quantization::None), &vectors);
    let half = recall(&build(&vectors, Quantization::Fp16), &vectors);

    assert!(full > 0.85, "the unquantized reference itself is broken: {full:.4}");
    assert!(
        half >= full - 0.10,
        "fp16 recall {half:.4} is more than 0.10 below f32 {full:.4}; that is a real \
         loss rather than the index's usual variation, and NDS-09 asks for the trade to \
         be known rather than discovered"
    );
}

#[test]
fn a_quantized_index_still_finds_a_vector_it_holds() {
    // The control. Recall is a ratio and a ratio can look healthy while the
    // index answers a subtly different question; an exact-match query has one
    // right answer. Held to a small corpus so the exact-search path is used --
    // that path quantizes the query too, and this is what catches it if it
    // stops doing so.
    let vectors = corpus(40, DIM);
    let idx = build(&vectors, Quantization::Fp16);
    for (i, v) in vectors.iter().enumerate() {
        let got = idx.search(v, 1).expect("search");
        assert_eq!(got[0].0, NodeId(i as u64), "vector {i} did not find itself");
    }
}

#[test]
fn the_ddl_option_reaches_the_index_and_survives_the_backfill() {
    // The bug this test exists for: `CREATE VECTOR INDEX` registers the index
    // and the operator then calls `rebuild_vector_index` to backfill nodes that
    // already carry an embedding. That rebuild carried dimensions and metric
    // across and **not** quantization, so the option was honoured and then
    // undone one call later -- the caller asked for half the memory, got an f32
    // index, and was told the statement succeeded.
    let mut store = GraphStore::new();
    for ddl in [
        "CREATE VECTOR INDEX plain FOR (n:V) ON (n.v) OPTIONS {dimensions: 4}",
        "CREATE VECTOR INDEX half FOR (n:W) ON (n.v) OPTIONS {dimensions: 4, quantization: 'fp16'}",
    ] {
        let q = parse_query(ddl).unwrap_or_else(|e| panic!("{ddl}\n  {e:?}"));
        MutQueryExecutor::new(&mut store, "default".to_string())
            .execute(&q)
            .unwrap_or_else(|e| panic!("{ddl}\n  {e:?}"));
    }

    let q_of = |label: &str| {
        store
            .vector_index
            .get_index(label, "v")
            .map(|i| i.read().unwrap().quantization())
    };
    assert_eq!(q_of("V"), Some(Quantization::None), "the default is unchanged");
    assert_eq!(q_of("W"), Some(Quantization::Fp16), "the option reached the index");
}

#[test]
fn an_unrecognised_quantization_is_refused_rather_than_defaulted() {
    // Defaulting to full precision would be the worst outcome: a caller who
    // asked for `fp8` would believe their index was a quarter of the size.
    for bad in [
        "CREATE VECTOR INDEX x FOR (n:X) ON (n.v) OPTIONS {dimensions: 4, quantization: 'fp8'}",
        "CREATE VECTOR INDEX x FOR (n:X) ON (n.v) OPTIONS {dimensions: 4, quantization: 'int8'}",
        "CREATE VECTOR INDEX x FOR (n:X) ON (n.v) OPTIONS {dimensions: 4, quantization: 7}",
    ] {
        assert!(parse_query(bad).is_err(), "{bad} should be refused");
    }
    // The spellings that are accepted, so the refusal above is about the value
    // and not about the option existing.
    for good in ["none", "fp16", "f16", "FP16"] {
        let ddl = format!(
            "CREATE VECTOR INDEX x FOR (n:X) ON (n.v) OPTIONS {{dimensions: 4, quantization: '{good}'}}"
        );
        assert!(parse_query(&ddl).is_ok(), "{ddl} should parse");
    }
}

#[test]
fn a_quantized_index_round_trips_through_the_unchanged_file_format() {
    // Quantization is a runtime memory choice, not a file format: a snapshot
    // written by a quantized index has to load into an unquantized one and the
    // reverse, with no migration.
    let vectors = corpus(300, DIM);
    let idx = build(&vectors, Quantization::Fp16);
    let dir = std::env::temp_dir().join(format!("samyama-q-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let path = dir.join("index.bin");
    idx.dump(&path).expect("dump");

    let back_full = VectorIndex::load(&path, DIM, DistanceMetric::Cosine).expect("load as f32");
    assert_eq!(back_full.len(), vectors.len());
    assert_eq!(back_full.quantization(), Quantization::None);

    let back_half =
        VectorIndex::load_with_quantization(&path, DIM, DistanceMetric::Cosine, Quantization::Fp16)
            .expect("load as fp16");
    assert_eq!(back_half.len(), vectors.len());
    assert_eq!(back_half.quantization(), Quantization::Fp16);
    assert_eq!(back_half.vector_bytes(), back_full.vector_bytes() / 2);

    let _ = std::fs::remove_file(&path);
}
