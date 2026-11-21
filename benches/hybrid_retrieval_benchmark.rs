//! p50 / p95 of each stage of `db.hybrid.search` (#1279).
//!
//! A synthetic corpus: `--docs` documents, each a few words drawn from a small
//! vocabulary and a `--dim`-dimensional embedding, each citing `--cites`
//! others. Every query draws two words and a random vector and runs the five
//! stages through `hybrid_search` directly, so the figures are the engine's
//! and exclude parsing and planning (`total` here vs. the Cypher round trip
//! below shows what those add).
//!
//! The LLM stage of #1279's flow is not measured: the engine does not call a
//! model, so its latency is the caller's to measure.
//!
//!   cargo bench --bench hybrid_retrieval_benchmark
//!   cargo bench --bench hybrid_retrieval_benchmark -- --docs 100000 --queries 500 --hops 2

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::hybrid_ops::{
    hybrid_search, HybridRequest, StageTimings, VectorIndexRef,
};
use samyama::query::QueryEngine;
use samyama::vector::DistanceMetric;
use std::time::Instant;

#[path = "common/bench_setup.rs"]
mod bench_setup;

const VOCAB: &[&str] = &[
    "aspirin",
    "statin",
    "myopathy",
    "bleeding",
    "kinase",
    "receptor",
    "insulin",
    "tumour",
    "biomarker",
    "trial",
    "dosage",
    "toxicity",
    "pathway",
    "protein",
    "gene",
    "variant",
    "cohort",
    "placebo",
    "response",
    "inhibitor",
    "antibody",
    "therapy",
    "hepatic",
    "renal",
    "cardiac",
    "neural",
    "immune",
    "metabolic",
    "clinical",
    "outcome",
    "adverse",
    "marker",
];

/// splitmix64: deterministic, so two runs build the same corpus.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn unit(&mut self) -> f32 {
        (self.next() >> 40) as f32 / (1u64 << 24) as f32 * 2.0 - 1.0
    }
    fn vector(&mut self, dim: usize) -> Vec<f32> {
        (0..dim).map(|_| self.unit()).collect()
    }
    fn words(&mut self, n: usize) -> String {
        (0..n)
            .map(|_| VOCAB[self.below(VOCAB.len())])
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn build(docs: usize, dim: usize, cites: usize, rng: &mut Rng) -> GraphStore {
    let mut store = GraphStore::new();
    let mut ids = Vec::with_capacity(docs);
    for _ in 0..docs {
        let id = store.create_node("Doc");
        let words = 4 + rng.below(12);
        let _ = store.set_node_property(
            "default",
            id,
            "text",
            PropertyValue::String(rng.words(words)),
        );
        let _ =
            store.set_node_property("default", id, "emb", PropertyValue::Vector(rng.vector(dim)));
        ids.push(id);
    }
    for &src in &ids {
        for _ in 0..cites {
            let dst = ids[rng.below(ids.len())];
            if dst != src {
                let _ = store.create_edge(src, dst, "CITES");
            }
        }
    }
    store.create_fulltext_index("doc_text", "Doc", "text");
    store
        .create_vector_index("Doc", "emb", dim, DistanceMetric::Cosine)
        .expect("vector index");
    store.rebuild_vector_index();
    store
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn main() {
    bench_setup::init();
    let calibration = bench_setup::report_calibration();

    let args: Vec<String> = std::env::args().collect();
    let arg = |flag: &str| -> Option<usize> {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse().ok())
    };
    let docs = arg("--docs").unwrap_or(20_000);
    let dim = arg("--dim").unwrap_or(64);
    let cites = arg("--cites").unwrap_or(5);
    let queries = arg("--queries").unwrap_or(200);
    let hops = arg("--hops").unwrap_or(1);
    let k = arg("--k").unwrap_or(10);

    let mut rng = Rng(1279);
    let t = Instant::now();
    let store = build(docs, dim, cites, &mut rng);
    println!(
        "{docs} docs, {dim}-d cosine, {cites} cites each; built in {:.1} s",
        t.elapsed().as_secs_f64()
    );
    println!("{queries} queries, k {k}, hops {hops}, maxExpanded 100\n");

    let request = |rng: &mut Rng| HybridRequest {
        fulltext_index: Some("doc_text".into()),
        query_text: Some(rng.words(2)),
        vector_index: Some(VectorIndexRef::On {
            label: "Doc".into(),
            property: "emb".into(),
        }),
        query_vector: Some(rng.vector(dim)),
        k,
        candidates: 50.max(k),
        hops,
        ..HybridRequest::default()
    };

    // Warm up the index and allocator.
    for _ in 0..10 {
        let _ = hybrid_search(&store, &request(&mut rng)).expect("search");
    }

    let mut samples: Vec<StageTimings> = Vec::with_capacity(queries);
    let mut rows = 0usize;
    for _ in 0..queries {
        let out = hybrid_search(&store, &request(&mut rng)).expect("search");
        rows += out.hits.len();
        samples.push(out.timings);
    }

    println!(
        "{:<10} {:>10} {:>10} {:>10}",
        "stage", "p50 ms", "p95 ms", "max ms"
    );
    println!("{:-<10} {:->10} {:->10} {:->10}", "", "", "", "");
    type Stage = (&'static str, fn(&StageTimings) -> f64);
    let stages: [Stage; 6] = [
        ("bm25", |t| t.bm25_ms),
        ("vector", |t| t.vector_ms),
        ("fusion", |t| t.fusion_ms),
        ("expansion", |t| t.expansion_ms),
        ("rerank", |t| t.rerank_ms),
        ("total", |t| t.total_ms),
    ];
    for (name, get) in stages {
        let mut v: Vec<f64> = samples.iter().map(get).collect();
        v.sort_by(f64::total_cmp);
        println!(
            "{name:<10} {:>10.3} {:>10.3} {:>10.3}",
            percentile(&v, 50.0),
            percentile(&v, 95.0),
            v.last().copied().unwrap_or(0.0)
        );
    }
    println!(
        "\n{:.1} rows per query",
        rows as f64 / queries.max(1) as f64
    );

    // The same search through Cypher: parse, plan and project included.
    let engine = QueryEngine::new();
    let mut cypher: Vec<f64> = Vec::with_capacity(queries);
    for _ in 0..queries {
        let req = request(&mut rng);
        let vector = req
            .query_vector
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| format!("{x}"))
            .collect::<Vec<_>>()
            .join(", ");
        let q = format!(
            "CALL db.hybrid.search('doc_text', '{}', {{label: 'Doc', property: 'emb'}}, [{vector}], \
             {{k: {k}, hops: {hops}}}) YIELD node, score RETURN node, score",
            req.query_text.as_deref().unwrap()
        );
        let t = Instant::now();
        let _ = engine.execute(&q, &store).expect("cypher search");
        cypher.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    cypher.sort_by(f64::total_cmp);
    println!(
        "through Cypher: p50 {:.3} ms, p95 {:.3} ms",
        percentile(&cypher, 50.0),
        percentile(&cypher, 95.0)
    );

    bench_setup::report_drift(calibration);
}
