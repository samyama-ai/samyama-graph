//! Retrieval quality on BEIR / MS MARCO: BM25, dense and client-side RRF
//! (#1988).
//!
//! Recall against exact neighbours (`ann_benchmark`) says how well the index
//! approximates brute force. It does not say whether the answers are any good.
//! This bench scores the engine's answers against human relevance judgments --
//! nDCG@10, recall@100 and MRR@10, as BEIR reports them -- for each retrieval
//! configuration, and times every query.
//!
//! ```text
//! # A BEIR directory (corpus.jsonl, queries.jsonl, qrels/<split>.tsv and,
//! # optionally, dense/ from scripts/prepare_beir_datasets.py):
//! cargo bench --bench retrieval_quality_benchmark -- --dataset data/beir/scifact
//! cargo bench --bench retrieval_quality_benchmark -- --dataset data/beir/msmarco --split dev
//!
//! # No dataset: a tiny built-in topical corpus that exercises every row.
//! cargo bench --bench retrieval_quality_benchmark -- --synthetic
//! ```
//!
//! Configurations (one CSV row each, per `ef` where it applies):
//!
//! | config | what runs |
//! |---|---|
//! | `bm25` | the full-text index (`db.index.fulltext.queryNodes`), BM25 k1=1.2 b=0.75 |
//! | `dense-hnsw` | the HNSW index (`db.index.vector.queryNodes`) at each `--ef` |
//! | `dense-exact` | brute force over the same vectors (`--exact`); the ceiling HNSW approximates. Not timed |
//! | `rrf-client:bm25+dense-hnsw` | RRF of the two lists above, fused **in this harness** |
//! | `rrf-client:bm25+dense-exact` | the same over the exact list (`--exact`) |
//!
//! The RRF rows are client-side fusion of two result lists the engine
//! returned. They are not #1279's in-engine `db.hybrid.search`, which is not on
//! `dev` yet; the in-engine row is added when it is. The dense and RRF rows
//! need `dense/` in the dataset directory; without it only `bm25` runs.
//!
//! Options:
//!
//! | flag | default | |
//! |---|---|---|
//! | `--split` | `test` | reads `qrels/<split>.tsv` (`dev` for MS MARCO) |
//! | `--k` | 10 | cut-off for nDCG and MRR |
//! | `--depth` | 100 | results fetched per search; the recall cut-off and the RRF input depth |
//! | `--ef` | `100,200,400` | HNSW search widths |
//! | `--m`, `--ef-construction` | 16, 200 | HNSW build parameters |
//! | `--rrf-k` | 60 | the RRF constant |
//! | `--exact` | off | add the brute-force dense rows |
//! | `--no-dense` | off | ignore `dense/` and run BM25 only |
//! | `--queries N` | all | the first N judged queries, by id |
//! | `--path` | `direct` | `direct` (the index call) or `cypher` (the procedure through `QueryEngine`) |
//! | `--name` | directory name | dataset name in the CSV |
//! | `--csv FILE` | none | append rows to FILE, writing a header if new |
//!
//! Latency is one client thread, in-process: no network. The competitor
//! script (`benches/retrieval_quality/competitors.py`) measures over each
//! system's HTTP API, so compare quality columns across systems freely and
//! latency columns only with that difference in mind.
//!
//! Run with `--release` on a quiet host and compare the calibration line
//! before comparing timings (#529).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[path = "common/bench_setup.rs"]
mod bench_setup;

#[path = "ann_common/mod.rs"]
mod ann_common;

#[path = "retrieval_common/mod.rs"]
mod retrieval_common;

use ann_common::{Metric, Vectors};
use retrieval_common::Judgments;
use samyama::graph::{GraphStore, Label, NodeId, PropertyValue};
use samyama::query::{BoundParams, QueryEngine, Value};

const LABEL: &str = "Doc";
const ID: &str = "doc_id";
const TEXT: &str = "text";
const EMBEDDING: &str = "embedding";
const FULLTEXT_INDEX: &str = "beir_text";

#[derive(Clone, Copy, PartialEq, Eq)]
enum SearchPath {
    Direct,
    Cypher,
}

impl SearchPath {
    fn name(self) -> &'static str {
        match self {
            SearchPath::Direct => "direct",
            SearchPath::Cypher => "cypher",
        }
    }
}

struct Args {
    dataset: Option<PathBuf>,
    synthetic: bool,
    split: String,
    name: Option<String>,
    k: usize,
    depth: usize,
    ef: Vec<usize>,
    m: usize,
    ef_construction: usize,
    rrf_k: f64,
    exact: bool,
    no_dense: bool,
    queries: Option<usize>,
    path: SearchPath,
    csv: Option<PathBuf>,
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

enum Invocation {
    Run(Args),
    /// No dataset named at all. `scripts/verify-sweep.sh` runs every bench
    /// with no arguments, so this is a skip, not a usage error -- the same
    /// answer `ann_benchmark` gives (see its `NoDataset`).
    NoDataset,
}

fn parse_args() -> Result<Invocation, String> {
    let mut a = Args {
        dataset: None,
        synthetic: false,
        split: "test".into(),
        name: None,
        k: 10,
        depth: 100,
        ef: vec![100, 200, 400],
        m: 16,
        ef_construction: 200,
        rrf_k: 60.0,
        exact: false,
        no_dense: false,
        queries: None,
        path: SearchPath::Direct,
        csv: None,
    };
    // `cargo bench` passes `--bench` to every harness=false target.
    let mut it = std::env::args().skip(1).filter(|a| a != "--bench");
    while let Some(flag) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--dataset" => a.dataset = Some(value()?.into()),
            "--synthetic" => a.synthetic = true,
            "--split" => a.split = value()?,
            "--name" => a.name = Some(value()?),
            "--k" => a.k = one(&value()?, "--k")?,
            "--depth" => a.depth = one(&value()?, "--depth")?,
            "--ef" => a.ef = list(&value()?, "--ef")?,
            "--m" => a.m = one(&value()?, "--m")?,
            "--ef-construction" => a.ef_construction = one(&value()?, "--ef-construction")?,
            "--rrf-k" => {
                let v = value()?;
                a.rrf_k = v
                    .parse::<f64>()
                    .ok()
                    .filter(|x| *x >= 0.0)
                    .ok_or_else(|| format!("--rrf-k: `{v}` is not a non-negative number"))?;
            }
            "--exact" => a.exact = true,
            "--no-dense" => a.no_dense = true,
            "--queries" => a.queries = Some(one(&value()?, "--queries")?),
            "--path" => {
                a.path = match value()?.as_str() {
                    "direct" => SearchPath::Direct,
                    "cypher" => SearchPath::Cypher,
                    other => return Err(format!("unknown path `{other}`; use direct or cypher")),
                }
            }
            "--csv" => a.csv = Some(value()?.into()),
            other => return Err(format!("unknown flag `{other}`")),
        }
    }
    match (a.dataset.is_some(), a.synthetic) {
        (false, false) => Ok(Invocation::NoDataset),
        (true, true) => Err("--dataset and --synthetic are alternatives".into()),
        _ => Ok(Invocation::Run(a)),
    }
}

// ------------------------------------------------------------------ dataset

struct Dense {
    model: String,
    metric: Metric,
    /// One row per corpus document, in `Dataset::doc_ids` order.
    docs: Vectors,
    /// One row per scored query, in `Dataset::queries` order.
    queries: Vectors,
}

struct Dataset {
    name: String,
    doc_ids: Vec<String>,
    doc_texts: Vec<String>,
    /// The scored queries -- judged, with at least one relevant document --
    /// sorted by id.
    queries: Vec<(String, String)>,
    qrels: HashMap<String, Judgments>,
    dense: Option<Dense>,
}

fn read(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn read_jsonl(path: &Path) -> Result<Vec<(String, String)>, String> {
    let text = read(path)?;
    let mut out = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if let Some(row) = retrieval_common::parse_beir_line(line)
            .map_err(|e| format!("{} line {}: {e}", path.display(), n + 1))?
        {
            out.push(row);
        }
    }
    Ok(out)
}

fn read_ids(path: &Path) -> Result<Vec<String>, String> {
    Ok(read(path)?
        .lines()
        .map(|l| l.trim_end_matches('\r').to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

/// Reorder `rows` (whose ids are `ids`) to follow `wanted`. Every wanted id
/// must have a row: a missing vector would silently become a document the
/// dense search can never return.
fn align(
    rows: &Vectors,
    ids: &[String],
    wanted: impl Iterator<Item = String>,
    what: &str,
) -> Result<Vectors, String> {
    if rows.len() != ids.len() {
        return Err(format!(
            "{what}: {} vectors but {} ids",
            rows.len(),
            ids.len()
        ));
    }
    let at: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, s)| (s.as_str(), i))
        .collect();
    let mut data = Vec::new();
    for id in wanted {
        let i = at
            .get(id.as_str())
            .ok_or_else(|| format!("{what}: no vector for `{id}`"))?;
        data.extend_from_slice(rows.row(*i));
    }
    Ok(Vectors {
        dim: rows.dim,
        data,
    })
}

/// The judged queries that have text, sorted by id, cut to `limit`.
fn pick_queries(
    qrels: &HashMap<String, Judgments>,
    text: &HashMap<String, String>,
    limit: Option<usize>,
) -> Vec<(String, String)> {
    let scored = retrieval_common::scored_queries(qrels);
    let missing = scored.iter().filter(|q| !text.contains_key(*q)).count();
    if missing > 0 {
        eprintln!("warning: {missing} judged queries are not in queries.jsonl and are skipped");
    }
    scored
        .into_iter()
        .filter_map(|q| text.get(&q).map(|t| (q.clone(), t.clone())))
        .take(limit.unwrap_or(usize::MAX))
        .collect()
}

fn load_beir(dir: &Path, a: &Args) -> Result<Dataset, String> {
    let corpus = read_jsonl(&dir.join("corpus.jsonl"))?;
    let query_text: HashMap<String, String> = read_jsonl(&dir.join("queries.jsonl"))?
        .into_iter()
        .collect();
    let qrels =
        retrieval_common::parse_qrels(&read(&dir.join("qrels").join(format!("{}.tsv", a.split)))?)?;
    let (doc_ids, doc_texts): (Vec<String>, Vec<String>) = corpus.into_iter().unzip();
    let mut seen = std::collections::HashSet::new();
    if let Some(dup) = doc_ids.iter().find(|id| !seen.insert(id.as_str())) {
        return Err(format!("corpus.jsonl: document `{dup}` appears twice"));
    }
    let queries = pick_queries(&qrels, &query_text, a.queries);

    let dense_dir = dir.join("dense");
    let dense = if a.no_dense || !dense_dir.exists() {
        None
    } else {
        let metric = Metric::parse(read(&dense_dir.join("metric.txt"))?.trim())?;
        let model = read(&dense_dir.join("model.txt"))?.trim().to_string();
        let docs = ann_common::read_fvecs(&dense_dir.join("corpus.fvecs"), None)?;
        let docs = align(
            &docs,
            &read_ids(&dense_dir.join("corpus_ids.txt"))?,
            doc_ids.iter().cloned(),
            "dense/corpus",
        )?;
        let qv = ann_common::read_fvecs(&dense_dir.join("queries.fvecs"), None)?;
        let qv = align(
            &qv,
            &read_ids(&dense_dir.join("query_ids.txt"))?,
            queries.iter().map(|(q, _)| q.clone()),
            "dense/queries",
        )?;
        if docs.dim != qv.dim {
            return Err(format!(
                "dense: documents have {} dimensions and queries {}",
                docs.dim, qv.dim
            ));
        }
        Some(Dense {
            model,
            metric,
            docs,
            queries: qv,
        })
    };
    let name = a.name.clone().unwrap_or_else(|| {
        dir.file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    Ok(Dataset {
        name,
        doc_ids,
        doc_texts,
        queries,
        qrels,
        dense,
    })
}

fn load_synthetic(a: &Args) -> Dataset {
    let s = retrieval_common::synthetic(20, 50, 32, 0x5EED);
    let text: HashMap<String, String> = s.queries.iter().cloned().collect();
    let queries = pick_queries(&s.qrels, &text, a.queries);
    let (doc_ids, doc_texts): (Vec<String>, Vec<String>) = s.docs.into_iter().unzip();
    let flat = |rows: Vec<Vec<f32>>| Vectors {
        dim: s.dim,
        data: rows.into_iter().flatten().collect(),
    };
    let dense = if a.no_dense {
        None
    } else {
        let qids: Vec<String> = s.queries.iter().map(|(q, _)| q.clone()).collect();
        let qv = flat(s.query_vectors);
        let qv = align(
            &qv,
            &qids,
            queries.iter().map(|(q, _)| q.clone()),
            "synthetic",
        )
        .expect("every synthetic query has a vector");
        Some(Dense {
            model: "synthetic".into(),
            metric: Metric::Cosine,
            docs: flat(s.doc_vectors),
            queries: qv,
        })
    };
    Dataset {
        name: a.name.clone().unwrap_or_else(|| "synthetic".into()),
        doc_ids,
        doc_texts,
        queries,
        qrels: s.qrels,
        dense,
    }
}

// -------------------------------------------------------------------- store

struct Loaded {
    store: GraphStore,
    position: HashMap<NodeId, u32>,
    load: Duration,
    fulltext_build: Duration,
    vector_build: Duration,
}

fn load_store(ds: &Dataset, a: &Args) -> Result<Loaded, String> {
    let mut store = GraphStore::new();
    let mut position = HashMap::with_capacity(ds.doc_ids.len());
    let started = Instant::now();
    for (i, (id, text)) in ds.doc_ids.iter().zip(&ds.doc_texts).enumerate() {
        let mut props = HashMap::new();
        props.insert(ID.to_string(), PropertyValue::String(id.clone()));
        props.insert(TEXT.to_string(), PropertyValue::String(text.clone()));
        if let Some(d) = &ds.dense {
            props.insert(
                EMBEDDING.to_string(),
                PropertyValue::Vector(d.docs.row(i).to_vec()),
            );
        }
        let node = store.create_node_with_properties("default", vec![Label::new(LABEL)], props);
        position.insert(node, i as u32);
    }
    let load = started.elapsed();

    let started = Instant::now();
    let indexed = store.create_fulltext_index(FULLTEXT_INDEX, LABEL, TEXT);
    let fulltext_build = started.elapsed();
    if indexed != ds.doc_ids.len() {
        eprintln!(
            "warning: the full-text index holds {indexed} of {} documents",
            ds.doc_ids.len()
        );
    }

    let started = Instant::now();
    if let Some(d) = &ds.dense {
        store
            .create_vector_index_with_params(
                None,
                LABEL,
                EMBEDDING,
                d.docs.dim,
                ann_common::samyama_metric(d.metric),
                samyama::vector::index::Quantization::None,
                samyama::vector::HnswParams {
                    m: a.m,
                    ef_construction: a.ef_construction,
                    ef: None,
                },
            )
            .map_err(|e| e.to_string())?;
        store.rebuild_vector_index();
    }
    let vector_build = started.elapsed();
    Ok(Loaded {
        store,
        position,
        load,
        fulltext_build,
        vector_build,
    })
}

// ----------------------------------------------------------------- searches

fn ids_of(
    l: &Loaded,
    ds: &Dataset,
    nodes: impl Iterator<Item = NodeId>,
) -> Result<Vec<String>, String> {
    nodes
        .map(|n| {
            l.position
                .get(&n)
                .map(|p| ds.doc_ids[*p as usize].clone())
                .ok_or_else(|| format!("unknown node {n:?}"))
        })
        .collect()
}

fn ids_from_batch(batch: &samyama::query::RecordBatch) -> Result<Vec<String>, String> {
    batch
        .records
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::Property(PropertyValue::String(s))) => Ok(s.clone()),
            other => Err(format!("row without a string `id`: {other:?}")),
        })
        .collect()
}

fn bm25(
    l: &Loaded,
    ds: &Dataset,
    engine: &QueryEngine,
    path: SearchPath,
    query: &str,
    depth: usize,
) -> Result<Vec<String>, String> {
    let text = retrieval_common::bm25_query_text(query);
    if text.is_empty() {
        return Ok(Vec::new());
    }
    match path {
        SearchPath::Direct => {
            let hits = l
                .store
                .fulltext
                .search(FULLTEXT_INDEX, &text, depth)
                .ok_or("the full-text index is missing")?;
            ids_of(l, ds, hits.into_iter().map(|h| h.node))
        }
        SearchPath::Cypher => {
            // `text` is alphanumerics and spaces only, so it needs no escaping.
            let q = format!(
                "CALL db.index.fulltext.queryNodes('{FULLTEXT_INDEX}', '{text}', {depth}) \
                 YIELD node RETURN node.{ID} AS id"
            );
            let batch = engine
                .execute_with_params(&q, &l.store, &BoundParams::new())
                .map_err(|e| e.to_string())?;
            ids_from_batch(&batch)
        }
    }
}

fn dense_hnsw(
    l: &Loaded,
    ds: &Dataset,
    engine: &QueryEngine,
    path: SearchPath,
    q: &[f32],
    depth: usize,
    ef: usize,
) -> Result<Vec<String>, String> {
    match path {
        SearchPath::Direct => {
            let hits = l
                .store
                .vector_search_where(LABEL, EMBEDDING, q, depth, Some(ef), |_| true)
                .map_err(|e| e.to_string())?;
            ids_of(l, ds, hits.into_iter().map(|(n, _)| n))
        }
        SearchPath::Cypher => {
            let text = format!(
                "CALL db.index.vector.queryNodes('{LABEL}', '{EMBEDDING}', $q, {depth}, {{ef: {ef}}}) \
                 YIELD node RETURN node.{ID} AS id"
            );
            let mut params = BoundParams::new();
            params.insert("q".to_string(), PropertyValue::Vector(q.to_vec()));
            let batch = engine
                .execute_with_params(&text, &l.store, &params)
                .map_err(|e| e.to_string())?;
            ids_from_batch(&batch)
        }
    }
}

// ------------------------------------------------------------------ scoring

/// One configuration's answers: per query, its latency (`None` when the row
/// is not timed) and its ranked document ids (`None` on error).
struct Run {
    latency: Vec<Option<Duration>>,
    answers: Vec<Option<Vec<String>>>,
}

fn timed<F: Fn(usize) -> Result<Vec<String>, String>>(n: usize, search: F) -> Run {
    // One untimed pass over a few queries so the first does not pay for cold
    // caches and, on the Cypher path, for planning.
    for i in 0..n.min(20) {
        let _ = search(i);
    }
    let mut latency = Vec::with_capacity(n);
    let mut answers = Vec::with_capacity(n);
    for i in 0..n {
        let at = Instant::now();
        let got = search(i);
        latency.push(Some(at.elapsed()));
        answers.push(got.ok());
    }
    Run { latency, answers }
}

/// Fuse two runs per query. The latency is the two searches one after the
/// other plus the fusion -- a client that sends them in parallel would see
/// the larger of the two instead.
fn fuse(a: &Run, b: &Run, rrf_k: f64, depth: usize) -> Run {
    let mut latency = Vec::with_capacity(a.answers.len());
    let mut answers = Vec::with_capacity(a.answers.len());
    for i in 0..a.answers.len() {
        let at = Instant::now();
        let got = match (&a.answers[i], &b.answers[i]) {
            (Some(x), Some(y)) => Some(retrieval_common::rrf(
                &[x.as_slice(), y.as_slice()],
                rrf_k,
                depth,
            )),
            _ => None,
        };
        let took = at.elapsed();
        latency.push(match (a.latency[i], b.latency[i]) {
            (Some(x), Some(y)) => Some(x + y + took),
            _ => None,
        });
        answers.push(got);
    }
    Run { latency, answers }
}

struct Row {
    config: String,
    ef: Option<usize>,
    ndcg: f64,
    recall: f64,
    mrr: f64,
    p50: Option<Duration>,
    p99: Option<Duration>,
    mean: Option<Duration>,
    errors: usize,
}

fn score(ds: &Dataset, a: &Args, config: &str, ef: Option<usize>, run: &Run) -> Row {
    let (mut ndcg, mut recall, mut mrr) = (Vec::new(), Vec::new(), Vec::new());
    let mut errors = 0;
    for (i, (qid, _)) in ds.queries.iter().enumerate() {
        let judged = &ds.qrels[qid];
        match &run.answers[i] {
            Some(ranked) => {
                let ranked = retrieval_common::drop_identical(qid, ranked.clone());
                ndcg.push(retrieval_common::ndcg_at_k(&ranked, judged, a.k));
                recall.push(retrieval_common::recall_at_k(&ranked, judged, a.depth));
                mrr.push(retrieval_common::mrr_at_k(&ranked, judged, a.k));
            }
            None => {
                errors += 1;
                ndcg.push(0.0);
                recall.push(0.0);
                mrr.push(0.0);
            }
        }
    }
    let mut lat: Vec<Duration> = run.latency.iter().flatten().copied().collect();
    lat.sort();
    let timed = !lat.is_empty();
    Row {
        config: config.to_string(),
        ef,
        ndcg: retrieval_common::mean(&ndcg),
        recall: retrieval_common::mean(&recall),
        mrr: retrieval_common::mean(&mrr),
        p50: timed.then(|| ann_common::percentile(&lat, 50.0)),
        p99: timed.then(|| ann_common::percentile(&lat, 99.0)),
        mean: timed.then(|| lat.iter().sum::<Duration>() / lat.len() as u32),
        errors,
    }
}

// ---------------------------------------------------------------------- csv

/// The columns `benches/retrieval_quality/competitors.py` writes too.
const CSV_HEADER: &str = "dataset,system,config,path,embedding_model,metric,corpus,queries,k,depth,\
m,ef_construction,ef,rrf_k,ndcg_at_k,recall_at_depth,mrr_at_k,p50_us,p99_us,mean_us,errors,calibration_ms";

fn micros(d: Option<Duration>) -> String {
    d.map(|d| format!("{:.1}", d.as_secs_f64() * 1e6))
        .unwrap_or_default()
}

fn main() {
    bench_setup::init();
    let a = match parse_args() {
        Ok(Invocation::Run(a)) => a,
        Ok(Invocation::NoDataset) => {
            eprintln!("SKIP: retrieval_quality_benchmark needs a dataset.");
            eprintln!(
                "      Pass --synthetic for the built-in corpus, or --dataset DIR for a BEIR \
                 directory (scripts/prepare_beir_datasets.py writes one)."
            );
            eprintln!(
                "      Skipping rather than failing: the benchmark did not run, it did not break."
            );
            return;
        }
        Err(e) => {
            eprintln!("retrieval_quality_benchmark: {e}");
            std::process::exit(2);
        }
    };
    let calibration = bench_setup::report_calibration();

    let ds = match (&a.dataset, a.synthetic) {
        (Some(dir), _) => match load_beir(dir, &a) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("retrieval_quality_benchmark: {e}");
                std::process::exit(2);
            }
        },
        _ => load_synthetic(&a),
    };
    if ds.queries.is_empty() {
        eprintln!("retrieval_quality_benchmark: no judged query has text; nothing to score");
        std::process::exit(2);
    }
    println!(
        "Dataset {}: {} documents, {} scored queries (qrels/{}.tsv), dense: {}",
        ds.name,
        ds.doc_ids.len(),
        ds.queries.len(),
        a.split,
        ds.dense
            .as_ref()
            .map(|d| format!("{} ({} dims, {})", d.model, d.docs.dim, d.metric.name()))
            .unwrap_or_else(|| "none (BM25 row only)".into())
    );

    let loaded = match load_store(&ds, &a) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("retrieval_quality_benchmark: index build failed: {e}");
            std::process::exit(1);
        }
    };
    println!(
        "Loaded in {:.2} s; full-text index {:.2} s; vector index {:.2} s (m={}, ef_construction={})",
        loaded.load.as_secs_f64(),
        loaded.fulltext_build.as_secs_f64(),
        loaded.vector_build.as_secs_f64(),
        a.m,
        a.ef_construction
    );

    let engine = QueryEngine::new();
    let n = ds.queries.len();
    let mut rows = Vec::new();

    let bm25_run = timed(n, |i| {
        bm25(&loaded, &ds, &engine, a.path, &ds.queries[i].1, a.depth)
    });
    rows.push(score(&ds, &a, "bm25", None, &bm25_run));

    if let Some(d) = &ds.dense {
        for &ef in &a.ef {
            let run = timed(n, |i| {
                dense_hnsw(&loaded, &ds, &engine, a.path, d.queries.row(i), a.depth, ef)
            });
            rows.push(score(&ds, &a, "dense-hnsw", Some(ef), &run));
            let fused = fuse(&bm25_run, &run, a.rrf_k, a.depth);
            rows.push(score(
                &ds,
                &a,
                "rrf-client:bm25+dense-hnsw",
                Some(ef),
                &fused,
            ));
        }
        if a.exact {
            eprintln!("Computing exact dense results for {n} queries ...");
            let truth = ann_common::exact_ground_truth(&d.docs, &d.queries, a.depth, d.metric);
            let run = Run {
                latency: vec![None; n],
                answers: (0..n)
                    .map(|i| {
                        Some(
                            truth
                                .row(i)
                                .iter()
                                .map(|p| ds.doc_ids[*p as usize].clone())
                                .collect(),
                        )
                    })
                    .collect(),
            };
            rows.push(score(&ds, &a, "dense-exact", None, &run));
            let fused = fuse(&bm25_run, &run, a.rrf_k, a.depth);
            rows.push(score(&ds, &a, "rrf-client:bm25+dense-exact", None, &fused));
        }
    }

    println!(
        "\n  {:<30} {:>5} {:>9} {:>11} {:>8} {:>10} {:>10} {:>6}",
        "config",
        "ef",
        format!("nDCG@{}", a.k),
        format!("recall@{}", a.depth),
        format!("MRR@{}", a.k),
        "p50 µs",
        "p99 µs",
        "errors"
    );
    for r in &rows {
        println!(
            "  {:<30} {:>5} {:>9.4} {:>11.4} {:>8.4} {:>10} {:>10} {:>6}",
            r.config,
            r.ef.map(|e| e.to_string()).unwrap_or_else(|| "-".into()),
            r.ndcg,
            r.recall,
            r.mrr,
            micros(r.p50),
            micros(r.p99),
            r.errors
        );
    }
    println!(
        "\nrrf-client rows are fused in this harness from the engine's two result lists \
         (k={}), not by #1279's in-engine db.hybrid.search.",
        a.rrf_k
    );
    if rows.iter().any(|r| r.errors > 0) {
        println!("Some queries failed and were scored as 0; see the errors column.");
    }

    if let Some(csv) = &a.csv {
        let new = !csv.exists();
        let mut out = String::new();
        if new {
            out.push_str(CSV_HEADER);
            out.push('\n');
        }
        let (model, metric) = ds
            .dense
            .as_ref()
            .map(|d| (d.model.clone(), d.metric.name().to_string()))
            .unwrap_or_default();
        for r in &rows {
            let dense = r.config != "bm25";
            out.push_str(&format!(
                "{},samyama-graph,{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{:.6},{:.6},{},{},{},{},{:.0}\n",
                ds.name,
                r.config,
                a.path.name(),
                if dense { model.as_str() } else { "" },
                if dense { metric.as_str() } else { "" },
                ds.doc_ids.len(),
                n,
                a.k,
                a.depth,
                if dense { a.m.to_string() } else { String::new() },
                if dense { a.ef_construction.to_string() } else { String::new() },
                r.ef.map(|e| e.to_string()).unwrap_or_default(),
                if r.config.starts_with("rrf") { a.rrf_k.to_string() } else { String::new() },
                r.ndcg,
                r.recall,
                r.mrr,
                micros(r.p50),
                micros(r.p99),
                micros(r.mean),
                r.errors,
                calibration.as_secs_f64() * 1000.0,
            ));
        }
        use std::io::Write;
        let written = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(csv)
            .and_then(|mut f| f.write_all(out.as_bytes()));
        match written {
            Ok(()) => println!("\nWrote {} rows to {}", rows.len(), csv.display()),
            Err(e) => {
                eprintln!("retrieval_quality_benchmark: {}: {e}", csv.display());
                std::process::exit(1);
            }
        }
    }

    bench_setup::report_drift(calibration);
}
