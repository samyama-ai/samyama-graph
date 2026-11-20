//! The dataset-free parts of the retrieval-quality benchmark (#1988): BEIR
//! file parsing, the ranking metrics, client-side reciprocal rank fusion, the
//! query sanitiser for the full-text index, and a tiny synthetic corpus.
//!
//! Shared by `benches/retrieval_quality_benchmark.rs`, which runs the
//! configurations, and `tests/retrieval_quality_metrics.rs`, which checks every
//! formula here against hand-computed values. A bench target cannot run its
//! own tests, so the logic lives here rather than in the bench. Nothing in
//! this file touches the engine.

#![allow(dead_code)]

use std::collections::HashMap;

/// Relevance judgments of one query: document id -> grade.
pub type Judgments = HashMap<String, i32>;

// ------------------------------------------------------------------ parsing

/// Parse a BEIR qrels file (`query-id \t corpus-id \t score`).
///
/// The header line BEIR ships (`query-id corpus-id score`) is skipped: it is
/// recognised by a grade that does not parse as an integer, so a file without
/// a header loses no row. A repeated (query, document) pair keeps the last
/// grade, as `pytrec_eval` does when it builds its dictionary.
pub fn parse_qrels(text: &str) -> Result<HashMap<String, Judgments>, String> {
    let mut out: HashMap<String, Judgments> = HashMap::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() != 3 {
            return Err(format!(
                "qrels line {}: expected 3 tab-separated columns, got {}",
                n + 1,
                cols.len()
            ));
        }
        let grade = match cols[2].trim().parse::<i32>() {
            Ok(g) => g,
            Err(_) if n == 0 => continue,
            Err(_) => {
                return Err(format!(
                    "qrels line {}: grade `{}` is not an integer",
                    n + 1,
                    cols[2]
                ))
            }
        };
        out.entry(cols[0].trim().to_string())
            .or_default()
            .insert(cols[1].trim().to_string(), grade);
    }
    Ok(out)
}

/// One line of `corpus.jsonl` or `queries.jsonl`: its `_id` and its text.
///
/// A corpus document's text is `title + " " + text`, the concatenation
/// Pyserini's BEIR baselines index and the one the prepare script embeds; a
/// query has no title, so it is its `text`. Every system in the comparison is
/// given this same string, so no system sees a field another does not.
pub fn parse_beir_line(line: &str) -> Result<Option<(String, String)>, String> {
    if line.trim().is_empty() {
        return Ok(None);
    }
    let v: serde_json::Value = serde_json::from_str(line).map_err(|e| format!("not JSON: {e}"))?;
    let id = match v.get("_id") {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        _ => return Err("no `_id`".into()),
    };
    let field = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("").trim();
    let text = match (field("title"), field("text")) {
        ("", t) => t.to_string(),
        (title, "") => title.to_string(),
        (title, t) => format!("{title} {t}"),
    };
    Ok(Some((id, text)))
}

/// A query as the full-text index should receive it: its alphanumeric runs
/// separated by single spaces.
///
/// The index's query language gives `"` (phrase) and a trailing `*` (prefix)
/// a meaning. A BEIR query is natural language, and passing it raw would turn
/// a quoted title into a phrase query and an asterisk into a prefix search --
/// neither of which is BM25. Splitting on non-alphanumerics is exactly what
/// the index's tokeniser does to a document, so no word is lost; and the
/// result holds no quote, so it can be written into a Cypher string literal
/// without escaping.
pub fn bm25_query_text(query: &str) -> String {
    query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

// ------------------------------------------------------------------ metrics
//
// The definitions BEIR reports through `pytrec_eval` (trec_eval):
//
// - nDCG@k with **linear** gain: DCG = sum over ranks i = 1..k of
//   rel_i / log2(i + 1); IDCG is the same sum over the judged grades sorted
//   descending. Negative grades count as 0.
// - recall@k: relevant documents (grade > 0) in the top k over all relevant.
// - MRR@k: 1 / rank of the first relevant document in the top k, else 0.
//
// One difference, stated rather than hidden: trec_eval re-sorts a run by score
// and breaks ties by document id; these take the ranking as the system
// returned it. A run with tied scores can therefore differ in the fourth
// decimal from a trec_eval figure.

fn grade(judged: &Judgments, id: &str) -> f64 {
    judged.get(id).copied().unwrap_or(0).max(0) as f64
}

/// Normalised discounted cumulative gain at `k`. 0 when nothing is relevant.
pub fn ndcg_at_k<S: AsRef<str>>(ranked: &[S], judged: &Judgments, k: usize) -> f64 {
    let dcg: f64 = ranked
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, id)| grade(judged, id.as_ref()) / ((i + 2) as f64).log2())
        .sum();
    let mut ideal: Vec<f64> = judged
        .values()
        .map(|g| (*g).max(0) as f64)
        .filter(|g| *g > 0.0)
        .collect();
    ideal.sort_by(|a, b| b.total_cmp(a));
    let idcg: f64 = ideal
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, g)| g / ((i + 2) as f64).log2())
        .sum();
    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
}

/// Share of the relevant documents found in the top `k`. 0 when nothing is
/// relevant. A document returned twice counts once.
pub fn recall_at_k<S: AsRef<str>>(ranked: &[S], judged: &Judgments, k: usize) -> f64 {
    let relevant = judged.values().filter(|g| **g > 0).count();
    if relevant == 0 {
        return 0.0;
    }
    let mut seen = std::collections::HashSet::new();
    let found = ranked
        .iter()
        .take(k)
        .map(|s| s.as_ref())
        .filter(|id| judged.get(*id).is_some_and(|g| *g > 0) && seen.insert(*id))
        .count();
    found as f64 / relevant as f64
}

/// Reciprocal rank of the first relevant document in the top `k`.
pub fn mrr_at_k<S: AsRef<str>>(ranked: &[S], judged: &Judgments, k: usize) -> f64 {
    ranked
        .iter()
        .take(k)
        .position(|id| judged.get(id.as_ref()).is_some_and(|g| *g > 0))
        .map(|i| 1.0 / (i + 1) as f64)
        .unwrap_or(0.0)
}

/// Drop a returned document whose id is the query's own id.
///
/// BEIR's `EvaluateRetrieval.evaluate` does this by default
/// (`ignore_identical_ids=True`): in ArguAna and Quora the queries are
/// themselves corpus documents, and a system would otherwise be credited or
/// charged for finding the query. It changes nothing on a dataset whose query
/// and document ids are disjoint.
pub fn drop_identical(query_id: &str, ranked: Vec<String>) -> Vec<String> {
    ranked.into_iter().filter(|d| d != query_id).collect()
}

// ---------------------------------------------------------------------- RRF

/// Reciprocal rank fusion (Cormack et al., 2009): each document scores
/// sum over lists of 1 / (`k` + rank), rank counted from 1, and the fused list
/// is the top `depth` by that score.
///
/// This is the fusion the harness applies to two result lists it already has
/// -- client-side, as a user without in-engine fusion would. It is not
/// #1279's `db.hybrid.search`. Ties go to the document with the better best
/// rank, then to the list that named it first, so the order is deterministic.
pub fn rrf<S: AsRef<str>>(lists: &[&[S]], k: f64, depth: usize) -> Vec<String> {
    // id -> (score, best rank, order of first appearance)
    let mut acc: HashMap<&str, (f64, usize, usize)> = HashMap::new();
    let mut order = 0usize;
    for list in lists {
        let mut seen = std::collections::HashSet::new();
        for (i, id) in list.iter().enumerate() {
            let id = id.as_ref();
            // A list naming a document twice contributes its better rank only.
            if !seen.insert(id) {
                continue;
            }
            let rank = i + 1;
            let e = acc.entry(id).or_insert_with(|| {
                order += 1;
                (0.0, rank, order)
            });
            e.0 += 1.0 / (k + rank as f64);
            e.1 = e.1.min(rank);
        }
    }
    let mut fused: Vec<(&str, (f64, usize, usize))> = acc.into_iter().collect();
    fused.sort_by(|a, b| {
        b.1 .0
            .total_cmp(&a.1 .0)
            .then(a.1 .1.cmp(&b.1 .1))
            .then(a.1 .2.cmp(&b.1 .2))
    });
    fused
        .into_iter()
        .take(depth)
        .map(|(id, _)| id.to_string())
        .collect()
}

// ---------------------------------------------------------------- summaries

/// Mean of per-query values, 0 for none.
pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

/// The queries that are scored: those with at least one judgment above 0.
///
/// A query whose judgments are all 0 has an IDCG of 0 and no relevant
/// document, so every system scores 0 on it and it only dilutes the mean.
/// Sorted, so every run walks the queries in the same order.
pub fn scored_queries(qrels: &HashMap<String, Judgments>) -> Vec<String> {
    let mut q: Vec<String> = qrels
        .iter()
        .filter(|(_, j)| j.values().any(|g| *g > 0))
        .map(|(id, _)| id.clone())
        .collect();
    q.sort();
    q
}

// ---------------------------------------------------------------- synthetic

/// A splitmix64 stream: deterministic, no dependency, good enough to pick
/// words.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    /// Roughly uniform in [-1, 1).
    pub fn unit(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 23) as f32 - 1.0
    }
}

/// A corpus, its queries, judgments and embeddings, in the shape the bench
/// reads from a BEIR directory.
pub struct Synthetic {
    pub docs: Vec<(String, String)>,
    pub queries: Vec<(String, String)>,
    pub qrels: HashMap<String, Judgments>,
    pub dim: usize,
    /// One row per document, in `docs` order.
    pub doc_vectors: Vec<Vec<f32>>,
    /// One row per query, in `queries` order.
    pub query_vectors: Vec<Vec<f32>>,
}

/// A small topical corpus that exercises every code path, not a model of real
/// text. Its numbers mean nothing beyond "the plumbing works".
///
/// `topics` topics of `per_topic` documents. Each topic has its own words;
/// every document holds six of its topic's words and six shared filler words.
/// Each topic has two queries of two topic words: a document holding both is
/// graded 2, one holding either is graded 1. A document's embedding is its
/// topic's centre plus noise, so the dense search finds the topic and BM25
/// finds the words -- they disagree, which is what makes fusion visible.
pub fn synthetic(topics: usize, per_topic: usize, dim: usize, seed: u64) -> Synthetic {
    let mut rng = Rng::new(seed);
    let topic_words = 30;
    let filler_words = 50;
    let centres: Vec<Vec<f32>> = (0..topics)
        .map(|_| (0..dim).map(|_| rng.unit()).collect())
        .collect();
    let noisy = |rng: &mut Rng, c: &[f32], scale: f32| -> Vec<f32> {
        c.iter().map(|x| x + scale * rng.unit()).collect()
    };

    let mut docs = Vec::new();
    let mut doc_vectors = Vec::new();
    let mut words_of: Vec<Vec<usize>> = Vec::new();
    for (t, centre) in centres.iter().enumerate() {
        for i in 0..per_topic {
            let mine: Vec<usize> = (0..6).map(|_| rng.below(topic_words)).collect();
            let mut text: Vec<String> = mine.iter().map(|w| format!("t{t}word{w}")).collect();
            text.extend((0..6).map(|_| format!("filler{}", rng.below(filler_words))));
            docs.push((format!("d{t}-{i}"), text.join(" ")));
            doc_vectors.push(noisy(&mut rng, centre, 0.6));
            words_of.push(mine);
        }
    }

    let mut queries = Vec::new();
    let mut query_vectors = Vec::new();
    let mut qrels: HashMap<String, Judgments> = HashMap::new();
    for (t, centre) in centres.iter().enumerate() {
        for j in 0..2 {
            let a = rng.below(topic_words);
            let b = (a + 1 + rng.below(topic_words - 1)) % topic_words;
            let qid = format!("q{t}-{j}");
            queries.push((qid.clone(), format!("t{t}word{a} t{t}word{b}")));
            query_vectors.push(noisy(&mut rng, centre, 0.6));
            let judged = qrels.entry(qid).or_default();
            for i in 0..per_topic {
                let w = &words_of[t * per_topic + i];
                let g = w.contains(&a) as i32 + w.contains(&b) as i32;
                if g > 0 {
                    judged.insert(format!("d{t}-{i}"), g);
                }
            }
        }
    }
    Synthetic {
        docs,
        queries,
        qrels,
        dim,
        doc_vectors,
        query_vectors,
    }
}
