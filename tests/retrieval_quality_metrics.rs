//! The retrieval-quality bench's metrics, fusion and parsing (#1988).
//!
//! Every `[[bench]]` is `harness = false`, so the bench cannot test itself;
//! the logic it shares lives in `benches/retrieval_common` and is checked
//! here. Each expected number is worked by hand in the comment above it.
//! No timing is asserted.

#[path = "../benches/retrieval_common/mod.rs"]
mod retrieval_common;

use retrieval_common::*;
use std::collections::HashMap;

const TOL: f64 = 1e-9;

fn judged(pairs: &[(&str, i32)]) -> Judgments {
    pairs.iter().map(|(d, g)| (d.to_string(), *g)).collect()
}

fn close(got: f64, want: f64, what: &str) {
    assert!((got - want).abs() < TOL, "{what}: got {got}, want {want}");
}

// --------------------------------------------------------------------- nDCG

#[test]
fn ndcg_uses_linear_gain_and_a_log2_discount() {
    // Grades a=3, b=2, c=1; the run is [b, x, a].
    //   DCG  = 2/log2(2) + 0/log2(3) + 3/log2(4) = 2 + 0 + 1.5 = 3.5
    //   IDCG = 3/log2(2) + 2/log2(3) + 1/log2(4) = 3 + 1.2618595 + 0.5 = 4.7618595
    //   nDCG = 3.5 / 4.7618595 = 0.7350070
    let j = judged(&[("a", 3), ("b", 2), ("c", 1)]);
    let got = ndcg_at_k(&["b", "x", "a"], &j, 10);
    close(got, 3.5 / (3.0 + 2.0 / 3f64.log2() + 0.5), "nDCG@10");
    assert!((got - 0.7350070).abs() < 1e-6, "{got}");
}

#[test]
fn ndcg_cuts_both_the_run_and_the_ideal_at_k() {
    // At k = 2 the run contributes b only: DCG = 2. The ideal is a then b:
    // IDCG = 3 + 2/log2(3) = 4.2618595. nDCG@2 = 0.4692787.
    let j = judged(&[("a", 3), ("b", 2), ("c", 1)]);
    let got = ndcg_at_k(&["b", "x", "a"], &j, 2);
    close(got, 2.0 / (3.0 + 2.0 / 3f64.log2()), "nDCG@2");
    assert!((got - 0.4692787).abs() < 1e-6, "{got}");
}

#[test]
fn ndcg_of_the_ideal_ranking_is_one_and_of_nothing_relevant_is_zero() {
    let j = judged(&[("a", 3), ("b", 2), ("c", 1)]);
    close(ndcg_at_k(&["a", "b", "c"], &j, 10), 1.0, "ideal");
    close(ndcg_at_k(&["x", "y"], &j, 10), 0.0, "no hit");
    close(ndcg_at_k::<&str>(&[], &j, 10), 0.0, "empty run");
    // No relevant document at all: IDCG is 0, and the answer is 0, not NaN.
    close(ndcg_at_k(&["a"], &judged(&[("a", 0)]), 10), 0.0, "IDCG 0");
}

#[test]
fn ndcg_treats_a_negative_grade_as_zero() {
    // a=-1 counts 0, b=1 at rank 2: DCG = 1/log2(3) = 0.6309298. IDCG = 1.
    let j = judged(&[("a", -1), ("b", 1)]);
    close(
        ndcg_at_k(&["a", "b"], &j, 10),
        1.0 / 3f64.log2(),
        "negative grade",
    );
}

// ------------------------------------------------------------------- recall

#[test]
fn recall_counts_distinct_relevant_documents_in_the_top_k() {
    // Relevant (grade > 0): a, b, d -- three. c is judged 0.
    // Top 5 of [c, a, a, x, d] holds a (once) and d: 2/3.
    let j = judged(&[("a", 1), ("b", 1), ("c", 0), ("d", 2)]);
    let run = ["c", "a", "a", "x", "d"];
    close(recall_at_k(&run, &j, 5), 2.0 / 3.0, "recall@5");
    // Top 2 is [c, a]: 1/3.
    close(recall_at_k(&run, &j, 2), 1.0 / 3.0, "recall@2");
    close(
        recall_at_k(&run, &judged(&[("c", 0)]), 5),
        0.0,
        "nothing relevant",
    );
}

// ---------------------------------------------------------------------- MRR

#[test]
fn mrr_is_the_reciprocal_rank_of_the_first_relevant_document() {
    // c is judged 0 and does not count; b at rank 3 gives 1/3.
    let j = judged(&[("b", 1), ("c", 0)]);
    close(mrr_at_k(&["x", "c", "b"], &j, 10), 1.0 / 3.0, "MRR@10");
    // Cut at 2, b is out of reach.
    close(mrr_at_k(&["x", "c", "b"], &j, 2), 0.0, "MRR@2");
    close(mrr_at_k(&["b"], &j, 10), 1.0, "first");
}

// ---------------------------------------------------------------------- RRF

#[test]
fn rrf_sums_reciprocal_ranks_across_lists() {
    // k = 60, A = [a, b, c], B = [c, a, d]:
    //   a = 1/61 + 1/62 = 0.0325225
    //   c = 1/63 + 1/61 = 0.0322664
    //   b = 1/62        = 0.0161290
    //   d = 1/63        = 0.0158730
    let a = ["a", "b", "c"];
    let b = ["c", "a", "d"];
    assert_eq!(rrf(&[&a[..], &b[..]], 60.0, 10), ["a", "c", "b", "d"]);
    assert_eq!(rrf(&[&a[..], &b[..]], 60.0, 3), ["a", "c", "b"]);
}

#[test]
fn rrf_breaks_ties_by_best_rank_then_first_appearance() {
    // A = [a, b], B = [b, a]: both score 1/61 + 1/62 and both have best rank
    // 1. a was named first, so a leads -- every run, not by hash order.
    let a = ["a", "b"];
    let b = ["b", "a"];
    for _ in 0..20 {
        assert_eq!(rrf(&[&a[..], &b[..]], 60.0, 10), ["a", "b"]);
    }
    // A = [x, y], B = [z]: x and z tie on 1/61 at best rank 1; x came first.
    // y = 1/62 is last.
    assert_eq!(
        rrf(&[&["x", "y"][..], &["z"][..]], 60.0, 10),
        ["x", "z", "y"]
    );
}

#[test]
fn rrf_counts_a_document_once_per_list() {
    // A = [a, a, b]: a contributes 1/61 once, b 1/63. B = [b]: b gains 1/61.
    //   b = 1/63 + 1/61 = 0.0322664 > a = 1/61 = 0.0163934.
    let a = ["a", "a", "b"];
    let b = ["b"];
    assert_eq!(rrf(&[&a[..], &b[..]], 60.0, 10), ["b", "a"]);
}

// ------------------------------------------------------------------ parsing

#[test]
fn qrels_skip_the_header_and_keep_grades() {
    let q =
        parse_qrels("query-id\tcorpus-id\tscore\r\nq1\td1\t2\r\nq1\td2\t0\nq2\td1\t1\n\n").unwrap();
    assert_eq!(q.len(), 2);
    assert_eq!(q["q1"], judged(&[("d1", 2), ("d2", 0)]));
    assert_eq!(q["q2"], judged(&[("d1", 1)]));
    // No header: the first line is data, not skipped.
    let q = parse_qrels("q1\td1\t1\n").unwrap();
    assert_eq!(q["q1"], judged(&[("d1", 1)]));
}

#[test]
fn qrels_refuse_a_malformed_line() {
    let e = parse_qrels("q1\td1\t1\nq2\td2\tyes\n").unwrap_err();
    assert!(e.contains("line 2"), "{e}");
    let e = parse_qrels("q1 d1 1\n").unwrap_err();
    assert!(e.contains("3 tab-separated"), "{e}");
}

#[test]
fn a_corpus_line_is_title_then_text() {
    let line = r#"{"_id": "d1", "title": "Graphs", "text": "are sets of edges.", "metadata": {}}"#;
    assert_eq!(
        parse_beir_line(line).unwrap(),
        Some(("d1".into(), "Graphs are sets of edges.".into()))
    );
    // Queries have no title; some corpora use numeric ids.
    assert_eq!(
        parse_beir_line(r#"{"_id": 7, "text": "what is a graph"}"#).unwrap(),
        Some(("7".into(), "what is a graph".into()))
    );
    assert_eq!(
        parse_beir_line(r#"{"_id": "d2", "title": "Only a title", "text": ""}"#).unwrap(),
        Some(("d2".into(), "Only a title".into()))
    );
    assert_eq!(parse_beir_line("   ").unwrap(), None);
    assert!(parse_beir_line(r#"{"text": "no id"}"#).is_err());
    assert!(parse_beir_line("not json").is_err());
}

#[test]
fn a_bm25_query_carries_no_query_syntax() {
    // The quotes would make a phrase query and the asterisk a prefix search.
    assert_eq!(
        bm25_query_text(r#"What is "graph*" theory? (2024) don't"#),
        "What is graph theory 2024 don t"
    );
    assert_eq!(bm25_query_text("?!"), "");
    assert!(!bm25_query_text("it's").contains('\''));
}

#[test]
fn identical_ids_are_dropped_and_nothing_else() {
    let ranked = vec!["q1".to_string(), "d1".into(), "q1".into(), "d2".into()];
    assert_eq!(drop_identical("q1", ranked), ["d1", "d2"]);
}

#[test]
fn only_queries_with_a_relevant_document_are_scored() {
    let mut q: HashMap<String, Judgments> = HashMap::new();
    q.insert("q2".into(), judged(&[("d1", 1)]));
    q.insert("q1".into(), judged(&[("d1", 0), ("d2", 2)]));
    q.insert("q3".into(), judged(&[("d1", 0)]));
    assert_eq!(scored_queries(&q), ["q1", "q2"]);
    close(mean(&[]), 0.0, "mean of none");
    close(mean(&[1.0, 0.0, 0.5]), 0.5, "mean");
}

// ---------------------------------------------------------------- synthetic

#[test]
fn the_synthetic_corpus_is_deterministic_and_consistent() {
    let s = synthetic(20, 50, 32, 7);
    let t = synthetic(20, 50, 32, 7);
    assert_eq!(s.docs, t.docs);
    assert_eq!(s.queries, t.queries);
    assert_eq!(s.doc_vectors, t.doc_vectors);
    assert_eq!(s.docs.len(), 1000);
    assert_eq!(s.queries.len(), 40);
    assert_eq!(s.doc_vectors.len(), 1000);
    assert_eq!(s.query_vectors.len(), 40);
    assert!(s.doc_vectors.iter().all(|v| v.len() == 32));

    // Every judgment is earned: a document graded g holds exactly g of its
    // query's two words.
    let text: HashMap<&str, &str> = s
        .docs
        .iter()
        .map(|(i, t)| (i.as_str(), t.as_str()))
        .collect();
    for (qid, qtext) in &s.queries {
        let words: Vec<&str> = qtext.split(' ').collect();
        assert_eq!(words.len(), 2);
        for (doc, g) in &s.qrels[qid] {
            let held = text[doc.as_str()].split(' ').collect::<Vec<_>>();
            let n = words.iter().filter(|w| held.contains(w)).count() as i32;
            assert_eq!(n, *g, "{qid} {doc}");
        }
    }
    assert!(
        !scored_queries(&s.qrels).is_empty(),
        "the synthetic corpus must have something to score"
    );
}

// ------------------------------------------------------- the bench's queries

/// The two procedure calls the bench sends on `--path cypher`, against a
/// store shaped as it loads one. Checked here because the bench cannot be run
/// in CI with data, and a statement the engine rejects would score every
/// query as an error.
#[test]
fn the_benchs_cypher_statements_return_document_ids() {
    use samyama::graph::{GraphStore, Label, PropertyValue};
    use samyama::query::{BoundParams, QueryEngine, Value};

    let mut store = GraphStore::new();
    for (id, text, v) in [
        (
            "d1",
            "graph databases store relationships",
            vec![1.0f32, 0.0],
        ),
        ("d2", "relational tables and joins", vec![0.0, 1.0]),
    ] {
        let mut props = HashMap::new();
        props.insert("doc_id".to_string(), PropertyValue::String(id.into()));
        props.insert("text".to_string(), PropertyValue::String(text.into()));
        props.insert("embedding".to_string(), PropertyValue::Vector(v));
        store.create_node_with_properties("default", vec![Label::new("Doc")], props);
    }
    assert_eq!(store.create_fulltext_index("beir_text", "Doc", "text"), 2);
    store
        .create_vector_index_with_params(
            None,
            "Doc",
            "embedding",
            2,
            samyama::vector::DistanceMetric::Cosine,
            samyama::vector::index::Quantization::None,
            samyama::vector::HnswParams {
                m: 16,
                ef_construction: 200,
                ef: None,
            },
        )
        .unwrap();
    store.rebuild_vector_index();

    let engine = QueryEngine::new();
    let ids = |q: &str, params: &BoundParams| -> Vec<String> {
        let batch = engine
            .execute_with_params(q, &store, params)
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        batch
            .records
            .iter()
            .map(|r| match r.get("id") {
                Some(Value::Property(PropertyValue::String(s))) => s.clone(),
                other => panic!("{q}: {other:?}"),
            })
            .collect()
    };

    let text = bm25_query_text("Graph relationships?");
    let q = format!(
        "CALL db.index.fulltext.queryNodes('beir_text', '{text}', 100) \
         YIELD node RETURN node.doc_id AS id"
    );
    assert_eq!(ids(&q, &BoundParams::new()), ["d1"]);

    let mut params = BoundParams::new();
    params.insert("q".to_string(), PropertyValue::Vector(vec![0.1, 0.9]));
    let q = "CALL db.index.vector.queryNodes('Doc', 'embedding', $q, 2, {ef: 100}) \
             YIELD node RETURN node.doc_id AS id";
    assert_eq!(ids(q, &params), ["d2", "d1"]);
}
