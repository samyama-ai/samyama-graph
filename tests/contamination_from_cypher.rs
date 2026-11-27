//! Benchmark contamination, callable from Cypher (ML-12, #1762).
//!
//! `algo.contamination` reports which corpus texts contain a benchmark item,
//! by word n-gram overlap. These tests plant known overlaps in a small corpus
//! (a verbatim copy inside a longer text, a copy with different case and
//! punctuation, a copy with a light edit, a short question quoted in an
//! answer) next to texts that only share common words, and hold the report to
//! finding exactly the planted pairs, with the overlap the definition gives.
//!
//! No public benchmark ships with the engine. The benchmark here is a handful
//! of invented questions; a caller loads the real one as nodes.

use std::collections::BTreeSet;

use samyama::graph::{GraphStore, PropertyValue};
use samyama::query::executor::Record;
use samyama::query::QueryEngine;

const TENANT: &str = "default";

fn run(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> Vec<Record> {
    engine
        .execute_mut(q, store, TENANT)
        .unwrap_or_else(|e| panic!("{q}: {e}"))
        .records
}

fn err(store: &mut GraphStore, engine: &QueryEngine, q: &str) -> String {
    match engine.execute_mut(q, store, TENANT) {
        Ok(r) => panic!("{q}: expected an error, got {} row(s)", r.records.len()),
        Err(e) => e.to_string(),
    }
}

fn text(rec: &Record, col: &str) -> String {
    match rec.get(col).and_then(|v| v.as_property()) {
        Some(PropertyValue::String(s)) => s.clone(),
        other => panic!("{col} is {other:?}"),
    }
}

fn int(rec: &Record, col: &str) -> i64 {
    match rec.get(col).and_then(|v| v.as_property()) {
        Some(PropertyValue::Integer(i)) => *i,
        other => panic!("{col} is {other:?}"),
    }
}

fn float(rec: &Record, col: &str) -> f64 {
    match rec.get(col).and_then(|v| v.as_property()) {
        Some(PropertyValue::Float(f)) => *f,
        other => panic!("{col} is {other:?}"),
    }
}

// Four benchmark questions. Q1 and Q2 have 15 tokens (8 distinct 8-grams), Q3
// has 16 (9); Q4 has 3, fewer than n, so its one gram is the whole question.
const Q1: &str = "Which river flows through the capital of France before it finally reaches the English Channel?";
const Q2: &str =
    "How many moons orbit the planet Neptune according to the most recent survey by astronomers?";
const Q3: &str =
    "What year did the first crewed spacecraft land on the Moon and return safely to Earth?";
const Q4: &str = "Who wrote Hamlet?";

/// The benchmark, and a corpus with planted overlaps:
///
/// | doc | planted | expected |
/// |---|---|---|
/// | exact | Q1 verbatim, inside a paragraph | Q1, overlap 1.0 |
/// | reformatted | Q2 in upper case, punctuation and spacing changed | Q2, overlap 1.0 |
/// | edited-end | Q3 with its last word changed | Q3, 8 of 9 grams |
/// | edited-middle | Q3 with a word in the middle changed | Q3, 1 of 9: below the default 0.5 |
/// | quoted-short | Q4 quoted in an answer | Q4, its one gram |
/// | shuffled-short | Q4's words in another order | nothing |
/// | common-words | the words of Q1, never 8 in a row | nothing |
/// | unrelated | a recipe | nothing |
fn corpus() -> (GraphStore, QueryEngine) {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    for (name, q) in [("Q1", Q1), ("Q2", Q2), ("Q3", Q3), ("Q4", Q4)] {
        run(
            &mut store,
            &engine,
            &format!("CREATE (:Question {{name: '{name}', text: \"{q}\"}})"),
        );
    }
    let docs = [
        ("exact", format!("Geography notes. {Q1} The answer is the Seine. More notes follow here.")),
        (
            "reformatted",
            "HOW MANY MOONS ORBIT THE PLANET NEPTUNE -- according to the most-recent survey by astronomers"
                .to_string(),
        ),
        ("edited-end", Q3.replace("Earth", "base")),
        ("edited-middle", Q3.replace("land", "touch down")),
        ("quoted-short", "Trivia: who wrote Hamlet? Shakespeare, around 1600.".to_string()),
        ("shuffled-short", "Hamlet wrote who, they asked.".to_string()),
        (
            "common-words",
            "The river reaches the channel. France has a capital. Which flows before it finally? Through the English."
                .to_string(),
        ),
        ("unrelated", "Whisk two eggs with a cup of flour and bake for twenty minutes.".to_string()),
    ];
    for (name, t) in docs {
        run(
            &mut store,
            &engine,
            &format!("CREATE (:Doc {{name: '{name}', text: \"{t}\"}})"),
        );
    }
    (store, engine)
}

const CALL: &str = "CALL algo.contamination({corpusLabel: 'Doc', benchmarkLabel: 'Question'}) \
                    YIELD corpus, benchmark, shared, ngrams, overlap \
                    RETURN corpus.name AS doc, benchmark.name AS q, shared, ngrams, overlap";

fn pairs(rows: &[Record]) -> Vec<(String, String, i64, i64)> {
    rows.iter()
        .map(|r| {
            (
                text(r, "doc"),
                text(r, "q"),
                int(r, "shared"),
                int(r, "ngrams"),
            )
        })
        .collect()
}

#[test]
fn finds_exactly_the_planted_overlaps_at_the_default_threshold() {
    let (mut store, engine) = corpus();
    let rows = run(&mut store, &engine, CALL);
    let got = pairs(&rows);
    let s = |a: &str, b: &str, sh: i64, n: i64| (a.to_string(), b.to_string(), sh, n);
    assert_eq!(
        got,
        vec![
            // Overlap 1.0 first, by corpus id: exact, reformatted, quoted-short.
            s("exact", "Q1", 8, 8),
            s("reformatted", "Q2", 8, 8),
            s("quoted-short", "Q4", 1, 1),
            s("edited-end", "Q3", 8, 9),
        ]
    );
    let overlaps: Vec<f64> = rows.iter().map(|r| float(r, "overlap")).collect();
    assert_eq!(overlaps[..3], [1.0, 1.0, 1.0]);
    assert!((overlaps[3] - 8.0 / 9.0).abs() < 1e-12, "{overlaps:?}");
}

#[test]
fn a_lower_threshold_also_reports_the_heavier_edit() {
    let (mut store, engine) = corpus();
    let q = CALL.replace(
        "benchmarkLabel: 'Question'",
        "benchmarkLabel: 'Question', minOverlap: 0.1",
    );
    let got: BTreeSet<(String, String, i64, i64)> =
        pairs(&run(&mut store, &engine, &q)).into_iter().collect();
    // "land" is token 7 of 16 (from 0), inside every 8-gram but the last
    // ("on the moon and return safely to earth"): 1 of 9, about 0.11.
    assert!(
        got.contains(&("edited-middle".into(), "Q3".into(), 1, 9)),
        "{got:?}"
    );
    for clean in ["shuffled-short", "common-words", "unrelated"] {
        assert!(
            got.iter().all(|(d, ..)| d != clean),
            "{clean} reported: {got:?}"
        );
    }
}

#[test]
fn n_is_the_window_length() {
    let (mut store, engine) = corpus();
    // At n = 2 the common-words doc shares 5 of Q1's 14 bigrams ("reaches
    // the", "before it", ...): a short window reports phrase reuse, not copying.
    let q = CALL.replace(
        "benchmarkLabel: 'Question'",
        "benchmarkLabel: 'Question', n: 2, minOverlap: 0.2",
    );
    let docs: BTreeSet<String> = run(&mut store, &engine, &q)
        .iter()
        .map(|r| text(r, "doc"))
        .collect();
    assert!(docs.contains("common-words"), "{docs:?}");
    // At n = 8 (the default) it is not reported at any threshold.
    let q = CALL.replace(
        "benchmarkLabel: 'Question'",
        "benchmarkLabel: 'Question', minOverlap: 0.01",
    );
    let docs: BTreeSet<String> = run(&mut store, &engine, &q)
        .iter()
        .map(|r| text(r, "doc"))
        .collect();
    assert!(!docs.contains("common-words"), "{docs:?}");
}

#[test]
fn case_accents_and_punctuation_do_not_hide_a_copy() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    run(
        &mut store,
        &engine,
        "CREATE (:Q {text: 'Où se trouve le Musée d’Orsay à Paris?'})",
    );
    run(
        &mut store,
        &engine,
        "CREATE (:D {text: 'OÙ SE TROUVE LE MUSÉE D ORSAY ... À PARIS'})",
    );
    let rows = run(
        &mut store,
        &engine,
        "CALL algo.contamination({corpusLabel: 'D', benchmarkLabel: 'Q', n: 3}) YIELD overlap RETURN overlap",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(float(&rows[0], "overlap"), 1.0);
}

#[test]
fn empty_texts_are_never_matched_and_a_node_is_not_its_own_contamination() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    // One node carries both labels: it is not reported against itself.
    run(
        &mut store,
        &engine,
        &format!("CREATE (:Doc:Question {{name: 'both', text: \"{Q1}\"}})"),
    );
    run(
        &mut store,
        &engine,
        "CREATE (:Doc {name: 'empty', text: ''}), (:Question {name: 'blank', text: ' ?! '})",
    );
    let rows = run(
        &mut store,
        &engine,
        &CALL.replace(
            "benchmarkLabel: 'Question'",
            "benchmarkLabel: 'Question', minOverlap: 0.01",
        ),
    );
    assert!(rows.is_empty(), "{:?}", pairs(&rows));
}

#[test]
fn benchmark_property_reads_a_different_property_on_the_benchmark() {
    let mut store = GraphStore::new();
    let engine = QueryEngine::new();
    run(
        &mut store,
        &engine,
        &format!("CREATE (:Question {{name: 'Q1', prompt: \"{Q1}\"}})"),
    );
    run(
        &mut store,
        &engine,
        &format!("CREATE (:Doc {{name: 'exact', body: \"{Q1}\"}})"),
    );
    let rows = run(
        &mut store,
        &engine,
        "CALL algo.contamination({corpusLabel: 'Doc', benchmarkLabel: 'Question', property: 'body', \
         benchmarkProperty: 'prompt'}) YIELD corpus, benchmark RETURN corpus.name AS doc, benchmark.name AS q",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        (text(&rows[0], "doc"), text(&rows[0], "q")),
        ("exact".into(), "Q1".into())
    );
}

#[test]
fn aliases_and_repeated_calls_return_the_same_rows() {
    let (mut store, engine) = corpus();
    let first = pairs(&run(&mut store, &engine, CALL));
    assert_eq!(
        first,
        pairs(&run(&mut store, &engine, CALL)),
        "not deterministic"
    );
    for alias in ["algo.ngramOverlap", "algo.contaminationCheck"] {
        assert_eq!(
            first,
            pairs(&run(
                &mut store,
                &engine,
                &CALL.replace("algo.contamination", alias)
            )),
            "{alias}"
        );
    }
}

#[test]
fn what_it_cannot_check_is_refused_not_reported_clean() {
    let (mut store, engine) = corpus();
    let call = |extra: &str| {
        format!("CALL algo.contamination({{corpusLabel: 'Doc', benchmarkLabel: 'Question'{extra}}}) YIELD overlap RETURN overlap")
    };
    let cases = [
        // A misspelt label must not read as "no contamination".
        ("CALL algo.contamination({corpusLabel: 'Docs', benchmarkLabel: 'Question'}) YIELD overlap RETURN overlap".to_string(), "no node has the corpus label `Docs`"),
        ("CALL algo.contamination({corpusLabel: 'Doc', benchmarkLabel: 'Questions'}) YIELD overlap RETURN overlap".to_string(), "no node has the benchmark label `Questions`"),
        ("CALL algo.contamination({benchmarkLabel: 'Question'}) YIELD overlap RETURN overlap".to_string(), "`corpusLabel` is required"),
        ("CALL algo.contamination({corpusLabel: 'Doc'}) YIELD overlap RETURN overlap".to_string(), "`benchmarkLabel` is required"),
        (call(", n: 0"), "`n` must be an integer of at least 1"),
        (call(", n: 2.5"), "`n` must be an integer of at least 1"),
        (call(", minOverlap: 0.0"), "`minOverlap` must be above 0 and at most 1"),
        (call(", minOverlap: 1.5"), "`minOverlap` must be above 0 and at most 1"),
        (call(", property: 'nope'"), "have no `nope`"),
        (call(", treshold: 0.5"), "treshold"),
    ];
    for (q, want) in cases {
        let e = err(&mut store, &engine, &q);
        assert!(e.contains(want), "{q}\n  wanted {want:?}\n  got {e}");
    }
    // A node of the label without the text is refused, naming how many.
    run(&mut store, &engine, "CREATE (:Doc {name: 'no-text'})");
    let e = err(&mut store, &engine, CALL);
    assert!(e.contains("1 of 9 :Doc node(s) have no `text`"), "{e}");
    // A text that is not a string is refused, not stringified.
    let (mut store, engine) = corpus();
    run(
        &mut store,
        &engine,
        "CREATE (:Question {name: 'Q5', text: 42})",
    );
    let e = err(&mut store, &engine, CALL);
    assert!(e.contains("not a string"), "{e}");
}
