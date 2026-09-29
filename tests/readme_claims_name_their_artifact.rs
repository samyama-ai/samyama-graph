//! README result lines that the harness claim register cannot see (samyama-graph#1330).
//!
//! `CH-REPRO` checks every claim in the register against a current measurement, and
//! publishes `result_lines_the_scan_cannot_see` for the README results outside it. Three of
//! those have no harness suite behind them, so the register cannot hold them:
//!
//! - HIER "Latest: **108/108 agree**" — backed by `benchmarks/hier/results/PROVENANCE.json`,
//!   an artifact in this repository that no suite publishes as a measurement;
//! - the biomedical "96 of 100 queries" result, stated three times, measured once on
//!   2026-04-02;
//! - the XK02 "10.3 seconds" headline, from the same run.
//!
//! What this test can check without re-running anything is that each line still says
//! where it came from: the HIER figure matches the file it names, and the two historical
//! results carry their date wherever they are stated. A new, undated statement of either
//! — the "96/100 queries pass" shape this issue found three times — fails here.

use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn readme() -> String {
    std::fs::read_to_string(root().join("README.md")).expect("read README.md")
}

fn json(rel: &str) -> serde_json::Value {
    let text = std::fs::read_to_string(root().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// The paragraph (text up to the next blank line) that starts at byte `at`.
fn paragraph_from(text: &str, at: usize) -> &str {
    let rest = &text[at..];
    &rest[..rest.find("\n\n").unwrap_or(rest.len())]
}

/// The paragraph that contains byte `at` — or, inside a table, just that row, so one dated
/// row cannot vouch for an undated one beside it.
fn paragraph_around(text: &str, at: usize) -> &str {
    let line_start = text[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
    if text[line_start..].starts_with('|') {
        let rest = &text[line_start..];
        return &rest[..rest.find('\n').unwrap_or(rest.len())];
    }
    let start = text[..at].rfind("\n\n").map(|i| i + 2).unwrap_or(0);
    paragraph_from(text, start)
}

#[test]
fn hier_agreement_matches_its_provenance_file() {
    let readme = readme();
    let prov = json("benchmarks/hier/results/PROVENANCE.json");
    let corpus = json("benchmarks/hier/queries.json");

    let queries = prov["queries"].as_u64().expect("PROVENANCE.queries");
    let agreed = prov["agreed"].as_u64().expect("PROVENANCE.agreed");
    let commit = prov["commit"].as_str().expect("PROVENANCE.commit");
    let corpus_size = corpus["queries"].as_array().expect("corpus queries").len() as u64;

    let re = regex::Regex::new(r"Latest: \*\*(\d+)/(\d+)\s+agree\*\*").unwrap();
    let hits: Vec<_> = re.captures_iter(&readme).collect();
    assert_eq!(
        hits.len(),
        1,
        "README should state the HIER agreement once, as 'Latest: **N/M agree**'"
    );
    let cap = &hits[0];
    let (n, m): (u64, u64) = (cap[1].parse().unwrap(), cap[2].parse().unwrap());
    assert_eq!(
        (n, m),
        (agreed, queries),
        "README says {n}/{m} agree; PROVENANCE.json records agreed={agreed}, queries={queries}"
    );

    let para = paragraph_around(&readme, cap.get(0).unwrap().start());
    assert!(
        para.contains("benchmarks/hier/results/PROVENANCE.json"),
        "the HIER agreement line must name the file it comes from"
    );
    assert!(
        para.contains(&format!("`{commit}`")),
        "the HIER agreement line must name PROVENANCE.json's commit `{commit}`"
    );
    if prov["dirty"].as_bool() == Some(true) {
        assert!(
            para.contains("\"dirty\": true"),
            "PROVENANCE.json records a dirty tree; the README must say so rather than \
             present `{commit}` as a reproducible commit"
        );
    }
    let outside = corpus_size - queries;
    if outside > 0 {
        assert!(
            para.contains(&format!("{outside} further corpus queries")),
            "the corpus has {corpus_size} queries and the denominator is {queries}; the README \
             must say {outside} are outside it"
        );
    }
}

#[test]
fn every_statement_of_the_biomedical_result_is_dated() {
    let readme = readme();
    let re = regex::Regex::new(r"(\d+)\s*(?:/|of)\s*100 queries").unwrap();
    let hits: Vec<_> = re.captures_iter(&readme).collect();
    assert!(
        !hits.is_empty(),
        "README no longer states the biomedical result; update this test"
    );
    for cap in &hits {
        let at = cap.get(0).unwrap().start();
        let para = paragraph_around(&readme, at);
        let line_no = readme[..at].lines().count() + 1;
        assert_eq!(
            &cap[1], "96",
            "README.md:{line_no} states {}/100; the one measurement (2026-04-02) is 96",
            &cap[1]
        );
        assert!(
            para.contains("measured 2026-04-02"),
            "README.md:{line_no} states the biomedical result without its date. No harness \
             suite re-measures it, so the claim register cannot check it; the text has to say \
             when it was measured (#1330)"
        );
    }
}

#[test]
fn the_xk02_headline_matches_the_figure_it_quotes() {
    let readme = readme();
    let head = regex::Regex::new(r"\*\*(\d+\.\d) seconds\.\*\* One query\. Four databases\.")
        .unwrap()
        .captures(&readme)
        .expect("README cross-KG headline");
    let rest = &readme[head.get(0).unwrap().end()..];
    let next = &rest[..rest.find("[See all 100").unwrap_or(rest.len())];
    let ms = regex::Regex::new(r"([\d,]+\.\d) ms")
        .unwrap()
        .captures(next)
        .expect("the headline must be followed by the verified-results.csv time it rounds");
    let ms: f64 = ms[1].replace(',', "").parse().unwrap();
    let secs: f64 = head[1].parse().unwrap();
    assert!(
        (ms / 1000.0 - secs).abs() < 0.05 + 1e-9,
        "headline says {secs} s; the quoted CSV figure is {ms} ms"
    );
    assert!(
        next.contains("verified-results.csv") && next.contains("measured 2026-04-02"),
        "the XK02 headline must name verified-results.csv and its measurement date"
    );

    let row = regex::Regex::new(r"\| XK02 \|[^|]*\| (\d+\.\d)s \|")
        .unwrap()
        .captures(&readme)
        .expect("XK02 row in the Cross-KG table");
    assert_eq!(
        &row[1], &head[1],
        "the XK02 table row and the headline disagree"
    );
}
